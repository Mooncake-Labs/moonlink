//! A replication reconnect in the middle of a non-streamed transaction (a review probe):
//! postgres re-sends the whole transaction, and none of it may be applied twice.
//! cargo test -p moonlink_backend --features test-utils --test test_reconnect_mid_transaction -- --test-threads=1
mod common;

use arrow::util::display::{ArrayFormatter, FormatOptions};
use common::{connect_to_postgres, current_wal_lsn, get_database_uri};
use moonlink::decode_read_state_for_testing;
use moonlink_backend::table_config::{MooncakeConfig, TableConfig};
use moonlink_backend::MoonlinkBackend;
use moonlink_metadata_store::SqliteMetadataStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serial_test::serial;
use std::collections::HashSet;
use std::fs::File;
use std::time::Duration;
use tempfile::TempDir;
use tokio_postgres::Client;

const DB: &str = "reconnect-mid-txn";

fn table_config(tmp: &TempDir) -> String {
    let root = tmp.path().to_str().unwrap().to_string();
    let cfg = TableConfig {
        mooncake_config: MooncakeConfig {
            skip_index_merge: true,
            skip_data_compaction: true,
            append_only: Some(false),
            row_identity: Some(moonlink::row::IdentityProp::FullRow),
        },
        iceberg_config: Some(moonlink::AccessorConfig::new_with_storage_config(
            moonlink::StorageConfig::FileSystem {
                root_directory: root,
                atomic_write_dir: None,
            },
        )),
    };
    serde_json::to_string(&cfg).unwrap()
}

async fn new_backend(tmp: &TempDir) -> MoonlinkBackend {
    let base = tmp.path().to_str().unwrap().to_string();
    let store = SqliteMetadataStore::new_with_directory(&base)
        .await
        .unwrap();
    MoonlinkBackend::new(base, None, Box::new(store))
        .await
        .unwrap()
}

async fn setup(ddl: &str) -> (TempDir, MoonlinkBackend, Client) {
    let uri = get_database_uri();
    let (client, _) = connect_to_postgres(&uri).await;
    client
        .simple_query(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name LIKE 'moonlink_slot%' AND database = current_database() AND active_pid IS NOT NULL;",
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    client
        .simple_query(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'moonlink_slot%' AND database = current_database();
             DROP PUBLICATION IF EXISTS moonlink_pub;",
        )
        .await
        .unwrap();
    client.simple_query(ddl).await.unwrap();
    let tmp = TempDir::new().unwrap();
    let backend = new_backend(&tmp).await;
    (tmp, backend, client)
}

async fn mirror(backend: &MoonlinkBackend, tmp: &TempDir, dst: &str, src: &str) {
    backend
        .create_table(
            DB.to_string(),
            dst.to_string(),
            src.to_string(),
            get_database_uri(),
            table_config(tmp),
            None,
        )
        .await
        .unwrap();
}

/// Every visible row of the mirror (duplicates kept) as (id, formatted values of `cols`), sorted.
/// Err(message) if the scan fails (errored mirror) or times out.
async fn scan_all(
    backend: &MoonlinkBackend,
    dst: &str,
    lsn: u64,
    cols: &[&str],
) -> Result<Vec<(i64, Vec<Option<String>>)>, String> {
    let state = tokio::time::timeout(
        Duration::from_secs(30),
        backend.scan_table(DB.to_string(), dst.to_string(), Some(lsn)),
    )
    .await
    .map_err(|_| "scan timed out".to_string())?
    .map_err(|e| format!("{e:?}"))?;
    let (files, puffins, _dvs, pos_deletes) = decode_read_state_for_testing(&state);
    assert!(
        puffins.is_empty(),
        "helper does not decode deletion vectors"
    );
    let deleted: HashSet<(u32, u32)> = pos_deletes
        .iter()
        .map(|d| (d.data_file_number, d.data_file_row_number))
        .collect();
    let opts = FormatOptions::default().with_null("<NULL>");
    let mut out = Vec::new();
    for (file_no, path) in files.iter().enumerate() {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let mut row_no = 0u32;
        for batch in reader {
            let batch = batch.unwrap();
            let id_col = batch.column_by_name("id").expect("id column");
            let id_fmt = ArrayFormatter::try_new(id_col.as_ref(), &opts).unwrap();
            let fmts: Vec<Option<(ArrayFormatter, &dyn arrow::array::Array)>> = cols
                .iter()
                .map(|c| {
                    batch.column_by_name(c).map(|a| {
                        (
                            ArrayFormatter::try_new(a.as_ref(), &opts).unwrap(),
                            a.as_ref(),
                        )
                    })
                })
                .collect();
            for i in 0..batch.num_rows() {
                if !deleted.contains(&(file_no as u32, row_no)) {
                    let id: i64 = id_fmt.value(i).to_string().parse().unwrap();
                    let vals = fmts
                        .iter()
                        .map(|f| match f {
                            None => Some("<MISSING COLUMN>".to_string()),
                            Some((fmt, arr)) => (!arr.is_null(i)).then(|| fmt.value(i).to_string()),
                        })
                        .collect();
                    out.push((id, vals));
                }
                row_no += 1;
            }
        }
    }
    out.sort();
    Ok(out)
}

async fn walsender_pid(client: &Client) -> Option<i32> {
    client
        .query_opt(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name LIKE 'moonlink_slot%' AND database = current_database() AND active_pid IS NOT NULL",
            &[],
        )
        .await
        .unwrap()
        .map(|r| r.get(0))
}

/// The walsender is killed while a large (but not streamed) transaction is being sent. The rows
/// already handed to the table before the disconnect must not be applied twice after the replay.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn reconnect_mid_transaction_applies_it_once() {
    let (tmp, backend, client) =
        setup("DROP TABLE IF EXISTS rvb_dup; CREATE TABLE rvb_dup (id BIGINT PRIMARY KEY);").await;
    mirror(&backend, &tmp, "rvb_dup_mc", "public.rvb_dup").await;
    let mut expected = 0i64;
    let mut terminated = 0;
    for (round, delay_ms) in [30u64, 80, 150, 300, 600].into_iter().enumerate() {
        let base = (round as i64) * 1_000_000;
        client
            .simple_query(&format!(
                "INSERT INTO rvb_dup SELECT i FROM generate_series({base}, {base} + 199999) i;"
            ))
            .await
            .unwrap();
        expected += 200_000;
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        if let Some(pid) = walsender_pid(&client).await {
            client
                .simple_query(&format!("SELECT pg_terminate_backend({pid});"))
                .await
                .unwrap();
            terminated += 1;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    let lsn = current_wal_lsn(&client).await;
    let mut got = None;
    for _ in 0..6 {
        match scan_all(&backend, "rvb_dup_mc", lsn, &[]).await {
            Ok(rows) => {
                got = Some(rows);
                break;
            }
            Err(_) => continue,
        }
    }
    let rows = got.expect("mirror must be readable");
    let unique: HashSet<i64> = rows.iter().map(|(i, _)| *i).collect();
    eprintln!(
        "terminated {terminated} walsenders; rows {} unique {} expected {expected}",
        rows.len(),
        unique.len()
    );
    assert_eq!(unique.len() as i64, expected, "rows lost");
    assert_eq!(rows.len() as i64, expected, "rows applied twice");
    assert!(terminated > 0, "no replication connection was cut");
}
