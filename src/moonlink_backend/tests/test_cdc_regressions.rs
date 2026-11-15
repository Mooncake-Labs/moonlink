//! Regression tests for the Postgres CDC ingest path. Run serially against a local Postgres with
//! wal_level=logical (DATABASE_URL), e.g.:
//! cargo test -p moonlink_backend --features test-utils --test test_cdc_regressions -- --test-threads=1
mod common;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use common::{connect_to_postgres, current_wal_lsn, get_database_uri};
use moonlink::decode_read_state_for_testing;
use moonlink_backend::table_config::{MooncakeConfig, TableConfig};
use moonlink_backend::MoonlinkBackend;
use moonlink_connectors::pg_replicate::PostgresConnection;
use moonlink_metadata_store::SqliteMetadataStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serial_test::serial;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::time::Duration;
use tempfile::TempDir;
use tokio_postgres::Client;

const DB: &str = "cdc-regressions";

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

async fn new_backend(tmp: &TempDir) -> Result<MoonlinkBackend, String> {
    let base = tmp.path().to_str().unwrap().to_string();
    let store = SqliteMetadataStore::new_with_directory(&base)
        .await
        .unwrap();
    MoonlinkBackend::new(base, None, Box::new(store))
        .await
        .map_err(|e| format!("{e:?}"))
}

/// Fresh backend + client, with all moonlink slots/publications from earlier runs removed.
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
    let backend = new_backend(&tmp).await.unwrap();
    (tmp, backend, client)
}

async fn mirror(backend: &MoonlinkBackend, tmp: &TempDir, dst: &str, src: &str, uri: &str) {
    backend
        .create_table(
            DB.to_string(),
            dst.to_string(),
            src.to_string(),
            uri.to_string(),
            table_config(tmp),
            None,
        )
        .await
        .unwrap();
}

/// Visible rows of the mirror as id -> value of `col` (cast to string), applying in-memory
/// position deletes. Returns None if the scan does not complete within `timeout`.
async fn scan_rows(
    backend: &MoonlinkBackend,
    dst: &str,
    lsn: u64,
    col: &str,
    timeout: Duration,
) -> Option<BTreeMap<i64, Option<String>>> {
    let state = tokio::time::timeout(
        timeout,
        backend.scan_table(DB.to_string(), dst.to_string(), Some(lsn)),
    )
    .await
    .ok()?
    .unwrap();
    let (files, puffins, _dvs, pos_deletes) = decode_read_state_for_testing(&state);
    assert!(
        puffins.is_empty(),
        "test helper does not decode deletion vectors"
    );
    let deleted: HashSet<(u32, u32)> = pos_deletes
        .iter()
        .map(|d| (d.data_file_number, d.data_file_row_number))
        .collect();
    let mut out = BTreeMap::new();
    for (file_no, path) in files.iter().enumerate() {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let mut row_no = 0u32;
        for batch in reader {
            let batch = batch.unwrap();
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            let vals = cast(batch.column_by_name(col).unwrap(), &DataType::Utf8).unwrap();
            let vals = vals.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..batch.num_rows() {
                if !deleted.contains(&(file_no as u32, row_no)) {
                    let v = (!vals.is_null(i)).then(|| vals.value(i).to_string());
                    out.insert(ids.value(i), v);
                }
                row_no += 1;
            }
        }
    }
    Some(out)
}

async fn scan_ids(backend: &MoonlinkBackend, dst: &str, lsn: u64) -> Option<Vec<i64>> {
    scan_rows(backend, dst, lsn, "id", Duration::from_secs(30))
        .await
        .map(|m| m.into_keys().collect())
}

/// The test connection string pointed at another database on the same server.
fn uri_for_database(uri: &str, database: &str) -> String {
    let (base, query) = match uri.split_once('?') {
        Some((base, query)) => (base, format!("?{query}")),
        None => (uri, String::new()),
    };
    let (server, _) = base.rsplit_once('/').unwrap();
    format!("{server}/{database}{query}")
}

async fn slot_count(client: &Client) -> i64 {
    client
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'moonlink_slot%'",
            &[],
        )
        .await
        .unwrap()
        .get(0)
}

/// Slots are dropped by a background shutdown task once the last mirror of a database is gone.
async fn wait_for_no_slots(client: &Client) {
    for _ in 0..40 {
        if slot_count(client).await == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(
        slot_count(client).await,
        0,
        "replication slot was not dropped"
    );
}

/// Restarting moonlink must not drop and re-create the publication: changes committed while the
/// table is not yet re-added (here, before the new backend recovers it) must still replicate.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn publication_restart_keeps_rows_committed_during_recovery() {
    let uri = get_database_uri();
    let (tmp, backend, client) =
        setup("DROP TABLE IF EXISTS rv_gap; CREATE TABLE rv_gap (id BIGINT PRIMARY KEY);").await;
    mirror(&backend, &tmp, "rv_gap_mc", "public.rv_gap", &uri).await;
    client
        .simple_query("INSERT INTO rv_gap VALUES (1);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    scan_ids(&backend, "rv_gap_mc", lsn).await.unwrap();
    backend
        .create_snapshot(DB.to_string(), "rv_gap_mc".to_string(), lsn)
        .await
        .unwrap();
    backend.shutdown_connection(&uri, false).await;
    drop(backend);
    // A restart's first step: connecting to the source. A write right after it must not be lost.
    drop(PostgresConnection::new(uri.clone()).await.unwrap());
    client
        .simple_query("INSERT INTO rv_gap VALUES (2);")
        .await
        .unwrap();
    let backend = new_backend(&tmp).await.unwrap();
    client
        .simple_query("INSERT INTO rv_gap VALUES (3);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let ids = scan_ids(&backend, "rv_gap_mc", lsn)
        .await
        .expect("scan timed out");
    assert_eq!(ids, vec![1, 2, 3]);
}

/// Two connection strings for the same database (pg_mooncake's default one embeds the current
/// user) must share one replication connection: the second mirror must not take the slot or the
/// publication away from the first, across restarts too, and dropping both removes the slot.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn second_connection_string_for_same_database_shares_replication() {
    let uri = get_database_uri();
    let sep = if uri.contains('?') { '&' } else { '?' };
    let uri2 = format!("{uri}{sep}application_name=second_user");
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_u1, rv_u2;
         CREATE TABLE rv_u1 (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_u2 (id BIGINT PRIMARY KEY);",
    )
    .await;
    mirror(&backend, &tmp, "rv_u1_mc", "public.rv_u1", &uri).await;
    client
        .simple_query("INSERT INTO rv_u1 VALUES (1);")
        .await
        .unwrap();
    mirror(&backend, &tmp, "rv_u2_mc", "public.rv_u2", &uri2).await;
    client
        .simple_query("INSERT INTO rv_u1 VALUES (2); INSERT INTO rv_u2 VALUES (20);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(scan_ids(&backend, "rv_u1_mc", lsn).await, Some(vec![1, 2]));
    assert_eq!(scan_ids(&backend, "rv_u2_mc", lsn).await, Some(vec![20]));
    assert_eq!(slot_count(&client).await, 1);

    // Recovery registers both mirrors again, in either order, on one connection.
    backend.shutdown_connection(&uri, false).await;
    drop(backend);
    let backend = new_backend(&tmp).await.unwrap();
    client
        .simple_query("INSERT INTO rv_u1 VALUES (3); INSERT INTO rv_u2 VALUES (30);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_u1_mc", lsn).await,
        Some(vec![1, 2, 3])
    );
    assert_eq!(
        scan_ids(&backend, "rv_u2_mc", lsn).await,
        Some(vec![20, 30])
    );

    for dst in ["rv_u1_mc", "rv_u2_mc"] {
        backend
            .drop_table(DB.to_string(), dst.to_string())
            .await
            .unwrap();
    }
    wait_for_no_slots(&client).await;
}

/// Database names that are not valid slot names (`my-app`) must still be mirrorable.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn database_name_that_is_not_a_valid_slot_name_can_be_mirrored() {
    let admin_uri = get_database_uri();
    let (admin, _) = connect_to_postgres(&admin_uri).await;
    admin
        .simple_query(r#"DROP DATABASE IF EXISTS "my-app" WITH (FORCE);"#)
        .await
        .unwrap();
    admin
        .simple_query(r#"CREATE DATABASE "my-app";"#)
        .await
        .unwrap();
    let uri = uri_for_database(&admin_uri, "my-app");
    let (client, _) = connect_to_postgres(&uri).await;
    client
        .simple_query("CREATE TABLE rv_db (id BIGINT PRIMARY KEY);")
        .await
        .unwrap();
    let tmp = TempDir::new().unwrap();
    let backend = new_backend(&tmp).await.unwrap();
    mirror(&backend, &tmp, "rv_db_mc", "public.rv_db", &uri).await;
    client
        .simple_query("INSERT INTO rv_db VALUES (1);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(scan_ids(&backend, "rv_db_mc", lsn).await, Some(vec![1]));
    let slot: String = client
        .query_one(
            "SELECT slot_name::text FROM pg_replication_slots WHERE database = 'my-app'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        slot,
        moonlink_connectors::pg_replicate::slot_name_for_database("my-app")
    );
    backend
        .drop_table(DB.to_string(), "rv_db_mc".to_string())
        .await
        .unwrap();
    wait_for_no_slots(&client).await;
    drop(client);
    admin
        .simple_query(r#"DROP DATABASE IF EXISTS "my-app" WITH (FORCE);"#)
        .await
        .unwrap();
}
