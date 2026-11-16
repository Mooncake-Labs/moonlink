//! Regression tests for the Postgres CDC ingest path. Run serially against a local Postgres with
//! wal_level=logical (DATABASE_URL), e.g.:
//! cargo test -p moonlink_backend --features test-utils --test test_cdc_regressions -- --test-threads=1
//! Tests that stop and start the source Postgres run only with MOONLINK_TEST_PG_CTL set to a
//! command taking `stop` or `start` (for example a script around pg_ctl).
mod common;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::util::display::array_value_to_string;
use common::{connect_to_postgres, current_wal_lsn, get_database_uri};
use moonlink_backend::table_config::{MooncakeConfig, TableConfig};
use moonlink_backend::MoonlinkBackend;
use moonlink_connectors::pg_replicate::PostgresConnection;
use moonlink_metadata_store::SqliteMetadataStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serial_test::serial;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::sync::Arc;
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

/// An error as text for assertions: its debug form without any backtrace (captured with
/// RUST_BACKTRACE=1, which names the test's own functions and would match their words).
fn error_text(e: &impl std::fmt::Debug) -> String {
    let text = format!("{e:?}");
    match text.split_once("Stack backtrace") {
        Some((message, _)) => message.to_string(),
        None => text,
    }
}

async fn new_backend(tmp: &TempDir) -> Result<MoonlinkBackend, String> {
    let base = tmp.path().to_str().unwrap().to_string();
    let store = SqliteMetadataStore::new_with_directory(&base)
        .await
        .unwrap();
    MoonlinkBackend::new(base, None, Box::new(store))
        .await
        .map_err(|e| error_text(&e))
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
    // A test that failed while forcing large transactions to stream must not leak that setting.
    stream_large_transactions(&client, false).await;
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

/// Visible rows of the mirror as id -> value of `col` (cast to string), applying position deletes
/// and deletion vectors. Returns None if the scan does not complete within `timeout`.
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
    let (files, pos_deletes) = common::data_files_and_deleted_rows(&state).await;
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
            let column = batch.column_by_name(col).unwrap();
            // Nested values (lists, structs) have no cast to text and bytes may not be utf-8;
            // display them instead (bytes as hex).
            let vals = match column.data_type() {
                DataType::List(_) | DataType::Struct(_) | DataType::Binary => {
                    let text: Vec<Option<String>> = (0..column.len())
                        .map(|i| {
                            (!column.is_null(i)).then(|| array_value_to_string(column, i).unwrap())
                        })
                        .collect();
                    Arc::new(StringArray::from(text)) as ArrayRef
                }
                _ => cast(column, &DataType::Utf8).unwrap(),
            };
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
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'moonlink_slot%' AND database = current_database()",
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

/// The initial copy and the replication stream must meet at an exact boundary: with a concurrent
/// writer using asynchronous commit, every row is in the mirror exactly once, whether or not the
/// table had rows when the mirror was created.
#[rstest::rstest]
#[case::populated_table(20000)]
#[case::empty_table(0)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn initial_copy_and_stream_apply_each_row_exactly_once(#[case] initial_rows: i64) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(&format!(
        "DROP TABLE IF EXISTS rv_copy; CREATE TABLE rv_copy (id BIGINT PRIMARY KEY);
         INSERT INTO rv_copy SELECT generate_series(1, {initial_rows});"
    ))
    .await;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (stop, uri) = (stop.clone(), uri.clone());
        tokio::spawn(async move {
            let (w, _) = connect_to_postgres(&uri).await;
            w.simple_query("SET synchronous_commit = off;")
                .await
                .unwrap();
            let mut id = 1_000_000i64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                w.simple_query(&format!("INSERT INTO rv_copy VALUES ({id});"))
                    .await
                    .unwrap();
                id += 1;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    mirror(&backend, &tmp, "rv_copy_mc", "public.rv_copy", &uri).await;
    let copy_slots: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'moonlink_copy_%'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        copy_slots, 0,
        "temporary copy slot outlived the initial copy"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.await.unwrap();
    // Let ingestion settle before scanning (see the read-path LSN ordering work).
    tokio::time::sleep(Duration::from_secs(3)).await;
    client
        .simple_query("INSERT INTO rv_copy VALUES (-1);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let state = backend
        .scan_table(DB.to_string(), "rv_copy_mc".to_string(), Some(lsn))
        .await
        .unwrap();
    let counts = common::nonunique_ids_from_state(&state);
    let source_ids: HashSet<i64> = client
        .query("SELECT id FROM rv_copy", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, i64>(0))
        .collect();
    let duplicated: Vec<i64> = counts
        .iter()
        .filter(|(_, c)| **c > 1)
        .map(|(i, _)| *i)
        .collect();
    let missing: Vec<i64> = source_ids
        .iter()
        .filter(|i| !counts.contains_key(i))
        .copied()
        .collect();
    assert!(
        duplicated.is_empty() && missing.is_empty() && counts.len() == source_ids.len(),
        "{} duplicated (e.g. {:?}), {} missing (e.g. {:?}), mirror {} ids, source {} ids",
        duplicated.len(),
        &duplicated[..duplicated.len().min(5)],
        missing.len(),
        &missing[..missing.len().min(5)],
        counts.len(),
        source_ids.len()
    );
}

/// Request a snapshot to probe a mirror's state: errored mirrors reject it with the user-facing
/// "drop and re-create" message.
async fn snapshot_error(backend: &MoonlinkBackend, dst: &str, lsn: u64) -> Option<String> {
    // Generous: a snapshot on a loaded CI runner can take well over 30 s.
    tokio::time::timeout(
        Duration::from_secs(120),
        backend.create_snapshot(DB.to_string(), dst.to_string(), lsn),
    )
    .await
    .expect("create_snapshot timed out")
    .err()
    .map(|e| error_text(&e))
}

/// Any change to a mirrored table's definition puts that mirror, and only that mirror, into the
/// errored state; it never panics replication and the mirror can still be dropped.
#[rstest::rstest]
#[case::alter_column_type(
    "ALTER TABLE rv_ddl ALTER COLUMN a TYPE TEXT;",
    "INSERT INTO rv_ddl VALUES (2, 'two');",
    "type of column(s) a changed"
)]
#[case::add_column(
    "ALTER TABLE rv_ddl ADD COLUMN b INT DEFAULT 5;",
    "INSERT INTO rv_ddl VALUES (2, 2, 2);",
    "columns changed"
)]
#[case::drop_column(
    "ALTER TABLE rv_ddl DROP COLUMN a;",
    "INSERT INTO rv_ddl VALUES (2);",
    "columns changed"
)]
#[case::rename_column(
    "ALTER TABLE rv_ddl RENAME COLUMN a TO c;",
    "INSERT INTO rv_ddl VALUES (2, 2);",
    "columns changed"
)]
#[case::replica_identity_default(
    "ALTER TABLE rv_ddl REPLICA IDENTITY DEFAULT;",
    "UPDATE rv_ddl SET a = 9 WHERE id = 1;",
    "REPLICA IDENTITY"
)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn source_schema_change_errors_only_that_mirror(
    #[case] ddl: &str,
    #[case] dml: &str,
    #[case] reason: &str,
) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_ddl, rv_bystander;
         CREATE TABLE rv_ddl (id BIGINT PRIMARY KEY, a INT);
         CREATE TABLE rv_bystander (id BIGINT PRIMARY KEY);",
    )
    .await;
    mirror(&backend, &tmp, "rv_ddl_mc", "public.rv_ddl", &uri).await;
    mirror(
        &backend,
        &tmp,
        "rv_bystander_mc",
        "public.rv_bystander",
        &uri,
    )
    .await;
    client
        .simple_query("INSERT INTO rv_ddl VALUES (1, 1); INSERT INTO rv_bystander VALUES (1);")
        .await
        .unwrap();
    client.simple_query(ddl).await.unwrap();
    // The changed table's rows and the bystander's rows in one transaction.
    client
        .simple_query(&format!(
            "BEGIN; {dml} INSERT INTO rv_bystander VALUES (2); COMMIT;"
        ))
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO rv_bystander VALUES (3);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![1, 2, 3])
    );
    let err = snapshot_error(&backend, "rv_ddl_mc", lsn)
        .await
        .expect("changed mirror must be errored");
    assert!(err.contains(reason) && err.contains("re-create"), "{err}");
    backend
        .drop_table(DB.to_string(), "rv_ddl_mc".to_string())
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO rv_bystander VALUES (4);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![1, 2, 3, 4])
    );
    assert_eq!(snapshot_error(&backend, "rv_bystander_mc", lsn).await, None);
}

/// An errored mirror never persists again, so it must not hold the replication slot back.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn errored_mirror_does_not_hold_back_the_slot() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_ddl, rv_bystander;
         CREATE TABLE rv_ddl (id BIGINT PRIMARY KEY, a INT);
         CREATE TABLE rv_bystander (id BIGINT PRIMARY KEY);",
    )
    .await;
    mirror(&backend, &tmp, "rv_ddl_mc", "public.rv_ddl", &uri).await;
    mirror(
        &backend,
        &tmp,
        "rv_bystander_mc",
        "public.rv_bystander",
        &uri,
    )
    .await;
    client
        .simple_query(
            "ALTER TABLE rv_ddl ADD COLUMN b INT; INSERT INTO rv_ddl VALUES (1, 1, 1);
             INSERT INTO rv_bystander VALUES (1);",
        )
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![1])
    );
    assert!(snapshot_error(&backend, "rv_ddl_mc", lsn).await.is_some());
    backend
        .create_snapshot(DB.to_string(), "rv_bystander_mc".to_string(), lsn)
        .await
        .unwrap();
    // Slot feedback is sent every 10 seconds.
    let mut confirmed = 0u64;
    for _ in 0..30 {
        let row = client
            .query_one(
                "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name LIKE 'moonlink_slot%'",
                &[],
            )
            .await
            .unwrap();
        confirmed = row.get::<_, tokio_postgres::types::PgLsn>(0).into();
        if confirmed >= lsn {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(
        confirmed >= lsn,
        "slot stuck at {confirmed}, expected >= {lsn}"
    );
}

/// A value moonlink cannot represent (timestamp 'infinity') fails only its own mirror. No other
/// change in the same batch or transaction is dropped.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn unconvertible_value_errors_only_its_mirror() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_ts, rv_bystander;
         CREATE TABLE rv_ts (id BIGINT PRIMARY KEY, ts TIMESTAMP);
         CREATE TABLE rv_bystander (id BIGINT PRIMARY KEY);",
    )
    .await;
    mirror(&backend, &tmp, "rv_ts_mc", "public.rv_ts", &uri).await;
    mirror(
        &backend,
        &tmp,
        "rv_bystander_mc",
        "public.rv_bystander",
        &uri,
    )
    .await;
    // One transaction whose bad row sits in the middle of both tables' rows.
    client
        .simple_query(
            "BEGIN;
             INSERT INTO rv_bystander SELECT generate_series(1, 50);
             INSERT INTO rv_ts SELECT i, CASE WHEN i = 50 THEN 'infinity'::timestamp ELSE '2020-01-01'::timestamp END FROM generate_series(1, 100) i;
             INSERT INTO rv_bystander SELECT generate_series(51, 100);
             COMMIT;
             INSERT INTO rv_bystander VALUES (101);",
        )
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some((1..=101).collect::<Vec<i64>>())
    );
    let err = snapshot_error(&backend, "rv_ts_mc", lsn)
        .await
        .expect("mirror with the bad value must be errored");
    assert!(
        err.contains("column ts") && err.contains("'infinity'") && err.contains("re-create"),
        "{err}"
    );
    backend
        .drop_table(DB.to_string(), "rv_ts_mc".to_string())
        .await
        .unwrap();
}

/// An UPDATE that leaves large (out-of-line, TOASTed) values untouched sends them as "unchanged";
/// the mirror must keep the full values, not empty or default ones.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn update_keeps_unchanged_toasted_values() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_toast;
         CREATE TABLE rv_toast (id BIGINT PRIMARY KEY, name TEXT, big TEXT, doc JSONB, raw BYTEA);
         ALTER TABLE rv_toast ALTER COLUMN big SET STORAGE EXTERNAL;
         ALTER TABLE rv_toast ALTER COLUMN doc SET STORAGE EXTERNAL;
         ALTER TABLE rv_toast ALTER COLUMN raw SET STORAGE EXTERNAL;",
    )
    .await;
    mirror(&backend, &tmp, "rv_toast_mc", "public.rv_toast", &uri).await;
    client
        .simple_query(
            "INSERT INTO rv_toast
             SELECT 1, 'a', repeat('x', 64000),
                    jsonb_build_object('k', repeat('y', 30000)),
                    decode(repeat('41', 20000), 'hex');
             UPDATE rv_toast SET name = 'b' WHERE id = 1;",
        )
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let timeout = Duration::from_secs(30);
    let name = scan_rows(&backend, "rv_toast_mc", lsn, "name", timeout)
        .await
        .unwrap();
    assert_eq!(name.get(&1).cloned().flatten().as_deref(), Some("b"));
    let len =
        |rows: BTreeMap<i64, Option<String>>| rows.get(&1).cloned().flatten().map(|v| v.len());
    let big = scan_rows(&backend, "rv_toast_mc", lsn, "big", timeout)
        .await
        .unwrap();
    assert_eq!(len(big), Some(64000));
    let doc = scan_rows(&backend, "rv_toast_mc", lsn, "doc", timeout)
        .await
        .unwrap();
    assert!(len(doc).unwrap_or(0) > 30000, "jsonb value lost");
    // The helper reads BYTEA through a cast to UTF-8, hence the ASCII bytes above.
    let raw = scan_rows(&backend, "rv_toast_mc", lsn, "raw", timeout)
        .await
        .unwrap();
    assert!(len(raw).unwrap_or(0) >= 20000, "bytea value lost");
}

/// The test connection string with other credentials.
fn uri_for_user(uri: &str, user: &str, password: &str) -> String {
    let (scheme, rest) = uri.split_once("://").unwrap();
    let host_part = rest.rsplit_once('@').map(|(_, host)| host).unwrap_or(rest);
    format!("{scheme}://{user}:{password}@{host_part}")
}

/// Replication must survive the source refusing connections for a while (restart, network,
/// failover): it keeps retrying, applies everything committed meanwhile once it is back, and the
/// mirror can still be dropped, including its slot.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn replication_resumes_after_source_refuses_connections() {
    let admin_uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_outage;
         CREATE TABLE rv_outage (id BIGINT PRIMARY KEY);
         DROP ROLE IF EXISTS rv_outage_user;
         CREATE ROLE rv_outage_user SUPERUSER LOGIN PASSWORD 'rv_outage';",
    )
    .await;
    let uri = uri_for_user(&admin_uri, "rv_outage_user", "rv_outage");
    mirror(&backend, &tmp, "rv_outage_mc", "public.rv_outage", &uri).await;
    client
        .simple_query("INSERT INTO rv_outage VALUES (1);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(scan_ids(&backend, "rv_outage_mc", lsn).await, Some(vec![1]));

    // Refuse new connections from moonlink's role and cut the stream.
    client
        .simple_query(
            "ALTER ROLE rv_outage_user NOLOGIN;
             SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
              WHERE slot_name LIKE 'moonlink_slot%' AND active_pid IS NOT NULL;",
        )
        .await
        .unwrap();
    for id in 2..=4 {
        client
            .simple_query(&format!("INSERT INTO rv_outage VALUES ({id});"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    client
        .simple_query("ALTER ROLE rv_outage_user LOGIN;")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO rv_outage VALUES (5);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let ids = scan_rows(&backend, "rv_outage_mc", lsn, "id", Duration::from_secs(90))
        .await
        .map(|rows| rows.into_keys().collect::<Vec<_>>());
    assert_eq!(ids, Some(vec![1, 2, 3, 4, 5]));

    backend
        .drop_table(DB.to_string(), "rv_outage_mc".to_string())
        .await
        .unwrap();
    wait_for_no_slots(&client).await;
    client
        .simple_query("DROP OWNED BY rv_outage_user; DROP ROLE rv_outage_user;")
        .await
        .unwrap();
}

async fn try_mirror(
    backend: &Arc<MoonlinkBackend>,
    tmp: &TempDir,
    dst: &str,
    src: &str,
    uri: &str,
) -> std::result::Result<(), String> {
    let (b, cfg, dst, src, uri) = (
        backend.clone(),
        table_config(tmp),
        dst.to_string(),
        src.to_string(),
        uri.to_string(),
    );
    match tokio::spawn(async move {
        b.create_table(DB.to_string(), dst, src, uri, cfg, None)
            .await
    })
    .await
    {
        Ok(result) => result.map_err(|e| error_text(&e)),
        Err(join_error) => panic!("create_table panicked: {join_error}"),
    }
}

/// A failing initial copy returns an error from create_table (no panic), leaves nothing behind
/// (a retry fails the same way, no slot stays when it was the database's first mirror), and
/// never disturbs other mirrors.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn failed_initial_copy_is_a_clean_error() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_good, rv_bad;
         CREATE TABLE rv_good (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_bad (id BIGINT PRIMARY KEY, ts TIMESTAMP);
         INSERT INTO rv_bad VALUES (1, 'infinity');",
    )
    .await;
    let backend = Arc::new(backend);
    // As the database's first mirror: no slot may remain afterwards.
    let err = try_mirror(&backend, &tmp, "rv_bad_mc", "public.rv_bad", &uri)
        .await
        .unwrap_err();
    assert!(err.contains("infinity"), "{err}");
    wait_for_no_slots(&client).await;

    mirror(&backend, &tmp, "rv_good_mc", "public.rv_good", &uri).await;
    for attempt in 0..2 {
        let err = try_mirror(
            &backend,
            &tmp,
            &format!("rv_bad_mc{attempt}"),
            "public.rv_bad",
            &uri,
        )
        .await
        .unwrap_err();
        assert!(err.contains("infinity"), "attempt {attempt}: {err}");
    }
    client
        .simple_query("INSERT INTO rv_good VALUES (5);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(scan_ids(&backend, "rv_good_mc", lsn).await, Some(vec![5]));

    // The bad table can be mirrored once the bad value is gone.
    client
        .simple_query("UPDATE rv_bad SET ts = '2020-01-01';")
        .await
        .unwrap();
    try_mirror(&backend, &tmp, "rv_bad_mc", "public.rv_bad", &uri)
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(scan_ids(&backend, "rv_bad_mc", lsn).await, Some(vec![1]));
}

/// A source table can have only one mirror; a second one is refused without affecting the first.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn second_mirror_of_same_source_is_refused() {
    let uri = get_database_uri();
    let (tmp, backend, client) =
        setup("DROP TABLE IF EXISTS rv_one; CREATE TABLE rv_one (id BIGINT PRIMARY KEY);").await;
    let backend = Arc::new(backend);
    mirror(&backend, &tmp, "rv_one_mc", "public.rv_one", &uri).await;
    let err = try_mirror(&backend, &tmp, "rv_one_mc2", "public.rv_one", &uri)
        .await
        .unwrap_err();
    assert!(err.contains("already mirrored"), "{err}");
    client
        .simple_query("INSERT INTO rv_one VALUES (1);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(scan_ids(&backend, "rv_one_mc", lsn).await, Some(vec![1]));
}

async fn scan_error(backend: &MoonlinkBackend, dst: &str) -> Option<String> {
    backend
        .scan_table(DB.to_string(), dst.to_string(), None)
        .await
        .err()
        .map(|e| error_text(&e))
}

/// A mirror that cannot be recovered at startup (its source table was dropped) must not stop
/// the service or the other mirrors; it reports why and can be dropped.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn startup_survives_a_mirror_whose_source_is_gone() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_keep, rv_gone;
         CREATE TABLE rv_keep (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_gone (id BIGINT PRIMARY KEY);",
    )
    .await;
    mirror(&backend, &tmp, "rv_keep_mc", "public.rv_keep", &uri).await;
    mirror(&backend, &tmp, "rv_gone_mc", "public.rv_gone", &uri).await;
    client
        .simple_query("INSERT INTO rv_keep VALUES (1); INSERT INTO rv_gone VALUES (1);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    scan_ids(&backend, "rv_keep_mc", lsn).await.unwrap();
    backend.shutdown_connection(&uri, false).await;
    drop(backend);
    client.simple_query("DROP TABLE rv_gone;").await.unwrap();

    let backend = new_backend(&tmp).await.expect("startup must not fail");
    client
        .simple_query("INSERT INTO rv_keep VALUES (2);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_keep_mc", lsn).await,
        Some(vec![1, 2])
    );
    let err = scan_error(&backend, "rv_gone_mc")
        .await
        .expect("must be errored");
    assert!(
        err.contains("could not be restored") && err.contains("re-create"),
        "{err}"
    );
    backend
        .drop_table(DB.to_string(), "rv_gone_mc".to_string())
        .await
        .unwrap();
    backend.shutdown_connection(&uri, false).await;
    drop(backend);
    let backend = new_backend(&tmp).await.unwrap();
    let err = scan_error(&backend, "rv_gone_mc").await.unwrap();
    assert!(err.contains("not found"), "dropped mirror came back: {err}");
}

/// A mirror that entered the errored state stays errored across a restart (the slot has moved
/// past it, so resuming would serve stale data as if healthy), and once dropped it can be
/// re-created from scratch with the current source contents.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn errored_mirror_stays_errored_after_restart_until_recreated() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_ddl, rv_bystander;
         CREATE TABLE rv_ddl (id BIGINT PRIMARY KEY, a INT);
         CREATE TABLE rv_bystander (id BIGINT PRIMARY KEY);",
    )
    .await;
    mirror(&backend, &tmp, "rv_ddl_mc", "public.rv_ddl", &uri).await;
    mirror(
        &backend,
        &tmp,
        "rv_bystander_mc",
        "public.rv_bystander",
        &uri,
    )
    .await;
    client
        .simple_query(
            "INSERT INTO rv_ddl VALUES (1, 1);
             ALTER TABLE rv_ddl ADD COLUMN b INT;
             INSERT INTO rv_ddl VALUES (2, 2, 2);
             INSERT INTO rv_bystander VALUES (1);",
        )
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![1])
    );
    assert!(snapshot_error(&backend, "rv_ddl_mc", lsn).await.is_some());
    // Let the errored state be persisted.
    tokio::time::sleep(Duration::from_millis(500)).await;
    backend.shutdown_connection(&uri, false).await;
    drop(backend);

    let backend = new_backend(&tmp).await.unwrap();
    let err = scan_error(&backend, "rv_ddl_mc")
        .await
        .expect("errored mirror must stay errored after restart");
    assert!(err.contains("schema changes are not supported"), "{err}");
    client
        .simple_query("INSERT INTO rv_bystander VALUES (2);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![1, 2])
    );

    backend
        .drop_table(DB.to_string(), "rv_ddl_mc".to_string())
        .await
        .unwrap();
    mirror(&backend, &tmp, "rv_ddl_mc", "public.rv_ddl", &uri).await;
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(scan_ids(&backend, "rv_ddl_mc", lsn).await, Some(vec![1, 2]));
}

/// Partitioned source tables: REPLICA IDENTITY FULL must reach every partition, so updates and
/// deletes replicate (with a primary key) and are not rejected on the source (without one).
#[rstest::rstest]
#[case::with_primary_key("id BIGINT PRIMARY KEY, v TEXT")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn partitioned_source_replicates_updates_and_deletes(#[case] columns: &str) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(&format!(
        "DROP TABLE IF EXISTS rv_part, rv_bystander;
         CREATE TABLE rv_part ({columns}) PARTITION BY RANGE (id);
         CREATE TABLE rv_part_1 PARTITION OF rv_part FOR VALUES FROM (0) TO (100);
         CREATE TABLE rv_part_2 PARTITION OF rv_part FOR VALUES FROM (100) TO (200);
         CREATE TABLE rv_bystander (id BIGINT PRIMARY KEY);"
    ))
    .await;
    mirror(&backend, &tmp, "rv_part_mc", "public.rv_part", &uri).await;
    mirror(
        &backend,
        &tmp,
        "rv_bystander_mc",
        "public.rv_bystander",
        &uri,
    )
    .await;
    client
        .simple_query(
            "INSERT INTO rv_part VALUES (1, 'a'), (150, 'b'), (151, 'c');
             UPDATE rv_part SET v = 'z' WHERE id IN (1, 150);
             DELETE FROM rv_part WHERE id = 151;
             INSERT INTO rv_bystander VALUES (7);",
        )
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let rows = scan_rows(&backend, "rv_part_mc", lsn, "v", Duration::from_secs(30)).await;
    assert_eq!(
        rows,
        Some(BTreeMap::from([
            (1, Some("z".to_string())),
            (150, Some("z".to_string()))
        ]))
    );
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![7])
    );
}

/// TRUNCATE is not applied to mirrors; a truncated source puts only its mirror in the errored
/// state (never silently keeps the old rows), and a truncate that rolls back changes nothing.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn truncate_errors_only_that_mirror() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_trunc, rv_rolled_back, rv_bystander;
         CREATE TABLE rv_trunc (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_rolled_back (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_bystander (id BIGINT PRIMARY KEY);",
    )
    .await;
    for t in ["rv_trunc", "rv_rolled_back", "rv_bystander"] {
        mirror(
            &backend,
            &tmp,
            &format!("{t}_mc"),
            &format!("public.{t}"),
            &uri,
        )
        .await;
    }
    // Separate statements: in one multi-statement query, ROLLBACK would also undo the INSERTs.
    for sql in [
        "INSERT INTO rv_trunc VALUES (1); INSERT INTO rv_rolled_back VALUES (1);",
        "BEGIN; TRUNCATE rv_rolled_back; ROLLBACK;",
        "TRUNCATE rv_trunc;",
        "INSERT INTO rv_trunc VALUES (2); INSERT INTO rv_bystander VALUES (1);",
    ] {
        client.simple_query(sql).await.unwrap();
    }
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![1])
    );
    assert_eq!(
        scan_ids(&backend, "rv_rolled_back_mc", lsn).await,
        Some(vec![1])
    );
    let err = snapshot_error(&backend, "rv_trunc_mc", lsn)
        .await
        .expect("truncated mirror must be errored");
    assert!(
        err.contains("was truncated") && err.contains("re-create"),
        "{err}"
    );
    backend
        .drop_table(DB.to_string(), "rv_trunc_mc".to_string())
        .await
        .unwrap();
}

/// A mirror whose source never changes after creation (here: still empty) must be readable at
/// any later LSN, e.g. the reader's own last commit to another table.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn idle_empty_mirror_is_readable_at_later_lsn() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_empty, rv_bystander;
         CREATE TABLE rv_empty (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_bystander (id BIGINT PRIMARY KEY);",
    )
    .await;
    mirror(&backend, &tmp, "rv_empty_mc", "public.rv_empty", &uri).await;
    mirror(
        &backend,
        &tmp,
        "rv_bystander_mc",
        "public.rv_bystander",
        &uri,
    )
    .await;
    client
        .simple_query("INSERT INTO rv_bystander VALUES (1);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![1])
    );
    let empty = scan_rows(&backend, "rv_empty_mc", lsn, "id", Duration::from_secs(20)).await;
    assert_eq!(
        empty,
        Some(BTreeMap::new()),
        "scan of the idle mirror did not complete"
    );
    assert_eq!(snapshot_error(&backend, "rv_empty_mc", lsn).await, None);
}

/// Numeric values reach the mirror exactly (unconstrained numeric included); values the mirror
/// cannot store exactly (NaN, too large) put the mirror in the errored state instead of being
/// shifted, nulled or panicking replication.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn numeric_values_are_exact_or_error_the_mirror() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_num, rv_nan, rv_big, rv_bystander;
         CREATE TABLE rv_num (id BIGINT PRIMARY KEY, n NUMERIC, f NUMERIC(20, 5));
         CREATE TABLE rv_nan (id BIGINT PRIMARY KEY, n NUMERIC);
         CREATE TABLE rv_big (id BIGINT PRIMARY KEY, n NUMERIC);
         CREATE TABLE rv_bystander (id BIGINT PRIMARY KEY);",
    )
    .await;
    for t in ["rv_num", "rv_nan", "rv_big", "rv_bystander"] {
        mirror(
            &backend,
            &tmp,
            &format!("{t}_mc"),
            &format!("public.{t}"),
            &uri,
        )
        .await;
    }
    client
        .simple_query(
            "INSERT INTO rv_num VALUES (1, 1.5, 12.5), (2, 123.456, -0.00001), (3, -0.0000000001, 0);
             INSERT INTO rv_nan VALUES (1, 'NaN');
             INSERT INTO rv_big VALUES (1, 1e39);
             INSERT INTO rv_bystander VALUES (1);",
        )
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let timeout = Duration::from_secs(30);
    let n = scan_rows(&backend, "rv_num_mc", lsn, "n", timeout)
        .await
        .unwrap();
    let f = scan_rows(&backend, "rv_num_mc", lsn, "f", timeout)
        .await
        .unwrap();
    let as_f64 = |rows: &BTreeMap<i64, Option<String>>, id: i64| {
        rows[&id].as_deref().unwrap().parse::<f64>().unwrap()
    };
    assert_eq!(
        (as_f64(&n, 1), as_f64(&n, 2), as_f64(&n, 3)),
        (1.5, 123.456, -0.0000000001)
    );
    assert_eq!(
        (as_f64(&f, 1), as_f64(&f, 2), as_f64(&f, 3)),
        (12.5, -0.00001, 0.0)
    );
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![1])
    );
    let nan = snapshot_error(&backend, "rv_nan_mc", lsn)
        .await
        .expect("NaN must error");
    assert!(nan.contains("NaN"), "{nan}");
    let big = snapshot_error(&backend, "rv_big_mc", lsn)
        .await
        .expect("1e39 must error");
    assert!(big.contains("does not fit"), "{big}");
}

/// Arrays that are not one-dimensional with default bounds cannot be mirrored: a mirror that
/// receives one is errored (never silently mangled or dropped together with its transaction),
/// and creating a mirror of a table that holds one fails cleanly.
#[rstest::rstest]
#[case::int_array_2d("a", "'{{1,2},{3,4}}'")]
#[case::text_array_2d("t", "'{{x,y},{z,w}}'")]
#[case::explicit_bounds("a", "'[0:1]={1,2}'")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn arrays_that_are_not_one_dimensional_error_the_mirror(
    #[case] column: &str,
    #[case] value: &str,
) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_arr, rv_arr_copy, rv_bystander;
         CREATE TABLE rv_arr (id BIGINT PRIMARY KEY, a INT[], t TEXT[]);
         CREATE TABLE rv_arr_copy (id BIGINT PRIMARY KEY, a INT[], t TEXT[]);
         CREATE TABLE rv_bystander (id BIGINT PRIMARY KEY);",
    )
    .await;
    let backend = Arc::new(backend);
    mirror(&backend, &tmp, "rv_arr_mc", "public.rv_arr", &uri).await;
    mirror(
        &backend,
        &tmp,
        "rv_bystander_mc",
        "public.rv_bystander",
        &uri,
    )
    .await;
    client
        .simple_query(&format!(
            "BEGIN;
             INSERT INTO rv_arr (id, {column}) VALUES (1, {value});
             INSERT INTO rv_arr VALUES (2, '{{5}}', '{{y}}');
             INSERT INTO rv_bystander VALUES (1);
             COMMIT;"
        ))
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![1])
    );
    let err = snapshot_error(&backend, "rv_arr_mc", lsn)
        .await
        .expect("mirror must be errored");
    assert!(err.contains("not supported"), "{err}");

    client
        .simple_query(&format!(
            "INSERT INTO rv_arr_copy (id, {column}) VALUES (1, {value});"
        ))
        .await
        .unwrap();
    let err = try_mirror(&backend, &tmp, "rv_arr_copy_mc", "public.rv_arr_copy", &uri)
        .await
        .unwrap_err();
    assert!(err.contains("not supported"), "{err}");
}

/// numeric typmods that a decimal cannot represent (scale above precision, negative scale) are
/// refused cleanly when the mirror is created, leaving nothing behind.
#[rstest::rstest]
#[case::scale_above_precision("NUMERIC(3, 6)", "0.000123")]
#[case::negative_scale("NUMERIC(3, -3)", "12000")]
#[case::precision_above_255("NUMERIC(260, 2)", "1.5")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn unrepresentable_numeric_typmod_is_refused(#[case] column_type: &str, #[case] value: &str) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(&format!(
        "DROP TABLE IF EXISTS rv_typmod;
         CREATE TABLE rv_typmod (id BIGINT PRIMARY KEY, v {column_type});
         INSERT INTO rv_typmod VALUES (1, {value});"
    ))
    .await;
    let backend = Arc::new(backend);
    let err = try_mirror(&backend, &tmp, "rv_typmod_mc", "public.rv_typmod", &uri)
        .await
        .unwrap_err();
    assert!(
        err.contains("not supported") && err.contains("numeric("),
        "{err}"
    );
    wait_for_no_slots(&client).await;
}

/// Inheritance parents cannot be mirrored faithfully (children's changes are not replicated with
/// the parent): creating such a mirror is refused, and a mirror whose table gained a child is
/// errored at the next start rather than silently diverging.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn inheritance_parent_is_refused() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_parent, rv_later_parent CASCADE;
         CREATE TABLE rv_parent (id BIGINT, v TEXT);
         CREATE TABLE rv_child (extra INT) INHERITS (rv_parent);
         CREATE TABLE rv_later_parent (id BIGINT PRIMARY KEY);",
    )
    .await;
    let backend = Arc::new(backend);
    let err = try_mirror(&backend, &tmp, "rv_parent_mc", "public.rv_parent", &uri)
        .await
        .unwrap_err();
    assert!(err.contains("inheritance children"), "{err}");

    mirror(
        &backend,
        &tmp,
        "rv_later_parent_mc",
        "public.rv_later_parent",
        &uri,
    )
    .await;
    backend.shutdown_connection(&uri, false).await;
    drop(backend);
    client
        .simple_query("CREATE TABLE rv_later_child () INHERITS (rv_later_parent);")
        .await
        .unwrap();
    let backend = new_backend(&tmp).await.unwrap();
    let err = scan_error(&backend, "rv_later_parent_mc")
        .await
        .expect("must be errored after a child appeared");
    assert!(err.contains("inheritance children"), "{err}");
    client
        .simple_query("DROP TABLE rv_parent, rv_later_parent CASCADE;")
        .await
        .unwrap();
}

/// Dropping a source database that still has mirrors (after removing its slot, as postgres
/// requires) must not stop moonlink from starting; the orphaned mirror reports why and can be
/// dropped.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn startup_survives_a_dropped_source_database() {
    let admin_uri = get_database_uri();
    let (admin, _) = connect_to_postgres(&admin_uri).await;
    admin
        .simple_query("DROP DATABASE IF EXISTS rv_dropdb WITH (FORCE);")
        .await
        .unwrap();
    admin
        .simple_query("CREATE DATABASE rv_dropdb;")
        .await
        .unwrap();
    let uri = uri_for_database(&admin_uri, "rv_dropdb");
    let (client, _) = connect_to_postgres(&uri).await;
    client
        .simple_query("CREATE TABLE rv_t (id BIGINT PRIMARY KEY); INSERT INTO rv_t VALUES (1);")
        .await
        .unwrap();
    let tmp = TempDir::new().unwrap();
    let backend = new_backend(&tmp).await.unwrap();
    mirror(&backend, &tmp, "rv_t_mc", "public.rv_t", &uri).await;
    backend.shutdown_connection(&uri, false).await;
    drop(backend);
    drop(client);
    admin
        .simple_query(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
              WHERE database = 'rv_dropdb' AND active_pid IS NOT NULL;",
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    admin
        .simple_query(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
              WHERE database = 'rv_dropdb';",
        )
        .await
        .unwrap();
    admin
        .simple_query("DROP DATABASE rv_dropdb WITH (FORCE);")
        .await
        .unwrap();

    let backend = new_backend(&tmp).await.expect("startup must not fail");
    let err = scan_error(&backend, "rv_t_mc")
        .await
        .expect("must be errored");
    assert!(err.contains("could not be restored"), "{err}");
    backend
        .drop_table(DB.to_string(), "rv_t_mc".to_string())
        .await
        .unwrap();
    drop(backend);
    let backend = new_backend(&tmp).await.unwrap();
    assert!(scan_error(&backend, "rv_t_mc")
        .await
        .unwrap()
        .contains("not found"));
}

/// Array values survive when NULL arrays follow them in the same batch, on the initial copy and
/// via CDC.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn arrays_followed_by_null_keep_their_elements() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_arrays;
         CREATE TABLE rv_arrays (id BIGINT PRIMARY KEY, a INT[], t TEXT[]);
         INSERT INTO rv_arrays VALUES (1, '{1,2,3}', '{a,b}'), (2, NULL, NULL);",
    )
    .await;
    mirror(&backend, &tmp, "rv_arrays_mc", "public.rv_arrays", &uri).await;
    client
        .simple_query(
            "INSERT INTO rv_arrays VALUES (3, '{4,5}', '{c}'), (4, NULL, NULL), (5, '{}', '{}');",
        )
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let timeout = Duration::from_secs(30);
    let expected = |values: [Option<&str>; 5]| -> BTreeMap<i64, Option<String>> {
        (1..=5)
            .zip(values)
            .map(|(id, v)| (id, v.map(str::to_string)))
            .collect()
    };
    assert_eq!(
        scan_rows(&backend, "rv_arrays_mc", lsn, "a", timeout).await,
        Some(expected([
            Some("[1, 2, 3]"),
            None,
            Some("[4, 5]"),
            None,
            Some("[]")
        ]))
    );
    assert_eq!(
        scan_rows(&backend, "rv_arrays_mc", lsn, "t", timeout).await,
        Some(expected([
            Some("[a, b]"),
            None,
            Some("[c]"),
            None,
            Some("[]")
        ]))
    );
}

/// A table without a primary key (full-row identity) with array and composite columns: the
/// copied rows are indexed, so later updates and deletes of them apply.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn keyless_table_with_nested_columns_replicates() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_keyless;
         DROP TYPE IF EXISTS rv_pair;
         CREATE TYPE rv_pair AS (x INT, y TEXT);
         CREATE TABLE rv_keyless (id BIGINT, a INT[], c rv_pair);
         INSERT INTO rv_keyless VALUES (1, '{1,2}', ROW(1, 'x')), (2, '{3}', ROW(2, 'y')),
                                       (3, NULL, NULL), (4, '{}', ROW(NULL, NULL));",
    )
    .await;
    mirror(&backend, &tmp, "rv_keyless_mc", "public.rv_keyless", &uri).await;
    client
        .simple_query(
            "UPDATE rv_keyless SET id = 10 WHERE id = 1;
             DELETE FROM rv_keyless WHERE id IN (2, 4);
             INSERT INTO rv_keyless VALUES (5, '{5}', ROW(5, 'z'));
             DELETE FROM rv_keyless WHERE id = 5;",
        )
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let timeout = Duration::from_secs(30);
    let a = scan_rows(&backend, "rv_keyless_mc", lsn, "a", timeout)
        .await
        .unwrap();
    let c = scan_rows(&backend, "rv_keyless_mc", lsn, "c", timeout)
        .await
        .unwrap();
    assert_eq!(a.keys().copied().collect::<Vec<_>>(), vec![3, 10]);
    assert_eq!(a[&10].as_deref(), Some("[1, 2]"));
    assert_eq!(c[&10].as_deref(), Some("{x: 1, y: x}"));
    assert_eq!((a[&3].as_deref(), c[&3].as_deref()), (None, None));
    client
        .simple_query("DROP TABLE rv_keyless; DROP TYPE rv_pair;")
        .await
        .unwrap();
}

/// numeric NaN (also allowed in numeric(p,s)) and +/-Infinity have no decimal representation: a
/// mirror that receives one via CDC is errored and a mirror of a table holding one is refused, never NULL.
#[rstest::rstest]
#[case::nan("NUMERIC", "NaN", "NaN")]
#[case::constrained_nan("NUMERIC(10, 2)", "NaN", "NaN")]
#[case::infinity("NUMERIC", "Infinity", "Infinity")]
#[case::negative_infinity("NUMERIC", "-Infinity", "Infinity")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn numeric_special_values_error_the_mirror(
    #[case] column_type: &str,
    #[case] value: &str,
    #[case] reported: &str,
) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(&format!(
        "DROP TABLE IF EXISTS rv_special, rv_special_copy, rv_bystander;
         CREATE TABLE rv_special (id BIGINT PRIMARY KEY, v {column_type});
         CREATE TABLE rv_special_copy (id BIGINT PRIMARY KEY, v {column_type});
         INSERT INTO rv_special_copy VALUES (1, 1), (2, '{value}');
         CREATE TABLE rv_bystander (id BIGINT PRIMARY KEY);"
    ))
    .await;
    let backend = Arc::new(backend);
    mirror(&backend, &tmp, "rv_special_mc", "public.rv_special", &uri).await;
    mirror(
        &backend,
        &tmp,
        "rv_bystander_mc",
        "public.rv_bystander",
        &uri,
    )
    .await;
    client
        .simple_query(&format!(
            "INSERT INTO rv_special VALUES (1, 1), (2, '{value}');
             INSERT INTO rv_bystander VALUES (1);"
        ))
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![1])
    );
    let err = snapshot_error(&backend, "rv_special_mc", lsn)
        .await
        .expect("mirror must be errored");
    assert!(err.contains(reported), "{err}");

    let err = try_mirror(
        &backend,
        &tmp,
        "rv_special_copy_mc",
        "public.rv_special_copy",
        &uri,
    )
    .await
    .unwrap_err();
    assert!(err.contains(reported), "{err}");
}

/// Generated columns are not published by logical replication, so a table with one is refused
/// when the mirror is created (its definition would name a column the mirror never receives).
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn table_with_generated_column_is_refused() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_generated;
         CREATE TABLE rv_generated (id BIGINT PRIMARY KEY, a INT,
                                    twice INT GENERATED ALWAYS AS (a * 2) STORED);
         INSERT INTO rv_generated (id, a) VALUES (1, 1);",
    )
    .await;
    let backend = Arc::new(backend);
    let err = try_mirror(
        &backend,
        &tmp,
        "rv_generated_mc",
        "public.rv_generated",
        &uri,
    )
    .await
    .unwrap_err();
    assert!(err.contains("generated columns (\"twice\")"), "{err}");
    wait_for_no_slots(&client).await;
}

/// json keeps its text and jsonb its postgres form, on the copy and via CDC: key order,
/// duplicate keys, whitespace and numbers that do not fit a double survive.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn json_values_are_stored_as_postgres_prints_them() {
    let uri = get_database_uri();
    let values = r#"('{"b": 1,  "a": [1.10, 12345678901234567890123]}', '{"b": 1,  "a": [1.10, 12345678901234567890123]}'),
                    ('{"k": 1, "k": 2}', '{"k": 1, "k": 2}')"#;
    let (tmp, backend, client) = setup(&format!(
        "DROP TABLE IF EXISTS rv_json;
         CREATE TABLE rv_json (id BIGSERIAL PRIMARY KEY, j JSON, jb JSONB);
         INSERT INTO rv_json (j, jb) VALUES {values};"
    ))
    .await;
    mirror(&backend, &tmp, "rv_json_mc", "public.rv_json", &uri).await;
    client
        .simple_query(&format!("INSERT INTO rv_json (j, jb) VALUES {values};"))
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    for column in ["j", "jb"] {
        let source: BTreeMap<i64, Option<String>> = client
            .query(&format!("SELECT id, {column}::text FROM rv_json"), &[])
            .await
            .unwrap()
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        assert_eq!(source.len(), 4);
        assert_eq!(
            scan_rows(&backend, "rv_json_mc", lsn, column, Duration::from_secs(30)).await,
            Some(source),
            "{column}"
        );
    }
}

/// Elements of numeric(p,s) arrays keep their value, on the copy and via CDC.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn numeric_array_elements_keep_their_value() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_num_arr;
         CREATE TABLE rv_num_arr (id BIGINT PRIMARY KEY, a NUMERIC(5, 2)[], u NUMERIC[]);
         INSERT INTO rv_num_arr VALUES (1, '{1.50,-0.01,NULL}', '{1.5,42}');",
    )
    .await;
    mirror(&backend, &tmp, "rv_num_arr_mc", "public.rv_num_arr", &uri).await;
    client
        .simple_query("INSERT INTO rv_num_arr VALUES (2, '{999.99}', '{-0.0000000001}');")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let timeout = Duration::from_secs(30);
    let a = scan_rows(&backend, "rv_num_arr_mc", lsn, "a", timeout)
        .await
        .unwrap();
    let u = scan_rows(&backend, "rv_num_arr_mc", lsn, "u", timeout)
        .await
        .unwrap();
    assert_eq!(a[&1].as_deref(), Some("[1.5000000000, -0.0100000000, ]"));
    assert_eq!(a[&2].as_deref(), Some("[999.9900000000]"));
    assert_eq!(u[&1].as_deref(), Some("[1.5000000000, 42.0000000000]"));
    assert_eq!(u[&2].as_deref(), Some("[-0.0000000001]"));
}

/// character(n) loses only its space padding and "char" nothing; bytea keeps its bytes. Compared
/// with postgres's own text form, on the copy and via CDC.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn character_and_bytea_values_are_exact() {
    let uri = get_database_uri();
    let values = "('a\t', ' ', '\\x00ff'), ('ab', 'x', '\\x'), (' b', '\"', '\\x6162')";
    let (tmp, backend, client) = setup(&format!(
        "DROP TABLE IF EXISTS rv_chars;
         CREATE TABLE rv_chars (id BIGSERIAL PRIMARY KEY, c CHAR(4), ch \"char\", b BYTEA);
         INSERT INTO rv_chars (c, ch, b) VALUES {values};"
    ))
    .await;
    mirror(&backend, &tmp, "rv_chars_mc", "public.rv_chars", &uri).await;
    client
        .simple_query(&format!("INSERT INTO rv_chars (c, ch, b) VALUES {values};"))
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    for (column, source_text) in [
        ("c", "c::text"),
        ("ch", "ch::text"),
        ("b", "encode(b, 'hex')"),
    ] {
        let source: BTreeMap<i64, Option<String>> = client
            .query(&format!("SELECT id, {source_text} FROM rv_chars"), &[])
            .await
            .unwrap()
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        assert_eq!(source.len(), 6);
        assert_eq!(
            scan_rows(
                &backend,
                "rv_chars_mc",
                lsn,
                column,
                Duration::from_secs(30)
            )
            .await,
            Some(source),
            "{column}"
        );
    }
}

/// Arrays and composites whose element or field type cannot be converted are refused when the
/// mirror is created, even for an empty table, instead of erroring it at the first row.
#[rstest::rstest]
#[case::enum_array("rv_mood[]")]
#[case::interval_array("INTERVAL[]")]
#[case::composite_with_interval("rv_span")]
#[case::array_of_composite_with_interval("rv_span[]")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn nested_unsupported_types_are_refused(#[case] column_type: &str) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(&format!(
        "DROP TABLE IF EXISTS rv_nested;
         DROP TYPE IF EXISTS rv_mood, rv_span;
         CREATE TYPE rv_mood AS ENUM ('sad', 'happy');
         CREATE TYPE rv_span AS (n INT, d INTERVAL);
         CREATE TABLE rv_nested (id BIGINT PRIMARY KEY, v {column_type});"
    ))
    .await;
    let backend = Arc::new(backend);
    let err = try_mirror(&backend, &tmp, "rv_nested_mc", "public.rv_nested", &uri)
        .await
        .unwrap_err();
    assert!(err.contains("is not supported"), "{err}");
    wait_for_no_slots(&client).await;

    // Arrays of supported types (and of supported composites) are still accepted.
    client
        .simple_query(
            "DROP TABLE rv_nested; DROP TYPE rv_span; CREATE TYPE rv_span AS (n INT, t TEXT);
             CREATE TABLE rv_nested (id BIGINT PRIMARY KEY, a INT[], t TEXT[], s rv_span[]);",
        )
        .await
        .unwrap();
    mirror(&backend, &tmp, "rv_nested_mc", "public.rv_nested", &uri).await;
    backend
        .drop_table(DB.to_string(), "rv_nested_mc".to_string())
        .await
        .unwrap();
    client
        .simple_query("DROP TABLE rv_nested; DROP TYPE rv_mood, rv_span;")
        .await
        .unwrap();
}

async fn confirmed_flush(client: &Client) -> u64 {
    let row = client
        .query_one(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name LIKE 'moonlink_slot%' AND database = current_database()",
            &[],
        )
        .await
        .unwrap();
    row.get::<_, tokio_postgres::types::PgLsn>(0).into()
}

async fn wait_confirmed_at_least(client: &Client, lsn: u64) -> u64 {
    let mut c = 0;
    for _ in 0..40 {
        c = confirmed_flush(client).await;
        if c >= lsn {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    c
}

/// A mirror that fails recovery while the other mirrors of its database recover is errored for
/// good: the slot advances past its changes from then on, so a later start that could restore it
/// must not resume it as healthy with a gap.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn mirror_that_failed_recovery_is_not_resumed_with_a_gap() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_a, rv_b, rv_b_tmp;
         CREATE TABLE rv_a (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_b (id BIGINT PRIMARY KEY);",
    )
    .await;
    mirror(&backend, &tmp, "rv_a_mc", "public.rv_a", &uri).await;
    mirror(&backend, &tmp, "rv_b_mc", "public.rv_b", &uri).await;
    client
        .simple_query("INSERT INTO rv_a VALUES (1); INSERT INTO rv_b VALUES (1);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(scan_ids(&backend, "rv_b_mc", lsn).await, Some(vec![1]));
    assert_eq!(snapshot_error(&backend, "rv_a_mc", lsn).await, None);
    assert_eq!(snapshot_error(&backend, "rv_b_mc", lsn).await, None);
    backend.shutdown_connection(&uri, false).await;
    drop(backend);

    // Start 1: rv_b cannot be restored; rv_a keeps replicating and moves the slot on.
    client
        .simple_query("ALTER TABLE rv_b RENAME TO rv_b_tmp;")
        .await
        .unwrap();
    let backend = new_backend(&tmp).await.unwrap();
    assert!(scan_error(&backend, "rv_b_mc").await.is_some());
    client
        .simple_query("INSERT INTO rv_b_tmp VALUES (2); INSERT INTO rv_a VALUES (2);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(snapshot_error(&backend, "rv_a_mc", lsn).await, None);
    assert!(wait_confirmed_at_least(&client, lsn).await >= lsn);
    backend.shutdown_connection(&uri, false).await;
    drop(backend);

    // Start 2: the cause is gone, but rv_b missed row 2 for good.
    client
        .simple_query("ALTER TABLE rv_b_tmp RENAME TO rv_b;")
        .await
        .unwrap();
    let backend = new_backend(&tmp).await.unwrap();
    let err = scan_error(&backend, "rv_b_mc")
        .await
        .expect("must stay errored");
    assert!(
        err.contains("could not be restored") && err.contains("re-create"),
        "{err}"
    );
    backend
        .drop_table(DB.to_string(), "rv_b_mc".to_string())
        .await
        .unwrap();
    mirror(&backend, &tmp, "rv_b2_mc", "public.rv_b", &uri).await;
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(scan_ids(&backend, "rv_b2_mc", lsn).await, Some(vec![1, 2]));
}

/// Dropping an errored placeholder releases what it held on the source: its table leaves the
/// publication, and when it was the database's last mirror the slot and publication are dropped
/// (otherwise the source retains WAL forever).
#[rstest::rstest]
#[case::last_mirror(false)]
#[case::database_still_has_a_live_mirror(true)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn dropping_an_errored_placeholder_releases_the_source(#[case] keep_live_mirror: bool) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_t, rv_live;
         CREATE TABLE rv_t (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_live (id BIGINT PRIMARY KEY);",
    )
    .await;
    mirror(&backend, &tmp, "rv_t_mc", "public.rv_t", &uri).await;
    if keep_live_mirror {
        mirror(&backend, &tmp, "rv_live_mc", "public.rv_live", &uri).await;
    }
    client
        .simple_query("INSERT INTO rv_t VALUES (1); TRUNCATE rv_t; INSERT INTO rv_t VALUES (2);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert!(snapshot_error(&backend, "rv_t_mc", lsn).await.is_some());
    tokio::time::sleep(Duration::from_millis(500)).await;
    backend.shutdown_connection(&uri, false).await;
    drop(backend);

    let backend = new_backend(&tmp).await.unwrap();
    assert!(scan_error(&backend, "rv_t_mc").await.is_some());
    assert_eq!(slot_count(&client).await, 1);
    backend
        .drop_table(DB.to_string(), "rv_t_mc".to_string())
        .await
        .unwrap();
    let published: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_publication_tables WHERE pubname = 'moonlink_pub' AND tablename = 'rv_t'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(published, 0, "dropped mirror's table is still published");
    if keep_live_mirror {
        assert_eq!(slot_count(&client).await, 1);
        client
            .simple_query("INSERT INTO rv_live VALUES (1);")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;
        assert_eq!(scan_ids(&backend, "rv_live_mc", lsn).await, Some(vec![1]));
    } else {
        wait_for_no_slots(&client).await;
    }
}

/// Restart moonlink (keeping its data) with `ddl` run on the source while it is down.
async fn restart_with(
    backend: MoonlinkBackend,
    tmp: &TempDir,
    client: &Client,
    uri: &str,
    ddl: &str,
) -> MoonlinkBackend {
    backend.shutdown_connection(uri, false).await;
    drop(backend);
    client.simple_query(ddl).await.unwrap();
    new_backend(tmp).await.unwrap()
}

/// A change to the source table made while moonlink is stopped is caught at recovery: that
/// mirror is errored instead of resuming on a schema its stored data does not have (or bound to
/// a different table of the same name). Other mirrors are unaffected.
#[rstest::rstest]
#[case::rename_column("ALTER TABLE rv_down RENAME COLUMN a TO c;")]
#[case::retype_column("ALTER TABLE rv_down ALTER COLUMN a TYPE BIGINT;")]
#[case::add_column("ALTER TABLE rv_down ADD COLUMN c INT;")]
#[case::drop_and_readd_column(
    "ALTER TABLE rv_down DROP COLUMN a; ALTER TABLE rv_down ADD COLUMN a INT;"
)]
#[case::recreate_table("DROP TABLE rv_down; CREATE TABLE rv_down (id BIGINT PRIMARY KEY, a INT);")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn schema_change_while_stopped_errors_the_mirror(#[case] ddl: &str) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_down, rv_bystander;
         CREATE TABLE rv_down (id BIGINT PRIMARY KEY, a INT);
         CREATE TABLE rv_bystander (id BIGINT PRIMARY KEY);
         INSERT INTO rv_down VALUES (1, 1);",
    )
    .await;
    mirror(&backend, &tmp, "rv_down_mc", "public.rv_down", &uri).await;
    mirror(
        &backend,
        &tmp,
        "rv_bystander_mc",
        "public.rv_bystander",
        &uri,
    )
    .await;
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(snapshot_error(&backend, "rv_down_mc", lsn).await, None);
    let backend = restart_with(backend, &tmp, &client, &uri, ddl).await;
    let err = scan_error(&backend, "rv_down_mc")
        .await
        .expect("must be errored");
    assert!(
        err.contains("changed in a way its mirror cannot follow"),
        "{err}"
    );
    client
        .simple_query("INSERT INTO rv_bystander VALUES (1);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![1])
    );
}

/// Changes no Relation message shows (a column dropped and re-added with the same name and type,
/// a generated column added) error the mirror while replicating, before the table's next change
/// is applied.
#[rstest::rstest]
#[case::drop_and_readd_column(
    "ALTER TABLE rv_shape DROP COLUMN b; ALTER TABLE rv_shape ADD COLUMN b INT;"
)]
#[case::generated_column_added(
    "ALTER TABLE rv_shape ADD COLUMN g INT GENERATED ALWAYS AS (b * 2) STORED;"
)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn same_shape_schema_change_errors_the_mirror(#[case] ddl: &str) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_shape;
         CREATE TABLE rv_shape (id BIGINT PRIMARY KEY, b INT);
         INSERT INTO rv_shape VALUES (1, 10);",
    )
    .await;
    mirror(&backend, &tmp, "rv_shape_mc", "public.rv_shape", &uri).await;
    client
        .simple_query(&format!(
            "{ddl} INSERT INTO rv_shape (id, b) VALUES (2, 20);"
        ))
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let err = snapshot_error(&backend, "rv_shape_mc", lsn)
        .await
        .expect("must be errored");
    assert!(err.contains("source table changed"), "{err}");
}

/// Whether rows of a partition that joins or leaves the table were ever replicated cannot be told
/// from the catalog, so any partition change other than adding an empty partition errors that
/// mirror (never silently keeps or misses rows). Other mirrors keep replicating.
#[rstest::rstest]
#[case::attach_with_rows(
    "CREATE TABLE rv_part_2 (id BIGINT PRIMARY KEY, v TEXT);
     INSERT INTO rv_part_2 VALUES (150, 'x');
     ALTER TABLE rv_part ATTACH PARTITION rv_part_2 FOR VALUES FROM (100) TO (200);"
)]
#[case::create_partition_and_fill_at_once(
    "CREATE TABLE rv_part_2 PARTITION OF rv_part FOR VALUES FROM (100) TO (200);
     INSERT INTO rv_part VALUES (150, 'x');"
)]
#[case::detach_with_rows("ALTER TABLE rv_part DETACH PARTITION rv_part_1;")]
#[case::detach_then_delete(
    "ALTER TABLE rv_part DETACH PARTITION rv_part_1; DELETE FROM rv_part_1;"
)]
#[case::detach_then_truncate("ALTER TABLE rv_part DETACH PARTITION rv_part_1; TRUNCATE rv_part_1;")]
#[case::detach_empty("ALTER TABLE rv_part DETACH PARTITION rv_part_3;")]
#[case::detach_modify_reattach_in_one_transaction(
    "BEGIN; ALTER TABLE rv_part DETACH PARTITION rv_part_1; DELETE FROM rv_part_1;
     INSERT INTO rv_part_1 VALUES (5, 'while detached');
     ALTER TABLE rv_part ATTACH PARTITION rv_part_1 FOR VALUES FROM (0) TO (100); COMMIT;"
)]
#[case::detach_empty_reattach(
    "ALTER TABLE rv_part DETACH PARTITION rv_part_1; DELETE FROM rv_part_1;
     ALTER TABLE rv_part ATTACH PARTITION rv_part_1 FOR VALUES FROM (0) TO (100);"
)]
#[case::drop_partition("DROP TABLE rv_part_1;")]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn partition_change_errors_the_mirror(#[case] ddl: &str) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_part, rv_part_1, rv_part_2, rv_part_3, rv_part_4, rv_bystander;
         CREATE TABLE rv_part (id BIGINT PRIMARY KEY, v TEXT) PARTITION BY RANGE (id);
         CREATE TABLE rv_part_1 PARTITION OF rv_part FOR VALUES FROM (0) TO (100);
         CREATE TABLE rv_part_3 PARTITION OF rv_part FOR VALUES FROM (200) TO (300);
         CREATE TABLE rv_part_4 PARTITION OF rv_part FOR VALUES FROM (300) TO (400);
         CREATE TABLE rv_bystander (id BIGINT PRIMARY KEY);
         INSERT INTO rv_part VALUES (1, 'a');",
    )
    .await;
    mirror(&backend, &tmp, "rv_part_mc", "public.rv_part", &uri).await;
    mirror(
        &backend,
        &tmp,
        "rv_bystander_mc",
        "public.rv_bystander",
        &uri,
    )
    .await;
    client
        .simple_query(&format!(
            "{ddl} INSERT INTO rv_part VALUES (350, 'b'); INSERT INTO rv_bystander VALUES (1);"
        ))
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let err = snapshot_error(&backend, "rv_part_mc", lsn)
        .await
        .expect("must be errored");
    assert!(err.contains("source table changed"), "{err}");
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![1])
    );
    client
        .simple_query("DROP TABLE IF EXISTS rv_part, rv_part_1, rv_part_2, rv_part_3, rv_part_4;")
        .await
        .unwrap();
}

/// New partitions that are empty when first seen (created ahead of use, or attached empty) are
/// adopted with REPLICA IDENTITY FULL, so their rows, updates and deletes replicate exactly, also
/// after a restart.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn new_partitions_are_adopted() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_part, rv_part_2, rv_part_3;
         CREATE TABLE rv_part (id BIGINT PRIMARY KEY, v TEXT) PARTITION BY RANGE (id);
         CREATE TABLE rv_part_1 PARTITION OF rv_part FOR VALUES FROM (0) TO (100);
         INSERT INTO rv_part VALUES (1, 'a');",
    )
    .await;
    mirror(&backend, &tmp, "rv_part_mc", "public.rv_part", &uri).await;
    client
        .simple_query(
            "CREATE TABLE rv_part_2 PARTITION OF rv_part FOR VALUES FROM (100) TO (200);
             CREATE TABLE rv_part_3 (id BIGINT PRIMARY KEY, v TEXT);
             ALTER TABLE rv_part ATTACH PARTITION rv_part_3 FOR VALUES FROM (200) TO (300);",
        )
        .await
        .unwrap();
    // Adopted by the poll (or at the next change): REPLICA IDENTITY FULL on both.
    let mut full = 0i64;
    for _ in 0..40 {
        full = client
            .query_one(
                "SELECT count(*) FROM pg_class WHERE relname IN ('rv_part_2', 'rv_part_3') AND relreplident = 'f'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if full == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(full, 2, "new partitions did not get REPLICA IDENTITY FULL");
    client
        .simple_query(
            "INSERT INTO rv_part VALUES (150, 'b'), (250, 'c');
             UPDATE rv_part SET v = 'z' WHERE id IN (150, 250);",
        )
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(snapshot_error(&backend, "rv_part_mc", lsn).await, None);
    let expected = BTreeMap::from([
        (1, Some("a".to_string())),
        (150, Some("z".to_string())),
        (250, Some("z".to_string())),
    ]);
    assert_eq!(
        scan_rows(&backend, "rv_part_mc", lsn, "v", Duration::from_secs(30)).await,
        Some(expected.clone())
    );
    let backend = restart_with(backend, &tmp, &client, &uri, "SELECT 1;").await;
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_rows(&backend, "rv_part_mc", lsn, "v", Duration::from_secs(30)).await,
        Some(expected)
    );
    client.simple_query("DROP TABLE rv_part;").await.unwrap();
}

/// A partitioned table without a primary key is refused: partitions created later do not inherit
/// REPLICA IDENTITY FULL, and postgres would reject UPDATE and DELETE on them until it is set.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn keyless_partitioned_source_is_refused() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_part;
         CREATE TABLE rv_part (id BIGINT, v TEXT) PARTITION BY RANGE (id);
         CREATE TABLE rv_part_1 PARTITION OF rv_part FOR VALUES FROM (0) TO (100);",
    )
    .await;
    let backend = Arc::new(backend);
    let err = try_mirror(&backend, &tmp, "rv_part_mc", "public.rv_part", &uri)
        .await
        .unwrap_err();
    assert!(err.contains("add a primary key"), "{err}");
    wait_for_no_slots(&client).await;
}

/// A keyless mirror written by an older version stored some values differently (json,
/// character(n), unconstrained numeric, arrays); its rows are matched by value, so it is errored
/// at recovery with the re-create message. Mirrors with a primary key, or with only unchanged
/// value formats, keep replicating.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn keyless_mirror_from_an_older_version_is_errored() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_old_json, rv_old_plain, rv_old_keyed;
         CREATE TABLE rv_old_json (id BIGINT, j JSONB);
         CREATE TABLE rv_old_plain (id BIGINT, t TEXT);
         CREATE TABLE rv_old_keyed (id BIGINT PRIMARY KEY, j JSONB);
         INSERT INTO rv_old_json VALUES (1, '{}'); INSERT INTO rv_old_plain VALUES (1, 'x');
         INSERT INTO rv_old_keyed VALUES (1, '{}');",
    )
    .await;
    for t in ["rv_old_json", "rv_old_plain", "rv_old_keyed"] {
        mirror(
            &backend,
            &tmp,
            &format!("{t}_mc"),
            &format!("public.{t}"),
            &uri,
        )
        .await;
    }
    let lsn = current_wal_lsn(&client).await;
    for t in ["rv_old_json", "rv_old_plain", "rv_old_keyed"] {
        assert_eq!(
            snapshot_error(&backend, &format!("{t}_mc"), lsn).await,
            None
        );
    }
    backend.shutdown_connection(&uri, false).await;
    drop(backend);
    // What an older version leaves behind: no source fingerprints.
    std::fs::remove_dir_all(tmp.path().join("source_fingerprints")).unwrap();

    let backend = new_backend(&tmp).await.unwrap();
    let err = scan_error(&backend, "rv_old_json_mc")
        .await
        .expect("must be errored");
    assert!(
        err.contains("older moonlink version") && err.contains("re-create"),
        "{err}"
    );
    client
        .simple_query(
            "INSERT INTO rv_old_plain VALUES (2, 'y'); INSERT INTO rv_old_keyed VALUES (2, '{}');",
        )
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_old_plain_mc", lsn).await,
        Some(vec![1, 2])
    );
    assert_eq!(
        scan_ids(&backend, "rv_old_keyed_mc", lsn).await,
        Some(vec![1, 2])
    );
}

/// Commits on an empty source table between joining the publication and the copy's boundary are
/// in the copy and dropped; they must not become the table's commit LSN (nothing would ever be
/// readable at it), so the idle mirror stays readable at later LSNs.
#[rstest::rstest]
#[case::with_discarded_commits(true)]
#[case::control_no_writes(false)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn empty_copy_with_commits_at_the_boundary_stays_readable(#[case] write: bool) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_q, rv_bystander;
         CREATE TABLE rv_q (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_bystander (id BIGINT PRIMARY KEY);",
    )
    .await;
    mirror(
        &backend,
        &tmp,
        "rv_bystander_mc",
        "public.rv_bystander",
        &uri,
    )
    .await;
    // Keep a transaction with an xid open so creating the temporary copy slot has to wait.
    let (blocker, _) = connect_to_postgres(&uri).await;
    blocker
        .simple_query("BEGIN; SELECT txid_current();")
        .await
        .unwrap();

    let backend = Arc::new(backend);
    let b2 = backend.clone();
    let cfg = table_config(&tmp);
    let uri2 = uri.clone();
    let create = tokio::spawn(async move {
        b2.create_table(
            DB.to_string(),
            "rv_q_mc".to_string(),
            "public.rv_q".to_string(),
            uri2,
            cfg,
            None,
        )
        .await
    });
    let mut waiting = false;
    for _ in 0..100 {
        let n: i64 = client
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'moonlink_copy_%'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if n > 0 {
            waiting = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(waiting, "copy slot creation never started");
    if write {
        client
            .simple_query("INSERT INTO rv_q VALUES (1); DELETE FROM rv_q;")
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    blocker.simple_query("COMMIT;").await.unwrap();
    create.await.unwrap().unwrap();

    client
        .simple_query("INSERT INTO rv_bystander VALUES (1);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_bystander_mc", lsn).await,
        Some(vec![1])
    );
    let q = scan_rows(&backend, "rv_q_mc", lsn, "id", Duration::from_secs(20)).await;
    assert_eq!(
        q,
        Some(BTreeMap::new()),
        "scan of the idle empty mirror did not complete"
    );
    // Forcing a snapshot at a later LSN completes too (a dropped commit is not its commit LSN).
    assert_eq!(snapshot_error(&backend, "rv_q_mc", lsn).await, None);
}

/// While every mirror of a database is errored, nothing needs the WAL the stream has received:
/// the slot keeps advancing instead of retaining WAL until the mirrors are dropped.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn slot_advances_while_every_mirror_is_errored() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_t, rv_unmirrored;
         CREATE TABLE rv_t (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_unmirrored (id BIGINT);",
    )
    .await;
    mirror(&backend, &tmp, "rv_t_mc", "public.rv_t", &uri).await;
    client
        .simple_query("INSERT INTO rv_t VALUES (1); TRUNCATE rv_t; INSERT INTO rv_t VALUES (2);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert!(snapshot_error(&backend, "rv_t_mc", lsn).await.is_some());
    client
        .simple_query("INSERT INTO rv_unmirrored SELECT generate_series(1, 1000);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let confirmed = wait_confirmed_at_least(&client, lsn).await;
    assert!(confirmed >= lsn, "slot frozen at {confirmed} (< {lsn})");
}

/// A table is added through the database's shared connection, i.e. with another connection
/// string's role; a connection string whose own role cannot read the table must not mirror it.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn second_connection_string_needs_its_own_select_privilege() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_open, rv_secret;
         CREATE TABLE rv_open (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_secret (id BIGINT PRIMARY KEY);
         DO $$ BEGIN CREATE ROLE rv_reader LOGIN REPLICATION PASSWORD 'rv_reader';
         EXCEPTION WHEN duplicate_object THEN NULL; END $$;
         REVOKE ALL ON rv_secret FROM rv_reader;",
    )
    .await;
    let backend = Arc::new(backend);
    mirror(&backend, &tmp, "rv_open_mc", "public.rv_open", &uri).await;
    let reader_uri = uri_for_user(&uri, "rv_reader", "rv_reader");
    let err = try_mirror(
        &backend,
        &tmp,
        "rv_secret_mc",
        "public.rv_secret",
        &reader_uri,
    )
    .await
    .unwrap_err();
    assert!(err.contains("no SELECT privilege"), "{err}");
    client
        .simple_query("GRANT SELECT ON rv_secret TO rv_reader;")
        .await
        .unwrap();
    try_mirror(
        &backend,
        &tmp,
        "rv_secret_mc",
        "public.rv_secret",
        &reader_uri,
    )
    .await
    .unwrap();
    for dst in ["rv_open_mc", "rv_secret_mc"] {
        backend
            .drop_table(DB.to_string(), dst.to_string())
            .await
            .unwrap();
    }
    client
        .simple_query("DROP TABLE rv_open, rv_secret; DROP ROLE rv_reader;")
        .await
        .unwrap();
}

/// A partition attached by a transaction that got its xid before a concurrent load of the table
/// committed: no ordering of transaction ids tells whether those rows were published. A partition
/// holding rows when it is first seen errors the mirror, whatever the interleaving.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn partition_attached_after_a_concurrent_load_errors_the_mirror() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_part, rv_part_1, rv_part_2, rv_x;
         CREATE TABLE rv_part (id BIGINT PRIMARY KEY, v TEXT) PARTITION BY RANGE (id);
         CREATE TABLE rv_part_1 PARTITION OF rv_part FOR VALUES FROM (0) TO (100);
         CREATE TABLE rv_part_2 (id BIGINT PRIMARY KEY, v TEXT);
         CREATE TABLE rv_x (id BIGINT PRIMARY KEY);",
    )
    .await;
    mirror(&backend, &tmp, "rv_part_mc", "public.rv_part", &uri).await;
    let (loader, _) = connect_to_postgres(&uri).await;
    client
        .simple_query("BEGIN; INSERT INTO rv_x VALUES (1);")
        .await
        .unwrap();
    loader
        .simple_query("INSERT INTO rv_part_2 VALUES (150, 'never published');")
        .await
        .unwrap();
    client
        .simple_query(
            "ALTER TABLE rv_part ATTACH PARTITION rv_part_2 FOR VALUES FROM (100) TO (200); COMMIT;",
        )
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO rv_part VALUES (2, 'b');")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let err = snapshot_error(&backend, "rv_part_mc", lsn)
        .await
        .expect("must be errored");
    assert!(err.contains("already held rows"), "{err}");
    client
        .simple_query("DROP TABLE rv_part, rv_x;")
        .await
        .unwrap();
}

/// The command that stops and starts the test Postgres (`$MOONLINK_TEST_PG_CTL stop|start`, for
/// example a script around pg_ctl); tests that need it are skipped without one.
fn postgres_control() -> Option<String> {
    std::env::var("MOONLINK_TEST_PG_CTL")
        .ok()
        .filter(|command| !command.is_empty())
}

/// Postgres stopped through `MOONLINK_TEST_PG_CTL`, started again when dropped (also when the
/// test fails before starting it), so that later tests find it running.
struct StoppedPostgres {
    control: String,
    started: bool,
}

impl StoppedPostgres {
    fn stop(control: &str) -> Self {
        local_postgres(control, "stop");
        Self {
            control: control.to_string(),
            started: false,
        }
    }

    fn start(mut self) {
        self.started = true;
        local_postgres(&self.control, "start");
    }
}

impl Drop for StoppedPostgres {
    fn drop(&mut self) {
        if !self.started {
            let _ = std::process::Command::new(&self.control)
                .arg("start")
                .status();
        }
    }
}

fn local_postgres(control: &str, action: &str) {
    let status = std::process::Command::new(control)
        .arg(action)
        .status()
        .unwrap_or_else(|e| panic!("MOONLINK_TEST_PG_CTL {control} {action}: {e}"));
    assert!(
        status.success(),
        "MOONLINK_TEST_PG_CTL {control} {action}: {status}"
    );
}

/// A source that cannot be reached at startup (restarting, refusing connections) is a transient
/// failure: its mirrors are retried, never persisted as errored, and are restored with every
/// change once the source is back, in the same run and after a restart.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn mirror_is_restored_once_an_unreachable_source_is_back() {
    let Some(control) = postgres_control() else {
        eprintln!("skipped: set MOONLINK_TEST_PG_CTL to a command taking stop|start for the test Postgres");
        return;
    };
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_down;
         CREATE TABLE rv_down (id BIGINT PRIMARY KEY);
         INSERT INTO rv_down VALUES (1);",
    )
    .await;
    mirror(&backend, &tmp, "rv_down_mc", "public.rv_down", &uri).await;
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(snapshot_error(&backend, "rv_down_mc", lsn).await, None);
    backend.shutdown_connection(&uri, false).await;
    drop(backend);
    drop(client);

    let stopped = StoppedPostgres::stop(&control);
    let backend = new_backend(&tmp).await.expect("startup must not fail");
    let err = scan_error(&backend, "rv_down_mc")
        .await
        .expect("not available while the source is down");
    assert!(err.contains("not available yet"), "{err}");
    // Not persisted: a restart while the source is still down retries again.
    drop(backend);
    let backend = new_backend(&tmp).await.unwrap();
    stopped.start();

    let (client, _) = connect_to_postgres(&uri).await;
    client
        .simple_query("INSERT INTO rv_down VALUES (2);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let mut rows = None;
    for _ in 0..90 {
        if scan_error(&backend, "rv_down_mc").await.is_none() {
            rows = scan_ids(&backend, "rv_down_mc", lsn).await;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert_eq!(rows, Some(vec![1, 2]), "mirror was not restored");
    assert_eq!(snapshot_error(&backend, "rv_down_mc", lsn).await, None);
}

const LATE_OTHER_DB: &str = "cdc_regressions_late_other";

/// At startup mirrors register one at a time. No slot may advance before every mirror is
/// restored or pending: if the first mirror of a database is errored on replay (its source was
/// truncated while moonlink was down), a mirror of that database registered later (here delayed
/// by a slow recovery of a mirror of another database) must still get every change made while
/// moonlink was down. Recovery order is by mirror name (rv_s1_a, rv_s2_c, rv_s3_b).
#[rstest::rstest]
#[case::first_mirror_errors_on_replay(true)]
#[case::control_first_mirror_healthy(false)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn mirror_registered_late_at_startup_keeps_changes_made_while_down(#[case] truncate_a: bool) {
    let uri = get_database_uri();
    let (admin, _) = connect_to_postgres(&uri).await;
    admin
        .simple_query(&format!(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = '{LATE_OTHER_DB}';"
        ))
        .await
        .unwrap();
    admin
        .simple_query(&format!(
            "DROP DATABASE IF EXISTS {LATE_OTHER_DB} WITH (FORCE);"
        ))
        .await
        .unwrap();
    admin
        .simple_query(&format!("CREATE DATABASE {LATE_OTHER_DB};"))
        .await
        .unwrap();
    let other_uri = uri_for_database(&uri, LATE_OTHER_DB);
    {
        let (other, _) = connect_to_postgres(&other_uri).await;
        other
            .simple_query("CREATE TABLE rv_c (id BIGINT PRIMARY KEY); INSERT INTO rv_c VALUES (1);")
            .await
            .unwrap();
    }
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_a, rv_b;
         CREATE TABLE rv_a (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_b (id BIGINT PRIMARY KEY);
         INSERT INTO rv_a VALUES (1); INSERT INTO rv_b VALUES (1);",
    )
    .await;
    mirror(&backend, &tmp, "rv_s1_a", "public.rv_a", &uri).await;
    mirror(&backend, &tmp, "rv_s2_c", "public.rv_c", &other_uri).await;
    mirror(&backend, &tmp, "rv_s3_b", "public.rv_b", &uri).await;
    let lsn = current_wal_lsn(&client).await;
    for t in ["rv_s1_a", "rv_s3_b"] {
        assert_eq!(scan_ids(&backend, t, lsn).await, Some(vec![1]));
        assert_eq!(snapshot_error(&backend, t, lsn).await, None);
    }
    backend.shutdown_connection(&uri, false).await;
    backend.shutdown_connection(&other_uri, false).await;
    drop(backend);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // While moonlink is down.
    if truncate_a {
        client.simple_query("TRUNCATE rv_a;").await.unwrap();
    }
    client
        .simple_query("INSERT INTO rv_b VALUES (2);")
        .await
        .unwrap();

    // Hold new connections to the other database for longer than one status interval.
    let (locker, _) = connect_to_postgres(&uri).await;
    locker
        .simple_query(&format!(
            "BEGIN; ALTER DATABASE {LATE_OTHER_DB} RENAME TO {LATE_OTHER_DB}_held;"
        ))
        .await
        .unwrap();
    let base = tmp.path().to_str().unwrap().to_string();
    let startup = tokio::spawn(async move {
        let store = SqliteMetadataStore::new_with_directory(&base)
            .await
            .unwrap();
        MoonlinkBackend::new(base, None, Box::new(store))
            .await
            .map_err(|e| error_text(&e))
    });
    tokio::time::sleep(Duration::from_secs(25)).await;
    let confirmed_while_held = confirmed_flush(&client).await;
    locker.simple_query("ROLLBACK;").await.unwrap();
    let backend = startup.await.unwrap().unwrap();

    client
        .simple_query("INSERT INTO rv_b VALUES (3);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let rows = scan_ids(&backend, "rv_s3_b", lsn).await;
    eprintln!(
        "truncate_a={truncate_a} confirmed_while_held={confirmed_while_held} rv_s3_b={rows:?}"
    );
    assert_eq!(
        rows,
        Some(vec![1, 2, 3]),
        "mirror registered after an all-errored window lost the change made while moonlink was down"
    );
    for t in ["rv_s1_a", "rv_s2_c", "rv_s3_b"] {
        let _ = backend.drop_table(DB.to_string(), t.to_string()).await;
    }
}

async fn count_copy_slots(client: &Client) -> i64 {
    client
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'moonlink_copy_%'",
            &[],
        )
        .await
        .unwrap()
        .get(0)
}

/// The documented remedy for a schema change is drop + re-create. The re-created mirror
/// must stay healthy across a restart, although the slot (held back by another, idle mirror of
/// the database) still replays the WAL from before the ALTER.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn recreated_mirror_survives_replay_of_an_older_schema_change() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_sc, rv_idle;
         CREATE TABLE rv_sc (id BIGINT PRIMARY KEY, a INT);
         CREATE TABLE rv_idle (id BIGINT PRIMARY KEY);
         INSERT INTO rv_idle VALUES (1);",
    )
    .await;
    mirror(&backend, &tmp, "rv_idle_mc", "public.rv_idle", &uri).await;
    mirror(&backend, &tmp, "rv_sc_mc", "public.rv_sc", &uri).await;
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(snapshot_error(&backend, "rv_idle_mc", lsn).await, None);
    client
        .simple_query("INSERT INTO rv_sc VALUES (1, 1); ALTER TABLE rv_sc ADD COLUMN b INT; INSERT INTO rv_sc VALUES (2, 2, 2);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert!(snapshot_error(&backend, "rv_sc_mc", lsn).await.is_some());
    backend
        .drop_table(DB.to_string(), "rv_sc_mc".to_string())
        .await
        .unwrap();
    mirror(&backend, &tmp, "rv_sc_mc", "public.rv_sc", &uri).await;
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(snapshot_error(&backend, "rv_sc_mc", lsn).await, None);
    assert_eq!(scan_ids(&backend, "rv_sc_mc", lsn).await, Some(vec![1, 2]));
    let confirmed = confirmed_flush(&client).await;
    eprintln!("rv9 slot confirmed {confirmed} vs current {lsn} before restart");
    backend.shutdown_connection(&uri, false).await;
    drop(backend);

    let backend = new_backend(&tmp).await.unwrap();
    client
        .simple_query("INSERT INTO rv_sc VALUES (3, 3, 3);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let err = snapshot_error(&backend, "rv_sc_mc", lsn).await;
    eprintln!("rv9 re-created mirror after restart: {err:?}");
    assert_eq!(
        err, None,
        "re-created mirror errored by replay of the old schema change"
    );
}

/// An empty copy with the main stream lagging behind the copy boundary when the copy finishes (a
/// large transaction of another mirror precedes the commits in WAL). Commits that the boundary
/// drops then reach the sink after it stopped holding the table's commit LSN back.
#[rstest::rstest]
#[case::lagging_stream(1_000_000)]
#[case::control_no_lag(0)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn empty_copy_with_a_lagging_stream_stays_readable(#[case] lag_rows: i64) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_lag_q, rv_lag_big;
         CREATE TABLE rv_lag_q (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_lag_big (id BIGINT PRIMARY KEY);",
    )
    .await;
    mirror(&backend, &tmp, "rv_lag_big_mc", "public.rv_lag_big", &uri).await;
    let (blocker, _) = connect_to_postgres(&uri).await;
    blocker
        .simple_query("BEGIN; SELECT txid_current();")
        .await
        .unwrap();

    let backend = Arc::new(backend);
    let b2 = backend.clone();
    let cfg = table_config(&tmp);
    let uri2 = uri.clone();
    let create = tokio::spawn(async move {
        b2.create_table(
            DB.to_string(),
            "rv_lag_q_mc".to_string(),
            "public.rv_lag_q".to_string(),
            uri2,
            cfg,
            None,
        )
        .await
    });
    let mut waiting = false;
    for _ in 0..100 {
        if count_copy_slots(&client).await > 0 {
            waiting = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(waiting, "copy slot creation never started");
    if lag_rows > 0 {
        client
            .simple_query(&format!(
                "INSERT INTO rv_lag_big SELECT g FROM generate_series(1, {lag_rows}) g;"
            ))
            .await
            .unwrap();
    }
    client
        .simple_query("INSERT INTO rv_lag_q VALUES (1); DELETE FROM rv_lag_q;")
        .await
        .unwrap();
    blocker.simple_query("COMMIT;").await.unwrap();
    create.await.unwrap().unwrap();

    let lsn = current_wal_lsn(&client).await;
    let big = scan_rows(
        &backend,
        "rv_lag_big_mc",
        lsn,
        "id",
        Duration::from_secs(300),
    )
    .await;
    assert_eq!(big.map(|m| m.len() as i64), Some(lag_rows));
    let q = scan_rows(&backend, "rv_lag_q_mc", lsn, "id", Duration::from_secs(20)).await;
    assert_eq!(
        q,
        Some(BTreeMap::new()),
        "scan of the idle empty mirror did not complete"
    );
}

/// A mirror errored by a TRUNCATE is replaced the documented way: creating the replacement
/// while the errored mirror still exists is refused ("drop that mirror first"); after dropping
/// it, the new mirror holds exactly the source's rows, is not errored by the slot replaying the
/// old TRUNCATE (whether or not the slot already moved past it), keeps the table published and
/// replicates on.
#[rstest::rstest]
#[case::slot_still_before_truncate(false)]
#[case::slot_advanced_past_truncate(true)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn recreating_an_errored_mirror_requires_dropping_it_first(#[case] advance_slot: bool) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_dup, rv_keep;
         CREATE TABLE rv_dup (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_keep (id BIGINT PRIMARY KEY);",
    )
    .await;
    mirror(&backend, &tmp, "rv_dup_old", "public.rv_dup", &uri).await;
    // Another, idle mirror of the database keeps the slot behind when not advancing it.
    mirror(&backend, &tmp, "rv_keep_mc", "public.rv_keep", &uri).await;
    client
        .simple_query(
            "INSERT INTO rv_dup VALUES (1); TRUNCATE rv_dup; INSERT INTO rv_dup VALUES (2);",
        )
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert!(snapshot_error(&backend, "rv_dup_old", lsn).await.is_some());
    if advance_slot {
        client
            .simple_query("INSERT INTO rv_keep VALUES (1);")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;
        assert_eq!(snapshot_error(&backend, "rv_keep_mc", lsn).await, None);
        assert!(wait_confirmed_at_least(&client, lsn).await >= lsn);
    }
    backend.shutdown_connection(&uri, false).await;
    drop(backend);

    let backend = Arc::new(new_backend(&tmp).await.unwrap());
    assert!(scan_error(&backend, "rv_dup_old").await.is_some());
    let err = try_mirror(&backend, &tmp, "rv_dup_new", "public.rv_dup", &uri)
        .await
        .unwrap_err();
    assert!(err.contains("drop that mirror first"), "{err}");
    backend
        .drop_table(DB.to_string(), "rv_dup_old".to_string())
        .await
        .unwrap();
    mirror(&backend, &tmp, "rv_dup_new", "public.rv_dup", &uri).await;
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(snapshot_error(&backend, "rv_dup_new", lsn).await, None);
    assert_eq!(scan_ids(&backend, "rv_dup_new", lsn).await, Some(vec![2]));
    client
        .simple_query("INSERT INTO rv_dup VALUES (3);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let published: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_publication_tables WHERE pubname = 'moonlink_pub' AND tablename = 'rv_dup'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(published, 1);
    assert_eq!(
        scan_ids(&backend, "rv_dup_new", lsn).await,
        Some(vec![2, 3])
    );
}

/// Make every transaction larger than 64 kB of changes stream in progress (or reset that).
async fn stream_large_transactions(client: &Client, on: bool) {
    let set = if on {
        "ALTER SYSTEM SET logical_decoding_work_mem = '64kB';"
    } else {
        "ALTER SYSTEM RESET logical_decoding_work_mem;"
    };
    client.simple_query(set).await.unwrap();
    client
        .simple_query("SELECT pg_reload_conf();")
        .await
        .unwrap();
}

/// A failing change inside a streamed (large) transaction errors that mirror at the commit, like
/// in any other transaction: an unconvertible value, or the classic reload BEGIN; TRUNCATE;
/// INSERT ...; COMMIT, also inside a savepoint or a PL/pgSQL exception block. The other table of the transaction gets exactly its rows.
#[rstest::rstest]
#[case::bad_value(
    "INSERT INTO rv_st VALUES (10, '2024-01-01'), (11, 'infinity'), (12, '2024-01-02');",
    "could not be applied"
)]
#[case::truncate_and_reload(
    "INSERT INTO rv_st VALUES (10, '2024-01-01'); TRUNCATE rv_st; INSERT INTO rv_st VALUES (20, '2024-01-02');",
    "truncated"
)]
#[case::truncate_and_reload_in_a_savepoint(
    "SAVEPOINT r; TRUNCATE rv_st; INSERT INTO rv_st VALUES (7, '2024-01-07'); RELEASE SAVEPOINT r;",
    "truncated"
)]
#[case::truncate_and_reload_in_an_exception_block(
    "DO $$ BEGIN BEGIN TRUNCATE rv_st; INSERT INTO rv_st VALUES (7, '2024-01-07'); EXCEPTION WHEN others THEN NULL; END; END $$;",
    "truncated"
)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn failure_in_a_streamed_transaction_errors_the_mirror(
    #[case] changes: &str,
    #[case] reason: &str,
) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_st, rv_fill;
         CREATE TABLE rv_st (id BIGINT PRIMARY KEY, ts TIMESTAMP);
         CREATE TABLE rv_fill (id BIGINT PRIMARY KEY, v TEXT);
         INSERT INTO rv_st VALUES (1, '2023-01-01'), (2, '2023-01-02');",
    )
    .await;
    mirror(&backend, &tmp, "rv_st_mc", "public.rv_st", &uri).await;
    mirror(&backend, &tmp, "rv_fill_mc", "public.rv_fill", &uri).await;
    let streamed_before = stream_txns(&client).await;
    stream_large_transactions(&client, true).await;
    run_large_transaction(
        &client,
        &format!(
            "BEGIN;
             INSERT INTO rv_fill SELECT i, repeat('f', 500) FROM generate_series(1, 400) i;
             {changes}
             INSERT INTO rv_fill SELECT i, repeat('g', 500) FROM generate_series(401, 800) i;
             UPDATE rv_fill SET v = 'u' WHERE id % 7 = 0;"
        ),
        "COMMIT",
        true,
        streamed_before,
    )
    .await;
    let lsn = current_wal_lsn(&client).await;
    let err = snapshot_error(&backend, "rv_st_mc", lsn)
        .await
        .expect("mirror must be errored");
    assert!(err.contains(reason), "{err}");
    let source: BTreeMap<i64, Option<String>> = client
        .query("SELECT id, v FROM rv_fill", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        scan_rows(&backend, "rv_fill_mc", lsn, "v", Duration::from_secs(30)).await,
        Some(source)
    );
    stop_streaming_large_transactions(&client, true, streamed_before).await;
}

/// A new partition is judged without reading it: a partition the mirror's role cannot read (or
/// whose rows row-level security hides) is neither adopted as empty nor ignored.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn partition_the_mirror_role_cannot_read_is_judged_by_size() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_part, rv_part_1, rv_part_2;
         DO $$ BEGIN
           IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rv_repl') THEN
             EXECUTE 'DROP OWNED BY rv_repl'; EXECUTE 'DROP ROLE rv_repl';
           END IF;
         END $$;
         CREATE ROLE rv_repl LOGIN REPLICATION PASSWORD 'rv';
         DO $$ BEGIN EXECUTE format('GRANT CREATE ON DATABASE %I TO rv_repl', current_database()); END $$;
         GRANT CREATE ON SCHEMA public TO rv_repl;
         SET ROLE rv_repl;
         CREATE TABLE rv_part (id BIGINT PRIMARY KEY, v TEXT) PARTITION BY RANGE (id);
         CREATE TABLE rv_part_1 PARTITION OF rv_part FOR VALUES FROM (0) TO (100);
         INSERT INTO rv_part VALUES (1, 'a');
         RESET ROLE;",
    )
    .await;
    let repl_uri = uri_for_user(&uri, "rv_repl", "rv");
    mirror(&backend, &tmp, "rv_part_mc", "public.rv_part", &repl_uri).await;
    client
        .simple_query(
            "CREATE TABLE rv_part_2 (id BIGINT PRIMARY KEY, v TEXT);
             INSERT INTO rv_part_2 VALUES (150, 'never published');
             ALTER TABLE rv_part ATTACH PARTITION rv_part_2 FOR VALUES FROM (100) TO (200);
             INSERT INTO rv_part VALUES (2, 'b');",
        )
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let err = snapshot_error(&backend, "rv_part_mc", lsn)
        .await
        .expect("must be errored");
    assert!(err.contains("already held rows"), "{err}");
    backend
        .drop_table(DB.to_string(), "rv_part_mc".to_string())
        .await
        .unwrap();
    client
        .simple_query("DROP TABLE rv_part; DROP OWNED BY rv_repl; DROP ROLE rv_repl;")
        .await
        .unwrap();
}

/// A mirror created on a fresh connection whose slot lags behind its table's history (held for a
/// mirror of the database that cannot be restored yet): the loop starts streaming that history
/// right after the new table registers. The new mirror must already be in copy mode then, so the
/// replay (INSERT 9; TRUNCATE; INSERT 2) is judged against the copy's boundary instead of being
/// applied on top of the copy.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn mirror_created_on_a_lagging_slot_does_not_apply_replayed_history() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_re, rv_held;
         DO $$ BEGIN
           IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rv_role2') THEN
             EXECUTE 'DROP OWNED BY rv_role2'; EXECUTE 'DROP ROLE rv_role2';
           END IF;
         END $$;
         CREATE ROLE rv_role2 LOGIN REPLICATION PASSWORD 'rv2';
         CREATE TABLE rv_re (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_held (id BIGINT PRIMARY KEY);
         GRANT SELECT ON rv_held TO rv_role2;",
    )
    .await;
    let other_uri = uri_for_user(&uri, "rv_role2", "rv2");
    mirror(&backend, &tmp, "rv_re_mc", "public.rv_re", &uri).await;
    mirror(&backend, &tmp, "rv_held_mc", "public.rv_held", &other_uri).await;
    client
        .simple_query("INSERT INTO rv_re VALUES (9); TRUNCATE rv_re; INSERT INTO rv_re VALUES (2);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert!(snapshot_error(&backend, "rv_re_mc", lsn).await.is_some());
    backend
        .drop_table(DB.to_string(), "rv_re_mc".to_string())
        .await
        .unwrap();
    backend.shutdown_connection(&uri, false).await;
    drop(backend);

    // rv_held cannot be restored for now (its role cannot log in): pending, its slot held.
    client
        .simple_query("ALTER ROLE rv_role2 NOLOGIN;")
        .await
        .unwrap();
    let backend = new_backend(&tmp).await.unwrap();
    let err = scan_error(&backend, "rv_held_mc").await.unwrap();
    assert!(err.contains("not available yet"), "{err}");
    mirror(&backend, &tmp, "rv_re_new", "public.rv_re", &uri).await;
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(snapshot_error(&backend, "rv_re_new", lsn).await, None);
    assert_eq!(scan_ids(&backend, "rv_re_new", lsn).await, Some(vec![2]));
    for dst in ["rv_re_new", "rv_held_mc"] {
        backend
            .drop_table(DB.to_string(), dst.to_string())
            .await
            .unwrap();
    }
    client
        .simple_query("DROP TABLE rv_re, rv_held; DROP OWNED BY rv_role2; DROP ROLE rv_role2;")
        .await
        .unwrap();
}

/// A streamed transaction (larger than logical_decoding_work_mem) committed inside an empty
/// copy's boundary is dropped; it must not become the table's consistent view either, or
/// create_snapshot of the idle mirror never completes.
#[rstest::rstest]
#[case::streamed(true)]
#[case::control_not_streamed(false)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn empty_copy_with_a_streamed_boundary_commit_can_snapshot(#[case] streamed: bool) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_st, rv_st_by;
         CREATE TABLE rv_st (id BIGINT PRIMARY KEY, pad TEXT);
         CREATE TABLE rv_st_by (id BIGINT PRIMARY KEY);",
    )
    .await;
    let work_mem = if streamed { "'64kB'" } else { "DEFAULT" };
    // ALTER SYSTEM cannot run in the implicit transaction of a multi-statement query.
    client
        .simple_query(&format!(
            "ALTER SYSTEM SET logical_decoding_work_mem = {work_mem};"
        ))
        .await
        .unwrap();
    client
        .simple_query("SELECT pg_reload_conf();")
        .await
        .unwrap();
    mirror(&backend, &tmp, "rv_st_by_mc", "public.rv_st_by", &uri).await;
    let (blocker, _) = connect_to_postgres(&uri).await;
    blocker
        .simple_query("BEGIN; SELECT txid_current();")
        .await
        .unwrap();

    let backend = Arc::new(backend);
    let b2 = backend.clone();
    let cfg = table_config(&tmp);
    let uri2 = uri.clone();
    let create = tokio::spawn(async move {
        b2.create_table(
            DB.to_string(),
            "rv_st_mc".to_string(),
            "public.rv_st".to_string(),
            uri2,
            cfg,
            None,
        )
        .await
    });
    let mut waiting = false;
    for _ in 0..100 {
        if count_copy_slots(&client).await > 0 {
            waiting = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(waiting, "copy slot creation never started");
    let streamed_before = stream_txns(&client).await;
    client
        .simple_query(
            "BEGIN; INSERT INTO rv_st SELECT g, repeat('x', 100) FROM generate_series(1, 5000) g;
             DELETE FROM rv_st; COMMIT;",
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    blocker.simple_query("COMMIT;").await.unwrap();
    create.await.unwrap().unwrap();

    client
        .simple_query("INSERT INTO rv_st_by VALUES (1);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(scan_ids(&backend, "rv_st_by_mc", lsn).await, Some(vec![1]));
    let streamed_after = if streamed {
        wait_until_streamed(&client, streamed_before).await
    } else {
        stream_txns(&client).await
    };
    client
        .simple_query("ALTER SYSTEM RESET logical_decoding_work_mem;")
        .await
        .unwrap();
    client
        .simple_query("SELECT pg_reload_conf();")
        .await
        .unwrap();
    eprintln!("rv11 streamed={streamed} stream_txns {streamed_before} -> {streamed_after}");
    if streamed {
        assert!(
            streamed_after > streamed_before,
            "the transaction was not streamed"
        );
    }
    let scan = scan_rows(&backend, "rv_st_mc", lsn, "id", Duration::from_secs(20)).await;
    assert_eq!(
        scan,
        Some(BTreeMap::new()),
        "scan of the idle empty mirror did not complete"
    );
    let snapshot = tokio::time::timeout(
        Duration::from_secs(30),
        backend.create_snapshot(DB.to_string(), "rv_st_mc".to_string(), lsn),
    )
    .await;
    assert!(
        matches!(snapshot, Ok(Ok(_))),
        "create_snapshot of the idle empty mirror did not complete: {snapshot:?}"
    );
}

/// Undo [`stream_large_transactions`] once the mirrors have caught up: the walsender decodes a
/// transaction after its commit and rereads the setting meanwhile, so resetting it earlier can
/// stop the rest of the transaction from being streamed. With `streamed`, check that it was.
async fn stop_streaming_large_transactions(client: &Client, streamed: bool, streamed_before: i64) {
    let streamed_after = if streamed {
        wait_until_streamed(client, streamed_before).await
    } else {
        stream_txns(client).await
    };
    stream_large_transactions(client, false).await;
    if streamed {
        assert!(
            streamed_after > streamed_before,
            "the large transaction was not streamed"
        );
    }
}

/// Run a large transaction: `body` (starting with BEGIN) on a connection of its own, then `end`
/// (COMMIT or ROLLBACK). With `streamed`, the transaction is ended only once postgres has
/// streamed it, failing if it does not. Decoding can lag the client by seconds, and postgres 18
/// discards a large transaction that has already aborted when decoding reaches it instead of
/// streaming it.
async fn run_large_transaction(
    client: &Client,
    body: &str,
    end: &str,
    streamed: bool,
    streamed_before: i64,
) {
    let (writer, _connection) = connect_to_postgres(&get_database_uri()).await;
    writer.simple_query(body).await.unwrap();
    if streamed {
        let streamed_after = wait_until_streamed(client, streamed_before).await;
        assert!(
            streamed_after > streamed_before,
            "the large transaction was not streamed before its {end}"
        );
    }
    writer.simple_query(&format!("{end};")).await.unwrap();
}

/// Wait up to 30 s for postgres to stream a transaction after `streamed_before` streamed ones (slot
/// statistics are reported about once a second); returns the number streamed.
async fn wait_until_streamed(client: &Client, streamed_before: i64) -> i64 {
    let mut streamed = stream_txns(client).await;
    for _ in 0..150 {
        if streamed > streamed_before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        streamed = stream_txns(client).await;
    }
    streamed
}

async fn stream_txns(client: &Client) -> i64 {
    client
        .query_one(
            "SELECT coalesce(sum(stream_txns), 0)::bigint FROM pg_stat_replication_slots WHERE slot_name LIKE 'moonlink_slot%'",
            &[],
        )
        .await
        .unwrap()
        .get(0)
}

/// Dropping the last other mirror of a database, errored placeholder or live, never drops the slot
/// while a mirror of that database is pending, whatever connection string it uses (here: another
/// role that cannot log in for now): once restored, it replays everything written while pending.
#[rstest::rstest]
#[case::errored_placeholder(true)]
#[case::live_mirror(false)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn dropping_the_last_other_mirror_keeps_the_slot_of_a_pending_mirror(
    #[case] errored_first: bool,
) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_pa, rv_pb;
         DO $$ BEGIN
           IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rv_role2') THEN
             EXECUTE 'DROP OWNED BY rv_role2'; DROP ROLE rv_role2;
           END IF;
         END $$;
         CREATE ROLE rv_role2 LOGIN SUPERUSER PASSWORD 'pw2';
         CREATE TABLE rv_pa (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_pb (id BIGINT PRIMARY KEY);
         INSERT INTO rv_pa VALUES (1); INSERT INTO rv_pb VALUES (1);",
    )
    .await;
    let uri2 = uri_for_user(&uri, "rv_role2", "pw2");
    mirror(&backend, &tmp, "rv_s1_pa", "public.rv_pa", &uri).await;
    mirror(&backend, &tmp, "rv_s2_pb", "public.rv_pb", &uri2).await;
    if errored_first {
        client.simple_query("TRUNCATE rv_pa;").await.unwrap();
    }
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        snapshot_error(&backend, "rv_s1_pa", lsn).await.is_some(),
        errored_first
    );
    assert_eq!(snapshot_error(&backend, "rv_s2_pb", lsn).await, None);
    tokio::time::sleep(Duration::from_millis(500)).await;
    backend.shutdown_connection(&uri, false).await;
    drop(backend);
    tokio::time::sleep(Duration::from_secs(1)).await;

    client
        .simple_query("ALTER ROLE rv_role2 NOLOGIN;")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO rv_pb VALUES (2);")
        .await
        .unwrap();
    let backend = new_backend(&tmp).await.unwrap();
    let pending = scan_error(&backend, "rv_s2_pb").await;
    eprintln!("rv10 pb while role cannot log in: {pending:?}");
    assert!(
        pending
            .as_deref()
            .is_some_and(|e| e.contains("not available yet")),
        "expected a pending mirror: {pending:?}"
    );
    if !errored_first {
        let lsn = current_wal_lsn(&client).await;
        assert_eq!(scan_ids(&backend, "rv_s1_pa", lsn).await, Some(vec![1]));
    }
    backend
        .drop_table(DB.to_string(), "rv_s1_pa".to_string())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let slots_after_drop = slot_count(&client).await;
    eprintln!("rv10 slots after dropping the other mirror: {slots_after_drop}");

    client
        .simple_query("ALTER ROLE rv_role2 LOGIN;")
        .await
        .unwrap();
    let mut restored = false;
    for _ in 0..90 {
        if scan_error(&backend, "rv_s2_pb").await.is_none() {
            restored = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(restored, "pending mirror was never restored");
    client
        .simple_query("INSERT INTO rv_pb VALUES (3);")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let rows = scan_ids(&backend, "rv_s2_pb", lsn).await;
    eprintln!("rv10 pb after restore: {rows:?}");
    let _ = backend
        .drop_table(DB.to_string(), "rv_s2_pb".to_string())
        .await;
    assert_eq!(
        (slots_after_drop, rows),
        (1, Some(vec![1, 2, 3])),
        "dropping the other mirror released the slot a pending mirror of the same database waited for"
    );
}

const PENDING_OTHER_DB: &str = "cdc_regressions_pending_other";

async fn slots_in_database(client: &Client, database: &str) -> i64 {
    client
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'moonlink_slot%' AND database = $1",
            &[&database],
        )
        .await
        .unwrap()
        .get(0)
}

/// A mirror pending in one database keeps only that database's slot: dropping the last mirror
/// (errored placeholder or live) of another database meanwhile drops that database's slot, or
/// nothing would ever release it.
#[rstest::rstest]
#[case::errored_placeholder(true)]
#[case::live_mirror(false)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn pending_mirror_keeps_only_its_own_database_slot(#[case] errored_other: bool) {
    let uri = get_database_uri();
    let (admin, _) = connect_to_postgres(&uri).await;
    for statement in [
        format!("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = '{PENDING_OTHER_DB}'"),
        format!("DROP DATABASE IF EXISTS {PENDING_OTHER_DB} WITH (FORCE)"),
        format!("CREATE DATABASE {PENDING_OTHER_DB}"),
    ] {
        admin.simple_query(&statement).await.unwrap();
    }
    let other_uri = uri_for_database(&uri, PENDING_OTHER_DB);
    {
        let (other, _) = connect_to_postgres(&other_uri).await;
        other
            .simple_query("CREATE TABLE rv_x (id BIGINT PRIMARY KEY); INSERT INTO rv_x VALUES (1);")
            .await
            .unwrap();
    }
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_p;
         DO $$ BEGIN
           IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rv_role3') THEN
             EXECUTE 'DROP OWNED BY rv_role3'; DROP ROLE rv_role3;
           END IF;
         END $$;
         CREATE ROLE rv_role3 LOGIN SUPERUSER PASSWORD 'pw3';
         CREATE TABLE rv_p (id BIGINT PRIMARY KEY);
         INSERT INTO rv_p VALUES (1);",
    )
    .await;
    let uri3 = uri_for_user(&uri, "rv_role3", "pw3");
    mirror(&backend, &tmp, "rv_s1_x", "public.rv_x", &other_uri).await;
    mirror(&backend, &tmp, "rv_s2_p", "public.rv_p", &uri3).await;
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(scan_ids(&backend, "rv_s2_p", lsn).await, Some(vec![1]));
    if errored_other {
        let (other, _) = connect_to_postgres(&other_uri).await;
        other.simple_query("TRUNCATE rv_x;").await.unwrap();
        let other_lsn = current_wal_lsn(&other).await;
        assert!(snapshot_error(&backend, "rv_s1_x", other_lsn)
            .await
            .is_some());
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    backend.shutdown_connection(&other_uri, false).await;
    backend.shutdown_connection(&uri3, false).await;
    drop(backend);
    tokio::time::sleep(Duration::from_secs(1)).await;

    client
        .simple_query("ALTER ROLE rv_role3 NOLOGIN;")
        .await
        .unwrap();
    let backend = new_backend(&tmp).await.unwrap();
    let pending = scan_error(&backend, "rv_s2_p").await;
    assert!(
        pending
            .as_deref()
            .is_some_and(|e| e.contains("not available yet")),
        "expected a pending mirror: {pending:?}"
    );
    backend
        .drop_table(DB.to_string(), "rv_s1_x".to_string())
        .await
        .unwrap();
    let mut other_slots = slots_in_database(&client, PENDING_OTHER_DB).await;
    for _ in 0..40 {
        if other_slots == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        other_slots = slots_in_database(&client, PENDING_OTHER_DB).await;
    }
    let own_slots = slot_count(&client).await;
    client
        .simple_query("ALTER ROLE rv_role3 LOGIN;")
        .await
        .unwrap();
    let _ = backend
        .drop_table(DB.to_string(), "rv_s2_p".to_string())
        .await;
    // Do not leak the slot into later tests.
    admin
        .simple_query(&format!(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = '{PENDING_OTHER_DB}' AND NOT active;"
        ))
        .await
        .unwrap();
    assert_eq!(
        (other_slots, own_slots),
        (0, 1),
        "(slots of the database without mirrors, slots of the pending mirror's database)"
    );
}

/// Once every mirror of a database is errored, its slot keeps advancing (errored mirrors never
/// apply changes again): at runtime through the replication loop, and after a restart, when they
/// are placeholders without a loop, through the placeholder watcher.
#[rstest::rstest]
#[case::after_restart(true)]
#[case::runtime(false)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn database_whose_mirrors_are_all_errored_does_not_pin_its_slot(#[case] restart: bool) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_ae, rv_ae_noise;
         CREATE TABLE rv_ae (id BIGINT PRIMARY KEY);
         CREATE TABLE rv_ae_noise (id BIGINT);",
    )
    .await;
    mirror(&backend, &tmp, "rv_ae_mc", "public.rv_ae", &uri).await;
    client
        .simple_query("INSERT INTO rv_ae VALUES (1); TRUNCATE rv_ae;")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert!(snapshot_error(&backend, "rv_ae_mc", lsn).await.is_some());
    tokio::time::sleep(Duration::from_millis(500)).await;
    let backend = if restart {
        backend.shutdown_connection(&uri, false).await;
        drop(backend);
        let backend = new_backend(&tmp).await.unwrap();
        assert!(scan_error(&backend, "rv_ae_mc").await.is_some());
        backend
    } else {
        backend
    };
    let before = confirmed_flush(&client).await;
    client
        .simple_query("INSERT INTO rv_ae_noise SELECT g FROM generate_series(1, 1000) g;")
        .await
        .unwrap();
    let target = current_wal_lsn(&client).await;
    let after = wait_confirmed_at_least(&client, target).await;
    let _ = before;
    let _ = backend
        .drop_table(DB.to_string(), "rv_ae_mc".to_string())
        .await;
    assert!(
        after >= target,
        "slot pinned at {after} (< {target}) although every mirror of the database is errored"
    );
}

/// Savepoints inside a streamed (large) transaction: Postgres tags a subtransaction's changes with
/// its own xid. Released savepoints, nested ones and PL/pgSQL exception blocks commit with the
/// transaction, exactly. A rolled-back savepoint's changes cannot be taken back out of the
/// streamed transaction, so the mirrors of the tables it changed are errored when the transaction
/// commits; other tables of the transaction stay exact, and if the whole transaction rolls back
/// (stream abort) nothing is applied and nothing errors. Non-streamed transactions are exact.
#[rstest::rstest]
#[case::released_savepoint(
    "SAVEPOINT s; INSERT INTO rv_sp SELECT i, repeat('s', 500) FROM generate_series(1001, 1400) i; RELEASE SAVEPOINT s;",
    true,
    "COMMIT",
    false
)]
#[case::update_and_delete_in_nested_savepoints(
    "SAVEPOINT s1; UPDATE rv_sp SET v = 'upd' WHERE id < -100; SAVEPOINT s2; DELETE FROM rv_sp WHERE id < -250; RELEASE SAVEPOINT s2; RELEASE SAVEPOINT s1;",
    true,
    "COMMIT",
    false
)]
#[case::plpgsql_exception_block(
    "DO $$ BEGIN BEGIN INSERT INTO rv_sp SELECT i, repeat('p', 500) FROM generate_series(2001, 2400) i; EXCEPTION WHEN others THEN NULL; END; END $$;",
    true,
    "COMMIT",
    false
)]
#[case::rolled_back_savepoint(
    "SAVEPOINT s; INSERT INTO rv_sp SELECT i, repeat('s', 500) FROM generate_series(1001, 1400) i; ROLLBACK TO SAVEPOINT s;",
    true,
    "COMMIT",
    true
)]
#[case::rolled_back_savepoint_then_transaction_rolled_back(
    "SAVEPOINT s; INSERT INTO rv_sp SELECT i, repeat('s', 500) FROM generate_series(1001, 1400) i; ROLLBACK TO SAVEPOINT s;",
    true,
    "ROLLBACK",
    false
)]
#[case::released_savepoint_then_transaction_rolled_back(
    "SAVEPOINT s; INSERT INTO rv_sp SELECT i, repeat('s', 500) FROM generate_series(1001, 1400) i; RELEASE SAVEPOINT s;",
    true,
    "ROLLBACK",
    false
)]
#[case::released_savepoint_not_streamed(
    "SAVEPOINT s; INSERT INTO rv_sp SELECT i, repeat('s', 500) FROM generate_series(1001, 1400) i; RELEASE SAVEPOINT s;",
    false,
    "COMMIT",
    false
)]
#[case::rolled_back_savepoint_not_streamed(
    "SAVEPOINT s; INSERT INTO rv_sp SELECT i, repeat('s', 500) FROM generate_series(1001, 1400) i; ROLLBACK TO SAVEPOINT s;",
    false,
    "COMMIT",
    false
)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn savepoints_in_a_streamed_transaction_apply_exactly_or_error_the_mirror(
    #[case] savepoint: &str,
    #[case] streamed: bool,
    #[case] end: &str,
    #[case] errors: bool,
) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_sp, rv_sq;
         CREATE TABLE rv_sp (id BIGINT PRIMARY KEY, v TEXT);
         CREATE TABLE rv_sq (id BIGINT PRIMARY KEY, v TEXT);
         INSERT INTO rv_sp SELECT i, 'pre' FROM generate_series(-300, -1) i;",
    )
    .await;
    mirror(&backend, &tmp, "rv_sp_mc", "public.rv_sp", &uri).await;
    mirror(&backend, &tmp, "rv_sq_mc", "public.rv_sq", &uri).await;
    let streamed_before = stream_txns(&client).await;
    stream_large_transactions(&client, streamed).await;
    run_large_transaction(
        &client,
        &format!(
            "BEGIN;
             INSERT INTO rv_sq SELECT i, repeat('a', 500) FROM generate_series(1, 400) i;
             INSERT INTO rv_sp VALUES (0, 'top');
             {savepoint}
             INSERT INTO rv_sq SELECT i, repeat('b', 500) FROM generate_series(401, 800) i;
             INSERT INTO rv_sp VALUES (1, 'top');"
        ),
        end,
        streamed,
        streamed_before,
    )
    .await;
    // A later small transaction: the mirrors keep following after the large one.
    client
        .simple_query(
            "INSERT INTO rv_sp VALUES (5000, 'after'); INSERT INTO rv_sq VALUES (5000, 'after');",
        )
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    let source = |table: &'static str| {
        let client = &client;
        async move {
            client
                .query(&format!("SELECT id, v FROM {table}"), &[])
                .await
                .unwrap()
                .iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect::<BTreeMap<i64, Option<String>>>()
        }
    };
    assert_eq!(snapshot_error(&backend, "rv_sq_mc", lsn).await, None);
    assert_eq!(
        scan_rows(&backend, "rv_sq_mc", lsn, "v", Duration::from_secs(30)).await,
        Some(source("rv_sq").await),
        "table without savepoint changes"
    );
    let sp_error = snapshot_error(&backend, "rv_sp_mc", lsn).await;
    if errors {
        let err = sp_error.expect("a rolled-back savepoint must error the mirror it changed");
        assert!(
            err.contains("was rolled back inside a transaction"),
            "{err}"
        );
    } else {
        assert_eq!(sp_error, None);
        assert_eq!(
            scan_rows(&backend, "rv_sp_mc", lsn, "v", Duration::from_secs(30)).await,
            Some(source("rv_sp").await),
            "table with savepoint changes"
        );
    }
    stop_streaming_large_transactions(&client, streamed, streamed_before).await;
}

/// A failure of a streamed transaction (TRUNCATE, unconvertible value) is not erased by a
/// savepoint rolled back later in it.
#[rstest::rstest]
#[case::truncate_then_rollback_to_savepoint(
    "INSERT INTO rv_st VALUES (10, '2024-01-01'); TRUNCATE rv_st; INSERT INTO rv_st VALUES (20, '2024-01-02');",
    "truncated"
)]
#[case::bad_value_then_rollback_to_savepoint(
    "INSERT INTO rv_st VALUES (10, '2024-01-01'), (11, 'infinity');",
    "could not be applied"
)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn rolled_back_savepoint_keeps_the_failures_of_its_streamed_transaction(
    #[case] changes: &str,
    #[case] reason: &str,
) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_st, rv_fill;
         CREATE TABLE rv_st (id BIGINT PRIMARY KEY, ts TIMESTAMP);
         CREATE TABLE rv_fill (id BIGINT PRIMARY KEY, v TEXT);
         INSERT INTO rv_st VALUES (1, '2023-01-01'), (2, '2023-01-02');",
    )
    .await;
    mirror(&backend, &tmp, "rv_st_mc", "public.rv_st", &uri).await;
    mirror(&backend, &tmp, "rv_fill_mc", "public.rv_fill", &uri).await;
    let streamed_before = stream_txns(&client).await;
    stream_large_transactions(&client, true).await;
    run_large_transaction(
        &client,
        &format!(
            "BEGIN;
             INSERT INTO rv_fill SELECT i, repeat('f', 500) FROM generate_series(1, 400) i;
             {changes}
             SAVEPOINT s;
             INSERT INTO rv_st VALUES (30, '2024-01-03');
             ROLLBACK TO SAVEPOINT s;
             INSERT INTO rv_fill SELECT i, repeat('g', 500) FROM generate_series(401, 800) i;"
        ),
        "COMMIT",
        true,
        streamed_before,
    )
    .await;
    let lsn = current_wal_lsn(&client).await;
    let err = snapshot_error(&backend, "rv_st_mc", lsn)
        .await
        .expect("mirror must be errored");
    assert!(err.contains(reason), "{err}");
    let source: BTreeMap<i64, Option<String>> = client
        .query("SELECT id, v FROM rv_fill", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(snapshot_error(&backend, "rv_fill_mc", lsn).await, None);
    assert_eq!(
        scan_rows(&backend, "rv_fill_mc", lsn, "v", Duration::from_secs(30)).await,
        Some(source)
    );
    stop_streaming_large_transactions(&client, true, streamed_before).await;
}

/// Postgres never decodes changes of an unlogged partition: one that appears later (created
/// unlogged, or an existing partition set unlogged) errors the mirror, and a partitioned source
/// with one is refused.
#[rstest::rstest]
#[case::unlogged_partition_created("CREATE UNLOGGED TABLE rv_part_2 PARTITION OF rv_part FOR VALUES FROM (100) TO (200); INSERT INTO rv_part VALUES (150, 'u');")]
#[case::partition_set_unlogged(
    "ALTER TABLE rv_part_4 SET UNLOGGED; INSERT INTO rv_part VALUES (350, 'u');"
)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn unlogged_partition_errors_the_mirror(#[case] ddl: &str) {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_part, rv_part_1, rv_part_2, rv_part_4;
         CREATE TABLE rv_part (id BIGINT PRIMARY KEY, v TEXT) PARTITION BY RANGE (id);
         CREATE TABLE rv_part_1 PARTITION OF rv_part FOR VALUES FROM (0) TO (100);
         CREATE TABLE rv_part_4 PARTITION OF rv_part FOR VALUES FROM (300) TO (400);
         INSERT INTO rv_part VALUES (1, 'a');",
    )
    .await;
    mirror(&backend, &tmp, "rv_part_mc", "public.rv_part", &uri).await;
    client.simple_query(ddl).await.unwrap();
    // Nothing of an unlogged partition reaches the stream; the source is polled every 5 s.
    let mut err = None;
    for _ in 0..20 {
        client
            .simple_query("INSERT INTO rv_part VALUES (2, 'b') ON CONFLICT (id) DO UPDATE SET v = rv_part.v || 'x';")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;
        err = snapshot_error(&backend, "rv_part_mc", lsn).await;
        if err.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let err = err.expect("a mirror with an unlogged partition must be errored");
    assert!(
        err.contains("is unlogged, so postgres never sends its changes"),
        "{err}"
    );

    let backend = Arc::new(backend);
    backend
        .drop_table(DB.to_string(), "rv_part_mc".to_string())
        .await
        .unwrap();
    let err = try_mirror(&backend, &tmp, "rv_part_mc", "public.rv_part", &uri)
        .await
        .unwrap_err();
    assert!(
        err.contains("is unlogged, so postgres never sends its changes"),
        "{err}"
    );
    client
        .simple_query("DROP TABLE IF EXISTS rv_part, rv_part_1, rv_part_2, rv_part_4;")
        .await
        .unwrap();
}

/// Only partitions that hold rows matter: an unlogged sub-partitioned member (allowed before
/// Postgres 18) whose leaves are regular tables is mirrored and replicates.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn unlogged_partitioned_member_with_logged_leaves_replicates() {
    let uri = get_database_uri();
    let (tmp, backend, client) = setup(
        "DROP TABLE IF EXISTS rv_part;
         CREATE TABLE rv_part (id BIGINT PRIMARY KEY, v TEXT) PARTITION BY RANGE (id);",
    )
    .await;
    if let Err(e) = client
        .simple_query(
            "CREATE UNLOGGED TABLE rv_part_1 PARTITION OF rv_part FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (id);",
        )
        .await
    {
        eprintln!("skipped: this Postgres has no unlogged partitioned tables ({e})");
        return;
    }
    client
        .simple_query(
            "CREATE TABLE rv_part_1a PARTITION OF rv_part_1 FOR VALUES FROM (0) TO (50);
             INSERT INTO rv_part VALUES (1, 'a');",
        )
        .await
        .unwrap();
    mirror(&backend, &tmp, "rv_part_mc", "public.rv_part", &uri).await;
    client
        .simple_query("INSERT INTO rv_part VALUES (2, 'b');")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(
        scan_ids(&backend, "rv_part_mc", lsn).await,
        Some(vec![1, 2])
    );
    // Also after the poll compared the source again.
    tokio::time::sleep(Duration::from_secs(6)).await;
    client
        .simple_query("INSERT INTO rv_part VALUES (3, 'c');")
        .await
        .unwrap();
    let lsn = current_wal_lsn(&client).await;
    assert_eq!(snapshot_error(&backend, "rv_part_mc", lsn).await, None);
    assert_eq!(
        scan_ids(&backend, "rv_part_mc", lsn).await,
        Some(vec![1, 2, 3])
    );
    client
        .simple_query("DROP TABLE IF EXISTS rv_part;")
        .await
        .unwrap();
}
