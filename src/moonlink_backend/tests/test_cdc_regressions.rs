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
    tokio::time::timeout(
        Duration::from_secs(30),
        backend.create_snapshot(DB.to_string(), dst.to_string(), lsn),
    )
    .await
    .expect("create_snapshot timed out")
    .err()
    .map(|e| format!("{e:?}"))
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
        Ok(result) => result.map_err(|e| format!("{e:?}")),
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
