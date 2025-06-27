use arrow_array::Int64Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;
use tempfile::TempDir;
use tokio_postgres::{connect, types::PgLsn, Client, NoTls};

use std::{collections::HashSet, fs::File};

use moonlink::decode_read_state_for_testing;
use moonlink_backend::{
    recreate_directory, MoonlinkBackend, ReadState, DEFAULT_MOONLINK_TEMP_FILE_PATH,
};

pub const URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";

pub struct TestGuard {
    backend: Arc<MoonlinkBackend<&'static str>>,
    table_name: Option<&'static str>,
    tmp: Option<TempDir>,
}

impl TestGuard {
    pub async fn new(table_name: Option<&'static str>) -> (Self, Client) {
        let (tmp, backend, client) = setup_backend(table_name).await;
        let guard = Self {
            backend: Arc::new(backend),
            table_name,
            tmp: Some(tmp),
        };
        (guard, client)
    }

    pub fn backend(&self) -> &Arc<MoonlinkBackend<&'static str>> {
        &self.backend
    }

    #[allow(dead_code)]
    pub fn tmp(&self) -> Option<&TempDir> {
        self.tmp.as_ref()
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        // move everything we need into the async block
        let backend = Arc::clone(&self.backend);
        let table = self.table_name;
        let tmp = self.tmp.take();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                if let Some(table) = table {
                    let _ = backend.drop_table(table).await;
                }
                let _ = backend.shutdown_connection(URI).await;
                let _ = recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH);
                drop(tmp);
            });
        });
    }
}

/// Return the current WAL LSN as a simple `u64`.
pub async fn current_wal_lsn(client: &Client) -> u64 {
    let row = client
        .query_one("SELECT pg_current_wal_lsn()", &[])
        .await
        .unwrap();
    let lsn: PgLsn = row.get(0);
    lsn.into()
}

/// Read the first column of a Parquet file into a `Vec<Option<i64>>`.
pub fn read_ids_from_parquet(path: &str) -> Vec<Option<i64>> {
    let file = File::open(path).unwrap_or_else(|_| panic!("open {path}"));
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..col.len()).map(|i| Some(col.value(i))).collect()
}

/// Extract **all** primary-key IDs referenced in `read_state`.
pub fn ids_from_state(read_state: &ReadState) -> HashSet<i64> {
    let (files, _, _, _) = decode_read_state_for_testing(read_state);
    files
        .into_iter()
        .flat_map(|f| read_ids_from_parquet(&f).into_iter().flatten())
        .collect()
}

/// Spin up a backend + scratch TempDir + psql client, and guarantee
/// a **fresh table** named `table_name` exists and is registered with
/// Moonlink.
async fn setup_backend(
    table_name: Option<&'static str>,
) -> (TempDir, MoonlinkBackend<&'static str>, Client) {
    let temp_dir = TempDir::new().unwrap();
    let uri = URI;
    let backend = MoonlinkBackend::<&'static str>::new(temp_dir.path().to_str().unwrap().into());

    // Connect to Postgres.
    let (client, connection) = connect(uri, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Clear any leftover replication slot from previous runs.
    let _ = client
        .simple_query(
            "SELECT pg_terminate_backend(active_pid)
             FROM pg_replication_slots
             WHERE slot_name = 'moonlink_slot_postgres';",
        )
        .await;
    let _ = client
        .simple_query("SELECT pg_drop_replication_slot('moonlink_slot_postgres')")
        .await;

    // Re-create the working table.
    if let Some(table_name) = table_name {
        client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {0};
                 CREATE TABLE {0} (id BIGINT PRIMARY KEY, name TEXT);",
                table_name
            ))
            .await
            .unwrap();
        backend
            .create_table(table_name, &format!("public.{table_name}"), uri)
            .await
            .unwrap();
    }

    (temp_dir, backend, client)
}

/// Reusable helper for the "create table / insert rows / detect change"
/// scenario used in two places.
#[allow(dead_code)]
pub async fn smoke_create_and_insert(
    backend: &MoonlinkBackend<&'static str>,
    client: &Client,
    uri: &str,
) {
    client
        .simple_query(
            "DROP TABLE IF EXISTS test;
                           CREATE TABLE test (id BIGINT PRIMARY KEY, name TEXT);",
        )
        .await
        .unwrap();

    backend
        .create_table("test", "public.test", uri)
        .await
        .unwrap();

    // First two rows.
    client
        .simple_query("INSERT INTO test VALUES (1,'foo'),(2,'bar');")
        .await
        .unwrap();

    let old = backend.scan_table(&"test", None).await.unwrap();
    let lsn = current_wal_lsn(client).await;
    let new = backend.scan_table(&"test", Some(lsn)).await.unwrap();
    assert_ne!(old.data, new.data);

    recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
}
