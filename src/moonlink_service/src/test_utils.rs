use crate::{start_with_config, ServiceConfig};
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::UnixStream;

/// Moonlink database name used by service tests.
#[cfg(feature = "postgres-integration")]
pub(crate) const DATABASE: &str = "test-database";

/// A moonlink service running on a temporary base path, reachable over its unix socket.
pub(crate) struct TestService {
    base_dir: TempDir,
}

impl TestService {
    /// Start the service the same way the pg_mooncake background worker does, and wait until its RPC socket accepts connections.
    pub(crate) async fn start() -> Self {
        let base_dir = tempfile::tempdir().unwrap();
        let config = ServiceConfig {
            base_path: base_dir.path().to_str().unwrap().to_string(),
            data_server_uri: None,
            tcp_port: None,
            log_directory: None,
        };
        tokio::spawn(async move {
            start_with_config(config).await.unwrap();
        });
        let service = Self { base_dir };
        for _ in 0..1200 {
            if UnixStream::connect(service.socket_path()).await.is_ok() {
                return service;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "moonlink service did not start listening on {:?}",
            service.socket_path()
        );
    }

    pub(crate) fn socket_path(&self) -> PathBuf {
        self.base_dir.path().join("moonlink.sock")
    }

    pub(crate) async fn connect(&self) -> UnixStream {
        UnixStream::connect(self.socket_path()).await.unwrap()
    }
}

#[cfg(feature = "postgres-integration")]
pub(crate) mod pg {
    use tokio_postgres::types::PgLsn;
    use tokio_postgres::{Client, NoTls};

    pub(crate) fn database_uri() -> String {
        std::env::var("DATABASE_URL")
            .expect("DATABASE_URL must point at a Postgres with wal_level=logical")
    }

    pub(crate) async fn connect() -> Client {
        let (client, connection) = tokio_postgres::connect(&database_uri(), NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    pub(crate) async fn current_wal_lsn(client: &Client) -> u64 {
        let row = client
            .query_one("SELECT pg_current_wal_lsn()", &[])
            .await
            .unwrap();
        let lsn: PgLsn = row.get(0);
        lsn.into()
    }

    /// (Re)create `public.{table}` with a primary key and `rows` rows (`id` 1..=rows).
    pub(crate) async fn create_source_table(client: &Client, table: &str, rows: i64) {
        client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {table};
                 CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT);
                 INSERT INTO {table} SELECT i, 'name-' || i FROM generate_series(1, {rows}) AS i;"
            ))
            .await
            .unwrap();
    }

    pub(crate) async fn drop_source_table(client: &Client, table: &str) {
        client
            .simple_query(&format!("DROP TABLE IF EXISTS {table};"))
            .await
            .unwrap();
    }
}
