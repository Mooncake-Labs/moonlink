/// Test environment to setup and cleanup a test case.
use tokio_postgres::{connect, Client, NoTls};

pub(crate) struct TestEnvironment {
    postgres_client: Client,
    _connection_handle: tokio::task::JoinHandle<()>,
}

impl TestEnvironment {
    /// Delete test moonlink metadata table.
    pub(crate) async fn new(uri: &str) -> Self {
        let (postgres_client, connection) = connect(uri, NoTls).await.unwrap();
        let _connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {e}");
            }
        });
        postgres_client
            .simple_query("DROP TABLE IF EXISTS mooncake.tables")
            .await
            .unwrap();
        postgres_client
            .simple_query("CREATE SCHEMA IF NOT EXISTS mooncake;")
            .await
            .unwrap();
        Self {
            postgres_client,
            _connection_handle,
        }
    }

    /// Delete moonlink schema.
    pub(crate) async fn delete_mooncake_schema(&self) {
        self.postgres_client
            .simple_query("DROP SCHEMA IF EXISTS mooncake")
            .await
            .unwrap();
    }
}
