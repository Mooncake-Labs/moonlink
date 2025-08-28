use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
/// Test environment to setup and cleanup a test case.
use tokio_postgres::{connect, Client};

#[allow(dead_code)]
pub(crate) struct TestEnvironment {
    postgres_client: Client,
    _connection_handle: tokio::task::JoinHandle<()>,
}

#[allow(dead_code)]
impl TestEnvironment {
    async fn delete_tables_if_exists(postgres_client: &Client) {
        postgres_client
            .simple_query("DROP TABLE IF EXISTS tables")
            .await
            .unwrap();
        postgres_client
            .simple_query("DROP TABLE IF EXISTS secrets")
            .await
            .unwrap();
    }

    /// Delete test moonlink metadata table.
    pub(crate) async fn new(uri: &str) -> Self {
        let (postgres_client, _connection_handle) = get_postgres_client(uri).await;
        Self::delete_tables_if_exists(&postgres_client).await;
        Self {
            postgres_client,
            _connection_handle,
        }
    }

    /// Delete moonlink schema.
    pub(crate) async fn delete_mooncake_schema(&self) {
        Self::delete_tables_if_exists(&self.postgres_client).await;
    }
}

pub(crate) async fn get_postgres_client(uri: &str) -> (Client, tokio::task::JoinHandle<()>) {
    #[cfg(not(feature = "test-tls"))]
    let connector = TlsConnector::new().unwrap();

    #[cfg(feature = "test-tls")]
    let connector = TlsConnector::builder()
        .add_root_certificate(
            native_tls::Certificate::from_pem(
                std::fs::read("../../.devcontainer/certs/ca.crt")
                    .unwrap()
                    .as_slice(),
            )
            .unwrap(),
        )
        .build()
        .unwrap();

    let tls = MakeTlsConnector::new(connector);
    let (postgres_client, connection) = connect(uri, tls).await.unwrap();
    let _connection_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Postgres connection error: {e}");
        }
    });
    (postgres_client, _connection_handle)
}
