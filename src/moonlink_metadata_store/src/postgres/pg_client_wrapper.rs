use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{connect, Client};

use crate::error::Result;

/// A wrapper around tokio postgres client and connection.
pub(super) struct PgClientWrapper {
    /// Postgres client.
    pub(super) postgres_client: Client,
    /// Postgres connection join handle, which would be cancelled at destruction.
    _pg_connection: tokio::task::JoinHandle<()>,
}

impl PgClientWrapper {
    pub(super) async fn new(uri: &str) -> Result<Self> {
        let tls_connector = TlsConnector::new().unwrap();
        let tls = MakeTlsConnector::new(tls_connector);

        let (postgres_client, connection) = connect(uri, tls).await?;

        // Spawn connection driver in background to keep eventloop alive.
        let _pg_connection = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Postgres connection error: {e}");
            }
        });

        Ok(PgClientWrapper {
            postgres_client,
            _pg_connection,
        })
    }
}
