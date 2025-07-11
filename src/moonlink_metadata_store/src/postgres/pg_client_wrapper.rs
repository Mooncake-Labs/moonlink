use tokio_postgres::{connect, Client, NoTls};

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
        let (postgres_client, connection) = connect(uri, NoTls).await?;

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
