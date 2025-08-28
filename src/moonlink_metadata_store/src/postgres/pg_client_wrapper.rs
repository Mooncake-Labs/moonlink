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
        let (postgres_client, _pg_connection) = connect_to_postgres(uri).await?;

        Ok(PgClientWrapper {
            postgres_client,
            _pg_connection,
        })
    }
}

#[cfg(not(feature = "test-tls"))]
pub(crate) async fn connect_to_postgres(
    uri: &str,
) -> Result<(Client, tokio::task::JoinHandle<()>)> {
    let tls_connector = TlsConnector::new().unwrap();
    let tls = MakeTlsConnector::new(tls_connector);
    let (postgres_client, connection) = connect(uri, tls).await?;

    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    Ok((postgres_client, connection_handle))
}

#[cfg(feature = "test-tls")]
pub(crate) async fn connect_to_postgres(
    uri: &str,
) -> Result<(Client, tokio::task::JoinHandle<()>)> {
    let root_cert_pem = std::fs::read("../../.devcontainer/certs/ca.crt").unwrap();

    let connector = TlsConnector::builder()
        .add_root_certificate(native_tls::Certificate::from_pem(root_cert_pem.as_slice()).unwrap())
        .build()
        .unwrap();
    let tls = MakeTlsConnector::new(connector);
    let (client, connection) = connect(uri, tls).await.unwrap();
    let connection_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok((client, connection_handle))
}
