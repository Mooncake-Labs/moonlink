mod error;
mod logging;
mod rpc_server;

pub use error::Error;
pub use error::Result;

use moonlink_backend::MoonlinkBackend;
use moonlink_metadata_store::SqliteMetadataStore;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{error, info};

#[derive(Debug)]
pub struct ServiceConfig {
    /// Base location for moonlink storage (including cache files, iceberg tables, etc).
    /// The RPC server listens on `{base_path}/moonlink.sock`.
    pub base_path: String,
}

pub async fn start_with_config(config: ServiceConfig) -> Result<()> {
    logging::init_logging();

    // Initialize moonlink backend.
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let sqlite_metadata_accessor = SqliteMetadataStore::new_with_directory(&config.base_path)
        .await
        .unwrap();
    let backend = MoonlinkBackend::new(
        config.base_path.clone(),
        /*data_server_uri=*/ None,
        Box::new(sqlite_metadata_accessor),
    )
    .await?;
    let backend = Arc::new(backend);

    // Start RPC server on Unix socket
    let socket_path = std::path::PathBuf::from(&config.base_path).join("moonlink.sock");
    let rpc_handle = tokio::spawn(async move {
        if let Err(e) = rpc_server::start_unix_server(backend, socket_path).await {
            error!("RPC server failed: {}", e);
        }
    });
    info!("Moonlink service started successfully");

    // Wait for termination signal
    let _ = sigterm.recv().await;
    info!("Received SIGTERM, shutting down...");
    rpc_handle.abort();

    info!("Moonlink service shut down complete");
    Ok(())
}

#[cfg(test)]
mod schema_wire_golden;

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod test;
