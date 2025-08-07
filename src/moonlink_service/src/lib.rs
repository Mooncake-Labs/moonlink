mod error;
pub mod rest_api;
pub mod rpc_server;

pub use error::Result;
use moonlink_backend::MoonlinkBackend;
use moonlink_metadata_store::SqliteMetadataStore;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, warn};

pub struct ServiceConfig {
    pub base_path: String,
    pub rest_api_port: Option<u16>,
}

pub async fn start_with_config(config: ServiceConfig) -> Result<()> {
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let sqlite_metadata_accessor = SqliteMetadataStore::new_with_directory(&config.base_path)
        .await
        .unwrap();
    let mut backend =
        MoonlinkBackend::new(config.base_path.clone(), Box::new(sqlite_metadata_accessor)).await?;

    if config.rest_api_port.is_some() {
        backend.initialize_event_api().await?;
    }

    let backend = Arc::new(backend);

    // Start RPC server on Unix socket
    let socket_path = std::path::PathBuf::from(&config.base_path).join("moonlink.sock");
    let rpc_backend = backend.clone();
    let rpc_handle = tokio::spawn(async move {
        if let Err(e) = rpc_server::start_unix_server(rpc_backend, socket_path).await {
            warn!("RPC server failed: {}", e);
        }
    });

    // Optionally start REST API
    let rest_api_handle = if let Some(port) = config.rest_api_port {
        let api_state = rest_api::ApiState::new(backend.clone());
        Some(tokio::spawn(async move {
            if let Err(e) = rest_api::start_server(api_state, port).await {
                warn!("REST API server failed: {}", e);
            }
        }))
    } else {
        None
    };

    info!("Moonlink service started successfully");

    // Wait for termination signal
    let _ = sigterm.recv().await;
    info!("Received SIGTERM, shutting down...");

    // Clean shutdown: abort background servers
    if let Some(handle) = rest_api_handle {
        handle.abort();
    }
    rpc_handle.abort();

    info!("Moonlink service shut down complete");
    Ok(())
}
