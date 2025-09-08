mod error;
mod logging;
mod otel;
pub(crate) mod rest_api;
mod rpc_server;

pub use error::Result;

use moonlink_backend::MoonlinkBackend;
use moonlink_metadata_store::SqliteMetadataStore;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::{atomic::Ordering, Arc};
use tokio::{
    net::TcpListener,
    signal::unix::{signal, SignalKind},
};
use tracing::{error, info};

/// Default readiness probe port number.
pub(crate) const READINESS_PROBE_PORT: u16 = 5050;

/// Service initiation and execution status.
struct ServiceStatus {
    /// Whether the service starts up successfully.
    ready: AtomicBool,
}

#[derive(Debug)]
pub struct ServiceConfig {
    /// Base location for moonlink storage (including cache files, iceberg tables, etc).
    pub base_path: String,
    /// Data server URI, if assigned, all table scan file path should be remapped to conform data server.
    pub data_server_uri: Option<String>,
    /// Used for REST API as ingestion source.
    pub rest_api_port: Option<u16>,
    /// Used for otel data ingestion.
    pub otel_api_port: Option<u16>,
    /// Used for moonlink standalone deployment.
    pub tcp_port: Option<u16>,
}

impl ServiceConfig {
    /// Whether moonlink is deployed as a standalone service.
    pub fn in_standalone_deployment_mode(&self) -> bool {
        self.rest_api_port.is_some() || self.tcp_port.is_some()
    }
}

async fn service_ready(
    axum::extract::State(state): axum::extract::State<Arc<ServiceStatus>>,
) -> impl axum::response::IntoResponse {
    if !state.ready.load(Ordering::SeqCst) {
        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, "not ready");
    }
    (axum::http::StatusCode::OK, "ready")
}

/// Setup readiness probe for moonlink backend service.
fn setup_readiness_probe() -> Arc<ServiceStatus> {
    let service_status = Arc::new(ServiceStatus {
        ready: AtomicBool::new(false),
    });
    let service_status_clone = service_status.clone();
    tokio::spawn(async move {
        let app = axum::Router::new()
            .route("/ready", axum::routing::get(service_ready))
            .with_state(service_status_clone);
        let addr = SocketAddr::from(([0, 0, 0, 0], READINESS_PROBE_PORT));
        let listener = TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
        info!("health server on {addr}");
    });
    service_status
}

pub async fn start_with_config(config: ServiceConfig) -> Result<()> {
    // Set logging config before service start.
    logging::init_logging();

    // Register HTTP endpoint for readiness probe.
    let service_status = if config.in_standalone_deployment_mode() {
        Some(setup_readiness_probe())
    } else {
        None
    };

    // Initialize moonlink backend.
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let sqlite_metadata_accessor = SqliteMetadataStore::new_with_directory(&config.base_path)
        .await
        .unwrap();
    let mut backend = MoonlinkBackend::new(
        config.base_path.clone(),
        config.data_server_uri.clone(),
        Box::new(sqlite_metadata_accessor),
    )
    .await?;

    if config.rest_api_port.is_some() {
        backend.initialize_event_api().await?;
    }

    let backend = Arc::new(backend);

    // Start RPC server on Unix socket
    let socket_path = std::path::PathBuf::from(&config.base_path).join("moonlink.sock");
    let rpc_backend = backend.clone();
    let rpc_handle = tokio::spawn(async move {
        if let Err(e) = rpc_server::start_unix_server(rpc_backend, socket_path).await {
            error!("RPC server failed: {}", e);
        }
    });

    // Optionally start REST API
    let (rest_api_handle, rest_api_shutdown_signal) = if let Some(port) = config.rest_api_port {
        let api_state = rest_api::ApiState::new(backend.clone());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            if let Err(e) = rest_api::start_server(api_state, port, shutdown_rx).await {
                error!("REST API server failed: {}", e);
            }
        });
        (Some(handle), Some(shutdown_tx))
    } else {
        (None, None)
    };

    // Optionally start otel HTTP endpoint.
    let (otel_api_handle, otel_api_shutdown_signal) = if let Some(otel_port) = config.otel_api_port
    {
        if let Some(rest_port) = config.rest_api_port {
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            let backend_clone = backend.clone();
            let handle = tokio::spawn(async move {
                if let Err(e) = otel::service::start_otel_service(
                    otel_port,
                    rest_port,
                    backend_clone,
                    shutdown_rx,
                )
                .await
                {
                    error!("OTEL service failed: {}", e);
                }
                info!("Starting OTLP/HTTP metrics starts at port {otel_port}");
            });
            (Some(handle), Some(shutdown_tx))
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    // Optionally start TCP server.
    let tcp_api_handle = if let Some(port) = config.tcp_port {
        let backend_clone = backend.clone();
        let addr: std::net::SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
        // TODO(hjiang): Implement graceful shutdown for TCP server.
        let handle = tokio::spawn(async move {
            if let Err(e) = rpc_server::start_tcp_server(backend_clone, addr).await {
                error!("TCP rpc server failed: {}", e);
            }
            println!("TCP rpc server starts at port {port}");
        });
        Some(handle)
    } else {
        None
    };

    // Moonlink and backend services have started.
    if let Some(service_status) = service_status {
        service_status.ready.store(true, Ordering::SeqCst);
    }
    info!("Moonlink service started successfully");

    // Wait for termination signal
    let _ = sigterm.recv().await;
    info!("Received SIGTERM, shutting down...");

    // Clean shutdown: abort background servers
    if let Some(handle) = rest_api_handle {
        rest_api_shutdown_signal
            .expect("REST API shutdown sender supposed to be valid")
            .send(())
            .unwrap();
        handle.await?;
    }

    if let Some(handle) = otel_api_handle {
        otel_api_shutdown_signal
            .expect("OTEL HTTP API shutdown sender supposed to be valid")
            .send(())
            .unwrap();
        handle.await?;
    }

    if let Some(handle) = tcp_api_handle {
        handle.abort();
    }

    rpc_handle.abort();

    info!("Moonlink service shut down complete");
    Ok(())
}

#[cfg(all(test, any(feature = "standalone-test", feature = "otel-integration")))]
mod test_guard;

#[cfg(all(test, any(feature = "standalone-test", feature = "otel-integration")))]
mod test_utils;

#[cfg(all(test, feature = "standalone-test"))]
mod test;
