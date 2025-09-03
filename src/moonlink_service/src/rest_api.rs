use axum::{
    extract::{Path, State},
    http::{Method, StatusCode},
    response::Json,
    routing::{delete, get, post},
    Router,
};
use moonlink::StorageConfig;
use moonlink_backend::{table_config::TableConfig, table_status::TableStatus};
use moonlink_backend::{EventRequest, FileEventOperation, RowEventOperation};
use moonlink_backend::{FileEventRequest, RowEventRequest, SnapshotRequest, REST_API_URI};
use moonlink_connectors::rest_ingest::schema_builder::{build_arrow_schema, FieldSchema};
use moonlink_error::ErrorStatus;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{mpsc, oneshot};
use tower_http::cors::{Any, CorsLayer};
use tracing::{debug, info};

/// API state shared across handlers
#[derive(Clone)]
pub struct ApiState {
    /// Reference to the backend for table operations
    pub backend: Arc<moonlink_backend::MoonlinkBackend>,
}

impl ApiState {
    pub fn new(backend: Arc<moonlink_backend::MoonlinkBackend>) -> Self {
        Self { backend }
    }
}

/// ====================
/// Error message
/// ====================
///
/// Request mode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RequestMode {
    /// Only issues request, but not block wait its completion.
    Async,
    /// Block wait request completion.
    Sync,
}

/// Error response structure
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub message: String,
}

/// ====================
/// Create table
/// ====================
///
/// Request structure for table creation
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTableRequest {
    pub database: String,
    pub table: String,
    pub schema: Vec<FieldSchema>,
    pub table_config: TableConfig,
}

/// Response structure for table creation
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTableResponse {
    pub database: String,
    pub table: String,
    pub lsn: u64,
}

/// ====================
/// Drop table
/// ====================
///
/// Request structure for table drop.
#[derive(Debug, Serialize, Deserialize)]
pub struct DropTableRequest {
    pub database: String,
    pub table: String,
}

/// Response structure for table drop.
#[derive(Debug, Serialize, Deserialize)]
pub struct DropTableResponse {}

/// ====================
/// List table
/// ====================
///
/// Response structure for table list.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListTablesResponse {
    pub tables: Vec<TableStatus>,
}

/// ====================
/// Optimize table
/// ====================
///
/// Request structure for table optimization.
#[derive(Debug, Serialize, Deserialize)]
pub struct OptimizeTableRequest {
    pub database: String,
    pub table: String,
    pub mode: String,
}

/// Response structure for table optimization.
#[derive(Debug, Serialize, Deserialize)]
pub struct OptimizeTableResponse {}

/// ====================
/// Create Snapshot
/// ====================
///
/// Request structure for snapshot creation.
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateSnapShotRequest {
    pub database: String,
    pub table: String,
    pub lsn: u64,
}

/// Response structure for snapshot creation.
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateSnapShotResponse {}

/// ====================
/// Data ingestion
/// ====================
///
/// Request structure for data ingestion
#[derive(Debug, Serialize, Deserialize)]
pub struct IngestRequest {
    pub operation: String,
    pub data: serde_json::Value,
    /// Whether to enable synchronous mode.
    pub request_mode: RequestMode,
}

/// Response structure for data ingestion
#[derive(Debug, Serialize, Deserialize)]
pub struct IngestResponse {
    pub table: String,
    pub operation: String,
    /// Assigned for synchronous mode.
    pub lsn: Option<u64>,
}

/// ====================
/// File upload
/// ====================
///
#[derive(Debug, Serialize, Deserialize)]
pub struct FileUploadRequest {
    /// Ingestion operation.
    pub operation: String,
    /// Files to ingest into mooncake table.
    pub files: Vec<String>,
    /// Storage configuration to access files.
    pub storage_config: StorageConfig,
    /// Whether to enable synchronous mode.
    pub request_mode: RequestMode,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileUploadResponse {
    /// Assigned for synchronous mode.
    pub lsn: Option<u64>,
}

/// ====================
/// Health check
/// ====================
///
/// Health check response
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub service: String,
    pub status: String,
    pub timestamp: u64,
}

/// Map backend error to appropriate HTTP status code based on error type
fn get_backend_error_status_code(error: &moonlink_backend::Error) -> StatusCode {
    match error {
        moonlink_backend::Error::InvalidArgumentError(_)
        | moonlink_backend::Error::ParseIntError(_)
        | moonlink_backend::Error::Json(_) => StatusCode::BAD_REQUEST,

        _ => match error.get_status() {
            ErrorStatus::Temporary => StatusCode::SERVICE_UNAVAILABLE,
            ErrorStatus::Permanent => StatusCode::INTERNAL_SERVER_ERROR,
        },
    }
}

/// Create the router with all API endpoints    
pub fn create_router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/tables", get(list_tables))
        .route("/tables/{table}", post(create_table))
        .route("/tables/{table}", delete(drop_table))
        .route("/ingest/{table}", post(ingest_data))
        .route("/upload/{table}", post(upload_files))
        .route("/tables/{table}/optimize", post(optimize_table))
        .route("/tables/{table}/snapshot", post(create_snapshot))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET, Method::POST, Method::DELETE])
                .allow_headers(Any),
        )
}

/// Health check endpoint
async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        service: "moonlink-rest-api".to_string(),
        status: "healthy".to_string(),
        timestamp: SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    })
}

/// Table creation endpoint
async fn create_table(
    Path(table): Path<String>,
    State(state): State<ApiState>,
    Json(payload): Json<CreateTableRequest>,
) -> Result<Json<CreateTableResponse>, (StatusCode, Json<ErrorResponse>)> {
    debug!(
        "Received table creation request for '{}': {:?}",
        table, payload
    );

    let arrow_schema = match build_arrow_schema(&payload.schema) {
        Ok(s) => s,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    message: format!(
                        "Invalid schema on table {} creation {:?} because {}",
                        table, payload.schema, e
                    ),
                }),
            ));
        }
    };

    // Serialization not expect to fail.
    let serialized_table_config = match serde_json::to_string(&payload.table_config) {
        Ok(cfg) => cfg,
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    message: format!("Serialize table config failed: {e}"),
                }),
            ));
        }
    };

    // Create table in backend
    match state
        .backend
        .create_table(
            payload.database.clone(),
            payload.table.clone(),
            table.clone(),
            REST_API_URI.to_string(),
            serialized_table_config,
            Some(arrow_schema),
        )
        .await
    {
        Ok(()) => {
            info!(
                "Successfully created table '{}' with ID {}:{}",
                table, payload.database, payload.table,
            );
            Ok(Json(CreateTableResponse {
                database: payload.database.clone(),
                table,
                // A new table is always with LSN 1.
                lsn: 1,
            }))
        }
        Err(e) => Err((
            get_backend_error_status_code(&e),
            Json(ErrorResponse {
                message: format!(
                    "Failed to create table {} with ID {}.{}: {}",
                    table, payload.database, payload.table, e
                ),
            }),
        )),
    }
}

/// Table drop endpoint
async fn drop_table(
    Path(table): Path<String>,
    State(state): State<ApiState>,
    Json(payload): Json<DropTableRequest>,
) -> Result<Json<DropTableResponse>, (StatusCode, Json<ErrorResponse>)> {
    debug!("Received table drop request for '{}': {:?}", table, payload);

    // Drop table in backend
    state
        .backend
        .drop_table(payload.database.clone(), payload.table.clone())
        .await
        .map_err(|e| {
            (
                get_backend_error_status_code(&e),
                Json(ErrorResponse {
                    message: format!(
                        "Failed to drop table {} with ID {}.{}: {}",
                        table, payload.database, payload.table, e
                    ),
                }),
            )
        })?;
    Ok(Json(DropTableResponse {}))
}

/// Table list endpoint
async fn list_tables(
    State(state): State<ApiState>,
) -> Result<Json<ListTablesResponse>, (StatusCode, Json<ErrorResponse>)> {
    match state.backend.list_tables().await {
        Ok(tables) => Ok(Json(ListTablesResponse { tables })),
        Err(e) => Err((
            get_backend_error_status_code(&e),
            Json(ErrorResponse {
                message: format!("Failed to list tables: {e}"),
            }),
        )),
    }
}

/// File upload endpoint.
async fn upload_files(
    Path(src_table_name): Path<String>,
    State(state): State<ApiState>,
    Json(payload): Json<FileUploadRequest>,
) -> Result<Json<FileUploadResponse>, (StatusCode, Json<ErrorResponse>)> {
    debug!(
        "Received file upload request for table '{}': {:?}",
        src_table_name, payload
    );

    let operation = match payload.operation.as_str() {
        "insert" => FileEventOperation::Insert,
        "upload" => FileEventOperation::Upload,
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    message: format!(
                        "Invalid operation '{}' for file upload. Must be 'insert' or 'upload'",
                        payload.operation
                    ),
                }),
            ));
        }
    };

    // Create REST request.
    let (tx, mut rx) = mpsc::channel(1);
    let file_event_request = FileEventRequest {
        src_table_name: src_table_name.clone(),
        operation,
        storage_config: payload.storage_config,
        files: payload.files,
        tx: if payload.request_mode == RequestMode::Sync {
            Some(tx)
        } else {
            None
        },
    };
    let rest_event_request = EventRequest::FileRequest(file_event_request);
    state
        .backend
        .send_event_request(rest_event_request)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    message: format!("Failed to process request for file upload request: {e}"),
                }),
            )
        })?;

    let lsn: Option<u64> = if payload.request_mode == RequestMode::Sync {
        rx.recv().await
    } else {
        None
    };
    Ok(Json(FileUploadResponse { lsn }))
}

async fn optimize_table(
    Path(table): Path<String>,
    State(state): State<ApiState>,
    Json(payload): Json<OptimizeTableRequest>,
) -> Result<Json<OptimizeTableResponse>, (StatusCode, Json<ErrorResponse>)> {
    debug!(
        "Received table optimize request for '{}': {:?}",
        table, payload
    );
    match state
        .backend
        .optimize_table(
            payload.database.clone(),
            payload.table.clone(),
            payload.mode.as_str(),
        )
        .await
    {
        Ok(_) => Ok(Json(OptimizeTableResponse {})),
        Err(e) => {
            let status_code = get_backend_error_status_code(&e);
            Err((
                status_code,
                Json(ErrorResponse {
                    message: format!(
                        "Failed to optimize table {} with ID {}.{}: {}",
                        table, payload.database, payload.table, e
                    ),
                }),
            ))
        }
    }
}

/// Create snapshot endpoint
async fn create_snapshot(
    Path(src_table_name): Path<String>,
    State(state): State<ApiState>,
    Json(payload): Json<CreateSnapShotRequest>,
) -> Result<Json<CreateSnapShotResponse>, (StatusCode, Json<ErrorResponse>)> {
    debug!(
        "Received create snapshot request for table {} with ID {}.{}",
        src_table_name, &payload.database, &payload.table,
    );

    let (tx, mut rx) = mpsc::channel(1);
    let snapshot_request = SnapshotRequest {
        src_table_name: src_table_name.clone(),
        lsn: payload.lsn,
        tx,
    };
    let rest_event_request = EventRequest::SnapshotRequest(snapshot_request);
    state
        .backend
        .send_event_request(rest_event_request)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    message: format!("Failed to process snapshot creation request: {e}"),
                }),
            )
        })?;

    // Block until snapshot creation event has been sent to moonlink table handler.
    let _ = rx.recv().await;

    // Now it's ensured all events before snapshot creation have been received by table handler, we could block wait snapshot creation completion.
    match state
        .backend
        .create_snapshot(payload.database.clone(), payload.table.clone(), payload.lsn)
        .await
    {
        Ok(_) => Ok(Json(CreateSnapShotResponse {})),
        Err(e) => {
            let status_code = get_backend_error_status_code(&e);
            Err((
                status_code,
                Json(ErrorResponse {
                    message: format!(
                        "Failed to create snapshot for table {} with ID {}.{}: {}",
                        src_table_name, payload.database, payload.table, e
                    ),
                }),
            ))
        }
    }
}

/// Data ingestion endpoint
async fn ingest_data(
    Path(src_table_name): Path<String>,
    State(state): State<ApiState>,
    Json(payload): Json<IngestRequest>,
) -> Result<Json<IngestResponse>, (StatusCode, Json<ErrorResponse>)> {
    debug!(
        "Received ingestion request for table '{}': {:?}",
        src_table_name, payload
    );

    // Parse operation.
    let operation = match payload.operation.to_lowercase().as_str() {
        "insert" => RowEventOperation::Insert,
        "upsert" => RowEventOperation::Upsert,
        "delete" => RowEventOperation::Delete,
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    message: format!(
                        "Invalid operation '{}' for data ingestion. Must be 'insert', 'upsert', or 'delete'",
                        payload.operation
                    ),
                }),
            ));
        }
    };

    // Create REST request
    let (tx, mut rx) = mpsc::channel(1);
    let row_event_request = RowEventRequest {
        src_table_name: src_table_name.clone(),
        operation,
        payload: payload.data,
        timestamp: SystemTime::now(),
        tx: if payload.request_mode == RequestMode::Sync {
            Some(tx)
        } else {
            None
        },
    };
    let rest_event_request = EventRequest::RowRequest(row_event_request);

    state
        .backend
        .send_event_request(rest_event_request)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    message: format!(
                        "Failed to process data ingestion request for table {src_table_name} because {e}"
                    ),
                }),
            )
        })?;

    let lsn: Option<u64> = if payload.request_mode == RequestMode::Sync {
        rx.recv().await
    } else {
        None
    };
    Ok(Json(IngestResponse {
        table: src_table_name,
        operation: payload.operation,
        lsn,
    }))
}

/// Start the REST API server
pub async fn start_server(
    state: ApiState,
    port: u16,
    shutdown_signal: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_router(state);
    let addr = format!("0.0.0.0:{port}");

    info!("Starting REST API server on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            shutdown_signal.await.ok();
        })
        .await?;

    Ok(())
}
