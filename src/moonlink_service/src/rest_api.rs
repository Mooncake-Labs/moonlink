use axum::{
    extract::{Path, State},
    http::{Method, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use moonlink::StorageConfig;
use moonlink_backend::table_config::TableConfig;
use moonlink_backend::{
    EventRequest, FileEventOperation, FileEventRequest, RowEventOperation, RowEventRequest,
    REST_API_URI,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::oneshot;
use tower_http::cors::{Any, CorsLayer};
use tracing::{debug, error, info};

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
/// Error response structure
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
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

/// Field schema definition
#[derive(Debug, Serialize, Deserialize)]
pub struct FieldSchema {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Response structure for table creation
#[derive(Debug, Serialize)]
pub struct CreateTableResponse {
    pub status: String,
    pub message: String,
    pub table_name: String,
    pub schema: String,
}

/// ====================
/// Data ingestion
/// ====================
///
/// Request structure for data ingestion
#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub operation: String,
    pub data: serde_json::Value,
}

/// Response structure for data ingestion
#[derive(Debug, Serialize)]
pub struct IngestResponse {
    pub status: String,
    pub message: String,
    pub table: String,
    pub operation: String,
}

/// ====================
/// File upload
/// ====================
///
#[derive(Debug, Deserialize)]
pub struct FileUploadRequest {
    /// Ingestion operation.
    pub operation: String,
    /// Files to ingest into mooncake table.
    pub files: Vec<String>,
    /// Storage configuration to access files.
    pub storage_config: StorageConfig,
}

#[derive(Debug, Serialize)]
pub struct FileUploadResponse {
    pub status: String,
    pub message: String,
}

/// ====================
/// Health check
/// ====================
///
/// Health check response
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub service: String,
    pub status: String,
    pub timestamp: u64,
}

/// Create the router with all API endpoints
pub fn create_router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/tables/{table}", post(create_table))
        .route("/ingest/{table}", post(ingest_data))
        .route("/upload/{table}", post(upload_files))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET, Method::POST])
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
    Path(table_name): Path<String>,
    State(state): State<ApiState>,
    Json(payload): Json<CreateTableRequest>,
) -> Result<Json<CreateTableResponse>, (StatusCode, Json<ErrorResponse>)> {
    debug!(
        "Received table creation request for '{}': {:?}",
        table_name, payload
    );

    // Convert field schemas to Arrow schema with proper field IDs (like PostgreSQL)
    use arrow_schema::{DataType, Field, Schema};

    let mut field_id = 0;
    let fields: Result<Vec<Field>, String> = payload
        .schema
        .iter()
        .map(|field| {
            let data_type = match field.data_type.as_str() {
                "int32" => DataType::Int32,
                "int64" => DataType::Int64,
                "string" | "text" => DataType::Utf8,
                "boolean" | "bool" => DataType::Boolean,
                "float32" => DataType::Float32,
                "float64" => DataType::Float64,
                _ => return Err(format!("Unsupported data type: {}", field.data_type)),
            };

            // Create field with metadata (like PostgreSQL does)
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            field_id += 1;

            let field_with_metadata =
                Field::new(&field.name, data_type, field.nullable).with_metadata(metadata);

            Ok(field_with_metadata)
        })
        .collect();

    let fields = match fields {
        Ok(fields) => fields,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_schema".to_string(),
                    message: e,
                }),
            ));
        }
    };

    let arrow_schema = Schema::new(fields);

    // Serialization not expect to fail.
    let serialized_table_config = serde_json::to_string(&payload.table_config).unwrap();

    // Create table in backend
    match state
        .backend
        .create_table(
            payload.database.clone(),
            payload.table.clone(),
            table_name.clone(),
            REST_API_URI.to_string(),
            serialized_table_config,
            Some(arrow_schema),
        )
        .await
    {
        Ok(()) => {
            info!(
                "Successfully created table '{}' with ID {}:{}",
                table_name, payload.database, payload.table,
            );
            Ok(Json(CreateTableResponse {
                status: "success".to_string(),
                message: "Table created successfully".to_string(),
                table_name,
                schema: payload.database.clone(),
            }))
        }
        Err(e) => {
            error!("Failed to create table '{}': {}", table_name, e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "table_creation_failed".to_string(),
                    message: format!("Failed to create table: {e}"),
                }),
            ))
        }
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
                    error: "invalid_operation".to_string(),
                    message: format!(
                        "Invalid operation '{}'. Must be 'insert' or 'upload'",
                        payload.operation
                    ),
                }),
            ));
        }
    };

    // Create REST request.
    let file_event_request = FileEventRequest {
        src_table_name: src_table_name.clone(),
        operation,
        storage_config: payload.storage_config,
        files: payload.files,
    };
    let rest_event_request = EventRequest::FileRequest(file_event_request);
    state
        .backend
        .send_event_request(rest_event_request)
        .await
        .map_err(|e| {
            error!("Failed to send event request: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "file_upload_failed".to_string(),
                    message: format!("Failed to process request: {e}"),
                }),
            )
        })?;
    Ok(Json(FileUploadResponse {
        status: "success".to_string(),
        message: "File queued for ingestion".to_string(),
    }))
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

    // Parse operation
    let operation = match payload.operation.as_str() {
        "insert" => RowEventOperation::Insert,
        "update" => RowEventOperation::Update,
        "delete" => RowEventOperation::Delete,
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "invalid_operation".to_string(),
                    message: format!(
                        "Invalid operation '{}'. Must be 'insert', 'update', or 'delete'",
                        payload.operation
                    ),
                }),
            ));
        }
    };

    // Create REST request
    let row_event_request = RowEventRequest {
        src_table_name: src_table_name.clone(),
        operation,
        payload: payload.data,
        timestamp: SystemTime::now(),
    };
    let rest_event_request = EventRequest::RowRequest(row_event_request);

    state
        .backend
        .send_event_request(rest_event_request)
        .await
        .map_err(|e| {
            error!("Failed to send event request: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "ingestion_failed".to_string(),
                    message: format!("Failed to process request: {e}"),
                }),
            )
        })?;
    Ok(Json(IngestResponse {
        status: "success".to_string(),
        message: "Data queued for ingestion".to_string(),
        table: src_table_name,
        operation: payload.operation,
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
