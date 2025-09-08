use moonlink::StorageConfig;

use std::time::SystemTime;
use tokio::sync::mpsc;

/// ======================
/// Row event request
/// ======================
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowEventOperation {
    Insert,
    Upsert,
    Delete,
}

#[derive(Debug, Clone)]
pub enum IngestRequestPayload {
    Json(serde_json::Value),
    Protobuf(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct RowEventRequest {
    pub src_table_name: String,
    pub operation: RowEventOperation,
    pub payload: IngestRequestPayload,
    pub timestamp: SystemTime,
    /// An optional channel for commit LSN, used to synchronize request completion.
    /// TODO(hjiang): Handle error propagation.
    pub tx: Option<mpsc::Sender<u64>>,
}

/// ======================
/// File event request
/// ======================
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileEventOperation {
    /// Insert by rows.
    Insert,
    /// Upload by files.
    Upload,
}

#[derive(Debug, Clone)]
pub struct FileEventRequest {
    /// Src table name.
    pub src_table_name: String,
    /// File event operation.
    pub operation: FileEventOperation,
    /// Storage config, which provides access to storage backend.
    pub storage_config: StorageConfig,
    /// Parquet files to upload, which will be processed in order.
    pub files: Vec<String>,
    /// An optional channel for commit LSN, used to synchronize request completion.
    /// TODO(hjiang): Handle error propagation.
    pub tx: Option<mpsc::Sender<u64>>,
}

/// ======================
/// Table snapshot request
/// ======================
///
#[derive(Debug, Clone)]
pub struct SnapshotRequest {
    /// Src table name.
    pub src_table_name: String,
    /// Requested LSN.
    pub lsn: u64,
    /// Channel used to synchronize snapshot completion.
    pub tx: mpsc::Sender<u64>,
}

/// ======================
/// Table flush request
/// ======================
///
#[derive(Debug, Clone)]
pub struct FlushRequest {
    /// Src table name.
    pub src_table_name: String,
    /// Requested LSN.
    pub lsn: u64,
    /// Channel used to synchronize flush completion.
    pub tx: mpsc::Sender<u64>,
}

/// ======================
/// Event request
/// ======================
///
#[derive(Debug, Clone)]
pub enum EventRequest {
    RowRequest(RowEventRequest),
    FileRequest(FileEventRequest),
    SnapshotRequest(SnapshotRequest),
    FlushRequest(FlushRequest),
}

impl EventRequest {
    /// Get event compleion receiver.
    pub fn get_request_tx(&self) -> Option<mpsc::Sender<u64>> {
        match &self {
            EventRequest::RowRequest(req) => req.tx.clone(),
            EventRequest::FileRequest(req) => req.tx.clone(),
            EventRequest::SnapshotRequest(req) => Some(req.tx.clone()),
            EventRequest::FlushRequest(req) => Some(req.tx.clone()),
        }
    }
}
