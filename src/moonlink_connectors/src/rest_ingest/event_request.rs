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
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct RowEventRequest {
    pub src_table_name: String,
    pub operation: RowEventOperation,
    pub payload: serde_json::Value,
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
/// Event request
/// ======================
///
#[derive(Debug, Clone)]
pub enum EventRequest {
    RowRequest(RowEventRequest),
    FileRequest(FileEventRequest),
}

impl EventRequest {
    /// Get event compleion receiver.
    pub fn get_request_tx(&self) -> Option<mpsc::Sender<u64>> {
        match &self {
            EventRequest::RowRequest(req) => req.tx.clone(),
            EventRequest::FileRequest(req) => req.tx.clone(),
        }
    }
}
