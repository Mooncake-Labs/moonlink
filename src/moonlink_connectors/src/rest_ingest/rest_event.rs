use crate::rest_ingest::SrcTableId;
use crate::{rest_ingest::event_request::RowEventOperation, Result};
use moonlink::row::MoonlinkRow;
use moonlink::StorageConfig;

use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;

/// ======================
/// Rest event
/// ======================
///
#[derive(Debug, Clone)]
pub enum RestEvent {
    RowEvent {
        src_table_id: SrcTableId,
        operation: RowEventOperation,
        row: MoonlinkRow,
        lsn: u64,
        timestamp: SystemTime,
    },
    Commit {
        lsn: u64,
        timestamp: SystemTime,
    },
    FileInsertEvent {
        /// Source table id.
        src_table_id: SrcTableId,
        /// Used for file row insertion operation.
        table_events: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<Result<RestEvent>>>>,
    },
    FileUploadEvent {
        /// Source table id.
        src_table_id: SrcTableId,
        /// Storage config to access files to upload.
        /// Assume all files share the same config.
        storage_config: StorageConfig,
        /// Used to directly ingest into mooncake table.
        files: Vec<String>,
        /// LSN for the ingestion event.
        lsn: u64,
    },
    Snapshot {
        /// Source table id.
        src_table_id: SrcTableId,
        /// LSN to create snapshot.
        lsn: u64,
    },
    Flush {
        /// Source table id.
        src_table_id: SrcTableId,
    },
}

impl RestEvent {
    /// Get event LSN, if applicable.
    pub fn lsn(&self) -> Option<u64> {
        match &self {
            RestEvent::RowEvent { lsn, .. } => Some(*lsn),
            RestEvent::Commit { lsn, .. } => Some(*lsn),
            RestEvent::FileInsertEvent { .. } => None,
            RestEvent::FileUploadEvent { lsn, .. } => Some(*lsn),
            RestEvent::Snapshot { .. } => None,
            RestEvent::Flush { .. } => None,
        }
    }
}
