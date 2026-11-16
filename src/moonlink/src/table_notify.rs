use crate::row::MoonlinkRow;
use crate::storage::mooncake_table::snapshot::MooncakeSnapshotOutput;
use crate::storage::mooncake_table::DataCompactionPayload;
use crate::storage::mooncake_table::DataCompactionResult;
use crate::storage::mooncake_table::DiskSliceWriter;
use crate::storage::mooncake_table::FileIndiceMergePayload;
use crate::storage::mooncake_table::FileIndiceMergeResult;
use crate::storage::mooncake_table::PersistenceSnapshotPayload;
use crate::storage::mooncake_table::PersistenceSnapshotResult;
use crate::Result;
use crate::StorageConfig;

/// Table maintenance status.
#[derive(Clone, Debug)]
pub enum TableMaintenanceStatus<T> {
    /// Requested to skip table maintenance, so it's unknown whether there's maintenance payload.
    Unknown,
    /// Nothing to maintenance.
    Nothing,
    /// Table maintenance payload.
    Payload(T),
}
pub type IndexMergeMaintenanceStatus = TableMaintenanceStatus<FileIndiceMergePayload>;
pub type DataCompactionMaintenanceStatus = TableMaintenanceStatus<DataCompactionPayload>;

impl<T> TableMaintenanceStatus<T> {
    /// Return whether there's nothing to maintain.
    pub fn is_nothing(&self) -> bool {
        matches!(self, TableMaintenanceStatus::Nothing)
    }
    pub fn has_payload(&self) -> bool {
        matches!(self, TableMaintenanceStatus::Payload(_))
    }
    pub fn get_payload_reference(&self) -> Option<&T> {
        match self {
            TableMaintenanceStatus::Payload(payload) => Some(payload),
            _ => None,
        }
    }
    pub fn take_payload(self) -> Option<T> {
        match self {
            TableMaintenanceStatus::Payload(payload) => Some(payload),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct EvictedFiles {
    /// Evicted files by object storage cache to delete.
    pub files: Vec<String>,
}

impl std::fmt::Debug for EvictedFiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvictedFiles")
            .field("evicted files count", &self.files.len())
            .finish()
    }
}

/// Completion notifications for mooncake table, including snapshot creation and compaction, etc.
///
/// TODO(hjiang): Revisit whether we need to place the payload into box.
#[allow(clippy::large_enum_variant)]
/// Event types that can be processed by the TableHandler
#[derive(Clone, Debug)]
pub enum TableEvent {
    /// ==============================
    /// Replication events
    /// ==============================
    ///
    /// Append a row to the table
    Append {
        row: MoonlinkRow,
        xact_id: Option<u32>,
        lsn: u64,
    },
    /// Delete a row from the table
    Delete {
        row: MoonlinkRow,
        lsn: u64,
        xact_id: Option<u32>,
        delete_if_exists: bool,
    },
    /// Commit all pending operations with a given LSN and xact_id
    Commit { lsn: u64, xact_id: Option<u32> },
    /// Abort current stream with given xact_id
    StreamAbort { xact_id: u32 },
    FlushResult {
        /// Background event id.
        event_id: uuid::Uuid,
        /// Transaction ID
        xact_id: Option<u32>,
        /// Result for mem slice flush.
        flush_result: Option<Result<DiskSliceWriter>>,
    },

    /// ==============================
    /// Bulk ingestion events
    /// ==============================
    ///
    LoadFiles {
        /// Parquet files to directly load into mooncake table, without schema validation, index construction, etc.
        files: Vec<String>,
        /// Storage config to access files, assume all files share the same access.
        storage_config: StorageConfig,
        /// LSN for the bulk upload operation.
        lsn: u64,
    },

    /// ==============================
    /// Test events
    /// ==============================
    ///
    /// Commit and flush the table to disk
    CommitFlush { lsn: u64, xact_id: Option<u32> },
    /// Flush the transaction stream with given xact_id
    StreamFlush { xact_id: u32 },

    /// ==============================
    /// Interactive blocking events
    /// ==============================
    ///
    /// Force a mooncake and iceberg snapshot.
    /// - If [`lsn`] unassigned, will force snapshot on the latest committed LSN.
    ForceSnapshot { lsn: Option<u64> },
    /// There's at most one outstanding force table maintenance requests.
    ///
    /// Force a regular index merge operation.
    ForceRegularIndexMerge,
    /// Force a regular data compaction operation.
    ForceRegularDataCompaction,
    /// Force a full table maintenance operation.
    ForceFullMaintenance,
    /// Drop table.
    DropTable,
    /// Put the table into the errored state (requested by any layer, e.g. CDC).
    EnterErrored { reason: String },
    /// Every change up to `lsn` has been delivered to this table (all earlier events for it are
    /// ahead of this one in its channel). Lets a table with nothing left to persist report `lsn`
    /// as its flush LSN, so it does not hold the replication slot back.
    ReplicationProgress { lsn: u64 },
    /// Internal: an errored table stopped waiting for in-flight background tasks before dropping.
    ErroredDropTimeout,
    /// Internal: a background task of the table panicked.
    BackgroundTaskFailed { task: &'static str, message: String },
    /// Test-only: make the event loop panic.
    #[cfg(test)]
    PanicForTest,
    /// Test-only: how long an errored table waits for background tasks before dropping.
    #[cfg(test)]
    SetErroredDropWaitForTest { wait: std::time::Duration },
    /// Test-only: make the table's handler task die (panic), like an unexpected crash.
    #[cfg(any(test, feature = "test-utils"))]
    KillForTest,
    /// Test-only: how long garbage collection keeps superseded files when no read path reports
    /// live readers (harnesses that drive a table handler without a ReadStateManager).
    #[cfg(test)]
    SetGcGraceForTest { grace: std::time::Duration },
    /// Alter table,
    AlterTable { columns_to_drop: Vec<String> },
    /// Start initial table copy.
    /// `start_lsn` is the `pg_current_wal_lsn` when the initial copy starts.
    StartInitialCopy,
    /// Finish initial table copy and merge buffered changes.
    /// `start_lsn` is the `pg_current_wal_lsn` when the initial copy starts. We want this in FinishInitialCopy so we can set the commit LSN correctly.
    FinishInitialCopy { start_lsn: u64 },

    /// ==============================
    /// Table internal events
    /// ==============================
    ///
    /// Periodical mooncake snapshot.
    PeriodicalMooncakeTableSnapshot(uuid::Uuid),
    /// Mooncake snapshot completes.
    MooncakeTableSnapshotResult {
        /// Mooncake snapshot result.
        mooncake_snapshot_result: MooncakeSnapshotOutput,
    },
    /// Regular iceberg persistence.
    RegularIcebergSnapshot {
        /// Payload used to create a new iceberg snapshot.
        persistence_snapshot_payload: PersistenceSnapshotPayload,
    },
    /// Iceberg snapshot completes.
    PersistenceSnapshotResult {
        /// Result for iceberg snapshot.
        persistence_snapshot_result: Result<PersistenceSnapshotResult>,
    },
    /// Index merge completes.
    IndexMergeResult {
        /// Result for index merge.
        index_merge_result: Result<FileIndiceMergeResult>,
    },
    /// Data compaction completes.
    DataCompactionResult {
        /// Result for data compaction.
        data_compaction_result: Result<DataCompactionResult>,
    },
    /// Evicted files to delete.
    EvictedFilesToDelete {
        /// Evicted data files by object storage cache.
        evicted_files: EvictedFiles,
    },
}

impl TableEvent {
    pub fn is_ingest_event(&self) -> bool {
        #[cfg(test)]
        {
            matches!(
                self,
                TableEvent::Append { .. }
                    | TableEvent::Delete { .. }
                    | TableEvent::Commit { .. }
                    | TableEvent::StreamAbort { .. }
                    | TableEvent::CommitFlush { .. }
                    | TableEvent::StreamFlush { .. }
            )
        }
        #[cfg(not(test))]
        {
            matches!(
                self,
                TableEvent::Append { .. }
                    | TableEvent::Delete { .. }
                    | TableEvent::Commit { .. }
                    | TableEvent::StreamAbort { .. }
            )
        }
    }

    /// Whether current table event indicates a streaming write transaction.
    pub fn is_streaming_update(&self) -> bool {
        match &self {
            TableEvent::Append { xact_id, .. } => xact_id.is_some(),
            TableEvent::Delete { xact_id, .. } => xact_id.is_some(),
            TableEvent::StreamAbort { .. } => true,
            TableEvent::Commit { xact_id, .. } => xact_id.is_some(),
            TableEvent::CommitFlush { xact_id, .. } => xact_id.is_some(),
            TableEvent::StreamFlush { .. } => true,
            _ => false,
        }
    }

    pub fn get_lsn_for_ingest_event(&self) -> Option<u64> {
        match self {
            TableEvent::Append { lsn, .. } => Some(*lsn),
            TableEvent::Delete { lsn, .. } => Some(*lsn),
            TableEvent::Commit { lsn, .. } => Some(*lsn),
            TableEvent::StreamAbort { .. } => None,
            TableEvent::CommitFlush { lsn, .. } => Some(*lsn),
            _ => None,
        }
    }
}
