use crate::row::MoonlinkRow;
use crate::storage::mooncake_table::DataCompactionPayload;
use crate::storage::mooncake_table::DataCompactionResult;
use crate::storage::mooncake_table::FileIndiceMergePayload;
use crate::storage::mooncake_table::FileIndiceMergeResult;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::IcebergSnapshotResult;

use crate::NonEvictableHandle;
use crate::Result;

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
        is_copied: bool,
    },
    /// Delete a row from the table
    Delete {
        row: MoonlinkRow,
        lsn: u64,
        xact_id: Option<u32>,
    },
    /// Commit all pending operations with a given LSN and xact_id
    Commit { lsn: u64, xact_id: Option<u32> },
    /// Abort current stream with given xact_id
    StreamAbort { xact_id: u32 },
    /// ==============================
    /// Test events
    /// ==============================
    ///
    /// Flush the table to disk
    Flush { lsn: u64 },
    /// Flush the transaction stream with given xact_id
    StreamFlush { xact_id: u32 },
    /// Shutdown the handler
    Shutdown,
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
    PeriodicalMooncakeTableSnapshot,
    /// Mooncake snapshot completes.
    MooncakeTableSnapshotResult {
        /// Mooncake snapshot LSN.
        lsn: u64,
        /// Payload used to create an iceberg snapshot.
        iceberg_snapshot_payload: Option<IcebergSnapshotPayload>,
        /// Payload used to trigger a data compaction.
        data_compaction_payload: Option<DataCompactionPayload>,
        /// Payload used to trigger an index merge.
        file_indice_merge_payload: Option<FileIndiceMergePayload>,
        /// Evicted object storage cache to delete.
        evicted_data_files_to_delete: Vec<String>,
    },
    /// Iceberg snapshot completes.
    IcebergSnapshotResult {
        /// Result for iceberg snapshot.
        iceberg_snapshot_result: Result<IcebergSnapshotResult>,
    },
    /// Index merge completes.
    IndexMergeResult {
        /// Result for index merge.
        index_merge_result: FileIndiceMergeResult,
    },
    /// Data compaction completes.
    DataCompactionResult {
        /// Result for data compaction.
        data_compaction_result: Result<DataCompactionResult>,
    },
    /// Read request completion.
    ReadRequestCompletion {
        /// Cache handles, which are pinned before query.
        cache_handles: Vec<NonEvictableHandle>,
    },
    /// Evicted data files to delete.
    EvictedDataFilesToDelete {
        /// Evicted data files by object storage cache.
        evicted_data_files: Vec<String>,
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
                    | TableEvent::Flush { .. }
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

    pub fn get_lsn_for_ingest_event(&self) -> Option<u64> {
        match self {
            TableEvent::Append { lsn, .. } => Some(*lsn),
            TableEvent::Delete { lsn, .. } => Some(*lsn),
            TableEvent::Commit { lsn, .. } => Some(*lsn),
            TableEvent::StreamAbort { .. } => None,
            TableEvent::Flush { lsn } => Some(*lsn),
            _ => None,
        }
    }
}
