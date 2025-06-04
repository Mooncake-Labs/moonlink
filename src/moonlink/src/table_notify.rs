use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::IcebergSnapshotResult;
use crate::Result;

/// Completion notifications for mooncake table, including snapshot creation and compaction, etc.
pub(crate) enum TableNotify {
    /// Mooncake snapshot completes.
    MooncakeTableSnapshot {
        /// Mooncake snapshot LSN.
        lsn: u64,
        /// Payload used to create an iceberg snapshot.
        iceberg_snapshot_payload: Option<IcebergSnapshotPayload>,
    },
    /// Iceberg snapshot completes.
    IcebergSnapshot {
        /// Result for iceberg snapshot.
        iceberg_snapshot_result: Result<IcebergSnapshotResult>,
    },
}
