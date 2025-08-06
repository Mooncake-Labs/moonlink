use iceberg::spec::TableMetadata;

use crate::storage::iceberg::iceberg_table_manager::MOONCAKE_TABLE_FLUSH_LSN;
use iceberg::Result as IcebergResult;

/// This file contains util functions on iceberg snapshot.
///
/// Moonlink snapshot properties.
pub(super) struct SnapshotProperty {
    /// Iceberg flush LSN.
    pub(super) flush_lsn: Option<u64>,
}

/// Get moonlink customized snapshot
pub(super) fn get_snapshot_properties(
    table_metadata: &TableMetadata,
) -> IcebergResult<SnapshotProperty> {
    let current_snapshot = table_metadata.current_snapshot().unwrap();
    let snapshot_summary = current_snapshot.summary();

    // Extract flush LSN.
    let mut flush_lsn: Option<u64> = None;
    if let Some(lsn) = snapshot_summary
        .additional_properties
        .get(MOONCAKE_TABLE_FLUSH_LSN)
    {
        flush_lsn = Some(lsn.parse().unwrap());
    }
    Ok(SnapshotProperty { flush_lsn })
}
