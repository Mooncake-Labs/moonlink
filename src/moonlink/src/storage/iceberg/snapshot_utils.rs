use iceberg::spec::TableMetadata;

use crate::storage::iceberg::iceberg_table_manager::{
    MOONCAKE_TABLE_FLUSH_LSN, MOONCAKE_WAL_METADATA,
};
use crate::storage::wal::iceberg_corresponding_wal_metadata::IcebergCorrespondingWalMetadata;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;

/// This file contains util functions on iceberg snapshot.
///
/// Moonlink snapshot properties.
pub(super) struct SnapshotProperty {
    /// Iceberg flush LSN.
    pub(super) flush_lsn: Option<u64>,
    /// WAL persisted metadata.
    pub(super) corresponding_wal_metadata: IcebergCorrespondingWalMetadata,
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

    if let Some(wal) = snapshot_summary
        .additional_properties
        .get(MOONCAKE_WAL_METADATA)
    {
        let parsed_wal = serde_json::from_str(wal).map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                format!("failed to parse WAL metadata {wal}: {e:?}"),
            )
        })?;
        Ok(SnapshotProperty {
            flush_lsn,
            corresponding_wal_metadata: parsed_wal,
        })
    } else {
        Err(IcebergError::new(
            iceberg::ErrorKind::DataInvalid,
            "WAL metadata not found in snapshot summary.",
        ))
    }
}
