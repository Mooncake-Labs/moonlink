use iceberg::spec::TableMetadata;

use crate::storage::table::common::{MOONCAKE_LOOKUP_KEY_VERSION, MOONCAKE_TABLE_FLUSH_LSN};
use iceberg::Result as IcebergResult;

/// This file contains util functions on iceberg snapshot.
///
/// Moonlink snapshot properties.
pub(super) struct SnapshotProperty {
    /// Iceberg flush LSN.
    pub(super) flush_lsn: Option<u64>,
    /// Lookup-key hash version of the persisted file indices; None for tables written before the
    /// property existed.
    pub(super) lookup_key_version: Option<u32>,
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
        flush_lsn = Some(lsn.parse().map_err(|_| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("invalid {MOONCAKE_TABLE_FLUSH_LSN} snapshot property: {lsn}"),
            )
        })?);
    }
    let lookup_key_version = match snapshot_summary
        .additional_properties
        .get(MOONCAKE_LOOKUP_KEY_VERSION)
    {
        Some(v) => Some(v.parse().map_err(|_| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("invalid {MOONCAKE_LOOKUP_KEY_VERSION} snapshot property: {v}"),
            )
        })?),
        None => None,
    };
    Ok(SnapshotProperty {
        flush_lsn,
        lookup_key_version,
    })
}
