/// Mooncake table states.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TableSnapshotStatus {
    /// Cardinality.
    pub cardinality: u64,
    /// Mooncake table commit LSN.
    pub commit_lsn: u64,
    /// Iceberg flush LSN.
    pub flush_lsn: Option<u64>,
    /// Iceberg warehouse location.
    pub iceberg_warehouse_location: String,
}
