use serde::{Deserialize, Serialize};

/// Current table status.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableStatus {
    /// Mooncake database name.
    pub database: String,
    /// Mooncake table name.
    pub table: String,
    /// Mooncake table commit LSN.
    pub commit_lsn: u64,
    /// Iceberg flush LSN.
    pub flush_lsn: Option<u64>,
    /// Cardinality.
    pub cardinality: u64,
    /// Iceberg warehouse location.
    pub iceberg_warehouse_location: String,
}
