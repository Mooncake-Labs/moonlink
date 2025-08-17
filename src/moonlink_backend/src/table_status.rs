/// Current table status.
#[derive(Clone, Debug, PartialEq)]
pub struct TableStatus {
    /// Mooncake database name.
    pub mooncake_database: String,
    /// Mooncake table name.
    pub mooncake_table: String,
    /// Mooncake table commit LSN.
    pub commit_lsn: u64,
    /// Iceberg flush LSN.
    pub flush_lsn: Option<u64>,
    /// Iceberg warehouse location.
    pub iceberg_warehouse_location: String,
}
