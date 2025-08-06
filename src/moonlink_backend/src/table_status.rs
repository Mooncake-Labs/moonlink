/// Current table status.
#[derive(Clone, Debug, PartialEq)]
pub struct TableStatus {
    /// Database id.
    pub database_id: u32,
    /// Table id.
    pub table_id: u32,
    /// Mooncake table commit LSN.
    pub commit_lsn: u64,
    /// Iceberg flush LSN.
    pub flush_lsn: Option<u64>,
    /// Iceberg warehouse location.
    pub iceberg_warehouse_location: String,
}
