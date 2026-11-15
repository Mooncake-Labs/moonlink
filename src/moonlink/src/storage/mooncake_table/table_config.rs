use crate::storage::mooncake_table_config::MooncakeTableConfig;
use crate::storage::table::iceberg::iceberg_table_config::IcebergTableConfig;

/// Configuration including everything related to a column storage table.

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TableConfig {
    /// Mooncake table config.
    pub mooncake_table_config: MooncakeTableConfig,
    /// Iceberg table config.
    pub iceberg_table_config: IcebergTableConfig,
}
