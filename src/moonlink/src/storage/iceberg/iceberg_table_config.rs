use crate::{storage::filesystem::accessor_config::AccessorConfig, StorageConfig};

#[derive(Clone, Debug, PartialEq)]
pub struct IcebergTableConfig {
    /// Namespace for the iceberg table.
    pub namespace: Vec<String>,
    /// Iceberg table name.
    pub table_name: String,
    // Accessor config.
    pub accessor_config: AccessorConfig,
}

impl IcebergTableConfig {
    const DEFAULT_WAREHOUSE_URI: &str = "/tmp/moonlink_iceberg";
    const DEFAULT_NAMESPACE: &str = "namespace";
    const DEFAULT_TABLE: &str = "table";
}

impl Default for IcebergTableConfig {
    fn default() -> Self {
        let storage_config = StorageConfig::FileSystem {
            root_directory: Self::DEFAULT_WAREHOUSE_URI.to_string(),
            // There's only one iceberg writer per-table, no need for atomic write feature.
            atomic_write_dir: None,
        };
        Self {
            namespace: vec![Self::DEFAULT_NAMESPACE.to_string()],
            table_name: Self::DEFAULT_TABLE.to_string(),
            accessor_config: AccessorConfig::new_with_storage_config(storage_config),
        }
    }
}
