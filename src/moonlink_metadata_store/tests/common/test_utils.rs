use moonlink::{AccessorConfig, IcebergTableConfig, MoonlinkTableConfig, StorageConfig};

/// Test utils for postgres metadata storage tests.
///
/// Create a moonlink table config for test.
pub(crate) fn get_moonlink_table_config() -> MoonlinkTableConfig {
    MoonlinkTableConfig {
        iceberg_table_config: IcebergTableConfig {
            namespace: vec!["namespace".to_string()],
            table_name: "table".to_string(),
            accessor_config: AccessorConfig::new_with_storage_config(StorageConfig::FileSystem {
                root_directory: "/tmp/test_warehouse_uri".to_string(),
            }),
        },
        ..Default::default()
    }
}
