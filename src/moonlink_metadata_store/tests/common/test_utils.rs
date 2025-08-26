use moonlink::{AccessorConfig, IcebergTableConfig, MoonlinkTableConfig, StorageConfig, WalConfig};

/// Test utils for postgres metadata storage tests.
///
/// Create a moonlink table config for test.
#[allow(dead_code)]
pub(crate) fn get_moonlink_table_config() -> MoonlinkTableConfig {
    let iceberg_accessor_config =
        AccessorConfig::new_with_storage_config(StorageConfig::FileSystem {
            root_directory: "/tmp/test_warehouse_uri".to_string(),
            atomic_write_dir: None,
        });
    let wal_accessor_config = AccessorConfig::new_with_storage_config(StorageConfig::FileSystem {
        root_directory: "/tmp/test_wal_uri".to_string(),
        atomic_write_dir: None,
    });
    MoonlinkTableConfig {
        iceberg_table_config: IcebergTableConfig {
            namespace: vec!["namespace".to_string()],
            table_name: "table".to_string(),
            catalog: moonlink::IcebergCatalogConfig::File {
                accessor_config: iceberg_accessor_config,
            },
        },
        wal_table_config: WalConfig::new(wal_accessor_config, "dst-database.dst-schema.dst-table"),
        ..Default::default()
    }
}
