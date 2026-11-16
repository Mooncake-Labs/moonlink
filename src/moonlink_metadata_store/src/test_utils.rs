#[cfg(feature = "storage-s3")]
use moonlink::{
    AccessorConfig, IcebergCatalogConfig, IcebergTableConfig, MoonlinkTableConfig, StorageConfig,
};

#[cfg(feature = "storage-s3")]
pub fn get_s3_moonlink_table_config() -> MoonlinkTableConfig {
    let iceberg_storage = StorageConfig::S3 {
        access_key_id: "access-key".to_string(),
        secret_access_key: "secret".to_string(),
        region: "us-west-2".to_string(),
        bucket: "moonlink-iceberg".to_string(),
        endpoint: None,
    };
    MoonlinkTableConfig {
        iceberg_table_config: IcebergTableConfig {
            namespace: vec!["namespace".to_string()],
            table_name: "table".to_string(),
            data_accessor_config: AccessorConfig::new_with_storage_config(iceberg_storage.clone()),
            metadata_accessor_config: IcebergCatalogConfig::File {
                accessor_config: AccessorConfig::new_with_storage_config(iceberg_storage.clone()),
            },
        },
        ..Default::default()
    }
}
