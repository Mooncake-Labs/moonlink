use moonlink::{AccessorConfig, IcebergTableConfig, MoonlinkTableConfig, StorageConfig, WalConfig};

#[cfg(feature = "storage-s3")]
pub fn get_s3_moonlink_table_config(database: &str, table: &str) -> MoonlinkTableConfig {
    let iceberg_storage = StorageConfig::S3 {
        access_key_id: "access-key".to_string(),
        secret_access_key: "secret".to_string(),
        region: "us-west-2".to_string(),
        bucket: "moonlink-iceberg".to_string(),
        endpoint: None,
    };
    let wal_storage = StorageConfig::S3 {
        access_key_id: "access-key-wal".to_string(),
        secret_access_key: "secret-wal".to_string(),
        region: "us-east-1".to_string(),
        bucket: "moonlink-wal".to_string(),
        endpoint: None,
    };
    let wal_accessor = AccessorConfig::new_with_storage_config(wal_storage);
    MoonlinkTableConfig {
        iceberg_table_config: IcebergTableConfig {
            namespace: vec!["namespace".to_string()],
            table_name: "table".to_string(),
            accessor_config: AccessorConfig::new_with_storage_config(iceberg_storage),
        },
        wal_table_config: WalConfig::new(wal_accessor, &format!("{database}.{table}")),
        ..Default::default()
    }
}

#[cfg(feature = "storage-gcs")]
pub fn get_gcs_moonlink_table_config(database: &str, table: &str) -> MoonlinkTableConfig {
    let iceberg_storage = StorageConfig::Gcs {
        project: "proj-ice".to_string(),
        region: "us-central1".to_string(),
        bucket: "moonlink-iceberg".to_string(),
        access_key_id: "access-key".to_string(),
        secret_access_key: "secret".to_string(),
        endpoint: None,
        disable_auth: false,
        write_option: None,
    };
    let wal_storage = StorageConfig::Gcs {
        project: "proj-wal".to_string(),
        region: "europe-west1".to_string(),
        bucket: "moonlink-wal".to_string(),
        access_key_id: "access-key-wal".to_string(),
        secret_access_key: "secret-wal".to_string(),
        endpoint: None,
        disable_auth: false,
        write_option: None,
    };
    let wal_accessor = AccessorConfig::new_with_storage_config(wal_storage);
    MoonlinkTableConfig {
        iceberg_table_config: IcebergTableConfig {
            namespace: vec!["namespace".to_string()],
            table_name: "table".to_string(),
            accessor_config: AccessorConfig::new_with_storage_config(iceberg_storage),
        },
        wal_table_config: WalConfig::new(wal_accessor, &format!("{database}.{table}")),
        ..Default::default()
    }
}
