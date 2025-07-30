use crate::error::Result;
use moonlink::{
    AccessorConfig, DataCompactionConfig, FileIndexMergeConfig, IcebergPersistenceConfig,
    IcebergTableConfig, MooncakeTableConfig, MoonlinkSecretType, MoonlinkTableConfig,
    MoonlinkTableSecret, StorageConfig,
};
/// This module contains util functions related to moonlink config.
use serde::{Deserialize, Serialize};
#[cfg(any(feature = "storage-gcs", feature = "storage-s3"))]
use url::Url;

/// Struct for iceberg table config.
/// Notice it's a subset of [`IcebergTableConfig`] since we want to keep things persisted minimum.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct IcebergTableConfigForPersistence {
    /// Table warehouse location.
    warehouse_uri: String,
    /// Namespace for the iceberg table.
    namespace: String,
    /// Iceberg table name.
    table_name: String,
}

impl IcebergTableConfigForPersistence {
    /// Get bucket for iceberg table config, only applies to object storage backend.
    #[cfg(any(feature = "storage-gcs", feature = "storage-s3"))]
    fn get_bucket_name(&self) -> Option<String> {
        if let Ok(url) = Url::parse(&self.warehouse_uri) {
            return Some(url.host_str()?.to_string());
        }
        None
    }
}

/// Struct for mooncake table config.
/// Notice it's a subset of [`MooncakeTableConfig`] since we want to keep things persisted minimum.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct MooncakeTableConfigForPersistence {
    /// Number of batch records which decides when to flush records from MemSlice to disk.
    mem_slice_size: usize,
    /// Number of new deletion records which decides whether to create a new mooncake table snapshot.
    snapshot_deletion_record_count: usize,
    /// Max number of rows in each record batch within MemSlice.
    batch_size: usize,
    /// Disk slice parquet file flush threshold.
    disk_slice_parquet_file_size: usize,
    /// Config for data compaction.
    data_compaction_config: DataCompactionConfig,
    /// Config for index merge.
    file_index_config: FileIndexMergeConfig,
    /// Config for iceberg persistence config.
    persistence_config: IcebergPersistenceConfig,
}

/// Struct for moonlink table config.
/// Notice it's a subset of [`MoonlinkTableConfig`] since we want to keep things persisted minimum.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct MoonlinkTableConfigForPersistence {
    /// Mooncake table configuration.
    mooncake_table_config: MooncakeTableConfigForPersistence,
    /// Iceberg table configuration.
    iceberg_table_config: IcebergTableConfigForPersistence,
}

impl MoonlinkTableConfigForPersistence {
    /// Get mooncake table config from persisted moonlink config.
    fn get_mooncake_table_config(&self) -> MooncakeTableConfig {
        MooncakeTableConfig {
            mem_slice_size: self.mooncake_table_config.mem_slice_size,
            snapshot_deletion_record_count: self
                .mooncake_table_config
                .snapshot_deletion_record_count,
            batch_size: self.mooncake_table_config.batch_size,
            disk_slice_parquet_file_size: self.mooncake_table_config.disk_slice_parquet_file_size,
            persistence_config: self.mooncake_table_config.persistence_config.clone(),
            data_compaction_config: self.mooncake_table_config.data_compaction_config.clone(),
            file_index_config: self.mooncake_table_config.file_index_config.clone(),
            temp_files_directory: MooncakeTableConfig::DEFAULT_TEMP_FILE_DIRECTORY.to_string(),
        }
    }
}

/// Parse moonlink table config into json value to persist into postgres, and return the secret entry.
/// TODO(hjiang): Handle namespace better.
pub(crate) fn parse_moonlink_table_config(
    moonlink_table_config: MoonlinkTableConfig,
) -> Result<(serde_json::Value, Option<MoonlinkTableSecret>)> {
    // Serialize mooncake table config.
    let iceberg_config = moonlink_table_config.iceberg_table_config;
    let mooncake_config = moonlink_table_config.mooncake_table_config;
    let persisted = MoonlinkTableConfigForPersistence {
        iceberg_table_config: IcebergTableConfigForPersistence {
            warehouse_uri: iceberg_config.accessor_config.get_root_path(),
            namespace: iceberg_config.namespace[0].to_string(),
            table_name: iceberg_config.table_name,
        },
        mooncake_table_config: MooncakeTableConfigForPersistence {
            mem_slice_size: mooncake_config.mem_slice_size,
            snapshot_deletion_record_count: mooncake_config.snapshot_deletion_record_count,
            batch_size: mooncake_config.batch_size,
            disk_slice_parquet_file_size: mooncake_config.disk_slice_parquet_file_size,
            data_compaction_config: mooncake_config.data_compaction_config.clone(),
            file_index_config: mooncake_config.file_index_config.clone(),
            persistence_config: mooncake_config.persistence_config.clone(),
        },
    };
    let config_json = serde_json::to_value(&persisted)?;

    // Extract table secret entry.
    let security_metadata_entry = iceberg_config
        .accessor_config
        .extract_security_metadata_entry();

    Ok((config_json, security_metadata_entry))
}

/// Recover filesystem config from persisted config and secret.
fn recover_storage_config(
    persisted_config: &MoonlinkTableConfigForPersistence,
    secret_entry: Option<MoonlinkTableSecret>,
) -> StorageConfig {
    if let Some(secret_entry) = secret_entry {
        match &secret_entry.secret_type {
            #[cfg(feature = "storage-gcs")]
            MoonlinkSecretType::Gcs => {
                return StorageConfig::Gcs {
                    project: secret_entry.project.unwrap(),
                    region: secret_entry.region.unwrap(),
                    bucket: persisted_config
                        .iceberg_table_config
                        .get_bucket_name()
                        .unwrap(),
                    access_key_id: secret_entry.key_id,
                    secret_access_key: secret_entry.secret,
                    endpoint: secret_entry.endpoint,
                    disable_auth: false,
                };
            }
            #[cfg(feature = "storage-s3")]
            MoonlinkSecretType::S3 => {
                return StorageConfig::S3 {
                    access_key_id: secret_entry.key_id,
                    secret_access_key: secret_entry.secret,
                    region: secret_entry.region.unwrap(),
                    bucket: persisted_config
                        .iceberg_table_config
                        .get_bucket_name()
                        .unwrap(),
                    endpoint: secret_entry.endpoint,
                };
            }
            #[cfg(feature = "storage-fs")]
            MoonlinkSecretType::FileSystem => {
                return StorageConfig::FileSystem {
                    root_directory: persisted_config.iceberg_table_config.warehouse_uri.clone(),
                };
            }
        }
    }
    StorageConfig::FileSystem {
        root_directory: persisted_config.iceberg_table_config.warehouse_uri.clone(),
    }
}

/// Deserialize json value to moonlink table config.
pub(crate) fn deserialze_moonlink_table_config(
    serialized_config: serde_json::Value,
    secret_entry: Option<MoonlinkTableSecret>,
) -> Result<MoonlinkTableConfig> {
    let parsed: MoonlinkTableConfigForPersistence = serde_json::from_value(serialized_config)?;
    let storage_config = recover_storage_config(&parsed, secret_entry);
    let mooncake_table_config = parsed.get_mooncake_table_config();

    let moonlink_table_config = MoonlinkTableConfig {
        iceberg_table_config: IcebergTableConfig {
            namespace: vec![parsed.iceberg_table_config.namespace],
            table_name: parsed.iceberg_table_config.table_name,
            accessor_config: AccessorConfig::new_with_storage_config(storage_config),
        },
        mooncake_table_config,
    };

    Ok(moonlink_table_config)
}

#[cfg(test)]
mod tests {
    use super::*;

    use moonlink::{MooncakeTableConfig, MoonlinkTableConfig};

    #[test]
    fn test_moonlink_table_config_serde() {
        let old_moonlink_table_config = MoonlinkTableConfig {
            iceberg_table_config: IcebergTableConfig::default(),
            mooncake_table_config: MooncakeTableConfig::default(),
        };
        let (serialized_persisted_config, secret_entry) =
            parse_moonlink_table_config(old_moonlink_table_config.clone()).unwrap();
        let new_moonlink_table_config =
            deserialze_moonlink_table_config(serialized_persisted_config, secret_entry).unwrap();
        assert_eq!(old_moonlink_table_config, new_moonlink_table_config);
    }

    #[cfg(any(feature = "storage-gcs", feature = "storage-s3"))]
    #[test]
    fn test_get_bucket_name() {
        // Test on S3 bucket.
        let config = IcebergTableConfigForPersistence {
            warehouse_uri: "s3://my-bucket-name/path/to/table".to_string(),
            namespace: "test_ns".to_string(),
            table_name: "test_table".to_string(),
        };
        assert_eq!(config.get_bucket_name(), Some("my-bucket-name".to_string()));

        // Test on GCS bucket.
        let config = IcebergTableConfigForPersistence {
            warehouse_uri: "gs://my-bucket-name/path/to/table".to_string(),
            namespace: "test_ns".to_string(),
            table_name: "test_table".to_string(),
        };
        assert_eq!(config.get_bucket_name(), Some("my-bucket-name".to_string()));
    }
}
