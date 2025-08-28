use std::path::Path;

use crate::{Error, Result};
use moonlink::row::IdentityProp;
use moonlink::MooncakeTableId;
use moonlink::{
    AccessorConfig, DataCompactionConfig, FileIndexMergeConfig, IcebergTableConfig,
    MooncakeTableConfig, MoonlinkTableConfig, StorageConfig, WalConfig,
};
/// Configuration on table creation.
use serde::{Deserialize, Serialize};

/// Mooncake table config.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MooncakeConfig {
    /// Whether background regular index merge is enabled.
    #[serde(default)]
    pub skip_index_merge: bool,
    /// Whether background regular data compaction is enabled.
    #[serde(default)]
    pub skip_data_compaction: bool,
    /// Whether this is an append-only table (no indexes, no deletes).
    #[serde(default = "MooncakeConfig::default_append_only")]
    pub append_only: bool,
    /// Row identity of the table.
    #[serde(default = "MooncakeConfig::default_row_identity")]
    pub row_identity: IdentityProp,
}

impl Default for MooncakeConfig {
    fn default() -> Self {
        Self {
            skip_index_merge: false,
            skip_data_compaction: false,
            append_only: MooncakeConfig::default_append_only(),
            row_identity: MooncakeConfig::default_row_identity(),
        }
    }
}

impl MooncakeConfig {
    // Notice, default value for the table config should be a valid combination.
    const DEFAULT_APPEND_ONLY: bool = true;
    const DEFAULT_ROW_IDENTITY: IdentityProp = IdentityProp::None;

    pub fn default_append_only() -> bool {
        Self::DEFAULT_APPEND_ONLY
    }
    pub fn default_row_identity() -> IdentityProp {
        Self::DEFAULT_ROW_IDENTITY
    }

    /// Return whether config is valid.
    pub fn is_valid(&self) -> bool {
        if self.append_only && self.row_identity != IdentityProp::None {
            return false;
        }
        if self.row_identity == IdentityProp::None && !self.append_only {
            return false;
        }
        true
    }

    /// Convert to mooncake table config.
    pub(crate) fn take_as_mooncake_table_config(
        self,
        temp_files_dir: String,
    ) -> MooncakeTableConfig {
        let index_merge_config = if self.skip_index_merge {
            FileIndexMergeConfig::disabled()
        } else {
            FileIndexMergeConfig::enabled()
        };
        let data_compaction_config = if self.skip_data_compaction {
            DataCompactionConfig::disabled()
        } else {
            DataCompactionConfig::enabled()
        };

        let mut mooncake_table_config = MooncakeTableConfig::new(temp_files_dir);
        mooncake_table_config.file_index_config = index_merge_config;
        mooncake_table_config.data_compaction_config = data_compaction_config;
        mooncake_table_config.append_only = self.append_only;
        mooncake_table_config.row_identity = self.row_identity;
        mooncake_table_config
    }
}

/// Mooncake table configuration specified at creation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TableConfig {
    /// Mooncake table configuration.
    #[serde(rename = "mooncake")]
    #[serde(default)]
    pub mooncake_config: MooncakeConfig,

    /// Iceberg storage config.
    #[serde(rename = "iceberg")]
    #[serde(default)]
    pub iceberg_config: Option<AccessorConfig>,

    /// WAL storage config.
    #[serde(rename = "wal")]
    #[serde(default)]
    pub wal_config: Option<AccessorConfig>,
}

impl TableConfig {
    /// Return whether the config is valid.
    pub fn is_valid(&self) -> bool {
        if !self.mooncake_config.is_valid() {
            return false;
        }
        true
    }

    /// Convert table config from serialized plain json string.
    pub fn from_json_or_default(json: &str, default_table_directory: &str) -> Result<Self> {
        let mut config: TableConfig = serde_json::from_str(json)?;
        if config.iceberg_config.is_none() {
            let storage_config = StorageConfig::FileSystem {
                root_directory: default_table_directory.to_string(),
                // By default disable atomic write option.
                atomic_write_dir: None,
            };
            config.iceberg_config = Some(AccessorConfig::new_with_storage_config(storage_config));
        }
        if config.wal_config.is_none() {
            let storage_config =
                WalConfig::default_storage_config_local(Path::new(default_table_directory));
            config.wal_config = Some(AccessorConfig::new_with_storage_config(storage_config));
        }

        // Check whehther config is valid.
        if !config.is_valid() {
            // WARNING: table config could contain sensitive information like secrets, we should use customized stringify function, which hides certain information, instead of directly using the raw json string.
            return Err(Error::invalid_config(format!(
                "Table config {config:?} is invalid"
            )));
        }

        Ok(config)
    }

    /// Convert to moonlink config.
    pub(crate) fn take_as_moonlink_config(
        self,
        temp_files_dir: String,
        mooncake_table_id: &MooncakeTableId,
    ) -> MoonlinkTableConfig {
        MoonlinkTableConfig {
            mooncake_table_config: self
                .mooncake_config
                .take_as_mooncake_table_config(temp_files_dir),
            iceberg_table_config: IcebergTableConfig {
                namespace: vec![mooncake_table_id.database.clone()],
                table_name: mooncake_table_id.table.clone(),
                data_accessor_config: self.iceberg_config.clone().unwrap(),
                metadata_accessor_config: moonlink::IcebergCatalogConfig::File {
                    accessor_config: self.iceberg_config.clone().unwrap(),
                },
            },
            wal_table_config: WalConfig::new(
                self.wal_config.unwrap(),
                &mooncake_table_id.to_string(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_config_from_empty_json() {
        let actual_table_config =
            TableConfig::from_json_or_default("{}", /*default_table_directory=*/ "/tmp/path")
                .unwrap();
        let expected_table_config = TableConfig {
            mooncake_config: MooncakeConfig {
                skip_index_merge: false,
                skip_data_compaction: false,
                append_only: true,
                row_identity: IdentityProp::None,
            },
            iceberg_config: Some(AccessorConfig::new_with_storage_config(
                moonlink::StorageConfig::FileSystem {
                    root_directory: "/tmp/path".to_string(),
                    atomic_write_dir: None,
                },
            )),
            wal_config: Some(AccessorConfig::new_with_storage_config(
                moonlink::StorageConfig::FileSystem {
                    root_directory: "/tmp/path".to_string(),
                    atomic_write_dir: None,
                },
            )),
        };
        assert_eq!(actual_table_config, expected_table_config);
    }

    #[test]
    fn test_table_config_from_valid_json() {
        let serialized = r#"
            {
                "mooncake": {
                    "skip_index_merge": true
                },
                "iceberg": {
                    "storage_config": {
                        "fs": {
                            "root_directory": "/tmp"
                        }
                    }
                },
                "wal": {
                    "storage_config": {
                        "fs": {
                            "root_directory": "/tmp/wal"
                        }
                    }
                }
            }
        "#;

        // Deserialize and check.
        let actual_table_config = TableConfig::from_json_or_default(
            serialized,
            /*default_table_directory=*/ "/tmp/path",
        )
        .unwrap();
        let expected_table_config = TableConfig {
            mooncake_config: MooncakeConfig {
                skip_index_merge: true,
                skip_data_compaction: false,
                append_only: true,
                row_identity: IdentityProp::None,
            },
            iceberg_config: Some(AccessorConfig::new_with_storage_config(
                moonlink::StorageConfig::FileSystem {
                    root_directory: "/tmp".to_string(),
                    atomic_write_dir: None,
                },
            )),
            wal_config: Some(AccessorConfig::new_with_storage_config(
                moonlink::StorageConfig::FileSystem {
                    root_directory: "/tmp/wal".to_string(),
                    atomic_write_dir: None,
                },
            )),
        };
        assert_eq!(expected_table_config, actual_table_config);
    }

    #[test]
    #[cfg(feature = "storage-gcs")]
    fn test_table_config_from_valid_json_with_gcs() {
        let serialized = r#"
            {
                "mooncake": {
                    "skip_index_merge": true
                },
                "iceberg": {
                    "storage_config": {
                        "gcs": {
                            "project": "gcs-proj",
                            "region": "us-west1",
                            "bucket": "moonlink",
                            "access_key_id": "access-key",
                            "secret_access_key": "secret"
                        }
                    }
                },
                "wal": {
                    "storage_config": {
                        "gcs": {
                            "project": "gcs-proj",
                            "region": "us-west1",
                            "bucket": "moonlink-wal",
                            "access_key_id": "access-key-wal",
                            "secret_access_key": "secret-wal"
                        }
                    }
                }
            }
        "#;

        // Deserialize and check.
        let actual_table_config = TableConfig::from_json_or_default(
            serialized,
            /*default_table_directory=*/ "/tmp/path",
        )
        .unwrap();
        let expected_table_config = TableConfig {
            mooncake_config: MooncakeConfig {
                skip_index_merge: true,
                skip_data_compaction: false,
                append_only: true,
                row_identity: IdentityProp::None,
            },
            iceberg_config: Some(AccessorConfig::new_with_storage_config(
                moonlink::StorageConfig::Gcs {
                    project: "gcs-proj".to_string(),
                    region: "us-west1".to_string(),
                    bucket: "moonlink".to_string(),
                    access_key_id: "access-key".to_string(),
                    secret_access_key: "secret".to_string(),
                    endpoint: None,
                    disable_auth: false,
                    write_option: None,
                },
            )),
            wal_config: Some(AccessorConfig::new_with_storage_config(
                moonlink::StorageConfig::Gcs {
                    project: "gcs-proj".to_string(),
                    region: "us-west1".to_string(),
                    bucket: "moonlink-wal".to_string(),
                    access_key_id: "access-key-wal".to_string(),
                    secret_access_key: "secret-wal".to_string(),
                    endpoint: None,
                    disable_auth: false,
                    write_option: None,
                },
            )),
        };
        assert_eq!(expected_table_config, actual_table_config);
    }

    #[test]
    #[cfg(feature = "storage-s3")]
    fn test_table_config_from_valid_json_with_s3() {
        let serialized = r#"
            {
                "mooncake": {
                    "skip_index_merge": true
                },
                "iceberg": {
                    "storage_config": {
                        "s3": {
                            "region": "us-west1",
                            "bucket": "moonlink",
                            "access_key_id": "access-key",
                            "secret_access_key": "secret"
                        }
                    }
                },
                "wal": {
                    "storage_config": {
                        "s3": {
                            "region": "us-west1",
                            "bucket": "moonlink-wal",
                            "access_key_id": "access-key-wal",
                            "secret_access_key": "secret-wal"
                        }
                    }
                }
            }
        "#;

        // Deserialize and check.
        let actual_table_config = TableConfig::from_json_or_default(
            serialized,
            /*default_table_directory=*/ "/tmp/path",
        )
        .unwrap();
        let expected_table_config = TableConfig {
            mooncake_config: MooncakeConfig {
                skip_index_merge: true,
                skip_data_compaction: false,
                append_only: true,
                row_identity: IdentityProp::None,
            },
            iceberg_config: Some(AccessorConfig::new_with_storage_config(
                moonlink::StorageConfig::S3 {
                    region: "us-west1".to_string(),
                    bucket: "moonlink".to_string(),
                    access_key_id: "access-key".to_string(),
                    secret_access_key: "secret".to_string(),
                    endpoint: None,
                },
            )),
            wal_config: Some(AccessorConfig::new_with_storage_config(
                moonlink::StorageConfig::S3 {
                    region: "us-west1".to_string(),
                    bucket: "moonlink-wal".to_string(),
                    access_key_id: "access-key-wal".to_string(),
                    secret_access_key: "secret-wal".to_string(),
                    endpoint: None,
                },
            )),
        };
        assert_eq!(expected_table_config, actual_table_config);
    }

    #[test]
    fn test_table_config_with_append_only() {
        let serialized = r#"
            {
                "mooncake": {
                    "append_only": true,
                    "skip_index_merge": true,
                    "skip_data_compaction": true
                }
            }
        "#;

        // Deserialize and check.
        let actual_table_config = TableConfig::from_json_or_default(
            serialized,
            /*default_table_directory=*/ "/tmp/path",
        )
        .unwrap();
        let expected_table_config = TableConfig {
            mooncake_config: MooncakeConfig {
                skip_index_merge: true,
                skip_data_compaction: true,
                append_only: true,
                row_identity: IdentityProp::None,
            },
            iceberg_config: Some(AccessorConfig::new_with_storage_config(
                moonlink::StorageConfig::FileSystem {
                    root_directory: "/tmp/path".to_string(),
                    atomic_write_dir: None,
                },
            )),
            wal_config: Some(AccessorConfig::new_with_storage_config(
                moonlink::StorageConfig::FileSystem {
                    root_directory: "/tmp/path".to_string(),
                    atomic_write_dir: None,
                },
            )),
        };
        assert_eq!(expected_table_config, actual_table_config);
    }
}
