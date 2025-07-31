use crate::Result;
use moonlink::{
    AccessorConfig as IcebergConfig, DataCompactionConfig, FileIndexMergeConfig,
    IcebergTableConfig, MooncakeTableConfig, MoonlinkTableConfig, StorageConfig,
};
/// Configuration on table creation.
use serde::{Deserialize, Serialize};

/// Default namespace for all iceberg tables.
const DEFAULT_ICEBERG_NAMESPACE: &str = "default";

/// Mooncake table config.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Default)]
pub struct MooncakeConfig {
    /// Whether background regular index merge is enabled.
    #[serde(default)]
    pub skip_index_merge: bool,
    /// Whether background regular data compaction is enabled.
    #[serde(default)]
    pub skip_data_compaction: bool,
}

impl MooncakeConfig {
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
    pub iceberg_config: Option<IcebergConfig>,
}

impl TableConfig {
    /// Convert table config from serialized plain json string.
    pub fn from_json_or_default(json: &str, default_table_directory: &str) -> Result<Self> {
        let mut config: TableConfig = serde_json::from_str(json)?;
        if config.iceberg_config.is_none() {
            let storage_config = StorageConfig::FileSystem {
                root_directory: default_table_directory.to_string(),
            };
            config.iceberg_config = Some(IcebergConfig::new_with_storage_config(storage_config));
        }
        Ok(config)
    }

    /// Convert to moonlink config.
    pub(crate) fn take_as_moonlink_config(
        self,
        temp_files_dir: String,
        mooncake_table_id: String,
    ) -> MoonlinkTableConfig {
        MoonlinkTableConfig {
            mooncake_table_config: self
                .mooncake_config
                .take_as_mooncake_table_config(temp_files_dir),
            iceberg_table_config: IcebergTableConfig {
                namespace: vec![DEFAULT_ICEBERG_NAMESPACE.to_string()],
                table_name: mooncake_table_id,
                accessor_config: self.iceberg_config.unwrap(),
            },
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
            },
            iceberg_config: Some(IcebergConfig::new_with_storage_config(
                moonlink::StorageConfig::FileSystem {
                    root_directory: "/tmp/path".to_string(),
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
                        "FileSystem": {
                            "root_directory": "/tmp"
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
            },
            iceberg_config: Some(IcebergConfig::new_with_storage_config(
                moonlink::StorageConfig::FileSystem {
                    root_directory: "/tmp".to_string(),
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
                        "Gcs": {
                            "project": "gcs-proj",
                            "region": "us-west1",
                            "bucket": "moonlink",
                            "access_key_id": "access-key",
                            "secret_access_key": "secret"
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
            },
            iceberg_config: Some(IcebergConfig::new_with_storage_config(
                moonlink::StorageConfig::Gcs {
                    project: "gcs-proj".to_string(),
                    region: "us-west1".to_string(),
                    bucket: "moonlink".to_string(),
                    access_key_id: "access-key".to_string(),
                    secret_access_key: "secret".to_string(),
                    endpoint: None,
                    disable_auth: false,
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
                        "S3": {
                            "region": "us-west1",
                            "bucket": "moonlink",
                            "access_key_id": "access-key",
                            "secret_access_key": "secret"
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
            },
            iceberg_config: Some(IcebergConfig::new_with_storage_config(
                moonlink::StorageConfig::S3 {
                    region: "us-west1".to_string(),
                    bucket: "moonlink".to_string(),
                    access_key_id: "access-key".to_string(),
                    secret_access_key: "secret".to_string(),
                    endpoint: None,
                },
            )),
        };
        assert_eq!(expected_table_config, actual_table_config);
    }
}
