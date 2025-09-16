use crate::error::Result;
use moonlink::row::IdentityProp;
use moonlink::{
    DataCompactionConfig, DiskSliceWriterConfig, FileIndexMergeConfig, IcebergPersistenceConfig,
    IcebergTableConfig, MooncakeTableConfig, MoonlinkTableConfig, WalConfig,
};
/// This module contains util functions related to moonlink config.
use serde::{Deserialize, Serialize};

/// Struct for mooncake table config.
/// Notice it's a subset of [`MooncakeTableConfig`] since we want to keep things persisted minimum.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct MooncakeTableConfigForPersistence {
    /// Number of batch records which decides when to flush records from MemSlice to disk.
    #[serde(default = "MooncakeTableConfig::default_mem_slice_size")]
    mem_slice_size: usize,

    /// Number of new deletion records which decides whether to create a new mooncake table snapshot.
    #[serde(default = "MooncakeTableConfig::default_snapshot_deletion_record_count")]
    snapshot_deletion_record_count: usize,

    /// Max number of rows in each record batch within MemSlice.
    #[serde(default = "MooncakeTableConfig::default_batch_size")]
    batch_size: usize,

    /// Disk slice parquet file flush threshold.
    #[serde(default = "MooncakeTableConfig::default_disk_slice_parquet_file_size")]
    disk_slice_parquet_file_size: usize,

    /// Config for data compaction.
    #[serde(default)]
    data_compaction_config: DataCompactionConfig,

    /// Config for index merge.
    #[serde(default)]
    file_index_config: FileIndexMergeConfig,

    /// Config for iceberg persistence config.
    #[serde(default)]
    persistence_config: IcebergPersistenceConfig,

    /// Whether this is an append-only table (no indexes, no deletes).
    #[serde(default = "MoonlinkTableConfigForPersistence::default_append_only")]
    append_only: bool,

    /// Identity of a single row.
    #[serde(default = "MoonlinkTableConfigForPersistence::default_row_identity")]
    row_identity: IdentityProp,
}

impl MooncakeTableConfigForPersistence {
    /// Validate the config.
    /// Notice, persisted config should keep backward compatibility and forward compatibility, and ALWAYS be valid.
    fn validate(&self) {
        if self.append_only {
            assert_eq!(self.row_identity, IdentityProp::None);
        }
        if self.row_identity == IdentityProp::None {
            assert!(self.append_only);
        }
    }
}

/// Struct for moonlink table config.
/// Notice it's a subset of [`MoonlinkTableConfig`] since we want to keep things persisted minimum.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct MoonlinkTableConfigForPersistence {
    /// Mooncake table configuration.
    mooncake_table_config: MooncakeTableConfigForPersistence,
    /// Iceberg table configuration.
    iceberg_table_config: IcebergTableConfig,
    /// WAL configuration.
    wal_config: WalConfig,
}

impl MoonlinkTableConfigForPersistence {
    // Notice, default value for the table config should be a valid combination.
    const DEFAULT_APPEND_ONLY: bool = true;
    const DEFAULT_ROW_IDENTITY: IdentityProp = IdentityProp::None;

    pub fn default_append_only() -> bool {
        Self::DEFAULT_APPEND_ONLY
    }
    pub fn default_row_identity() -> IdentityProp {
        Self::DEFAULT_ROW_IDENTITY
    }

    /// Validate the config.
    /// Notice, persisted config should keep backward compatibility and forward compatibility, and ALWAYS be valid.
    fn validate(&self) {
        self.mooncake_table_config.validate();
    }

    /// Get mooncake table config from persisted moonlink config.
    fn get_mooncake_table_config(&self) -> MooncakeTableConfig {
        // Validate before exporting into mooncake table config.
        self.validate();

        MooncakeTableConfig {
            append_only: self.mooncake_table_config.append_only,
            row_identity: self.mooncake_table_config.row_identity.clone(),
            mem_slice_size: self.mooncake_table_config.mem_slice_size,
            snapshot_deletion_record_count: self
                .mooncake_table_config
                .snapshot_deletion_record_count,
            batch_size: self.mooncake_table_config.batch_size,
            disk_slice_writer_config: DiskSliceWriterConfig {
                parquet_file_size: self.mooncake_table_config.disk_slice_parquet_file_size,
                chaos_config: None,
            },
            persistence_config: self.mooncake_table_config.persistence_config.clone(),
            data_compaction_config: self.mooncake_table_config.data_compaction_config.clone(),
            file_index_config: self.mooncake_table_config.file_index_config.clone(),
            temp_files_directory: MooncakeTableConfig::DEFAULT_TEMP_FILE_DIRECTORY.to_string(),
        }
    }
}

/// Parse moonlink table config into json value to persist into postgres, and return the secret entry.
/// TODO(hjiang): Handle namespace better.
///
/// Returns:
/// - serialized json value of the persisted config
pub(crate) fn parse_moonlink_table_config(
    moonlink_table_config: MoonlinkTableConfig,
) -> Result<serde_json::Value> {
    // Serialize mooncake table config.
    let iceberg_table_config = moonlink_table_config.iceberg_table_config;
    let wal_config = moonlink_table_config.wal_table_config;
    let mooncake_config = moonlink_table_config.mooncake_table_config;
    let persisted = MoonlinkTableConfigForPersistence {
        iceberg_table_config,
        wal_config,
        mooncake_table_config: MooncakeTableConfigForPersistence {
            mem_slice_size: mooncake_config.mem_slice_size,
            snapshot_deletion_record_count: mooncake_config.snapshot_deletion_record_count,
            batch_size: mooncake_config.batch_size,
            disk_slice_parquet_file_size: mooncake_config
                .disk_slice_writer_config
                .parquet_file_size,
            data_compaction_config: mooncake_config.data_compaction_config.clone(),
            file_index_config: mooncake_config.file_index_config.clone(),
            persistence_config: mooncake_config.persistence_config.clone(),
            append_only: mooncake_config.append_only,
            row_identity: mooncake_config.row_identity,
        },
    };
    let config_json = serde_json::to_value(&persisted)?;

    Ok(config_json)
}

/// Deserialize json value to moonlink table config.
pub(crate) fn deserialize_moonlink_table_config(
    serialized_config: serde_json::Value,
) -> Result<MoonlinkTableConfig> {
    let parsed: MoonlinkTableConfigForPersistence = serde_json::from_value(serialized_config)?;
    let mooncake_table_config = parsed.get_mooncake_table_config();

    let moonlink_table_config = MoonlinkTableConfig {
        iceberg_table_config: parsed.iceberg_table_config,
        wal_table_config: parsed.wal_config,
        mooncake_table_config,
    };

    Ok(moonlink_table_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use moonlink::{MooncakeTableConfig, MoonlinkTableConfig};
    use serde_json::json;

    #[test]
    fn test_moonlink_table_config_serde() {
        let old_moonlink_table_config = MoonlinkTableConfig {
            iceberg_table_config: IcebergTableConfig::default(),
            mooncake_table_config: MooncakeTableConfig::default(),
            wal_table_config: WalConfig::default(),
        };
        let serialized_persisted_config =
            parse_moonlink_table_config(old_moonlink_table_config.clone()).unwrap();
        let new_moonlink_table_config =
            deserialize_moonlink_table_config(serialized_persisted_config).unwrap();
        assert_eq!(
            new_moonlink_table_config.mooncake_table_config,
            old_moonlink_table_config.mooncake_table_config
        );
        assert_eq!(
            new_moonlink_table_config.iceberg_table_config,
            old_moonlink_table_config.iceberg_table_config
        );
    }

    // Testing scenario: serialized json config only contains partial fields, check whether json deserialization succeeds, and populates default value correctly.
    #[test]
    fn test_mooncake_persisted_config_serde() {
        // Intentionally miss a few fields persisted config.
        let json_input = json!({
            "disk_slice_parquet_file_size": 22222,
            "data_compaction_config": {
                "min_data_file_to_compact": 10,
                "data_file_final_size": 123456
            },
            "file_index_config": {
                "min_file_indices_to_merge": 5,
                "index_block_final_size": 654321
            }
        });

        let actual_persisted_config: MooncakeTableConfigForPersistence =
            serde_json::from_value(json_input).unwrap();
        // Apart from assigned fields, all other fields should be assigned default value.
        let expected_persisted_config = MooncakeTableConfigForPersistence {
            // Mooncake table config.
            mem_slice_size: MooncakeTableConfig::default_mem_slice_size(),
            snapshot_deletion_record_count:
                MooncakeTableConfig::default_snapshot_deletion_record_count(),
            batch_size: MooncakeTableConfig::default_batch_size(),
            disk_slice_parquet_file_size: 22222,
            // Data compaction config.
            data_compaction_config: DataCompactionConfig {
                min_data_file_to_compact: 10,
                max_data_file_to_compact: DataCompactionConfig::default_max_data_file_to_compact(),
                data_file_final_size: 123456,
                data_file_deletion_percentage:
                    DataCompactionConfig::default_data_file_deletion_percentage(),
            },
            // Index merge config.
            file_index_config: FileIndexMergeConfig {
                min_file_indices_to_merge: 5,
                max_file_indices_to_merge: FileIndexMergeConfig::default_max_file_indices_to_merge(
                ),
                index_block_final_size: 654321,
            },
            // Iceberg persistence config.
            persistence_config: IcebergPersistenceConfig::default(),
            // Append-only config.
            append_only: true,
            // Row identity.
            row_identity: IdentityProp::None,
        };
        assert_eq!(actual_persisted_config, expected_persisted_config);
    }
}
