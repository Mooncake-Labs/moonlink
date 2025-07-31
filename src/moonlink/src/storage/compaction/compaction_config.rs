use more_asserts as ma;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

/// Configurations for data compaction.
#[derive(Clone, Debug, PartialEq, TypedBuilder, Deserialize, Serialize)]
pub struct DataCompactionConfig {
    /// Number of existing data files with deletion vector and under final size to trigger a compaction operation.
    #[serde(default = "DataCompactionConfig::default_min_data_file_to_compact")]
    pub min_data_file_to_compact: u32,

    /// Max number of existing data files in one compaction operation.
    #[serde(default = "DataCompactionConfig::default_max_data_file_to_compact")]
    pub max_data_file_to_compact: u32,

    /// Number of bytes for a block index to consider it finalized and won't be merged again.
    #[serde(default = "DataCompactionConfig::default_data_file_final_size")]
    pub data_file_final_size: u64,
}

impl DataCompactionConfig {
    #[cfg(test)]
    pub const DEFAULT_MIN_DATA_FILE_TO_COMPACT: u32 = u32::MAX;
    #[cfg(test)]
    pub const DEFAULT_MAX_DATA_FILE_TO_COMPACT: u32 = u32::MAX;
    #[cfg(test)]
    pub const DEFAULT_DATA_FILE_FINAL_SIZE: u64 = u64::MAX;

    #[cfg(all(not(test), debug_assertions))]
    pub const DEFAULT_MIN_DATA_FILE_TO_COMPACT: u32 = 4;
    #[cfg(all(not(test), debug_assertions))]
    pub const DEFAULT_MAX_DATA_FILE_TO_COMPACT: u32 = 8;
    #[cfg(all(not(test), debug_assertions))]
    pub const DEFAULT_DATA_FILE_FINAL_SIZE: u64 = 1 << 10; // 1KiB

    #[cfg(all(not(test), not(debug_assertions)))]
    pub const DEFAULT_MIN_DATA_FILE_TO_COMPACT: u32 = 16;
    #[cfg(all(not(test), not(debug_assertions)))]
    pub const DEFAULT_MAX_DATA_FILE_TO_COMPACT: u32 = 32;
    #[cfg(all(not(test), not(debug_assertions)))]
    pub const DEFAULT_DATA_FILE_FINAL_SIZE: u64 = 1 << 29; // 512MiB

    pub fn default_min_data_file_to_compact() -> u32 {
        Self::DEFAULT_MIN_DATA_FILE_TO_COMPACT
    }
    pub fn default_max_data_file_to_compact() -> u32 {
        Self::DEFAULT_MAX_DATA_FILE_TO_COMPACT
    }
    pub fn default_data_file_final_size() -> u64 {
        Self::DEFAULT_DATA_FILE_FINAL_SIZE
    }

    pub fn validate(&self) {
        ma::assert_le!(self.min_data_file_to_compact, self.max_data_file_to_compact);
    }
}

impl Default for DataCompactionConfig {
    fn default() -> Self {
        Self {
            min_data_file_to_compact: Self::DEFAULT_MIN_DATA_FILE_TO_COMPACT,
            max_data_file_to_compact: Self::DEFAULT_MAX_DATA_FILE_TO_COMPACT,
            data_file_final_size: Self::DEFAULT_DATA_FILE_FINAL_SIZE,
        }
    }
}

impl DataCompactionConfig {
    /// Return a default config, with data compaction enabled.
    pub fn enabled() -> Self {
        Self::default()
    }

    /// Return a default config, with data compaction disabled.
    pub fn disabled() -> Self {
        Self {
            min_data_file_to_compact: u32::MAX,
            max_data_file_to_compact: u32::MAX,
            data_file_final_size: u64::MAX,
        }
    }
}
