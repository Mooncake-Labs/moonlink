use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::index::index_merge_config::FileIndexMergeConfig;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct IcebergPersistenceConfig {
    /// Number of new data files to trigger an iceberg snapshot.
    #[serde(default = "IcebergPersistenceConfig::default_new_data_file_count")]
    pub new_data_file_count: usize,

    /// Number of unpersisted committed delete logs to trigger an iceberg snapshot.
    #[serde(default = "IcebergPersistenceConfig::default_new_committed_deletion_log")]
    pub new_committed_deletion_log: usize,
}

// TODO(hjiang): Add another threshold for merged file indices to trigger iceberg snapshot.
impl IcebergPersistenceConfig {
    #[cfg(debug_assertions)]
    pub(crate) const DEFAULT_ICEBERG_NEW_DATA_FILE_COUNT: usize = 1;
    #[cfg(debug_assertions)]
    pub(crate) const DEFAULT_ICEBERG_SNAPSHOT_NEW_COMMITTED_DELETION_LOG: usize = 1000;

    #[cfg(not(debug_assertions))]
    pub(crate) const DEFAULT_ICEBERG_NEW_DATA_FILE_COUNT: usize = 1;
    #[cfg(not(debug_assertions))]
    pub(crate) const DEFAULT_ICEBERG_SNAPSHOT_NEW_COMMITTED_DELETION_LOG: usize = 1000;

    pub fn default_new_data_file_count() -> usize {
        Self::DEFAULT_ICEBERG_NEW_DATA_FILE_COUNT
    }
    pub fn default_new_committed_deletion_log() -> usize {
        Self::DEFAULT_ICEBERG_SNAPSHOT_NEW_COMMITTED_DELETION_LOG
    }
}

impl Default for IcebergPersistenceConfig {
    fn default() -> Self {
        Self {
            new_data_file_count: Self::DEFAULT_ICEBERG_NEW_DATA_FILE_COUNT,
            new_committed_deletion_log: Self::DEFAULT_ICEBERG_SNAPSHOT_NEW_COMMITTED_DELETION_LOG,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MooncakeTableConfig {
    /// Number of batch records which decides when to flush records from MemSlice to disk.
    pub mem_slice_size: usize,
    /// Number of new deletion records which decides whether to create a new mooncake table snapshot.
    pub snapshot_deletion_record_count: usize,
    /// Max number of rows in each record batch within MemSlice.
    pub batch_size: usize,
    /// Disk slice parquet file flush threshold.
    pub disk_slice_parquet_file_size: usize,
    /// Config for iceberg persistence config.
    pub persistence_config: IcebergPersistenceConfig,
    /// Config for data compaction.
    pub data_compaction_config: DataCompactionConfig,
    /// Config for index merge.
    pub file_index_config: FileIndexMergeConfig,
    /// Filesystem directory to store temporary files, used for union read.
    pub temp_files_directory: String,
}

impl Default for MooncakeTableConfig {
    fn default() -> Self {
        Self::new(Self::DEFAULT_TEMP_FILE_DIRECTORY.to_string())
    }
}

impl MooncakeTableConfig {
    #[cfg(debug_assertions)]
    pub(crate) const DEFAULT_MEM_SLICE_SIZE: usize = MooncakeTableConfig::DEFAULT_BATCH_SIZE * 8;
    #[cfg(debug_assertions)]
    pub(super) const DEFAULT_SNAPSHOT_DELETION_RECORD_COUNT: usize = 1000;
    #[cfg(debug_assertions)]
    pub(crate) const DEFAULT_BATCH_SIZE: usize = 128;
    #[cfg(debug_assertions)]
    pub(crate) const DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE: usize = 1024 * 1024 * 2; // 2MiB

    #[cfg(not(debug_assertions))]
    pub(crate) const DEFAULT_MEM_SLICE_SIZE: usize = MooncakeTableConfig::DEFAULT_BATCH_SIZE * 32;
    #[cfg(not(debug_assertions))]
    pub(super) const DEFAULT_SNAPSHOT_DELETION_RECORD_COUNT: usize = 1000;
    #[cfg(not(debug_assertions))]
    pub(crate) const DEFAULT_BATCH_SIZE: usize = 4096;
    #[cfg(not(debug_assertions))]
    pub(crate) const DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE: usize = 1024 * 1024 * 128; // 128MiB

    /// Default local directory to hold temporary files for union read.
    pub const DEFAULT_TEMP_FILE_DIRECTORY: &str = "/tmp/moonlink_temp_file";

    pub fn new(temp_files_directory: String) -> Self {
        Self {
            mem_slice_size: Self::DEFAULT_MEM_SLICE_SIZE,
            snapshot_deletion_record_count: Self::DEFAULT_SNAPSHOT_DELETION_RECORD_COUNT,
            batch_size: Self::DEFAULT_BATCH_SIZE,
            disk_slice_parquet_file_size: Self::DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE,
            persistence_config: IcebergPersistenceConfig::default(),
            data_compaction_config: DataCompactionConfig::default(),
            file_index_config: FileIndexMergeConfig::default(),
            temp_files_directory,
        }
    }

    // Default value accessor.
    pub fn default_mem_slice_size() -> usize {
        Self::DEFAULT_MEM_SLICE_SIZE
    }
    pub fn default_snapshot_deletion_record_count() -> usize {
        Self::DEFAULT_SNAPSHOT_DELETION_RECORD_COUNT
    }
    pub fn default_batch_size() -> usize {
        Self::DEFAULT_BATCH_SIZE
    }
    pub fn default_disk_slice_parquet_file_size() -> usize {
        Self::DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE
    }

    // Validation util function.
    pub fn validate(&self) {
        self.file_index_config.validate();
        self.data_compaction_config.validate();
    }

    // Accessor functions.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
    pub fn iceberg_snapshot_new_data_file_count(&self) -> usize {
        self.persistence_config.new_data_file_count
    }
    pub fn snapshot_deletion_record_count(&self) -> usize {
        self.snapshot_deletion_record_count
    }
    pub fn iceberg_snapshot_new_committed_deletion_log(&self) -> usize {
        self.persistence_config.new_committed_deletion_log
    }
}
