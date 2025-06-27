use typed_builder::TypedBuilder;

/// Configuration for index merge.
///
/// TODO(hjiang): To reduce code change before preview release, disable data compaction by default until we do further testing to make sure moonlink fine.
#[derive(Clone, Debug, PartialEq, TypedBuilder)]
pub struct FileIndexMergeConfig {
    /// Number of existing index blocks under final size to trigger a merge operation.
    pub file_indices_to_merge: u32,
    /// Number of bytes for a block index to consider it finalized and won't be merged again.
    pub index_block_final_size: u64,
}

impl FileIndexMergeConfig {
    #[cfg(debug_assertions)]
    pub const DEFAULT_FILE_INDICES_TO_MERGE: u32 = u32::MAX;
    #[cfg(debug_assertions)]
    pub const DEFAULT_INDEX_BLOCK_FINAL_SIZE: u64 = u64::MAX;

    #[cfg(not(debug_assertions))]
    pub const DEFAULT_FILE_INDICES_TO_MERGE: u32 = u32::MAX;
    #[cfg(not(debug_assertions))]
    pub const DEFAULT_INDEX_BLOCK_FINAL_SIZE: u64 = u64::MAX;
}

impl Default for FileIndexMergeConfig {
    fn default() -> Self {
        Self {
            file_indices_to_merge: Self::DEFAULT_FILE_INDICES_TO_MERGE,
            index_block_final_size: Self::DEFAULT_INDEX_BLOCK_FINAL_SIZE,
        }
    }
}
