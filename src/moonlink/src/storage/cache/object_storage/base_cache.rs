/// This trait provides the interface for object storage cache, which is used to provides write-through and read-through caching feature for data files.
///
/// At moonlink, object storage cache
/// - leverage local filesystem for on-disk cache
/// - evictable if a cache entry is unreferenced
use async_trait::async_trait;

use crate::storage::cache::object_storage::cache_handle::NonEvictableHandle;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::storage_utils::TableUniqueFileId;
use crate::Result;
#[cfg(test)]
use mockall::*;
use smallvec::SmallVec;

pub type InlineEvictedFiles = SmallVec<[String; 1]>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileMetadata {
    /// Size of the current file.
    pub(crate) file_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    /// Cache data file at local filesystem.
    pub(crate) cache_filepath: String,
    /// File metadata.
    pub(crate) file_metadata: FileMetadata,
}

#[async_trait]
#[cfg_attr(test, automock)]
pub trait CacheTrait: std::fmt::Debug + Send + Sync {
    /// Import cache entry to the cache. If there's no enough disk space, panic directly.
    /// Precondition: the file is not managed by cache.
    #[allow(async_fn_in_trait)]
    async fn import_cache_entry(
        &self,
        file_id: TableUniqueFileId,
        cache_entry: CacheEntry,
    ) -> (NonEvictableHandle, InlineEvictedFiles);

    /// Similar to [`delete_cache_entry`], but doesn't panic if requested entry doesn't exist.
    #[allow(async_fn_in_trait)]
    async fn try_delete_cache_entry(&self, file_id: TableUniqueFileId) -> InlineEvictedFiles;

    /// Attempt to get a pinned cache file entry.
    ///
    /// If the requested file is already pinned, cache handle will returned immediately without any IO operations.
    /// Otherwise, an IO operation might be performed, depending on whether the corresponding cache entry happens to be alive.
    /// If there's no sufficient disk space, return [`None`].
    #[allow(async_fn_in_trait)]
    async fn get_cache_entry(
        &self,
        file_id: TableUniqueFileId,
        remote_filepath: &str,
        filesystem_accessor: &dyn BaseFileSystemAccess,
    ) -> Result<(
        Option<NonEvictableHandle>,
        InlineEvictedFiles, /*files_to_delete*/
    )>;

    /// Increment reference count by 1 for the given cache handle.
    ///
    /// Precondition: the requested file id already has its cache entry pinned, otherwise panic.
    ///
    /// TODO(hjiang):
    /// 1. Consider error propagation, since as a library it shouldn't panic for failed precondition.
    /// 2. Add unit tests.
    #[allow(async_fn_in_trait)]
    async fn increment_reference_count(&self, cache_handle: &NonEvictableHandle);
}
