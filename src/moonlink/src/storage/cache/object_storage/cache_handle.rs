use std::sync::Arc;

use crate::storage::cache::object_storage::base_cache::CacheEntry;
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCacheInternal;
use crate::storage::storage_utils::TableUniqueFileId;

use crate::storage::cache::object_storage::base_cache::InlineEvictedFiles;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct NonEvictableHandle {
    /// File id for the mooncake table data file.
    pub(crate) file_id: TableUniqueFileId,
    /// Non-evictable cache entry.
    pub(crate) cache_entry: CacheEntry,
    /// Access to cache, used to unreference at drop.
    cache: Arc<RwLock<ObjectStorageCacheInternal>>,
}

impl std::fmt::Debug for NonEvictableHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NonEvictableHandle")
            .field("file_id", &self.file_id)
            .field("cache_entry", &self.cache_entry)
            .finish()
    }
}

impl NonEvictableHandle {
    pub(super) fn new(
        file_id: TableUniqueFileId,
        cache_entry: CacheEntry,
        cache: Arc<RwLock<ObjectStorageCacheInternal>>,
    ) -> Self {
        Self {
            file_id,
            cache,
            cache_entry,
        }
    }

    /// Get cache file path.
    pub(crate) fn get_cache_filepath(&self) -> &str {
        &self.cache_entry.cache_filepath
    }

    /// Unreference the pinned cache file.
    #[must_use]
    pub(crate) async fn unreference(&self) -> Vec<String> {
        let mut guard = self.cache.write().await;
        guard.unreference(self.file_id)
    }

    /// Unreference and pinned cache file and mark it as deleted.
    #[must_use]
    pub(crate) async fn unreference_and_delete(&self) -> InlineEvictedFiles {
        let mut guard = self.cache.write().await;

        // Total bytes within cache doesn't change, so current cache entry not evicted.
        let cur_evicted_files = guard.unreference(self.file_id);
        assert!(cur_evicted_files.is_empty());

        // The cache entry could be held elsewhere.
        guard.delete_cache_entry(self.file_id, /*panic_if_non_existent=*/ true)
    }

    /// Unreference and try import remote files, used for local filesystem optimization if enabled.
    ///
    /// This is an optimization for cases where both cache files and persisted files live on local filesystem, so we don't need to store the same files twice.
    /// The idea way, from users's perspective, is to switch from local filepath to remote if possible, but that leads to state machine being over-complicated.
    /// For example, we need to keep another pending state in cache, to record file paths requested to replace with remote, when they're (1) in use, or (2) in use and requested to delete.
    /// To make implementation easy, the implementation here only attempt once at unreference; if it fails, the replacement will never happen.
    ///
    /// But local cache files are still subject to eviction and deletion, for example, when
    /// - Object storage cache goes out of space;
    /// - Maintenance job like compaction kicks in and requests to delete old compacted files;
    /// - Moonlink process restarts and recreates the cache directory.
    #[must_use]
    pub(crate) async fn unreference_and_replace_with_remote(
        &self,
        remote_filepath: &str,
    ) -> Vec<String> {
        let mut guard = self.cache.write().await;

        // First unreference the cache handle as usual.
        let cur_evicted_files = guard.unreference(self.file_id);
        assert!(cur_evicted_files.is_empty());

        // Then try to replace cache filepath with remote file, if applicable.
        guard.try_replace_evictable_with_remote(&self.file_id, remote_filepath)
    }

    /// Replace current cache filepath with remote, used for local filesystem optimization if enabled.
    ///
    /// This is an optimization for cases where both cache files and persisted files live on local filesystem, so we don't need to store the same files twice.
    /// The idea way, from users's perspective, is to switch from local filepath to remote if possible, but that leads to state machine being over-complicated.
    /// For example, we need to keep another pending state in cache, to record file paths requested to replace with remote, when they're (1) in use, or (2) in use and requested to delete.
    /// To make implementation easy, the implementation here only attempt once at invocation, it succeeds if the current cache entry is the only non-evictable reference count, and not requested to delete;
    /// if it fails, the replacement will never happen.
    ///
    /// But local cache files are still subject to eviction and deletion, for example, when
    /// - Object storage cache goes out of space;
    /// - Maintenance job like compaction kicks in and requests to delete old compacted files;
    /// - Moonlink process restarts and recreates the cache directory.
    #[must_use]
    #[allow(dead_code)]
    pub(crate) async fn replace_with_remote(&mut self, remote_filepath: &str) -> Vec<String> {
        let mut guard = self.cache.write().await;

        // Try to replace cache filepath with remote file, if applicable.
        let evicted_files_to_delete =
            guard.try_replace_only_reference_count_with_remote(&self.file_id, remote_filepath);
        // If replacement succeeds, local cache filepath will be returned and to evict.
        if !evicted_files_to_delete.is_empty() {
            self.cache_entry.cache_filepath = remote_filepath.to_string();
        }

        evicted_files_to_delete
    }
}
