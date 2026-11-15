use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Object storage cache, which caches data file in file granularity at local filesystem.
use crate::storage::cache::object_storage::base_cache::{
    CacheEntry, CacheTrait, FileMetadata, InlineEvictedFiles,
};
use crate::storage::cache::object_storage::cache_config::ObjectStorageCacheConfig;
use crate::storage::cache::object_storage::cache_handle::NonEvictableHandle;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::path_utils;
use crate::storage::storage_utils::TableUniqueFileId;
use crate::Result;

use lru::LruCache;
use more_asserts as ma;
use smallvec::SmallVec;
#[cfg(test)]
use tempfile::TempDir;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CacheEntryWrapper {
    /// Cache entry.
    pub(crate) cache_entry: CacheEntry,
    /// Reference count.
    pub(crate) reference_count: u32,
    /// Whether the cache file could be deleted.
    /// It's set to false when local filesystem optimization turned on, and we use remote file as local cache at the same time.
    pub(crate) deletable: bool,
}

/// A cache entry could be either evictable or non-evictable.
/// A general lifecycle of a cache entry is to
/// (1) fetch and mark as non-evictable on access
/// (2) dereference after usage, down-level to evictable when it's unreferenced
pub(crate) struct ObjectStorageCacheInternal {
    /// Cache configuration.
    #[allow(dead_code)]
    config: ObjectStorageCacheConfig,
    /// Current number of bytes of all cache entries, which only accounts overall bytes for evictable cache and non-evictable cache.
    pub(crate) cur_bytes: u64,
    /// Whether the over-budget warning was logged since the cache was last within its budget.
    over_budget_warned: bool,
    /// Deleted entries, which should be evicted right away after no reference count, and should never be referenced again.
    pub(crate) evicted_entries: HashSet<TableUniqueFileId>,
    /// Evictable object storage cache entries.
    pub(crate) evictable_cache: LruCache<TableUniqueFileId, CacheEntryWrapper>,
    /// Total file size of [`Self::evictable_cache`].
    evictable_bytes: u64,
    /// Non-evictable object storage cache entries.
    pub(crate) non_evictable_cache: HashMap<TableUniqueFileId, CacheEntryWrapper>,
}

impl ObjectStorageCacheInternal {
    /// Util function to remove entries from evictable cache, until overall file size drops down below max size.
    ///
    /// # Arguments
    ///
    /// * tolerate_insufficiency: if true, tolerate disk space insufficiency by returning `false` in such case; otherwise panic if nothing to evict when insufficient disk space.
    ///
    /// Return
    /// - whether cache entries eviction succeeds or not.
    /// - evicted files to delete
    fn evict_cache_entries(
        &mut self,
        max_bytes: u64,
        tolerate_insufficiency: bool,
    ) -> (bool, Vec<String>) {
        let evicted_files_to_delete = self.evict_lru_while_above(max_bytes);
        if self.cur_bytes > max_bytes {
            if tolerate_insufficiency {
                return (false, evicted_files_to_delete);
            }
            // The entry must stay pinned (the table references it), so exceed the budget.
            if !self.over_budget_warned {
                self.over_budget_warned = true;
                tracing::warn!(
                    cur_bytes = self.cur_bytes,
                    max_bytes,
                    "object storage cache is over its budget and only pinned entries are left; keeping them"
                );
            }
            return (true, evicted_files_to_delete);
        }
        self.over_budget_warned = false;
        (true, evicted_files_to_delete)
    }

    /// Evict least recently used entries while the cache holds more than `max_bytes`; returns the
    /// files to delete.
    fn evict_lru_while_above(&mut self, max_bytes: u64) -> Vec<String> {
        let mut evicted_files_to_delete = vec![];
        while self.cur_bytes > max_bytes {
            let Some((_, mut cache_entry_wrapper)) = self.evictable_cache.pop_lru() else {
                break;
            };
            assert_eq!(cache_entry_wrapper.reference_count, 0);
            let file_size = cache_entry_wrapper.cache_entry.file_metadata.file_size;
            self.cur_bytes -= file_size;
            self.evictable_bytes -= file_size;

            if cache_entry_wrapper.deletable {
                let cache_filepath =
                    std::mem::take(&mut cache_entry_wrapper.cache_entry.cache_filepath);
                evicted_files_to_delete.push(cache_filepath);
            }
        }
        evicted_files_to_delete
    }

    #[cfg(test)]
    pub(crate) fn assert_evictable_bytes_consistent(&self) {
        let sum: u64 = self
            .evictable_cache
            .iter()
            .map(|(_, entry)| entry.cache_entry.file_metadata.file_size)
            .sum();
        assert_eq!(self.evictable_bytes, sum);
    }

    /// Insert into the evictable cache.
    fn push_evictable(
        &mut self,
        file_id: TableUniqueFileId,
        cache_entry_wrapper: CacheEntryWrapper,
    ) {
        self.evictable_bytes += cache_entry_wrapper.cache_entry.file_metadata.file_size;
        if let Some((_, replaced)) = self.evictable_cache.push(file_id, cache_entry_wrapper) {
            self.evictable_bytes -= replaced.cache_entry.file_metadata.file_size;
        }
    }

    /// Remove from the evictable cache.
    fn pop_evictable(&mut self, file_id: &TableUniqueFileId) -> Option<CacheEntryWrapper> {
        let cache_entry_wrapper = self.evictable_cache.pop(file_id)?;
        self.evictable_bytes -= cache_entry_wrapper.cache_entry.file_metadata.file_size;
        Some(cache_entry_wrapper)
    }

    /// Util function to insert into non-evictable cache.
    ///
    /// Return
    /// - whether cache entries eviction succeeds or not.
    /// - data files which get evicted from LRU cache, and will be deleted locally.
    ///
    /// NOTICE:
    /// - cache current bytes won't be updated.
    /// - If insertion fails due to insufficiency, the input cache entry won't be inserted into cache.
    fn insert_non_evictable(
        &mut self,
        file_id: TableUniqueFileId,
        cache_entry_wrapper: CacheEntryWrapper,
        max_bytes: u64,
        tolerate_insufficiency: bool,
    ) -> (bool, Vec<String>) {
        assert!(self.evictable_cache.get(&file_id).is_none());
        // `cur_bytes` already counts the new entry. If the pinned entries and it exceed the budget
        // it can't be cached; evicting is still worth it, but only back down to the budget.
        let file_size = cache_entry_wrapper.cache_entry.file_metadata.file_size;
        if tolerate_insufficiency && self.cur_bytes - self.evictable_bytes > max_bytes {
            let evicted_files_to_delete =
                self.evict_lru_while_above(max_bytes.saturating_add(file_size));
            return (false, evicted_files_to_delete);
        }
        assert!(self
            .non_evictable_cache
            .insert(file_id, cache_entry_wrapper)
            .is_none());
        let (evict_succ, evicted_files_to_delete) =
            self.evict_cache_entries(max_bytes, tolerate_insufficiency);
        if !evict_succ {
            assert!(self.non_evictable_cache.remove(&file_id).is_some());
        }

        (evict_succ, evicted_files_to_delete)
    }

    /// Mark the requested cache entry as deleted, and return evicted files.
    pub(super) fn delete_cache_entry(
        &mut self,
        file_id: TableUniqueFileId,
        panic_if_non_existent: bool,
    ) -> InlineEvictedFiles {
        let mut evicted_files_to_delete: InlineEvictedFiles = InlineEvictedFiles::new();

        // If the requested entries are already evictable, remove it directly.
        if let Some(cache_entry_wrapper) = self.pop_evictable(&file_id) {
            assert_eq!(cache_entry_wrapper.reference_count, 0);
            self.cur_bytes -= cache_entry_wrapper.cache_entry.file_metadata.file_size;

            if cache_entry_wrapper.deletable {
                evicted_files_to_delete.push(cache_entry_wrapper.cache_entry.cache_filepath);
            }
        }
        // Otherwise, we leave a marker, so when the entries get unreferences it will be deleted.
        else {
            let exists_in_cache = self.non_evictable_cache.contains_key(&file_id);
            if exists_in_cache {
                assert!(self.evicted_entries.insert(file_id));
            } else if panic_if_non_existent {
                panic!("Requested file id {file_id:?} should exist in object storage cache");
            }
        }

        evicted_files_to_delete
    }

    /// Unreference the given cache entry.
    pub(super) fn unreference(&mut self, file_id: TableUniqueFileId) -> Vec<String> {
        let cache_entry_wrapper = self.non_evictable_cache.get_mut(&file_id);
        let cache_entry_wrapper = cache_entry_wrapper
            .unwrap_or_else(|| panic!("No reference count for file id {file_id:?}"));
        cache_entry_wrapper.reference_count -= 1;

        // Aggregate cache entries to delete.
        let mut evicted_files_to_delete = vec![];

        // Down-level to evictable if reference count goes away.
        if cache_entry_wrapper.reference_count == 0 {
            let cache_entry_wrapper = self.non_evictable_cache.remove(&file_id).unwrap();

            // If the current entry has already been requested to delete.
            if self.evicted_entries.remove(&file_id) {
                ma::assert_ge!(
                    self.cur_bytes,
                    cache_entry_wrapper.cache_entry.file_metadata.file_size
                );
                self.cur_bytes -= cache_entry_wrapper.cache_entry.file_metadata.file_size;

                if cache_entry_wrapper.deletable {
                    evicted_files_to_delete.push(cache_entry_wrapper.cache_entry.cache_filepath);
                }
            }
            // The cache entry is not requested to delete.
            else {
                self.push_evictable(file_id, cache_entry_wrapper);
            }
        }

        evicted_files_to_delete
    }

    /// Attempt to replace an evictable cache entry with remote path, if the filepath lives on local filesystem.
    /// Return evicted files to delete.
    pub(super) fn try_replace_evictable_with_remote(
        &mut self,
        file_id: &TableUniqueFileId,
        remote_path: &str,
    ) -> Vec<String> {
        // Local filesystem optimization doesn't apply here.
        if !self.config.optimize_local_filesystem {
            return vec![];
        }
        if !path_utils::is_local_filepath(remote_path) {
            return vec![];
        }

        // Only replace with remote filepath when requested file lives at evictable cache.
        if let Some(cache_entry_wrapper) = self.evictable_cache.get_mut(file_id) {
            let old_cache_filepath =
                std::mem::take(&mut cache_entry_wrapper.cache_entry.cache_filepath);
            cache_entry_wrapper.cache_entry.cache_filepath = remote_path.to_string();
            cache_entry_wrapper.deletable = false;

            return vec![old_cache_filepath];
        }

        // Otherwise, do nothing.
        vec![]
    }

    /// Attempt to replace the cache entry with remote path, if it's the last reference count on local filesystem, and not requested to delete.
    /// Return evicted files to delete; it's non empty if replacement succeeds.
    pub(super) fn try_replace_only_reference_count_with_remote(
        &mut self,
        file_id: &TableUniqueFileId,
        remote_path: &str,
    ) -> Vec<String> {
        // Local filesystem optimization doesn't apply here.
        if !self.config.optimize_local_filesystem {
            return vec![];
        }
        if !path_utils::is_local_filepath(remote_path) {
            return vec![];
        }

        // If the cache entry has been requested to delete, skip.
        if self.evicted_entries.contains(file_id) {
            return vec![];
        }

        // Only replace with remote filepath when requested file is the only reference count at the non-evictable cache.
        if let Some(cache_entry_wrapper) = self.non_evictable_cache.get_mut(file_id) {
            ma::assert_ge!(cache_entry_wrapper.reference_count, 1);
            if cache_entry_wrapper.reference_count > 1 {
                return vec![];
            }

            let old_cache_filepath =
                std::mem::take(&mut cache_entry_wrapper.cache_entry.cache_filepath);
            cache_entry_wrapper.cache_entry.cache_filepath = remote_path.to_string();
            cache_entry_wrapper.deletable = false;

            return vec![old_cache_filepath];
        }

        // Otherwise, do nothing.
        vec![]
    }

    /// ================================
    /// Test util functions
    /// ================================
    ///
    /// Test util function to get reference count for the given file id, return 0 if doesn't exist.
    #[cfg(test)]
    pub(crate) fn get_non_evictable_entry_ref_count(&self, file_id: &TableUniqueFileId) -> u32 {
        let cache_entry = self.non_evictable_cache.get(file_id);
        if let Some(cache_entry) = cache_entry {
            return cache_entry.reference_count;
        }
        0
    }
}

// TODO(hjiang): Add stats for cache, like cache hit/miss rate, cache size, etc.
#[derive(Clone)]
pub struct ObjectStorageCache {
    /// Cache configs.
    config: ObjectStorageCacheConfig,
    /// Object storage caches.
    pub(crate) cache: Arc<RwLock<ObjectStorageCacheInternal>>,
}

// A dummy [`Debug`] trait implementation.
impl std::fmt::Debug for ObjectStorageCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStorageCache")
            .field("config", &self.config)
            .finish()
    }
}

impl ObjectStorageCache {
    pub fn new(config: ObjectStorageCacheConfig) -> Self {
        let evictable_cache = LruCache::unbounded();
        Self {
            config: config.clone(),
            cache: Arc::new(RwLock::new(ObjectStorageCacheInternal {
                config,
                cur_bytes: 0,
                over_budget_warned: false,
                evicted_entries: HashSet::new(),
                evictable_cache,
                evictable_bytes: 0,
                non_evictable_cache: HashMap::new(),
            })),
        }
    }

    /// Read from remote [`src`] and write to local cache file, return cache entries.
    async fn load_from_remote(
        &self,
        src: &str,
        filesystem_accessor: &dyn BaseFileSystemAccess,
    ) -> Result<CacheEntry> {
        let src_pathbuf = std::path::PathBuf::from(src);
        let suffix = src_pathbuf.extension().unwrap().to_str().unwrap();
        let mut dst_pathbuf = std::path::PathBuf::from(&self.config.cache_directory);
        dst_pathbuf.push(format!("{}.{}", Uuid::now_v7(), suffix));
        let dst_filepath = dst_pathbuf.to_str().unwrap().to_string();
        let object_metadata = filesystem_accessor
            .copy_from_remote_to_local(src, &dst_filepath)
            .await?;
        Ok(CacheEntry {
            cache_filepath: dst_filepath,
            file_metadata: FileMetadata {
                file_size: object_metadata.size,
            },
        })
    }

    /// Look up or load, and pin, a cache entry. With `must_pin` the entry is pinned even if that
    /// exceeds the budget; otherwise `None` is returned when there's no room for it.
    async fn get_cache_entry_impl(
        &self,
        file_id: TableUniqueFileId,
        remote_filepath: &str,
        filesystem_accessor: &dyn BaseFileSystemAccess,
        must_pin: bool,
    ) -> Result<(
        Option<NonEvictableHandle>,
        InlineEvictedFiles, /*files_to_delete*/
    )> {
        {
            let mut guard = self.cache.write().await;

            // Check non-evictable cache.
            let value = guard.non_evictable_cache.get_mut(&file_id);
            if let Some(value) = value {
                ma::assert_gt!(value.reference_count, 0);
                value.reference_count += 1;
                let cache_entry = value.cache_entry.clone();
                let non_evictable_handle =
                    NonEvictableHandle::new(file_id, cache_entry, self.cache.clone());
                return Ok((
                    Some(non_evictable_handle),
                    /*files_to_delete=*/ SmallVec::new(),
                ));
            }

            // Check evictable cache.
            let value = guard.pop_evictable(&file_id);
            if let Some(mut value) = value {
                assert_eq!(value.reference_count, 0);
                value.reference_count += 1;
                let cache_entry = value.cache_entry.clone();
                // Moving an entry from evictable to pinned doesn't change the cached bytes, so no
                // eviction is needed; evicting here could even drop this very entry when the cache
                // is over budget, leaving the returned handle dangling.
                assert!(guard.non_evictable_cache.insert(file_id, value).is_none());
                let non_evictable_handle =
                    NonEvictableHandle::new(file_id, cache_entry, self.cache.clone());
                return Ok((
                    Some(non_evictable_handle),
                    /*files_to_delete=*/ SmallVec::new(),
                ));
            }
        }

        // Place IO operation out of critical section.
        let cache_entry_wrapper = self
            .get_cache_handle_from_remote(remote_filepath, filesystem_accessor)
            .await?;
        let file_size = cache_entry_wrapper.cache_entry.file_metadata.file_size;
        let downloaded_filepath = cache_entry_wrapper
            .deletable
            .then(|| cache_entry_wrapper.cache_entry.cache_filepath.clone());
        let non_evictable_handle = NonEvictableHandle::new(
            file_id,
            cache_entry_wrapper.cache_entry.clone(),
            self.cache.clone(),
        );

        let files_to_delete = {
            let mut guard = self.cache.write().await;
            guard.cur_bytes += file_size;

            let (cache_succ, files_to_delete) = guard.insert_non_evictable(
                file_id,
                cache_entry_wrapper,
                self.config.max_bytes,
                /*tolerate_insufficiency=*/ !must_pin,
            );
            if cache_succ {
                return Ok((Some(non_evictable_handle), files_to_delete.into()));
            }

            // Otherwise, it means cache entry failed to insert.
            ma::assert_ge!(guard.cur_bytes, file_size);
            guard.cur_bytes -= file_size;
            files_to_delete
        };

        // Nothing references the download; callers read the remote file instead.
        if let Some(downloaded_filepath) = downloaded_filepath {
            if let Err(e) = tokio::fs::remove_file(&downloaded_filepath).await {
                tracing::warn!(
                    path = downloaded_filepath,
                    error = %e,
                    "failed to delete a download the object storage cache had no room for"
                );
            }
        }
        Ok((None, files_to_delete.into()))
    }

    /// Get cache entry from remote filepath [`src`].
    async fn get_cache_handle_from_remote(
        &self,
        src: &str,
        filesystem_accessor: &dyn BaseFileSystemAccess,
    ) -> Result<CacheEntryWrapper> {
        // If the remote filepath indicates a local filesystem one, use it as cache as well.
        if self.config.optimize_local_filesystem && path_utils::is_local_filepath(src) {
            let file_size = tokio::fs::metadata(src).await?.len();
            let cache_entry = CacheEntry {
                cache_filepath: src.to_string(),
                file_metadata: FileMetadata { file_size },
            };
            return Ok(CacheEntryWrapper {
                cache_entry,
                reference_count: 1,
                deletable: false,
            });
        }

        // The requested item doesn't exist, perform IO operations to load.
        let cache_entry = self.load_from_remote(src, filesystem_accessor).await?;
        Ok(CacheEntryWrapper {
            cache_entry,
            reference_count: 1,
            deletable: true,
        })
    }

    /// ================================
    /// Test/bench util functions
    /// ================================
    ///
    #[cfg(test)]
    pub fn default_for_test(temp_dir: &TempDir) -> Self {
        let config = ObjectStorageCacheConfig::default_for_test(temp_dir);
        Self::new(config)
    }
    #[cfg(feature = "bench")]
    pub fn default_for_bench() -> Self {
        let config = ObjectStorageCacheConfig::default_for_bench();
        Self::new(config)
    }
    /// Test util function to create an object storage cache for feature=bench.
    #[cfg(feature = "bench")]
    pub fn create_bench_object_storage_cache() -> Arc<dyn CacheTrait> {
        let object_storage_cache = ObjectStorageCache::default_for_bench();
        Arc::new(object_storage_cache)
    }
    /// Test util function to get reference count for reference count.
    #[cfg(test)]
    pub(crate) async fn get_non_evictable_entry_ref_count(
        &self,
        file_id: &TableUniqueFileId,
    ) -> u32 {
        let guard = self.cache.read().await;
        guard.get_non_evictable_entry_ref_count(file_id)
    }

    /// Test util function to get non-evictable filenames.
    #[cfg(test)]
    pub(crate) async fn get_non_evictable_filenames(&self) -> Vec<TableUniqueFileId> {
        let guard = self.cache.read().await;
        guard
            .non_evictable_cache
            .keys()
            .cloned()
            .collect::<Vec<_>>()
    }
}

#[async_trait::async_trait]
impl CacheTrait for ObjectStorageCache {
    async fn import_cache_entry(
        &self,
        file_id: TableUniqueFileId,
        cache_entry: CacheEntry,
    ) -> (NonEvictableHandle, InlineEvictedFiles) {
        let cache_entry_wrapper = CacheEntryWrapper {
            cache_entry: cache_entry.clone(),
            reference_count: 1,
            deletable: true,
        };
        let file_size = cache_entry.file_metadata.file_size;
        let non_evictable_handle =
            NonEvictableHandle::new(file_id, cache_entry, self.cache.clone());

        let mut guard = self.cache.write().await;
        guard.cur_bytes += file_size;

        let cache_files_to_delete = guard
            .insert_non_evictable(
                file_id,
                cache_entry_wrapper,
                self.config.max_bytes,
                /*tolerate_insufficiency=*/ false,
            )
            .1;
        (non_evictable_handle, cache_files_to_delete.into())
    }

    async fn get_cache_entry(
        &self,
        file_id: TableUniqueFileId,
        remote_filepath: &str,
        filesystem_accessor: &dyn BaseFileSystemAccess,
    ) -> Result<(
        Option<NonEvictableHandle>,
        InlineEvictedFiles, /*files_to_delete*/
    )> {
        self.get_cache_entry_impl(
            file_id,
            remote_filepath,
            filesystem_accessor,
            /*must_pin=*/ false,
        )
        .await
    }

    async fn get_cache_entry_pinned(
        &self,
        file_id: TableUniqueFileId,
        remote_filepath: &str,
        filesystem_accessor: &dyn BaseFileSystemAccess,
    ) -> Result<(NonEvictableHandle, InlineEvictedFiles)> {
        let (handle, files_to_delete) = self
            .get_cache_entry_impl(
                file_id,
                remote_filepath,
                filesystem_accessor,
                /*must_pin=*/ true,
            )
            .await?;
        // With `must_pin` the entry is inserted even over budget, so a handle is always returned.
        Ok((handle.expect("must_pin always pins"), files_to_delete))
    }

    async fn try_delete_cache_entry(&self, file_id: TableUniqueFileId) -> InlineEvictedFiles {
        let mut guard = self.cache.write().await;
        guard.delete_cache_entry(file_id, /*panic_if_non_existent=*/ false)
    }

    async fn increment_reference_count(&self, cache_handle: &NonEvictableHandle) {
        let mut guard = self.cache.write().await;
        let value = guard.non_evictable_cache.get_mut(&cache_handle.file_id);
        if let Some(value) = value {
            ma::assert_gt!(value.reference_count, 0);
            value.reference_count += 1;
            return;
        }
        panic!(
            "Requested to increment reference count for file id {:?} and file path {:?}, but not pinned in cache.",
            cache_handle.file_id, cache_handle.get_cache_filepath(),
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::cache::object_storage::test_utils::*;
    use crate::storage::storage_utils::TableId;
    use crate::{create_data_file, FileSystemAccessor};

    use super::*;

    use tempfile::tempdir;

    /// Test util function to get cache entry.
    async fn get_cache_handle_impl(
        file_index: i32,
        remote_file_directory: std::path::PathBuf,
        object_storage_cache: ObjectStorageCache,
        filesystem_accessor: &dyn BaseFileSystemAccess,
    ) -> NonEvictableHandle {
        let filename = format!("{file_index}.parquet");
        let test_file = create_test_file(remote_file_directory.as_path(), &filename).await;
        let data_file = create_data_file(
            /*file_id=*/ file_index as u64,
            test_file.to_str().unwrap().to_string(),
        );
        let unique_file_id = TableUniqueFileId {
            table_id: TableId(0),
            file_id: data_file.file_id(),
        };
        let (cache_handle, cache_to_delete) = object_storage_cache
            .get_cache_entry(unique_file_id, data_file.file_path(), filesystem_accessor)
            .await
            .unwrap();
        assert!(cache_to_delete.is_empty());
        cache_handle.unwrap()
    }

    #[tokio::test]
    async fn test_increment_ref_count() {
        let cache_file_directory = tempdir().unwrap();
        let test_cache_file =
            create_test_file(cache_file_directory.path(), TEST_CACHE_FILENAME_1).await;

        let config = ObjectStorageCacheConfig {
            // Set max bytes larger than one file, but less than two files.
            max_bytes: CONTENT.len() as u64,
            cache_directory: cache_file_directory.path().to_str().unwrap().to_string(),
            optimize_local_filesystem: false,
        };
        let cache = ObjectStorageCache::new(config);

        // Import cache entry.
        let cache_entry = CacheEntry {
            cache_filepath: test_cache_file.to_str().unwrap().to_string(),
            file_metadata: FileMetadata {
                file_size: CONTENT.len() as u64,
            },
        };
        let file_id = get_table_unique_file_id(/*file_id=*/ 0);
        let (cache_handle, evicted_files_to_delete) =
            cache.import_cache_entry(file_id, cache_entry.clone()).await;
        assert_eq!(
            cache_handle.cache_entry.cache_filepath,
            test_cache_file.to_str().unwrap().to_string()
        );
        assert!(evicted_files_to_delete.is_empty());

        // Increment the reference count.
        cache.increment_reference_count(&cache_handle).await;
        assert_eq!(cache.get_non_evictable_entry_ref_count(&file_id).await, 2);
    }

    #[tokio::test]
    async fn test_concurrent_object_storage_cache() {
        const PARALLEL_TASK_NUM: usize = 10;
        let mut handle_futures = Vec::with_capacity(PARALLEL_TASK_NUM);

        let cache_file_directory = tempdir().unwrap();
        let remote_file_directory = tempdir().unwrap();

        let config = ObjectStorageCacheConfig {
            // Set max bytes larger than one file, but less than two files.
            max_bytes: (CONTENT.len() * PARALLEL_TASK_NUM) as u64,
            cache_directory: cache_file_directory.path().to_str().unwrap().to_string(),
            optimize_local_filesystem: false,
        };
        let cache = ObjectStorageCache::new(config);
        let filesystem_accessor = FileSystemAccessor::default_for_test(&remote_file_directory);

        for idx in 0..PARALLEL_TASK_NUM {
            let temp_cache = cache.clone();
            let temp_filesystem_accessor = filesystem_accessor.clone();
            let remote_file_dir_pathbuf = remote_file_directory.path().to_path_buf();
            let handle = tokio::task::spawn_blocking(async move || -> NonEvictableHandle {
                get_cache_handle_impl(
                    idx as i32,
                    remote_file_dir_pathbuf,
                    temp_cache,
                    temp_filesystem_accessor.as_ref(),
                )
                .await
            });
            handle_futures.push(handle);
        }

        let results = futures::future::join_all(handle_futures).await;
        for cur_handle_future in results.into_iter() {
            let non_evictable_handle = cur_handle_future.unwrap().await;
            check_file_content(&non_evictable_handle.cache_entry.cache_filepath).await;
            assert_eq!(
                non_evictable_handle.cache_entry.file_metadata.file_size as usize,
                CONTENT.len()
            );
        }
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 0);
        assert_eq!(
            cache.cache.read().await.non_evictable_cache.len(),
            PARALLEL_TASK_NUM
        );
        check_directory_file_count(&cache_file_directory, PARALLEL_TASK_NUM).await;
        check_directory_file_count(&remote_file_directory, PARALLEL_TASK_NUM).await;
    }

    #[tokio::test]
    async fn test_concurrent_cache_access_with_local_optimization() {
        const PARALLEL_TASK_NUM: usize = 10;
        let mut handle_futures = Vec::with_capacity(PARALLEL_TASK_NUM);

        let cache_file_directory = tempdir().unwrap();
        let remote_file_directory = tempdir().unwrap();

        let config = ObjectStorageCacheConfig {
            // Set max bytes larger than one file, but less than two files.
            max_bytes: (CONTENT.len() * PARALLEL_TASK_NUM) as u64,
            cache_directory: cache_file_directory.path().to_str().unwrap().to_string(),
            optimize_local_filesystem: true,
        };
        let cache = ObjectStorageCache::new(config);
        let filesystem_accessor = FileSystemAccessor::default_for_test(&cache_file_directory);

        for idx in 0..PARALLEL_TASK_NUM {
            let temp_cache = cache.clone();
            let temp_filesystem_accessor = filesystem_accessor.clone();
            let remote_file_dir_pathbuf = remote_file_directory.path().to_path_buf();
            let handle = tokio::task::spawn_blocking(async move || -> NonEvictableHandle {
                get_cache_handle_impl(
                    idx as i32,
                    remote_file_dir_pathbuf,
                    temp_cache,
                    temp_filesystem_accessor.as_ref(),
                )
                .await
            });
            handle_futures.push(handle);
        }

        let results = futures::future::join_all(handle_futures).await;
        for cur_handle_future in results.into_iter() {
            let non_evictable_handle = cur_handle_future.unwrap().await;
            check_file_content(&non_evictable_handle.cache_entry.cache_filepath).await;
            assert_eq!(
                non_evictable_handle.cache_entry.file_metadata.file_size as usize,
                CONTENT.len()
            );
        }
        assert_eq!(cache.cache.read().await.evictable_cache.len(), 0);
        assert_eq!(
            cache.cache.read().await.non_evictable_cache.len(),
            PARALLEL_TASK_NUM
        );
        check_directory_file_count(&cache_file_directory, 0).await;
        check_directory_file_count(&remote_file_directory, PARALLEL_TASK_NUM).await;
    }
}
