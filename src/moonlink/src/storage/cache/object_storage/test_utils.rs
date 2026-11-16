use tempfile::TempDir;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::storage::cache::object_storage::cache_config::ObjectStorageCacheConfig;
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
use crate::storage::storage_utils::FileId;
use crate::storage::storage_utils::TableId;
use crate::storage::storage_utils::TableUniqueFileId;

/// Content for test files.
pub(crate) const CONTENT: &[u8; 10] = b"0123456789";
/// File path for two test cache files.
pub(crate) const TEST_CACHE_FILENAME_1: &str = "cache-1.parquet";
pub(crate) const TEST_CACHE_FILENAME_2: &str = "cache-2.parquet";
/// File path for two test remote files.
pub(crate) const TEST_REMOTE_FILENAME_1: &str = "remote-1.parquet";

/// Test util function to return table unique id, with table id hard-coded to 0.
pub(crate) fn get_table_unique_file_id(file_id: u64) -> TableUniqueFileId {
    TableUniqueFileId {
        table_id: TableId(0),
        file_id: FileId(file_id),
    }
}

/// Util function to prepare a test file.
pub(crate) async fn create_test_file(
    tmp_dir: &std::path::Path,
    filename: &str,
) -> std::path::PathBuf {
    let filepath = tmp_dir.join(filename);
    let mut file = tokio::fs::File::create(&filepath).await.unwrap();
    file.write_all(CONTENT).await.unwrap();
    file.flush().await.unwrap();
    filepath
}

/// Test util function to assert file content.
pub(crate) async fn check_file_content(filepath: &str) {
    let mut file = tokio::fs::File::open(filepath).await.unwrap();
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).await.unwrap();
    assert_eq!(contents, CONTENT);
}

/// Test util function to create config for cache.
pub(crate) fn get_test_cache_config(tmp_dir: &TempDir) -> ObjectStorageCacheConfig {
    ObjectStorageCacheConfig {
        // Set max bytes larger than one file, but less than two files.
        max_bytes: 15,
        cache_directory: tmp_dir.path().to_str().unwrap().to_string(),
        optimize_local_filesystem: false,
    }
}

/// Test util function to create object storage cache, with local filesystem optimization disabled.
pub(crate) fn get_test_object_storage_cache(tmp_dir: &TempDir) -> ObjectStorageCache {
    let config = get_test_cache_config(tmp_dir);
    ObjectStorageCache::new(config)
}

/// Test util function to get file number under the given directory.
pub(crate) async fn check_directory_file_count(tmp_dir: &TempDir, expected_count: usize) {
    let mut actual_count = 0;
    let mut entries = tokio::fs::read_dir(tmp_dir.path()).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let metadata = entry.metadata().await.unwrap();
        if metadata.is_file() {
            actual_count += 1;
        }
    }
    assert_eq!(actual_count, expected_count);
}

/// Test util function to check the byte size for cache.
pub(crate) async fn assert_cache_bytes_size(cache: &mut ObjectStorageCache, expected_bytes: u64) {
    let guard = cache.cache.read().await;
    assert_eq!(guard.cur_bytes, expected_bytes);
    guard.assert_evictable_bytes_consistent();
}

/// Test util function to check the number of entries pending to delete.
pub(crate) async fn assert_pending_eviction_entries_size(
    cache: &mut ObjectStorageCache,
    expected_count: usize,
) {
    let guard = cache.cache.read().await;
    assert_eq!(guard.evicted_entries.len(), expected_count);
}

/// Test util function to check evictable cache size.
pub(crate) async fn assert_evictable_cache_size(
    cache: &mut ObjectStorageCache,
    expected_count: usize,
) {
    let guard = cache.cache.read().await;
    assert_eq!(guard.evictable_cache.len(), expected_count);
    guard.assert_evictable_bytes_consistent();
}

/// Test util function to check non-evictable cache size.
pub(crate) async fn assert_non_evictable_cache_size(
    cache: &mut ObjectStorageCache,
    expected_count: usize,
) {
    let guard = cache.cache.read().await;
    assert_eq!(guard.non_evictable_cache.len(), expected_count);
}

/// Test util function to check non-evictable cache handle reference count.
pub(crate) async fn assert_non_evictable_cache_handle_ref_count(
    cache: &mut ObjectStorageCache,
    file_id: TableUniqueFileId,
    expected_ref_count: u32,
) {
    let guard = cache.cache.read().await;
    let non_evictable = &guard.non_evictable_cache;
    assert_eq!(
        non_evictable
            .get(&file_id)
            .as_ref()
            .unwrap()
            .reference_count,
        expected_ref_count
    );
}
