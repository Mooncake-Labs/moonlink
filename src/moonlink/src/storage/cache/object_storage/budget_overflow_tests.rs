//! The cache budget is advisory for files the table must keep pinned (imported data files,
//! file index blocks, deletion vectors): running out of budget must never panic, drop a pinned
//! entry, or hand out a handle to an entry the cache no longer tracks.
use crate::storage::cache::object_storage::base_cache::{CacheEntry, CacheTrait, FileMetadata};
use crate::storage::cache::object_storage::cache_config::ObjectStorageCacheConfig;
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
use crate::storage::cache::object_storage::test_utils::*;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;

use tempfile::{tempdir, TempDir};

/// A cache whose budget is smaller than a single test file.
fn tiny_cache(cache_dir: &TempDir, optimize_local_filesystem: bool) -> ObjectStorageCache {
    ObjectStorageCache::new(ObjectStorageCacheConfig {
        max_bytes: 1,
        cache_directory: cache_dir.path().to_str().unwrap().to_string(),
        optimize_local_filesystem,
    })
}

fn cache_entry_for(path: &std::path::Path) -> CacheEntry {
    CacheEntry {
        cache_filepath: path.to_str().unwrap().to_string(),
        file_metadata: FileMetadata {
            file_size: CONTENT.len() as u64,
        },
    }
}

/// Importing files the table owns keeps them pinned even when they exceed the budget.
#[tokio::test]
async fn test_import_over_budget_keeps_entries_pinned() {
    let cache_dir = tempdir().unwrap();
    let mut cache = tiny_cache(&cache_dir, /*optimize_local_filesystem=*/ false);
    let file_1 = create_test_file(cache_dir.path(), TEST_CACHE_FILENAME_1).await;
    let file_2 = create_test_file(cache_dir.path(), TEST_CACHE_FILENAME_2).await;

    let (handle_1, evicted) = cache
        .import_cache_entry(get_table_unique_file_id(0), cache_entry_for(&file_1))
        .await;
    assert!(evicted.is_empty());
    let (handle_2, evicted) = cache
        .import_cache_entry(get_table_unique_file_id(1), cache_entry_for(&file_2))
        .await;
    assert!(evicted.is_empty());

    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await;
    assert_cache_bytes_size(&mut cache, 2 * CONTENT.len() as u64).await;

    // Once unpinned, entries become evictable and the budget is enforced again.
    let evicted = handle_1.unreference().await;
    assert!(evicted.is_empty());
    let evicted = handle_2.unreference().await;
    assert!(evicted.is_empty());
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await;
    check_file_content(file_1.to_str().unwrap()).await;
}

/// `get_cache_entry` declines to cache when there's no room; `get_cache_entry_pinned` (used
/// for index blocks and deletion vectors, at recovery and at persistence) always pins.
#[tokio::test]
async fn test_pinned_lookup_over_budget_always_returns_a_handle() {
    let remote_dir = tempdir().unwrap();
    let cache_dir = tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&remote_dir);
    let mut cache = tiny_cache(&cache_dir, /*optimize_local_filesystem=*/ false);
    let remote_1 = create_test_file(remote_dir.path(), TEST_REMOTE_FILENAME_1).await;
    let remote_2 = create_test_file(remote_dir.path(), "remote-2.parquet").await;

    let (handle, _) = cache
        .get_cache_entry(
            get_table_unique_file_id(0),
            remote_1.to_str().unwrap(),
            filesystem_accessor.as_ref(),
        )
        .await
        .unwrap();
    assert!(handle.is_none());
    assert_cache_bytes_size(&mut cache, 0).await;

    let (handle, _) = cache
        .get_cache_entry_pinned(
            get_table_unique_file_id(1),
            remote_2.to_str().unwrap(),
            filesystem_accessor.as_ref(),
        )
        .await
        .unwrap();
    check_file_content(handle.get_cache_filepath()).await;
    assert_non_evictable_cache_handle_ref_count(&mut cache, get_table_unique_file_id(1), 1).await;
    assert_cache_bytes_size(&mut cache, CONTENT.len() as u64).await;

    // A second reference to the pinned entry works too.
    let (handle_again, _) = cache
        .get_cache_entry(
            get_table_unique_file_id(1),
            remote_2.to_str().unwrap(),
            filesystem_accessor.as_ref(),
        )
        .await
        .unwrap();
    assert!(handle_again.is_some());
    assert_non_evictable_cache_handle_ref_count(&mut cache, get_table_unique_file_id(1), 2).await;
}

/// Re-pinning an evictable entry while the cache is over budget keeps that entry; it used to be
/// dropped from the cache while its handle was returned, so the later unreference panicked.
#[tokio::test]
async fn test_repin_evictable_entry_while_over_budget() {
    let cache_dir = tempdir().unwrap();
    let remote_dir = tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&remote_dir);
    let mut cache = tiny_cache(&cache_dir, /*optimize_local_filesystem=*/ true);
    let local_1 = create_test_file(remote_dir.path(), TEST_CACHE_FILENAME_1).await;
    let local_2 = create_test_file(remote_dir.path(), TEST_CACHE_FILENAME_2).await;

    // Two pinned local files put the cache over budget.
    let (handle_1, _) = cache
        .get_cache_entry_pinned(
            get_table_unique_file_id(0),
            local_1.to_str().unwrap(),
            filesystem_accessor.as_ref(),
        )
        .await
        .unwrap();
    let (handle_2, _) = cache
        .get_cache_entry_pinned(
            get_table_unique_file_id(1),
            local_2.to_str().unwrap(),
            filesystem_accessor.as_ref(),
        )
        .await
        .unwrap();
    // Unpin the first: it becomes evictable but stays (the eviction pass runs on insertion).
    assert!(handle_1.unreference().await.is_empty());
    assert_evictable_cache_size(&mut cache, 1).await;

    // Pin it again while the cache is still over budget.
    let (handle_1_again, evicted) = cache
        .get_cache_entry(
            get_table_unique_file_id(0),
            local_1.to_str().unwrap(),
            filesystem_accessor.as_ref(),
        )
        .await
        .unwrap();
    assert!(evicted.is_empty());
    let handle_1_again = handle_1_again.unwrap();
    assert_non_evictable_cache_handle_ref_count(&mut cache, get_table_unique_file_id(0), 1).await;

    // Both handles release cleanly.
    assert!(handle_1_again.unreference().await.is_empty());
    assert!(handle_2.unreference().await.is_empty());
    check_file_content(local_1.to_str().unwrap()).await;
    check_file_content(local_2.to_str().unwrap()).await;
}

/// A lookup that can't be cached because the pinned entries and the new file exceed the budget
/// evicts least recently used entries back down to the budget (never further, and never the
/// pinned ones), then deletes its own download.
async fn check_unpinned_lookup_over_budget(
    max_bytes: u64,
    pinned: usize,
    unpinned: usize,
    expected_cached: usize,
) {
    let remote_dir = tempdir().unwrap();
    let cache_dir = tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&remote_dir);
    let mut cache = ObjectStorageCache::new(ObjectStorageCacheConfig {
        max_bytes,
        cache_directory: cache_dir.path().to_str().unwrap().to_string(),
        optimize_local_filesystem: false,
    });
    let cached = pinned + unpinned;
    let mut remote_files = vec![];
    for id in 0..=cached {
        let name = format!("remote-{id}.parquet");
        remote_files.push(create_test_file(remote_dir.path(), &name).await);
    }
    let remote = |id: usize| remote_files[id].to_str().unwrap().to_string();

    let mut handles = vec![];
    for id in 0..cached {
        let (handle, _) = cache
            .get_cache_entry_pinned(
                get_table_unique_file_id(id as u64),
                &remote(id),
                filesystem_accessor.as_ref(),
            )
            .await
            .unwrap();
        handles.push(handle);
    }
    let mut unpinned_files = vec![];
    for handle in handles.drain(..unpinned) {
        unpinned_files.push(handle.get_cache_filepath().to_string());
        assert!(handle.unreference().await.is_empty());
    }
    assert_evictable_cache_size(&mut cache, unpinned).await;
    assert_cache_bytes_size(&mut cache, (cached * CONTENT.len()) as u64).await;

    let (handle, evicted) = cache
        .get_cache_entry(
            get_table_unique_file_id(cached as u64),
            &remote(cached),
            filesystem_accessor.as_ref(),
        )
        .await
        .unwrap();
    assert!(handle.is_none());
    assert_cache_bytes_size(&mut cache, (expected_cached * CONTENT.len()) as u64).await;
    assert_evictable_cache_size(&mut cache, expected_cached - pinned).await;
    // Least recently used first.
    assert_eq!(
        evicted.into_vec(),
        unpinned_files[..cached - expected_cached].to_vec()
    );
    for file in &unpinned_files[..cached - expected_cached] {
        std::fs::remove_file(file).unwrap();
    }
    // The download that could not be cached is deleted, leaving only the cached files.
    assert_eq!(
        std::fs::read_dir(cache_dir.path()).unwrap().count(),
        expected_cached
    );
}

/// Pinned entries alone exceed the budget: every evictable entry goes, the pinned excess stays.
#[tokio::test]
async fn test_unpinned_lookup_with_pinned_entries_over_budget() {
    let file = CONTENT.len() as u64;
    check_unpinned_lookup_over_budget(
        2 * file,
        /*pinned=*/ 3,
        /*unpinned=*/ 1,
        /*expected_cached=*/ 3,
    )
    .await;
}

/// The pinned entries fit, the new file doesn't: evictable entries go until the cache fits.
#[tokio::test]
async fn test_unpinned_lookup_that_does_not_fit_trims_to_budget() {
    let file = CONTENT.len() as u64;
    check_unpinned_lookup_over_budget(
        file * 5 / 2,
        /*pinned=*/ 2,
        /*unpinned=*/ 2,
        /*expected_cached=*/ 2,
    )
    .await;
}
