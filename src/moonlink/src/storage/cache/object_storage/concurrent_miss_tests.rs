//! Lookups of the same file that miss the cache at the same time (e.g. a scan building its read
//! state while background compaction reads the same data file) share one cache entry.
use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::cache::object_storage::cache_config::ObjectStorageCacheConfig;
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
use crate::storage::cache::object_storage::test_utils::*;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;

use tempfile::{tempdir, TempDir};

fn cache(
    cache_dir: &TempDir,
    max_bytes: u64,
    optimize_local_filesystem: bool,
) -> ObjectStorageCache {
    ObjectStorageCache::new(ObjectStorageCacheConfig {
        max_bytes,
        cache_directory: cache_dir.path().to_str().unwrap().to_string(),
        optimize_local_filesystem,
    })
}

/// Two lookups both load the file before either caches it; the second pins the first one's entry.
async fn check_concurrent_misses_share_one_entry(optimize_local_filesystem: bool) {
    let remote_dir = tempdir().unwrap();
    let cache_dir = tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&remote_dir);
    let mut cache = cache(&cache_dir, 1 << 20, optimize_local_filesystem).line_up_loads(2);
    let remote = create_test_file(remote_dir.path(), TEST_REMOTE_FILENAME_1).await;
    let remote = remote.to_str().unwrap();
    let file_id = get_table_unique_file_id(0);

    let (unpinned, pinned) = tokio::join!(
        cache.get_cache_entry(file_id, remote, filesystem_accessor.as_ref()),
        cache.get_cache_entry_pinned(file_id, remote, filesystem_accessor.as_ref()),
    );
    let (unpinned, evicted) = unpinned.unwrap();
    assert!(evicted.is_empty());
    let unpinned = unpinned.expect("a lookup within the budget is cached");
    let (pinned, evicted) = pinned.unwrap();
    assert!(evicted.is_empty());

    assert_eq!(unpinned.get_cache_filepath(), pinned.get_cache_filepath());
    check_file_content(pinned.get_cache_filepath()).await;
    assert_non_evictable_cache_handle_ref_count(&mut cache, file_id, 2).await;
    assert_cache_bytes_size(&mut cache, CONTENT.len() as u64).await;
    if !optimize_local_filesystem {
        // The second download was deleted.
        assert_eq!(std::fs::read_dir(cache_dir.path()).unwrap().count(), 1);
    }

    assert!(unpinned.unreference().await.is_empty());
    assert!(pinned.unreference().await.is_empty());
    assert_non_evictable_cache_size(&mut cache, 0).await;
    assert_evictable_cache_size(&mut cache, 1).await;
    assert_cache_bytes_size(&mut cache, CONTENT.len() as u64).await;
}

#[tokio::test]
async fn test_concurrent_misses_share_one_entry() {
    check_concurrent_misses_share_one_entry(/*optimize_local_filesystem=*/ false).await;
}

#[tokio::test]
async fn test_concurrent_misses_share_one_entry_with_local_optimization() {
    check_concurrent_misses_share_one_entry(/*optimize_local_filesystem=*/ true).await;
}

/// Many tasks look up and release a few shared files under a small budget, so lookups race on
/// the same file while entries are evicted and re-pinned. Afterwards nothing is pinned and the
/// byte counts agree.
async fn stress_shared_files(optimize_local_filesystem: bool) {
    const FILES: u64 = 64;
    const TASKS: u64 = 16;
    const OPERATIONS: usize = 400;
    let remote_dir = tempdir().unwrap();
    let cache_dir = tempdir().unwrap();
    for i in 0..FILES {
        create_test_file(remote_dir.path(), &format!("{i}.parquet")).await;
    }
    // Room for 20 of the 10-byte files: lookups keep evicting and overflowing.
    let cache = cache(&cache_dir, 200, optimize_local_filesystem);
    let filesystem_accessor = FileSystemAccessor::default_for_test(&remote_dir);
    let mut tasks = Vec::new();
    for t in 0..TASKS {
        let cache = cache.clone();
        let filesystem_accessor = filesystem_accessor.clone();
        let remote_dir = remote_dir.path().to_path_buf();
        tasks.push(tokio::spawn(async move {
            let mut rng = t.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut held = Vec::new();
            for _ in 0..OPERATIONS {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let i = rng % FILES;
                if (rng >> 32) % 8 < 4 {
                    let path = remote_dir.join(format!("{i}.parquet"));
                    let (handle, _) = cache
                        .get_cache_entry(
                            get_table_unique_file_id(i),
                            path.to_str().unwrap(),
                            filesystem_accessor.as_ref(),
                        )
                        .await
                        .unwrap();
                    held.extend(handle);
                } else if !held.is_empty() {
                    let handle = held.swap_remove((rng as usize >> 8) % held.len());
                    // Evicted downloads would be deleted by the caller; the temp dir goes anyway.
                    let _evicted_files = handle.unreference().await;
                }
                cache.cache.read().await.assert_evictable_bytes_consistent();
            }
            for handle in held {
                let _evicted_files = handle.unreference().await;
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let guard = cache.cache.read().await;
    guard.assert_evictable_bytes_consistent();
    assert!(guard.non_evictable_cache.is_empty(), "pins left");
    let evictable_bytes: u64 = guard
        .evictable_cache
        .iter()
        .map(|(_, entry)| entry.cache_entry.file_metadata.file_size)
        .sum();
    assert_eq!(evictable_bytes, guard.cur_bytes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_shared_file_lookups_under_contention() {
    stress_shared_files(/*optimize_local_filesystem=*/ false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_shared_file_lookups_under_contention_with_local_optimization() {
    stress_shared_files(/*optimize_local_filesystem=*/ true).await;
}
