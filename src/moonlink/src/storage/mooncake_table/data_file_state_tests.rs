use tempfile::TempDir;
use tokio::sync::mpsc::Receiver;

/// This file contains state-machine based unit test for data files and file index, which checks their state transfer for a few operations, for example, compaction and request read.
///
/// ====================================
/// State machine for data file
/// ====================================
///
/// remote storage state:
/// - Has remote storage
/// - no remote storage
///
/// Local cache state:
/// - Local cache pinned
/// - No local cache (including unreferenced)
///
/// Use request state (use includes request to read or compact, which adds reference count):
/// - Requested to use
/// - Not requested to use
///
/// Constraint:
/// - Impossible to have “no remote storage” and “no local cache”
/// - Local cache pinned indicates either “no remote storage” or “request to use”
///
/// Possible states for disk file entries:
/// (0) No such entry
/// (1) Has remote storage, no local cache, not used
/// (2) Has remote storage, no local cache, in use
/// (3) Has remote storage, local cache pinned, in use
/// (4) no remote storage, local cache pinned, in use
/// (5) no remote storage, local cache pinned, not used
///
/// State inputs:
/// - Iceberg snapshot completion
/// - Request to use, and pinned in cache
/// - Request to use, but not pinned in cache
/// - Use finished, still pinned in cache
/// - Use finished, but not pinned in cache
///
/// State transition for disk file entries:
/// Initial state: no entry
/// - No entry + disk write => no remote, local, not used
///
/// Initial state: no remote, local, not used
/// - no remote, local, not used + use => no remote, local, in use
/// - no remote, local, not used + persist => remote, no local, not used
///
/// Initial state: no remote, local, in use
/// - no remote, local, in use + persist => remote, local, in use
/// - no remote, local, in use + use => no remote, local, in use
/// - no remote, local, in use + use over => no remote, local, in use
///
/// Initial state: remote, local, in use
/// - remote, local, in use + use => remote, local, in use
/// - remote, local, in use + use over & pinned => remote, local, in use
/// - remote, local, in use + use over & unpinned => remote, no local, not used
///
/// Initial state: remote, no local, not used
/// - remote, no local, not used + use & pinned => remote, local, in use
/// - remote, no local, not used + use & unpinned => remote, no local, in use
///
/// Initial state: remote, no local, in use
/// - remote, no local, in use + use & pinned => remote, local, in use
/// - remote, no local, in use + use & unpinned => remote, no local, in use
/// - remote, no local, in use + use over => remote, no local, not used
///
/// For more details, please refer to https://docs.google.com/document/d/1f2d0E_Zi8FbR4QmW_YEhcZwMpua0_pgkaNdrqM1qh2E/edit?usp=sharing
///
/// File indices share most of the operations with data files;
/// for example, they're created at disk slice / stream transaction, persist to iceberg, perform data compaction, etc;
/// so we combine the test state-machine for file indices and data files.
use crate::row::{MoonlinkRow, RowValue};
use crate::storage::mooncake_table::state_test_utils::*;
use crate::table_notify::TableNotify;
use crate::{MooncakeTable, ObjectStorageCache, ObjectStorageCacheConfig};

/// ========================
/// Test util function for read
/// ========================
///
/// Prepare persisted data files in mooncake table.
/// Rows are committed and flushed with LSN 1.
async fn prepare_test_disk_file_for_read(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableNotify>) {
    let (mut table, table_notify) =
        create_mooncake_table_and_notify_for_read(temp_dir, object_storage_cache).await;

    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    table.flush(/*lsn=*/ 1).await.unwrap();

    (table, table_notify)
}

/// Test scenario: when shutdown table, all cache entries should be unpinned.
#[tokio::test]
async fn test_shutdown_table() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Shutdown the table, which unreferences all cache handles in the snapshot.
    table.shutdown().await.unwrap();

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        1
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        1, // index block
    );
}

/// ========================
/// Use by read
/// ========================
///
/// Test scenario: no remote, local, not used + use => no remote, local, in use
#[tokio::test]
async fn test_5_read_4() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());
    let snapshot_read_output = table.request_read().await.unwrap();
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_some());
    assert_eq!(
        file.file_path(),
        &disk_file_entry
            .cache_handle
            .as_ref()
            .unwrap()
            .cache_entry
            .cache_filepath
    );
    assert!(is_local_file(file, &temp_dir));

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // data file and index block
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(file.file_id()))
            .await,
        2
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // data file and index block
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(file.file_id()))
            .await,
        1
    );
}

/// Test scenario: no remote, local, not used + persist => remote, no local, not used
#[tokio::test]
async fn test_5_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    // Till now, iceberg snapshot has been persisted, need an extra mooncake snapshot to reflect persistence result.
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_none());
    assert!(is_remote_file(file, &temp_dir));

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        1
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        1, // index block
    );
}

/// Test scenario: no remote, local, in use + persist => remote, local, in use
#[tokio::test]
async fn test_4_3() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output = table.request_read().await.unwrap();
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Create iceberg snapshot and reflect persistence result to mooncake snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_none());
    assert!(is_remote_file(file, &temp_dir));

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // data file and index block
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(file.file_id()))
            .await,
        1
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        1
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        1, // index block
    );
}

/// Test scenario: no remote, local, in use + use => no remote, local, in use
#[tokio::test]
async fn test_4_read_4() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count for the first time.
    let snapshot_read_output_1 = table.request_read().await.unwrap();
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;

    // Read and increment reference count for the second time.
    let snapshot_read_output_2 = table.request_read().await.unwrap();
    let snapshot_read_output_2_clone = snapshot_read_output_2.clone();
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;

    // Read and increment reference count for the third time.
    let read_state_3 = snapshot_read_output_2_clone.take_as_read_state().await;

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_some());
    assert!(is_local_file(file, &temp_dir));

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // data file and index block
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(file.file_id()))
            .await,
        4,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1, read_state_2, read_state_3],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // data file and index block
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(file.file_id()))
            .await,
        1,
    );
}

/// Test scenario: no remote, local, in use + use over => no remote, local, in use
#[tokio::test]
async fn test_4_read_and_read_over_4() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Read, increment reference count and drop to declare finish.
    let snapshot_read_output = table.request_read().await.unwrap();
    let read_state = snapshot_read_output.take_as_read_state().await;
    drop(read_state);
    table.sync_read_request(&mut table_notify).await;

    // Create a mooncake snapshot to reflect read request completion result.
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_some());
    assert!(is_local_file(file, &temp_dir));

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // data file and index block
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(file.file_id()))
            .await,
        1
    );
}

/// Test scenario: remote, local, in use + use => remote, local, in use
#[tokio::test]
async fn test_3_read_3() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output_1 = table.request_read().await.unwrap();
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;

    // Create iceberg snapshot and reflect persistence result to mooncake snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output_2 = table.request_read().await.unwrap();
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_none());
    assert!(is_remote_file(file, &temp_dir));

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // data file and index block
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(file.file_id()))
            .await,
        2,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1, read_state_2],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        1
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        1, // index block
    );
}

/// Test scenario: remote, local, in use + use over & pinned => remote, local, in use
#[tokio::test]
async fn test_3_read_and_read_over_and_pinned_3() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output_1 = table.request_read().await.unwrap();
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;

    // Create iceberg snapshot and reflect persistence result to mooncake snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output_2 = table.request_read().await.unwrap();
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;
    drop(read_state_2);
    table.sync_read_request(&mut table_notify).await;

    // Create a mooncake snapshot to reflect read request completion result.
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_none());
    assert!(is_remote_file(file, &temp_dir));

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // data file and index block
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(file.file_id()))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        1
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        1, // index block
    );
}

/// Test scenario: remote, local, in use + use over & unpinned => remote, no local, not used
#[tokio::test]
async fn test_3_read_and_read_over_and_unpinned_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Create iceberg snapshot and reflect persistence result to mooncake snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output = table.request_read().await.unwrap();
    let read_state = snapshot_read_output.take_as_read_state().await;
    drop(read_state);
    table.sync_read_request(&mut table_notify).await;

    // Create a mooncake snapshot to reflect read request completion result.
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_none());
    assert!(is_remote_file(file, &temp_dir));

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        1
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        1, // index block
    );
}

/// Test scenario: remote, no local, not used + use & pinned => remote, local, in use
#[tokio::test]
async fn test_1_read_and_pinned_3() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Create iceberg snapshot and reflect persistence result to mooncake snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output = table.request_read().await.unwrap();
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_none());
    assert!(is_remote_file(file, &temp_dir));

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // data file and index block
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(file.file_id()))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        1
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        1, // index block
    );
}

/// Test scenario: remote, no local, not used + use & unpinned => remote, no local, not used
#[tokio::test]
async fn test_1_read_and_unpinned_3() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        ONE_FILE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let mut object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Create iceberg snapshot and reflect persistence result to mooncake snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Import second data file into cache, so the cached entry will be evicted.
    import_fake_cache_entry(&temp_dir, &mut object_storage_cache).await;

    // Read and increment reference count.
    let snapshot_read_output = table.request_read().await.unwrap();
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_none());
    assert!(is_remote_file(file, &temp_dir));

    // Check cache state.
    check_file_not_pinned(&object_storage_cache, file.file_id()).await;
    check_file_pinned(&object_storage_cache, FAKE_FILE_ID.file_id).await;

    // Drop all read states and check reference count.
    drop_read_states(vec![read_state], &mut table, &mut table_notify).await;
    check_file_not_pinned(&object_storage_cache, file.file_id()).await;
    check_file_pinned(&object_storage_cache, FAKE_FILE_ID.file_id).await;
}

/// Test scenario: remote, no local, in use + use & pinned => remote, local, in use
#[tokio::test]
async fn test_2_read_and_pinned_3() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        ONE_FILE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let mut object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Create iceberg snapshot and reflect persistence result to mooncake snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Import second data file into cache, so the cached entry will be evicted.
    let mut fake_cache_handle = import_fake_cache_entry(&temp_dir, &mut object_storage_cache).await;

    // Read, but no reference count hold within read state.
    let snapshot_read_output_1 = table.request_read().await.unwrap();
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;
    // Till now, the state is (remote, no local, in use).

    // Unreference the second cache handle, so we could pin requires files again in cache.
    let evicted_files_to_delete = fake_cache_handle.unreference().await;
    assert!(evicted_files_to_delete.is_empty());

    let snapshot_read_output_2 = table.request_read().await.unwrap();
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;

    // Check fake file has been evicted.
    let fake_filepath = temp_dir.path().join(FAKE_FILE_NAME);
    table
        .sync_delete_evicted_files(
            &mut table_notify,
            vec![fake_filepath.to_str().unwrap().to_string()],
        )
        .await;

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_none());
    assert!(is_remote_file(file, &temp_dir));

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // data file and index block
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(file.file_id()))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1, read_state_2],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        1, // data file
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        1, // index block
    );
}

/// Test scenario: remote, no local, in use + use & unpinned => remote, no local, in use
#[tokio::test]
async fn test_2_read_and_unpinned_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        ONE_FILE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let mut object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Create iceberg snapshot and reflect persistence result to mooncake snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Import second data file into cache, so the cached entry will be evicted.
    import_fake_cache_entry(&temp_dir, &mut object_storage_cache).await;

    // Read, but no reference count hold within read state.
    let snapshot_read_output_1 = table.request_read().await.unwrap();
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;
    // Till now, the state is (remote, no local, in use).

    // Read, but no reference count hold within read state.
    let snapshot_read_output_2 = table.request_read().await.unwrap();
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_none());
    assert!(is_remote_file(file, &temp_dir));

    // Check cache state.
    check_file_not_pinned(&object_storage_cache, file.file_id()).await;
    check_file_pinned(&object_storage_cache, FAKE_FILE_ID.file_id).await;

    // Drop all read states and check reference count; cache only manages fake file here.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1, read_state_2],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    check_file_not_pinned(&object_storage_cache, file.file_id()).await;
    check_file_pinned(&object_storage_cache, FAKE_FILE_ID.file_id).await;
}

/// Test scenario: remote, no local, in use + use over => remote, no local, not used
#[tokio::test]
async fn test_2_read_over_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        ONE_FILE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let mut object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Create iceberg snapshot and reflect persistence result to mooncake snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Import second data file into cache, so the cached entry will be evicted.
    import_fake_cache_entry(&temp_dir, &mut object_storage_cache).await;

    // Read and increment reference count.
    let snapshot_read_output = table.request_read().await.unwrap();
    let read_state = snapshot_read_output.take_as_read_state().await;
    // Till now, the state is (remote, no local, in use).

    drop(read_state);

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_none());
    assert!(is_remote_file(file, &temp_dir));

    // Check cache state.
    check_file_not_pinned(&object_storage_cache, file.file_id()).await;
    check_file_pinned(&object_storage_cache, FAKE_FILE_ID.file_id).await;
}

/// There're two things different from use for read:
/// - we can only check data compaction after its completion
/// - data compaction only happens when there's remote path
///
/// Possible state transfer:
/// - remote, local, in use + use + use over & pinned => (old) remote, local, in use, (new) no remote, local, no use
/// - remote, local, in use + use + use over & unpinned => (old) remote, local, not used, (new) no remote, local, no use
/// - remote, no local, not used + use & pinned + use over & pinned => (old) remote, local, in use, (new) no remote, local, no use
/// - remote, no local, not used + use & pinned + use over & unpinned => (old) remote, no local, not used, (new) no remote, local, no use
/// - remote, no local, in use + use + use over => (old) remote, no local, in use, (new) no remote, local, no use
///
/// ========================
/// Test util function for compaction
/// ========================
///
/// Test util function to create two data files for compaction.
/// Rows are committed and flushed with LSN 1 and 2 respectively.
async fn prepare_test_disk_files_for_compaction(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableNotify>) {
    let (mut table, table_notify) =
        create_mooncake_table_and_notify_for_compaction(temp_dir, object_storage_cache).await;

    // Append, commit and flush the first row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    table.flush(/*lsn=*/ 1).await.unwrap();

    // Append, commit and flush the second row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 2);
    table.flush(/*lsn=*/ 2).await.unwrap();

    (table, table_notify)
}

/// ========================
/// Use by compaction
/// ========================
///
/// Test scenario: remote, local, in use + use + use over & pinned => (old) remote, local, in use, (new) no remote, local, no use
#[tokio::test]
async fn test_3_compact_3_5() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_files_for_compaction(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Get old compacted files before compaction.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 2);
    let old_compacted_data_files = disk_files.keys().cloned().collect::<Vec<_>>();
    let old_compacted_index_block_files = table.get_index_block_files().await;

    // Read and increment reference count.
    let snapshot_read_output = table.request_read().await.unwrap();
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Create iceberg snapshot and reflect persistence result to mooncake snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, data_compaction_payload, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());
    assert!(data_compaction_payload.is_some());

    // Perform data compaction: use pinned local cache file and unreference.
    let mut evicted_files_to_delete = table
        .perform_data_compaction_for_test(&mut table_notify, data_compaction_payload.unwrap())
        .await;
    evicted_files_to_delete.sort();
    assert_eq!(evicted_files_to_delete, old_compacted_index_block_files);

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (new_compacted_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_some());
    assert!(is_local_file(new_compacted_file, &temp_dir));
    let new_compacted_data_file_size = disk_file_entry.file_size;
    let new_compacted_index_block_size = table.get_index_block_files_size().await;

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    // Two old compacted data files, one new compacted data file, and one new compacted index block
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        4,
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_file.file_id()
            ))
            .await,
        1,
    );
    for cur_old_compacted_file in old_compacted_data_files.iter() {
        assert_eq!(
            object_storage_cache
                .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                    cur_old_compacted_file.file_id()
                ))
                .await,
            1,
        );
    }

    // Drop all read states and check reference count and evicted files to delete.
    let mut actual_files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    actual_files_to_delete.sort();
    let mut expected_files_to_delete = old_compacted_data_files
        .iter()
        .map(|f| f.file_path().clone())
        .collect::<Vec<_>>();
    expected_files_to_delete.sort();
    assert_eq!(actual_files_to_delete, expected_files_to_delete);

    // Check cache status.
    assert_eq!(
        object_storage_cache.cache.read().await.cur_bytes,
        (new_compacted_data_file_size as u64) + new_compacted_index_block_size,
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evicted_entries
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // data file and index block
    );
}

/// Test scenario: remote, local, in use + use + use over & unpinned => (old) remote, no local, not used, (new) no remote, local, no use
#[tokio::test]
async fn test_3_compact_1_5() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_files_for_compaction(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Get old compacted files before compaction.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 2);
    let mut old_compacted_data_files = disk_files
        .keys()
        .map(|f| f.file_path().clone())
        .collect::<Vec<_>>();
    old_compacted_data_files.sort();

    let mut old_compacted_index_block_files = table.get_index_block_files().await;
    old_compacted_index_block_files.sort();

    // Read and increment reference count.
    let snapshot_read_output = table.request_read().await.unwrap();
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Create iceberg snapshot and reflect persistence result to mooncake snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, data_compaction_payload, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());
    assert!(data_compaction_payload.is_some());

    // Perform data compaction: use pinned local cache file and unreference.
    let mut evicted_files_to_delete = table
        .perform_data_compaction_for_test(&mut table_notify, data_compaction_payload.unwrap())
        .await;
    evicted_files_to_delete.sort();
    assert_eq!(evicted_files_to_delete, old_compacted_index_block_files);

    // Drop read state, so old data files are unreferenced any more.
    let mut files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, old_compacted_data_files);

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (new_compacted_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_some());
    assert!(is_local_file(new_compacted_file, &temp_dir));
    let new_compacted_data_file_size = disk_file_entry.file_size;
    let new_compacted_file_index_size = table.get_index_block_files_size().await;

    // Check cache state.
    assert_eq!(
        object_storage_cache.cache.read().await.cur_bytes,
        (new_compacted_data_file_size as u64) + new_compacted_file_index_size,
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evicted_entries
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // data file and index block.
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_file.file_id()
            ))
            .await,
        1,
    );
}

/// Test scenario: remote, no local, not used + use & pinned + use over & unpinned => (old) remote, no local, not used, (new) no remote, local, no use
#[tokio::test]
async fn test_1_compact_1_5() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        ONE_FILE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let mut object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_files_for_compaction(&temp_dir, object_storage_cache.clone()).await;
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Get old compacted files before compaction.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 2);

    // Create iceberg snapshot and reflect persistence result to mooncake snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, data_compaction_payload, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());
    assert!(data_compaction_payload.is_some());

    // Import second data file into cache, so the cached entry will be evicted.
    let mut fake_cache_handle = import_fake_cache_entry(&temp_dir, &mut object_storage_cache).await;
    let evicted_files_to_delete = fake_cache_handle.unreference().await;
    assert!(evicted_files_to_delete.is_empty());

    // Perform data compaction: use remote file to perform compaction.
    let evicted_files_to_delete = table
        .perform_data_compaction_for_test(&mut table_notify, data_compaction_payload.unwrap())
        .await;
    // It contains one fake file, and two downloaded local file and their file indices.
    assert_eq!(evicted_files_to_delete.len(), 5);

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (new_compacted_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_some());
    assert!(is_local_file(new_compacted_file, &temp_dir));
    let new_compacted_data_file_size = disk_file_entry.file_size;
    let file_indices = table.get_index_block_files().await;
    assert_eq!(file_indices.len(), 1);
    let new_compacted_index_block_size = table.get_index_block_files_size().await;

    // Check cache state.
    assert_eq!(
        object_storage_cache.cache.read().await.cur_bytes,
        (new_compacted_data_file_size as u64) + new_compacted_index_block_size,
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evicted_entries
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // data file and index block
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_file.file_id()
            ))
            .await,
        1,
    );
}
