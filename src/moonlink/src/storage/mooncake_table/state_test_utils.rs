use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

#[cfg(test)]
use crate::row::MoonlinkRow;
use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::cache::object_storage::base_cache::{CacheEntry, FileMetadata};
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::iceberg::test_utils::*;
use crate::storage::index::persisted_bucket_hash_map::GlobalIndex;
use crate::storage::mooncake_table::{
    DataCompactionPayload, DataCompactionResult, DiskFileEntry, FileIndiceMergePayload,
    FileIndiceMergeResult, IcebergPersistenceConfig, IcebergSnapshotPayload, IcebergSnapshotResult,
    MooncakeTableConfig, Snapshot, SnapshotOption,
};
use crate::storage::storage_utils::{
    FileId, MooncakeDataFileRef, ProcessedDeletionRecord, TableId, TableUniqueFileId,
};
use crate::storage::{io_utils, PuffinBlobRef};
use crate::table_notify::TableEvent;
use crate::{
    IcebergTableConfig, MooncakeTable, NonEvictableHandle, ObjectStorageCache,
    ObjectStorageCacheConfig, ReadState, SnapshotReadOutput,
};

/// This module contains util functions for state-based tests.
///
/// Test constant to mimic an infinitely large object storage cache.
pub(super) const INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE: u64 = u64::MAX;
/// File index blocks are always pinned at cache, whose size is much less than data file.
/// Test constant to allow only one data file and multiple index block files in object storage cache.
const INDEX_BLOCK_FILES_SIZE_UPPER_BOUND: u64 = 100;
pub(super) const ONE_FILE_CACHE_SIZE: u64 = FAKE_FILE_SIZE + INDEX_BLOCK_FILES_SIZE_UPPER_BOUND;
/// Iceberg test namespace and table name.
pub(super) const ICEBERG_TEST_NAMESPACE: &str = "namespace";
pub(super) const ICEBERG_TEST_TABLE: &str = "test_table";
/// Test constant for table id.
pub(super) const TEST_TABLE_ID: TableId = TableId(0);
/// File attributes for a fake file.
///
/// File id for the fake file.
pub(super) const FAKE_FILE_ID: TableUniqueFileId = TableUniqueFileId {
    table_id: TEST_TABLE_ID,
    file_id: FileId(100),
};
/// Fake file size.
pub(super) const FAKE_FILE_SIZE: u64 = 1 << 30; // 1GiB
/// Fake filename.
pub(super) const FAKE_FILE_NAME: &str = "fake-file-name";

/// Test util function to get unique table file id.
pub(super) fn get_unique_table_file_id(file_id: FileId) -> TableUniqueFileId {
    TableUniqueFileId {
        table_id: TEST_TABLE_ID,
        file_id,
    }
}

/// Test util function to decide whether a given file is remote file.
pub(super) fn is_remote_file(file: &MooncakeDataFileRef, temp_dir: &TempDir) -> bool {
    // Local filesystem directory for iceberg warehouse.
    let mut temp_pathbuf = temp_dir.path().to_path_buf();
    temp_pathbuf.push(ICEBERG_TEST_NAMESPACE); // iceberg namespace
    temp_pathbuf.push(ICEBERG_TEST_TABLE); // iceberg table name

    file.file_path().starts_with(temp_pathbuf.to_str().unwrap())
}

/// Test util function to decide whether a given file is local file.
pub(super) fn is_local_file(file: &MooncakeDataFileRef, temp_dir: &TempDir) -> bool {
    !is_remote_file(file, temp_dir)
}

/// Test util function to get fake file path.
pub(super) fn get_fake_file_path(temp_dir: &TempDir) -> String {
    temp_dir
        .path()
        .join(FAKE_FILE_NAME)
        .to_str()
        .unwrap()
        .to_string()
}

/// Test util function to import a second object storage cache entry.
pub(super) async fn import_fake_cache_entry(
    temp_dir: &TempDir,
    cache: &mut ObjectStorageCache,
) -> NonEvictableHandle {
    // Create a physical fake file, so later evicted files deletion won't fail.
    let fake_filepath = get_fake_file_path(temp_dir);
    let filepath = std::path::PathBuf::from(&fake_filepath);
    tokio::fs::File::create(&filepath).await.unwrap();

    let cache_entry = CacheEntry {
        cache_filepath: fake_filepath,
        file_metadata: FileMetadata {
            file_size: FAKE_FILE_SIZE,
        },
    };
    cache.import_cache_entry(FAKE_FILE_ID, cache_entry).await.0
}

/// Test util function to check certain data file doesn't exist in non evictable cache.
pub(super) async fn check_file_not_pinned(
    object_storage_cache: &ObjectStorageCache,
    file_id: FileId,
) {
    let non_evicted_file_ids = object_storage_cache.get_non_evictable_filenames().await;
    let table_unique_file_id = get_unique_table_file_id(file_id);
    assert!(!non_evicted_file_ids.contains(&table_unique_file_id));
}

/// Test util function to check certain data file exists in non evictable cache.
pub(super) async fn check_file_pinned(object_storage_cache: &ObjectStorageCache, file_id: FileId) {
    let non_evicted_file_ids = object_storage_cache.get_non_evictable_filenames().await;
    let table_unique_file_id = get_unique_table_file_id(file_id);
    assert!(non_evicted_file_ids.contains(&table_unique_file_id));
}

/// Test util function to get iceberg table config.
pub(super) fn get_iceberg_table_config(temp_dir: &TempDir) -> IcebergTableConfig {
    IcebergTableConfig {
        warehouse_uri: temp_dir.path().to_str().unwrap().to_string(),
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
    }
}

/// Test util function to create mooncake table and table notify for compaction test.
pub(super) async fn create_mooncake_table_and_notify_for_compaction(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableEvent>) {
    let path = temp_dir.path().to_path_buf();
    let warehouse_uri = path.clone().to_str().unwrap().to_string();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = IcebergTableConfig {
        warehouse_uri,
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
    };
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 0,
            ..Default::default()
        },
        // Trigger compaction as long as there're two data files.
        data_compaction_config: DataCompactionConfig {
            data_file_final_size: u64::MAX,
            data_file_to_compact: 2,
        },
        ..Default::default()
    };

    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*version=*/ TEST_TABLE_ID.0,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
        object_storage_cache,
    )
    .await
    .unwrap();

    let (notify_tx, notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    (table, notify_rx)
}

/// Test util function to get index block files, and the overall file size.
pub(super) fn get_index_block_files(
    file_indices: Vec<GlobalIndex>,
) -> (Vec<MooncakeDataFileRef>, u64) {
    let mut index_block_files = vec![];
    let mut overall_file_size = 0;
    for cur_file_index in file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            index_block_files.push(cur_index_block.index_file.clone());
            overall_file_size += cur_index_block.file_size;
        }
    }
    (index_block_files, overall_file_size)
}

/// ===================================
/// Cache utils
///  ===================================
///
/// Test util function to create an infinitely large object storage cache.
pub(super) fn create_infinite_object_storage_cache(
    temp_dir: &TempDir,
    optimize_local_filesystem: bool,
) -> ObjectStorageCache {
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
        optimize_local_filesystem,
    );
    ObjectStorageCache::new(cache_config)
}

/// Test util function to create an object storage cache, with size of only one file.
pub(super) fn create_object_storage_cache_with_one_file_size(
    temp_dir: &TempDir,
    optimize_local_filesystem: bool,
) -> ObjectStorageCache {
    let cache_config = ObjectStorageCacheConfig::new(
        ONE_FILE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
        optimize_local_filesystem,
    );
    ObjectStorageCache::new(cache_config)
}

/// ===================================
/// Request read
/// ===================================
///
/// Test util function to drop read states, and apply the synchronized response to mooncake table.
pub(super) async fn drop_read_states(
    read_states: Vec<Arc<ReadState>>,
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) {
    for cur_read_state in read_states.into_iter() {
        drop(cur_read_state);
        sync_read_request_for_test(table, receiver).await;
    }
}

/// Test util function to drop read states and create a mooncake snapshot to reflect.
/// Return evicted files to delete.
pub(super) async fn drop_read_states_and_create_mooncake_snapshot(
    read_states: Vec<Arc<ReadState>>,
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) -> Vec<String> {
    drop_read_states(read_states, table, receiver).await;
    let (_, _, _, files_to_delete) = create_mooncake_snapshot_for_test(table, receiver).await;
    files_to_delete
}

/// Test util function to create mooncake table and table notify for read test.
pub(super) async fn create_mooncake_table_and_notify_for_read(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableEvent>) {
    let path = temp_dir.path().to_path_buf();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = get_iceberg_table_config(temp_dir);
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 0,
            ..Default::default()
        },
        ..Default::default()
    };

    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*version=*/ TEST_TABLE_ID.0,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
        object_storage_cache,
    )
    .await
    .unwrap();

    let (notify_tx, notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    (table, notify_rx)
}

/// ===================================
/// Accessors for mooncake table
/// ===================================
///
/// Test util function to get disk files for the given mooncake table.
pub(crate) async fn get_disk_files_for_snapshot(
    table: &MooncakeTable,
) -> HashMap<MooncakeDataFileRef, DiskFileEntry> {
    let guard = table.snapshot.read().await;
    guard.current_snapshot.disk_files.clone()
}

/// Test util function to get sorted data file filepaths for the given mooncake table, and assert on expected disk file number.
pub(crate) async fn get_disk_files_for_snapshot_and_assert(
    table: &MooncakeTable,
    expected_file_num: usize,
) -> Vec<String> {
    let guard = table.snapshot.read().await;
    assert_eq!(guard.current_snapshot.disk_files.len(), expected_file_num);
    let data_files = guard
        .current_snapshot
        .disk_files
        .keys()
        .map(|f| f.file_path().to_string())
        .sorted()
        .collect::<Vec<_>>();
    data_files
}

/// Test util to get all index block file ids for the table, and assert there's only one file.
pub(super) async fn get_only_index_block_file_id(
    table: &MooncakeTable,
    temp_dir: &TempDir,
    is_local: bool,
) -> FileId {
    let mut file_ids = vec![];

    let guard = table.snapshot.read().await;
    for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            assert!(cur_index_block.cache_handle.is_some());
            file_ids.push(cur_index_block.index_file.file_id());
            if is_local {
                assert!(is_local_file(&cur_index_block.index_file, temp_dir));
            } else {
                assert!(is_remote_file(&cur_index_block.index_file, temp_dir));
            }
        }
    }

    assert_eq!(file_ids.len(), 1);
    file_ids[0]
}

/// Test util to get all index block file ids for the table, and assert there's only one file.
pub(super) async fn get_only_index_block_filepath(table: &MooncakeTable) -> String {
    let mut index_block_filepaths = vec![];

    let guard = table.snapshot.read().await;
    for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            assert!(cur_index_block.cache_handle.is_some());
            index_block_filepaths.push(cur_index_block.index_file.file_path().to_string());
        }
    }

    assert_eq!(index_block_filepaths.len(), 1);
    index_block_filepaths[0].clone()
}

/// Test util to get the only puffin blob ref for the given mooncake snapshot.
pub(super) fn get_only_puffin_blob_ref_from_snapshot(snapshot: &Snapshot) -> PuffinBlobRef {
    let disk_files = snapshot.disk_files.clone();
    assert_eq!(disk_files.len(), 1);
    disk_files
        .iter()
        .next()
        .unwrap()
        .1
        .puffin_deletion_blob
        .as_ref()
        .unwrap()
        .clone()
}

/// Test util to get the only puffin blob ref for the given mooncake table.
pub(super) async fn get_only_puffin_blob_ref_from_table(table: &MooncakeTable) -> PuffinBlobRef {
    let guard = table.snapshot.read().await;
    let disk_files = guard.current_snapshot.disk_files.clone();
    assert_eq!(disk_files.len(), 1);
    disk_files
        .iter()
        .next()
        .unwrap()
        .1
        .puffin_deletion_blob
        .as_ref()
        .unwrap()
        .clone()
}

/// Test util to get data files and index block filepaths for the given mooncake table, filepaths returned in alphabetical order.
pub(super) async fn get_data_files_and_index_block_files(table: &MooncakeTable) -> Vec<String> {
    let mut files = vec![];

    let guard = table.snapshot.read().await;

    // Check and get data files.
    let disk_files = guard.current_snapshot.disk_files.clone();
    for (cur_file, _) in disk_files.iter() {
        files.push(cur_file.file_path().to_string());
    }

    // Check and get index block files.
    for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            files.push(cur_index_block.index_file.file_path().to_string());
        }
    }

    files.sort();
    files
}

///Test util function to assert there's only one data file in table snapshot, and it indicates remote file.
pub(super) async fn get_only_remote_data_file_id(
    table: &MooncakeTable,
    temp_dir: &TempDir,
) -> FileId {
    let guard = table.snapshot.read().await;
    let disk_files = &guard.current_snapshot.disk_files;
    assert_eq!(disk_files.len(), 1);
    let data_file = disk_files.iter().next().unwrap().0;
    assert!(is_remote_file(data_file, temp_dir));
    data_file.file_id()
}

/// Test util function to assert there's only one data file in table snapshot, and it indicates local file.
pub(super) async fn get_only_local_data_file_id(
    table: &MooncakeTable,
    temp_dir: &TempDir,
) -> FileId {
    let guard = table.snapshot.read().await;
    let disk_files = &guard.current_snapshot.disk_files;
    assert_eq!(disk_files.len(), 1);
    let (data_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_some());

    assert_eq!(
        data_file.file_path(),
        &disk_file_entry
            .cache_handle
            .as_ref()
            .unwrap()
            .cache_entry
            .cache_filepath
    );
    assert!(is_local_file(data_file, temp_dir));

    data_file.file_id()
}

/// Test util function to get committed and uncommitted deletion logs states.
pub(crate) async fn get_deletion_logs_for_snapshot(
    table: &MooncakeTable,
) -> (
    Vec<ProcessedDeletionRecord>,
    Vec<Option<ProcessedDeletionRecord>>,
) {
    let guard = table.snapshot.read().await;
    (
        guard.committed_deletion_log.clone(),
        guard.uncommitted_deletion_log.clone(),
    )
}

/// Test util function to get all index block filepaths from the given mooncake table, returned in alphabetic order.
pub(super) async fn get_index_block_filepaths(table: &MooncakeTable) -> Vec<String> {
    let guard = table.snapshot.read().await;
    let mut index_block_files = vec![];
    for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            index_block_files.push(cur_index_block.index_file.file_path().clone());
        }
    }

    index_block_files.sort();
    index_block_files
}

/// Test util function to get overall file size for all index block files from the given mooncake table.
pub(super) async fn get_index_block_files_size(table: &MooncakeTable) -> u64 {
    let guard = table.snapshot.read().await;
    let mut index_blocks_file_size = 0;
    for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            index_blocks_file_size += cur_index_block.file_size;
        }
    }
    index_blocks_file_size
}

/// Test util function to get index block file ids for the given mooncake table.
pub(super) async fn get_index_block_file_ids(table: &MooncakeTable) -> Vec<FileId> {
    let guard = table.snapshot.read().await;
    let mut index_block_files = vec![];
    for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
        for cur_index_block in cur_file_index.index_blocks.iter() {
            index_block_files.push(cur_index_block.index_file.file_id());
        }
    }
    index_block_files
}

/// Test util to get new compacted data file size and file id for the given mooncake table.
/// Assert the file is of local filepath, and there's only one new compacted data file.
pub(super) async fn get_new_compacted_local_file_size_and_id(
    table: &MooncakeTable,
    temp_dir: &TempDir,
) -> (usize, FileId) {
    let disk_files = get_disk_files_for_snapshot(table).await;
    assert_eq!(disk_files.len(), 1);
    let (new_compacted_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_some());
    assert!(is_local_file(new_compacted_file, temp_dir));
    let new_compacted_data_file_size = disk_file_entry.file_size;
    (new_compacted_data_file_size, new_compacted_file.file_id())
}

/// ===================================
/// Operation for mooncake table
/// ===================================
///
/// -------- Delete evicted files --------
/// Test util function to block wait delete request, and check whether matches expected data files.
#[cfg(test)]
pub(crate) async fn sync_delete_evicted_files(
    receiver: &mut Receiver<TableEvent>,
    mut expected_files_to_delete: Vec<String>,
) {
    let notification = receiver.recv().await.unwrap();
    if let TableEvent::EvictedDataFilesToDelete {
        mut evicted_data_files,
    } = notification
    {
        evicted_data_files.sort();
        expected_files_to_delete.sort();
        assert_eq!(evicted_data_files, expected_files_to_delete);
    } else {
        panic!("Receive other notifications other than delete evicted files")
    }
}

/// -------- Request read --------
/// Perform a read request for the given table.
pub(super) async fn perform_read_request_for_test(table: &mut MooncakeTable) -> SnapshotReadOutput {
    let mut guard = table.snapshot.write().await;
    guard.request_read().await.unwrap()
}

/// Block wait read request to finish, and set the result to the snapshot buffer.
/// Precondition: there's ongoing read request.
pub(super) async fn sync_read_request_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) {
    let notification = receiver.recv().await.unwrap();
    if let TableEvent::ReadRequest { cache_handles } = notification {
        table.set_read_request_res(cache_handles);
    } else {
        panic!("Receive other notifications other than read request")
    }
}

/// -------- Index merge --------
/// Perform an index merge for the given table, and reflect the result to snapshot.
/// Return evicted files to delete.
pub(crate) async fn perform_index_merge_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
    index_merge_payload: FileIndiceMergePayload,
) -> Vec<String> {
    // Perform and block wait index merge.
    table.perform_index_merge(index_merge_payload);
    let index_merge_result = sync_index_merge(receiver).await;

    table.set_file_indices_merge_res(index_merge_result);
    assert!(table.create_snapshot(SnapshotOption {
        force_create: true,
        skip_iceberg_snapshot: false,
        skip_file_indices_merge: false,
        skip_data_file_compaction: true,
    }));
    let (_, _, _, evicted_files_to_delete) = sync_mooncake_snapshot(receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(evicted_files_to_delete.clone())
        .await
        .unwrap();

    evicted_files_to_delete
}

/// -------- Data compaction --------
/// Perform data compaction for the given table, and reflect the result to snapshot.
/// Return evicted files to delete.
pub(crate) async fn perform_data_compaction_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
    data_compaction_payload: DataCompactionPayload,
) -> Vec<String> {
    // Perform and block wait data compaction.
    table.perform_data_compaction(data_compaction_payload);
    let data_compaction_result = sync_data_compaction(receiver).await;

    table.set_data_compaction_res(data_compaction_result);
    assert!(table.create_snapshot(SnapshotOption {
        force_create: true,
        skip_iceberg_snapshot: false,
        skip_file_indices_merge: true,
        skip_data_file_compaction: false,
    }));
    let (_, _, _, evicted_files_to_delete) = sync_mooncake_snapshot(receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(evicted_files_to_delete.clone())
        .await
        .unwrap();

    evicted_files_to_delete
}

/// ===================================
/// Operation synchronization function
/// ===================================
///
/// Test util function to block wait and get iceberg / file indices merge payload.
async fn sync_mooncake_snapshot(
    receiver: &mut Receiver<TableEvent>,
) -> (
    Option<IcebergSnapshotPayload>,
    Option<FileIndiceMergePayload>,
    Option<DataCompactionPayload>,
    Vec<String>,
) {
    let notification = receiver.recv().await.unwrap();
    if let TableEvent::MooncakeTableSnapshot {
        iceberg_snapshot_payload,
        file_indice_merge_payload,
        data_compaction_payload,
        evicted_data_files_to_delete,
        ..
    } = notification
    {
        (
            iceberg_snapshot_payload,
            file_indice_merge_payload,
            data_compaction_payload,
            evicted_data_files_to_delete,
        )
    } else {
        panic!("Expected mooncake snapshot completion notification, but get others.");
    }
}
async fn sync_iceberg_snapshot(receiver: &mut Receiver<TableEvent>) -> IcebergSnapshotResult {
    let notification = receiver.recv().await.unwrap();
    if let TableEvent::IcebergSnapshot {
        iceberg_snapshot_result,
    } = notification
    {
        iceberg_snapshot_result.unwrap()
    } else {
        panic!("Expected iceberg completion snapshot notification, but get mooncake one.");
    }
}
async fn sync_index_merge(receiver: &mut Receiver<TableEvent>) -> FileIndiceMergeResult {
    let notification = receiver.recv().await.unwrap();
    if let TableEvent::IndexMerge { index_merge_result } = notification {
        index_merge_result
    } else {
        panic!("Expected index merge completion notification, but get another one.");
    }
}
async fn sync_data_compaction(receiver: &mut Receiver<TableEvent>) -> DataCompactionResult {
    let notification = receiver.recv().await.unwrap();
    if let TableEvent::DataCompaction {
        data_compaction_result,
    } = notification
    {
        data_compaction_result.unwrap()
    } else {
        panic!("Expected data compaction completion notification, but get mooncake one.");
    }
}

/// ===================================
/// Composite util functions
/// ===================================
///
// Test util function, which creates mooncake snapshot for testing.
pub(crate) async fn create_mooncake_snapshot_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) -> (
    Option<IcebergSnapshotPayload>,
    Option<FileIndiceMergePayload>,
    Option<DataCompactionPayload>,
    Vec<String>,
) {
    let mooncake_snapshot_created = table.create_snapshot(SnapshotOption {
        force_create: true,
        skip_iceberg_snapshot: false,
        skip_data_file_compaction: false,
        skip_file_indices_merge: false,
    });
    assert!(mooncake_snapshot_created);
    sync_mooncake_snapshot(receiver).await
}

// Test util function, which updates mooncake table snapshot and create iceberg snapshot in a serial fashion.
pub(crate) async fn create_mooncake_and_persist_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) {
    // Create mooncake snapshot and block wait completion.
    let (iceberg_snapshot_payload, _, _, evicted_data_files_to_delete) =
        create_mooncake_snapshot_for_test(table, receiver).await;

    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(evicted_data_files_to_delete)
        .await
        .unwrap();

    // Create iceberg snapshot if possible.
    if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
        table.persist_iceberg_snapshot(iceberg_snapshot_payload);
        let iceberg_snapshot_result = sync_iceberg_snapshot(receiver).await;
        table.set_iceberg_snapshot_res(iceberg_snapshot_result);
    }
}

// Test util to block wait current mooncake snapshot completion, get the iceberg persistence payload, and perform a new mooncake snapshot and wait completion.
async fn sync_mooncake_snapshot_and_create_new_by_iceberg_payload(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) {
    let (iceberg_snapshot_payload, _, _, evicted_data_files_to_delete) =
        sync_mooncake_snapshot(receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(evicted_data_files_to_delete)
        .await
        .unwrap();

    let iceberg_snapshot_payload = iceberg_snapshot_payload.unwrap();
    table.persist_iceberg_snapshot(iceberg_snapshot_payload);
    let iceberg_snapshot_result = sync_iceberg_snapshot(receiver).await;
    table.set_iceberg_snapshot_res(iceberg_snapshot_result);

    // Create mooncake snapshot after buffering iceberg snapshot result, to make sure mooncake snapshot is at a consistent state.
    assert!(table.create_snapshot(SnapshotOption {
        force_create: true,
        skip_iceberg_snapshot: true,
        skip_file_indices_merge: true,
        skip_data_file_compaction: true,
    }));
}

// Test util function, which does the following things in serial fashion.
// (1) updates mooncake table snapshot, (2) create iceberg snapshot, (3) trigger data compaction, (4) perform data compaction, (5) another mooncake and iceberg snapshot.
//
// # Arguments
//
// * injected_committed_deletion_rows: rows to delete and commit in between data compaction initiation and snapshot creation
// * injected_uncommitted_deletion_rows: rows to delete but not commit in between data compaction initiation and snapshot creation
#[cfg(test)]
pub(crate) async fn create_mooncake_and_persist_for_data_compaction_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
    injected_committed_deletion_rows: Vec<(MoonlinkRow, u64 /*lsn*/)>,
    injected_uncommitted_deletion_rows: Vec<(MoonlinkRow, u64 /*lsn*/)>,
) {
    // Create mooncake snapshot.
    let force_snapshot_option = SnapshotOption {
        force_create: true,
        skip_iceberg_snapshot: false,
        skip_file_indices_merge: true,
        skip_data_file_compaction: false,
    };
    assert!(table.create_snapshot(force_snapshot_option.clone()));

    // Create iceberg snapshot.
    let (iceberg_snapshot_payload, _, _, evicted_data_files_to_delete) =
        sync_mooncake_snapshot(receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(evicted_data_files_to_delete)
        .await
        .unwrap();

    if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
        table.persist_iceberg_snapshot(iceberg_snapshot_payload);
        let iceberg_snapshot_result = sync_iceberg_snapshot(receiver).await;
        table.set_iceberg_snapshot_res(iceberg_snapshot_result);
    }

    // Get data compaction payload.
    assert!(table.create_snapshot(force_snapshot_option.clone()));
    let (iceberg_snapshot_payload, _, data_compaction_payload, evicted_data_files_to_delete) =
        sync_mooncake_snapshot(receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(evicted_data_files_to_delete)
        .await
        .unwrap();

    assert!(iceberg_snapshot_payload.is_none());
    let data_compaction_payload = data_compaction_payload.unwrap();

    // Perform and block wait data compaction.
    table.perform_data_compaction(data_compaction_payload);
    let data_compaction_result = sync_data_compaction(receiver).await;

    // Before create snapshot for compaction results, perform another deletion operations.
    for (cur_row, lsn) in injected_committed_deletion_rows {
        table.delete(cur_row, lsn - 1).await;
        table.commit(/*lsn=*/ lsn);
    }
    for (cur_row, lsn) in injected_uncommitted_deletion_rows {
        table.delete(cur_row, lsn - 1).await;
    }

    // Set data compaction result and trigger another iceberg snapshot.
    table.set_data_compaction_res(data_compaction_result);
    assert!(table.create_snapshot(SnapshotOption {
        force_create: true,
        skip_iceberg_snapshot: false,
        skip_file_indices_merge: true,
        skip_data_file_compaction: false,
    }));
    sync_mooncake_snapshot_and_create_new_by_iceberg_payload(table, receiver).await;
}

// Test util function, which does the following things in serial fashion.
// (1) updates mooncake table snapshot, (2) create iceberg snapshot, (3) trigger index merge, (4) perform index merge, (5) another mooncake and iceberg snapshot.
pub(crate) async fn create_mooncake_and_iceberg_snapshot_for_index_merge_for_test(
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableEvent>,
) {
    // Create mooncake snapshot.
    let force_snapshot_option = SnapshotOption {
        force_create: true,
        skip_iceberg_snapshot: false,
        skip_file_indices_merge: false,
        skip_data_file_compaction: false,
    };
    assert!(table.create_snapshot(force_snapshot_option.clone()));

    // Create iceberg snapshot.
    let (iceberg_snapshot_payload, _, _, evicted_data_files_to_delete) =
        sync_mooncake_snapshot(receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(evicted_data_files_to_delete)
        .await
        .unwrap();

    if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
        table.persist_iceberg_snapshot(iceberg_snapshot_payload);
        let iceberg_snapshot_result = sync_iceberg_snapshot(receiver).await;
        table.set_iceberg_snapshot_res(iceberg_snapshot_result);
    }

    // Perform index merge.
    assert!(table.create_snapshot(force_snapshot_option.clone()));
    let (iceberg_snapshot_payload, file_indice_merge_payload, _, evicted_data_files_to_delete) =
        sync_mooncake_snapshot(receiver).await;
    // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
    io_utils::delete_local_files(evicted_data_files_to_delete)
        .await
        .unwrap();

    assert!(iceberg_snapshot_payload.is_none());
    let file_indice_merge_payload = file_indice_merge_payload.unwrap();

    table.perform_index_merge(file_indice_merge_payload);
    let index_merge_result = sync_index_merge(receiver).await;
    table.set_file_indices_merge_res(index_merge_result);
    assert!(table.create_snapshot(SnapshotOption {
        force_create: true,
        skip_iceberg_snapshot: false,
        skip_file_indices_merge: false,
        skip_data_file_compaction: false,
    }));
    sync_mooncake_snapshot_and_create_new_by_iceberg_payload(table, receiver).await;
}
