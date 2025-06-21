use std::sync::Arc;

use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::cache::object_storage::base_cache::{CacheEntry, FileMetadata};
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::iceberg::test_utils::*;
use crate::storage::index::persisted_bucket_hash_map::GlobalIndex;
use crate::storage::mooncake_table::TableConfig as MooncakeTableConfig;
use crate::storage::storage_utils::{FileId, MooncakeDataFileRef, TableId, TableUniqueFileId};
use crate::table_notify::TableNotify;
use crate::{IcebergTableConfig, MooncakeTable, NonEvictableHandle, ObjectStorageCache, ReadState};

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

/// Test util function to drop read states, and apply the synchronized response to mooncake table.
pub(super) async fn drop_read_states(
    read_states: Vec<Arc<ReadState>>,
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableNotify>,
) {
    for cur_read_state in read_states.into_iter() {
        drop(cur_read_state);
        table.sync_read_request(receiver).await;
    }
}
/// Test util function to drop read states and create a mooncake snapshot to reflect.
/// Return evicted files to delete.
pub(super) async fn drop_read_states_and_create_mooncake_snapshot(
    read_states: Vec<Arc<ReadState>>,
    table: &mut MooncakeTable,
    receiver: &mut Receiver<TableNotify>,
) -> Vec<String> {
    drop_read_states(read_states, table, receiver).await;
    let (_, _, _, files_to_delete) = table.create_mooncake_snapshot_for_test(receiver).await;
    files_to_delete
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

/// Test util function to create mooncake table and table notify for read test.
pub(super) async fn create_mooncake_table_and_notify_for_read(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableNotify>) {
    let path = temp_dir.path().to_path_buf();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = get_iceberg_table_config(temp_dir);
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        iceberg_snapshot_new_data_file_count: 0,
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

/// Test util function to create mooncake table and table notify for compaction test.
pub(super) async fn create_mooncake_table_and_notify_for_compaction(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableNotify>) {
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
        iceberg_snapshot_new_data_file_count: 0,
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
