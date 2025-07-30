/// ====================================
/// State machine for file indices
/// ====================================
///
/// Possible states:
/// (1) No file index
/// (2) No remote, local
/// (3) Remote, local
///
/// Constraint:
/// Only perform index merge when has remote path
///
/// Difference with data files:
/// - File index always sits on-disk
/// - Data file has an extra state: not referenced but not requested to deleted
/// - Current usage include only compaction and index merge; after all usage for file indices, they are requested to delete
/// - File indices won’t be used by both compaction and index merge, so no need to pin before usage
///
/// State transition input:
/// - Import into mooncake snapshot
/// - Persist into iceberg table
/// - Recover from iceberg table
/// - Use file index (i.e. index merge, compaction)
/// - Usage finishes + request to delete
///
/// State machine transfer:
/// Initial state: no file index
/// - No file index + import => no remote, local
/// - No file index + recover => no remote, local
///
/// Initial state: no remote, local
/// - No remote, local + persist => remote, local
///
/// Initial state: Remote, local
/// - Remote, local + use => remote, local
/// - Remote, local + use over + request delete => no file index
///
/// For more details, please refer to https://docs.google.com/document/d/1Q8zJqxwM9Gc5foX2ela8aAbW4bmWV8wBRkDSh86vvAY/edit?usp=sharing
///
/// Most of the state transitions are shared with data files, with only two differences:
/// - no file index + recover => no remote, local
/// - index merge related states
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

use crate::row::{MoonlinkRow, RowValue};
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::filesystem::storage_config::StorageConfig;
use crate::storage::index::index_merge_config::FileIndexMergeConfig;
use crate::storage::mooncake_table::table_accessor_test_utils::*;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use crate::storage::mooncake_table::test_utils_commons::*;
use crate::storage::mooncake_table::IcebergPersistenceConfig;
use crate::storage::mooncake_table::MooncakeTableConfig;
use crate::table_notify::TableEvent;
use crate::{
    IcebergTableConfig, IcebergTableManager, MooncakeTable, ObjectStorageCache,
    ObjectStorageCacheConfig, TableManager,
};

async fn prepare_test_disk_file(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableEvent>) {
    let (mut table, mut table_notify) =
        create_mooncake_table_and_notify_for_read(temp_dir, object_storage_cache).await;

    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    flush_table_and_sync(&mut table, &mut table_notify, /*lsn=*/ 1)
        .await
        .unwrap();

    (table, table_notify)
}

/// ========================
/// Recovery
/// ========================
///
/// Test scenario: no file index + recover => remote, local
#[tokio::test]
async fn test_1_recover_3() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
        /*optimize_local_filesystem=*/ false,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file(&temp_dir, ObjectStorageCache::new(cache_config)).await;
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Now the disk file and deletion vector has been persist into iceberg.
    let object_storage_cache_for_recovery = ObjectStorageCache::default_for_test(&temp_dir);
    let iceberg_table_config = get_iceberg_table_config(&temp_dir);
    let mut iceberg_table_manager_to_recover = IcebergTableManager::new(
        table.metadata.clone(),
        object_storage_cache_for_recovery.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config,
    )
    .unwrap();
    let (next_file_id, mooncake_snapshot) = iceberg_table_manager_to_recover
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one data file, one index block file

    // Check data file has been pinned in mooncake table.
    let file_indices = mooncake_snapshot.indices.file_indices.clone();
    assert_eq!(file_indices.len(), 1);
    let (index_block_files, overall_file_size) = get_index_block_files(file_indices);
    assert_eq!(index_block_files.len(), 1);

    // Check cache state.
    assert_eq!(
        object_storage_cache_for_recovery
            .cache
            .read()
            .await
            .evicted_entries
            .len(),
        0,
    );
    assert_eq!(
        object_storage_cache_for_recovery
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0,
    );
    assert_eq!(
        object_storage_cache_for_recovery
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        1,
    );
    assert_eq!(
        object_storage_cache_for_recovery
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                index_block_files[0].file_id()
            ))
            .await,
        1,
    );
    assert_eq!(
        object_storage_cache_for_recovery
            .cache
            .read()
            .await
            .cur_bytes,
        overall_file_size
    );
}

/// ========================
/// Index merge utils
/// ========================
///
/// Test util function to create mooncake table and table notify for index merge test.
pub(super) async fn create_mooncake_table_and_notify_for_index_merge(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableEvent>) {
    let path = temp_dir.path().to_path_buf();
    let warehouse_uri = path.clone().to_str().unwrap().to_string();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let storage_config = StorageConfig::FileSystem {
        root_directory: warehouse_uri.clone(),
    };
    let iceberg_table_config = IcebergTableConfig {
        accessor_config: AccessorConfig::new_with_storage_config(storage_config),
        ..Default::default()
    };
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever [`create_snapshot`] is called.
    let mooncake_table_config = MooncakeTableConfig {
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: 0,
            ..Default::default()
        },
        // Trigger index merge as long as there're two index block files.
        file_index_config: FileIndexMergeConfig {
            index_block_final_size: u64::MAX,
            min_file_indices_to_merge: 2,
            max_file_indices_to_merge: u32::MAX,
        },
        ..Default::default()
    };

    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        TEST_TABLE_ID.0,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
        object_storage_cache,
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();

    let (notify_tx, notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    (table, notify_rx)
}

/// Test util function to create two data files for index merge.
/// Rows are committed and flushed with LSN 1 and 2 respectively.
async fn prepare_test_disk_files_for_index_merge(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableEvent>) {
    let (mut table, mut table_notify) =
        create_mooncake_table_and_notify_for_index_merge(temp_dir, object_storage_cache).await;

    // Append, commit and flush the first row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    flush_table_and_sync(&mut table, &mut table_notify, /*lsn=*/ 1)
        .await
        .unwrap();

    // Append, commit and flush the second row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 2);
    flush_table_and_sync(&mut table, &mut table_notify, /*lsn=*/ 2)
        .await
        .unwrap();

    (table, table_notify)
}

/// ========================
/// Use by index merge
/// ========================
///
/// Test scenario: remote, local + use => remote, local
/// Test scenario: remote, local + use over => no file index
#[tokio::test]
async fn test_3_index_merge() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
        /*optimize_local_filesystem=*/ false,
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_files_for_index_merge(&temp_dir, object_storage_cache.clone()).await; // <---
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, index_merge_payload, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());
    let index_merge_payload = index_merge_payload.take_payload().unwrap();

    // Get data files and old merged index block files.
    let disk_files = get_disk_files_for_table(&table).await;
    assert_eq!(disk_files.len(), 2);
    let mut old_compacted_index_block_files = get_index_block_filepaths(&table).await;
    assert_eq!(old_compacted_index_block_files.len(), 2);
    old_compacted_index_block_files.sort();

    // Perform index merge and sync.
    let mut evicted_files_to_delete =
        perform_index_merge_for_test(&mut table, &mut table_notify, index_merge_payload).await;
    evicted_files_to_delete.sort();
    assert_eq!(old_compacted_index_block_files, evicted_files_to_delete);

    let merged_file_indices = get_index_block_filepaths(&table).await;
    assert_eq!(merged_file_indices.len(), 1);

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evicted_entries
            .len(),
        0,
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        2, // data files
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        1, // merged index block
    );
}
