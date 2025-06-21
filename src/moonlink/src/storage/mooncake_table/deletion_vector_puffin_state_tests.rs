use tempfile::TempDir;
use tokio::sync::mpsc::Receiver;

use crate::row::{MoonlinkRow, RowValue};
/// Possible states:
/// (1) No deletion vector
/// (2) Deletion vector referenced, not requested to delete
/// (3) Deletion vector referenced, requested to delete
/// (4) Deletion vector not referenced and requested to delete
///
/// Difference with data files:
/// - Deletion vector always sits on-disk, and stored as cache handle
/// - Due to (1), before usage (i.e. read, compact), deletion vector should add reference count
/// - Data file has an extra state: not referenced but not requested to deleted
///
/// State transition input:
/// - Persist into iceberg table
/// - Recover from iceberg table
/// - Use deletion vector (including read and compact)
/// - Usage finishes
/// - Request to delete
///
/// State machine transfer:
/// Initial state: no deletion vector
/// - No deletion vector + persist => referenced, not requested to delete
/// - No deletion vector + recover => referenced, not requested to delete
///
/// Initial state: referenced, not requested to delete
/// - Referenced, no delete + use => referenced, no delete
/// - Referenced, no delete + use over => referenced, no delete
///
/// Initial state: referenced, not requested to delete
/// - Referenced, no delete + delete & referenced => referenced, requested to delete
/// - Referenced, no delete + delete & unreferenced => no entry
///
/// Initial state: referenced, requested to delete
/// - Referenced, to delete + use over & referenced => referenced, to delete
/// - Referenced, to delete + use over & unreferenced => no entry
///
/// For more details, please refer to https://docs.google.com/document/d/1LDWLWhgFP5-da8P50t-uZIO6a4lK2Na5P70ibNOWu-g/edit?usp=sharing
use crate::storage::mooncake_table::state_test_utils::*;
use crate::table_notify::TableNotify;
use crate::{
    IcebergTableManager, MooncakeTable, ObjectStorageCache, ObjectStorageCacheConfig, TableManager,
};

/// ========================
/// Test util function for read
/// ========================
///
/// Prepare persisted data files and their deletion vector in mooncake table.
/// Rows are committed and flushed with LSN 1, and deleted with LSN 3.
async fn prepare_test_deletion_vector_for_read(
    temp_dir: &TempDir,
    object_storage_cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableNotify>) {
    let (mut table, table_notify) =
        create_mooncake_table_and_notify_for_read(temp_dir, object_storage_cache).await;

    // Append a new row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    table.flush(/*lsn=*/ 1).await.unwrap();

    // Delete the row.
    table.delete(/*row=*/ row.clone(), /*lsn=*/ 2).await;
    table.commit(/*lsn=*/ 3);
    table.flush(/*lsn=*/ 3).await.unwrap();

    (table, table_notify)
}

/// ========================
/// Use by read
/// ========================
///
/// Test scenario: no deletion vector + persist => referenced, not requested to delete
#[tokio::test]
async fn test_1_persist_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_deletion_vector_for_read(&temp_dir, object_storage_cache.clone()).await;
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
    let (_, disk_file_entry) = disk_files.iter().next().unwrap();
    let puffin_blob_ref = disk_file_entry.puffin_deletion_blob.as_ref().unwrap();

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
        1, // Data file.
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // Puffin file and index block.
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&puffin_blob_ref.puffin_file_cache_handle.file_id)
            .await,
        1
    );
}

/// Test scenario: no deletion vector + recover => referenced, not requested to delete
#[tokio::test]
async fn test_1_recover_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );

    let (mut table, mut table_notify) =
        prepare_test_deletion_vector_for_read(&temp_dir, ObjectStorageCache::new(cache_config))
            .await;
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Now the disk file and deletion vector has been persist into iceberg.
    let object_storage_cache_for_recovery = ObjectStorageCache::default_for_test(&temp_dir);
    let mut iceberg_table_manager_to_recover = IcebergTableManager::new(
        table.metadata.clone(),
        object_storage_cache_for_recovery.clone(),
        get_iceberg_table_config(&temp_dir),
    )
    .unwrap();
    let mooncake_snapshot = iceberg_table_manager_to_recover
        .load_snapshot_from_table()
        .await
        .unwrap();

    // Check data file has been pinned in mooncake table.
    let disk_files = mooncake_snapshot.disk_files.clone();
    assert_eq!(disk_files.len(), 1);
    let (_, disk_file_entry) = disk_files.iter().next().unwrap();
    let puffin_blob_ref = disk_file_entry.puffin_deletion_blob.as_ref().unwrap();

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
        2, // Puffin file and index block.
    );
    assert_eq!(
        object_storage_cache_for_recovery
            .get_non_evictable_entry_ref_count(&puffin_blob_ref.puffin_file_cache_handle.file_id)
            .await,
        1,
    );
}

/// Test scenario: referenced, no delete + use => referenced, no delete
/// Test scenario: referenced, no delete + use over => referenced, no delete
#[tokio::test]
async fn test_2_read() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_deletion_vector_for_read(&temp_dir, object_storage_cache.clone()).await;
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, _, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Use by read.
    let snapshot_read_output = table.request_read().await.unwrap();
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 1);
    let (_, disk_file_entry) = disk_files.iter().next().unwrap();
    let puffin_blob_ref = disk_file_entry.puffin_deletion_blob.as_ref().unwrap();

    // Check cache state.
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .evictable_cache
            .len(),
        0,
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        3, // Puffin file, data file, and index block.
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&puffin_blob_ref.puffin_file_cache_handle.file_id)
            .await,
        2,
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
        1, // data file
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        2, // puffin file and index block.
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&puffin_blob_ref.puffin_file_cache_handle.file_id)
            .await,
        1
    );
}

/// ========================
/// Test util function for compaction
/// ========================
///
/// Test util function to create two data files for compaction.
/// Rows are committed and flushed with LSN 1 and 2 respectively.
async fn prepare_test_disk_files_with_deletion_vector_for_compaction(
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

    // Deletion, commit and flush the first row.
    table.delete(/*row=*/ row.clone(), /*lsn=*/ 2).await;
    table.commit(/*lsn=*/ 3);
    table.flush(/*lsn=*/ 3).await.unwrap();

    // Append, commit and flush the second row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 4);
    table.flush(/*lsn=*/ 4).await.unwrap();

    // Deletion, commit and flush the second row.
    table.delete(/*row=*/ row.clone(), /*lsn=*/ 5).await;
    table.commit(/*lsn=*/ 6);
    table.flush(/*lsn=*/ 6).await.unwrap();

    (table, table_notify)
}

/// ========================
/// Use by compaction
/// ========================
///
/// Test scenario: referenced, no delete + delete & referenced => referenced, requested to delete
/// Test scenario: referenced, no delete + delete & unreferenced => no entry
#[tokio::test]
async fn test_2_compact() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
    );
    let object_storage_cache = ObjectStorageCache::new(cache_config);

    let (mut table, mut table_notify) =
        prepare_test_disk_files_with_deletion_vector_for_compaction(
            &temp_dir,
            object_storage_cache.clone(),
        )
        .await;
    table
        .create_mooncake_and_iceberg_snapshot_for_test(&mut table_notify)
        .await
        .unwrap();
    let (_, _, data_compaction_payload, files_to_delete) = table
        .create_mooncake_snapshot_for_test(&mut table_notify)
        .await;
    assert!(files_to_delete.is_empty());

    // Get old snapshot disk files.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert_eq!(disk_files.len(), 2);
    let mut old_compacted_puffin_file_ids = vec![];
    let mut old_compacted_puffin_files = vec![];
    for (_, disk_entry) in disk_files.iter() {
        old_compacted_puffin_file_ids.push(
            disk_entry
                .puffin_deletion_blob
                .as_ref()
                .unwrap()
                .puffin_file_cache_handle
                .file_id,
        );
        old_compacted_puffin_files.push(
            disk_entry
                .puffin_deletion_blob
                .as_ref()
                .unwrap()
                .puffin_file_cache_handle
                .get_cache_filepath()
                .to_string(),
        );
    }
    assert_eq!(old_compacted_puffin_files.len(), 2);
    let old_compacted_index_block_files = table.get_index_block_files().await;
    assert_eq!(old_compacted_index_block_files.len(), 2);

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
        2, // Data files.
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        4, // Puffin files and index blocks.
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&old_compacted_puffin_file_ids[0])
            .await,
        1,
    );
    assert_eq!(
        object_storage_cache
            .get_non_evictable_entry_ref_count(&old_compacted_puffin_file_ids[1])
            .await,
        1,
    );

    // Use by compaction.
    let evicted_files = table
        .perform_data_compaction_for_test(&mut table_notify, data_compaction_payload.unwrap())
        .await;
    // Include both two data files and their puffin files, index blocks.
    assert_eq!(evicted_files.len(), 6);
    assert!(evicted_files.contains(&old_compacted_puffin_files[0]));
    assert!(evicted_files.contains(&old_compacted_puffin_files[1]));
    assert!(evicted_files.contains(&old_compacted_index_block_files[0]));
    assert!(evicted_files.contains(&old_compacted_index_block_files[1]));

    // Check data file has been pinned in mooncake table.
    let disk_files = table.get_disk_files_for_snapshot().await;
    assert!(disk_files.is_empty());

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
        0,
    );
    assert_eq!(
        object_storage_cache
            .cache
            .read()
            .await
            .non_evictable_cache
            .len(),
        0,
    );
}
