use rstest::rstest;
use tempfile::TempDir;
use tokio::sync::mpsc::Receiver;

use crate::row::{MoonlinkRow, RowValue};
use crate::storage::cache::object_storage::test_utils::*;
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
use crate::storage::mooncake_table::cache_test_utils::*;
use crate::storage::mooncake_table::table_accessor_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use crate::storage::mooncake_table::test_utils_commons::*;
use crate::table_notify::TableEvent;
use crate::{
    IcebergTableManager, MooncakeTable, ObjectStorageCache, ObjectStorageCacheConfig, TableManager,
};

use crate::storage::mooncake_table::table_creation_test_utils::*;
/// ========================
/// Test util function for read
/// ========================
///
/// Prepare persisted data files and their deletion vector in mooncake table.
/// Rows are committed and flushed with LSN 1, and deleted with LSN 3.
async fn prepare_test_deletion_vector_for_read(
    temp_dir: &TempDir,
    cache: ObjectStorageCache,
    use_batch_write: bool,
) -> (MooncakeTable, Receiver<TableEvent>) {
    let (mut table, mut table_notify) =
        create_mooncake_table_and_notify_for_read(temp_dir, cache).await;

    // Append a new row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);

    if use_batch_write {
        table.append(row.clone()).unwrap();
        table.commit(/*lsn=*/ 1);
        flush_table_and_sync(&mut table, &mut table_notify, /*lsn=*/ 1)
            .await
            .unwrap();

        // Delete the row.
        table.delete(/*row=*/ row.clone(), /*lsn=*/ 2).await;
        table.commit(/*lsn=*/ 3);
        flush_table_and_sync(&mut table, &mut table_notify, /*lsn=*/ 3)
            .await
            .unwrap();
    } else {
        table
            .append_in_stream_batch(row.clone(), /*xact_id=*/ 0)
            .unwrap();
        table
            .commit_transaction_stream(/*xact_id=*/ 0, /*lsn=*/ 1)
            .await
            .unwrap();

        // Delete the row.
        table
            .delete_in_stream_batch(row.clone(), /*xact_id=*/ 1)
            .await;
        table
            .commit_transaction_stream(/*xact_id=*/ 1, /*lsn=*/ 3)
            .await
            .unwrap();
    }

    (table, table_notify)
}

/// ========================
/// Use by read
/// ========================
///
/// Test scenario: no deletion vector + persist => referenced, not requested to delete
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_1_persist_2_without_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_infinite_object_storage_cache(&temp_dir, /*optimize_local_filesystem=*/ false);

    let (mut table, mut table_notify) =
        prepare_test_deletion_vector_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Check data file has been pinned in mooncake table.
    let puffin_blob_ref = get_only_puffin_blob_ref_from_table(&table).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // Data file.
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // Puffin file and index block.
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&puffin_blob_ref.puffin_file_cache_handle.file_id)
            .await,
        1
    );
}

/// State transfer is the same as [`test_1_persist_2_without_local_optimization`].
/// Test scenario: no deletion vector + persist => referenced, not requested to delete
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_1_persist_2_with_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_infinite_object_storage_cache(&temp_dir, /*optimize_local_filesystem=*/ true);

    let (mut table, mut table_notify) =
        prepare_test_deletion_vector_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let local_data_files_and_index_blocks = get_data_files_and_index_block_files(&table).await;

    let (_, _, _, _, mut files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, local_data_files_and_index_blocks);

    // Check data file has been pinned in mooncake table.
    let puffin_blob_ref = get_only_puffin_blob_ref_from_table(&table).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // Data file.
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // Puffin file and index block.
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&puffin_blob_ref.puffin_file_cache_handle.file_id)
            .await,
        1
    );
}

/// Test scenario: no deletion vector + recover => referenced, not requested to delete
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_1_recover_2_without_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
        /*optimize_local_filesystem=*/ false,
    );

    let (mut table, mut table_notify) = prepare_test_deletion_vector_for_read(
        &temp_dir,
        ObjectStorageCache::new(cache_config),
        use_batch_write,
    )
    .await;
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Now the disk file and deletion vector has been persist into iceberg.
    let mut cache_for_recovery = ObjectStorageCache::default_for_test(&temp_dir);
    let iceberg_table_config = get_iceberg_table_config(&temp_dir);
    let mut iceberg_table_manager_to_recover = IcebergTableManager::new(
        table.metadata.clone(),
        cache_for_recovery.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config,
    )
    .unwrap();
    let (next_file_id, mooncake_snapshot) = iceberg_table_manager_to_recover
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 3); // one data file, one index block file, one deletion vector puffin

    // Check data file has been pinned in mooncake table.
    let puffin_blob_ref = get_only_puffin_blob_ref_from_snapshot(&mooncake_snapshot);

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache_for_recovery, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache_for_recovery, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache_for_recovery, /*expected_count=*/ 2).await; // Puffin file and index block.
    assert_eq!(
        cache_for_recovery
            .get_non_evictable_entry_ref_count(&puffin_blob_ref.puffin_file_cache_handle.file_id)
            .await,
        1,
    );
}

/// State transfer is the same as [`test_1_recover_2_without_local_optimization`].
/// Test scenario: no deletion vector + recover => referenced, not requested to delete
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_1_recover_2_with_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache_config = ObjectStorageCacheConfig::new(
        INFINITE_LARGE_OBJECT_STORAGE_CACHE_SIZE,
        temp_dir.path().to_str().unwrap().to_string(),
        /*optimize_local_filesystem=*/ true,
    );

    let (mut table, mut table_notify) = prepare_test_deletion_vector_for_read(
        &temp_dir,
        ObjectStorageCache::new(cache_config),
        use_batch_write,
    )
    .await;
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let local_data_files_and_index_blocks = get_data_files_and_index_block_files(&table).await;

    let (_, _, _, _, mut files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, local_data_files_and_index_blocks);

    // Now the disk file and deletion vector has been persist into iceberg.
    let mut cache_for_recovery = ObjectStorageCache::default_for_test(&temp_dir);
    let iceberg_table_config = get_iceberg_table_config(&temp_dir);
    let mut iceberg_table_manager_to_recover = IcebergTableManager::new(
        table.metadata.clone(),
        cache_for_recovery.clone(),
        create_test_filesystem_accessor(&iceberg_table_config),
        iceberg_table_config,
    )
    .unwrap();
    let (next_file_id, mooncake_snapshot) = iceberg_table_manager_to_recover
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 3); // one data file, one index block file, one deletion vector puffin

    // Check data file has been pinned in mooncake table.
    let puffin_blob_ref = get_only_puffin_blob_ref_from_snapshot(&mooncake_snapshot);

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache_for_recovery, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache_for_recovery, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache_for_recovery, /*expected_count=*/ 2).await; // Puffin file and index block.
    assert_eq!(
        cache_for_recovery
            .get_non_evictable_entry_ref_count(&puffin_blob_ref.puffin_file_cache_handle.file_id)
            .await,
        1,
    );
}

/// Test scenario: referenced, no delete + use => referenced, no delete
/// Test scenario: referenced, no delete + use over => referenced, no delete
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_2_read_without_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_infinite_object_storage_cache(&temp_dir, /*optimize_local_filesystem=*/ false);

    let (mut table, mut table_notify) =
        prepare_test_deletion_vector_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Use by read.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Check data file has been pinned in mooncake table.
    let puffin_blob_ref = get_only_puffin_blob_ref_from_table(&table).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 3).await; // Puffin file, data file, and index block.
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&puffin_blob_ref.puffin_file_cache_handle.file_id)
            .await,
        2,
    );

    // Drop all read states and check reference count.
    drop_read_states_and_create_mooncake_snapshot(vec![read_state], &mut table, &mut table_notify)
        .await;
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // data file
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // puffin file and index block.
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&puffin_blob_ref.puffin_file_cache_handle.file_id)
            .await,
        1
    );
}

/// State transfer is the same as [`test_2_read_without_local_optimization`].
/// Test scenario: referenced, no delete + use => referenced, no delete
/// Test scenario: referenced, no delete + use over => referenced, no delete
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_2_read_with_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_infinite_object_storage_cache(&temp_dir, /*optimize_local_filesystem=*/ true);

    let (mut table, mut table_notify) =
        prepare_test_deletion_vector_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let local_data_files_and_index_blocks = get_data_files_and_index_block_files(&table).await;

    let (_, _, _, _, mut files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, local_data_files_and_index_blocks);

    // Use by read.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Check data file has been pinned in mooncake table.
    let puffin_blob_ref = get_only_puffin_blob_ref_from_table(&table).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 3).await; // Puffin file, data file, and index block.
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&puffin_blob_ref.puffin_file_cache_handle.file_id)
            .await,
        2,
    );

    // Drop all read states and check reference count.
    drop_read_states_and_create_mooncake_snapshot(vec![read_state], &mut table, &mut table_notify)
        .await;
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // data file
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // puffin file and index block.
    assert_eq!(
        cache
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
    cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableEvent>) {
    let (mut table, mut table_notify) =
        create_mooncake_table_and_notify_for_compaction(temp_dir, cache).await;

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

    // Deletion, commit and flush the first row.
    table.delete(/*row=*/ row.clone(), /*lsn=*/ 2).await;
    table.commit(/*lsn=*/ 3);
    flush_table_and_sync(&mut table, &mut table_notify, /*lsn=*/ 3)
        .await
        .unwrap();

    // Append, commit and flush the second row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 4);
    flush_table_and_sync(&mut table, &mut table_notify, /*lsn=*/ 4)
        .await
        .unwrap();

    // Deletion, commit and flush the second row.
    table.delete(/*row=*/ row.clone(), /*lsn=*/ 5).await;
    table.commit(/*lsn=*/ 6);
    flush_table_and_sync(&mut table, &mut table_notify, /*lsn=*/ 6)
        .await
        .unwrap();

    (table, table_notify)
}

/// ========================
/// Use by compaction
/// ========================
///
/// Test scenario: referenced, no delete + delete & referenced => referenced, requested to delete
/// Test scenario: referenced, no delete + delete & unreferenced => no entry
#[tokio::test]
async fn test_2_compact_without_local_optimization() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_infinite_object_storage_cache(&temp_dir, /*optimize_local_filesystem=*/ false);

    let (mut table, mut table_notify) =
        prepare_test_disk_files_with_deletion_vector_for_compaction(&temp_dir, cache.clone()).await;
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, data_compaction_payload, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Get old snapshot disk files.
    let disk_files = get_disk_files_for_table(&table).await;
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
    let old_compacted_index_block_files = get_index_block_filepaths(&table).await;
    assert_eq!(old_compacted_index_block_files.len(), 2);

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data files
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 4).await; // Puffin files and index blocks.
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&old_compacted_puffin_file_ids[0])
            .await,
        1,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&old_compacted_puffin_file_ids[1])
            .await,
        1,
    );

    // Use by compaction.
    let evicted_files = perform_data_compaction_for_test(
        &mut table,
        &mut table_notify,
        data_compaction_payload.take_payload().unwrap(),
    )
    .await;
    // Include both two data files and their puffin files, index blocks.
    assert_eq!(evicted_files.len(), 6);
    assert!(evicted_files.contains(&old_compacted_puffin_files[0]));
    assert!(evicted_files.contains(&old_compacted_puffin_files[1]));
    assert!(evicted_files.contains(&old_compacted_index_block_files[0]));
    assert!(evicted_files.contains(&old_compacted_index_block_files[1]));

    // Check data file has been pinned in mooncake table.
    let disk_files = get_disk_files_for_table(&table).await;
    assert!(disk_files.is_empty());

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
}

/// State transfer is the same as [`test_2_compact_without_local_optimization`].
/// Test scenario: referenced, no delete + delete & referenced => referenced, requested to delete
/// Test scenario: referenced, no delete + delete & unreferenced => no entry
#[tokio::test]
async fn test_2_compact_with_local_optimization() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_infinite_object_storage_cache(&temp_dir, /*optimize_local_filesystem=*/ true);

    let (mut table, mut table_notify) =
        prepare_test_disk_files_with_deletion_vector_for_compaction(&temp_dir, cache.clone()).await;
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let local_data_files_and_index_blocks = get_data_files_and_index_block_files(&table).await;

    let (_, _, _, data_compaction_payload, mut files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, local_data_files_and_index_blocks);

    // Get old snapshot disk files.
    let disk_files = get_disk_files_for_table(&table).await;
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
    let old_compacted_index_block_files = get_index_block_filepaths(&table).await;
    assert_eq!(old_compacted_index_block_files.len(), 2);

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data files
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 4).await; // Puffin files and index blocks.
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&old_compacted_puffin_file_ids[0])
            .await,
        1,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&old_compacted_puffin_file_ids[1])
            .await,
        1,
    );

    // Use by compaction.
    let evicted_files = perform_data_compaction_for_test(
        &mut table,
        &mut table_notify,
        data_compaction_payload.take_payload().unwrap(),
    )
    .await;
    assert!(evicted_files.is_empty());

    // Check data file has been pinned in mooncake table.
    let disk_files = get_disk_files_for_table(&table).await;
    assert!(disk_files.is_empty());

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
}
