// --------- state-based unit tests ---------
// Possible states for data records:
// (1) Uncommitted in-memory record batches
// (2) Committed in-memory record batches
// (3) Committed data files
// (4) Uncommitted and committed in-memory record batches
// (5) Committed in-memory record batches and committed data files
// (6) Uncommitted/committed record batches, and committed data files
// (7) No record batches
//
// Possible states for deletion vectors:
// (1) No deletion record
// (2) Only uncommitted deletion record
// (3) Only committed deletion record
// (4) Uncommitted and committed deletion record
// (5) Committed and flushed deletion record
// (6) Uncommitted deletion record and committed/flushed deletion record
//
// Persisted iceberg snapshot states:
// (1) No new snapshot
// (2) New snapshot with data files
// (3) New snapshot with deletion vectors
// (4) New snapshot with data files and deletion vectors
//
// State-based tests are used to guarantee persisted iceberg snapshots are at a consistent view.
// For states, refer to https://docs.google.com/document/d/1hZ0H66_eefjFezFQf1Pdr-A63eJ94OH9TsInNS-6xao/edit?usp=sharing
//
// A few testing assumptions / preparations:
// - There will be at most two types of data files, one written before test case perform any append/delete operations, another after new rows appended.
// - To differentiate these two types of data files, the first type of record batch contains two rows, the second type of record batch contains only one row.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use tokio::sync::mpsc::Receiver;

use crate::row::MoonlinkRow;
use crate::row::RowValue;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::storage::iceberg::table_manager::TableManager;
use crate::storage::iceberg::test_utils::*;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use crate::storage::mooncake_table::validation_test_utils::*;
use crate::storage::mooncake_table::Snapshot;
use crate::storage::mooncake_table::SnapshotOption;
use crate::storage::MooncakeTable;
use crate::table_notify::TableEvent;
use crate::FileSystemAccessor;

// ==============================
// Row preparation functions
// ==============================
//
// Test util functions to get a few moonlink rows for testing.
fn get_test_row_1() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(10),
    ])
}
fn get_test_row_2() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(20),
    ])
}
fn get_test_row_3() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ])
}
fn get_test_row_4() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(4),
        RowValue::ByteArray("David".as_bytes().to_vec()),
        RowValue::Int32(40),
    ])
}
fn get_test_row_5() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ])
}

// ==============================
// Test util functions
// ==============================
//
// Test util function to prepare for committed and persisted data file,
// here we write two rows and assume they'll be included in one arrow record batch and one data file.
async fn prepare_committed_and_flushed_data_files(
    table: &mut MooncakeTable,
    notify_rx: &mut Receiver<TableEvent>,
    lsn: u64,
) -> (MoonlinkRow, MoonlinkRow) {
    // Append first row.
    let row_1 = get_test_row_1();
    table.append(row_1.clone()).unwrap();

    // Append second row.
    let row_2 = get_test_row_4();
    table.append(row_2.clone()).unwrap();

    table.commit(lsn);
    flush_table_and_sync(table, notify_rx, lsn).await.unwrap();

    (row_2, row_1)
}

// Test util function to check whether the data file in iceberg snapshot is the same as initially-prepared one from [`prepare_committed_and_flushed_data_files`].
//
// # Arguments
//
// * deleted: whether the record in the data file has been deleted.
async fn check_prev_data_files(
    snapshot: &Snapshot,
    iceberg_table_manager: &IcebergTableManager,
    deleted: bool,
) {
    assert_eq!(snapshot.disk_files.len(), 1);
    let (data_file, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    let loaded_arrow_batch = load_arrow_batch(file_io, data_file.file_path().as_str())
        .await
        .unwrap();
    let expected_arrow_batch = RecordBatch::try_new(
        iceberg_table_manager.mooncake_table_metadata.schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 4])),
            Arc::new(StringArray::from(vec!["John", "David"])),
            Arc::new(Int32Array::from(vec![10, 40])),
        ],
    )
    .unwrap();
    assert_eq!(loaded_arrow_batch, expected_arrow_batch);

    // In the test suite, we only delete the second prepared row.
    assert!(!deletion_vector
        .batch_deletion_vector
        .is_deleted(/*row_idx=*/ 0));
    if deleted {
        assert!(deletion_vector
            .batch_deletion_vector
            .is_deleted(/*row_idx=*/ 1));
    } else {
        assert!(!deletion_vector
            .batch_deletion_vector
            .is_deleted(/*row_idx=*/ 1));
    }
}

// Test util function to check whether the data file in iceberg snapshot is the same as the newly appended row.
//
// # Arguments
//
// * deleted: whether the record in the data file has been deleted.
async fn check_new_data_files(
    snapshot: &Snapshot,
    iceberg_table_manager: &IcebergTableManager,
    deleted: bool,
) {
    assert_eq!(snapshot.disk_files.len(), 1);
    let (data_file, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    let loaded_arrow_batch = load_arrow_batch(file_io, data_file.file_path().as_str())
        .await
        .unwrap();
    let expected_arrow_batch = RecordBatch::try_new(
        iceberg_table_manager.mooncake_table_metadata.schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["John"])),
            Arc::new(Int32Array::from(vec![10])),
        ],
    )
    .unwrap();
    assert_eq!(loaded_arrow_batch, expected_arrow_batch);

    // In the test suite, we only delete the second prepared row.
    assert!(!deletion_vector
        .batch_deletion_vector
        .is_deleted(/*row_idx=*/ 0));
    if deleted {
        assert!(deletion_vector
            .batch_deletion_vector
            .is_deleted(/*row_idx=*/ 1));
    } else {
        assert!(!deletion_vector
            .batch_deletion_vector
            .is_deleted(/*row_idx=*/ 1));
    }
}

// Test util function to check whether the data files in iceberg snapshot is the same as initially-prepared one from `prepare_committed_and_flushed_data_files`, and the newly added row.
//
// # Arguments
//
// * deleted: whether the record in the data file has been deleted.
async fn check_prev_and_new_data_files(
    snapshot: &Snapshot,
    iceberg_table_manager: &IcebergTableManager,
    deleted: Vec<bool>,
) {
    assert_eq!(snapshot.disk_files.len(), 2);
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();

    let mut loaded_record_batches: Vec<RecordBatch> = Vec::with_capacity(2);
    let mut batch_deletion_vectors: Vec<&BatchDeletionVector> = Vec::with_capacity(2);
    for (cur_data_file, cur_deletion_vector) in snapshot.disk_files.iter() {
        let cur_arrow_batch = load_arrow_batch(file_io, cur_data_file.file_path().as_str())
            .await
            .unwrap();
        loaded_record_batches.push(cur_arrow_batch);
        batch_deletion_vectors.push(&cur_deletion_vector.batch_deletion_vector);
    }
    // In the test suite, the first record has two rows, and the second record has one row.
    if loaded_record_batches[0].num_rows() == 1 {
        loaded_record_batches.swap(0, 1);
        batch_deletion_vectors.swap(0, 1);
    }

    // Check data files.
    let expected_arrow_batch_1 = RecordBatch::try_new(
        iceberg_table_manager.mooncake_table_metadata.schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 4])),
            Arc::new(StringArray::from(vec!["John", "David"])),
            Arc::new(Int32Array::from(vec![10, 40])),
        ],
    )
    .unwrap();
    let expected_arrow_batch_2 = RecordBatch::try_new(
        iceberg_table_manager.mooncake_table_metadata.schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["Bob"])),
            Arc::new(Int32Array::from(vec![20])),
        ],
    )
    .unwrap();
    assert_eq!(loaded_record_batches[0], expected_arrow_batch_1);
    assert_eq!(loaded_record_batches[1], expected_arrow_batch_2);

    // Check deletion vector.
    assert!(!batch_deletion_vectors[0].is_deleted(/*row_idx=*/ 0));
    if deleted[0] {
        assert!(batch_deletion_vectors[0].is_deleted(/*row_idx=*/ 1));
    } else {
        assert!(!batch_deletion_vectors[0].is_deleted(/*row_idx=*/ 1));
    }

    if deleted[1] {
        assert!(batch_deletion_vectors[1].is_deleted(/*row_idx=*/ 0));
    } else {
        assert!(!batch_deletion_vectors[1].is_deleted(/*row_idx=*/ 0));
    }
}

// ==============================
// State validation functions
// ==============================
//
// Validate cases where no new iceberg snapshot created.
async fn validate_no_snapshot(
    iceberg_table_manager: &mut IcebergTableManager,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) {
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 0);
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager.config.accessor_config.get_root_path(),
        filesystem_accessor,
    )
    .await;
}

// Validate only old snapshot, but not newly created ones.
async fn validate_only_initial_snapshot(
    iceberg_table_manager: &mut IcebergTableManager,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) {
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one data file, one index block file
    check_prev_data_files(&snapshot, iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager.config.accessor_config.get_root_path(),
        filesystem_accessor,
    )
    .await;
}

// Validate new snapshot with new data files created, but no deletion vector.
async fn validate_only_new_data_files_in_snapshot(
    iceberg_table_manager: &mut IcebergTableManager,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) {
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one data file, one index block file
    check_new_data_files(&snapshot, iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager.config.accessor_config.get_root_path(),
        filesystem_accessor,
    )
    .await;
}

// Validate new snapshot with new deletion vector created, but no data files.
async fn validate_only_new_deletion_vectors_in_snapshot(
    iceberg_table_manager: &mut IcebergTableManager,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) {
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 3); // one data file, one index block file, one deletion vector puffin
    check_prev_data_files(&snapshot, iceberg_table_manager, /*deleted=*/ true).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager.config.accessor_config.get_root_path(),
        filesystem_accessor,
    )
    .await;
}

// Validate new snapshot with both new data files and deletion vector created.
async fn validate_new_data_files_and_deletion_vectors_in_snapshot(
    iceberg_table_manager: &mut IcebergTableManager,
    filesystem_accessor: &dyn BaseFileSystemAccess,
    expected_next_file_id: u32,
    expected_prev_files_deleted: Vec<bool>,
    expected_flush_lsn: u64,
) {
    let (next_file_id, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, expected_next_file_id);
    check_prev_and_new_data_files(
        &snapshot,
        iceberg_table_manager,
        expected_prev_files_deleted,
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.flush_lsn.unwrap(), expected_flush_lsn);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager.config.accessor_config.get_root_path(),
        filesystem_accessor,
    )
    .await;
}

// ==============================
// State machine tests
// ==============================
//
// Testing combination: (1) + (1) => no snapshot
#[tokio::test]
async fn test_state_1_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, _) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = get_test_row_1();
    table.append(row).unwrap();

    // Request to persist.
    assert!(!table.create_snapshot(SnapshotOption::default()));

    // Validate end state.
    validate_no_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (1) + (2) => no snapshot
#[tokio::test]
async fn test_state_1_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, _) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = get_test_row_1();
    table.append(row.clone()).unwrap();
    // Prepare deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 1).await;

    // Request to persist.
    assert!(!table.create_snapshot(SnapshotOption::default()));

    // Validate end state.
    validate_no_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (1) + (3) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_1_3() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_initial_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (1) + (4) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_1_4() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 400).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_initial_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (1) + (5) => snapshot with deletion vector
#[tokio::test]
async fn test_state_1_5() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}

// Testing combination: (1) + (6) => snapshot with deletion vector
#[tokio::test]
async fn test_state_1_6() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 400).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}

// Testing combination: (2) + (1) => no snapshot
#[tokio::test]
async fn test_state_2_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = get_test_row_1();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_no_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (2) + (2) => no snapshot
#[tokio::test]
async fn test_state_2_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = get_test_row_1();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    // Prepare deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 200).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_no_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (2) + (3) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_2_3() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_initial_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (2) + (4) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_2_4() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 500).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_initial_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (2) + (5) => snapshot with deletion vector
#[tokio::test]
async fn test_state_2_5() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}

// Testing combination: (2) + (6) => snapshot with deletion vector
#[tokio::test]
async fn test_state_2_6() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 500).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}

// Testing combination: (3) + (1) => snapshot with data file created
#[tokio::test]
async fn test_state_3_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = get_test_row_1();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 200)
        .await
        .unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_data_files_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}

// Testing combination: (3) + (2) => snapshot with data files created
#[tokio::test]
async fn test_state_3_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = get_test_row_1();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 200)
        .await
        .unwrap();
    // Prepare deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 300).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_data_files_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}

// Testing combination: (3) + (3) + committed deletion before flush => snapshot with data files and deletion vector
#[tokio::test]
async fn test_state_3_3_deletion_before_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 500)
        .await
        .unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/
        5, // two data files, two index block files, one deletion vector puffin
        /*expected_prev_files_deleted=*/ vec![true, false],
        /*expected_flush_lsn=*/ 500,
    )
    .await;
}

// Testing combination: (3) + (3) + committed deletion after flush => snapshot with data files
#[tokio::test]
async fn test_state_3_3_deletion_after_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/ 4, // two data files, two index block files
        /*expected_prev_files_deleted=*/ vec![false, false],
        /*expected_flush_lsn=*/ 300,
    )
    .await;
}

// Testing combination: (3) + (4) + committed deletion record before flush => snapshot with data files with deletion vector
#[tokio::test]
async fn test_state_3_4_committed_deletion_before_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 500)
        .await
        .unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/
        5, // two data files, two index block files, one deletion vector puffin
        /*expected_prev_files_deleted=*/ vec![true, false],
        /*expected_flush_lsn=*/ 500,
    )
    .await;
}

// Testing combination: (3) + (4) + committed deletion record after flush => snapshot with data files
#[tokio::test]
async fn test_state_3_4_committed_deletion_after_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/ 4, // two data files, two index block files
        /*expected_prev_files_deleted=*/ vec![false, false],
        /*expected_flush_lsn=*/ 300,
    )
    .await;
}

// Testing combination: (3) + (5) => snapshot with data files with deletion vector
#[tokio::test]
async fn test_state_3_5() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 400)
        .await
        .unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/
        5, // two data files, two index block files, one deletion vector puffin
        /*expected_prev_files_deleted=*/ vec![true, false],
        /*expected_flush_lsn=*/ 400,
    )
    .await;
}

// Testing combination: (3) + (6) => snapshot with data files with deletion vector
#[tokio::test]
async fn test_state_3_6() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 400)
        .await
        .unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 500).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/
        5, // two data files, two index block files, one deletion vector puffin
        /*expected_prev_files_deleted=*/ vec![true, false],
        /*expected_flush_lsn=*/ 400,
    )
    .await;
}

// Testing combination: (4) + (1) => no snapshot
#[tokio::test]
async fn test_state_4_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite (committed record batch).
    let row = get_test_row_1();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    // Prepare uncommitted record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_no_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (4) + (2) => no snapshot
#[tokio::test]
async fn test_state_4_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite (committed record batch).
    let row = get_test_row_1();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    // Prepare deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 200).await;
    // Prepare uncommitted record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_no_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (4) + (3) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_4_3() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed record batch).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare uncommitted record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_initial_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (4) + (4) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_4_4() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 500).await;
    // Prepare uncommitted record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_initial_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (4) + (5) => snapshot with deletion vector
#[tokio::test]
async fn test_state_4_5() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare committed record batch.
    let row = get_test_row_5();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare uncommitted record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}

// Testing combination: (4) + (6) => snapshot with deletion vector
#[tokio::test]
async fn test_state_4_6() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare committed record batch.
    let row = get_test_row_5();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare uncommitted record batch.
    let row = get_test_row_3();
    table.append(row.clone()).unwrap();
    // Prepare uncommitted deletion record.
    table.delete(row.clone(), /*lsn=*/ 500).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}

// Testing combination: (5) + (1) => snapshot with data file created
#[tokio::test]
async fn test_state_5_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = get_test_row_1();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 200)
        .await
        .unwrap();
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 300);

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_data_files_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}

// Testing combination: (5) + (2) => snapshot with data files created
#[tokio::test]
async fn test_state_5_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = get_test_row_1();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 200)
        .await
        .unwrap();
    // Prepare deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 300).await;
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 400);

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_data_files_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}

// Testing combination: (5) + (3) + committed deletion before flush => snapshot with data files and deletion vector
#[tokio::test]
async fn test_state_5_3_deletion_before_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 500)
        .await
        .unwrap();
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 600);

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/
        5, // two data files, two index block files, one deletion vector puffin
        /*expected_prev_files_deleted=*/ vec![true, false],
        /*expected_flush_lsn=*/ 500,
    )
    .await;
}

// Testing combination: (5) + (3) + committed deletion after flush => snapshot with data files
#[tokio::test]
async fn test_state_5_3_deletion_after_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 600);

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/ 4, // two data files, two index block files
        /*expected_prev_files_deleted=*/ vec![false, false],
        /*expected_flush_lsn=*/ 300,
    )
    .await;
}

// Testing combination: (5) + (4) + committed deletion record before flush => snapshot with data files with deletion vector
#[tokio::test]
async fn test_state_5_4_committed_deletion_before_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 500)
        .await
        .unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 700);

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/
        5, // two data files, two index block files, one deletion vector puffin
        /*expected_prev_files_deleted=*/ vec![true, false],
        /*expected_flush_lsn=*/ 500,
    )
    .await;
}

// Testing combination: (5) + (4) + committed deletion record after flush => snapshot with data files
#[tokio::test]
async fn test_state_5_4_committed_deletion_after_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 700);

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/ 4, // two data files, two index block files
        /*expected_prev_files_deleted=*/ vec![false, false],
        /*expected_flush_lsn=*/ 300,
    )
    .await;
}

// Testing combination: (5) + (5) => snapshot with data files and deletion vector
#[tokio::test]
async fn test_state_5_5() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare committed and flushed deletion record.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 400)
        .await
        .unwrap();
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 500);

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/
        5, // two data files, two index block files, one deletion vector puffin
        /*expected_prev_files_deleted=*/ vec![true, false],
        /*expected_flush_lsn=*/ 400,
    )
    .await;
}

// Testing combination: (5) + (6) => snapshot with data files and deletion vector
#[tokio::test]
async fn test_state_5_6() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare committed and flushed deletion record.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 400)
        .await
        .unwrap();
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 500);
    // Prepare uncommitted deletion record.
    table.delete(row.clone(), /*lsn=*/ 600).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/
        5, // two data files, two index block files, one deletion vector puffin
        /*expected_prev_files_deleted=*/ vec![true, false],
        /*expected_flush_lsn=*/ 400,
    )
    .await;
}

// Testing combination: (6) + (1) => snapshot with data file created
#[tokio::test]
async fn test_state_6_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = get_test_row_1();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 200)
        .await
        .unwrap();
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 300);
    // Prepare uncommitted record batch.
    let row = get_test_row_5();
    table.append(row).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_data_files_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}

// Testing combination: (6) + (2) => snapshot with data files created
#[tokio::test]
async fn test_state_6_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = get_test_row_1();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 200)
        .await
        .unwrap();
    // Prepare deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 300).await;
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare uncommitted record batch.
    let row = get_test_row_5();
    table.append(row).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_data_files_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}

// Testing combination: (6) + (3) + committed deletion before flush => snapshot with data files and deletion vector
#[tokio::test]
async fn test_state_6_3_deletion_before_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 500)
        .await
        .unwrap();
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 600);
    // Prepare uncommitted record batch.
    let row = get_test_row_5();
    table.append(row).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/
        5, // two data files, two index block files, one deletion vector puffin
        /*expected_prev_files_deleted=*/ vec![true, false],
        /*expected_flush_lsn=*/ 500,
    )
    .await;
}

// Testing combination: (6) + (3) + committed deletion after flush => snapshot with data files
#[tokio::test]
async fn test_state_6_3_deletion_after_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 600);
    // Prepare uncommitted record batch.
    let row = get_test_row_5();
    table.append(row).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/ 4, // two data files, two index block files
        /*expected_prev_files_deleted=*/ vec![false, false],
        /*expected_flush_lsn=*/ 300,
    )
    .await;
}

// Testing combination: (6) + (4) + committed deletion record before flush => snapshot with data files with deletion vector
#[tokio::test]
async fn test_state_6_4_committed_deletion_before_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;
    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 500)
        .await
        .unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 700);
    // Prepare uncommitted record batch.
    let row = get_test_row_5();
    table.append(row).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/
        5, // two data files, two index block files, one deletion vector puffin
        /*expected_prev_files_deleted=*/ vec![true, false],
        /*expected_flush_lsn=*/ 500,
    )
    .await;
}

// Testing combination: (6) + (4) + committed deletion record after flush => snapshot with data files
#[tokio::test]
async fn test_state_6_4_committed_deletion_after_flush() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 700);
    // Prepare uncommitted record batch.
    let row = get_test_row_5();
    table.append(row).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/ 4, // two data files, two index block files
        /*expected_prev_files_deleted=*/ vec![false, false],
        /*expected_flush_lsn=*/ 300,
    )
    .await;
}

// Testing combination: (6) + (5) => snapshot with data files
#[tokio::test]
async fn test_state_6_5() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare committed and flushed deletion records.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 400)
        .await
        .unwrap();
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 500);
    // Prepare uncommitted record batch.
    let row = get_test_row_5();
    table.append(row).unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/
        5, // two data files, two index block files, one deletion vector puffin
        /*expected_prev_files_deleted=*/ vec![true, false],
        /*expected_flush_lsn=*/ 400,
    )
    .await;
}

// Testing combination: (6) + (6) => snapshot with data files
#[tokio::test]
async fn test_state_6_6() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare committed and flushed deletion records.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare data files pre-requisite.
    let row = get_test_row_2();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 400)
        .await
        .unwrap();
    // Prepare committed but unflushed record batch.
    let row = get_test_row_3();
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 500);
    // Prepare uncommitted record batch.
    let row = get_test_row_5();
    table.append(row.clone()).unwrap();
    // Prepare uncommitted deletion record.
    table.delete(/*row=*/ row.clone(), /*lsn=*/ 600).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_new_data_files_and_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
        /*expected_next_file_id=*/
        5, // two data files, two index block files, one deletion vector puffin
        /*expected_prev_files_deleted=*/ vec![true, false],
        /*expected_flush_lsn=*/ 400,
    )
    .await;
}

// Testing combination: (7) + (1) => no snapshot
#[tokio::test]
async fn test_state_7_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_no_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (7) + (2) => no snapshot
#[tokio::test]
async fn test_state_7_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare uncommitted deletion record.
    table.delete(/*row=*/ old_row.clone(), /*lsn=*/ 200).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_initial_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (7) + (3) => no snapshot
#[tokio::test]
async fn test_state_7_3() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare committed deletion record.
    table.delete(/*row=*/ old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_initial_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (7) + (4) => no snapshot
#[tokio::test]
async fn test_state_7_4() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row_1, old_row_2) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare committed deletion record.
    table.delete(/*row=*/ old_row_1.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare uncommitted deletion record.
    table.delete(old_row_2, /*lsn=*/ 400).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_initial_snapshot(&mut iceberg_table_manager, filesystem_accessor.as_ref()).await;
}

// Testing combination: (7) + (5) => snapshot with deletion vector
#[tokio::test]
async fn test_state_7_5() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare committed and flushed deletion record.
    table.delete(/*row=*/ old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}

// Testing combination: (7) + (6) => snapshot with deletion vector
#[tokio::test]
async fn test_state_7_6() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager, mut notify_rx) =
        create_table_and_iceberg_manager(&temp_dir).await;
    // Prepare environment setup.
    let (old_row_1, old_row_2) =
        prepare_committed_and_flushed_data_files(&mut table, &mut notify_rx, /*lsn=*/ 100).await;
    // Prepare committed and flushed deletion record.
    table.delete(/*row=*/ old_row_1.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 300)
        .await
        .unwrap();
    // Prepare uncommitted deletion record.
    table.delete(old_row_2.clone(), /*lsn=*/ 400).await;

    // Request to persist.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Validate end state.
    validate_only_new_deletion_vectors_in_snapshot(
        &mut iceberg_table_manager,
        filesystem_accessor.as_ref(),
    )
    .await;
}
