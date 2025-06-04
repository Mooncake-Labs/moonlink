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
// State-based tests are used to guarantee persisted iceberg snapshots are at a consistent view.
// For states, refer to https://docs.google.com/document/d/1hZ0H66_eefjFezFQf1Pdr-A63eJ94OH9TsInNS-6xao/edit?usp=sharing
//
// A few testing assumptions / preparations:
// - There will be at most two types of data files, one written before test case perform any append/delete operations, another after new rows appended.
// - To differentiate these two types of data files, the first type of record batch contains two rows, the second type of record batch contains only one row.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use iceberg::Result as IcebergResult;

use crate::row::MoonlinkRow;
use crate::row::RowValue;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::storage::iceberg::iceberg_table_manager::TableManager;
use crate::storage::iceberg::test_utils::{
    check_deletion_vector_consistency_for_snapshot, create_table_and_iceberg_manager,
    load_arrow_batch,
};
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::Snapshot;
use crate::storage::MooncakeTable;

// Test util function to prepare for committed and persisted data file,
// here we write two rows and assume they'll be included in one arrow record batch and one data file.
async fn prepare_committed_and_flushed_data_files(
    table: &mut MooncakeTable,
    lsn: u64,
) -> (MoonlinkRow, MoonlinkRow) {
    // Append first row.
    let row_1 = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row_1.clone()).unwrap();

    // Append second row.
    let row_2 = MoonlinkRow::new(vec![
        RowValue::Int32(4),
        RowValue::ByteArray("David".as_bytes().to_vec()),
        RowValue::Int32(40),
    ]);
    table.append(row_2.clone()).unwrap();

    table.commit(lsn);
    table.flush(lsn).await.unwrap();

    (row_2, row_1)
}

// Test util function to check whether the data file in iceberg snapshot is the same as initially-prepared one from `prepare_committed_and_flushed_data_files`.
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
            Arc::new(Int32Array::from(vec![30, 40])),
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
            Arc::new(Int32Array::from(vec![30])),
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
            Arc::new(Int32Array::from(vec![30, 40])),
        ],
    )
    .unwrap();
    let expected_arrow_batch_2 = RecordBatch::try_new(
        iceberg_table_manager.mooncake_table_metadata.schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["Tim"])),
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

// Testing combination: (1) + (1) => no snapshot
#[tokio::test]
async fn test_state_1_1() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    assert!(table.create_snapshot().is_none());

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (1) + (2) => no snapshot
#[tokio::test]
async fn test_state_1_2() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    // Prepate deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 1).await;

    // Request to create snapshot.
    assert!(table.create_snapshot().is_none());

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (1) + (3) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_1_3() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (1) + (4) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_1_4() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 400).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (1) + (5) => snapshot with deletion vector
#[tokio::test]
async fn test_state_1_5() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ true).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (1) + (6) => snapshot with deletion vector
#[tokio::test]
async fn test_state_1_6() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 400).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ true).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (2) + (1) => no snapshot
#[tokio::test]
async fn test_state_2_1() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (2) + (2) => no snapshot
#[tokio::test]
async fn test_state_2_2() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    // Prepate deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 200).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (2) + (3) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_2_3() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (2) + (4) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_2_4() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 500).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (2) + (5) => snapshot with deletion vector
#[tokio::test]
async fn test_state_2_5() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ true).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (2) + (6) => snapshot with deletion vector
#[tokio::test]
async fn test_state_2_6() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 500).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ true).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (1) => snapshot with data file created
#[tokio::test]
async fn test_state_3_1() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_new_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (2) => snapshot with data files created
#[tokio::test]
async fn test_state_3_2() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();
    // Prepate deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 300).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_new_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (3) + committed deletion before flush => snapshot with data files and deletion vector
#[tokio::test]
async fn test_state_3_3_deletion_before_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 500).await.unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (3) + committed deletion after flush => snapshot with data files
#[tokio::test]
async fn test_state_3_3_deletion_after_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![false, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (4) + committed deletion record before flush => snapshot with data files with deletion vector
#[tokio::test]
async fn test_state_3_4_committed_deletion_before_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 500).await.unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (4) + committed deletion record after flush => snapshot with data files
#[tokio::test]
async fn test_state_3_4_committed_deletion_after_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![false, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (5) => snapshot with data files with deletion vector
#[tokio::test]
async fn test_state_3_5() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 400).await.unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 400);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (6) => snapshot with data files with deletion vector
#[tokio::test]
async fn test_state_3_6() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 400).await.unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 500).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 400);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (4) + (1) => no snapshot
#[tokio::test]
async fn test_state_4_1() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite (committed record batch).
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (4) + (2) => no snapshot
#[tokio::test]
async fn test_state_4_2() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite (committed record batch).
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    // Prepate deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 200).await;
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (4) + (3) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_4_3() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed record batch).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (4) + (4) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_4_4() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 500).await;
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (4) + (5) => snapshot with deletion vector
#[tokio::test]
async fn test_state_4_5() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare committed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("David".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ true).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (4) + (6) => snapshot with deletion vector
#[tokio::test]
async fn test_state_4_6() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed and flushed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare committed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("David".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    // Prepare uncommitted deletion record.
    table.delete(row.clone(), /*lsn=*/ 500).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ true).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (1) => snapshot with data file created
#[tokio::test]
async fn test_state_5_1() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 300);

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_new_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (2) => snapshot with data files created
#[tokio::test]
async fn test_state_5_2() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();
    // Prepate deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 300).await;
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 400);

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_new_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (3) + committed deletion before flush => snapshot with data files and deletion vector
#[tokio::test]
async fn test_state_5_3_deletion_before_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 500).await.unwrap();
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 600);

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (3) + committed deletion after flush => snapshot with data files
#[tokio::test]
async fn test_state_5_3_deletion_after_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 600);

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![false, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (4) + committed deletion record before flush => snapshot with data files with deletion vector
#[tokio::test]
async fn test_state_5_4_committed_deletion_before_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 500).await.unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 700);

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (4) + committed deletion record after flush => snapshot with data files
#[tokio::test]
async fn test_state_5_4_committed_deletion_after_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 700);

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![false, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (5) => snapshot with data files and deletion vector
#[tokio::test]
async fn test_state_5_5() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare committed and flushed deletion record.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 400).await.unwrap();
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 500);

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 400);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (6) => snapshot with data files and deletion vector
#[tokio::test]
async fn test_state_5_6() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare committed and flushed deletion record.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 400).await.unwrap();
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 500);
    // Prepare uncommitted deletion record.
    table.delete(row.clone(), /*lsn=*/ 600).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 400);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (1) => snapshot with data file created
#[tokio::test]
async fn test_state_6_1() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 300);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_new_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (2) => snapshot with data files created
#[tokio::test]
async fn test_state_6_2() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();
    // Prepate deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 300).await;
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_new_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (3) + committed deletion before flush => snapshot with data files and deletion vector
#[tokio::test]
async fn test_state_6_3_deletion_before_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 500).await.unwrap();
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 600);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (3) + committed deletion after flush => snapshot with data files
#[tokio::test]
async fn test_state_6_3_deletion_after_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 600);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![false, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (4) + committed deletion record before flush => snapshot with data files with deletion vector
#[tokio::test]
async fn test_state_6_4_committed_deletion_before_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 500).await.unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 700);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (4) + committed deletion record after flush => snapshot with data files
#[tokio::test]
async fn test_state_6_4_committed_deletion_after_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 700);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![false, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (5) => snapshot with data files
#[tokio::test]
async fn test_state_6_5() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare committed and flushed deletion records.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 400).await.unwrap();
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 500);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 400);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (6) => snapshot with data files
#[tokio::test]
async fn test_state_6_6() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare committed and flushed deletion records.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 400).await.unwrap();
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 500);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row.clone()).unwrap();
    // Prepare uncommitted deletion record.
    table.delete(/*row=*/ row.clone(), /*lsn=*/ 600).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 400);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (7) + (1) => no snapshot
#[tokio::test]
async fn test_state_7_1() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (7) + (2) => no snapshot
#[tokio::test]
async fn test_state_7_2() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare uncommitted deletion record.
    table.delete(/*row=*/ old_row.clone(), /*lsn=*/ 200).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (7) + (3) => no snapshot
#[tokio::test]
async fn test_state_7_3() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare committed deletion record.
    table.delete(/*row=*/ old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (7) + (4) => no snapshot
#[tokio::test]
async fn test_state_7_4() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row_1, old_row_2) =
        prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare committed deletion record.
    table.delete(/*row=*/ old_row_1.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepare uncommitted deletion record.
    table.delete(old_row_2, /*lsn=*/ 400).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (7) + (5) => snapshot with deletion vector
#[tokio::test]
async fn test_state_7_5() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row, _) = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare committed and flushed deletion record.
    table.delete(/*row=*/ old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ true).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (7) + (6) => snapshot with deletion vector
#[tokio::test]
async fn test_state_7_6() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let (old_row_1, old_row_2) =
        prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare committed and flushed deletion record.
    table.delete(/*row=*/ old_row_1.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare uncommitted deletion record.
    table.delete(old_row_2.clone(), /*lsn=*/ 400).await;

    // Request to create snapshot.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await
        .unwrap();

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ true).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}
