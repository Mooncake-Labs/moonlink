use arrow_array::{Int32Array, RecordBatch, StringArray};
use iceberg::{Error as IcebergError, ErrorKind};
use tempfile::tempdir;

use super::test_utils::*;
use crate::storage::mooncake_table::TableConfig as MooncakeTableConfig;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::MockTableManager;
use crate::storage::MooncakeTable;
use crate::storage::TableManager;
use crate::TableConfig;

use std::sync::Arc;

#[tokio::test]
async fn test_table_handler() {
    let mut env = TestEnvironment::default().await;

    env.append_row(1, "John", 30, None).await;
    env.commit(1).await;

    env.set_readable_lsn_with_cap(1, 100); // table_commit_lsn = 1, replication_lsn = 100
    env.verify_snapshot(1, &[1]).await;
    env.verify_snapshot(100, &[1]).await; // Reading at a higher LSN should still see data from LSN 1

    env.shutdown().await;
    println!("All table handler tests passed!");
}

#[tokio::test]
async fn test_table_handler_flush() {
    let mut env = TestEnvironment::default().await;

    let rows_data = vec![(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)];
    for (id, name, age) in rows_data {
        env.append_row(id, name, age, None).await;
    }

    env.commit(1).await;
    env.flush_table(1).await;

    env.set_readable_lsn_with_cap(1, 100);
    env.verify_snapshot(1, &[1, 2, 3]).await;

    env.shutdown().await;
    println!("All table handler flush tests passed!");
}

// Testing scenario: assign small parquet file size, which leads to multiple disk slice in one flush operation.
#[tokio::test]
async fn test_append_with_small_disk_slice() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config = MooncakeTableConfig {
        batch_size: 1,                   // One mem slice only contains one row.
        disk_slice_parquet_file_size: 1, // One parquet file only contains one arrow record.
        mem_slice_size: 1000,
        snapshot_deletion_record_count: 1000,
        iceberg_snapshot_new_data_file_count: 1000,
        iceberg_snapshot_new_committed_deletion_log: 1000,
        temp_files_directory: temp_dir.path().to_str().unwrap().to_string(),
    };
    let env = TestEnvironment::new(temp_dir, mooncake_table_config.clone()).await;

    // Append two rows, which appears in two parquet files.
    env.append_row(
        /*id=*/ 1, /*name=*/ "Alice", /*age=*/ 10, /*xact_id=*/ None,
    )
    .await;
    env.append_row(
        /*id=*/ 2, /*name=*/ "Blob", /*age=*/ 20, /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 10).await;
    env.flush_table(/*lsn=*/ 10).await;

    // Check whether both rows are accessible in snapshot.
    env.set_readable_lsn(10);
    env.verify_snapshot(/*target_lsn=*/ 10, /*expected_id=*/ &[1, 2])
        .await;
}

#[tokio::test]
async fn test_streaming_append_and_commit() {
    let mut env = TestEnvironment::default().await;
    let xact_id = 101;

    env.append_row(10, "Transaction-User", 25, Some(xact_id))
        .await;
    env.stream_commit(101, xact_id).await;

    env.set_readable_lsn(101);
    env.verify_snapshot(101, &[10]).await;

    env.shutdown().await;
}

#[tokio::test]
async fn test_streaming_delete() {
    let mut env = TestEnvironment::default().await;
    let xact_id = 101;

    env.append_row(10, "Transaction-User1", 25, Some(xact_id))
        .await;
    env.append_row(11, "Transaction-User2", 30, Some(xact_id))
        .await;

    // LSN for delete op (100) can be different from the stream commit LSN (101)
    env.delete_row(10, "Transaction-User1", 25, 100, Some(xact_id))
        .await;
    env.stream_commit(101, xact_id).await;

    env.set_readable_lsn(101);
    env.verify_snapshot(101, &[11]).await;

    env.shutdown().await;
}

#[tokio::test]
async fn test_streaming_abort() {
    let mut env = TestEnvironment::default().await;

    // Baseline data
    let baseline_xact_id = 100;
    env.append_row(1, "Baseline-User", 20, Some(baseline_xact_id))
        .await;
    env.stream_commit(100, baseline_xact_id).await;

    // Set table_commit_tx to allow ReadStateManager to know LSN 100 is committed.
    env.set_table_commit_lsn(100);

    // Transaction to be aborted
    let abort_xact_id = 102;
    env.append_row(20, "UserToAbort", 40, Some(abort_xact_id))
        .await;
    env.stream_abort(abort_xact_id).await;

    // Now enable reading up to LSN 100 by setting replication_tx.
    // The target_lsn for read is 100.
    env.set_replication_lsn(100);
    env.verify_snapshot(100, &[1]).await; // Should only see baseline data.

    env.shutdown().await;
}

#[tokio::test]
async fn test_concurrent_streaming_transactions() {
    let mut env = TestEnvironment::default().await;
    let xact_id_1 = 103; // Will be committed
    let xact_id_2 = 104; // Will be aborted

    env.append_row(30, "Transaction1-User", 35, Some(xact_id_1))
        .await;
    env.append_row(40, "Transaction2-User", 45, Some(xact_id_2))
        .await;

    env.stream_commit(103, xact_id_1).await; // Commit transaction 1 at LSN 103
    env.stream_abort(xact_id_2).await; // Abort transaction 2

    env.set_readable_lsn(103); // Make LSN 103 readable
    env.verify_snapshot(103, &[30]).await; // Verify only data from committed transaction 1

    env.shutdown().await;
}

#[tokio::test]
async fn test_stream_delete_unflushed_non_streamed_row() {
    let mut env = TestEnvironment::default().await;

    // Define LSNs and transaction ID for clarity
    let initial_insert_lsn = 10; // LSN for the non-streaming insert
    let stream_xact_id = 101; // Transaction ID for the streaming delete operation

    // The LSN passed to env.delete_row for a streaming op is used for the RawDeletionRecord,
    // but this LSN is typically overridden by the stream_commit_lsn when the transaction commits.
    // We use a distinct value here for clarity, but it's the stream_commit_lsn that's ultimately effective.
    let delete_op_event_lsn = 15;
    let stream_commit_lsn = 20; // LSN at which the streaming transaction (and its delete) is committed

    // --- Phase 1: Setup - Insert a row non-streamingly ---
    // This row (PK=1) will be added to pending writes.
    env.append_row(1, "Target User", 30, None).await;

    // Commit the non-streaming operation. This moves the row to the main mem_slice.
    // It is now "committed" at initial_insert_lsn but not yet flushed to disk.
    env.commit(initial_insert_lsn).await;

    // Inform the ReadStateManager that data up to initial_insert_lsn is committed.
    env.set_table_commit_lsn(initial_insert_lsn);
    // Set the replication LSN cap to allow reading up to initial_insert_lsn.
    env.set_replication_lsn(initial_insert_lsn);

    // Verify: The row (PK=1) should be visible in a snapshot at initial_insert_lsn.
    println!("1 Verifying snapshot at LSN {}", initial_insert_lsn);
    env.verify_snapshot(initial_insert_lsn, &[1]).await;

    // --- Phase 2: Action - Delete the non-streamed, unflushed row via a streaming transaction ---
    // Call delete_row for PK=1 within the context of stream_xact_id.
    // Inside table_handler.delete_in_stream_batch:
    //   - stream_state.mem_slice.delete() will be called. Since PK=1 was not added
    //     by stream_xact_id, it won't be in this transaction's mem_slice.
    //   - Thus, 'pos' returned by stream_state.mem_slice.delete() will be None.
    //   - The RawDeletionRecord will be created with pos: None.
    env.delete_row(
        1,
        "Target User",
        30,
        delete_op_event_lsn,
        Some(stream_xact_id),
    )
    .await;

    // Commit the streaming transaction.
    // During this commit, the TableHandler will process new_deletions for stream_xact_id.
    // For the deletion of PK=1 (which has pos: None), it should search the main mem_slice.
    // It will find PK=1 there and apply the deletion, associating it with stream_commit_lsn.
    env.stream_commit(stream_commit_lsn, stream_xact_id).await;

    // Update ReadStateManager: table state is now committed up to stream_commit_lsn.
    env.set_table_commit_lsn(stream_commit_lsn);
    // Update replication LSN cap to allow reading up to the new commit LSN.
    env.set_replication_lsn(stream_commit_lsn);

    // --- Phase 3: Verification ---
    // Verify: The row (PK=1) should NOT be visible in a snapshot at stream_commit_lsn.
    // The effective LSN for the read will be min(target_lsn=20, table_commit_lsn=20, replication_cap=20) = 20.
    println!("2 Verifying snapshot at LSN {}", stream_commit_lsn);
    env.verify_snapshot(stream_commit_lsn, &[]).await; // Expect empty slice (PK=1 deleted)

    env.shutdown().await;
    println!("Test test_stream_delete_unflushed_non_streamed_row passed!");
}

#[tokio::test]
async fn test_streaming_transaction_periodic_flush() {
    let mut env = TestEnvironment::default().await;
    let xact_id = 201;
    let commit_lsn = 20; // LSN at which the transaction will eventually commit
    let initial_read_lsn_target = commit_lsn; // For verifying no data pre-commit
    let final_read_lsn_target = commit_lsn; // For verifying all data post-commit

    // --- Phase 1: Append some data to the streaming transaction ---
    env.append_row(10, "StreamUser1-Part1", 25, Some(xact_id))
        .await;
    env.append_row(11, "StreamUser2-Part1", 30, Some(xact_id))
        .await;

    // --- Phase 2: Perform a periodic flush of the transaction stream ---
    env.stream_flush(xact_id).await;

    // --- Phase 3: Verify data is NOT visible after flush but BEFORE commit ---
    env.set_table_commit_lsn(0);
    env.set_replication_lsn(initial_read_lsn_target + 5);

    env.verify_snapshot(initial_read_lsn_target, &[]).await;

    // --- Phase 4: Append more data to the same transaction AFTER the periodic flush ---
    env.append_row(12, "StreamUser3-Part2", 35, Some(xact_id))
        .await;

    // --- Phase 5: Commit the streaming transaction ---
    env.stream_commit(commit_lsn, xact_id).await;

    // --- Phase 6: Verify ALL data (before and after periodic flush) is visible after commit ---

    env.set_table_commit_lsn(commit_lsn);
    env.set_replication_lsn(final_read_lsn_target + 5);

    env.verify_snapshot(final_read_lsn_target, &[10, 11, 12])
        .await;

    env.shutdown().await;
}

#[tokio::test]
async fn test_stream_delete_previously_flushed_row_same_xact() {
    let mut env = TestEnvironment::default().await;
    let xact_id = 401;
    let stream_commit_lsn = 40;

    // Phase 1: Append Row A (ID:10) to stream, then periodic flush
    env.append_row(10, "UserA-StreamFlush", 25, Some(xact_id))
        .await;
    env.stream_flush(xact_id).await; // Row A now in a xact-specific disk slice

    // Phase 2: In same stream, delete Row A (ID:10), append Row B (ID:11)
    env.delete_row(10, "UserA-StreamFlush", 25, 0, Some(xact_id))
        .await; // LSN placeholder
    env.append_row(11, "UserB-StreamSurvived", 30, Some(xact_id))
        .await;

    // Phase 3: Verify data is NOT visible before commit
    env.set_table_commit_lsn(0);
    env.set_replication_lsn(stream_commit_lsn + 5);
    env.verify_snapshot(stream_commit_lsn, &[]).await;

    // Phase 4: Commit the streaming transaction
    env.stream_commit(stream_commit_lsn, xact_id).await;

    // Phase 5: Verify final state
    env.set_table_commit_lsn(stream_commit_lsn);
    env.set_replication_lsn(stream_commit_lsn + 5);
    env.verify_snapshot(stream_commit_lsn, &[11]).await; // Only Row B (ID:11) should exist

    env.shutdown().await;
}

#[tokio::test]
async fn test_stream_delete_from_stream_memslice_row() {
    let mut env = TestEnvironment::default().await;
    let xact_id = 402;
    let stream_commit_lsn = 41;

    env.append_row(20, "UserC-StreamMem", 35, Some(xact_id))
        .await;

    // Phase 2: Delete Row C (ID:20) from stream's mem_slice, append Row D (ID:21)
    env.delete_row(20, "UserC-StreamMem", 35, 0, Some(xact_id))
        .await; // LSN placeholder
    env.append_row(21, "UserD-StreamSurvived", 40, Some(xact_id))
        .await;

    // Phase 3: Verify data is NOT visible before commit
    env.set_table_commit_lsn(0);
    env.set_replication_lsn(stream_commit_lsn + 5);
    env.verify_snapshot(stream_commit_lsn, &[]).await;

    // Phase 4: Commit the streaming transaction
    env.stream_commit(stream_commit_lsn, xact_id).await;

    println!("Phase 5: Verifying final state post-commit");
    // Phase 5: Verify final state
    env.set_table_commit_lsn(stream_commit_lsn);
    env.set_replication_lsn(stream_commit_lsn + 5);
    env.verify_snapshot(stream_commit_lsn, &[21]).await; // Only Row D (ID:21) should exist

    env.shutdown().await;
}

#[tokio::test]
async fn test_stream_delete_from_main_disk_row() {
    let mut env = TestEnvironment::default().await;
    let main_commit_lsn_flushed = 5; // LSN for the row that will be on disk
    let xact_id = 403;
    let stream_commit_lsn = 42;

    // Phase 1: Setup - Append Row G (ID:40), commit, and explicitly flush it to main disk
    env.append_row(40, "UserG-MainDisk", 50, None).await;
    env.commit(main_commit_lsn_flushed).await;
    env.flush_table(main_commit_lsn_flushed).await; // Explicit flush
    env.set_table_commit_lsn(main_commit_lsn_flushed);
    env.set_replication_lsn(main_commit_lsn_flushed + 5);
    env.verify_snapshot(main_commit_lsn_flushed, &[40]).await;

    // Phase 2: Start streaming transaction, delete Row G (ID:40), append Row H (ID:41)
    env.delete_row(40, "UserG-MainDisk", 50, 0, Some(xact_id))
        .await; // LSN placeholder
    env.append_row(41, "UserH-StreamSurvived", 55, Some(xact_id))
        .await;

    // Phase 3: Verify data is NOT visible before stream commit (Row G should still be there)
    // table_commit_lsn is still main_commit_lsn_flushed (5)
    env.set_replication_lsn(stream_commit_lsn + 5);
    // Effective read LSN = min(target=42, table_commit=5, replication=47) = 5
    env.verify_snapshot(stream_commit_lsn, &[40]).await; // Row G should still be visible

    // Phase 4: Commit the streaming transaction
    env.stream_commit(stream_commit_lsn, xact_id).await;

    // Phase 5: Verify final state
    env.set_table_commit_lsn(stream_commit_lsn);
    env.set_replication_lsn(stream_commit_lsn + 5);
    // Effective read LSN = min(target=42, table_commit=42, replication=47) = 42
    env.verify_snapshot(stream_commit_lsn, &[41]).await; // Only Row H (ID:41) should exist

    env.shutdown().await;
}

#[tokio::test]
async fn test_streaming_transaction_periodic_flush_then_abort() {
    let mut env = TestEnvironment::default().await;
    let baseline_xact_id = 500; // For baseline data
    let baseline_commit_lsn = 50;
    let aborted_xact_id = 501;
    // LSN for reads after abort; should reflect only committed data up to baseline_commit_lsn
    let read_lsn_after_abort = baseline_commit_lsn + 5;

    // --- Phase 1: Setup - Commit baseline data ---
    env.append_row(1, "BaselineUser", 30, Some(baseline_xact_id))
        .await;
    env.stream_commit(baseline_commit_lsn, baseline_xact_id)
        .await;
    env.set_table_commit_lsn(baseline_commit_lsn); // ReadStateManager knows LSN 50 is committed
    env.set_replication_lsn(read_lsn_after_abort);
    env.verify_snapshot(baseline_commit_lsn, &[1]).await;

    // --- Phase 3: Append Row A (ID:10) and periodically flush it ---
    env.append_row(10, "UserA-ToAbort-Flushed", 25, Some(aborted_xact_id))
        .await;
    env.stream_flush(aborted_xact_id).await; // Row A now in a xact-specific disk slice, uncommitted

    // --- Phase 4: Append Row B (ID:11) (stays in stream's mem-slice) ---
    env.append_row(11, "UserB-ToAbort-Mem", 35, Some(aborted_xact_id))
        .await;

    // --- Phase 5: Attempt to delete baseline Row (ID:1) within the aborted transaction ---
    env.delete_row(1, "BaselineUser", 30, 0, Some(aborted_xact_id))
        .await; // LSN placeholder

    // --- Phase 6: Abort the streaming transaction ---
    // This should discard TransactionStreamState for aborted_xact_id, including:
    // - The DiskSliceWriter containing Row A (ID:10).
    // - The MemSlice containing Row B (ID:11).
    // - The RawDeletionRecord for Row (ID:1).
    env.stream_abort(aborted_xact_id).await;

    // --- Phase 7: Verify state after abort ---
    // Effective read LSN = min(target=55, table_commit=50, replication=55) = 50
    env.verify_snapshot(read_lsn_after_abort, &[1]).await;

    env.shutdown().await;
}

// This test only checks whether drop table event send and receive works through table handler.
#[tokio::test]
async fn test_iceberg_drop_table() {
    let temp_dir = tempdir().unwrap();
    let mut env = TestEnvironment::new(temp_dir, MooncakeTableConfig::default()).await; // No temp files created.
    env.drop_iceberg_table().await.unwrap()
}

#[tokio::test]
async fn test_iceberg_snapshot_creation_for_batch_write() {
    // Set mooncake and iceberg flush and snapshot threshold to huge value, to verify force flush and force snapshot works as expected.
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config = MooncakeTableConfig {
        batch_size: MooncakeTableConfig::DEFAULT_BATCH_SIZE,
        disk_slice_parquet_file_size: MooncakeTableConfig::DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE,
        mem_slice_size: 1000,
        snapshot_deletion_record_count: 1000,
        iceberg_snapshot_new_data_file_count: 1000,
        iceberg_snapshot_new_committed_deletion_log: 1000,
        temp_files_directory: temp_dir.path().to_str().unwrap().to_string(),
    };
    let mut env = TestEnvironment::new(temp_dir, mooncake_table_config.clone()).await;

    // Arrow batches used in test.
    let arrow_batch_1 = RecordBatch::try_new(
        Arc::new(default_schema()),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["John".to_string()])),
            Arc::new(Int32Array::from(vec![30])),
        ],
    )
    .unwrap();
    let arrow_batch_2 = RecordBatch::try_new(
        Arc::new(default_schema()),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["Bob".to_string()])),
            Arc::new(Int32Array::from(vec![20])),
        ],
    )
    .unwrap();

    // ---- Create snapshot after new records appended ----
    // Append a new row to the mooncake table.
    env.append_row(
        /*id=*/ 1, /*name=*/ "John", /*age=*/ 30, /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 1).await;

    // Attempt an iceberg snapshot, with requested LSN already committed.
    let mut rx = env
        .iceberg_table_event_manager
        .initiate_snapshot(/*lsn=*/ 1)
        .await;
    rx.recv().await.unwrap().unwrap();

    // Load from iceberg table manager to check snapshot status.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let snapshot = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.disk_files.len(), 1);
    let (cur_data_file, cur_deletion_vector) = snapshot.disk_files.into_iter().next().unwrap();
    // Check data file.
    let actual_arrow_batch = load_arrow_batch(cur_data_file.file_path()).await;
    let expected_arrow_batch = arrow_batch_1.clone();
    assert_eq!(actual_arrow_batch, expected_arrow_batch);
    // Check deletion vector.
    assert!(cur_deletion_vector
        .batch_deletion_vector
        .collect_deleted_rows()
        .is_empty());
    check_deletion_vector_consistency(&cur_deletion_vector).await;
    assert!(cur_deletion_vector.puffin_deletion_blob.is_none());
    let old_data_file = cur_data_file;

    // ---- Create snapshot after new records appended and old records deleted ----
    //
    // Attempt an iceberg snapshot, which is a future flush LSN, and contains both new records and deletion records.
    let mut rx = env
        .iceberg_table_event_manager
        .initiate_snapshot(/*lsn=*/ 5)
        .await;
    env.append_row(
        /*id=*/ 2, /*name=*/ "Bob", /*age=*/ 20, /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 3).await;
    env.delete_row(
        /*id=*/ 1, /*name=*/ "John", /*age=*/ 30, /*lsn=*/ 4,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 5).await;

    // Block wait until iceberg snapshot created.
    rx.recv().await.unwrap().unwrap();

    // Load from iceberg table manager to check snapshot status.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let snapshot = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.disk_files.len(), 2);
    for (cur_data_file, cur_deletion_vector) in snapshot.disk_files.into_iter() {
        // Check the first data file.
        if cur_data_file.file_path() == old_data_file.file_path() {
            let actual_arrow_batch = load_arrow_batch(cur_data_file.file_path()).await;
            let expected_arrow_batch = arrow_batch_1.clone();
            assert_eq!(actual_arrow_batch, expected_arrow_batch);
            // Check the first deletion vector.
            assert_eq!(
                cur_deletion_vector
                    .batch_deletion_vector
                    .collect_deleted_rows(),
                vec![0]
            );
            check_deletion_vector_consistency(&cur_deletion_vector).await;
            continue;
        }

        // Check the second data file.
        let actual_arrow_batch = load_arrow_batch(cur_data_file.file_path()).await;
        let expected_arrow_batch = arrow_batch_2.clone();
        assert_eq!(actual_arrow_batch, expected_arrow_batch);
        // Check the second deletion vector.
        let deleted_rows = cur_deletion_vector
            .batch_deletion_vector
            .collect_deleted_rows();
        assert!(
            deleted_rows.is_empty(),
            "Deletion vector for the second data file is {:?}",
            deleted_rows
        );
        check_deletion_vector_consistency(&cur_deletion_vector).await;
    }

    // ---- Create snapshot only with old records deleted ----
    let mut rx = env
        .iceberg_table_event_manager
        .initiate_snapshot(/*lsn=*/ 7)
        .await;
    env.delete_row(
        /*id=*/ 2, /*name=*/ "Bob", /*age=*/ 20, /*lsn=*/ 6,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 7).await;

    // Block wait until iceberg snapshot created.
    rx.recv().await.unwrap().unwrap();

    // Load from iceberg table manager to check snapshot status.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let snapshot = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.disk_files.len(), 2);
    for (cur_data_file, cur_deletion_vector) in snapshot.disk_files.into_iter() {
        // Check the first data file.
        if cur_data_file.file_path() == old_data_file.file_path() {
            let actual_arrow_batch = load_arrow_batch(cur_data_file.file_path()).await;
            let expected_arrow_batch = arrow_batch_1.clone();
            assert_eq!(actual_arrow_batch, expected_arrow_batch);
            // Check the first deletion vector.
            assert_eq!(
                cur_deletion_vector
                    .batch_deletion_vector
                    .collect_deleted_rows(),
                vec![0]
            );
            check_deletion_vector_consistency(&cur_deletion_vector).await;
            continue;
        }

        // Check the second data file.
        let actual_arrow_batch = load_arrow_batch(cur_data_file.file_path()).await;
        let expected_arrow_batch = arrow_batch_2.clone();
        assert_eq!(actual_arrow_batch, expected_arrow_batch);
        // Check the second deletion vector.
        // Check the first deletion vector.
        assert_eq!(
            cur_deletion_vector
                .batch_deletion_vector
                .collect_deleted_rows(),
            vec![0]
        );
        check_deletion_vector_consistency(&cur_deletion_vector).await;
    }

    // Requested LSN is no later than current iceberg snapshot LSN.
    let mut rx = env
        .iceberg_table_event_manager
        .initiate_snapshot(/*lsn=*/ 1)
        .await;
    rx.recv().await.unwrap().unwrap();
}

#[tokio::test]
async fn test_iceberg_snapshot_creation_for_streaming_write() {
    // Set mooncake and iceberg flush and snapshot threshold to huge value, to verify force flush and force snapshot works as expected.
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config = MooncakeTableConfig {
        batch_size: MooncakeTableConfig::DEFAULT_BATCH_SIZE,
        disk_slice_parquet_file_size: MooncakeTableConfig::DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE,
        mem_slice_size: 1000,
        snapshot_deletion_record_count: 1000,
        iceberg_snapshot_new_data_file_count: 1000,
        iceberg_snapshot_new_committed_deletion_log: 1000,
        temp_files_directory: temp_dir.path().to_str().unwrap().to_string(),
    };
    let mut env = TestEnvironment::new(temp_dir, mooncake_table_config.clone()).await;

    // Arrow batches used in test.
    let arrow_batch_1 = RecordBatch::try_new(
        Arc::new(default_schema()),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["John".to_string()])),
            Arc::new(Int32Array::from(vec![30])),
        ],
    )
    .unwrap();
    let arrow_batch_2 = RecordBatch::try_new(
        Arc::new(default_schema()),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["Bob".to_string()])),
            Arc::new(Int32Array::from(vec![20])),
        ],
    )
    .unwrap();

    // ---- Create snapshot after new records appended ----
    // Append a new row to the mooncake table.
    env.append_row(
        /*id=*/ 1,
        /*name=*/ "John",
        /*age=*/ 30,
        /*xact_id=*/ Some(0),
    )
    .await;
    env.stream_commit(/*lsn=*/ 1, /*xact_id=*/ 0).await;

    // Attempt an iceberg snapshot, with requested LSN already committed.
    let mut rx = env
        .iceberg_table_event_manager
        .initiate_snapshot(/*lsn=*/ 1)
        .await;
    rx.recv().await.unwrap().unwrap();

    // Load from iceberg table manager to check snapshot status.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let snapshot = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.disk_files.len(), 1);
    let (cur_data_file, cur_deletion_vector) = snapshot.disk_files.into_iter().next().unwrap();
    // Check data file.
    let actual_arrow_batch = load_arrow_batch(cur_data_file.file_path()).await;
    let expected_arrow_batch = arrow_batch_1.clone();
    assert_eq!(actual_arrow_batch, expected_arrow_batch);
    // Check deletion vector.
    assert!(cur_deletion_vector
        .batch_deletion_vector
        .collect_deleted_rows()
        .is_empty());
    check_deletion_vector_consistency(&cur_deletion_vector).await;
    assert!(cur_deletion_vector.puffin_deletion_blob.is_none());
    let old_data_file = cur_data_file;

    // ---- Create snapshot after new records appended and old records deleted ----
    //
    // Attempt an iceberg snapshot, which is a future flush LSN, and contains both new records and deletion records.
    let mut rx = env
        .iceberg_table_event_manager
        .initiate_snapshot(/*lsn=*/ 5)
        .await;
    env.append_row(
        /*id=*/ 2,
        /*name=*/ "Bob",
        /*age=*/ 20,
        /*xact_id=*/ Some(3),
    )
    .await;
    env.stream_commit(/*lsn=*/ 3, /*xact_id=*/ 3).await;
    env.delete_row(
        /*id=*/ 1,
        /*name=*/ "John",
        /*age=*/ 30,
        /*lsn=*/ 4,
        /*xact_id=*/ Some(4),
    )
    .await;
    env.stream_commit(/*lsn=*/ 5, /*xact_id=*/ 4).await;

    // Block wait until iceberg snapshot created.
    rx.recv().await.unwrap().unwrap();

    // Load from iceberg table manager to check snapshot status.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let snapshot = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.disk_files.len(), 2);
    for (cur_data_file, cur_deletion_vector) in snapshot.disk_files.into_iter() {
        // Check the first data file.
        if cur_data_file.file_path() == old_data_file.file_path() {
            let actual_arrow_batch = load_arrow_batch(cur_data_file.file_path()).await;
            let expected_arrow_batch = arrow_batch_1.clone();
            assert_eq!(actual_arrow_batch, expected_arrow_batch);
            // Check the first deletion vector.
            assert_eq!(
                cur_deletion_vector
                    .batch_deletion_vector
                    .collect_deleted_rows(),
                vec![0]
            );
            check_deletion_vector_consistency(&cur_deletion_vector).await;
            continue;
        }

        // Check the second data file.
        let actual_arrow_batch = load_arrow_batch(cur_data_file.file_path()).await;
        let expected_arrow_batch = arrow_batch_2.clone();
        assert_eq!(actual_arrow_batch, expected_arrow_batch);
        // Check the second deletion vector.
        let deleted_rows = cur_deletion_vector
            .batch_deletion_vector
            .collect_deleted_rows();
        assert!(
            deleted_rows.is_empty(),
            "Deletion vector for the second data file is {:?}",
            deleted_rows
        );
        check_deletion_vector_consistency(&cur_deletion_vector).await;
    }

    // ---- Create snapshot only with old records deleted ----
    let mut rx = env
        .iceberg_table_event_manager
        .initiate_snapshot(/*lsn=*/ 7)
        .await;
    env.delete_row(
        /*id=*/ 2,
        /*name=*/ "Bob",
        /*age=*/ 20,
        /*lsn=*/ 6,
        /*xact_id=*/ Some(5),
    )
    .await;
    env.stream_commit(/*lsn=*/ 7, /*xact_id*/ 5).await;

    // Block wait until iceberg snapshot created.
    rx.recv().await.unwrap().unwrap();

    // Load from iceberg table manager to check snapshot status.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let snapshot = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.disk_files.len(), 2);
    for (cur_data_file, cur_deletion_vector) in snapshot.disk_files.into_iter() {
        // Check the first data file.
        if cur_data_file.file_path() == old_data_file.file_path() {
            let actual_arrow_batch = load_arrow_batch(cur_data_file.file_path()).await;
            let expected_arrow_batch = arrow_batch_1.clone();
            assert_eq!(actual_arrow_batch, expected_arrow_batch);
            // Check the first deletion vector.
            assert_eq!(
                cur_deletion_vector
                    .batch_deletion_vector
                    .collect_deleted_rows(),
                vec![0]
            );
            check_deletion_vector_consistency(&cur_deletion_vector).await;
            continue;
        }

        // Check the second data file.
        let actual_arrow_batch = load_arrow_batch(cur_data_file.file_path()).await;
        let expected_arrow_batch = arrow_batch_2.clone();
        assert_eq!(actual_arrow_batch, expected_arrow_batch);
        // Check the second deletion vector.
        // Check the first deletion vector.
        assert_eq!(
            cur_deletion_vector
                .batch_deletion_vector
                .collect_deleted_rows(),
            vec![0]
        );
        check_deletion_vector_consistency(&cur_deletion_vector).await;
    }

    // Requested LSN is no later than current iceberg snapshot LSN.
    let mut rx = env
        .iceberg_table_event_manager
        .initiate_snapshot(/*lsn=*/ 1)
        .await;
    rx.recv().await.unwrap().unwrap();
}

/// Testing scenario: iceberg snapshot request shouldn't block, even if there's no write operations to the table.
#[tokio::test]
async fn test_empty_table_snapshot_creation() {
    let temp_dir = tempdir().unwrap();
    let mut env = TestEnvironment::new(temp_dir, MooncakeTableConfig::default()).await;

    let mut rx_vec = Vec::with_capacity(10);
    for _ in 1..=10 {
        let rx = env
            .iceberg_table_event_manager
            .initiate_snapshot(/*lsn=*/ 0)
            .await;
        rx_vec.push(rx);
    }
    for mut cur_rx in rx_vec {
        cur_rx.recv().await.unwrap().unwrap();
    }
}

/// Testing senario: request iceberg snapshot with multiple LSNs.
#[tokio::test]
async fn test_multiple_snapshot_requests() {
    // Set mooncake and iceberg flush and snapshot threshold to huge value, to verify force flush and force snapshot works as expected.
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config = MooncakeTableConfig {
        batch_size: MooncakeTableConfig::DEFAULT_BATCH_SIZE,
        disk_slice_parquet_file_size: MooncakeTableConfig::DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE,
        mem_slice_size: 1000,
        snapshot_deletion_record_count: 1000,
        iceberg_snapshot_new_data_file_count: 1000,
        iceberg_snapshot_new_committed_deletion_log: 1000,
        temp_files_directory: temp_dir.path().to_str().unwrap().to_string(),
    };
    let mut env = TestEnvironment::new(temp_dir, mooncake_table_config.clone()).await;

    // Arrow batches used in test.
    let arrow_batch_1 = RecordBatch::try_new(
        Arc::new(default_schema()),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["John".to_string()])),
            Arc::new(Int32Array::from(vec![30])),
        ],
    )
    .unwrap();
    let arrow_batch_2 = RecordBatch::try_new(
        Arc::new(default_schema()),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["Bob".to_string()])),
            Arc::new(Int32Array::from(vec![20])),
        ],
    )
    .unwrap();

    // Make a commit request at the very beginning, so iceberg snapshot request won't return immediately.
    env.commit(/*lsn=*/ 0).await;

    // Create multiple iceberg snapshot requests in advance.
    let mut rx_vec = Vec::new();
    // First flush and commit LSN.
    rx_vec.push(
        env.iceberg_table_event_manager
            .initiate_snapshot(/*lsn=*/ 1)
            .await,
    );
    // Second flush and commit LSN.
    rx_vec.push(
        env.iceberg_table_event_manager
            .initiate_snapshot(/*lsn=*/ 2)
            .await,
    );
    // The same requested LSN as previous.
    rx_vec.push(
        env.iceberg_table_event_manager
            .initiate_snapshot(/*lsn=*/ 2)
            .await,
    );
    // A LSN already satisfied.
    rx_vec.push(
        env.iceberg_table_event_manager
            .initiate_snapshot(/*lsn=*/ 0)
            .await,
    );

    // Append a new row to the mooncake table.
    env.append_row(
        /*id=*/ 1, /*name=*/ "John", /*age=*/ 30, /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 1).await;

    // Append a new row to the mooncake table.
    env.append_row(
        /*id=*/ 2, /*name=*/ "Bob", /*age=*/ 20, /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 2).await;

    for mut rx in rx_vec.into_iter() {
        rx.recv().await.unwrap().unwrap();
    }

    // Check iceberg snapshot content.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let snapshot = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.disk_files.len(), 2);

    let mut visited = [false, false]; // Check both row flushed.
    for (cur_data_file, cur_deletion_vector) in snapshot.disk_files.into_iter() {
        // Check the data file.
        let actual_arrow_batch = load_arrow_batch(cur_data_file.file_path()).await;
        if actual_arrow_batch == arrow_batch_1 {
            visited[0] = true;
        }
        if actual_arrow_batch == arrow_batch_2 {
            visited[1] = true;
        }

        // Check the deletion vector.
        assert!(cur_deletion_vector
            .batch_deletion_vector
            .collect_deleted_rows()
            .is_empty(),);
        check_deletion_vector_consistency(&cur_deletion_vector).await;
    }
    assert_eq!(visited, [true, true]);
}

/// ---- Mock unit test ----
#[tokio::test]
async fn test_iceberg_snapshot_failure_mock_test() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config = TableConfig::new(temp_dir.path().to_str().unwrap().to_string());
    let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
        name: "table_name".to_string(),
        id: 0,
        schema: Arc::new(default_schema()),
        config: mooncake_table_config.clone(),
        path: temp_dir.path().to_path_buf(),
        identity: crate::row::IdentityProp::Keys(vec![0]),
    });

    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_sync_snapshot()
        .times(1)
        .returning(|_| {
            Box::pin(async move {
                Err(IcebergError::new(
                    ErrorKind::Unexpected,
                    "Intended error for unit test",
                ))
            })
        });

    let mooncake_table = MooncakeTable::new_with_table_manager(
        mooncake_table_metadata,
        Box::new(mock_table_manager),
        mooncake_table_config,
    )
    .await
    .unwrap();
    let mut env = TestEnvironment::new_with_mooncake_table(temp_dir, mooncake_table).await;

    // Append rows to trigger mooncake and iceberg snapshot.
    env.append_row(
        /*id=*/ 1, /*name=*/ "Alice", /*age=*/ 10, /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 10).await;

    // Initiate snapshot and block wait its completion, check whether error status is correctly propagated.
    let mut rx = env
        .iceberg_table_event_manager
        .initiate_snapshot(/*lsn=*/ 10)
        .await;
    let res = rx.recv().await.unwrap();
    assert!(res.is_err());
}

#[tokio::test]
async fn test_iceberg_drop_table_failure_mock_test() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config = TableConfig::new(temp_dir.path().to_str().unwrap().to_string());
    let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
        name: "table_name".to_string(),
        id: 0,
        schema: Arc::new(default_schema()),
        config: mooncake_table_config.clone(),
        path: temp_dir.path().to_path_buf(),
        identity: crate::row::IdentityProp::Keys(vec![0]),
    });

    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_drop_table()
        .times(1)
        .returning(|| {
            Box::pin(async move {
                Err(IcebergError::new(
                    ErrorKind::Unexpected,
                    "Intended error for unit test",
                ))
            })
        });

    let mooncake_table = MooncakeTable::new_with_table_manager(
        mooncake_table_metadata,
        Box::new(mock_table_manager),
        mooncake_table_config,
    )
    .await
    .unwrap();
    let mut env = TestEnvironment::new_with_mooncake_table(temp_dir, mooncake_table).await;

    // Drop table and block wait its completion, check whether error status is correctly propagated.
    let res = env.drop_iceberg_table().await;
    assert!(res.is_err());
}
