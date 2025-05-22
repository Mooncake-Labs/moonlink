use super::test_utils::*;
use crate::storage::mooncake_table::TableConfig as MooncakeTableConfig;
use crate::storage::IcebergOperation;

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

    println!("Initial insert LSN: {}", initial_insert_lsn);

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

#[tokio::test]
async fn test_iceberg_snapshot_creation() {
    let mooncake_table_config = MooncakeTableConfig {
        batch_size: MooncakeTableConfig::DEFAULT_BATCH_SIZE,
        // Flush to local filesystem as long as there's new data.
        mem_slice_size: 1,
        // Create mooncake table snapshot as long as there's new deletion records.
        snapshot_deletion_record_count: 1,
        // Create iceberg snapshot as long as there's new data file persisted.
        iceberg_snapshot_new_data_file_count: 1,
        // Create iceberg snapshot as long as there's new committed deletion logs.
        iceberg_snapshot_new_committed_deletion_log: 1,
    };
    let mut env = TestEnvironment::new(mooncake_table_config.clone()).await;

    // ---- Attempt to create snapshot before any rows appended ----
    // Snapshot creation function shouldn't get blocked.
    env.initiate_snapshot().await;
    env.sync_snapshot_completion().await;

    // ---- Create snapshot after new records appended ----
    // Append a new row to the mooncake table.
    env.append_row(
        /*id=*/ 1, /*name=*/ "John", /*age=*/ 30, /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 1).await;

    // Attempt an iceberg snapshot.
    env.initiate_snapshot().await;
    env.sync_snapshot_completion().await;

    // Load from iceberg table manager to make sure data file exists.
    let mut iceberg_table_manager = env.create_iceberg_table_manager(mooncake_table_config.clone());
    let snapshot = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.disk_files.len(), 1);
    let (cur_data_file, cur_deletion_vector) = snapshot.disk_files.into_iter().next().unwrap();
    assert!(tokio::fs::metadata(cur_data_file.file_path()).await.is_ok());
    assert!(cur_deletion_vector
        .batch_deletion_vector
        .collect_deleted_rows()
        .is_empty());
    assert!(cur_deletion_vector.puffin_deletion_blob.is_none());
}
