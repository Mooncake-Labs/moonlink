//! Streamed transactions deleting identical rows (tables without a key) that are still in the
//! main mem slice.
use super::test_utils::*;
use super::*;
use crate::row::IdentityProp;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use tokio::sync::mpsc;

async fn table_with_batch_size(context: &TestContext, batch_size: usize) -> MooncakeTable {
    let iceberg_table_config = test_iceberg_table_config(context, "dup");
    let mut config = test_mooncake_table_config(context);
    config.batch_size = batch_size;
    config.row_identity = IdentityProp::FullRow;
    MooncakeTable::new(
        (*create_test_arrow_schema()).clone(),
        "dup".to_string(),
        1,
        context.path(),
        iceberg_table_config.clone(),
        config,
        create_test_object_storage_cache(&context.temp_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap()
}

async fn assert_remaining(
    table: &mut MooncakeTable,
    rx: &mut mpsc::Receiver<TableEvent>,
    lsn: u64,
    expected: &[i32],
) {
    create_mooncake_snapshot_for_test(table, rx).await;
    table.commit(lsn);
    flush_table_and_sync(table, rx, lsn).await.unwrap();
    create_mooncake_snapshot_for_test(table, rx).await;
    let mut guard = table.snapshot.write().await;
    let out = guard.request_read().await.unwrap();
    verify_files_and_deletions(
        get_data_files_for_read(&out.data_file_paths).as_slice(),
        get_deletion_puffin_files_for_read(&out.puffin_cache_handles).as_slice(),
        out.position_deletes,
        out.deletion_vectors,
        expected,
    )
    .await;
}

async fn run_duplicate_stream_delete(batch_size: usize, duplicates: usize) {
    let context = TestContext::new("dup_stream_delete");
    let mut table = table_with_batch_size(&context, batch_size).await;
    let (tx, mut rx) = mpsc::channel(100);
    table.register_table_notify(tx).await;

    let dup = test_row(7, "dup", 1);
    for _ in 0..duplicates {
        table.append(dup.clone()).unwrap();
    }
    table.append(test_row(8, "other", 2)).unwrap();
    table.commit(10);

    let xact_id = 42;
    for _ in 0..duplicates {
        table.delete_in_stream_batch(dup.clone(), xact_id).await;
    }
    commit_transaction_stream_and_sync(&mut table, &mut rx, xact_id, 20).await;
    assert_remaining(&mut table, &mut rx, 30, &[8]).await;
}

#[tokio::test]
async fn test_stream_delete_of_two_duplicates_in_finalized_batches() {
    run_duplicate_stream_delete(/*batch_size=*/ 2, /*duplicates=*/ 2).await;
}

#[tokio::test]
async fn test_stream_delete_of_three_duplicates_in_open_batch() {
    run_duplicate_stream_delete(/*batch_size=*/ 4096, /*duplicates=*/ 3).await;
}

/// A non-streamed delete takes the row the streamed transaction had picked; at stream commit the
/// next identical row is deleted instead, so both duplicates end up deleted exactly once.
#[tokio::test]
async fn test_stream_delete_racing_regular_delete_of_duplicate() {
    let context = TestContext::new("dup_race");
    let mut table = table_with_batch_size(&context, 4096).await;
    let (tx, mut rx) = mpsc::channel(100);
    table.register_table_notify(tx).await;

    let dup = test_row(7, "dup", 1);
    table.append(dup.clone()).unwrap();
    table.append(dup.clone()).unwrap();
    table.append(test_row(8, "other", 2)).unwrap();
    table.commit(10);

    let xact_id = 42;
    table.delete_in_stream_batch(dup.clone(), xact_id).await;
    table.delete(dup.clone(), 14).await;
    table.commit(15);
    commit_transaction_stream_and_sync(&mut table, &mut rx, xact_id, 20).await;
    assert_remaining(&mut table, &mut rx, 30, &[8]).await;
}

/// Each streamed delete of a distinct mem-slice row costs O(1) in the claimed positions (a
/// quadratic rebuild took minutes for a bulk DELETE of recently inserted rows).
#[tokio::test]
async fn test_many_streamed_deletes_of_mem_slice_rows_scale_linearly() {
    let context = TestContext::new("many_stream_deletes");
    let mut table = table_with_batch_size(&context, 4096).await;
    let (tx, mut rx) = mpsc::channel(100);
    table.register_table_notify(tx).await;

    const ROWS: i32 = 20_000;
    for id in 0..ROWS {
        table.append(test_row(id, "row", id)).unwrap();
    }
    table.commit(10);

    let xact_id = 42;
    let started = std::time::Instant::now();
    for id in 0..ROWS {
        table
            .delete_in_stream_batch(test_row(id, "row", id), xact_id)
            .await;
    }
    // Quadratic behaviour took ~160 s here in a debug build; linear takes well under a second.
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "streamed deletes took {:?}",
        started.elapsed()
    );
    commit_transaction_stream_and_sync(&mut table, &mut rx, xact_id, 20).await;
    assert_remaining(&mut table, &mut rx, 30, &[]).await;
}
