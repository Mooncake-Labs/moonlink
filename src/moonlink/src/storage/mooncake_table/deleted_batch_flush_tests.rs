//! Flushing a mem slice in which whole in-memory batches were deleted.
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

/// Regular (non-streamed) deletes of every row of a finalized in-memory batch, then a flush.
#[tokio::test]
async fn test_flush_after_deleting_whole_batch_fullrow() {
    let context = TestContext::new("whole_batch");
    let mut table = table_with_batch_size(&context, 2).await;
    let (tx, mut rx) = mpsc::channel(100);
    table.register_table_notify(tx).await;
    table.append(test_row(1, "a", 1)).unwrap();
    table.append(test_row(2, "b", 2)).unwrap();
    table.append(test_row(3, "c", 3)).unwrap();
    table.commit(10);
    table.delete(test_row(1, "a", 1), 14).await;
    table.delete(test_row(2, "b", 2), 14).await;
    table.commit(15);
    assert_remaining(&mut table, &mut rx, 30, &[3]).await;
}
