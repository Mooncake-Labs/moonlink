//! Compaction selects whole groups of data files sharing file indices, so no row loses its
//! index entry; compaction outputs beyond 100 files keep correct index segments.
use super::test_utils::*;
use super::*;
use crate::row::IdentityProp;
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::index::index_merge_config::FileIndexMergeConfig;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use crate::storage::snapshot_options::{IcebergSnapshotOption, MaintenanceOption, SnapshotOption};
use crate::table_notify::{DataCompactionMaintenanceStatus, IndexMergeMaintenanceStatus};
use std::time::Duration;
use tokio::sync::mpsc;

async fn table_with(
    context: &TestContext,
    compaction: DataCompactionConfig,
    index_merge: FileIndexMergeConfig,
) -> (MooncakeTable, mpsc::Receiver<TableEvent>) {
    let iceberg_table_config = test_iceberg_table_config(context, "groups");
    let mut config = test_mooncake_table_config(context);
    config.batch_size = 2;
    config.row_identity = IdentityProp::FullRow;
    config.data_compaction_config = compaction;
    config.file_index_config = index_merge;
    let mut table = MooncakeTable::new(
        (*create_test_arrow_schema()).clone(),
        "groups".to_string(),
        1,
        context.path(),
        iceberg_table_config.clone(),
        config,
        create_test_object_storage_cache(&context.temp_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();
    let (tx, rx) = mpsc::channel(1000);
    table.register_table_notify(tx).await;
    (table, rx)
}

fn opt(merge: MaintenanceOption, compact: MaintenanceOption) -> SnapshotOption {
    SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4()),
        index_merge_option: merge,
        data_compaction_option: compact,
    }
}

async fn snapshot_and_persist(
    table: &mut MooncakeTable,
    rx: &mut mpsc::Receiver<TableEvent>,
    o: SnapshotOption,
) -> (IndexMergeMaintenanceStatus, DataCompactionMaintenanceStatus) {
    assert!(table.try_create_mooncake_snapshot(o));
    let (_, payload, merge, compact, _) = sync_mooncake_snapshot(table, rx).await;
    if let Some(p) = payload {
        table.persist_iceberg_snapshot(p);
        let r = sync_iceberg_snapshot(rx).await;
        table.set_persistence_snapshot_res(r);
    }
    (merge, compact)
}

/// Flush `n` single-row files (ids 1..=n), each persisted with its own file index.
async fn flush_files(table: &mut MooncakeTable, rx: &mut mpsc::Receiver<TableEvent>, n: i32) {
    for i in 1..=n {
        let lsn = i as u64 * 10;
        table.append(test_row(i, "r", i)).unwrap();
        table.commit(lsn);
        flush_table_and_sync(table, rx, lsn).await.unwrap();
        snapshot_and_persist(
            table,
            rx,
            opt(MaintenanceOption::Skip, MaintenanceOption::Skip),
        )
        .await;
    }
}

async fn merge_all_indices(table: &mut MooncakeTable, rx: &mut mpsc::Receiver<TableEvent>) {
    let (merge, _) = snapshot_and_persist(
        table,
        rx,
        opt(
            MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
            MaintenanceOption::Skip,
        ),
    )
    .await;
    let payload = merge.take_payload().expect("index merge payload");
    perform_index_merge_for_test(table, rx, payload).await;
    snapshot_and_persist(
        table,
        rx,
        opt(MaintenanceOption::Skip, MaintenanceOption::Skip),
    )
    .await;
}

/// Delete every row 1..=n and check the deletions all apply (no panic, all rows deleted).
async fn delete_all_and_check(
    table: &mut MooncakeTable,
    rx: &mut mpsc::Receiver<TableEvent>,
    n: i32,
) {
    let base = 100_000u64;
    for i in 1..=n {
        table.delete(test_row(i, "r", i), base + i as u64 * 2).await;
        table.commit(base + i as u64 * 2 + 1);
    }
    tokio::time::timeout(
        Duration::from_secs(60),
        create_mooncake_snapshot_for_test(table, rx),
    )
    .await
    .expect("deleting compacted rows panicked the snapshot task");
    let guard = table.snapshot.read().await;
    let live: usize = guard
        .current_snapshot
        .disk_files
        .values()
        .map(|e| e.num_rows - e.committed_deletion_vector.get_num_rows_deleted())
        .sum();
    assert_eq!(live, 0, "some rows could not be deleted");
}

fn compaction_config(min: u32, max: u32, final_size: u64) -> DataCompactionConfig {
    DataCompactionConfig {
        min_data_file_to_compact: min,
        max_data_file_to_compact: max,
        data_file_final_size: final_size,
        data_file_deletion_percentage: 0,
    }
}

fn merge_everything() -> FileIndexMergeConfig {
    FileIndexMergeConfig {
        min_file_indices_to_merge: 2,
        max_file_indices_to_merge: u32::MAX,
        index_block_final_size: u64::MAX,
    }
}

/// Three files share one merged index; a cap of two files must not split the group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_compaction_never_splits_a_shared_index() {
    let context = TestContext::new("groups_split");
    let (mut table, mut rx) = table_with(
        &context,
        compaction_config(2, 2, u64::MAX),
        merge_everything(),
    )
    .await;
    flush_files(&mut table, &mut rx, 3).await;
    merge_all_indices(&mut table, &mut rx).await;

    let (_, compaction) = snapshot_and_persist(
        &mut table,
        &mut rx,
        opt(
            MaintenanceOption::Skip,
            MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
        ),
    )
    .await;
    assert!(
        compaction.take_payload().is_none(),
        "group of 3 exceeds the cap of 2"
    );
    delete_all_and_check(&mut table, &mut rx, 3).await;
}

/// With room for the whole group, all three files are compacted and every row stays deletable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_compaction_takes_whole_group() {
    let context = TestContext::new("groups_whole");
    let (mut table, mut rx) = table_with(
        &context,
        compaction_config(2, 3, u64::MAX),
        merge_everything(),
    )
    .await;
    flush_files(&mut table, &mut rx, 3).await;
    merge_all_indices(&mut table, &mut rx).await;

    let (_, compaction) = snapshot_and_persist(
        &mut table,
        &mut rx,
        opt(
            MaintenanceOption::Skip,
            MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
        ),
    )
    .await;
    let payload = compaction.take_payload().expect("compaction payload");
    assert_eq!(payload.disk_files.len(), 3);
    perform_data_compaction_for_test(&mut table, &mut rx, payload).await;
    delete_all_and_check(&mut table, &mut rx, 3).await;
}

/// A file at or above the compaction size threshold keeps its own index, so small files stay
/// compactable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_index_merge_skips_large_files() {
    let context = TestContext::new("groups_large");
    // Every single-row file is "large" against a 1-byte threshold.
    let (mut table, mut rx) =
        table_with(&context, compaction_config(2, 32, 1), merge_everything()).await;
    flush_files(&mut table, &mut rx, 3).await;
    let (merge, _) = snapshot_and_persist(
        &mut table,
        &mut rx,
        opt(
            MaintenanceOption::BestEffort(uuid::Uuid::new_v4()),
            MaintenanceOption::Skip,
        ),
    )
    .await;
    assert!(merge.take_payload().is_none());
}

/// More than 100 compaction outputs: index segments must still point at the right files.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_compaction_with_more_than_100_outputs() {
    let context = TestContext::new("groups_many");
    const FILES: i32 = 105;
    // Tiny final size: every input batch becomes its own output file.
    let (mut table, mut rx) = table_with(
        &context,
        compaction_config(2, FILES as u32, 1),
        FileIndexMergeConfig {
            min_file_indices_to_merge: u32::MAX,
            max_file_indices_to_merge: u32::MAX,
            index_block_final_size: u64::MAX,
        },
    )
    .await;
    flush_files(&mut table, &mut rx, FILES).await;
    let (_, compaction) = snapshot_and_persist(
        &mut table,
        &mut rx,
        opt(
            MaintenanceOption::Skip,
            MaintenanceOption::ForceFull(uuid::Uuid::new_v4()),
        ),
    )
    .await;
    let payload = compaction.take_payload().expect("compaction payload");
    assert_eq!(payload.disk_files.len(), FILES as usize);
    perform_data_compaction_for_test(&mut table, &mut rx, payload).await;
    {
        let guard = table.snapshot.read().await;
        assert!(guard.current_snapshot.disk_files.len() > 100);
    }
    delete_all_and_check(&mut table, &mut rx, FILES).await;
}
