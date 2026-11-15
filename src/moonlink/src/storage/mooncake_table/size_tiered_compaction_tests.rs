//! Background compaction and index merge combine files of similar size: an earlier output is not
//! rewritten together with a handful of new small files.
use super::test_utils::*;
use super::*;
use crate::row::IdentityProp;
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::index::index_merge_config::FileIndexMergeConfig;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use crate::storage::snapshot_options::{IcebergSnapshotOption, MaintenanceOption, SnapshotOption};
use crate::table_notify::{DataCompactionMaintenanceStatus, IndexMergeMaintenanceStatus};
use tokio::sync::mpsc;

/// Large enough that a single-row file is above the size-tier floor.
const WIDE_NAME_LEN: usize = 128 * 1024;

/// Poorly compressible text, so file sizes follow the row sizes.
fn wide_name(seed: i32) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut x = (seed as u64).wrapping_mul(0x9E3779B97F4A7C15) | 1;
    (0..WIDE_NAME_LEN)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            ALPHABET[(x % ALPHABET.len() as u64) as usize] as char
        })
        .collect()
}

fn compaction_config(max_files: u32, deletion_percentage: u32) -> DataCompactionConfig {
    DataCompactionConfig {
        min_data_file_to_compact: 4,
        max_data_file_to_compact: max_files,
        data_file_final_size: u64::MAX,
        data_file_deletion_percentage: deletion_percentage,
    }
}

/// Keeps one index per file, so every data file is its own compaction group.
fn no_index_merge() -> FileIndexMergeConfig {
    FileIndexMergeConfig {
        min_file_indices_to_merge: u32::MAX,
        max_file_indices_to_merge: u32::MAX,
        index_block_final_size: u64::MAX,
    }
}

async fn create_table(context: &TestContext) -> (MooncakeTable, mpsc::Receiver<TableEvent>) {
    create_table_with(context, compaction_config(8, 0), no_index_merge()).await
}

async fn create_table_with(
    context: &TestContext,
    data_compaction_config: DataCompactionConfig,
    file_index_config: FileIndexMergeConfig,
) -> (MooncakeTable, mpsc::Receiver<TableEvent>) {
    let iceberg_table_config = test_iceberg_table_config(context, "tiered");
    let mut config = test_mooncake_table_config(context);
    config.row_identity = IdentityProp::FullRow;
    config.data_compaction_config = data_compaction_config;
    config.file_index_config = file_index_config;
    let mut table = MooncakeTable::new(
        (*create_test_arrow_schema()).clone(),
        "tiered".to_string(),
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

async fn snapshot_with_maintenance(
    table: &mut MooncakeTable,
    rx: &mut mpsc::Receiver<TableEvent>,
    index_merge: MaintenanceOption,
    compact: MaintenanceOption,
) -> (IndexMergeMaintenanceStatus, DataCompactionMaintenanceStatus) {
    assert!(table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4()),
        index_merge_option: index_merge,
        data_compaction_option: compact,
    }));
    let (_, payload, merge, compaction, _) = sync_mooncake_snapshot(table, rx).await;
    if let Some(p) = payload {
        table.persist_iceberg_snapshot(p);
        let r = sync_iceberg_snapshot(rx).await;
        table.set_persistence_snapshot_res(r);
    }
    (merge, compaction)
}

async fn snapshot_and_persist(
    table: &mut MooncakeTable,
    rx: &mut mpsc::Receiver<TableEvent>,
    compact: MaintenanceOption,
) -> DataCompactionMaintenanceStatus {
    let (merge, compaction) =
        snapshot_with_maintenance(table, rx, MaintenanceOption::Skip, compact).await;
    assert!(matches!(merge, IndexMergeMaintenanceStatus::Unknown));
    compaction
}

/// Flush and persist one wide single-row file per id.
async fn flush_wide_files(
    table: &mut MooncakeTable,
    rx: &mut mpsc::Receiver<TableEvent>,
    ids: std::ops::Range<i32>,
) {
    for id in ids {
        let lsn = id as u64 * 10;
        table.append(test_row(id, &wide_name(id), id)).unwrap();
        table.commit(lsn);
        flush_table_and_sync(table, rx, lsn).await.unwrap();
        snapshot_and_persist(table, rx, MaintenanceOption::Skip).await;
    }
}

async fn data_file_sizes(table: &MooncakeTable) -> HashMap<String, usize> {
    let guard = table.snapshot.read().await;
    guard
        .current_snapshot
        .disk_files
        .iter()
        .map(|(file, entry)| (file.file_path().clone(), entry.file_size))
        .collect()
}

fn best_effort() -> MaintenanceOption {
    MaintenanceOption::BestEffort(uuid::Uuid::new_v4())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_background_compaction_does_not_rewrite_large_output_with_small_files() {
    let context = TestContext::new("tiered_compaction");
    let (mut table, mut rx) = create_table(&context).await;

    // Eight similar files are compacted into one large file.
    flush_wide_files(&mut table, &mut rx, 1..9).await;
    let payload = snapshot_and_persist(&mut table, &mut rx, best_effort())
        .await
        .take_payload()
        .expect("eight similar files are compacted");
    assert_eq!(payload.disk_files.len(), 8);
    perform_data_compaction_for_test(&mut table, &mut rx, payload).await;
    snapshot_and_persist(&mut table, &mut rx, MaintenanceOption::Skip).await;
    let sizes = data_file_sizes(&table).await;
    assert_eq!(sizes.len(), 1);
    let (large_file, &large_size) = sizes.iter().next().unwrap();
    let large_file = large_file.clone();

    // Three new small files plus the large output reach the minimum of four, but the large file
    // is not merged with them.
    flush_wide_files(&mut table, &mut rx, 9..12).await;
    let small_size = *data_file_sizes(&table)
        .await
        .values()
        .filter(|&&s| s != large_size)
        .max()
        .unwrap();
    assert!(large_size > 4 * small_size.max(64 * 1024));
    let compaction = snapshot_and_persist(&mut table, &mut rx, best_effort()).await;
    assert!(compaction.take_payload().is_none());

    // A fourth small file completes a run of similar files; the large file stays untouched.
    flush_wide_files(&mut table, &mut rx, 12..13).await;
    let payload = snapshot_and_persist(&mut table, &mut rx, best_effort())
        .await
        .take_payload()
        .expect("four similar small files are compacted");
    assert_eq!(payload.disk_files.len(), 4);
    assert!(payload.disk_files.iter().all(|f| f.filepath != large_file));

    // An explicit request still merges everything eligible.
    let compaction = snapshot_and_persist(
        &mut table,
        &mut rx,
        MaintenanceOption::ForceRegular(uuid::Uuid::new_v4()),
    )
    .await;
    let payload = compaction.take_payload().expect("explicit compaction");
    assert_eq!(payload.disk_files.len(), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_background_compaction_takes_deletion_heavy_large_file() {
    let context = TestContext::new("tiered_compaction_deletions");
    let (mut table, mut rx) =
        create_table_with(&context, compaction_config(12, 50), no_index_merge()).await;

    // Twelve similar files are compacted into one large file.
    flush_wide_files(&mut table, &mut rx, 1..13).await;
    let payload = snapshot_and_persist(&mut table, &mut rx, best_effort())
        .await
        .take_payload()
        .expect("twelve similar files are compacted");
    assert_eq!(payload.disk_files.len(), 12);
    perform_data_compaction_for_test(&mut table, &mut rx, payload).await;
    snapshot_and_persist(&mut table, &mut rx, MaintenanceOption::Skip).await;
    let sizes = data_file_sizes(&table).await;
    assert_eq!(sizes.len(), 1);
    let large_size = *sizes.values().next().unwrap();

    // Half of its rows are deleted, between the LSNs of the last and the next flushes.
    for id in 1..7 {
        table.delete(test_row(id, &wide_name(id), id), 121).await;
    }
    table.commit(122);
    flush_table_and_sync(&mut table, &mut rx, 122)
        .await
        .unwrap();
    snapshot_and_persist(&mut table, &mut rx, MaintenanceOption::Skip).await;

    // With three new small files there are four eligible files. The large file's live half is
    // still far above the small files' size, but its deleted rows make it worth rewriting now.
    flush_wide_files(&mut table, &mut rx, 13..16).await;
    let sizes = data_file_sizes(&table).await;
    let large_file = sizes
        .iter()
        .find(|(_, &s)| s == large_size)
        .unwrap()
        .0
        .clone();
    let small_size = *sizes.values().filter(|&&s| s != large_size).max().unwrap();
    assert!(large_size / 2 > 4 * small_size.max(64 * 1024));
    let payload = snapshot_and_persist(&mut table, &mut rx, best_effort())
        .await
        .take_payload()
        .expect("the deletion-heavy file is compacted with the small files");
    assert_eq!(payload.disk_files.len(), 4);
    assert!(payload.disk_files.iter().any(|f| f.filepath == large_file));
}

async fn merge_best_effort(
    table: &mut MooncakeTable,
    rx: &mut mpsc::Receiver<TableEvent>,
) -> (IndexMergeMaintenanceStatus, DataCompactionMaintenanceStatus) {
    snapshot_with_maintenance(table, rx, best_effort(), MaintenanceOption::Skip).await
}

/// Rows per flushed file, so each file index is well above the size-tier floor.
const INDEX_ROWS_PER_FILE: i32 = 20_000;

/// Flush and persist one file of `INDEX_ROWS_PER_FILE` narrow rows per batch number.
async fn flush_many_row_files(
    table: &mut MooncakeTable,
    rx: &mut mpsc::Receiver<TableEvent>,
    batches: std::ops::Range<i32>,
) {
    for batch in batches {
        let lsn = batch as u64 * 10;
        for id in batch * INDEX_ROWS_PER_FILE..(batch + 1) * INDEX_ROWS_PER_FILE {
            table.append(test_row(id, "n", id)).unwrap();
        }
        table.commit(lsn);
        flush_table_and_sync(table, rx, lsn).await.unwrap();
        snapshot_and_persist(table, rx, MaintenanceOption::Skip).await;
    }
}

async fn index_sizes(table: &MooncakeTable) -> Vec<u64> {
    let guard = table.snapshot.read().await;
    guard
        .current_snapshot
        .indices
        .file_indices
        .iter()
        .map(|index| index.get_index_blocks_size())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_background_index_merge_does_not_rewrite_large_index_with_small_ones() {
    let context = TestContext::new("tiered_index_merge");
    let no_compaction = DataCompactionConfig {
        min_data_file_to_compact: u32::MAX,
        max_data_file_to_compact: u32::MAX,
        data_file_final_size: u64::MAX,
        data_file_deletion_percentage: 0,
    };
    let index_merge = FileIndexMergeConfig {
        min_file_indices_to_merge: 4,
        max_file_indices_to_merge: 8,
        index_block_final_size: u64::MAX,
    };
    let (mut table, mut rx) = create_table_with(&context, no_compaction, index_merge).await;

    // Eight similar indices are merged into one large index.
    flush_many_row_files(&mut table, &mut rx, 0..8).await;
    let small_size = index_sizes(&table).await[0];
    let (merge, _) = merge_best_effort(&mut table, &mut rx).await;
    let payload = merge
        .take_payload()
        .expect("eight similar indices are merged");
    assert_eq!(payload.file_indices.len(), 8);
    perform_index_merge_for_test(&mut table, &mut rx, payload).await;
    snapshot_and_persist(&mut table, &mut rx, MaintenanceOption::Skip).await;
    let sizes = index_sizes(&table).await;
    assert_eq!(sizes.len(), 1);
    assert!(small_size > 64 * 1024);
    assert!(sizes[0] > 4 * small_size);

    // Three new small indices plus the large one reach the minimum of four, but the large index
    // is not merged with them.
    flush_many_row_files(&mut table, &mut rx, 8..11).await;
    let (merge, _) = merge_best_effort(&mut table, &mut rx).await;
    assert!(merge.take_payload().is_none());

    // An explicit request still merges everything eligible.
    let (merge, _) = snapshot_with_maintenance(
        &mut table,
        &mut rx,
        MaintenanceOption::ForceRegular(uuid::Uuid::new_v4()),
        MaintenanceOption::Skip,
    )
    .await;
    let payload = merge.take_payload().expect("explicit index merge");
    assert_eq!(payload.file_indices.len(), 4);
}

/// Selection of compaction groups, on synthetic groups of one file each.
mod group_selection {
    use super::super::snapshot_maintenance::{
        choose_similar_sized_compaction_groups, CompactionGroup, CompactionGroupFile,
    };
    use crate::create_data_file;

    const MIB: usize = 1024 * 1024;

    fn group(file_id: u64, file_size: usize, deletion_percentage: usize) -> CompactionGroup {
        CompactionGroup {
            files: vec![CompactionGroupFile {
                data_file: create_data_file(file_id, format!("{file_id}.parquet")),
                file_size,
                deletion_percentage,
            }],
            file_indices: vec![],
        }
    }

    fn chosen_ids(chosen: Vec<CompactionGroup>) -> Vec<u64> {
        let mut ids: Vec<u64> = chosen
            .iter()
            .flat_map(|g| g.files.iter().map(|f| f.data_file.file_id().0))
            .collect();
        ids.sort();
        ids
    }

    #[test]
    fn test_partly_deleted_file_is_sized_by_live_bytes() {
        // 5 MiB with 80% deleted holds 1 MiB of live rows, like the three small files.
        let groups = vec![
            group(1, MIB, 0),
            group(2, MIB, 0),
            group(3, MIB, 0),
            group(4, 5 * MIB, 80),
        ];
        let chosen = choose_similar_sized_compaction_groups(groups, 4, 8, 0);
        assert_eq!(chosen_ids(chosen), vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_deletion_heavy_file_bypasses_size_tiers() {
        let groups = || {
            vec![
                group(1, MIB, 0),
                group(2, MIB, 0),
                group(3, MIB, 0),
                group(4, 300 * MIB, 70),
            ]
        };
        let chosen = choose_similar_sized_compaction_groups(groups(), 4, 8, 50);
        assert_eq!(chosen_ids(chosen), vec![1, 2, 3, 4]);
        // Below the deletion threshold the large file stays in its own tier.
        let chosen = choose_similar_sized_compaction_groups(groups(), 4, 8, 80);
        assert!(chosen.is_empty());
    }

    #[test]
    fn test_deletion_heavy_files_are_topped_up_with_the_smallest_files() {
        let groups = vec![
            group(1, 300 * MIB, 70),
            group(2, 40 * MIB, 0),
            group(3, MIB, 0),
            group(4, MIB, 0),
            group(5, 2 * MIB, 0),
        ];
        let chosen = choose_similar_sized_compaction_groups(groups, 4, 4, 50);
        assert_eq!(chosen_ids(chosen), vec![1, 3, 4, 5]);
    }

    #[test]
    fn test_deletion_heavy_files_still_need_min_files() {
        let groups = vec![group(1, 300 * MIB, 70), group(2, MIB, 0), group(3, MIB, 0)];
        let chosen = choose_similar_sized_compaction_groups(groups, 4, 8, 50);
        assert!(chosen.is_empty());
    }
}
