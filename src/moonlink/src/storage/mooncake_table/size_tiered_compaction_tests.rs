//! Background compaction merges files of similar size: an earlier compaction output is not
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

async fn create_table(context: &TestContext) -> (MooncakeTable, mpsc::Receiver<TableEvent>) {
    let iceberg_table_config = test_iceberg_table_config(context, "tiered");
    let mut config = test_mooncake_table_config(context);
    config.row_identity = IdentityProp::FullRow;
    config.data_compaction_config = DataCompactionConfig {
        min_data_file_to_compact: 4,
        max_data_file_to_compact: 8,
        data_file_final_size: u64::MAX,
        data_file_deletion_percentage: 0,
    };
    // Keep one index per file, so every data file is its own compaction group.
    config.file_index_config = FileIndexMergeConfig {
        min_file_indices_to_merge: u32::MAX,
        max_file_indices_to_merge: u32::MAX,
        index_block_final_size: u64::MAX,
    };
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

fn snapshot_option(compact: MaintenanceOption) -> SnapshotOption {
    SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4()),
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: compact,
    }
}

async fn snapshot_and_persist(
    table: &mut MooncakeTable,
    rx: &mut mpsc::Receiver<TableEvent>,
    compact: MaintenanceOption,
) -> DataCompactionMaintenanceStatus {
    assert!(table.try_create_mooncake_snapshot(snapshot_option(compact)));
    let (_, payload, merge, compaction, _) = sync_mooncake_snapshot(table, rx).await;
    assert!(matches!(merge, IndexMergeMaintenanceStatus::Unknown));
    if let Some(p) = payload {
        table.persist_iceberg_snapshot(p);
        let r = sync_iceberg_snapshot(rx).await;
        table.set_persistence_snapshot_res(r);
    }
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
