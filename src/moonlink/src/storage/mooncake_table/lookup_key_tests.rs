//! Lookup keys are persisted in file indices, so they must not change across processes or
//! releases; indices written with the old (per-process random) hash are rebuilt on load.
use super::test_utils::*;
use super::*;
use crate::row::{IdentityProp, RowValue};
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use crate::StorageConfig;
use std::process::Command;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::mpsc;

const CHILD_ENV: &str = "MOONLINK_LOOKUP_KEY_TEST_CHILD";
const TABLE_DIR_ENV: &str = "MOONLINK_LOOKUP_KEY_TEST_TABLE_DIR";
const TEMP_DIR_ENV: &str = "MOONLINK_LOOKUP_KEY_TEST_TEMP_DIR";
const TABLE_NAME: &str = "lookup_key";

fn is_child() -> bool {
    std::env::var(CHILD_ENV).is_ok()
}

/// Re-run one test of this binary in a fresh process.
fn run_child(test_name: &str, envs: &[(&str, String)]) -> (bool, String) {
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args([test_name, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1");
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let out = cmd.output().unwrap();
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

/// Pins the hash: changing it requires bumping LOOKUP_KEY_VERSION (and relying on the rebuild).
#[test]
fn test_lookup_key_golden_values() {
    let row = test_row(1, "John", 30);
    assert_eq!(
        IdentityProp::FullRow.get_lookup_key(&row),
        GOLDEN_FULL_ROW_KEY
    );
    assert_eq!(
        IdentityProp::Keys(vec![0, 1]).get_lookup_key(&row),
        GOLDEN_KEYS_KEY
    );
    // The identity-row form (used when reading identity columns back from parquet) agrees.
    assert_eq!(
        IdentityProp::Keys(vec![0, 1]).get_lookup_key_from_identity_row(&MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray(b"John".to_vec()),
        ])),
        GOLDEN_KEYS_KEY
    );
}
const GOLDEN_FULL_ROW_KEY: u64 = 9028426538318932472;
const GOLDEN_KEYS_KEY: u64 = 4113000140003891303;

#[test]
fn test_lookup_key_stable_across_processes() {
    let key = IdentityProp::FullRow.get_lookup_key(&test_row(1, "John", 30));
    if is_child() {
        println!("LOOKUP_KEY={key}");
        return;
    }
    let (ok, out) = run_child(
        "storage::mooncake_table::lookup_key_tests::test_lookup_key_stable_across_processes",
        &[],
    );
    assert!(ok, "child failed: {out}");
    let child_key: u64 = out
        .lines()
        .find_map(|l| l.split("LOOKUP_KEY=").nth(1))
        .expect("child did not print key")
        .trim()
        .parse()
        .unwrap();
    assert_eq!(key, child_key);
}

fn iceberg_config_at(table_dir: &str) -> IcebergTableConfig {
    let storage_config = StorageConfig::FileSystem {
        root_directory: table_dir.to_string(),
        atomic_write_dir: None,
    };
    IcebergTableConfig {
        namespace: vec!["default".to_string()],
        table_name: TABLE_NAME.to_string(),
        data_accessor_config: AccessorConfig::new_with_storage_config(storage_config.clone()),
        metadata_accessor_config: crate::IcebergCatalogConfig::File {
            accessor_config: AccessorConfig::new_with_storage_config(storage_config),
        },
    }
}

async fn open_fullrow_table(table_dir: &str, temp_dir: &str, cache_dir: &TempDir) -> MooncakeTable {
    let iceberg_table_config = iceberg_config_at(table_dir);
    let mut table_config = MooncakeTableConfig::new(temp_dir.to_string());
    table_config.batch_size = 2;
    table_config.row_identity = IdentityProp::FullRow;
    MooncakeTable::new(
        (*create_test_arrow_schema()).clone(),
        TABLE_NAME.to_string(),
        1,
        PathBuf::from(table_dir),
        iceberg_table_config.clone(),
        table_config,
        create_test_object_storage_cache(cache_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap()
}

async fn deleted_rows(table: &MooncakeTable) -> usize {
    let guard = table.snapshot.read().await;
    guard
        .current_snapshot
        .disk_files
        .values()
        .map(|e| e.committed_deletion_vector.collect_deleted_rows().len())
        .sum()
}

async fn write_three_rows_and_persist(
    table: &mut MooncakeTable,
    rx: &mut mpsc::Receiver<TableEvent>,
) {
    table.append(test_row(1, "John", 30)).unwrap();
    table.append(test_row(2, "Bob", 40)).unwrap();
    table.append(test_row(3, "Ann", 50)).unwrap();
    table.commit(100);
    flush_table_and_sync(table, rx, 100).await.unwrap();
    create_mooncake_and_persist_for_test(table, rx).await;
    assert_eq!(table.last_persistence_snapshot_lsn, Some(100));
}

/// Process 1 persists rows; process 2 (a restart) deletes one of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_fullrow_delete_after_restart() {
    if is_child() {
        let table_dir = std::env::var(TABLE_DIR_ENV).unwrap();
        let temp_dir = std::env::var(TEMP_DIR_ENV).unwrap();
        let cache_dir = TempDir::new().unwrap();
        let mut table = open_fullrow_table(&table_dir, &temp_dir, &cache_dir).await;
        let (tx, mut rx) = mpsc::channel(100);
        table.register_table_notify(tx).await;
        table.delete(test_row(2, "Bob", 40), 200).await;
        table.commit(201);
        tokio::time::timeout(
            Duration::from_secs(30),
            create_mooncake_snapshot_for_test(&mut table, &mut rx),
        )
        .await
        .expect("snapshot after restart never completed");
        assert_eq!(deleted_rows(&table).await, 1);
        println!("CHILD_OK");
        return;
    }
    let context = TestContext::new(TABLE_NAME);
    let table_dir = context.path().to_str().unwrap().to_string();
    let temp_dir = context.temp_dir.path().to_str().unwrap().to_string();
    {
        let cache_dir = TempDir::new().unwrap();
        let mut table = open_fullrow_table(&table_dir, &temp_dir, &cache_dir).await;
        let (tx, mut rx) = mpsc::channel(100);
        table.register_table_notify(tx).await;
        write_three_rows_and_persist(&mut table, &mut rx).await;
    }
    let (ok, out) = run_child(
        "storage::mooncake_table::lookup_key_tests::test_fullrow_delete_after_restart",
        &[(TABLE_DIR_ENV, table_dir), (TEMP_DIR_ENV, temp_dir)],
    );
    assert!(ok && out.contains("CHILD_OK"), "child failed:\n{out}");
}

/// A table persisted by an older moonlink (different hash, no key-version property) gets its
/// file indices rebuilt on load: deletes of persisted rows work, and once persisted the table
/// is stamped with the current version (no rebuild on the next load).
#[tokio::test]
async fn test_legacy_file_indices_rebuilt_on_load() {
    let context = TestContext::new(TABLE_NAME);
    let table_dir = context.path().to_str().unwrap().to_string();
    let temp_dir = context.temp_dir.path().to_str().unwrap().to_string();

    crate::row::LEGACY_LOOKUP_KEY_SALT_FOR_TEST.with(|s| s.set(0x5eed));
    {
        let cache_dir = TempDir::new().unwrap();
        let mut table = open_fullrow_table(&table_dir, &temp_dir, &cache_dir).await;
        let (tx, mut rx) = mpsc::channel(100);
        table.register_table_notify(tx).await;
        write_three_rows_and_persist(&mut table, &mut rx).await;
    }
    crate::row::LEGACY_LOOKUP_KEY_SALT_FOR_TEST.with(|s| s.set(0));

    // Reload with the current hash: indices are rebuilt, the delete finds its row.
    {
        let cache_dir = TempDir::new().unwrap();
        let mut table = open_fullrow_table(&table_dir, &temp_dir, &cache_dir).await;
        assert!(!table.next_snapshot_task.index_merge_result.is_empty());
        let (tx, mut rx) = mpsc::channel(100);
        table.register_table_notify(tx).await;
        table.delete(test_row(2, "Bob", 40), 200).await;
        table.commit(201);
        // A loaded table commits to iceberg again at its next flush; that commit carries the
        // rebuilt indices and the new key version.
        flush_table_and_sync(&mut table, &mut rx, 201)
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(30),
            create_mooncake_and_persist_for_test(&mut table, &mut rx),
        )
        .await
        .expect("snapshot with rebuilt indices never completed");
        assert_eq!(deleted_rows(&table).await, 1);
    }

    // Next load: current version, no rebuild, and deletes still work.
    {
        let cache_dir = TempDir::new().unwrap();
        let mut table = open_fullrow_table(&table_dir, &temp_dir, &cache_dir).await;
        assert_eq!(table.last_persistence_snapshot_lsn, Some(201));
        assert!(table.next_snapshot_task.index_merge_result.is_empty());
        let (tx, mut rx) = mpsc::channel(100);
        table.register_table_notify(tx).await;
        table.delete(test_row(3, "Ann", 50), 300).await;
        table.commit(301);
        tokio::time::timeout(
            Duration::from_secs(30),
            create_mooncake_snapshot_for_test(&mut table, &mut rx),
        )
        .await
        .expect("snapshot after second reload never completed");
        assert_eq!(deleted_rows(&table).await, 2);
    }
}
