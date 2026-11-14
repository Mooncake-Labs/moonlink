//! Iceberg metadata and superseded files do not accumulate.
use super::test_utils::*;
use super::*;
use crate::row::IdentityProp;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use std::sync::Arc;
use tokio::sync::mpsc;

fn files_under(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = vec![];
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(files_under(&p));
            } else {
                out.push(p);
            }
        }
    }
    out
}

fn table_dir(context: &TestContext) -> std::path::PathBuf {
    context.path().join("default").join("gc")
}

fn newest_metadata_json_size(context: &TestContext) -> u64 {
    files_under(&table_dir(context).join("metadata"))
        .into_iter()
        .filter_map(|p| {
            let name = p.file_name()?.to_string_lossy().to_string();
            let v: u32 = name
                .strip_prefix('v')?
                .strip_suffix(".metadata.json")?
                .parse()
                .ok()?;
            Some((v, std::fs::metadata(&p).ok()?.len()))
        })
        .max_by_key(|(v, _)| *v)
        .map(|(_, size)| size)
        .unwrap()
}

/// One live row, rewritten every commit (delete previous + insert next).
async fn churn(table: &mut MooncakeTable, rx: &mut mpsc::Receiver<TableEvent>, from: i32, to: i32) {
    for i in from..to {
        let lsn = (i as u64 + 1) * 10;
        table.append(test_row(i, "row", i)).unwrap();
        if i > 0 {
            table.delete(test_row(i - 1, "row", i - 1), lsn - 1).await;
        }
        table.commit(lsn);
        flush_table_and_sync(table, rx, lsn).await.unwrap();
        create_mooncake_and_persist_for_test(table, rx).await;
    }
}

/// Each metadata.json used to embed every snapshot ever taken (about 0.7 KB per commit).
#[tokio::test]
async fn test_metadata_json_does_not_grow_with_history() {
    let context = TestContext::new("gc");
    let mut table = test_table(&context, "gc", IdentityProp::FullRow).await;
    let (tx, mut rx) = mpsc::channel(100);
    table.register_table_notify(tx).await;
    churn(&mut table, &mut rx, 0, 5).await;
    let after_5 = newest_metadata_json_size(&context);
    churn(&mut table, &mut rx, 5, 50).await;
    let after_50 = newest_metadata_json_size(&context);
    assert!(
        after_50 < after_5 + 1024,
        "metadata.json grew from {after_5} to {after_50} bytes"
    );
}

fn names_in(dir: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.file_name().to_string_lossy().to_string())
                .collect()
        })
        .unwrap_or_default()
}

async fn live_rows(table: &MooncakeTable) -> usize {
    let guard = table.snapshot.read().await;
    guard
        .current_snapshot
        .disk_files
        .values()
        .map(|e| e.num_rows - e.committed_deletion_vector.get_num_rows_deleted())
        .sum()
}

async fn open_gc_table(context: &TestContext) -> (MooncakeTable, mpsc::Receiver<TableEvent>) {
    let mut table = test_table(context, "gc", IdentityProp::FullRow).await;
    let (tx, rx) = mpsc::channel(100);
    table.register_table_notify(tx).await;
    (table, rx)
}

/// After many commits only the current metadata (plus one previous metadata.json) and one
/// deletion-vector puffin per data file remain on disk.
#[tokio::test]
async fn test_superseded_iceberg_files_are_deleted() {
    let context = TestContext::new("gc");
    let (mut table, mut rx) = open_gc_table(&context).await;
    churn(&mut table, &mut rx, 0, 30).await;

    let metadata = names_in(&table_dir(&context).join("metadata"));
    let metadata_json = metadata
        .iter()
        .filter(|n| n.ends_with(".metadata.json"))
        .count();
    let manifest_lists = metadata.iter().filter(|n| n.starts_with("snap-")).count();
    assert!(metadata_json <= 2, "{metadata:?}");
    assert_eq!(manifest_lists, 1, "{metadata:?}");

    let data = names_in(&table_dir(&context).join("data"));
    let parquet = data.iter().filter(|n| n.ends_with(".parquet")).count();
    let dv_puffins = data
        .iter()
        .filter(|n| n.contains("deletion-vector"))
        .count();
    assert!(
        dv_puffins <= parquet,
        "old deletion vectors left behind: {data:?}"
    );
    drop(table);

    // Everything still loads, with the single live row.
    let (table, _rx) = open_gc_table(&context).await;
    assert_eq!(live_rows(&table).await, 1);
}

async fn compact_everything(table: &mut MooncakeTable, rx: &mut mpsc::Receiver<TableEvent>) {
    use crate::storage::snapshot_options::{
        IcebergSnapshotOption, MaintenanceOption, SnapshotOption,
    };
    assert!(table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::Skip,
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::ForceFull(uuid::Uuid::new_v4()),
    }));
    let (_, _, _, compaction, _) = sync_mooncake_snapshot(table, rx).await;
    let payload = compaction.take_payload().expect("compaction payload");
    perform_data_compaction_for_test(table, rx, payload).await;
    // Persist the compaction result.
    table.force_empty_persistence_payload();
    create_mooncake_and_persist_for_test(table, rx).await;
}

async fn write_files(table: &mut MooncakeTable, rx: &mut mpsc::Receiver<TableEvent>, n: i32) {
    for i in 1..=n {
        let lsn = i as u64 * 10;
        table.append(test_row(i, "r", i)).unwrap();
        table.commit(lsn);
        flush_table_and_sync(table, rx, lsn).await.unwrap();
        create_mooncake_and_persist_for_test(table, rx).await;
    }
}

fn parquet_in_data_dir(context: &TestContext) -> usize {
    names_in(&table_dir(context).join("data"))
        .iter()
        .filter(|n| n.ends_with(".parquet"))
        .count()
}

/// Data files replaced by compaction are deleted once no reader can use them.
#[tokio::test]
async fn test_compacted_files_are_deleted() {
    let context = TestContext::new("gc");
    let (mut table, mut rx) = open_gc_table(&context).await;
    write_files(&mut table, &mut rx, 4).await;
    assert_eq!(parquet_in_data_dir(&context), 4);
    compact_everything(&mut table, &mut rx).await;
    assert_eq!(parquet_in_data_dir(&context), 1);
    drop(table);
    let (table, _rx) = open_gc_table(&context).await;
    assert_eq!(live_rows(&table).await, 4);
}

/// While a reader still holds a snapshot from before the compaction, its files are kept.
#[tokio::test]
async fn test_gc_waits_for_live_readers() {
    let context = TestContext::new("gc");
    let (mut table, mut rx) = open_gc_table(&context).await;
    write_files(&mut table, &mut rx, 4).await;
    let reader_seq = Arc::new(std::sync::Mutex::new(Some(0u64)));
    let provider_seq = reader_seq.clone();
    table.set_live_read_seq_provider(Arc::new(move || *provider_seq.lock().unwrap()));

    compact_everything(&mut table, &mut rx).await;
    assert_eq!(
        parquet_in_data_dir(&context),
        5,
        "files deleted under a live reader"
    );

    // The reader finishes; the next commit collects the files.
    *reader_seq.lock().unwrap() = None;
    table.append(test_row(9, "r", 9)).unwrap();
    table.commit(500);
    flush_table_and_sync(&mut table, &mut rx, 500)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut rx).await;
    assert_eq!(parquet_in_data_dir(&context), 2);
}

/// Leftovers of crashes or failed commits are removed when the table is loaded; referenced files
/// are untouched.
#[tokio::test]
async fn test_load_sweeps_unreferenced_files() {
    let context = TestContext::new("gc");
    {
        let (mut table, mut rx) = open_gc_table(&context).await;
        write_files(&mut table, &mut rx, 2).await;
    }
    let data_dir = table_dir(&context).join("data");
    let metadata_dir = table_dir(&context).join("metadata");
    std::fs::write(data_dir.join("data-orphan.parquet"), b"orphan").unwrap();
    std::fs::write(metadata_dir.join("orphan-m0.avro"), b"orphan").unwrap();

    let (table, _rx) = open_gc_table(&context).await;
    assert!(!data_dir.join("data-orphan.parquet").exists());
    assert!(!metadata_dir.join("orphan-m0.avro").exists());
    assert_eq!(live_rows(&table).await, 2);
}
