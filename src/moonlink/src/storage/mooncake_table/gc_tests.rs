//! Iceberg metadata and superseded files do not accumulate.
use super::test_utils::*;
use super::*;
use crate::row::IdentityProp;
use crate::storage::mooncake_table::table_creation_test_utils::*;
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

/// GC table with the production cache setting (cache entries of persisted files point at the
/// iceberg files in place, so readers use them directly) and no reader grace period.
async fn open_gc_table(context: &TestContext) -> (MooncakeTable, mpsc::Receiver<TableEvent>) {
    let iceberg_table_config = test_iceberg_table_config(context, "gc");
    let mut table_config = test_mooncake_table_config(context);
    table_config.batch_size = 2;
    table_config.row_identity = IdentityProp::FullRow;
    table_config.persistence_config.new_committed_deletion_log = 1;
    let cache_dir = context.temp_dir.path().join("gc_cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let cache = Arc::new(crate::ObjectStorageCache::new(
        crate::ObjectStorageCacheConfig::new(
            1 << 30,
            cache_dir.to_str().unwrap().to_string(),
            /*optimize_local_filesystem=*/ true,
        ),
    ));
    let mut table = MooncakeTable::new(
        (*create_test_arrow_schema()).clone(),
        "gc".to_string(),
        1,
        context.path(),
        iceberg_table_config.clone(),
        table_config,
        cache,
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();
    table.set_gc_grace_for_test(std::time::Duration::ZERO);
    let (tx, rx) = mpsc::channel(100);
    table.register_table_notify(tx).await;
    (table, rx)
}

/// One more commit with a new row.
async fn commit_row(table: &mut MooncakeTable, rx: &mut mpsc::Receiver<TableEvent>, id: i32) {
    let lsn = 10_000 + id as u64 * 10;
    table.append(test_row(id, "r", id)).unwrap();
    table.commit(lsn);
    flush_table_and_sync(table, rx, lsn).await.unwrap();
    create_mooncake_and_persist_for_test(table, rx).await;
}

/// Files of the current mooncake snapshot's read state that no longer exist.
async fn missing_files_in_read(table: &MooncakeTable) -> Vec<String> {
    let mut guard = table.snapshot.write().await;
    let out = guard.request_read().await.unwrap();
    let mut paths: Vec<String> = out
        .puffin_cache_handles
        .iter()
        .map(|h| h.get_cache_filepath().to_string())
        .collect();
    paths.extend(
        out.data_file_paths
            .iter()
            .map(|f| f.get_file_path().to_string()),
    );
    paths
        .into_iter()
        .filter(|p| !std::path::Path::new(p).exists())
        .collect()
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
    // The current version and its predecessor (kept loadable as a fallback).
    assert!(metadata_json <= 2, "{metadata:?}");
    assert!(manifest_lists <= 2, "{metadata:?}");

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

/// Data files replaced by compaction are deleted once no reader can use them: not by the
/// compaction's own commit (the mooncake snapshot still serves them, and the previous version
/// keeps referencing them), but after the next one.
#[tokio::test]
async fn test_compacted_files_are_deleted() {
    let context = TestContext::new("gc");
    let (mut table, mut rx) = open_gc_table(&context).await;
    write_files(&mut table, &mut rx, 4).await;
    assert_eq!(parquet_in_data_dir(&context), 4);
    compact_everything(&mut table, &mut rx).await;
    assert_eq!(parquet_in_data_dir(&context), 5);
    assert!(missing_files_in_read(&table).await.is_empty());
    commit_row(&mut table, &mut rx, 100).await;
    assert_eq!(parquet_in_data_dir(&context), 2);
    assert!(missing_files_in_read(&table).await.is_empty());
    drop(table);
    let (table, _rx) = open_gc_table(&context).await;
    assert_eq!(live_rows(&table).await, 5);
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
    commit_row(&mut table, &mut rx, 100).await;
    assert_eq!(
        parquet_in_data_dir(&context),
        6,
        "files deleted under a live reader"
    );

    // The reader finishes; the next commit collects the files.
    *reader_seq.lock().unwrap() = None;
    commit_row(&mut table, &mut rx, 101).await;
    assert_eq!(parquet_in_data_dir(&context), 3);
}

/// A deletion-vector puffin replaced by a commit stays until the mooncake snapshot has applied
/// that commit: until then, read states built from the current snapshot still hand it out.
#[tokio::test]
async fn test_replaced_deletion_vector_kept_while_the_mooncake_snapshot_serves_it() {
    let context = TestContext::new("gc");
    let (mut table, mut rx) = open_gc_table(&context).await;
    // "No live readers": only the current mooncake snapshot can still hand out the file.
    table.set_live_read_seq_provider(Arc::new(|| None));
    for i in 1..=3 {
        table.append(test_row(i, "r", i)).unwrap();
    }
    table.commit(10);
    flush_table_and_sync(&mut table, &mut rx, 10).await.unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut rx).await;
    create_mooncake_and_persist_for_test(&mut table, &mut rx).await;

    for (row, lsn) in [(1, 20u64), (2, 30), (3, 40)] {
        table.delete(test_row(row, "r", row), lsn - 5).await;
        table.commit(lsn);
        flush_table_and_sync(&mut table, &mut rx, lsn)
            .await
            .unwrap();
        // Commits a new deletion vector; the mooncake snapshot has not applied it yet.
        create_mooncake_and_persist_for_test(&mut table, &mut rx).await;
        let missing = missing_files_in_read(&table).await;
        assert!(
            missing.is_empty(),
            "read state references deleted files: {missing:?}"
        );
        // Applies it.
        create_mooncake_and_persist_for_test(&mut table, &mut rx).await;
        let missing = missing_files_in_read(&table).await;
        assert!(
            missing.is_empty(),
            "read state references deleted files: {missing:?}"
        );
    }
    let dv_puffins = names_in(&table_dir(&context).join("data"))
        .iter()
        .filter(|n| n.contains("deletion-vector"))
        .count();
    assert!(dv_puffins <= 3, "superseded deletion vectors not collected");
}

/// An in-flight compaction or index merge reads files of the snapshot it was planned from; they
/// are kept until it finishes.
#[tokio::test]
async fn test_files_kept_while_maintenance_reads_them() {
    let context = TestContext::new("gc");
    let (mut table, mut rx) = open_gc_table(&context).await;
    write_files(&mut table, &mut rx, 4).await;
    let maintenance = table
        .gc_context
        .as_ref()
        .unwrap()
        .register_maintenance_reader();
    compact_everything(&mut table, &mut rx).await;
    commit_row(&mut table, &mut rx, 100).await;
    commit_row(&mut table, &mut rx, 101).await;
    assert_eq!(
        parquet_in_data_dir(&context),
        7,
        "files deleted under a maintenance task"
    );

    drop(maintenance);
    commit_row(&mut table, &mut rx, 102).await;
    assert_eq!(parquet_in_data_dir(&context), 4);
}

/// A second, read-only view of a table (e.g. a test validator) never deletes the owner's files.
#[tokio::test]
async fn test_validation_load_does_not_collect() {
    let context = TestContext::new("gc");
    let (mut table, mut rx) = open_gc_table(&context).await;
    write_files(&mut table, &mut rx, 2).await;
    let orphan = table_dir(&context).join("data").join("in-flight.parquet");
    std::fs::write(&orphan, b"written by an in-flight commit").unwrap();

    let iceberg_table_config = test_iceberg_table_config(&context, "gc");
    let mut table_config = test_mooncake_table_config(&context);
    table_config.row_identity = IdentityProp::FullRow;
    let cache_dir = tempfile::tempdir().unwrap();
    let view = MooncakeTable::new_without_gc(
        (*create_test_arrow_schema()).clone(),
        "gc".to_string(),
        1,
        context.path(),
        iceberg_table_config.clone(),
        table_config,
        create_test_object_storage_cache(&cache_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();
    assert_eq!(live_rows(&view).await, 2);
    assert!(orphan.exists());
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

/// The version before the current one stays loadable (the fallback when the hint is unusable),
/// also after a restart: load-time garbage collection keeps its data, deletion-vector and index
/// block files, although only the current version's index blocks are known without reading the
/// index blobs.
#[tokio::test]
async fn test_previous_version_loadable_after_restart() {
    let context = TestContext::new("gc");
    {
        let (mut table, mut rx) = open_gc_table(&context).await;
        write_files(&mut table, &mut rx, 4).await;
        table.delete(test_row(1, "r", 1), 45).await;
        table.commit(50);
        flush_table_and_sync(&mut table, &mut rx, 50).await.unwrap();
        create_mooncake_and_persist_for_test(&mut table, &mut rx).await;
        create_mooncake_and_persist_for_test(&mut table, &mut rx).await;
        // Replaces every data file, deletion vector and file index.
        compact_everything(&mut table, &mut rx).await;
    }
    // Restart: garbage collection at load.
    drop(open_gc_table(&context).await);

    let metadata_dir = table_dir(&context).join("metadata");
    let hint_path = metadata_dir.join("version-hint.text");
    let hint: u32 = std::fs::read_to_string(&hint_path)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    std::fs::write(&hint_path, (hint - 1).to_string()).unwrap();

    let iceberg_table_config = test_iceberg_table_config(&context, "gc");
    let mut table_config = test_mooncake_table_config(&context);
    table_config.row_identity = IdentityProp::FullRow;
    let cache_dir = tempfile::tempdir().unwrap();
    let previous = MooncakeTable::new_without_gc(
        (*create_test_arrow_schema()).clone(),
        "gc".to_string(),
        1,
        context.path(),
        iceberg_table_config.clone(),
        table_config,
        create_test_object_storage_cache(&cache_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .expect("the previous version no longer loads");
    let guard = previous.snapshot.read().await;
    assert_eq!(guard.current_snapshot.disk_files.len(), 4);
    let mut missing = vec![];
    for file in guard.current_snapshot.disk_files.keys() {
        if !std::path::Path::new(file.file_path()).exists() {
            missing.push(file.file_path().clone());
        }
    }
    assert!(
        missing.is_empty(),
        "previous version lost files: {missing:?}"
    );
    drop(guard);
    assert_eq!(live_rows(&previous).await, 3);
}
