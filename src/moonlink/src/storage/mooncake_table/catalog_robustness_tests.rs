//! The file catalog survives interrupted writes: a lost or empty version hint falls back to the
//! newest readable metadata file, a hint naming an unreadable metadata file fails the load, and
//! existing tables are never re-created empty.
use super::test_utils::*;
use super::*;
use crate::row::IdentityProp;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use tokio::sync::mpsc;

async fn open(context: &TestContext) -> (MooncakeTable, mpsc::Receiver<TableEvent>) {
    let mut table = test_table(context, "robust", IdentityProp::FullRow).await;
    let (tx, rx) = mpsc::channel(100);
    table.register_table_notify(tx).await;
    (table, rx)
}

/// Two persisted commits (rows 1 and 2).
async fn write_two_commits(context: &TestContext) {
    let (mut table, mut rx) = open(context).await;
    for i in 1..=2 {
        let lsn = i as u64 * 10;
        table.append(test_row(i, "r", i)).unwrap();
        table.commit(lsn);
        flush_table_and_sync(&mut table, &mut rx, lsn)
            .await
            .unwrap();
        create_mooncake_and_persist_for_test(&mut table, &mut rx).await;
    }
}

fn metadata_dir(context: &TestContext) -> std::path::PathBuf {
    context
        .path()
        .join("default")
        .join("robust")
        .join("metadata")
}

fn metadata_versions(context: &TestContext) -> Vec<u32> {
    let mut v: Vec<u32> = std::fs::read_dir(metadata_dir(context))
        .unwrap()
        .flatten()
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.strip_prefix('v')?
                .strip_suffix(".metadata.json")?
                .parse()
                .ok()
        })
        .collect();
    v.sort();
    v
}

async fn assert_two_rows(context: &TestContext) {
    let (table, _rx) = open(context).await;
    assert_eq!(table.get_persistence_snapshot_lsn(), Some(20));
    let guard = table.snapshot.read().await;
    let rows: usize = guard
        .current_snapshot
        .disk_files
        .values()
        .map(|e| e.num_rows)
        .sum();
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn test_empty_version_hint_falls_back_to_latest_metadata() {
    let context = TestContext::new("robust");
    write_two_commits(&context).await;
    std::fs::write(metadata_dir(&context).join("version-hint.text"), b"").unwrap();
    assert_two_rows(&context).await;
}

/// A hint naming a metadata file that cannot be read (corruption, a power loss without directory
/// fsync, a non-atomic write by an older moonlink) fails the load instead of silently loading an
/// older version that may miss changes already acknowledged to the source.
#[tokio::test]
async fn test_hint_pointing_to_unreadable_metadata_fails_the_load() {
    let context = TestContext::new("robust");
    write_two_commits(&context).await;
    let current = *metadata_versions(&context).last().unwrap();
    let dir = metadata_dir(&context);
    std::fs::write(
        dir.join(format!("v{current}.metadata.json")),
        b"{\"format-v",
    )
    .unwrap();
    let data_files_before = data_file_names(&context);

    let iceberg_table_config = test_iceberg_table_config(&context, "robust");
    let mut table_config = test_mooncake_table_config(&context);
    table_config.row_identity = IdentityProp::FullRow;
    let res = MooncakeTable::new(
        (*create_test_arrow_schema()).clone(),
        "robust".to_string(),
        1,
        context.path(),
        iceberg_table_config.clone(),
        table_config,
        create_test_object_storage_cache(&context.temp_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await;
    let err = res
        .err()
        .expect("loading a corrupted current metadata file must fail");
    assert!(err.to_string().contains("cannot be loaded"), "{err}");
    // Nothing was garbage-collected on the way.
    assert_eq!(data_file_names(&context), data_files_before);
}

/// After a fallback load (unusable hint) the load-time sweep deletes nothing: the loaded version
/// may be older than the committed one, whose files are the only copy of its changes.
#[tokio::test]
async fn test_fallback_load_keeps_unreferenced_files() {
    let context = TestContext::new("robust");
    write_two_commits(&context).await;
    let data_dir = metadata_dir(&context).parent().unwrap().join("data");
    std::fs::write(data_dir.join("unreferenced.parquet"), b"x").unwrap();
    std::fs::write(metadata_dir(&context).join("version-hint.text"), b"").unwrap();
    assert_two_rows(&context).await;
    assert!(data_dir.join("unreferenced.parquet").exists());
}

fn data_file_names(context: &TestContext) -> Vec<String> {
    let data_dir = metadata_dir(context).parent().unwrap().join("data");
    let mut names: Vec<String> = std::fs::read_dir(data_dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    names.sort();
    names
}

/// No hint at all (e.g. a crash right after the first metadata file was written): the table is
/// still found and loaded, never re-created empty.
#[tokio::test]
async fn test_missing_hint_does_not_recreate_table() {
    let context = TestContext::new("robust");
    write_two_commits(&context).await;
    std::fs::remove_file(metadata_dir(&context).join("version-hint.text")).unwrap();
    assert_two_rows(&context).await;
}

/// A schema-only commit used to reuse the current metadata version number, overwriting the live
/// metadata file in place.
#[tokio::test]
async fn test_schema_change_does_not_overwrite_current_metadata() {
    let context = TestContext::new("robust");
    let (mut table, mut rx) = open(&context).await;
    table.append(test_row(1, "r", 1)).unwrap();
    table.commit(10);
    flush_table_and_sync(&mut table, &mut rx, 10).await.unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut rx).await;
    let before = metadata_versions(&context);
    alter_table_and_persist_to_iceberg(&mut table, &mut rx).await;
    // One more commit after the schema change: this is the one that reused the version number.
    table.force_empty_persistence_payload();
    create_mooncake_and_persist_for_test(&mut table, &mut rx).await;
    let after = metadata_versions(&context);
    // Three commits (data, schema, forced) write three new versions; none rewrites an existing
    // one. (Old versions are garbage collected, so compare the newest version number.)
    assert_eq!(
        after.last().unwrap(),
        &(before.last().unwrap() + 3),
        "before {before:?}, after {after:?}"
    );
}
