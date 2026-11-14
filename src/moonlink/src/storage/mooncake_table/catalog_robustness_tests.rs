//! The file catalog survives interrupted writes: a lost, empty or dangling version hint falls
//! back to the newest readable metadata file, and existing tables are never re-created empty.
use super::test_utils::*;
use super::*;
use crate::row::IdentityProp;
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

#[tokio::test]
async fn test_hint_pointing_to_truncated_metadata_falls_back() {
    let context = TestContext::new("robust");
    write_two_commits(&context).await;
    let next = metadata_versions(&context).last().unwrap() + 1;
    let dir = metadata_dir(&context);
    std::fs::write(dir.join(format!("v{next}.metadata.json")), b"{\"format-v").unwrap();
    std::fs::write(dir.join("version-hint.text"), next.to_string()).unwrap();
    assert_two_rows(&context).await;

    // The next commit replaces the broken file and the table keeps working.
    let (mut table, mut rx) = open(&context).await;
    table.append(test_row(3, "r", 3)).unwrap();
    table.commit(30);
    flush_table_and_sync(&mut table, &mut rx, 30).await.unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut rx).await;
    drop(table);
    let (table, _rx) = open(&context).await;
    assert_eq!(table.get_persistence_snapshot_lsn(), Some(30));
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
    // Every commit (data + schema) adds new versions; none is rewritten.
    assert!(
        after.len() >= before.len() + 3,
        "before {before:?}, after {after:?}"
    );
    let distinct: std::collections::HashSet<_> = after.iter().collect();
    assert_eq!(distinct.len(), after.len());
    assert_eq!(after, (0..after.len() as u32).collect::<Vec<_>>());
}
