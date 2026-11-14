//! Iceberg metadata and superseded files do not accumulate.
use super::test_utils::*;
use super::*;
use crate::row::IdentityProp;
use crate::storage::mooncake_table::table_operation_test_utils::*;
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
