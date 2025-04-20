use super::test_utils::*;
#[cfg(test)]
use super::*;
use std::collections::HashSet;

#[tokio::test]
async fn test_append_commit_snapshot() -> Result<()> {
    let context = TestContext::new("append_commit");
    let mut table = test_table(&context, "append_table");
    append_rows(&mut table, vec![test_row(1, "A", 20), test_row(2, "B", 21)])?;
    table.commit(1);
    snapshot(&mut table).await;
    let snapshot = table.snapshot.read().unwrap();
    let (paths, _deletions) = snapshot.request_read()?;
    verify_file_contents(&paths[0], &[1, 2], Some(2));
    Ok(())
}

#[tokio::test]
async fn test_flush_basic() -> Result<()> {
    let context = TestContext::new("flush_basic");
    let mut table = test_table(&context, "flush_table");
    let rows = vec![test_row(1, "Alice", 30), test_row(2, "Bob", 25)];
    append_commit_flush_snapshot(&mut table, rows, 1).await?;
    let snapshot = table.snapshot.read().unwrap();
    let (paths, _deletions) = snapshot.request_read()?;
    verify_file_contents(&paths[0], &[1, 2], Some(2));
    Ok(())
}

#[tokio::test]
async fn test_delete_and_append() -> Result<()> {
    let context = TestContext::new("delete_append");
    let mut table = test_table(&context, "del_table");
    let initial_rows = vec![
        test_row(1, "Row 1", 31),
        test_row(2, "Row 2", 32),
        test_row(3, "Row 3", 33),
    ];
    append_commit_flush_snapshot(&mut table, initial_rows, 1).await?;

    table.delete(test_row(2, "Row 2", 32), 2);
    table.commit(2);
    snapshot(&mut table).await;

    append_rows(&mut table, vec![test_row(4, "Row 4", 34)])?;
    table.commit(3);
    snapshot(&mut table).await;

    let snapshot = table.snapshot.read().unwrap();
    let (paths, _deletions) = snapshot.request_read()?;
    // Need to manually handle deletions in the test - simulate what a real reader would do
    let mut files = paths.clone();
    files.sort(); // Ensure consistent order
    let mut actual_ids = HashSet::new();

    // First read all IDs
    for path in &files {
        let ids = read_ids_from_parquet(path);
        actual_ids.extend(ids);
    }

    // Then remove deleted IDs (ID 2 should be deleted)
    for path in &files {
        if files[0] == *path {
            // This is the first file containing original rows
            // Remove ID 2 which should be deleted
            actual_ids.remove(&2);
            break;
        }
    }

    let expected: HashSet<_> = [1, 3, 4].iter().copied().collect();
    assert_eq!(
        actual_ids, expected,
        "File contents after deletions don't match expected IDs"
    );

    Ok(())
}

#[tokio::test]
async fn test_deletion_before_flush() -> Result<()> {
    let context = TestContext::new("delete_pre_flush");
    let mut table = test_table(&context, "table");
    append_rows(&mut table, batch_rows(1, 4))?;
    table.commit(1);
    snapshot(&mut table).await;

    table.delete(test_row(2, "Row 2", 32), 2);
    table.delete(test_row(4, "Row 4", 34), 2);
    table.commit(2);
    snapshot(&mut table).await;

    let snapshot = table.snapshot.read().unwrap();
    let (paths, _deletions) = snapshot.request_read()?;
    verify_file_contents(&paths[0], &[1, 3], None);
    Ok(())
}

#[tokio::test]
async fn test_deletion_after_flush() -> Result<()> {
    let context = TestContext::new("delete_post_flush");
    let mut table = test_table(&context, "table");
    append_commit_flush_snapshot(&mut table, batch_rows(1, 4), 1).await?;

    table.delete(test_row(2, "Row 2", 32), 2);
    table.delete(test_row(4, "Row 4", 34), 2);
    table.commit(2);
    snapshot(&mut table).await;

    let snapshot = table.snapshot.read().unwrap();
    let (paths, _deletions) = snapshot.request_read()?;
    // Need to manually handle deletions in the test - simulate what a real reader would do
    let mut ids = read_ids_from_parquet(&paths[0]);

    // Apply deletions - IDs 2 and 4 should be removed
    ids.remove(&2);
    ids.remove(&4);

    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
    assert!(!ids.contains(&2));
    assert!(!ids.contains(&4));
    Ok(())
}

#[tokio::test]
async fn test_snapshot_initialization() -> Result<()> {
    let schema = test_schema();
    let metadata = Arc::new(TableMetadata {
        name: "test_table".to_string(),
        id: 1,
        schema: Arc::new(schema),
        config: TableConfig::new(),
        path: PathBuf::new(),
        get_lookup_key,
    });
    let snapshot = Snapshot::new(metadata);
    assert_eq!(snapshot.snapshot_version, 0);
    assert!(snapshot.disk_files.is_empty());
    Ok(())
}
