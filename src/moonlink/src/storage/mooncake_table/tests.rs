use super::test_utils::*;
#[cfg(test)]
use super::*;

use rstest::*;
use rstest_reuse::{self, *};

#[template]
#[rstest]
#[case(Identity::Keys(vec![0]))]
#[case(Identity::FullRow)]
#[case(Identity::SinglePrimitiveKey(0))]
fn shared_cases(#[case] identity: Identity) {}

#[apply(shared_cases)]
#[tokio::test]
async fn test_append_commit_snapshot(#[case] identity: Identity) -> Result<()> {
    let context = TestContext::new("append_commit");
    let mut table = test_table(&context, "append_table", identity);
    append_rows(&mut table, vec![test_row(1, "A", 20), test_row(2, "B", 21)])?;
    table.commit(1);
    snapshot(&mut table).await;
    let snapshot = table.snapshot.read().await;
    let ReadOutput { file_paths, .. } = snapshot.request_read()?;
    verify_file_contents(&file_paths[0], &[1, 2], Some(2));
    Ok(())
}

#[apply(shared_cases)]
#[tokio::test]
async fn test_flush_basic(#[case] identity: Identity) -> Result<()> {
    let context = TestContext::new("flush_basic");
    let mut table = test_table(&context, "flush_table", identity);
    let rows = vec![test_row(1, "Alice", 30), test_row(2, "Bob", 25)];
    append_commit_flush_snapshot(&mut table, rows, 1).await?;
    let snapshot = table.snapshot.read().await;
    let ReadOutput { file_paths, .. } = snapshot.request_read()?;
    verify_file_contents(&file_paths[0], &[1, 2], Some(2));
    Ok(())
}

#[apply(shared_cases)]
#[tokio::test]
async fn test_delete_and_append(#[case] identity: Identity) -> Result<()> {
    let context = TestContext::new("delete_append");
    let mut table = test_table(&context, "del_table", identity);
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

    let snapshot = table.snapshot.read().await;
    let ReadOutput {
        file_paths,
        deletions,
        ..
    } = snapshot.request_read()?;
    verify_files_and_deletions(&file_paths, deletions.as_slice(), &[1, 3, 4]);
    Ok(())
}

#[apply(shared_cases)]
#[tokio::test]
async fn test_deletion_before_flush(#[case] identity: Identity) -> Result<()> {
    let context = TestContext::new("delete_pre_flush");
    let mut table = test_table(&context, "table", identity);
    append_rows(&mut table, batch_rows(1, 4))?;
    table.commit(1);
    snapshot(&mut table).await;

    table.delete(test_row(2, "Row 2", 32), 2);
    table.delete(test_row(4, "Row 4", 34), 2);
    table.commit(2);
    snapshot(&mut table).await;

    let snapshot = table.snapshot.read().await;
    let ReadOutput { file_paths, .. } = snapshot.request_read()?;
    verify_file_contents(&file_paths[0], &[1, 3], None);
    Ok(())
}

#[apply(shared_cases)]
#[tokio::test]
async fn test_deletion_after_flush(#[case] identity: Identity) -> Result<()> {
    let context = TestContext::new("delete_post_flush");
    let mut table = test_table(&context, "table", identity);
    append_commit_flush_snapshot(&mut table, batch_rows(1, 4), 1).await?;

    table.delete(test_row(2, "Row 2", 32), 2);
    table.delete(test_row(4, "Row 4", 34), 2);
    table.commit(2);
    snapshot(&mut table).await;

    let snapshot = table.snapshot.read().await;
    let ReadOutput {
        file_paths,
        deletions,
        ..
    } = snapshot.request_read()?;
    assert_eq!(file_paths.len(), 1);
    let mut ids = read_ids_from_parquet(&file_paths[0]);

    for deletion in deletions {
        ids[deletion.1 as usize] = None;
    }
    let ids = ids.into_iter().flatten().collect::<Vec<_>>();

    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
    assert!(!ids.contains(&2));
    assert!(!ids.contains(&4));
    Ok(())
}

#[tokio::test]
async fn test_snapshot_initialization() -> Result<()> {
    let schema = test_schema();
    let identity = Identity::Keys(vec![0]);
    let metadata = Arc::new(TableMetadata {
        name: "test_table".to_string(),
        id: 1,
        schema: Arc::new(schema),
        config: TableConfig::new(),
        path: PathBuf::new(),
        identity,
    });
    let snapshot = Snapshot::new(metadata);
    assert_eq!(snapshot.snapshot_version, 0);
    assert!(snapshot.disk_files.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_full_row_with_duplication_and_identical() -> Result<()> {
    let context = TestContext::new("full_row_with_duplication_and_identical");
    let mut table = test_table(
        &context,
        "full_row_with_duplication_and_identical",
        Identity::FullRow,
    );

    // Insert duplicate rows (same identity, different values)
    let row1 = test_row(1, "A", 20);
    let row2 = test_row(1, "B", 21); // same id, different name
    let row3 = test_row(2, "C", 22);
    let row4 = test_row(2, "D", 23); // same id, different name

    // Insert identical rows (same identity and values)
    let row5 = test_row(3, "E", 24);
    let row6 = test_row(3, "E", 24); // identical to row5

    append_rows(
        &mut table,
        vec![
            row1.clone(),
            row2.clone(),
            row3.clone(),
            row4.clone(),
            row5.clone(),
            row6.clone(),
        ],
    )?;
    table.commit(1);
    snapshot(&mut table).await;

    // Delete one duplicate before flush (row1)
    table.delete(row1.clone(), 2);
    table.commit(2);
    snapshot(&mut table).await;

    // Verify that row1 is deleted, but row2 (same id) remains
    {
        let table_snapshot = table.snapshot.read().await;
        let ReadOutput {
            file_paths,
            deletions,
            ..
        } = table_snapshot.request_read()?;
        verify_files_and_deletions(&file_paths, &deletions, &[1, 2, 2, 3, 3]);
    }

    // Flush the table
    table.flush(3).await?;
    snapshot(&mut table).await;

    // Delete one duplicate during flush (row3)
    table.delete(row3.clone(), 4);
    table.commit(4);
    snapshot(&mut table).await;

    // Verify that row3 is deleted, but row4 (same id) remains
    {
        let table_snapshot = table.snapshot.read().await;
        let ReadOutput {
            file_paths,
            deletions,
            ..
        } = table_snapshot.request_read()?;
        verify_files_and_deletions(&file_paths, deletions.as_slice(), &[1, 2, 3, 3]);
    }

    // Delete one duplicate after flush (row5)
    table.delete(row5.clone(), 5);
    table.commit(5);
    snapshot(&mut table).await;

    {
        let table_snapshot = table.snapshot.read().await;
        let ReadOutput {
            file_paths,
            deletions,
            ..
        } = table_snapshot.request_read()?;
        verify_files_and_deletions(&file_paths, deletions.as_slice(), &[1, 2, 3]);
    }

    Ok(())
}
