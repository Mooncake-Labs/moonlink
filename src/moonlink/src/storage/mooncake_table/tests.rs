use super::test_utils::*;
use super::*;
use crate::storage::iceberg::iceberg_table_manager::MockTableManager;
use crate::storage::mooncake_table::snapshot::ReadOutput;
use crate::storage::mooncake_table::TableConfig as MooncakeTableConfig;
use iceberg::{Error as IcebergError, ErrorKind};
use rstest::*;
use rstest_reuse::{self, *};
use tempfile::TempDir;

#[template]
#[rstest]
#[case(IdentityProp::Keys(vec![0]))]
#[case(IdentityProp::FullRow)]
#[case(IdentityProp::SinglePrimitiveKey(0))]
fn shared_cases(#[case] identity: IdentityProp) {}

#[apply(shared_cases)]
#[tokio::test]
async fn test_append_commit_snapshot(#[case] identity: IdentityProp) -> Result<()> {
    let context = TestContext::new("append_commit");
    let mut table = test_table(&context, "append_table", identity).await;
    append_rows(&mut table, vec![test_row(1, "A", 20), test_row(2, "B", 21)])?;
    table.commit(1);
    snapshot(&mut table).await;
    let snapshot = table.snapshot.read().await;
    let ReadOutput {
        data_file_paths, ..
    } = snapshot.request_read().await?;
    verify_file_contents(&data_file_paths[0], &[1, 2], Some(2));
    Ok(())
}

#[apply(shared_cases)]
#[tokio::test]
async fn test_flush_basic(#[case] identity: IdentityProp) -> Result<()> {
    let context = TestContext::new("flush_basic");
    let mut table = test_table(&context, "flush_table", identity).await;
    let rows = vec![test_row(1, "Alice", 30), test_row(2, "Bob", 25)];
    append_commit_flush_snapshot(&mut table, rows, 1).await?;
    let snapshot = table.snapshot.read().await;
    let ReadOutput {
        data_file_paths, ..
    } = snapshot.request_read().await?;
    verify_file_contents(&data_file_paths[0], &[1, 2], Some(2));
    Ok(())
}

#[apply(shared_cases)]
#[tokio::test]
async fn test_delete_and_append(#[case] identity: IdentityProp) -> Result<()> {
    let context = TestContext::new("delete_append");
    let mut table = test_table(&context, "del_table", identity).await;
    let initial_rows = vec![
        test_row(1, "Row 1", 31),
        test_row(2, "Row 2", 32),
        test_row(3, "Row 3", 33),
    ];
    append_commit_flush_snapshot(&mut table, initial_rows, 1).await?;

    table.delete(test_row(2, "Row 2", 32), 2).await;
    table.commit(2);
    snapshot(&mut table).await;

    append_rows(&mut table, vec![test_row(4, "Row 4", 34)])?;
    table.commit(3);
    snapshot(&mut table).await;

    let snapshot = table.snapshot.read().await;
    let ReadOutput {
        data_file_paths,
        puffin_file_paths,
        position_deletes,
        deletion_vectors,
        ..
    } = snapshot.request_read().await?;
    verify_files_and_deletions(
        &data_file_paths,
        &puffin_file_paths,
        position_deletes,
        deletion_vectors,
        &[1, 3, 4],
    )
    .await;
    Ok(())
}

#[apply(shared_cases)]
#[tokio::test]
async fn test_deletion_before_flush(#[case] identity: IdentityProp) -> Result<()> {
    let context = TestContext::new("delete_pre_flush");
    let mut table = test_table(&context, "table", identity).await;
    append_rows(&mut table, batch_rows(1, 4))?;
    table.commit(1);
    snapshot(&mut table).await;

    table.delete(test_row(2, "Row 2", 32), 2).await;
    table.delete(test_row(4, "Row 4", 34), 2).await;
    table.commit(2);
    snapshot(&mut table).await;

    let snapshot = table.snapshot.read().await;
    let ReadOutput {
        data_file_paths, ..
    } = snapshot.request_read().await?;
    verify_file_contents(&data_file_paths[0], &[1, 3], None);
    Ok(())
}

#[apply(shared_cases)]
#[tokio::test]
async fn test_deletion_after_flush(#[case] identity: IdentityProp) -> Result<()> {
    let context = TestContext::new("delete_post_flush");
    let mut table = test_table(&context, "table", identity).await;
    append_commit_flush_snapshot(&mut table, batch_rows(1, 4), 1).await?;

    table.delete(test_row(2, "Row 2", 32), 2).await;
    table.delete(test_row(4, "Row 4", 34), 2).await;
    table.commit(2);
    snapshot(&mut table).await;

    let snapshot = table.snapshot.read().await;
    let ReadOutput {
        data_file_paths,
        position_deletes,
        ..
    } = snapshot.request_read().await?;
    assert_eq!(data_file_paths.len(), 1);
    let mut ids = read_ids_from_parquet(&data_file_paths[0]);

    for deletion in position_deletes {
        ids[deletion.1 as usize] = None;
    }
    let ids = ids.into_iter().flatten().collect::<Vec<_>>();

    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
    assert!(!ids.contains(&2));
    assert!(!ids.contains(&4));
    Ok(())
}

#[apply(shared_cases)]
#[tokio::test]
async fn test_update_rows(#[case] identity: IdentityProp) -> Result<()> {
    let row1 = test_row(1, "Row 1", 31);
    let row2 = test_row(2, "Row 2", 32);
    let row3 = test_row(3, "Row 3", 33);
    let row4 = test_row(4, "Row 4", 44);
    let updated_row2 = test_row(2, "New row 2", 30);

    let context = TestContext::new("update_rows");
    let mut table = test_table(&context, "update_table", identity).await;

    // Perform and check initial append operation.
    table.append(row1.clone())?;
    table.append(row2.clone())?;
    table.append(row3.clone())?;
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 100).await?;
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await?;
    {
        let table_snapshot = table.snapshot.read().await;
        let ReadOutput {
            data_file_paths,
            puffin_file_paths,
            position_deletes,
            deletion_vectors,
            ..
        } = table_snapshot.request_read().await?;
        verify_files_and_deletions(
            &data_file_paths,
            &puffin_file_paths,
            position_deletes,
            deletion_vectors,
            /*expected_ids=*/ &[1, 2, 3],
        )
        .await;
    }

    // Perform an update operation.
    table.delete(row2.clone(), /*lsn=*/ 200).await;
    table.append(updated_row2.clone())?;
    table.append(row4.clone())?;
    table.commit(/*lsn=*/ 300);
    table.flush(/*lsn=*/ 300).await?;

    // Check update result.
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await?;
    {
        let table_snapshot = table.snapshot.read().await;
        let ReadOutput {
            data_file_paths,
            puffin_file_paths,
            position_deletes,
            deletion_vectors,
            ..
        } = table_snapshot.request_read().await?;
        verify_files_and_deletions(
            &data_file_paths,
            &puffin_file_paths,
            position_deletes,
            deletion_vectors,
            /*expected_ids=*/ &[1, 2, 3, 4],
        )
        .await;
    }

    Ok(())
}

#[tokio::test]
async fn test_snapshot_initialization() -> Result<()> {
    let schema = test_schema();
    let identity = IdentityProp::Keys(vec![0]);
    let metadata = Arc::new(TableMetadata {
        name: "test_table".to_string(),
        id: 1,
        schema: Arc::new(schema),
        config: TableConfig::default(), // No temp files generated.
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
        IdentityProp::FullRow,
    )
    .await;

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
    table.delete(row1.clone(), 2).await;
    table.commit(2);
    snapshot(&mut table).await;

    // Verify that row1 is deleted, but row2 (same id) remains
    {
        let table_snapshot = table.snapshot.read().await;
        let ReadOutput {
            data_file_paths,
            puffin_file_paths,
            position_deletes,
            deletion_vectors,
            ..
        } = table_snapshot.request_read().await?;
        verify_files_and_deletions(
            &data_file_paths,
            &puffin_file_paths,
            position_deletes,
            deletion_vectors,
            &[1, 2, 2, 3, 3],
        )
        .await;
    }

    // Flush the table
    table.flush(3).await?;
    snapshot(&mut table).await;

    // Delete one duplicate during flush (row3)
    table.delete(row3.clone(), 4).await;
    table.commit(4);
    snapshot(&mut table).await;

    // Verify that row3 is deleted, but row4 (same id) remains
    {
        let table_snapshot = table.snapshot.read().await;
        let ReadOutput {
            data_file_paths,
            puffin_file_paths,
            position_deletes,
            deletion_vectors,
            ..
        } = table_snapshot.request_read().await?;
        verify_files_and_deletions(
            &data_file_paths,
            &puffin_file_paths,
            position_deletes,
            deletion_vectors,
            &[1, 2, 3, 3],
        )
        .await;
    }

    // Delete one duplicate after flush (row5)
    table.delete(row5.clone(), 5).await;
    table.commit(5);
    snapshot(&mut table).await;

    {
        let table_snapshot = table.snapshot.read().await;
        let ReadOutput {
            data_file_paths,
            puffin_file_paths,
            position_deletes,
            deletion_vectors,
            ..
        } = table_snapshot.request_read().await?;
        verify_files_and_deletions(
            &data_file_paths,
            &puffin_file_paths,
            position_deletes,
            deletion_vectors,
            &[1, 2, 3],
        )
        .await;
    }

    Ok(())
}

#[tokio::test]
async fn test_duplicate_deletion() -> Result<()> {
    // Create iceberg snapshot whenever `create_snapshot` is called.
    let context = TestContext::new("duplicate_deletion");
    let mut table = test_table(&context, "duplicate_deletion", IdentityProp::Keys(vec![0])).await;

    let old_row = test_row(1, "John", 30);
    table.append(old_row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 100).await.unwrap();
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await?;

    // Update operation.
    let new_row = old_row.clone();
    table.delete(/*row=*/ old_row.clone(), /*lsn=*/ 200).await;
    table.append(new_row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 200).await.unwrap();
    table
        .create_mooncake_and_iceberg_snapshot_for_test()
        .await?;

    {
        let table_snapshot = table.snapshot.read().await;
        let ReadOutput {
            data_file_paths,
            puffin_file_paths,
            position_deletes,
            deletion_vectors,
            ..
        } = table_snapshot.request_read().await?;
        verify_files_and_deletions(
            &data_file_paths,
            &puffin_file_paths,
            position_deletes,
            deletion_vectors,
            &[1],
        )
        .await;
    }
    Ok(())
}

/// ---- Mock unit test ----
#[tokio::test]
async fn test_snapshot_store_failure() {
    let temp_dir = TempDir::new().unwrap();
    let table_metadata = Arc::new(TableMetadata {
        name: "test_table".to_string(),
        id: 1,
        schema: Arc::new(test_schema()),
        config: TableConfig::default(), // No temp files generated.
        path: PathBuf::from(temp_dir.path()),
        identity: IdentityProp::Keys(vec![0]),
    });

    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_sync_snapshot()
        .times(1)
        .returning(|_| {
            Box::pin(async move {
                Err(IcebergError::new(
                    ErrorKind::Unexpected,
                    "Intended error for unit test",
                ))
            })
        });

    let mut table = MooncakeTable::new_with_table_manager(
        table_metadata,
        Box::new(mock_table_manager),
        MooncakeTableConfig::default(), // No temp files generated.
    )
    .await
    .unwrap();

    let row = test_row(1, "A", 20);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 100).await.unwrap();
    let mooncake_snapshot_handle = table.create_snapshot().unwrap();
    let (_, iceberg_snapshot_payload) = mooncake_snapshot_handle.await.unwrap();
    let iceberg_snapshot_handle = table.persist_iceberg_snapshot(iceberg_snapshot_payload.unwrap());
    assert!(iceberg_snapshot_handle.await.unwrap().is_err());
}
