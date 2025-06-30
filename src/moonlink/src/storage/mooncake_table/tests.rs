use super::test_utils::*;
use super::*;
use crate::storage::iceberg::table_manager::MockTableManager;
use crate::storage::mooncake_table::state_test_utils::*;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use iceberg::{Error as IcebergError, ErrorKind};
use rstest::*;
use rstest_reuse::{self, *};
use tempfile::TempDir;
use tokio::sync::mpsc;

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
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    append_rows(&mut table, vec![test_row(1, "A", 20), test_row(2, "B", 21)])?;
    table.commit(1);
    snapshot(&mut table, &mut event_completion_rx).await;
    let mut snapshot = table.snapshot.write().await;
    let SnapshotReadOutput {
        data_file_paths, ..
    } = snapshot.request_read().await?;
    verify_file_contents(&data_file_paths[0].get_file_path(), &[1, 2], Some(2));
    Ok(())
}

#[apply(shared_cases)]
#[tokio::test]
async fn test_flush_basic(#[case] identity: IdentityProp) -> Result<()> {
    let context = TestContext::new("flush_basic");
    let mut table = test_table(&context, "flush_table", identity).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let rows = vec![test_row(1, "Alice", 30), test_row(2, "Bob", 25)];
    append_commit_flush_snapshot(&mut table, &mut event_completion_rx, rows, 1).await?;
    let mut snapshot = table.snapshot.write().await;
    let SnapshotReadOutput {
        data_file_paths, ..
    } = snapshot.request_read().await?;
    verify_file_contents(&data_file_paths[0].get_file_path(), &[1, 2], Some(2));
    Ok(())
}

#[apply(shared_cases)]
#[tokio::test]
async fn test_delete_and_append(#[case] identity: IdentityProp) -> Result<()> {
    let context = TestContext::new("delete_append");
    let mut table = test_table(&context, "del_table", identity).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let initial_rows = vec![
        test_row(1, "Row 1", 31),
        test_row(2, "Row 2", 32),
        test_row(3, "Row 3", 33),
    ];
    append_commit_flush_snapshot(&mut table, &mut event_completion_rx, initial_rows, 1).await?;

    table.delete(test_row(2, "Row 2", 32), 1).await;
    table.commit(2);
    snapshot(&mut table, &mut event_completion_rx).await;

    append_rows(&mut table, vec![test_row(4, "Row 4", 34)])?;
    table.commit(3);
    snapshot(&mut table, &mut event_completion_rx).await;

    let mut snapshot = table.snapshot.write().await;
    let SnapshotReadOutput {
        data_file_paths,
        puffin_cache_handles,
        position_deletes,
        deletion_vectors,
        ..
    } = snapshot.request_read().await?;
    verify_files_and_deletions(
        get_data_files_for_read(&data_file_paths).as_slice(),
        get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
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
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    append_rows(&mut table, batch_rows(1, 4))?;
    table.commit(1);
    snapshot(&mut table, &mut event_completion_rx).await;

    table.delete(test_row(2, "Row 2", 32), 1).await;
    table.delete(test_row(4, "Row 4", 34), 1).await;
    table.commit(2);
    snapshot(&mut table, &mut event_completion_rx).await;

    let mut snapshot = table.snapshot.write().await;
    let SnapshotReadOutput {
        data_file_paths, ..
    } = snapshot.request_read().await?;
    verify_file_contents(&data_file_paths[0].get_file_path(), &[1, 3], None);
    Ok(())
}

#[apply(shared_cases)]
#[tokio::test]
async fn test_deletion_after_flush(#[case] identity: IdentityProp) -> Result<()> {
    let context = TestContext::new("delete_post_flush");
    let mut table = test_table(&context, "table", identity).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;
    append_commit_flush_snapshot(&mut table, &mut event_completion_rx, batch_rows(1, 4), 1).await?;

    table.delete(test_row(2, "Row 2", 32), 2).await;
    table.delete(test_row(4, "Row 4", 34), 2).await;
    table.commit(3);
    snapshot(&mut table, &mut event_completion_rx).await;

    let mut snapshot = table.snapshot.write().await;
    let SnapshotReadOutput {
        data_file_paths,
        position_deletes,
        ..
    } = snapshot.request_read().await?;
    assert_eq!(data_file_paths.len(), 1);
    let mut ids = read_ids_from_parquet(&data_file_paths[0].get_file_path());

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
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Perform and check initial append operation.
    table.append(row1.clone())?;
    table.append(row2.clone())?;
    table.append(row3.clone())?;
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 100).await?;
    create_mooncake_and_persist_for_test(&mut table, &mut event_completion_rx).await;
    {
        let mut table_snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            position_deletes,
            deletion_vectors,
            ..
        } = table_snapshot.request_read().await?;
        verify_files_and_deletions(
            get_data_files_for_read(&data_file_paths).as_slice(),
            get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
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
    create_mooncake_and_persist_for_test(&mut table, &mut event_completion_rx).await;
    {
        let mut table_snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            position_deletes,
            deletion_vectors,
            ..
        } = table_snapshot.request_read().await?;
        verify_files_and_deletions(
            get_data_files_for_read(&data_file_paths).as_slice(),
            get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
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
        config: MooncakeTableConfig::default(), // No temp files generated.
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
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

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
    snapshot(&mut table, &mut event_completion_rx).await;

    // Delete one duplicate before flush (row1)
    table.delete(row1.clone(), 1).await;
    table.commit(2);
    snapshot(&mut table, &mut event_completion_rx).await;

    // Verify that row1 is deleted, but row2 (same id) remains
    {
        let mut table_snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            position_deletes,
            deletion_vectors,
            ..
        } = table_snapshot.request_read().await?;
        verify_files_and_deletions(
            get_data_files_for_read(&data_file_paths).as_slice(),
            get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
            position_deletes,
            deletion_vectors,
            &[1, 2, 2, 3, 3],
        )
        .await;
    }

    // Flush the table
    table.flush(3).await?;
    snapshot(&mut table, &mut event_completion_rx).await;

    // Delete one duplicate during flush (row3)
    table.delete(row3.clone(), 3).await;
    table.commit(4);
    snapshot(&mut table, &mut event_completion_rx).await;

    // Verify that row3 is deleted, but row4 (same id) remains
    {
        let mut table_snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            position_deletes,
            deletion_vectors,
            ..
        } = table_snapshot.request_read().await?;
        verify_files_and_deletions(
            get_data_files_for_read(&data_file_paths).as_slice(),
            get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
            position_deletes,
            deletion_vectors,
            &[1, 2, 3, 3],
        )
        .await;
    }

    // Delete one duplicate after flush (row5)
    table.delete(row5.clone(), 4).await;
    table.commit(5);
    snapshot(&mut table, &mut event_completion_rx).await;

    {
        let mut table_snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            position_deletes,
            deletion_vectors,
            ..
        } = table_snapshot.request_read().await?;
        verify_files_and_deletions(
            get_data_files_for_read(&data_file_paths).as_slice(),
            get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
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
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let old_row = test_row(1, "John", 30);
    table.append(old_row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 100).await.unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut event_completion_rx).await;

    // Update operation.
    let new_row = old_row.clone();
    table.delete(/*row=*/ old_row.clone(), /*lsn=*/ 100).await;
    table.append(new_row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 200).await.unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut event_completion_rx).await;

    {
        let mut table_snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            position_deletes,
            deletion_vectors,
            ..
        } = table_snapshot.request_read().await?;
        verify_files_and_deletions(
            get_data_files_for_read(&data_file_paths).as_slice(),
            get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
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
async fn test_snapshot_load_failure() {
    let temp_dir = TempDir::new().unwrap();
    let table_metadata = Arc::new(TableMetadata {
        name: "test_table".to_string(),
        id: 1,
        schema: Arc::new(test_schema()),
        config: MooncakeTableConfig::default(), // No temp files generated.
        path: PathBuf::from(temp_dir.path()),
        identity: IdentityProp::Keys(vec![0]),
    });
    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_load_snapshot_from_table()
        .times(1)
        .returning(|| {
            Box::pin(async move {
                Err(IcebergError::new(
                    ErrorKind::Unexpected,
                    "Intended error for unit test",
                ))
            })
        });

    let table = MooncakeTable::new_with_table_manager(
        table_metadata,
        Box::new(mock_table_manager),
        ObjectStorageCache::default_for_test(&temp_dir),
    )
    .await;
    assert!(table.is_err());
}

#[tokio::test]
async fn test_snapshot_store_failure() {
    let temp_dir = TempDir::new().unwrap();
    let table_metadata = Arc::new(TableMetadata {
        name: "test_table".to_string(),
        id: 1,
        schema: Arc::new(test_schema()),
        config: MooncakeTableConfig::default(), // No temp files generated.
        path: PathBuf::from(temp_dir.path()),
        identity: IdentityProp::Keys(vec![0]),
    });
    let table_metadata_copy = table_metadata.clone();

    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_load_snapshot_from_table()
        .times(1)
        .returning(move || {
            let table_metadata_copy = table_metadata_copy.clone();
            Box::pin(async move {
                Ok((
                    /*next_file_id=*/ 0,
                    MooncakeSnapshot::new(table_metadata_copy),
                ))
            })
        });
    mock_table_manager
        .expect_sync_snapshot()
        .times(1)
        .returning(|_, _| {
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
        ObjectStorageCache::default_for_test(&temp_dir),
    )
    .await
    .unwrap();
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let row = test_row(1, "A", 20);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 100).await.unwrap();

    let (_, iceberg_snapshot_payload, _, _, evicted_data_files_cache) =
        snapshot(&mut table, &mut event_completion_rx).await;
    for cur_file in evicted_data_files_cache.into_iter() {
        tokio::fs::remove_file(&cur_file).await.unwrap();
    }

    let iceberg_snapshot_result = create_iceberg_snapshot(
        &mut table,
        iceberg_snapshot_payload,
        &mut event_completion_rx,
    )
    .await;
    assert!(iceberg_snapshot_result.is_err());
}
