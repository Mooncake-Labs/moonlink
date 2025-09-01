use super::test_utils::*;
use super::*;
use crate::storage::iceberg::base_iceberg_snapshot_fetcher::BaseIcebergSnapshotFetcher;
use crate::storage::iceberg::iceberg_snapshot_fetcher::IcebergSnapshotFetcher;
use crate::storage::iceberg::table_manager::MockTableManager;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use crate::storage::mooncake_table::test_utils::{append_rows, test_row, test_table, TestContext};
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::snapshot_options::IcebergSnapshotOption;
use crate::storage::snapshot_options::MaintenanceOption;
use crate::storage::snapshot_options::SnapshotOption;
use crate::storage::wal::test_utils::WAL_TEST_TABLE_ID;
use crate::table_handler::table_handler_state::MaintenanceProcessStatus;
use crate::table_handler::table_handler_state::TableHandlerState;
use crate::Error;
use crate::FileSystemAccessor;
use crate::WalConfig;
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
async fn test_append_commit_create_mooncake_snapshot_for_test(
    #[case] identity: IdentityProp,
) -> Result<()> {
    let context = TestContext::new("append_commit");
    let mut table = test_table(&context, "append_table", identity).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    append_rows(&mut table, vec![test_row(1, "A", 20), test_row(2, "B", 21)])?;
    table.commit(1);
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;
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
    append_commit_flush_create_mooncake_snapshot_for_test(
        &mut table,
        &mut event_completion_rx,
        rows,
        1,
    )
    .await?;
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
    append_commit_flush_create_mooncake_snapshot_for_test(
        &mut table,
        &mut event_completion_rx,
        initial_rows,
        1,
    )
    .await?;

    table.delete(test_row(2, "Row 2", 32), 1).await;
    table.commit(2);
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

    append_rows(&mut table, vec![test_row(4, "Row 4", 34)])?;
    table.commit(3);
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

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
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

    table.delete(test_row(2, "Row 2", 32), 1).await;
    table.delete(test_row(4, "Row 4", 34), 1).await;
    table.commit(2);
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

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
    append_commit_flush_create_mooncake_snapshot_for_test(
        &mut table,
        &mut event_completion_rx,
        batch_rows(1, 4),
        1,
    )
    .await?;

    table.delete(test_row(2, "Row 2", 32), 2).await;
    table.delete(test_row(4, "Row 4", 34), 2).await;
    table.commit(3);
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

    let mut snapshot = table.snapshot.write().await;
    let SnapshotReadOutput {
        data_file_paths,
        position_deletes,
        ..
    } = snapshot.request_read().await?;
    assert_eq!(data_file_paths.len(), 1);
    let mut ids = read_ids_from_parquet(&data_file_paths[0].get_file_path());

    for deletion in position_deletes {
        ids[deletion.data_file_row_number as usize] = None;
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
    flush_table_and_sync(&mut table, &mut event_completion_rx, /*lsn=*/ 100).await?;
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
    flush_table_and_sync(&mut table, &mut event_completion_rx, /*lsn=*/ 300).await?;

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
    let schema = create_test_arrow_schema();
    let metadata = Arc::new(TableMetadata {
        name: "test_table".to_string(),
        table_id: 1,
        schema,
        config: MooncakeTableConfig::default(), // No temp files generated.
        path: PathBuf::new(),
    });
    let snapshot = Snapshot::new(metadata);
    assert_eq!(snapshot.snapshot_version, 0);
    assert!(snapshot.disk_files.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_force_snapshot_without_new_commits() {
    let row = test_row(1, "Row 1", 31);

    let context = TestContext::new("force_snapshot_without_new_commits");
    let mut table = test_table(
        &context,
        "force_snapshot_without_new_commits",
        IdentityProp::FullRow,
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Perform and check initial append operation.
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    flush_table_and_sync(&mut table, &mut event_completion_rx, /*lsn=*/ 100)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut event_completion_rx).await;

    // Now there're no new commits, create a force snapshot again.
    //
    // Force snapshot is possible to flush with latest commit LSN if table at clean state.
    flush_table_and_sync(&mut table, &mut event_completion_rx, /*lsn=*/ 100)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut event_completion_rx).await;
    {
        let mut table_snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            position_deletes,
            deletion_vectors,
            ..
        } = table_snapshot.request_read().await.unwrap();
        verify_files_and_deletions(
            get_data_files_for_read(&data_file_paths).as_slice(),
            get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
            position_deletes,
            deletion_vectors,
            /*expected_ids=*/ &[1],
        )
        .await;
    }
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
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

    // Delete one duplicate before flush (row1)
    table.delete(row1.clone(), 1).await;
    table.commit(2);
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

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
    flush_table_and_sync(&mut table, &mut event_completion_rx, 3).await?;
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

    // Delete one duplicate during flush (row3)
    table.delete(row3.clone(), 3).await;
    table.commit(4);
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

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
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

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
    flush_table_and_sync(&mut table, &mut event_completion_rx, /*lsn=*/ 100)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut event_completion_rx).await;

    // Update operation.
    let new_row = old_row.clone();
    table.delete(/*row=*/ old_row.clone(), /*lsn=*/ 100).await;
    table.append(new_row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    flush_table_and_sync(&mut table, &mut event_completion_rx, /*lsn=*/ 200)
        .await
        .unwrap();
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

#[tokio::test]
async fn test_table_recovery() {
    let table_name = "table_recovery";

    let context = TestContext::new(table_name);
    let mut table = test_table(&context, table_name, IdentityProp::FullRow).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Write new rows and create iceberg snapshot.
    let row = test_row(1, "John", 30);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    flush_table_and_sync(&mut table, &mut event_completion_rx, /*lsn=*/ 100)
        .await
        .unwrap();
    create_mooncake_and_persist_for_test(&mut table, &mut event_completion_rx).await;

    // Recovery from iceberg snapshot and check mooncake table recovery.
    let iceberg_table_config = test_iceberg_table_config(&context, table_name);
    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path());
    let wal_manager = WalManager::new(&wal_config);
    let recovered_table = MooncakeTable::new(
        (*create_test_arrow_schema()).clone(),
        table_name.to_string(),
        /*table_id=*/ 1,
        context.path(),
        iceberg_table_config.clone(),
        test_mooncake_table_config(&context),
        wal_manager,
        create_test_object_storage_cache(&context.temp_dir),
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();
    assert_eq!(recovered_table.last_iceberg_snapshot_lsn.unwrap(), 100);
}

/// ---- Mock unit test ----
#[tokio::test]
async fn test_snapshot_load_failure() {
    let temp_dir = TempDir::new().unwrap();
    let table_metadata = Arc::new(TableMetadata {
        name: "test_table".to_string(),
        table_id: 1,
        schema: create_test_arrow_schema(),
        config: MooncakeTableConfig::default(), // No temp files generated.
        path: PathBuf::from(temp_dir.path()),
    });
    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_load_snapshot_from_table()
        .times(1)
        .returning(|| {
            Box::pin(async move {
                Err(Error::from(IcebergError::new(
                    ErrorKind::Unexpected,
                    "Intended error for unit test",
                )))
            })
        });

    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, temp_dir.path());
    let wal_manager = WalManager::new(&wal_config);

    let table = MooncakeTable::new_with_table_manager(
        table_metadata,
        Box::new(mock_table_manager),
        create_test_object_storage_cache(&temp_dir),
        FileSystemAccessor::default_for_test(&temp_dir),
        wal_manager,
    )
    .await;
    assert!(table.is_err());
}

#[tokio::test]
async fn test_snapshot_store_failure() {
    let temp_dir = TempDir::new().unwrap();
    let table_metadata = Arc::new(TableMetadata {
        name: "test_table".to_string(),
        table_id: 1,
        schema: create_test_arrow_schema(),
        config: MooncakeTableConfig::default(), // No temp files generated.
        path: PathBuf::from(temp_dir.path()),
    });
    let table_metadata_copy = table_metadata.clone();

    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_get_warehouse_location()
        .times(1)
        .returning(|| "".to_string());
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
                Err(Error::from(IcebergError::new(
                    ErrorKind::Unexpected,
                    "Intended error for unit test",
                )))
            })
        });

    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, temp_dir.path());
    let wal_manager = WalManager::new(&wal_config);

    let mut table = MooncakeTable::new_with_table_manager(
        table_metadata,
        Box::new(mock_table_manager),
        create_test_object_storage_cache(&temp_dir),
        FileSystemAccessor::default_for_test(&temp_dir),
        wal_manager,
    )
    .await
    .unwrap();
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let row = test_row(1, "A", 20);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    flush_table_and_sync(&mut table, &mut event_completion_rx, /*lsn=*/ 100)
        .await
        .unwrap();

    let (_, iceberg_snapshot_payload, _, _, evicted_data_files_cache) =
        create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;
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

#[tokio::test]
async fn test_alter_table_with_operations() {
    let context = TestContext::new("alter_table");
    let mut table = test_table(&context, "alter_table", IdentityProp::Keys(vec![0])).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Insert a row before altering the table
    let row_before = test_row(1, "A", 20);
    table.append(row_before.clone()).unwrap();
    table.commit(1);

    // Check that the schema contains the "age" field before alteration
    let fields_before: Vec<_> = table
        .metadata
        .schema
        .fields
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert!(fields_before.contains(&"age".to_string()));

    flush_table_and_sync(&mut table, &mut event_completion_rx, 1)
        .await
        .unwrap();
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;
    // Alter the table (removes the "age" field)
    alter_table(&mut table).await;

    // Check that the schema no longer contains the "age" field
    let fields_after: Vec<_> = table
        .metadata
        .schema
        .fields
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert!(!fields_after.contains(&"age".to_string()));
    // Ensure other fields are still present
    assert!(fields_after.contains(&"id".to_string()));
    assert!(fields_after.contains(&"name".to_string()));

    let row_after = MoonlinkRow::new(vec![
        crate::row::RowValue::Int32(2),
        crate::row::RowValue::ByteArray("B".as_bytes().to_vec()),
    ]);
    table.append(row_after.clone()).unwrap();
    table.commit(2);

    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

    // Verify that the table now contains both the old and new rows
    let mut table_snapshot = table.snapshot.write().await;
    let SnapshotReadOutput {
        data_file_paths,
        puffin_cache_handles,
        position_deletes,
        deletion_vectors,
        ..
    } = table_snapshot.request_read().await.unwrap();
    verify_files_and_deletions(
        get_data_files_for_read(&data_file_paths).as_slice(),
        get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
        position_deletes,
        deletion_vectors,
        &[1, 2],
    )
    .await;
}

#[tokio::test]
async fn test_streaming_begin_flush_commit_end_flush() {
    let context = TestContext::new("streaming_begin_flush_commit_end_flush");
    let mut table = test_table(
        &context,
        "streaming_begin_flush_commit_end_flush",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id = 1;
    let lsn = 1;

    // Append a row to streaming mem slice
    let row = test_row(1, "A", 20);
    table.append_in_stream_batch(row, xact_id).unwrap();

    // Begin the flush
    // This will drain the mem slice and add its relevant state to the stream state
    let disk_slice = flush_stream_and_sync_no_apply(
        &mut table,
        &mut event_completion_rx,
        xact_id,
        Some(lsn + 1),
    )
    .await
    .expect("Disk slice should be present");

    // Wait to apply the flush to simulate an async flush that hasn't returned
    // Commit the transaction while the flush is pending
    // This will move the state from the stream state to the snapshot task
    table
        .commit_transaction_stream_impl(xact_id, lsn + 1)
        .unwrap();

    // Make sure the row is visible in snapshot before flush finishes
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;
    // Read the snapshot
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths, ..
        } = snapshot.request_read().await.unwrap();
        verify_file_contents(&data_file_paths[0].get_file_path(), &[1], Some(1));
    }

    // Apply the flush
    table.apply_stream_flush_result(
        xact_id,
        disk_slice,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Make sure we can still read the row after flush
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths, ..
        } = snapshot.request_read().await.unwrap();
        verify_file_contents(&data_file_paths[0].get_file_path(), &[1], Some(1));
    }

    // Make sure we can still read the row after snapshot
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths, ..
        } = snapshot.request_read().await.unwrap();
        verify_file_contents(&data_file_paths[0].get_file_path(), &[1], Some(1));
    }
}

#[tokio::test]
async fn test_streaming_begin_flush_commit_end_flush_multiple() {
    let context = TestContext::new("streaming_begin_flush_commit_end_flush_multiple");
    let mut table = test_table(
        &context,
        "streaming_begin_flush_commit_end_flush_multiple",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id = 1;
    let lsn = 1;

    // Append a row to streaming mem slice
    let row1 = test_row(1, "A", 20);
    table.append_in_stream_batch(row1, xact_id).unwrap();

    // Begin the flush
    // This will drain the mem slice and add its relevant state to the stream state
    let disk_slice1 = flush_stream_and_sync_no_apply(
        &mut table,
        &mut event_completion_rx,
        xact_id,
        /*lsn=*/ None,
    )
    .await
    .unwrap();

    let row2 = test_row(2, "B", 21);
    table.append_in_stream_batch(row2, xact_id).unwrap();

    // Begin the flush
    // This will drain the mem slice and add its relevant state to the stream state
    let disk_slice2 =
        flush_stream_and_sync_no_apply(&mut table, &mut event_completion_rx, xact_id, Some(lsn))
            .await
            .unwrap();

    // Wait to apply the flush to simulate an async flush that hasn't returned
    // Commit the transaction while the flush is pending
    // This will move the state from the stream state to the snapshot task
    table.commit_transaction_stream_impl(xact_id, lsn).unwrap();

    // Make sure the rows are visible in snapshot before flush finishes
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;
    // Read the snapshot
    // (Should use the in memory file)
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths, ..
        } = snapshot.request_read().await.unwrap();
        verify_file_contents(&data_file_paths[0].get_file_path(), &[1, 2], Some(2));
    }

    // Apply the flush
    table.apply_stream_flush_result(
        xact_id,
        disk_slice1,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Make sure the stream state is still present since we have outstanding flush
    assert!(table.transaction_stream_states.contains_key(&xact_id));

    // Apply the second flush
    table.apply_stream_flush_result(
        xact_id,
        disk_slice2,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Assert the stream state is removed
    assert!(!table.transaction_stream_states.contains_key(&xact_id));

    // Make sure we can still read the rows after flush
    // (Should still use the in memory file)
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths, ..
        } = snapshot.request_read().await.unwrap();
        verify_file_contents(&data_file_paths[0].get_file_path(), &[1, 2], Some(2));
    }

    // Make sure we can still read the rows after snapshot
    // (Should now read the rows from disk)
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            mut data_file_paths,
            ..
        } = snapshot.request_read().await.unwrap();
        data_file_paths.sort_by_key(|data_file| data_file.get_file_path());
        verify_file_contents(&data_file_paths[1].get_file_path(), &[1], Some(1));
        verify_file_contents(&data_file_paths[2].get_file_path(), &[2], Some(1));
    }
}

#[tokio::test]
async fn test_streaming_begin_flush_commit_delete_end_flush() {
    let context = TestContext::new("streaming_begin_flush_commit_delete_end_flush");
    let mut table = test_table(
        &context,
        "streaming_begin_flush_commit_delete_end_flush",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id = 1;
    let lsn = 1;

    // Append rows to streaming mem slice
    let row1 = test_row(1, "A", 20);
    table.append_in_stream_batch(row1, xact_id).unwrap();
    let row2 = test_row(2, "B", 21);
    table.append_in_stream_batch(row2.clone(), xact_id).unwrap();

    // Begin the flush
    // This will drain the mem slice and add its relevant state to the stream state
    let disk_slice =
        flush_stream_and_sync_no_apply(&mut table, &mut event_completion_rx, xact_id, Some(lsn))
            .await
            .expect("Disk slice should be present");

    // Delete the row that is still flushing
    table.delete_in_stream_batch(row2, xact_id).await;

    // Make sure the data is not visible before commit
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths, ..
        } = snapshot.request_read().await.unwrap();
        assert_eq!(data_file_paths.len(), 0);
    }

    // Finish the flush
    table.apply_stream_flush_result(
        xact_id,
        disk_slice,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Commit the transaction
    table.commit_transaction_stream_impl(xact_id, lsn).unwrap();

    // Make sure we only have one row in the snapshot
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

    // Read the snapshot
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            position_deletes,
            deletion_vectors,
            ..
        } = snapshot.request_read().await.unwrap();
        verify_files_and_deletions(
            get_data_files_for_read(&data_file_paths).as_slice(),
            get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
            position_deletes,
            deletion_vectors,
            &[1],
        )
        .await;
    }
}

#[tokio::test]
async fn test_streaming_begin_flush_delete_commit_end_flush() {
    let context = TestContext::new("streaming_begin_flush_delete_commit_end_flush");
    let mut table = test_table(
        &context,
        "streaming_begin_flush_delete_commit_end_flush",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id = 1;
    let lsn = 1;

    // Append a row to streaming mem slice
    let row1 = test_row(1, "A", 20);
    table.append_in_stream_batch(row1, xact_id).unwrap();
    let row2 = test_row(2, "B", 21);
    table.append_in_stream_batch(row2.clone(), xact_id).unwrap();

    // Begin the flush
    // This will drain the mem slice and add its relevant state to the stream state
    let disk_slice = flush_stream_and_sync_no_apply(
        &mut table,
        &mut event_completion_rx,
        xact_id,
        Some(lsn + 1),
    )
    .await
    .unwrap();

    // Wait to apply the flush to simulate an async flush that hasn't returned
    // Commit the transaction while the flush is pending
    // This will move the state from the stream state to the snapshot task
    table
        .commit_transaction_stream_impl(xact_id, lsn + 1)
        .unwrap();

    // Make sure the rows are visible in snapshot before flush finishes
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;
    // Read the snapshot
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths, ..
        } = snapshot.request_read().await.unwrap();
        verify_file_contents(&data_file_paths[0].get_file_path(), &[1, 2], Some(2));
    }

    // Start a new transaction
    let xact_id2 = 2;
    let lsn2 = 2;

    // Delete the row from the previous transaction (has not been flushed yet)
    table.delete_in_stream_batch(row2, xact_id2).await;

    // Commit the new transaction
    let disk_slice2 = flush_stream_and_sync_no_apply(
        &mut table,
        &mut event_completion_rx,
        xact_id2,
        Some(lsn2 + 1),
    )
    .await
    .unwrap();
    table
        .commit_transaction_stream_impl(xact_id2, lsn2 + 1)
        .unwrap();

    table.apply_stream_flush_result(
        xact_id,
        disk_slice,
        uuid::Uuid::new_v4(), /*placeholder*/
    );
    table.apply_stream_flush_result(
        xact_id2,
        disk_slice2,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Make sure we only have one row in the snapshot
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

    // Read the snapshot
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            position_deletes,
            deletion_vectors,
            ..
        } = snapshot.request_read().await.unwrap();
        verify_files_and_deletions(
            get_data_files_for_read(&data_file_paths).as_slice(),
            get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
            position_deletes,
            deletion_vectors,
            &[1],
        )
        .await;
    }
}

#[tokio::test]
async fn test_streaming_begin_flush_commit_end_flush_delete() {
    let context = TestContext::new("streaming_begin_flush_commit_end_flush_delete");
    let mut table = test_table(
        &context,
        "streaming_begin_flush_commit_end_flush_delete",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id = 1;
    let lsn = 1;

    // Append a row to streaming mem slice
    let row1 = test_row(1, "A", 20);
    table.append_in_stream_batch(row1, xact_id).unwrap();
    let row2 = test_row(2, "B", 21);
    table.append_in_stream_batch(row2.clone(), xact_id).unwrap();

    // Begin the flush
    // This will drain the mem slice and add its relevant state to the stream state
    // Wait to apply the flush to simulate an async flush that hasn't returned
    // Commit the transaction while the flush is pending
    // This will move the state from the stream state to the snapshot task
    let disk_slice =
        flush_stream_and_sync_no_apply(&mut table, &mut event_completion_rx, xact_id, Some(lsn))
            .await
            .unwrap();

    // Commit the transaction
    table.commit_transaction_stream_impl(xact_id, lsn).unwrap();

    // Apply the flush
    table.apply_stream_flush_result(
        xact_id,
        disk_slice,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Make sure the rows are visible in snapshot before commit finishes
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;
    // Read the snapshot
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths, ..
        } = snapshot.request_read().await.unwrap();
        verify_file_contents(&data_file_paths[0].get_file_path(), &[1, 2], Some(2));
    }

    // Start a new transaction
    let xact_id2 = 2;
    let lsn2 = 2;

    // Delete the row from the previous transaction (has not been flushed yet)
    table.delete_in_stream_batch(row2, xact_id2).await;

    // Flush and Commit the new transaction
    commit_transaction_stream_and_sync(&mut table, &mut event_completion_rx, xact_id2, lsn2 + 1)
        .await;

    // Make sure we only have one row in the snapshot
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

    // Read the snapshot
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            position_deletes,
            deletion_vectors,
            ..
        } = snapshot.request_read().await.unwrap();
        verify_files_and_deletions(
            get_data_files_for_read(&data_file_paths).as_slice(),
            get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
            position_deletes,
            deletion_vectors,
            &[1],
        )
        .await;
    }
}

#[tokio::test]
async fn test_streaming_begin_flush_abort_end_flush() {
    let context = TestContext::new("streaming_begin_flush_abort_end_flush");
    let mut table = test_table(
        &context,
        "streaming_begin_flush_abort_end_flush",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id = 1;

    // Append a row to streaming mem slice
    let row = test_row(1, "A", 20);
    table.append_in_stream_batch(row, xact_id).unwrap();

    // Begin the flush
    // This will drain the mem slice and add its relevant state to the stream state
    let disk_slice =
        flush_stream_and_sync_no_apply(&mut table, &mut event_completion_rx, xact_id, None)
            .await
            .expect("Disk slice should be present");

    // Wait to apply the flush to simulate an async flush that hasn't returned
    // Abort the transaction while the flush is pending
    // This will move the state from the stream state to the snapshot task
    table.abort_in_stream_batch(xact_id);

    // Apply the flush
    table.apply_stream_flush_result(
        xact_id,
        disk_slice,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Make sure we can't read the row after flush
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths, ..
        } = snapshot.request_read().await.unwrap();
        assert_eq!(data_file_paths.len(), 0);
    }
}

#[tokio::test]
async fn test_streaming_begin_flush_abort_end_flush_multiple() {
    let context = TestContext::new("streaming_begin_flush_abort_end_flush_multiple");
    let mut table = test_table(
        &context,
        "streaming_begin_flush_abort_end_flush_multiple",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id = 1;

    // Append a row to streaming mem slice
    let row1 = test_row(1, "A", 20);
    table.append_in_stream_batch(row1, xact_id).unwrap();
    let row2 = test_row(2, "B", 21);
    table.append_in_stream_batch(row2, xact_id).unwrap();

    // Begin the flush
    // This will drain the mem slice and add its relevant state to the stream state
    let disk_slice1 =
        flush_stream_and_sync_no_apply(&mut table, &mut event_completion_rx, xact_id, None)
            .await
            .expect("Disk slice should be present");

    let disk_slice2 =
        flush_stream_and_sync_no_apply(&mut table, &mut event_completion_rx, xact_id, None)
            .await
            .expect("Disk slice should be present");

    // Wait to apply the flush to simulate an async flush that hasn't returned
    // Abort the transaction while the flush is pending
    // This will move the state from the stream state to the snapshot task
    table.abort_in_stream_batch(xact_id);

    // Apply the first flush
    table.apply_stream_flush_result(
        xact_id,
        disk_slice1,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Make sure the stream state is still present since we have outstanding flush
    assert!(table.transaction_stream_states.contains_key(&xact_id));

    // Apply the second flush
    table.apply_stream_flush_result(
        xact_id,
        disk_slice2,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Assert the stream state is removed
    assert!(!table.transaction_stream_states.contains_key(&xact_id));

    // Make sure we can't read the row after flush
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;
    {
        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths, ..
        } = snapshot.request_read().await.unwrap();
        assert_eq!(data_file_paths.len(), 0);
    }
}

/// ===================================
/// LSN Ordering Tests for Iceberg Snapshots
/// ===================================

#[tokio::test]
async fn test_ongoing_flush_lsns_tracking() -> Result<()> {
    let context = TestContext::new("ongoing_flush_tracking");
    let mut table = test_table(&context, "lsn_table", IdentityProp::FullRow).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Verify initially no pending flushes
    assert!(table.ongoing_flush_lsns.is_empty());
    assert_eq!(table.get_min_ongoing_flush_lsn(), u64::MAX);

    // Add data and start multiple flushes with monotonically increasing LSNs
    append_rows(&mut table, vec![test_row(1, "A", 20)])?;
    table.commit(1);
    let disk_slice_1 =
        flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, /*lsn=*/ 1)
            .await
            .expect("Disk slice 1 should be present");

    append_rows(&mut table, vec![test_row(2, "B", 21)])?;
    table.commit(2);
    let disk_slice_2 =
        flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, /*lsn=*/ 2)
            .await
            .expect("Disk slice 2 should be present");

    append_rows(&mut table, vec![test_row(3, "C", 22)])?;
    table.commit(3);
    let disk_slice_3 =
        flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, /*lsn=*/ 3)
            .await
            .expect("Disk slice 3 should be present");

    // Verify all LSNs are tracked
    assert!(table.ongoing_flush_lsns.contains_key(&1));
    assert!(table.ongoing_flush_lsns.contains_key(&2));
    assert!(table.ongoing_flush_lsns.contains_key(&3));
    assert_eq!(table.ongoing_flush_lsns.len(), 3);

    // Verify min is correctly calculated (should be 1)
    assert_eq!(table.get_min_ongoing_flush_lsn(), 1);

    // Complete flush with LSN 2 (out of order completion)
    table.apply_flush_result(disk_slice_2, uuid::Uuid::new_v4() /*placeholder*/);
    assert!(table.ongoing_flush_lsns.contains_key(&1));
    assert!(!table.ongoing_flush_lsns.contains_key(&2));
    assert!(table.ongoing_flush_lsns.contains_key(&3));
    assert_eq!(table.get_min_ongoing_flush_lsn(), 1); // Still 1

    // Complete flush with LSN 1
    table.apply_flush_result(disk_slice_1, uuid::Uuid::new_v4() /*placeholder*/);
    assert!(!table.ongoing_flush_lsns.contains_key(&1));
    assert!(table.ongoing_flush_lsns.contains_key(&3));
    assert_eq!(table.get_min_ongoing_flush_lsn(), 3); // Now 3

    // Complete last flush
    table.apply_flush_result(disk_slice_3, uuid::Uuid::new_v4() /*placeholder*/);
    assert!(table.ongoing_flush_lsns.is_empty());
    assert_eq!(table.get_min_ongoing_flush_lsn(), u64::MAX);

    Ok(())
}

#[tokio::test]
async fn test_streaming_flush_lsns_tracking() -> Result<()> {
    let context = TestContext::new("streaming_flush_tracking");
    let mut table = test_table(&context, "lsn_table", IdentityProp::FullRow).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id_1 = 1;
    let xact_id_2 = 2;

    // Start streaming transactions
    table.append_in_stream_batch(test_row(1, "A", 20), xact_id_1)?;
    table.append_in_stream_batch(test_row(2, "B", 21), xact_id_2)?;

    // Flush streaming transactions with different LSNs (can be out of order)
    let disk_slice_1 =
        flush_stream_and_sync_no_apply(&mut table, &mut event_completion_rx, xact_id_1, Some(50))
            .await
            .expect("Disk slice 1 should be present");
    table
        .commit_transaction_stream_impl(xact_id_1, /*lsn=*/ 50)
        .unwrap();

    let disk_slice_2 =
        flush_stream_and_sync_no_apply(&mut table, &mut event_completion_rx, xact_id_2, Some(100))
            .await
            .expect("Disk slice 2 should be present");
    table
        .commit_transaction_stream_impl(xact_id_2, /*lsn=*/ 100)
        .unwrap();

    // Verify both streaming LSNs are tracked
    assert!(table.ongoing_flush_lsns.contains_key(&100));
    assert!(table.ongoing_flush_lsns.contains_key(&50));
    assert_eq!(table.get_min_ongoing_flush_lsn(), 50);

    // Mix with regular flush (must be higher than previous regular flush)
    append_rows(&mut table, vec![test_row(3, "C", 22)])?;
    table.commit(103);
    let disk_slice_3 = flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 75)
        .await
        .expect("Disk slice 3 should be present");

    // Verify all three LSNs are tracked
    assert!(table.ongoing_flush_lsns.contains_key(&100));
    assert!(table.ongoing_flush_lsns.contains_key(&50));
    assert!(table.ongoing_flush_lsns.contains_key(&75));
    assert_eq!(table.get_min_ongoing_flush_lsn(), 50);

    // Complete streaming flushes
    table.apply_stream_flush_result(
        xact_id_1,
        disk_slice_1,
        uuid::Uuid::new_v4(), /*placeholder*/
    );
    table.apply_stream_flush_result(
        xact_id_2,
        disk_slice_2,
        uuid::Uuid::new_v4(), /*placeholder*/
    );
    table.apply_flush_result(disk_slice_3, uuid::Uuid::new_v4() /*placeholder*/);

    // Verify all pending flushes are cleared
    assert!(table.ongoing_flush_lsns.is_empty());
    assert_eq!(table.get_min_ongoing_flush_lsn(), u64::MAX);

    Ok(())
}

#[tokio::test]
async fn test_lsn_ordering_constraint_with_real_table_handler_state() -> Result<()> {
    let context = TestContext::new("lsn_ordering_constraint");
    let mut table = test_table(&context, "lsn_table", IdentityProp::FullRow).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Test case 1: No pending flushes - should allow any flush LSN
    assert!(table.ongoing_flush_lsns.is_empty());
    let min_pending = table.get_min_ongoing_flush_lsn();
    assert_eq!(min_pending, u64::MAX);

    // With no pending flushes, should allow iceberg snapshots
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        100,
        min_pending,
        true,
        false
    ));
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        1000,
        min_pending,
        true,
        false
    ));

    // Test case 2: Start some flushes
    append_rows(&mut table, vec![test_row(1, "A", 20)])?;
    table.commit(1);
    let disk_slice_1 = flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 30)
        .await
        .expect("Disk slice should be present");

    append_rows(&mut table, vec![test_row(2, "B", 21)])?;
    table.commit(2);
    let disk_slice_2 = flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 50)
        .await
        .expect("Disk slice should be present");

    let min_pending = table.get_min_ongoing_flush_lsn();
    assert_eq!(min_pending, 30);

    // Should allow iceberg snapshots with flush_lsn < 30
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        10,
        min_pending,
        true,
        false
    ));
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        29,
        min_pending,
        true,
        false
    ));

    // Should NOT allow iceberg snapshots with flush_lsn >= 30
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        30,
        min_pending,
        true,
        false
    ));
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        40,
        min_pending,
        true,
        false
    ));
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        100,
        min_pending,
        true,
        false
    ));

    // Test case 3: Test iceberg snapshot ongoing constraint
    // Even with valid flush_lsn < min_pending, should not allow due to ongoing snapshot
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        10,
        min_pending,
        true,
        true
    ));
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        29,
        min_pending,
        true,
        true
    ));

    // Test case 4: Test iceberg snapshot result not consumed constraint
    // Even with valid conditions, should not allow due to unconsumed result
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        10,
        min_pending,
        false,
        false
    ));
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        29,
        min_pending,
        false,
        false
    ));

    // Complete the flush with LSN 30
    table.apply_flush_result(disk_slice_1, uuid::Uuid::new_v4() /*placeholder*/);
    let min_pending = table.get_min_ongoing_flush_lsn();
    assert_eq!(min_pending, 50);

    // Now should allow iceberg snapshots with flush_lsn < 50
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        30,
        min_pending,
        true,
        false
    ));
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        49,
        min_pending,
        true,
        false
    ));

    // Should NOT allow iceberg snapshots with flush_lsn >= 50
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        50,
        min_pending,
        true,
        false
    ));
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        60,
        min_pending,
        true,
        false
    ));

    // Complete the remaining flush
    table.apply_flush_result(disk_slice_2, uuid::Uuid::new_v4() /*placeholder*/);
    let min_pending = table.get_min_ongoing_flush_lsn();
    assert_eq!(min_pending, u64::MAX);

    // Now should allow any iceberg snapshot again
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        100,
        min_pending,
        true,
        false
    ));
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        1000,
        min_pending,
        true,
        false
    ));

    Ok(())
}

#[tokio::test]
async fn test_out_of_order_flush_completion() -> Result<()> {
    let context = TestContext::new("out_of_order_flush");
    let mut table = test_table(&context, "lsn_table", IdentityProp::FullRow).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Start flushes in order: 10, 20, 30
    append_rows(&mut table, vec![test_row(1, "A", 20)])?;
    table.commit(1);
    let disk_slice_10 = flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 10)
        .await
        .expect("Disk slice should be present");

    append_rows(&mut table, vec![test_row(2, "B", 21)])?;
    table.commit(2);
    let disk_slice_20 = flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 20)
        .await
        .expect("Disk slice should be present");

    append_rows(&mut table, vec![test_row(3, "C", 22)])?;
    table.commit(3);
    let disk_slice_30 = flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 30)
        .await
        .expect("Disk slice should be present");

    // Verify all are pending and min is 10
    assert!(table.ongoing_flush_lsns.contains_key(&10));
    assert!(table.ongoing_flush_lsns.contains_key(&20));
    assert!(table.ongoing_flush_lsns.contains_key(&30));
    assert_eq!(table.get_min_ongoing_flush_lsn(), 10);

    // Complete flush 30 first (out of order)
    table.apply_flush_result(disk_slice_30, uuid::Uuid::new_v4() /*placeholder*/);
    assert!(!table.ongoing_flush_lsns.contains_key(&30));
    assert!(table.ongoing_flush_lsns.contains_key(&10));
    assert!(table.ongoing_flush_lsns.contains_key(&20));
    assert_eq!(table.get_min_ongoing_flush_lsn(), 10); // Still 10

    // Complete flush 10 (should update min to 20)
    table.apply_flush_result(disk_slice_10, uuid::Uuid::new_v4() /*placeholder*/);
    assert!(!table.ongoing_flush_lsns.contains_key(&10));
    assert!(table.ongoing_flush_lsns.contains_key(&20));
    assert_eq!(table.get_min_ongoing_flush_lsn(), 20);

    // Complete flush 20 (should clear all)
    table.apply_flush_result(disk_slice_20, uuid::Uuid::new_v4() /*placeholder*/);
    assert!(table.ongoing_flush_lsns.is_empty());
    assert_eq!(table.get_min_ongoing_flush_lsn(), u64::MAX);

    Ok(())
}

#[tokio::test]
async fn test_mixed_regular_and_streaming_lsn_ordering() -> Result<()> {
    let context = TestContext::new("mixed_lsn_ordering");
    let mut table = test_table(&context, "lsn_table", IdentityProp::FullRow).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id = 1;

    // Start regular flush with LSN 100
    append_rows(&mut table, vec![test_row(1, "A", 20)])?;
    table.commit(1);
    let regular_disk_slice =
        flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 100)
            .await
            .expect("Regular disk slice should be present");

    // Start streaming flush with LSN 50 (lower than regular, but allowed for streaming)
    table.append_in_stream_batch(test_row(2, "B", 21), xact_id)?;
    let streaming_disk_slice = flush_stream_and_sync_no_apply(
        &mut table,
        &mut event_completion_rx,
        xact_id,
        /*lsn=*/ Some(50),
    )
    .await
    .expect("Streaming disk slice should be present");
    table
        .commit_transaction_stream_impl(xact_id, /*lsn=*/ 50)
        .unwrap();

    // Verify both are tracked and min is 50
    assert!(table.ongoing_flush_lsns.contains_key(&100));
    assert!(table.ongoing_flush_lsns.contains_key(&50));
    assert_eq!(table.get_min_ongoing_flush_lsn(), 50);

    // According to table handler logic, iceberg snapshots with flush_lsn >= 50 should be blocked
    let min_pending = table.get_min_ongoing_flush_lsn();
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        50,
        min_pending,
        true,
        false
    )); // Blocked
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        75,
        min_pending,
        true,
        false
    )); // Blocked
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        40,
        min_pending,
        true,
        false
    )); // Allowed

    // Complete streaming flush first
    table.apply_stream_flush_result(
        xact_id,
        streaming_disk_slice,
        uuid::Uuid::new_v4(), /*placeholder*/
    );
    assert!(!table.ongoing_flush_lsns.contains_key(&50));
    assert!(table.ongoing_flush_lsns.contains_key(&100));
    assert_eq!(table.get_min_ongoing_flush_lsn(), 100);

    // Now iceberg snapshots with flush_lsn < 100 should be allowed
    let min_pending = table.get_min_ongoing_flush_lsn();
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        75,
        min_pending,
        true,
        false
    )); // Now allowed
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        100,
        min_pending,
        true,
        false
    )); // Still blocked

    // Complete regular flush
    table.apply_flush_result(
        regular_disk_slice,
        uuid::Uuid::new_v4(), /*placeholder*/
    );
    assert!(table.ongoing_flush_lsns.is_empty());
    assert_eq!(table.get_min_ongoing_flush_lsn(), u64::MAX);

    Ok(())
}

#[tokio::test]
async fn test_lsn_ordering_both_functions_in_tandem() -> Result<()> {
    let context = TestContext::new("lsn_ordering_tandem");
    let mut table = test_table(&context, "lsn_table", IdentityProp::FullRow).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Test case 1: No pending flushes - both functions should allow operations
    assert!(table.ongoing_flush_lsns.is_empty());
    let min_pending = table.get_min_ongoing_flush_lsn();
    assert_eq!(min_pending, u64::MAX);

    // With no pending flushes, should allow iceberg snapshots for any flush_lsn
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        100,
        min_pending,
        true,
        false
    ));
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        1000,
        min_pending,
        true,
        false
    ));

    // With no pending flushes, should not force snapshots for any commit_lsn (no maintenance needed)
    assert!(!TableHandlerState::should_force_snapshot_by_commit_lsn(
        100,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        None,
        false
    ));
    assert!(!TableHandlerState::should_force_snapshot_by_commit_lsn(
        1000,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        None,
        false
    ));

    // Test case 2: Start flushes and test both functions
    append_rows(&mut table, vec![test_row(1, "A", 20)])?;
    table.commit(1);
    let disk_slice_1 = flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 30)
        .await
        .expect("Disk slice should be present");

    append_rows(&mut table, vec![test_row(2, "B", 21)])?;
    table.commit(2);
    let disk_slice_2 = flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 50)
        .await
        .expect("Disk slice should be present");

    let min_pending = table.get_min_ongoing_flush_lsn();
    assert_eq!(min_pending, 30);

    // Test complementary LSN ordering logic:
    // can_initiate_iceberg_snapshot requires: flush_lsn < min_ongoing_flush_lsn
    // should_force_snapshot_by_commit_lsn requires: min_ongoing_flush_lsn >= commit_lsn (returns false if min_ongoing_flush_lsn < commit_lsn)

    // For LSNs < 30 (min pending):
    // - can_initiate_iceberg_snapshot should return true (flush_lsn < min_pending)
    // - should_force_snapshot_by_commit_lsn should return false (no force needed, min_pending >= commit_lsn)
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        10,
        min_pending,
        true,
        false
    ));
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        29,
        min_pending,
        true,
        false
    ));
    assert!(!TableHandlerState::should_force_snapshot_by_commit_lsn(
        10,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        None,
        false
    ));
    assert!(!TableHandlerState::should_force_snapshot_by_commit_lsn(
        29,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        None,
        false
    ));

    // For LSNs >= 30 (min pending):
    // - can_initiate_iceberg_snapshot should return false (flush_lsn >= min_pending)
    // - should_force_snapshot_by_commit_lsn should return false (constraint violated: min_pending < commit_lsn)
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        30,
        min_pending,
        true,
        false
    ));
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        40,
        min_pending,
        true,
        false
    ));
    assert!(!TableHandlerState::should_force_snapshot_by_commit_lsn(
        40,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        None,
        false
    )); // Blocked by LSN ordering
    assert!(!TableHandlerState::should_force_snapshot_by_commit_lsn(
        100,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        None,
        false
    )); // Blocked by LSN ordering

    // Test case 3: Complete first flush and test again
    table.apply_flush_result(disk_slice_1, uuid::Uuid::new_v4() /*placeholder*/);
    let min_pending = table.get_min_ongoing_flush_lsn();
    assert_eq!(min_pending, 50);

    // Now with min_pending = 50:
    // LSNs < 50 should allow iceberg snapshots
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        30,
        min_pending,
        true,
        false
    ));
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        49,
        min_pending,
        true,
        false
    ));
    assert!(!TableHandlerState::should_force_snapshot_by_commit_lsn(
        30,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        None,
        false
    ));
    assert!(!TableHandlerState::should_force_snapshot_by_commit_lsn(
        49,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        None,
        false
    ));

    // LSNs >= 50 should block both
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        50,
        min_pending,
        true,
        false
    ));
    assert!(!TableHandlerState::can_initiate_iceberg_snapshot(
        60,
        min_pending,
        true,
        false
    ));
    assert!(!TableHandlerState::should_force_snapshot_by_commit_lsn(
        50,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        None,
        false
    ));
    assert!(!TableHandlerState::should_force_snapshot_by_commit_lsn(
        60,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        None,
        false
    ));

    // Test case 4: Test state-based constraints for should_force_snapshot_by_commit_lsn
    // Test ReadyToPersist maintenance status - should force snapshot regardless of LSN (if LSN constraints allow)
    assert!(TableHandlerState::should_force_snapshot_by_commit_lsn(
        25,
        min_pending,
        &MaintenanceProcessStatus::ReadyToPersist,
        None,
        false
    )); // LSN 25 < min_pending (50), should be allowed and forced due to ReadyToPersist
    assert!(TableHandlerState::should_force_snapshot_by_commit_lsn(
        30,
        min_pending,
        &MaintenanceProcessStatus::ReadyToPersist,
        None,
        false
    )); // LSN 30 < min_pending (50), should be allowed and forced due to ReadyToPersist

    // But higher commit LSNs should still be blocked by the pending flush constraint
    assert!(!TableHandlerState::should_force_snapshot_by_commit_lsn(
        60,
        min_pending,
        &MaintenanceProcessStatus::ReadyToPersist,
        None,
        false
    )); // Blocked by LSN ordering even with ReadyToPersist

    // Test force snapshot request with largest_force_snapshot_lsn
    assert!(TableHandlerState::should_force_snapshot_by_commit_lsn(
        25,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        Some(25), // Request force snapshot for LSN 25
        false
    )); // LSN 25 >= requested (25) and < min_pending (50), should be allowed
    assert!(TableHandlerState::should_force_snapshot_by_commit_lsn(
        30,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        Some(25), // Request force snapshot for LSN 25
        false
    )); // LSN 30 >= requested (25) and < min_pending (50), should be allowed

    // Complete the remaining flush
    table.apply_flush_result(disk_slice_2, uuid::Uuid::new_v4() /*placeholder*/);
    let min_pending = table.get_min_ongoing_flush_lsn();
    assert_eq!(min_pending, u64::MAX);

    // Now with no pending flushes, both functions should work without LSN constraints
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        100,
        min_pending,
        true,
        false
    ));
    assert!(TableHandlerState::can_initiate_iceberg_snapshot(
        1000,
        min_pending,
        true,
        false
    ));

    // Force snapshot should now work for requested LSN (if other conditions are met)
    assert!(TableHandlerState::should_force_snapshot_by_commit_lsn(
        25,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        Some(25),
        false
    )); // Now force snapshot can proceed
    assert!(TableHandlerState::should_force_snapshot_by_commit_lsn(
        100,
        min_pending,
        &MaintenanceProcessStatus::Unrequested,
        Some(25),
        false
    )); // Higher LSNs also work

    // Test mooncake_snapshot_ongoing constraint
    assert!(!TableHandlerState::should_force_snapshot_by_commit_lsn(
        25,
        min_pending,
        &MaintenanceProcessStatus::ReadyToPersist,
        Some(25),
        true // mooncake_snapshot_ongoing = true
    )); // Should be blocked even with ReadyToPersist and force request

    Ok(())
}

/// ===================================
/// Tests for Iceberg Snapshot Pipeline
/// ===================================

#[tokio::test]
async fn test_iceberg_snapshot_blocked_by_ongoing_flushes() -> Result<()> {
    let context = TestContext::new("iceberg_snapshot_blocked");
    let mut table = test_table(&context, "blocked_table", IdentityProp::FullRow).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Add data and start a flush but don't complete it
    append_rows(&mut table, vec![test_row(1, "A", 20)])?;
    table.commit(1);
    let disk_slice_1 = flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 30)
        .await
        .expect("Disk slice should be present");

    // Add more data and commit it (this will be part of the snapshot)
    append_rows(&mut table, vec![test_row(2, "B", 21)])?;
    table.commit(2);

    // Verify we have pending flushes
    assert!(table.ongoing_flush_lsns.contains_key(&30));
    assert_eq!(table.get_min_ongoing_flush_lsn(), 30);

    // Create a mooncake snapshot - this will create an iceberg payload
    let created = table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4()),
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::Skip,
    });
    assert!(created, "Mooncake snapshot should be created");

    // Wait for mooncake snapshot completion
    let (commit_lsn, iceberg_snapshot_payload, _, _, _) =
        sync_mooncake_snapshot(&mut table, &mut event_completion_rx).await;

    assert_eq!(commit_lsn, 2, "Should have committed data up to LSN 2");

    // Test the table handler constraint logic
    if let Some(payload) = iceberg_snapshot_payload {
        let min_pending = table.get_min_ongoing_flush_lsn();

        // The table handler should block this iceberg snapshot due to pending flushes
        let can_initiate = TableHandlerState::can_initiate_iceberg_snapshot(
            payload.flush_lsn,
            min_pending,
            true,  // iceberg_snapshot_result_consumed
            false, // iceberg_snapshot_ongoing
        );

        assert!(!can_initiate,
            "Table handler should block iceberg snapshot due to pending flush at LSN {} vs payload flush LSN {}", 
            min_pending, payload.flush_lsn);
    }

    // Complete the pending flush
    table.apply_flush_result(disk_slice_1, uuid::Uuid::new_v4() /*placeholder*/);
    assert!(table.ongoing_flush_lsns.is_empty());

    // Now test that iceberg snapshots can proceed
    let created = table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4()),
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::Skip,
    });
    assert!(created, "Second mooncake snapshot should be created");

    let (_, iceberg_snapshot_payload, _, _, _) =
        sync_mooncake_snapshot(&mut table, &mut event_completion_rx).await;

    // Now table handler should allow iceberg snapshot
    if let Some(payload) = iceberg_snapshot_payload {
        let min_pending = table.get_min_ongoing_flush_lsn();
        let can_initiate = TableHandlerState::can_initiate_iceberg_snapshot(
            payload.flush_lsn,
            min_pending,
            true,
            false,
        );

        assert!(
            can_initiate,
            "Table handler should allow iceberg snapshot when no pending flushes"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_out_of_order_flush_completion_with_iceberg_snapshots() -> Result<()> {
    let context = TestContext::new("out_of_order_iceberg");
    let mut table = test_table(&context, "ooo_table", IdentityProp::FullRow).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Start multiple flushes
    append_rows(&mut table, vec![test_row(1, "A", 20)])?;
    table.commit(1);
    let disk_slice_1 = flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 10)
        .await
        .expect("Disk slice 1 should be present");

    append_rows(&mut table, vec![test_row(2, "B", 21)])?;
    table.commit(2);
    let disk_slice_2 = flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 20)
        .await
        .expect("Disk slice 2 should be present");

    append_rows(&mut table, vec![test_row(3, "C", 22)])?;
    table.commit(3);
    let disk_slice_3 = flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 30)
        .await
        .expect("Disk slice 3 should be present");

    // Verify all pending flushes and min pending LSN
    assert_eq!(table.get_min_ongoing_flush_lsn(), 10);
    assert!(table.ongoing_flush_lsns.contains_key(&10));
    assert!(table.ongoing_flush_lsns.contains_key(&20));
    assert!(table.ongoing_flush_lsns.contains_key(&30));

    // Create snapshot and test constraint with min_ongoing_flush_lsn = 10
    let created = table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4()),
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::Skip,
    });
    assert!(created);

    let (_, iceberg_snapshot_payload, _, _, _) =
        sync_mooncake_snapshot(&mut table, &mut event_completion_rx).await;

    // Test constraint with min pending flush LSN = 10
    if let Some(payload) = iceberg_snapshot_payload {
        let can_initiate = TableHandlerState::can_initiate_iceberg_snapshot(
            payload.flush_lsn,
            10, // min_ongoing_flush_lsn
            true,
            false,
        );

        // Should be blocked if flush_lsn >= 10
        if payload.flush_lsn >= 10 {
            assert!(
                !can_initiate,
                "Should block iceberg snapshot when flush_lsn {} >= min_ongoing_flush_lsn 10",
                payload.flush_lsn
            );
        } else {
            assert!(
                can_initiate,
                "Should allow iceberg snapshot when flush_lsn {} < min_ongoing_flush_lsn 10",
                payload.flush_lsn
            );
        }
    }

    // Complete flushes OUT OF ORDER - complete middle one first (LSN 20)
    table.apply_flush_result(disk_slice_2, uuid::Uuid::new_v4() /*placeholder*/);
    assert_eq!(table.get_min_ongoing_flush_lsn(), 10); // Should still be 10
    assert!(table.ongoing_flush_lsns.contains_key(&10));
    assert!(!table.ongoing_flush_lsns.contains_key(&20));
    assert!(table.ongoing_flush_lsns.contains_key(&30));

    // Complete the first flush (LSN 10) - now min should be 30
    table.apply_flush_result(disk_slice_1, uuid::Uuid::new_v4() /*placeholder*/);
    assert_eq!(table.get_min_ongoing_flush_lsn(), 30); // Now should be 30
    assert!(!table.ongoing_flush_lsns.contains_key(&10));
    assert!(!table.ongoing_flush_lsns.contains_key(&20));
    assert!(table.ongoing_flush_lsns.contains_key(&30));

    // Test constraint logic with new min pending flush LSN = 30
    let can_initiate_low = TableHandlerState::can_initiate_iceberg_snapshot(
        25, // flush_lsn < min_ongoing_flush_lsn
        30, // min_ongoing_flush_lsn
        true, false,
    );
    assert!(
        can_initiate_low,
        "Should allow iceberg snapshot when flush_lsn < min_ongoing_flush_lsn"
    );

    let can_initiate_high = TableHandlerState::can_initiate_iceberg_snapshot(
        35, // flush_lsn > min_ongoing_flush_lsn
        30, // min_ongoing_flush_lsn
        true, false,
    );
    assert!(
        !can_initiate_high,
        "Should block iceberg snapshot when flush_lsn >= min_ongoing_flush_lsn"
    );

    // Complete the last flush
    table.apply_flush_result(disk_slice_3, uuid::Uuid::new_v4() /*placeholder*/);
    assert!(table.ongoing_flush_lsns.is_empty());
    assert_eq!(table.get_min_ongoing_flush_lsn(), u64::MAX);

    // Now any flush LSN should be allowed
    let can_initiate_any = TableHandlerState::can_initiate_iceberg_snapshot(
        100,      // Any flush_lsn
        u64::MAX, // No pending flushes
        true,
        false,
    );
    assert!(
        can_initiate_any,
        "Should allow any iceberg snapshot when no pending flushes"
    );

    Ok(())
}

#[tokio::test]
async fn test_streaming_batch_id_mismatch_with_data_compaction() -> Result<()> {
    let context = TestContext::new("streaming_batch_id_mismatch");
    let mut table = test_table(
        &context,
        "streaming_batch_id_mismatch",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id = 1;

    // Step 1: Create initial data in main table and flush it to create data files
    // This is needed for data compaction to trigger
    append_rows(&mut table, vec![test_row(1, "Initial", 20)])?;
    table.commit(1);
    flush_table_and_sync(&mut table, &mut event_completion_rx, 10).await?;
    create_mooncake_and_persist_for_test(&mut table, &mut event_completion_rx).await;

    // Step 2: Start a streaming transaction with multiple operations
    // Add rows to streaming transaction
    table.append_in_stream_batch(test_row(2, "Stream1", 21), xact_id)?;
    table.append_in_stream_batch(test_row(3, "Stream2", 22), xact_id)?;

    // Step 3: Flush the streaming transaction
    // This creates a disk slice with the original batch IDs
    let disk_slice = flush_stream_and_sync_no_apply(
        &mut table,
        &mut event_completion_rx,
        xact_id,
        /*lsn=*/ Some(21),
    )
    .await
    .expect("Disk slice should be present");

    // Step 4: Delete one of the rows in the streaming transaction
    // This can cause some batches to become empty after filtering
    table
        .delete_in_stream_batch(test_row(3, "Stream2", 22), xact_id)
        .await;

    // Step 5: Commit the streaming transaction
    // This processes the batches and adds filtered ones to new_record_batches
    // But some original batch IDs might be missing from the filtered results
    table.commit_transaction_stream_impl(xact_id, 21)?;

    // Step 6: Apply the flush result
    // The disk slice still contains the original batch IDs
    table.apply_stream_flush_result(
        xact_id,
        disk_slice,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Step 7: Create a mooncake snapshot
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

    // Step 8: Add more data to trigger data compaction
    append_rows(&mut table, vec![test_row(4, "PostStream", 23)])?;
    table.commit(35); // Use LSN >= flush LSN (30) to satisfy validation
    flush_table_and_sync(&mut table, &mut event_completion_rx, 30).await?;

    // Step 9: Force data compaction
    let created = table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::Skip, // Skip iceberg to focus on the mooncake issue
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::ForceRegular(uuid::Uuid::new_v4()), // Trigger data compaction
    });

    assert!(created, "Mooncake snapshot should be created");

    // This should trigger the assertion failure:
    // "assertion failed: self.batches.remove(&b.id).is_some()"
    // in integrate_disk_slices at line 741
    sync_mooncake_snapshot(&mut table, &mut event_completion_rx).await;

    Ok(())
}

#[tokio::test]
async fn test_streaming_empty_batch_filtering() -> Result<()> {
    let context = TestContext::new("streaming_empty_batch_filtering");
    let mut table = test_table(
        &context,
        "streaming_empty_batch_filtering",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id1 = 1;
    let xact_id2 = 2;

    // Create some initial data for compaction
    append_rows(&mut table, vec![test_row(1, "Base", 20)])?;
    table.commit(1);
    flush_table_and_sync(&mut table, &mut event_completion_rx, 5).await?;

    // Stream 1: Add and immediately delete rows (creates empty batches)
    table.append_in_stream_batch(test_row(2, "ToDelete", 21), xact_id1)?;
    table.append_in_stream_batch(test_row(3, "AlsoDelete", 22), xact_id1)?;

    // Delete all rows in stream 1 (making batches empty after filtering)
    table
        .delete_in_stream_batch(test_row(2, "ToDelete", 21), xact_id1)
        .await;
    table
        .delete_in_stream_batch(test_row(3, "AlsoDelete", 22), xact_id1)
        .await;

    // Flush stream 1
    let disk_slice1 = flush_stream_and_sync_no_apply(
        &mut table,
        &mut event_completion_rx,
        xact_id1,
        /*lsn=*/ Some(11),
    )
    .await
    .expect("Stream 1 disk slice should be present");

    // Commit stream 1
    table.commit_transaction_stream_impl(xact_id1, /*lsn=*/ 11)?;
    table.apply_stream_flush_result(
        xact_id1,
        disk_slice1,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Stream 2: Normal operations
    table.append_in_stream_batch(test_row(4, "Keep", 23), xact_id2)?;
    commit_transaction_stream_and_sync(&mut table, &mut event_completion_rx, xact_id2, 15).await;

    // Add more data to trigger compaction threshold
    append_rows(&mut table, vec![test_row(5, "More", 24)])?;
    table.commit(16);
    flush_table_and_sync(&mut table, &mut event_completion_rx, 20).await?;

    // Create snapshot with data compaction
    let created = table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::Skip,
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::ForceRegular(uuid::Uuid::new_v4()),
    });

    assert!(created);

    sync_mooncake_snapshot(&mut table, &mut event_completion_rx).await;

    Ok(())
}

#[tokio::test]
async fn test_batch_id_removal_assertion_direct() -> Result<()> {
    let context = TestContext::new("batch_id_removal_assertion");
    let mut table = test_table(
        &context,
        "batch_id_removal_assertion",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id = 1;

    // Step 1: Create a streaming transaction with rows that will be filtered out
    // Add a row to the streaming transaction
    table.append_in_stream_batch(test_row(1, "ToFilter", 20), xact_id)?;

    // Step 2: Immediately delete the row, making the batch empty after filtering
    // This simulates the scenario where batches become empty and get filtered out
    table
        .delete_in_stream_batch(test_row(1, "ToFilter", 20), xact_id)
        .await;

    // Step 3: Flush the streaming transaction while it has the deletion
    // This creates a disk slice that contains the original batch ID
    // but the batch will be filtered out during processing (empty after deletion)
    let disk_slice =
        flush_stream_and_sync_no_apply(&mut table, &mut event_completion_rx, xact_id, Some(11))
            .await
            .expect("Disk slice should be present");

    // Step 4: Commit the streaming transaction
    table.commit_transaction_stream_impl(xact_id, 11)?;

    // Step 5: Apply the stream flush result
    table.apply_stream_flush_result(
        xact_id,
        disk_slice,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Step 6: Create a simple snapshot
    let created = table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::Skip,
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::Skip, // No compaction to avoid other issues
    });

    assert!(created, "Mooncake snapshot should be created");

    sync_mooncake_snapshot(&mut table, &mut event_completion_rx).await;

    Ok(())
}

#[tokio::test]
async fn test_puffin_deletion_blob_inconsistency_assertion() -> Result<()> {
    let context = TestContext::new("puffin_deletion_blob_inconsistency");
    let mut table = test_table(
        &context,
        "puffin_deletion_blob_inconsistency",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Step 1: Create initial data and flush it to create persistent data files
    append_rows(
        &mut table,
        vec![
            test_row(1, "Data1", 20),
            test_row(2, "Data2", 21),
            test_row(3, "Data3", 22),
            test_row(4, "Data4", 23),
        ],
    )?;
    table.commit(1);
    flush_table_and_sync(&mut table, &mut event_completion_rx, 10).await?;

    // Step 2: Delete some rows to create deletion vectors
    table.delete(test_row(2, "Data2", 21), 11).await;
    table.delete(test_row(3, "Data3", 22), 11).await;
    table.commit(12);

    // Step 3: Create and persist snapshot to create puffin deletion blobs
    // This should persist the deletion vectors as puffin blobs
    create_mooncake_and_persist_for_test(&mut table, &mut event_completion_rx).await;

    // Step 4: Add more data to trigger compaction threshold
    append_rows(
        &mut table,
        vec![
            test_row(5, "Data5", 24),
            test_row(6, "Data6", 25),
            test_row(7, "Data7", 26),
            test_row(8, "Data8", 27),
        ],
    )?;
    table.commit(15);
    flush_table_and_sync(&mut table, &mut event_completion_rx, 20).await?;

    // Step 5: Create more deletions and persist again
    table.delete(test_row(5, "Data5", 24), 21).await;
    table.delete(test_row(6, "Data6", 25), 21).await;
    table.commit(22);
    create_mooncake_and_persist_for_test(&mut table, &mut event_completion_rx).await;

    // Step 6: Add even more data to ensure we have enough files for compaction
    append_rows(
        &mut table,
        vec![test_row(9, "Data9", 28), test_row(10, "Data10", 29)],
    )?;
    table.commit(27);
    flush_table_and_sync(&mut table, &mut event_completion_rx, 26).await?;

    // Step 7: Force data compaction
    let created = table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::Skip,
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::ForceRegular(uuid::Uuid::new_v4()), // Force data compaction
    });

    assert!(created, "Mooncake snapshot should be created");

    sync_mooncake_snapshot(&mut table, &mut event_completion_rx).await;

    Ok(())
}

#[tokio::test]
async fn test_stream_commit_with_ongoing_flush_deletion_remapping() -> Result<()> {
    let context = TestContext::new("stream_commit_ongoing_flush_deletion");
    let mut table = test_table(
        &context,
        "stream_commit_ongoing_flush_deletion",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Step 1: Add some initial data and flush it to create a data file
    append_rows(&mut table, vec![test_row(1, "Initial", 20)])?;
    table.commit(1);
    flush_table_and_sync(&mut table, &mut event_completion_rx, 2).await?;

    // Step 2: Start a streaming transaction
    let xact_id = 1;
    table.append_in_stream_batch(test_row(2, "Stream", 21), xact_id)?;

    // Step 3: Delete a row from the committed data within the streaming transaction
    // This adds the deletion to committed_deletion_log
    table
        .delete_in_stream_batch(test_row(1, "Initial", 20), xact_id)
        .await;

    // Step 4: Flush the streaming transaction (creates disk slice)
    // The disk slice now contains the deletion remapping info
    let disk_slice =
        flush_stream_and_sync_no_apply(&mut table, &mut event_completion_rx, xact_id, Some(3))
            .await
            .expect("Disk slice should be present");

    // Step 5: Commit the streaming transaction
    // This processes deletions and adds them to committed_deletion_log
    // but the disk slice is still pending to be integrated
    table.commit_transaction_stream_impl(xact_id, 3)?;

    // Step 6: Apply the stream flush result
    // This adds the disk slice to snapshot_task for later processing
    table.apply_stream_flush_result(
        xact_id,
        disk_slice,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Step 7: Create a snapshot to trigger integrate_disk_slices
    // Without the fix, this would fail because deletions in committed_deletion_log
    // get remapped but not applied to batch_deletion_vector
    let created = table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::Skip,
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::Skip,
    });
    assert!(created, "Mooncake snapshot should be created");

    // This should succeed with the fix, but would fail without it
    sync_mooncake_snapshot(&mut table, &mut event_completion_rx).await;

    Ok(())
}

#[tokio::test]
async fn test_deletion_align_with_batch() -> Result<()> {
    let context = TestContext::new("deletion_align_with_batch");
    let mut table = test_table(
        &context,
        "deletion_align_with_batch",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // LSN 0-3: First streaming transaction
    let xact_id_0 = 0;
    table.append_in_stream_batch(test_row(0, "user", 0), xact_id_0)?; // LSN 0
    table.append_in_stream_batch(test_row(1, "user", 1), xact_id_0)?; // LSN 1
    table
        .delete_in_stream_batch(test_row(0, "user", 0), xact_id_0)
        .await; // LSN 2
    table.commit_transaction_stream_impl(xact_id_0, 3)?; // LSN 3

    // LSN 4-7: Non-streaming operations
    table.append(test_row(2, "user", 2))?; // LSN 4
    table.delete(test_row(1, "user", 1), 5).await; // LSN 5
    table.append(test_row(3, "user", 3))?; // LSN 6
    table.commit(7); // LSN 7 - CommitFlush

    // LSN 8-10: Second streaming transaction with flush
    let xact_id_1 = 1;
    table.append_in_stream_batch(test_row(4, "user", 4), xact_id_1)?; // LSN 8
    table.append_in_stream_batch(test_row(5, "user", 0), xact_id_1)?; // LSN 9
    commit_transaction_stream_and_sync(&mut table, &mut event_completion_rx, xact_id_1, 10).await; // LSN 10 - CommitFlush

    let created = table.try_create_mooncake_snapshot(SnapshotOption {
        uuid: uuid::Uuid::new_v4(),
        force_create: true,
        dump_snapshot: false,
        iceberg_snapshot_option: IcebergSnapshotOption::Skip,
        index_merge_option: MaintenanceOption::Skip,
        data_compaction_option: MaintenanceOption::Skip,
    });
    assert!(created, "Mooncake snapshot should be created");

    sync_mooncake_snapshot(&mut table, &mut event_completion_rx).await;

    Ok(())
}

#[tokio::test]
async fn test_disk_slice_write_failure() -> Result<()> {
    // Create a test context with an invalid directory to force write failure
    let temp_dir = TempDir::new().unwrap();
    let invalid_path = temp_dir.path().join("non_existent_directory");

    // Create table metadata with invalid path that will cause flush failure
    let table_metadata = Arc::new(TableMetadata {
        name: "test_table".to_string(),
        table_id: 1,
        schema: create_test_arrow_schema(),
        config: MooncakeTableConfig::default(),
        path: invalid_path, // This directory doesn't exist, causing write failures
    });

    let table_metadata_copy = table_metadata.clone();
    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_get_warehouse_location()
        .times(1)
        .returning(|| "".to_string());
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

    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, temp_dir.path());
    let wal_manager = WalManager::new(&wal_config);

    let mut table = MooncakeTable::new_with_table_manager(
        table_metadata,
        Box::new(mock_table_manager),
        create_test_object_storage_cache(&temp_dir),
        FileSystemAccessor::default_for_test(&temp_dir),
        wal_manager,
    )
    .await
    .unwrap();

    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Add some data to trigger a flush
    let row = test_row(1, "Alice", 20);
    table.append(row).unwrap();
    table.commit(100);

    // Attempt to flush - this should fail due to invalid directory
    table
        .flush(/*lsn=*/ 100, /*event_id=*/ uuid::Uuid::new_v4())
        .unwrap();

    // Wait for the flush result and verify it contains the expected error
    let flush_result = event_completion_rx.recv().await.unwrap();
    match flush_result {
        TableEvent::FlushResult {
            event_id: _,
            xact_id: _,
            flush_result,
        } => {
            match flush_result {
                Some(Err(e)) => {
                    // Verify the error contains information about the failed write
                    let error_msg = format!("{e:?}");
                    println!("error_msg: {error_msg}");
                    assert!(
                        error_msg.contains("No such file or directory")
                            || error_msg.contains("system cannot find the path"),
                        "Expected error about missing directory, got: {error_msg}"
                    );

                    // This error would cause the table handler to panic with "Fatal flush error"
                    // when it processes the FlushResult event in the table handler event loop
                }
                Some(Ok(_)) => {
                    panic!("Expected flush to fail, but it succeeded");
                }
                None => {
                    panic!("Expected flush error, but got None");
                }
            }
        }
        _ => {
            panic!("Expected FlushResult as first event, but got: {flush_result:?}");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_streaming_deletion_remap_sets_batch_deletion_vector() {
    let context = TestContext::new("streaming_deletion_remap_sets_batch_deletion_vector");
    let mut table = test_table(
        &context,
        "streaming_deletion_remap_sets_batch_deletion_vector",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id = 1;
    let flush_lsn = 1u64;

    // Append two rows into the streaming mem slice
    let row1 = test_row(1, "A", 20);
    let row2 = test_row(2, "B", 21);
    table.append_in_stream_batch(row1.clone(), xact_id).unwrap();
    table.append_in_stream_batch(row2.clone(), xact_id).unwrap();

    // Begin the stream flush (drains mem slice into stream_state.new_record_batches and adds a mem index)
    let disk_slice = flush_stream_and_sync_no_apply(
        &mut table,
        &mut event_completion_rx,
        xact_id,
        Some(flush_lsn),
    )
    .await
    .expect("Disk slice should be present");

    // Delete a row that belongs to the now-drained mem batches (it will be recorded as MemoryBatch)
    table.delete_in_stream_batch(row2.clone(), xact_id).await;

    // Apply the stream flush result BEFORE committing the transaction.
    // This must remap the MemoryBatch deletion into the new disk file and set the batch deletion vector.
    table.apply_stream_flush_result(
        xact_id,
        disk_slice,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Assert the in-memory stream state's flushed file deletion vector reflects the delete BEFORE commit.
    {
        let stream_state = table
            .transaction_stream_states
            .get(&xact_id)
            .expect("stream state should exist");
        let total_deleted: usize = stream_state
            .flushed_files
            .values()
            .map(|entry| entry.committed_deletion_vector.get_num_rows_deleted())
            .sum();
        assert_eq!(
            total_deleted, 1,
            "expected exactly one deleted row in batch deletion vector before commit"
        );
    }

    // Now commit the streaming transaction.
    table
        .commit_transaction_stream_impl(xact_id, flush_lsn + 1)
        .unwrap();

    // Create a snapshot and verify only the non-deleted row remains.
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

    let mut snapshot = table.snapshot.write().await;
    let SnapshotReadOutput {
        data_file_paths,
        puffin_cache_handles,
        position_deletes,
        deletion_vectors,
        ..
    } = snapshot.request_read().await.unwrap();

    verify_files_and_deletions(
        get_data_files_for_read(&data_file_paths).as_slice(),
        get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
        position_deletes,
        deletion_vectors,
        &[1],
    )
    .await;
}

#[tokio::test]
async fn test_streaming_commit_before_flush_finishes_sets_flush_lsn() -> Result<()> {
    let context = TestContext::new("streaming_commit_before_flush_finishes_sets_flush_lsn");
    let mut table = test_table(
        &context,
        "streaming_commit_before_flush_finishes_sets_flush_lsn",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id = 1u32;
    let commit_lsn = 100u64;

    // Append a row to streaming mem slice
    let row = test_row(1, "A", 20);
    table.append_in_stream_batch(row, xact_id)?;

    // Begin a streaming flush WITHOUT a writer LSN to simulate a periodic flush before commit
    let disk_slice = flush_stream_and_sync_no_apply(
        &mut table,
        &mut event_completion_rx,
        xact_id,
        Some(commit_lsn),
    )
    .await
    .expect("Disk slice should be present");

    // Commit the transaction while the flush is still pending
    table
        .commit_transaction_stream_impl(xact_id, commit_lsn)
        .unwrap();

    // Apply the flush after commit; this should set next flush LSN to commit_lsn
    table.apply_stream_flush_result(
        xact_id,
        disk_slice,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Create a snapshot to integrate the flush LSN into the current snapshot state
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

    // Verify the snapshot's flush LSN is set to the commit LSN
    let snapshot = table.snapshot.write().await;
    let status = snapshot.get_table_snapshot_states().unwrap();
    assert_eq!(status.flush_lsn, Some(commit_lsn));

    Ok(())
}

#[tokio::test]
async fn test_two_deletes_same_key_after_flush() -> Result<()> {
    let context = TestContext::new("test_two_deletes_same_key_after_flush");
    let mut table = test_table(
        &context,
        "test_two_deletes_same_key_after_flush",
        IdentityProp::SinglePrimitiveKey(0),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // write row 1 and flush
    let row1 = test_row(1, "A", 20);
    table.append(row1.clone()).unwrap();
    table.commit(100);
    flush_table_and_sync(&mut table, &mut event_completion_rx, 100)
        .await
        .unwrap();

    // update and insert row 1 and flush
    let row1_updated = test_row(1, "B", 21);
    table.delete(row1.clone(), 101).await;
    table.append(row1_updated.clone()).unwrap();
    table.commit(102);
    let _ = flush_table_and_sync_no_apply(&mut table, &mut event_completion_rx, 102)
        .await
        .unwrap();

    table.delete(row1.clone(), 103).await;
    table.commit(104);
    // Create a snapshot and verify all the rows are deleted.
    create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

    let mut snapshot = table.snapshot.write().await;
    let SnapshotReadOutput {
        data_file_paths,
        puffin_cache_handles,
        position_deletes,
        deletion_vectors,
        ..
    } = snapshot.request_read().await.unwrap();

    verify_files_and_deletions(
        get_data_files_for_read(&data_file_paths).as_slice(),
        get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
        position_deletes,
        deletion_vectors,
        &[],
    )
    .await;
    Ok(())
}

#[tokio::test]
async fn test_streaming_batch_id_assignment() -> Result<()> {
    let context = TestContext::new("streaming_batch_id_assignment");
    let mut table = test_table(&context, "lsn_table", IdentityProp::FullRow).await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id_1 = 1;
    let xact_id_2 = 2;

    // Start streaming transactions
    table.append_in_stream_batch(test_row(1, "A", 20), xact_id_1)?;
    table.append_in_stream_batch(test_row(2, "B", 21), xact_id_2)?;

    // Flush streaming transactions with different LSNs (can be out of order)
    let disk_slice_1 =
        flush_stream_and_sync_no_apply(&mut table, &mut event_completion_rx, xact_id_1, Some(50))
            .await
            .expect("Disk slice 1 should be present");
    table
        .commit_transaction_stream_impl(xact_id_1, /*lsn=*/ 50)
        .unwrap();

    let disk_slice_2 =
        flush_stream_and_sync_no_apply(&mut table, &mut event_completion_rx, xact_id_2, Some(100))
            .await
            .expect("Disk slice 2 should be present");
    table
        .commit_transaction_stream_impl(xact_id_2, /*lsn=*/ 100)
        .unwrap();

    // Verify both streaming LSNs are tracked
    assert!(table.ongoing_flush_lsns.contains_key(&100));
    assert!(table.ongoing_flush_lsns.contains_key(&50));
    assert_eq!(table.get_min_ongoing_flush_lsn(), 50);

    // Complete streaming flushes
    table.apply_stream_flush_result(
        xact_id_1,
        disk_slice_1,
        uuid::Uuid::new_v4(), /*placeholder*/
    );
    table.apply_stream_flush_result(
        xact_id_2,
        disk_slice_2,
        uuid::Uuid::new_v4(), /*placeholder*/
    );

    // Verify all pending flushes are cleared
    assert!(table.ongoing_flush_lsns.is_empty());
    assert_eq!(table.get_min_ongoing_flush_lsn(), u64::MAX);

    Ok(())
}

/// Testing scenario:
/// - Three ongoing flushes, two with flush LSN 13, another with flush LSN 16
/// - The completion order goes with 13, 16, 13
/// - Check whether flush LSN is 16 for iceberg snapshot
#[tokio::test]
async fn test_late_arriving_high_flush_lsn() {
    let context = TestContext::new("late_arriving_high_flush_lsn");
    let mut table = test_table(
        &context,
        "late_arriving_high_flush_lsn",
        IdentityProp::FullRow,
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let xact_id_1 = 1;
    let xact_id_2 = 2;
    let lsn_1 = 13;
    let lsn_2 = 16;

    let flush_event_ids = [
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
    ];

    // First flush operation.
    table
        .append_in_stream_batch(test_row(1, "A", 10), xact_id_1)
        .unwrap();
    table
        .flush_stream(xact_id_1, /*lsn=*/ None, flush_event_ids[0])
        .unwrap();

    // Second flush operation.
    table
        .commit_transaction_stream(xact_id_1, lsn_1, flush_event_ids[1])
        .unwrap();

    // Third flush operation.
    table
        .append_in_stream_batch(test_row(2, "B", 20), xact_id_2)
        .unwrap();
    table
        .commit_transaction_stream(xact_id_2, lsn_2, flush_event_ids[2])
        .unwrap();

    // Validate all streaming LSN are tracked.
    assert!(table.ongoing_flush_lsns.contains_key(&lsn_1));
    assert!(table.ongoing_flush_lsns.contains_key(&lsn_2));
    assert_eq!(table.get_min_ongoing_flush_lsn(), lsn_1);

    // Block wait all three flushes.
    let mut flush_results =
        get_flush_results(&mut event_completion_rx, /*expected_flushes=*/ 3).await;

    // Apply the flush result out of order.
    let disk_slice = flush_results.remove(&flush_event_ids[0]).unwrap();
    table.apply_stream_flush_result(xact_id_1, disk_slice, flush_event_ids[0]);

    let disk_slice = flush_results.remove(&flush_event_ids[2]).unwrap();
    table.apply_stream_flush_result(xact_id_2, disk_slice, flush_event_ids[2]);

    let disk_slice = flush_results.remove(&flush_event_ids[1]).unwrap();
    table.apply_stream_flush_result(xact_id_1, disk_slice, flush_event_ids[1]);

    // Create mooncake and iceberg snapshot, so we could check flush LSN.
    create_mooncake_and_persist_for_test(&mut table, &mut event_completion_rx).await;
    let iceberg_table_config = test_iceberg_table_config(&context, "late_arriving_high_flush_lsn");
    let iceberg_snapshot_fetcher = IcebergSnapshotFetcher::new(iceberg_table_config).unwrap();
    let flush_lsn = iceberg_snapshot_fetcher
        .get_flush_lsn()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(flush_lsn, lsn_2);
}

/// Testing scenario:
/// - Three ongoing flushes, two with flush LSN 13, another with flush LSN 16
/// - The completion order goes with 13, 16, 13
/// - Attempt to make mooncake and iceberg snapshot after LSN=16 flush finishes
#[tokio::test]
async fn test_out_of_order_flush_for_iceberg_snapshot() {
    let context = TestContext::new("out_of_order_flush_for_iceberg_snapshot");
    let mut table = test_table(
        &context,
        "out_of_order_flush_for_iceberg_snapshot",
        IdentityProp::FullRow,
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    // Start with a sync flush operation.
    let initial_lsn = 10;
    table.append(test_row(4, "D", 40)).unwrap();
    flush_table_and_sync(
        &mut table,
        &mut event_completion_rx,
        /*lsn=*/ initial_lsn,
    )
    .await
    .unwrap();

    // Now start out of order flush operations.
    let xact_id_1 = 1;
    let xact_id_2 = 2;
    let lsn_1 = 13;
    let lsn_2 = 16;

    let flush_event_ids = [
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
    ];

    // First flush operation.
    table
        .append_in_stream_batch(test_row(1, "A", 10), xact_id_1)
        .unwrap();
    table
        .flush_stream(xact_id_1, /*lsn=*/ None, flush_event_ids[0])
        .unwrap();

    // Second flush operation.
    table
        .commit_transaction_stream(xact_id_1, lsn_1, flush_event_ids[1])
        .unwrap();

    // Third flush operation.
    table
        .append_in_stream_batch(test_row(2, "B", 20), xact_id_2)
        .unwrap();
    table
        .commit_transaction_stream(xact_id_2, lsn_2, flush_event_ids[2])
        .unwrap();

    // Validate all streaming LSN are tracked.
    assert!(table.ongoing_flush_lsns.contains_key(&lsn_1));
    assert!(table.ongoing_flush_lsns.contains_key(&lsn_2));
    assert_eq!(table.get_min_ongoing_flush_lsn(), lsn_1);

    // Block wait all three flushes.
    let mut flush_results =
        get_flush_results(&mut event_completion_rx, /*expected_flushes=*/ 3).await;

    // Apply the flush result out of order.
    let disk_slice = flush_results.remove(&flush_event_ids[0]).unwrap();
    table.apply_stream_flush_result(xact_id_1, disk_slice, flush_event_ids[0]);

    let disk_slice = flush_results.remove(&flush_event_ids[2]).unwrap();
    table.apply_stream_flush_result(xact_id_2, disk_slice, flush_event_ids[2]);

    // Create mooncake and iceberg snapshot, so we could check flush LSN.
    create_mooncake_and_persist_for_test(&mut table, &mut event_completion_rx).await;
    let iceberg_table_config = test_iceberg_table_config(&context, "late_arriving_high_flush_lsn");
    let iceberg_snapshot_fetcher = IcebergSnapshotFetcher::new(iceberg_table_config).unwrap();
    let flush_lsn = iceberg_snapshot_fetcher.get_flush_lsn().await.unwrap();
    assert!(flush_lsn.is_none());
}

#[tokio::test]
async fn test_delete_if_exists() -> Result<()> {
    let context = TestContext::new("test_delete_if_exists");
    let mut table = test_table(
        &context,
        "test_delete_if_exists",
        IdentityProp::Keys(vec![0]),
    )
    .await;
    let (event_completion_tx, mut event_completion_rx) = mpsc::channel(100);
    table.register_table_notify(event_completion_tx).await;

    let row1 = test_row(1, "A", 20);
    table.append(row1.clone()).unwrap();
    table.commit(100);
    flush_table_and_sync(&mut table, &mut event_completion_rx, 100)
        .await
        .unwrap();

    // delete a row that doesn't exist
    let row2 = test_row(2, "B", 21);
    table.delete_if_exists(row2.clone(), 101).await;
    table.commit(102);
    flush_table_and_sync(&mut table, &mut event_completion_rx, 102)
        .await
        .unwrap();

    // create a snapshot and verify the row is not deleted
    {
        create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            position_deletes,
            deletion_vectors,
            ..
        } = snapshot.request_read().await.unwrap();

        verify_files_and_deletions(
            get_data_files_for_read(&data_file_paths).as_slice(),
            get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
            position_deletes,
            deletion_vectors,
            &[1],
        )
        .await;
        drop(snapshot);
    }

    // upsert a row
    let row3 = test_row(1, "C", 22);
    table.delete_if_exists(row3.clone(), 103).await;
    table.append(row3.clone()).unwrap();
    table.commit(104);
    flush_table_and_sync(&mut table, &mut event_completion_rx, 104)
        .await
        .unwrap();

    // create a snapshot and verify the row is upserted
    {
        create_mooncake_snapshot_for_test(&mut table, &mut event_completion_rx).await;

        let mut snapshot = table.snapshot.write().await;
        let SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            position_deletes,
            deletion_vectors,
            ..
        } = snapshot.request_read().await.unwrap();

        verify_files_and_deletions(
            get_data_files_for_read(&data_file_paths).as_slice(),
            get_deletion_puffin_files_for_read(&puffin_cache_handles).as_slice(),
            position_deletes,
            deletion_vectors,
            &[1],
        )
        .await;
        drop(snapshot);
    }

    Ok(())
}
