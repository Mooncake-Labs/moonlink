use super::test_utils::*;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::mooncake_table_config::MooncakeTableConfig;
use crate::storage::wal::test_utils::WAL_TEST_TABLE_ID;
use crate::storage::wal::WalManager;
use crate::storage::MockTableManager;
use crate::storage::MooncakeTable;
use crate::storage::PersistenceResult;
use crate::ObjectStorageCache;
use crate::TableEventManager;
use crate::WalConfig;

use iceberg::{Error as IcebergError, ErrorKind};
use tempfile::tempdir;

use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_iceberg_snapshot_failure_mock_test() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config =
        MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
    let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
        name: "table_name".to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config: mooncake_table_config.clone(),
        path: temp_dir.path().to_path_buf(),
        identity: crate::row::IdentityProp::Keys(vec![0]),
    });

    let mooncake_table_metadata_copy = mooncake_table_metadata.clone();
    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_get_warehouse_location()
        .times(1)
        .returning(|| "".to_string());
    mock_table_manager
        .expect_load_snapshot_from_table()
        .times(1)
        .returning(move || {
            let table_metadata_copy = mooncake_table_metadata_copy.clone();
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

    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, temp_dir.path());
    let wal_manager = WalManager::new(&wal_config);

    let mooncake_table = MooncakeTable::new_with_table_manager(
        mooncake_table_metadata,
        Box::new(mock_table_manager),
        ObjectStorageCache::default_for_test(&temp_dir),
        FileSystemAccessor::default_for_test(&temp_dir),
        wal_manager,
    )
    .await
    .unwrap();
    let mut env = TestEnvironment::new_with_mooncake_table(temp_dir, mooncake_table).await;

    // Append rows to trigger mooncake and iceberg snapshot.
    env.append_row(
        /*id=*/ 1, /*name=*/ "Alice", /*age=*/ 10, /*lsn=*/ 5,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 10).await;

    // Initiate snapshot and block wait its completion, check whether error status is correctly propagated.
    let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 10).await;
    let res = TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 1).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_iceberg_drop_table_failure_mock_test() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config =
        MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
    let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
        name: "table_name".to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config: mooncake_table_config.clone(),
        path: temp_dir.path().to_path_buf(),
        identity: crate::row::IdentityProp::Keys(vec![0]),
    });

    let mooncake_table_metadata_copy = mooncake_table_metadata.clone();
    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_get_warehouse_location()
        .times(1)
        .returning(|| "".to_string());
    mock_table_manager
        .expect_load_snapshot_from_table()
        .times(1)
        .returning(move || {
            let table_metadata_copy = mooncake_table_metadata_copy.clone();
            Box::pin(async move {
                Ok((
                    /*next_file_id=*/ 0,
                    MooncakeSnapshot::new(table_metadata_copy),
                ))
            })
        });
    mock_table_manager
        .expect_drop_table()
        .times(1)
        .returning(|| {
            Box::pin(async move {
                Err(IcebergError::new(
                    ErrorKind::Unexpected,
                    "Intended error for unit test",
                ))
            })
        });

    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, temp_dir.path());
    let wal_manager = WalManager::new(&wal_config);

    let mooncake_table = MooncakeTable::new_with_table_manager(
        mooncake_table_metadata,
        Box::new(mock_table_manager),
        ObjectStorageCache::default_for_test(&temp_dir),
        FileSystemAccessor::default_for_test(&temp_dir),
        wal_manager,
    )
    .await
    .unwrap();
    let mut env = TestEnvironment::new_with_mooncake_table(temp_dir, mooncake_table).await;

    // Drop table and block wait its completion, check whether error status is correctly propagated.
    let res = env.drop_table().await;
    assert!(res.is_err());
}

/// Testing scenario: persist completed index merge result to iceberg fails.
#[tokio::test]
async fn test_force_index_merge_with_failed_iceberg_persistence() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config =
        MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
    let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
        name: "table_name".to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config: mooncake_table_config.clone(),
        path: temp_dir.path().to_path_buf(),
        identity: crate::row::IdentityProp::Keys(vec![0]),
    });

    let mooncake_table_metadata_copy = mooncake_table_metadata.clone();
    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_get_warehouse_location()
        .times(1)
        .returning(|| "".to_string());
    mock_table_manager
        .expect_load_snapshot_from_table()
        .once()
        .returning(move || {
            let table_metadata_copy = mooncake_table_metadata_copy.clone();
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
        .returning(|snapshot_payload: IcebergSnapshotPayload, _| {
            Box::pin(async move {
                let mock_persistence_result = PersistenceResult {
                    remote_data_files: snapshot_payload.import_payload.data_files.clone(),
                    remote_file_indices: snapshot_payload.import_payload.file_indices.clone(),
                    puffin_blob_ref: HashMap::new(),
                };
                Ok(mock_persistence_result)
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

    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, temp_dir.path());
    let wal_manager = WalManager::new(&wal_config);

    let mooncake_table = MooncakeTable::new_with_table_manager(
        mooncake_table_metadata,
        Box::new(mock_table_manager),
        ObjectStorageCache::default_for_test(&temp_dir),
        FileSystemAccessor::default_for_test(&temp_dir),
        wal_manager,
    )
    .await
    .unwrap();
    let mut env = TestEnvironment::new_with_mooncake_table(temp_dir, mooncake_table).await;

    // Append one row, commit, and flush.
    env.append_row(
        /*id=*/ 0, /*name=*/ "Alice", /*age=*/ 10, /*lsn=*/ 10,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 20).await;
    env.flush_table(/*lsn=*/ 30).await;

    // Append another row, commit, and flush.
    env.append_row(
        /*id=*/ 1, /*name=*/ "BoB", /*age=*/ 20, /*lsn=*/ 40,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 50).await;
    env.flush_table(/*lsn=*/ 50).await;

    // Request a force index merge.
    let res = env.force_index_merge_and_sync().await;
    assert!(res.is_err());
}

/// Testing scenario: persist completed data compaction result to iceberg fails.
#[tokio::test]
async fn test_force_data_compaction_with_failed_iceberg_persistence() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config =
        MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
    let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
        name: "table_name".to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config: mooncake_table_config.clone(),
        path: temp_dir.path().to_path_buf(),
        identity: crate::row::IdentityProp::Keys(vec![0]),
    });

    let mooncake_table_metadata_copy = mooncake_table_metadata.clone();
    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_get_warehouse_location()
        .times(1)
        .returning(|| "".to_string());
    mock_table_manager
        .expect_load_snapshot_from_table()
        .once()
        .returning(move || {
            let table_metadata_copy = mooncake_table_metadata_copy.clone();
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
        .returning(|snapshot_payload: IcebergSnapshotPayload, _| {
            Box::pin(async move {
                let mock_persistence_result = PersistenceResult {
                    remote_data_files: snapshot_payload.import_payload.data_files.clone(),
                    remote_file_indices: snapshot_payload.import_payload.file_indices.clone(),
                    puffin_blob_ref: HashMap::new(),
                };
                Ok(mock_persistence_result)
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

    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, temp_dir.path());
    let wal_manager = WalManager::new(&wal_config);

    let mooncake_table = MooncakeTable::new_with_table_manager(
        mooncake_table_metadata,
        Box::new(mock_table_manager),
        ObjectStorageCache::default_for_test(&temp_dir),
        FileSystemAccessor::default_for_test(&temp_dir),
        wal_manager,
    )
    .await
    .unwrap();
    let mut env = TestEnvironment::new_with_mooncake_table(temp_dir, mooncake_table).await;

    // Append one row, commit, and flush.
    env.append_row(
        /*id=*/ 0, /*name=*/ "Alice", /*age=*/ 10, /*lsn=*/ 10,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 20).await;
    env.flush_table(/*lsn=*/ 30).await;

    // Append another row, commit, and flush.
    env.append_row(
        /*id=*/ 1, /*name=*/ "BoB", /*age=*/ 20, /*lsn=*/ 40,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 50).await;
    env.flush_table(/*lsn=*/ 50).await;

    // Request a force index merge.
    let res = env.force_data_compaction_and_sync().await;
    assert!(res.is_err());
}

/// Testing scenario: persist completed full compaction result to iceberg fails.
#[tokio::test]
async fn test_force_full_compaction_with_failed_iceberg_persistence() {
    let temp_dir = tempdir().unwrap();
    let mooncake_table_config =
        MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
    let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
        name: "table_name".to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config: mooncake_table_config.clone(),
        path: temp_dir.path().to_path_buf(),
        identity: crate::row::IdentityProp::Keys(vec![0]),
    });

    let mooncake_table_metadata_copy = mooncake_table_metadata.clone();
    let mut mock_table_manager = MockTableManager::new();
    mock_table_manager
        .expect_get_warehouse_location()
        .times(1)
        .returning(|| "".to_string());
    mock_table_manager
        .expect_load_snapshot_from_table()
        .once()
        .returning(move || {
            let table_metadata_copy = mooncake_table_metadata_copy.clone();
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
        .returning(|snapshot_payload: IcebergSnapshotPayload, _| {
            Box::pin(async move {
                let mock_persistence_result = PersistenceResult {
                    remote_data_files: snapshot_payload.import_payload.data_files.clone(),
                    remote_file_indices: snapshot_payload.import_payload.file_indices.clone(),
                    puffin_blob_ref: HashMap::new(),
                };
                Ok(mock_persistence_result)
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

    let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, temp_dir.path());
    let wal_manager = WalManager::new(&wal_config);

    let mooncake_table = MooncakeTable::new_with_table_manager(
        mooncake_table_metadata,
        Box::new(mock_table_manager),
        ObjectStorageCache::default_for_test(&temp_dir),
        FileSystemAccessor::default_for_test(&temp_dir),
        wal_manager,
    )
    .await
    .unwrap();
    let mut env = TestEnvironment::new_with_mooncake_table(temp_dir, mooncake_table).await;

    // Append one row, commit, and flush.
    env.append_row(
        /*id=*/ 0, /*name=*/ "Alice", /*age=*/ 10, /*lsn=*/ 10,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 20).await;
    env.flush_table(/*lsn=*/ 30).await;

    // Append another row, commit, and flush.
    env.append_row(
        /*id=*/ 1, /*name=*/ "BoB", /*age=*/ 20, /*lsn=*/ 40,
        /*xact_id=*/ None,
    )
    .await;
    env.commit(/*lsn=*/ 50).await;
    env.flush_table(/*lsn=*/ 50).await;

    // Request a force index merge.
    let res = env.force_full_maintenance_and_sync().await;
    assert!(res.is_err());
}
