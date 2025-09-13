use crate::row::MoonlinkRow;
use crate::row::RowValue;
use crate::storage::iceberg::catalog_test_utils::create_test_table_schema;
use crate::storage::iceberg::iceberg_table_config::IcebergTableConfig;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::storage::iceberg::rest_catalog::RestCatalog;
use crate::storage::iceberg::rest_catalog_test_utils::*;
use crate::storage::iceberg::schema_utils::assert_is_same_schema;
use crate::storage::iceberg::table_manager::TableManager;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use crate::IcebergCatalogConfig;

use iceberg::arrow::arrow_schema_to_schema;
use iceberg::Catalog;
use iceberg::NamespaceIdent;
use iceberg::TableIdent;
use tempfile::tempdir;

/// Test util functions to create moonlink rows.
fn test_row_1() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(10),
    ])
}

/// Test util function to create moonlink row with updated schema with [`create_test_updated_arrow_schema`].
fn test_row_with_updated_schema() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(100),
        RowValue::ByteArray("new_string".as_bytes().to_vec()),
    ])
}

struct TestGuard {
    iceberg_table_config: IcebergTableConfig,
}
impl TestGuard {
    fn new(iceberg_table_config: IcebergTableConfig) -> Self {
        Self {
            iceberg_table_config,
        }
    }
    /// Test util function to drop the test namespace and table.
    async fn drop_namespace_and_table(iceberg_table_config: IcebergTableConfig) {
        let rest_catalog_config = match &iceberg_table_config.metadata_accessor_config {
            IcebergCatalogConfig::Rest {
                rest_catalog_config,
            } => rest_catalog_config.clone(),
            other => panic!("Expects to have rest catalog config, but receives {other:?}"),
        };
        let catalog = RestCatalog::new(
            rest_catalog_config,
            iceberg_table_config.data_accessor_config.clone(),
            create_test_table_schema().unwrap(),
        )
        .await
        .unwrap();

        let namespace_ident = NamespaceIdent::from_vec(iceberg_table_config.namespace).unwrap();
        let table_ident = TableIdent::new(
            namespace_ident.clone(),
            iceberg_table_config.table_name.clone(),
        );
        catalog.drop_table(&table_ident).await.unwrap();
        catalog.drop_namespace(&namespace_ident).await.unwrap();
    }
}
impl Drop for TestGuard {
    fn drop(&mut self) {
        let iceberg_table_config = self.iceberg_table_config.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                Self::drop_namespace_and_table(iceberg_table_config).await;
            });
        })
    }
}

/// This file test iceberg table manager integration with rest catalog.
///
/// ================================
/// Test update schema with update
/// ================================
///
/// Testing scenario: perform a table schema update when there's no table update.
async fn test_schema_update_with_no_table_write_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let local_table_directory = table_temp_dir.path().to_str().unwrap().to_string();
    let mooncake_table_metadata = create_test_table_metadata(local_table_directory.clone());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    // Append, commit, flush and persist.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache.clone(),
    )
    .await;

    let updated_mooncake_table_metadata =
        alter_table_and_persist_to_iceberg(&mut table, &mut notify_rx).await;

    // Now the iceberg table has been created, create an iceberg table manager and check table status.
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        updated_mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor,
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 0);
    assert_eq!(snapshot.flush_lsn, Some(0));
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());

    let loaded_table = iceberg_table_manager_for_load
        .iceberg_table
        .as_ref()
        .unwrap();
    let actual_schema = loaded_table.metadata().current_schema();
    let expected_schema =
        arrow_schema_to_schema(updated_mooncake_table_metadata.schema.as_ref()).unwrap();
    assert_is_same_schema(actual_schema.as_ref().clone(), expected_schema);

    // =======================================
    // Table write after schema update
    // =======================================
    //
    // Perform more data file with the new schema should go through with no issue.
    let row = test_row_with_updated_schema();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 20);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 20)
        .await
        .unwrap();

    // Create a mooncake and iceberg snapshot to reflect new data file changes.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Now the iceberg table has been created, create an iceberg table manager and check table status.
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        updated_mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor,
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2); // one data file, one file index
    assert_eq!(snapshot.flush_lsn, Some(20));
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(snapshot.indices.file_indices.len(), 1);

    let loaded_table = iceberg_table_manager_for_load
        .iceberg_table
        .as_ref()
        .unwrap();
    let actual_schema = loaded_table.metadata().current_schema();
    let expected_schema =
        arrow_schema_to_schema(updated_mooncake_table_metadata.schema.as_ref()).unwrap();
    assert_is_same_schema(actual_schema.as_ref().clone(), expected_schema);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_schema_update_with_no_table_write() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_rest_iceberg_table_config(&iceberg_temp_dir);
    let _guard = TestGuard::new(iceberg_table_config.clone());

    // Common testing logic.
    test_schema_update_with_no_table_write_impl(iceberg_table_config).await;
}

/// ================================
/// Test update schema
/// ================================
///
/// Testing scenario: perform schema update after a sync operation.
async fn test_schema_update_impl(iceberg_table_config: IcebergTableConfig) {
    // Local filesystem to store write-through cache.
    let table_temp_dir = tempdir().unwrap();
    let local_table_directory = table_temp_dir.path().to_str().unwrap().to_string();
    let mooncake_table_metadata = create_test_table_metadata(local_table_directory.clone());

    // Local filesystem to store read-through cache.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = create_test_object_storage_cache(&cache_temp_dir);

    // Append, commit, flush and persist.
    let (mut table, mut notify_rx) = create_mooncake_table_and_notify(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache.clone(),
    )
    .await;
    let row = test_row_1();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 10);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 10)
        .await
        .unwrap();

    // Perform an schema update.
    let updated_mooncake_table_metadata =
        alter_table_and_persist_to_iceberg(&mut table, &mut notify_rx).await;

    // Now the iceberg table has been created, create an iceberg table manager and check table status.
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        updated_mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor,
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 2);
    assert_eq!(snapshot.flush_lsn, Some(10));
    assert_eq!(snapshot.disk_files.len(), 1);
    assert_eq!(snapshot.indices.file_indices.len(), 1);

    let loaded_table = iceberg_table_manager_for_load
        .iceberg_table
        .as_ref()
        .unwrap();
    let actual_schema = loaded_table.metadata().current_schema();
    let expected_schema =
        arrow_schema_to_schema(updated_mooncake_table_metadata.schema.as_ref()).unwrap();
    assert_is_same_schema(actual_schema.as_ref().clone(), expected_schema);

    // Perform more data file with the new schema should go through with no issue.
    let row = test_row_with_updated_schema();
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 20);
    flush_table_and_sync(&mut table, &mut notify_rx, /*lsn=*/ 20)
        .await
        .unwrap();

    // Create a mooncake and iceberg snapshot to reflect new data file changes.
    create_mooncake_and_persist_for_test(&mut table, &mut notify_rx).await;

    // Check iceberg snapshot after write following schema update.
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager_for_load = IcebergTableManager::new(
        updated_mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        filesystem_accessor,
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (next_file_id, snapshot) = iceberg_table_manager_for_load
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(next_file_id, 4); // two data files, two file indices
    assert_eq!(snapshot.flush_lsn, Some(20));
    assert_eq!(snapshot.disk_files.len(), 2);
    assert_eq!(snapshot.indices.file_indices.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_schema_update() {
    // Local filesystem for iceberg.
    let iceberg_temp_dir = tempdir().unwrap();
    let iceberg_table_config = get_rest_iceberg_table_config(&iceberg_temp_dir);
    let _guard = TestGuard::new(iceberg_table_config.clone());

    // Common testing logic.
    test_schema_update_impl(iceberg_table_config).await;
}
