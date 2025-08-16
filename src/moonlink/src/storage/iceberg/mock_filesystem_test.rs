use crate::storage::filesystem::accessor::base_filesystem_accessor::{
    BaseFileSystemAccess, MockBaseFileSystemAccess,
};
use crate::storage::iceberg::iceberg_table_config::IcebergTableConfig;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::Error;
use crate::{ObjectStorageCache, TableManager};

use iceberg::Error as IcebergError;

use std::sync::Arc;

/// Mock-based unit tests for iceberg table manager.
///
/// Test util function to create an iceberg table manager with the given filesystem accessor.
fn create_iceberg_table_manager_with_fs_accessor(
    filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
) -> IcebergTableManager {
    let temp_dir = tempfile::tempdir().unwrap();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);
    IcebergTableManager::new_with_filesystem_accessor(
        mooncake_table_metadata,
        object_storage_cache,
        filesystem_accessor,
        IcebergTableConfig::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn test_failed_iceberg_table_manager_drop_table() {
    let mut filesystem_accessor = MockBaseFileSystemAccess::new();
    filesystem_accessor
        .expect_remove_directory()
        .times(1)
        .returning(|_| {
            Box::pin(async move {
                Err(Error::from(IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    "Intended error for unit test",
                )))
            })
        });
    let mut iceberg_table_manager =
        create_iceberg_table_manager_with_fs_accessor(Arc::new(filesystem_accessor));
    let res = iceberg_table_manager.drop_table().await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_failed_recover_from_iceberg_table() {
    let mut filesystem_accessor = MockBaseFileSystemAccess::new();
    filesystem_accessor
        .expect_object_exists()
        .times(1)
        .returning(|_| {
            Box::pin(async move {
                Err(Error::from(IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    "Intended error for unit test",
                )))
            })
        });
    let mut iceberg_table_manager =
        create_iceberg_table_manager_with_fs_accessor(Arc::new(filesystem_accessor));
    let res = iceberg_table_manager.load_snapshot_from_table().await;
    assert!(res.is_err());
}
