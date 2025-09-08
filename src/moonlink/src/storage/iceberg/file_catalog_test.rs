use crate::storage::filesystem::accessor_config::AccessorConfig;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::gcs_test_utils;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::test_guard::TestGuard as GcsTestGuard;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::s3_test_utils;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::test_guard::TestGuard as S3TestGuard;
use crate::storage::filesystem::storage_config::StorageConfig;
use crate::storage::iceberg::catalog_test_impl::*;
use crate::storage::iceberg::file_catalog::FileCatalog;
use crate::storage::iceberg::file_catalog::NAMESPACE_INDICATOR_OBJECT_NAME;
use crate::storage::iceberg::file_catalog_test_utils::*;
#[cfg(feature = "storage-gcs")]
use crate::storage::iceberg::gcs_test_utils as iceberg_gcs_test_utils;
#[cfg(feature = "storage-s3")]
use crate::storage::iceberg::s3_test_utils as iceberg_s3_test_utils;

use iceberg::{Catalog, NamespaceIdent};
use std::collections::HashMap;
use tempfile::TempDir;

/// Test util function to get subdirectories and folders under the given folder.
/// NOTICE: directories names and file names returned are absolute path, not relative path.
async fn get_entities_under_directory(
    directory: &str,
) -> (Vec<String> /*directories*/, Vec<String> /*files*/) {
    let mut dirs = vec![];
    let mut files = vec![];

    let mut stream = tokio::fs::read_dir(directory).await.unwrap();
    while let Some(entry) = stream.next_entry().await.unwrap() {
        let metadata = entry.metadata().await.unwrap();
        if metadata.is_dir() {
            dirs.push(entry.path().to_str().unwrap().to_string());
        } else if metadata.is_file() {
            files.push(entry.path().to_str().unwrap().to_string());
        }
    }
    (dirs, files)
}

/// Test cases for iceberg table structure on local filesystem.
///
/// TODO(hjiang): Add the same hierarchy test for S3 catalog.
#[tokio::test]
async fn test_local_iceberg_table_creation() {
    const NAMESPACE: &str = "default";

    let temp_dir = TempDir::new().unwrap();
    let warehouse_path = temp_dir.path().to_str().unwrap();
    let storage_config = StorageConfig::FileSystem {
        root_directory: warehouse_path.to_string(),
        atomic_write_dir: None,
    };
    let catalog = FileCatalog::new(
        AccessorConfig::new_with_storage_config(storage_config),
        get_test_schema(),
    )
    .unwrap();
    let namespace_ident = NamespaceIdent::from_strs([NAMESPACE]).unwrap();
    let _ = catalog
        .create_namespace(&namespace_ident, /*properties=*/ HashMap::new())
        .await
        .unwrap();

    // Expected directory for iceberg namespace.
    let mut namespace_dir_pathbuf = temp_dir.path().to_path_buf();
    namespace_dir_pathbuf.push(NAMESPACE);
    let namespace_filepath = namespace_dir_pathbuf.to_str().unwrap().to_string();

    // Expected indicator file which marks an iceberg namespace.
    let mut indicator_pathbuf = namespace_dir_pathbuf.clone();
    indicator_pathbuf.push(NAMESPACE_INDICATOR_OBJECT_NAME);
    let indicator_filepath = indicator_pathbuf.to_str().unwrap().to_string();

    // The iceberg table should be placed under the temporary directory.
    let (dirs, files) = get_entities_under_directory(warehouse_path).await;
    assert_eq!(dirs, vec![namespace_filepath.clone()]);
    assert!(files.is_empty());

    // Check namespaces folder structure.
    let (dirs, files) = get_entities_under_directory(&namespace_filepath).await;
    assert!(dirs.is_empty());
    assert_eq!(files, vec![indicator_filepath.clone()]);
}

// Create S3 catalog with local minio deployment and a random bucket.
#[cfg(feature = "storage-s3")]
async fn create_s3_catalog() -> (FileCatalog, S3TestGuard) {
    let (bucket_name, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
    let test_guard = S3TestGuard::new(bucket_name).await;
    let file_catalog = iceberg_s3_test_utils::create_test_s3_catalog(&warehouse_uri);
    (file_catalog, test_guard)
}
// Create GCS catalog with local fake gcs deployment and a random bucket.
#[cfg(feature = "storage-gcs")]
async fn create_gcs_catalog() -> (FileCatalog, GcsTestGuard) {
    let (bucket_name, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
    let test_guard = GcsTestGuard::new(bucket_name).await;
    let file_catalog = iceberg_gcs_test_utils::create_gcs_catalog(&warehouse_uri);
    (file_catalog, test_guard)
}

/// ==============================
/// Test with features
/// ==============================
///
/// Namespace operations test.
#[tokio::test]
async fn test_catalog_namespace_operations_filesystem() {
    let temp_dir = TempDir::new().unwrap();
    let catalog = create_test_file_catalog(&temp_dir, get_test_schema());
    test_catalog_namespace_operations_impl(&catalog).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_catalog_namespace_operations_s3() {
    let (catalog, _test_guard) = create_s3_catalog().await;
    test_catalog_namespace_operations_impl(&catalog).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_catalog_namespace_operations_gcs() {
    let (catalog, _test_guard) = create_gcs_catalog().await;
    test_catalog_namespace_operations_impl(&catalog).await
}

/// Table operations test.
#[tokio::test]
async fn test_catalog_table_operations_filesystem() {
    let temp_dir = TempDir::new().unwrap();
    let catalog = create_test_file_catalog(&temp_dir, get_test_schema());
    test_catalog_table_operations_impl(&catalog).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_catalog_table_operations_s3() {
    let (catalog, _test_guard) = create_s3_catalog().await;
    test_catalog_table_operations_impl(&catalog).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_catalog_table_operations_gcs() {
    let (catalog, _test_guard) = create_gcs_catalog().await;
    test_catalog_table_operations_impl(&catalog).await
}

/// List operation test.
#[tokio::test]
async fn test_list_operation_filesystem() {
    let temp_dir = TempDir::new().unwrap();
    let catalog = create_test_file_catalog(&temp_dir, get_test_schema());
    test_list_operation_impl(&catalog).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_list_operation_s3() {
    let (catalog, _test_guard) = create_s3_catalog().await;
    test_list_operation_impl(&catalog).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_list_operation_gcs() {
    let (catalog, _test_guard) = create_gcs_catalog().await;
    test_list_operation_impl(&catalog).await
}

/// Update table test.
#[tokio::test]
async fn test_update_table_filesystem() {
    let temp_dir = TempDir::new().unwrap();
    let mut catalog = create_test_file_catalog(&temp_dir, get_test_schema());
    test_update_table_impl(&mut catalog).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_update_table_s3() {
    let (mut catalog, _test_guard) = create_s3_catalog().await;
    test_update_table_impl(&mut catalog).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_update_table_gcs() {
    let (mut catalog, _test_guard) = create_gcs_catalog().await;
    test_update_table_impl(&mut catalog).await;
}

/// Update schema test.
#[tokio::test]
async fn test_update_schema() {
    let temp_dir = TempDir::new().unwrap();
    let mut catalog = create_test_file_catalog(&temp_dir, get_test_schema());
    test_update_schema_impl(&mut catalog).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_update_schema_s3() {
    let (mut catalog, _test_guard) = create_s3_catalog().await;
    test_update_schema_impl(&mut catalog).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_update_schema_gcs() {
    let (mut catalog, _test_guard) = create_gcs_catalog().await;
    test_update_schema_impl(&mut catalog).await;
}

/// Requirement check failure.
#[tokio::test]
async fn test_update_table_with_requirement_check_failed() {
    let temp_dir = TempDir::new().unwrap();
    let catalog = create_test_file_catalog(&temp_dir, get_test_schema());
    test_update_table_with_requirement_check_failed_impl(&catalog).await;
}
