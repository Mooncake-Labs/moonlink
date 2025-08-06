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
use crate::storage::iceberg::catalog_test_utils;
use crate::storage::iceberg::file_catalog::FileCatalog;
use crate::storage::iceberg::file_catalog::NAMESPACE_INDICATOR_OBJECT_NAME;
use crate::storage::iceberg::file_catalog_test_utils::*;
#[cfg(feature = "storage-gcs")]
use crate::storage::iceberg::gcs_test_utils as iceberg_gcs_test_utils;
use crate::storage::iceberg::moonlink_catalog::PuffinWrite;
use crate::storage::iceberg::moonlink_catalog::SchemaUpdate;
#[cfg(feature = "storage-s3")]
use crate::storage::iceberg::s3_test_utils as iceberg_s3_test_utils;
use crate::storage::iceberg::table_commit_proxy::TableCommitProxy;

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

use iceberg::spec::{SnapshotReference, SnapshotRetention, MAIN_BRANCH};
use iceberg::Result as IcebergResult;
use iceberg::{
    Catalog, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableRequirement, TableUpdate,
};
use uuid::Uuid;

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

// Test util function to create a new table.
async fn create_test_table(catalog: &FileCatalog) -> IcebergResult<()> {
    // Define namespace and table.
    let namespace = NamespaceIdent::from_strs(["default"])?;
    let table_name = "test_table".to_string();

    let schema = get_test_schema();
    let table_creation = TableCreation::builder()
        .name(table_name.clone())
        .location(format!(
            "{}/{}/{}",
            catalog.get_warehouse_location(),
            namespace.to_url_string(),
            table_name
        ))
        .schema(schema.clone())
        .build();

    catalog
        .create_namespace(&namespace, /*properties=*/ HashMap::new())
        .await?;
    catalog.create_table(&namespace, table_creation).await?;

    Ok(())
}

async fn test_catalog_namespace_operations_impl(catalog: FileCatalog) -> IcebergResult<()> {
    let namespace = NamespaceIdent::from_strs(vec!["default", "ns"])?;

    // Ensure namespace does not exist.
    assert!(!catalog.namespace_exists(&namespace).await?);

    // Create parent namespace.
    catalog
        .create_namespace(
            &NamespaceIdent::from_strs(vec!["default"]).unwrap(),
            /*properties=*/ HashMap::new(),
        )
        .await?;

    // Create namespace and check.
    catalog
        .create_namespace(&namespace, /*properties=*/ HashMap::new())
        .await?;
    assert!(catalog.namespace_exists(&namespace).await?);

    // Get the namespace and check.
    let ns = catalog.get_namespace(&namespace).await?;
    assert_eq!(ns.name(), &namespace);

    // Drop the namespace and check.
    catalog.drop_namespace(&namespace).await?;
    assert!(!catalog.namespace_exists(&namespace).await?);

    Ok(())
}

async fn test_catalog_table_operations_impl(catalog: FileCatalog) -> IcebergResult<()> {
    // Define namespace and table.
    let namespace = NamespaceIdent::from_strs(vec!["default"])?;
    let table_name = "test_table".to_string();
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

    // Ensure table does not exist.
    let table_already_exists = catalog.table_exists(&table_ident).await?;
    assert!(!table_already_exists,);

    // TODO(hjiang): Add testcase to check list table here.
    create_test_table(&catalog).await?;
    assert!(catalog.table_exists(&table_ident).await?);

    let tables = catalog.list_tables(&namespace).await?;
    assert_eq!(tables.len(), 1);
    assert!(tables.contains(&table_ident));

    // Load table and check.
    let table = catalog.load_table(&table_ident).await?;
    let expected_schema = get_test_schema();
    assert_eq!(table.identifier(), &table_ident,);
    assert_eq!(*table.metadata().current_schema().as_ref(), expected_schema,);

    // Drop the table and check.
    catalog.drop_table(&table_ident).await?;
    let table_already_exists = catalog.table_exists(&table_ident).await?;
    assert!(!table_already_exists, "Table should not exist after drop");

    Ok(())
}

async fn test_list_operation_impl(catalog: FileCatalog) -> IcebergResult<()> {
    // List namespaces with non-existent parent namespace.
    let res = catalog
        .list_namespaces(Some(
            &NamespaceIdent::from_strs(["non-existent-ns"]).unwrap(),
        ))
        .await;
    assert!(res.is_err(),);
    let err = res.err().unwrap();
    assert_eq!(err.kind(), iceberg::ErrorKind::NamespaceNotFound,);

    // List tables with non-existent parent namespace.
    let res = catalog
        .list_tables(&NamespaceIdent::from_strs(["non-existent-ns"]).unwrap())
        .await;
    assert!(res.is_err(),);
    let err = res.err().unwrap();
    assert_eq!(err.kind(), iceberg::ErrorKind::NamespaceNotFound,);

    // Create default namespace.
    let default_namespace = NamespaceIdent::from_strs(["default"])?;
    catalog
        .create_namespace(&default_namespace, /*properties=*/ HashMap::new())
        .await?;

    // Create two children namespaces under default namespace.
    let child_namespace_1 = NamespaceIdent::from_strs(["default", "child1"])?;
    catalog
        .create_namespace(&child_namespace_1, /*properties=*/ HashMap::new())
        .await?;
    let child_namespace_2 = NamespaceIdent::from_strs(["default", "child2"])?;
    catalog
        .create_namespace(&child_namespace_2, /*properties=*/ HashMap::new())
        .await?;

    // Create two tables under default namespace.
    let table_creation_1 =
        catalog_test_utils::create_test_table_creation(&default_namespace, "child_table_1")?;
    catalog
        .create_table(&default_namespace, table_creation_1)
        .await?;

    let table_creation_2 =
        catalog_test_utils::create_test_table_creation(&default_namespace, "child_table_2")?;
    catalog
        .create_table(&default_namespace, table_creation_2)
        .await?;

    // List default namespace and check.
    let res = catalog.list_namespaces(/*parent=*/ None).await?;
    assert_eq!(res.len(), 1,);
    assert_eq!(res[0].to_url_string(), "default");

    // List namespaces under default namespace and check.
    let res = catalog
        .list_namespaces(Some(&NamespaceIdent::from_strs(["default"]).unwrap()))
        .await?;
    assert_eq!(res.len(), 2,);
    assert!(
        res.contains(&child_namespace_1),
        "Expects children namespace {child_namespace_1:?}, but actually {res:?}"
    );
    assert!(
        res.contains(&child_namespace_2),
        "Expects children namespace {child_namespace_2:?}, but actually {res:?}"
    );

    // List tables under default namespace and check.
    let child_table_1 = TableIdent::new(default_namespace.clone(), "child_table_1".to_string());
    let child_table_2 = TableIdent::new(default_namespace.clone(), "child_table_2".to_string());

    let res = catalog
        .list_tables(&NamespaceIdent::from_strs(["default"]).unwrap())
        .await?;
    assert_eq!(
        res.len(),
        2,
        "Expect two children tables, actually there're {res:?}"
    );
    assert!(
        res.contains(&child_table_1),
        "Expects children table {child_table_1:?}, but actually {res:?}"
    );
    assert!(
        res.contains(&child_table_2),
        "Expects children table {child_table_2:?}, but actually {res:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_update_table_with_requirement_check_failed() {
    let temp_dir = TempDir::new().unwrap();
    let catalog = create_test_file_catalog(&temp_dir, get_test_schema());
    create_test_table(&catalog).await.unwrap();

    let namespace = NamespaceIdent::from_strs(["default"]).unwrap();
    let table_name = "test_table".to_string();
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());
    catalog.load_metadata(&table_ident).await.unwrap();

    let table_commit_proxy = TableCommitProxy {
        ident: table_ident.clone(),
        requirements: vec![TableRequirement::UuidMatch {
            uuid: Uuid::new_v4(),
        }],
        updates: vec![],
    };
    let table_commit =
        unsafe { std::mem::transmute::<TableCommitProxy, TableCommit>(table_commit_proxy) };

    let res = catalog.update_table(table_commit).await;
    assert!(res.is_err());
}

async fn test_update_table_impl(mut catalog: FileCatalog) -> IcebergResult<()> {
    create_test_table(&catalog).await?;

    let namespace = NamespaceIdent::from_strs(["default"])?;
    let table_name = "test_table".to_string();
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());
    catalog.load_metadata(&table_ident).await?;

    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let mut table_updates = vec![];
    table_updates.append(&mut vec![
        TableUpdate::AddSnapshot {
            snapshot: iceberg::spec::Snapshot::builder()
                .with_snapshot_id(1)
                .with_sequence_number(1)
                .with_timestamp_ms(millis as i64)
                .with_schema_id(0)
                .with_manifest_list(format!(
                    "s3://{}/{}/snap-8161620281254644995-0-01966b87-6e93-7bc1-9e12-f1980d9737d3.avro",
                    namespace.to_url_string(),
                    table_name
                ))
                .with_parent_snapshot_id(None)
                .with_summary(iceberg::spec::Summary {
                    operation: iceberg::spec::Operation::Append,
                    additional_properties: HashMap::new(),
                })
                .build(),
        },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        }
    ]);

    let table_commit_proxy = TableCommitProxy {
        ident: table_ident.clone(),
        requirements: vec![],
        updates: table_updates,
    };
    let table_commit =
        unsafe { std::mem::transmute::<TableCommitProxy, TableCommit>(table_commit_proxy) };

    // Check table metadata.
    let table = catalog.update_table(table_commit).await?;
    catalog.clear_puffin_metadata();

    let table_metadata = table.metadata();
    assert_eq!(**table_metadata.current_schema(), get_test_schema(),);
    assert_eq!(table.identifier(), &table_ident,);
    assert_eq!(table_metadata.current_snapshot_id(), Some(1),);

    Ok(())
}

async fn test_update_schema_impl(mut catalog: FileCatalog) {
    create_test_table(&catalog).await.unwrap();

    let namespace_ident = NamespaceIdent::from_strs(["default"]).unwrap();
    let table_name = "test_table".to_string();
    let table_ident = TableIdent::new(namespace_ident, table_name);

    let new_schema = get_updated_test_schema();
    let new_schema_id = new_schema.schema_id();
    catalog
        .update_table_schema(new_schema.clone(), table_ident.clone())
        .await
        .unwrap();

    // Load table metadata to check schema.
    let (_, table_metadata) = catalog.load_metadata(&table_ident).await.unwrap();
    let table_schema_id = table_metadata.current_schema_id();
    assert_eq!(table_schema_id, new_schema_id);

    let table_schema = table_metadata.current_schema();
    assert_eq!(**table_schema, new_schema);
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
    test_catalog_namespace_operations_impl(catalog)
        .await
        .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_catalog_namespace_operations_s3() -> IcebergResult<()> {
    let (catalog, _test_guard) = create_s3_catalog().await;
    test_catalog_namespace_operations_impl(catalog).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_catalog_namespace_operations_gcs() -> IcebergResult<()> {
    let (catalog, _test_guard) = create_gcs_catalog().await;
    test_catalog_namespace_operations_impl(catalog).await
}

/// Table operations test.
#[tokio::test]
async fn test_catalog_table_operations_filesystem() {
    let temp_dir = TempDir::new().unwrap();
    let catalog = create_test_file_catalog(&temp_dir, get_test_schema());
    test_catalog_table_operations_impl(catalog).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_catalog_table_operations_s3() -> IcebergResult<()> {
    let (catalog, _test_guard) = create_s3_catalog().await;
    test_catalog_table_operations_impl(catalog).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_catalog_table_operations_gcs() -> IcebergResult<()> {
    let (catalog, _test_guard) = create_gcs_catalog().await;
    test_catalog_table_operations_impl(catalog).await
}

/// List operation test.
#[tokio::test]
async fn test_list_operation_filesystem() {
    let temp_dir = TempDir::new().unwrap();
    let catalog = create_test_file_catalog(&temp_dir, get_test_schema());
    test_list_operation_impl(catalog).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_list_operation_s3() -> IcebergResult<()> {
    let (catalog, _test_guard) = create_s3_catalog().await;
    test_list_operation_impl(catalog).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_list_operation_gcs() -> IcebergResult<()> {
    let (catalog, _test_guard) = create_gcs_catalog().await;
    test_list_operation_impl(catalog).await
}

/// Update table test.
#[tokio::test]
async fn test_update_table_filesystem() {
    let temp_dir = TempDir::new().unwrap();
    let catalog = create_test_file_catalog(&temp_dir, get_test_schema());
    test_update_table_impl(catalog).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_update_table_s3() -> IcebergResult<()> {
    let (catalog, _test_guard) = create_s3_catalog().await;
    create_test_table(&catalog).await?;
    test_update_table_impl(catalog).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_update_table_gcs() -> IcebergResult<()> {
    let (catalog, _test_guard) = create_gcs_catalog().await;
    create_test_table(&catalog).await?;
    test_update_table_impl(catalog).await
}

/// Update schema test.
#[tokio::test]
async fn test_update_schema() {
    let temp_dir = TempDir::new().unwrap();
    let catalog = create_test_file_catalog(&temp_dir, get_test_schema());
    test_update_schema_impl(catalog).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-s3")]
async fn test_update_schema_s3() {
    let (catalog, _test_guard) = create_s3_catalog().await;
    create_test_table(&catalog).await.unwrap();
    test_update_schema_impl(catalog).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "storage-gcs")]
async fn test_update_schema_gcs() {
    let (catalog, _test_guard) = create_gcs_catalog().await;
    create_test_table(&catalog).await.unwrap();
    test_update_schema_impl(catalog).await;
}
