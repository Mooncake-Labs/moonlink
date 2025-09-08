use crate::storage::iceberg::catalog_test_utils;
use crate::storage::iceberg::file_catalog_test_utils::*;
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::table_commit_proxy::TableCommitProxy;

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use iceberg::spec::{SnapshotReference, SnapshotRetention, MAIN_BRANCH};
use iceberg::{
    NamespaceIdent, TableCommit, TableCreation, TableIdent, TableRequirement, TableUpdate,
};

/// This file contains testing logic which is general to all types of catalogs.
///
/// Test util function to create a new table.
pub(crate) async fn create_test_table(catalog: &dyn MoonlinkCatalog) {
    // Define namespace and table.
    let namespace = NamespaceIdent::from_strs(["default"]).unwrap();
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
        .await
        .unwrap();
    catalog
        .create_table(&namespace, table_creation)
        .await
        .unwrap();
}

pub(crate) async fn test_catalog_namespace_operations_impl(catalog: &dyn MoonlinkCatalog) {
    let namespace = NamespaceIdent::from_strs(vec!["default", "ns"]).unwrap();

    // Ensure namespace does not exist.
    assert!(!catalog.namespace_exists(&namespace).await.unwrap());

    // Create parent namespace.
    catalog
        .create_namespace(
            &NamespaceIdent::from_strs(vec!["default"]).unwrap(),
            /*properties=*/ HashMap::new(),
        )
        .await
        .unwrap();

    // Create namespace and check.
    catalog
        .create_namespace(&namespace, /*properties=*/ HashMap::new())
        .await
        .unwrap();
    assert!(catalog.namespace_exists(&namespace).await.unwrap());

    // Get the namespace and check.
    let ns = catalog.get_namespace(&namespace).await.unwrap();
    assert_eq!(ns.name(), &namespace);

    // Drop the namespace and check.
    catalog.drop_namespace(&namespace).await.unwrap();
    assert!(!catalog.namespace_exists(&namespace).await.unwrap());
}

pub(crate) async fn test_catalog_table_operations_impl(catalog: &dyn MoonlinkCatalog) {
    // Define namespace and table.
    let namespace = NamespaceIdent::from_strs(vec!["default"]).unwrap();
    let table_name = "test_table".to_string();
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

    // Ensure table does not exist.
    let table_already_exists = catalog.table_exists(&table_ident).await.unwrap();
    assert!(!table_already_exists,);

    // TODO(hjiang): Add testcase to check list table here.
    create_test_table(catalog).await;
    assert!(catalog.table_exists(&table_ident).await.unwrap());

    let tables = catalog.list_tables(&namespace).await.unwrap();
    assert_eq!(tables.len(), 1);
    assert!(tables.contains(&table_ident));

    // Load table and check.
    let table = catalog.load_table(&table_ident).await.unwrap();
    let expected_schema = get_test_schema();
    assert_eq!(table.identifier(), &table_ident,);
    assert_eq!(*table.metadata().current_schema().as_ref(), expected_schema,);

    // Drop the table and check.
    catalog.drop_table(&table_ident).await.unwrap();
    let table_already_exists = catalog.table_exists(&table_ident).await.unwrap();
    assert!(!table_already_exists, "Table should not exist after drop");
}

pub(crate) async fn test_list_operation_impl(catalog: &dyn MoonlinkCatalog) {
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
    let default_namespace = NamespaceIdent::from_strs(["default"]).unwrap();
    catalog
        .create_namespace(&default_namespace, /*properties=*/ HashMap::new())
        .await
        .unwrap();

    // Create two children namespaces under default namespace.
    let child_namespace_1 = NamespaceIdent::from_strs(["default", "child1"]).unwrap();
    catalog
        .create_namespace(&child_namespace_1, /*properties=*/ HashMap::new())
        .await
        .unwrap();
    let child_namespace_2 = NamespaceIdent::from_strs(["default", "child2"]).unwrap();
    catalog
        .create_namespace(&child_namespace_2, /*properties=*/ HashMap::new())
        .await
        .unwrap();

    // Create two tables under default namespace.
    let table_creation_1 =
        catalog_test_utils::create_test_table_creation(&default_namespace, "child_table_1")
            .unwrap();
    catalog
        .create_table(&default_namespace, table_creation_1)
        .await
        .unwrap();

    let table_creation_2 =
        catalog_test_utils::create_test_table_creation(&default_namespace, "child_table_2")
            .unwrap();
    catalog
        .create_table(&default_namespace, table_creation_2)
        .await
        .unwrap();

    // List default namespace and check.
    let res = catalog.list_namespaces(/*parent=*/ None).await.unwrap();
    assert_eq!(res.len(), 1,);
    assert_eq!(res[0].to_url_string(), "default");

    // List namespaces under default namespace and check.
    let res = catalog
        .list_namespaces(Some(&NamespaceIdent::from_strs(["default"]).unwrap()))
        .await
        .unwrap();
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
        .await
        .unwrap();
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
}

pub(crate) async fn test_update_table_impl(catalog: &mut dyn MoonlinkCatalog) {
    create_test_table(catalog).await;

    let namespace = NamespaceIdent::from_strs(["default"]).unwrap();
    let table_name = "test_table".to_string();
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());
    catalog.load_metadata(&table_ident).await.unwrap();

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
    let table = catalog.update_table(table_commit).await.unwrap();
    catalog.clear_puffin_metadata();

    let table_metadata = table.metadata();
    assert_eq!(**table_metadata.current_schema(), get_test_schema(),);
    assert_eq!(table.identifier(), &table_ident,);
    assert_eq!(table_metadata.current_snapshot_id(), Some(1),);
}

pub(crate) async fn test_update_schema_impl(catalog: &mut dyn MoonlinkCatalog) {
    create_test_table(catalog).await;

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

pub(crate) async fn test_update_table_with_requirement_check_failed_impl(
    catalog: &dyn MoonlinkCatalog,
) {
    create_test_table(catalog).await;

    let namespace = NamespaceIdent::from_strs(["default"]).unwrap();
    let table_name = "test_table".to_string();
    let table_ident = TableIdent::new(namespace.clone(), table_name.clone());
    catalog.load_metadata(&table_ident).await.unwrap();

    let table_commit_proxy = TableCommitProxy {
        ident: table_ident.clone(),
        requirements: vec![TableRequirement::UuidMatch {
            uuid: uuid::Uuid::new_v4(),
        }],
        updates: vec![],
    };
    let table_commit =
        unsafe { std::mem::transmute::<TableCommitProxy, TableCommit>(table_commit_proxy) };

    let res = catalog.update_table(table_commit).await;
    assert!(res.is_err());
}
