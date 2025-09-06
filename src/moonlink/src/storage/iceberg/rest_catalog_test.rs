use crate::storage::iceberg::moonlink_catalog::PuffinWrite;
use crate::storage::iceberg::rest_catalog::RestCatalog;
use crate::storage::iceberg::rest_catalog_test_guard::RestCatalogTestGuard;
use crate::storage::iceberg::rest_catalog_test_utils::*;
use crate::storage::iceberg::table_commit_proxy::TableCommitProxy;
use iceberg::spec::{SnapshotReference, SnapshotRetention, MAIN_BRANCH};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg::{TableRequirement, TableUpdate};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_table() {
    let namespace = get_random_string();
    let table = get_random_string();
    let mut guard = RestCatalogTestGuard::new(namespace.clone(), None)
        .await
        .unwrap_or_else(|_| panic!("Rest catalog test guard creation fail, namespace={namespace}"));
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let catalog = RestCatalog::new(rest_catalog_config, accessor_config)
        .await
        .expect("Catalog creation fail");
    let namespace = NamespaceIdent::new(namespace);
    let table_creation = default_table_creation(table.clone());
    let table_name = table_creation.name.clone();
    catalog
        .create_table(&namespace, table_creation)
        .await
        .unwrap_or_else(|_| panic!("Table creation fail, namespace={namespace} table={table}"));
    guard.table = Some(TableIdent::new(namespace, table_name));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_drop_table() {
    let namespace = get_random_string();
    let table = get_random_string();
    let mut guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
        .await
        .unwrap_or_else(|_| panic!("Rest catalog test guard creation fail, namespace={namespace}"));
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let catalog = RestCatalog::new(rest_catalog_config, accessor_config)
        .await
        .expect("Catalog creation fail");
    let table_ident = guard.table.clone().unwrap();
    guard.table = None;
    assert!(catalog
        .table_exists(&table_ident)
        .await
        .unwrap_or_else(|_| panic!(
            "Table exist function fail, namespace={namespace} table={table}"
        )));
    catalog
        .drop_table(&table_ident)
        .await
        .unwrap_or_else(|_| panic!("Table creation fail, namespace={namespace} table={table}"));
    assert!(!catalog
        .table_exists(&table_ident)
        .await
        .unwrap_or_else(|_| panic!(
            "Table exist function fail, namespace={namespace} table={table}"
        )));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_table_exists() {
    let namespace = get_random_string();
    let table = get_random_string();
    let guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
        .await
        .unwrap_or_else(|_| panic!("Rest catalog test guard creation fail, namespace={namespace}"));
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let catalog = RestCatalog::new(rest_catalog_config, accessor_config)
        .await
        .expect("Catalog creation fail");

    // Check table existence.
    let table_ident = guard.table.clone().unwrap();
    assert!(catalog
        .table_exists(&table_ident)
        .await
        .unwrap_or_else(|_| panic!(
            "Table exist function fail, namespace={namespace} table={table}"
        )));

    // List tables and validate.
    let tables = catalog.list_tables(table_ident.namespace()).await.unwrap();
    assert_eq!(tables, vec![table_ident]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_load_table() {
    let namespace = get_random_string();
    let table = get_random_string();
    let guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
        .await
        .unwrap_or_else(|_| panic!("Rest catalog test guard creation fail, namespace={namespace}"));
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let catalog = RestCatalog::new(rest_catalog_config, accessor_config)
        .await
        .expect("Catalog creation fail");
    let table_ident = guard.table.clone().unwrap();
    let result = catalog.load_table(&table_ident).await.unwrap_or_else(|_| {
        panic!("Load table function fail, namespace={namespace} table={table}")
    });
    let result_table_ident = result.identifier().clone();
    assert_eq!(table_ident, result_table_ident);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_table_with_requirement_check_failed() {
    let namespace = get_random_string();
    let table = get_random_string();
    let guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
        .await
        .unwrap_or_else(|_| panic!("Rest catalog test guard creation fail, namespace={namespace}"));
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let catalog = RestCatalog::new(rest_catalog_config, accessor_config)
        .await
        .expect("Catalog creation fail");

    let table_ident = guard.table.clone().unwrap();
    let table_commit_proxy = TableCommitProxy {
        ident: table_ident.clone(),
        requirements: vec![TableRequirement::UuidMatch {
            uuid: Uuid::new_v4(),
        }],
        updates: vec![],
    };
    let table_commit = table_commit_proxy.take_as_table_commit();

    let res = catalog.update_table(table_commit).await;
    assert!(res.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_table() {
    let namespace = get_random_string();
    let table = get_random_string();
    let guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
        .await
        .unwrap_or_else(|_| panic!("Rest catalog test guard creation fail, namespace={namespace}"));
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let mut catalog = RestCatalog::new(rest_catalog_config, accessor_config)
        .await
        .expect("Catalog creation fail");

    let table_ident = guard.table.clone().unwrap();
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
                    NamespaceIdent::new(namespace.clone()).to_url_string(),
                    table
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
        },
    ]);

    let table_commit_proxy = TableCommitProxy {
        ident: table_ident.clone(),
        requirements: vec![],
        updates: table_updates,
    };
    let table_commit = table_commit_proxy.take_as_table_commit();

    let table_obj = catalog
        .update_table(table_commit)
        .await
        .unwrap_or_else(|_| panic!("Update table fail, namespace={namespace} table={table}"));
    catalog.clear_puffin_metadata();

    let table_metadata = table_obj.metadata();
    assert_eq!(table_obj.identifier(), &table_ident);
    assert_eq!(table_metadata.current_snapshot_id(), Some(1));
}
