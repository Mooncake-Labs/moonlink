use crate::storage::iceberg::rest_catalog::RestCatalog;
use crate::storage::iceberg::rest_catalog_test_guard::RestCatalogTestGuard;
use crate::storage::iceberg::rest_catalog_test_utils::*;
use iceberg::{Catalog, NamespaceIdent, TableIdent};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_table() {
    let namespace = get_random_string();
    let table = get_random_string();
    let mut guard = RestCatalogTestGuard::new(namespace.clone(), None)
        .await
        .unwrap_or_else(|_| panic!("Rest catalog test guard creation fail, namespace={namespace}"));
    let config = default_rest_catalog_config();
    let catalog = RestCatalog::new(config)
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
    let config = default_rest_catalog_config();
    let catalog = RestCatalog::new(config)
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
    let config = default_rest_catalog_config();
    let catalog = RestCatalog::new(config)
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
    let config = default_rest_catalog_config();
    let catalog = RestCatalog::new(config)
        .await
        .expect("Catalog creation fail");
    let table_ident = guard.table.clone().unwrap();
    let result = catalog.load_table(&table_ident).await.unwrap_or_else(|_| {
        panic!("Load table function fail, namespace={namespace} table={table}")
    });
    let result_table_ident = result.identifier().clone();
    assert_eq!(table_ident, result_table_ident);
}
