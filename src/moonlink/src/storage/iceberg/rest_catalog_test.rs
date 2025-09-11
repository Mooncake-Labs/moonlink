use std::collections::HashMap;

use crate::storage::iceberg::catalog_test_impl::*;
use crate::storage::iceberg::catalog_test_utils::*;
use crate::storage::iceberg::rest_catalog::RestCatalog;
use crate::storage::iceberg::rest_catalog_test_guard::RestCatalogTestGuard;
use crate::storage::iceberg::rest_catalog_test_utils::*;
use iceberg::{Catalog, NamespaceIdent, TableIdent};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_table_operation() {
    let mut guard = RestCatalogTestGuard::new().await.unwrap();

    // create a namespace
    let ns_name = get_random_string();
    let ns_ident = NamespaceIdent::new(ns_name);
    guard
        .catalog
        .create_namespace(&ns_ident, HashMap::new())
        .await
        .unwrap();

    guard.record_namespace(ns_ident.clone());

    // create a table
    let table_name = get_random_string();
    let creation = default_table_creation(table_name.clone());
    guard
        .catalog
        .create_table(&ns_ident, creation)
        .await
        .unwrap();
    let table_ident = TableIdent::new(ns_ident.clone(), table_name);

    guard.record_table(table_ident.clone());

    // check if the table exist
    assert!(guard.catalog.table_exists(&table_ident).await.unwrap());
    // check the list table method
    assert_eq!(
        guard.catalog.list_tables(&ns_ident).await.unwrap(),
        vec![table_ident.clone()]
    );

    // drop the table
    guard.catalog.drop_table(&table_ident).await.unwrap();

    guard.remove_table(&table_ident);

    assert!(!guard.catalog.table_exists(&table_ident).await.unwrap());
    assert_eq!(guard.catalog.list_tables(&ns_ident).await.unwrap(), vec![]);

    guard.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_namespace_operation() {
    let mut guard = RestCatalogTestGuard::new().await.unwrap();

    // create a namespace
    let ns_name_1 = get_random_string();
    let ns_name_2 = get_random_string();
    let ns_ident_parent = NamespaceIdent::new(ns_name_1.clone());
    let ns_ident = NamespaceIdent::from_strs(vec![ns_name_1, ns_name_2]).unwrap();
    let expected_namespace = guard
        .catalog
        .create_namespace(&ns_ident, HashMap::new())
        .await
        .unwrap();
    guard.record_namespace(ns_ident.clone());

    assert_eq!(
        guard.catalog.get_namespace(&ns_ident).await.unwrap(),
        expected_namespace
    );
    assert!(guard.catalog.namespace_exists(&ns_ident).await.unwrap());

    assert_eq!(
        guard
            .catalog
            .list_namespaces(Some(&ns_ident_parent))
            .await
            .unwrap(),
        vec![ns_ident.clone()]
    );

    guard.catalog.drop_namespace(&ns_ident).await.unwrap();

    guard.remove_namespace(&ns_ident);

    assert!(!guard.catalog.namespace_exists(&ns_ident).await.unwrap());

    guard.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_table_with_requirement_check_failed() {
    let namespace = get_random_string();
    let table = get_random_string();
    let catalog = RestCatalog::new(
        default_rest_catalog_config(),
        default_accessor_config(),
        create_test_table_schema().unwrap(),
    )
    .await
    .unwrap();
    test_update_table_with_requirement_check_failed_impl(
        &catalog,
        namespace.clone(),
        table.clone(),
    )
    .await;
    catalog
        .drop_table(&TableIdent::new(
            NamespaceIdent::new(namespace.clone()),
            table.clone(),
        ))
        .await
        .unwrap();
    catalog
        .drop_namespace(&NamespaceIdent::new(namespace.clone()))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_table() {
    let namespace = get_random_string();
    let table = get_random_string();
    let mut catalog = RestCatalog::new(
        default_rest_catalog_config(),
        default_accessor_config(),
        create_test_table_schema().unwrap(),
    )
    .await
    .unwrap();
    test_update_table_impl(&mut catalog, namespace.clone(), table.clone()).await;
    catalog
        .drop_table(&TableIdent::new(
            NamespaceIdent::new(namespace.clone()),
            table.clone(),
        ))
        .await
        .unwrap();
    catalog
        .drop_namespace(&NamespaceIdent::new(namespace.clone()))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_update_schema() {
    let namespace = get_random_string();
    let table = get_random_string();
    let mut catalog = RestCatalog::new(
        default_rest_catalog_config(),
        default_accessor_config(),
        create_test_table_schema().unwrap(),
    )
    .await
    .unwrap();
    test_update_schema_impl(&mut catalog, namespace.to_string(), table.to_string()).await;

    // Clean up test.
    catalog
        .drop_table(&TableIdent::new(
            NamespaceIdent::new(namespace.clone()),
            table.clone(),
        ))
        .await
        .unwrap();
    catalog
        .drop_namespace(&NamespaceIdent::new(namespace.clone()))
        .await
        .unwrap();
}
