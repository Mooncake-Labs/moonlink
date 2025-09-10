use std::collections::HashMap;

use crate::storage::iceberg::catalog_test_impl::*;
use crate::storage::iceberg::catalog_test_utils::*;
use crate::storage::iceberg::rest_catalog::RestCatalog;
use crate::storage::iceberg::rest_catalog_test_guard::RestCatalogTestGuard;
use crate::storage::iceberg::rest_catalog_test_utils::*;
use iceberg::{Catalog, NamespaceIdent, TableIdent};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_table_operation() {
    let mut guard = RestCatalogTestGuard::new()
        .await
        .expect("error: fail to create testguard");
    let catalog = guard.catalog.clone();
    let writer = catalog.write().await;

    // create a namespace
    let ns_name = get_random_string();
    let ns_ident = NamespaceIdent::new(ns_name);
    writer
        .catalog
        .create_namespace(&ns_ident, HashMap::new())
        .await
        .expect("error: fail to create a namespace");
    guard
        .namespace_idents
        .get_or_insert(Vec::new())
        .push(ns_ident.clone());

    // create a table
    let table_name = get_random_string();
    let creation = default_table_creation(table_name.clone());
    writer
        .catalog
        .create_table(&ns_ident, creation)
        .await
        .expect("error: fail to create an table");
    let table_ident = TableIdent::new(ns_ident.clone(), table_name);
    guard
        .tables_idents
        .get_or_insert(Vec::new())
        .push(table_ident.clone());

    // check if the table exist
    assert!(writer
        .table_exists(&table_ident)
        .await
        .expect("error: fail to check whether the table exist"));
    // check the list table method
    assert_eq!(
        writer
            .list_tables(&ns_ident)
            .await
            .expect("error: fail to list the tables"),
        vec![table_ident.clone()]
    );

    // drop the table
    writer
        .catalog
        .drop_table(&table_ident)
        .await
        .expect("error: fail to drop the table");

    if let Some(vec) = guard.tables_idents.as_mut() {
        vec.retain(|t| t != &table_ident);
    }

    assert!(!writer
        .table_exists(&table_ident)
        .await
        .expect("error: fail to check whether the table exist"));
    assert_eq!(
        writer
            .list_tables(&ns_ident)
            .await
            .expect("error: fail to list the tables"),
        vec![]
    );

    drop(writer);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_namespace_operation() {
    let mut guard = RestCatalogTestGuard::new()
        .await
        .expect("error: fail to create testguard");
    let catalog = guard.catalog.clone();
    let writer = catalog.write().await;

    // create a namespace
    let ns_name = get_random_string();
    let ns_ident = NamespaceIdent::new(ns_name);
    let expected_namespace = writer
        .catalog
        .create_namespace(&ns_ident, HashMap::new())
        .await
        .expect("error: fail to create a namespace");
    guard
        .namespace_idents
        .get_or_insert(Vec::new())
        .push(ns_ident.clone());

    assert_eq!(
        writer
            .catalog
            .get_namespace(&ns_ident)
            .await
            .expect("error: fail to get namespace"),
        expected_namespace
    );
    assert!(writer
        .catalog
        .namespace_exists(&ns_ident)
        .await
        .expect("error: fail to check if the namespace exist"));

    assert_eq!(
        writer
            .catalog
            .list_namespaces(None)
            .await
            .expect("error: fail to list the namespaces"),
        vec![ns_ident.clone()]
    );

    writer
        .catalog
        .drop_namespace(&ns_ident)
        .await
        .expect("error: fail to drop the namespace");

    if let Some(vec) = guard.namespace_idents.as_mut() {
        vec.retain(|t| t != &ns_ident);
    }

    assert!(!writer
        .catalog
        .namespace_exists(&ns_ident)
        .await
        .expect("error: fail to check if the namespace exist"));

    assert_eq!(
        writer
            .catalog
            .list_namespaces(None)
            .await
            .expect("error: fail to list the namespaces"),
        vec![]
    );

    drop(writer);
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
