use crate::storage::iceberg::rest_catalog::RestCatalog;
use crate::storage::iceberg::rest_catalog_test_guard::RestCatalogTestGuard;
use crate::storage::iceberg::rest_catalog_test_utils::*;
use crate::to_set;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use std::collections::HashMap;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_namespaces_returns_empty_vector() {
    let config = default_rest_catalog_config();
    let catalog = RestCatalog::new(config)
        .await
        .expect("Catalog creation fail");
    assert_eq!(catalog.list_namespaces(None).await.unwrap(), vec![]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_namespace() {
    let namespace = get_random_string();
    let config = default_rest_catalog_config();
    let catalog = RestCatalog::new(config)
        .await
        .expect("Catalog creation fail");
    let namespace_ident = NamespaceIdent::new(namespace.clone());
    catalog
        .create_namespace(&namespace_ident, /*properties=*/ HashMap::new())
        .await
        .unwrap_or_else(|_| panic!("Namespace creation failed, namespace={namespace}"));
    let _ = catalog.drop_namespace(&namespace_ident).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_namespaces_returns_single_namespace() {
    let namespace = get_random_string();
    let config = default_rest_catalog_config();
    let namespace_ident = NamespaceIdent::new(namespace.clone());
    let catalog = RestCatalog::new(config)
        .await
        .expect("Catalog creation fail");
    catalog
        .create_namespace(&namespace_ident, /*properties=*/ HashMap::new())
        .await
        .unwrap_or_else(|_| panic!("Namespace creation failed, namespace={namespace}"));
    assert_eq!(
        catalog.list_namespaces(None).await.unwrap(),
        vec![namespace_ident.clone()]
    );
    let _ = catalog.drop_namespace(&namespace_ident).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_namespaces_returns_multiple_namespaces() {
    let namespace_1 = get_random_string();
    let namespace_2 = get_random_string();
    let config = default_rest_catalog_config();
    let namespace_ident_1 = NamespaceIdent::new(namespace_1.clone());
    let namespace_ident_2 = NamespaceIdent::new(namespace_2.clone());
    let catalog = RestCatalog::new(config)
        .await
        .expect("Catalog creation fail");
    create_namespaces(&catalog, &vec![&namespace_ident_1, &namespace_ident_2]).await;
    assert_eq!(
        to_set(catalog.list_namespaces(None).await.unwrap()),
        to_set(vec![namespace_ident_1.clone(), namespace_ident_2.clone()])
    );
    let _ = catalog.drop_namespace(&namespace_ident_1).await;
    let _ = catalog.drop_namespace(&namespace_ident_2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_namespaces_returns_only_top_level_namespaces() {
    let namespace_1 = get_random_string();
    let namespace_2_1 = get_random_string();
    let namespace_2_2 = get_random_string();
    let namespace_3 = get_random_string();
    let config = default_rest_catalog_config();
    let namespace_ident_1 = NamespaceIdent::new(namespace_1.clone());
    let namespace_ident_2 = NamespaceIdent::from_strs(vec![namespace_2_1, namespace_2_2]).unwrap();
    let namespace_ident_3 = NamespaceIdent::new(namespace_3.clone());

    let catalog = RestCatalog::new(config)
        .await
        .expect("Catalog creation fail");
    create_namespaces(
        &catalog,
        &vec![&namespace_ident_1, &namespace_ident_2, &namespace_ident_3],
    )
    .await;
    assert_eq!(
        to_set(catalog.list_namespaces(None).await.unwrap()),
        to_set(vec![namespace_ident_1.clone(), namespace_ident_3.clone()])
    );
    let _ = catalog.drop_namespace(&namespace_ident_1).await;
    let _ = catalog.drop_namespace(&namespace_ident_2).await;
    let _ = catalog.drop_namespace(&namespace_ident_3).await;
}
