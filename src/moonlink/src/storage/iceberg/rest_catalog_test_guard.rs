use crate::storage::iceberg::catalog_test_utils::create_test_table_schema;
use crate::storage::iceberg::rest_catalog::RestCatalog;
/// A RAII-style test guard, which creates namespace ident, table ident at construction, and deletes at destruction.
use crate::storage::iceberg::rest_catalog_test_utils::*;
use iceberg::{Catalog, NamespaceIdent, TableIdent};

pub(crate) struct RestCatalogTestGuard {
    pub(crate) catalog: RestCatalog,
    pub(crate) namespace_idents: Option<Vec<NamespaceIdent>>,
    pub(crate) tables_idents: Option<Vec<TableIdent>>,
}

impl RestCatalogTestGuard {
    pub(crate) async fn new() -> Self {
        let rest_catalog_config = default_rest_catalog_config();
        let accessor_config = default_accessor_config();
        let catalog = RestCatalog::new(
            rest_catalog_config,
            accessor_config,
            create_test_table_schema().unwrap(),
        )
        .await
        .unwrap();
        Self {
            catalog,
            namespace_idents: None,
            tables_idents: None,
        }
    }

    pub(crate) fn record_table(&mut self, table_ident: TableIdent) {
        self.tables_idents
            .get_or_insert(Vec::new())
            .push(table_ident);
    }

    pub(crate) fn remove_table(&mut self, table_ident: &TableIdent) {
        if let Some(vec) = self.tables_idents.as_mut() {
            vec.retain(|t| t != table_ident);
        }
    }

    pub(crate) fn record_namespace(&mut self, ns_ident: NamespaceIdent) {
        self.namespace_idents
            .get_or_insert(Vec::new())
            .push(ns_ident);
    }

    pub(crate) fn remove_namespace(&mut self, ns_ident: &NamespaceIdent) {
        if let Some(vec) = self.namespace_idents.as_mut() {
            vec.retain(|ns| ns != ns_ident);
        }
    }
}

impl Drop for RestCatalogTestGuard {
    fn drop(&mut self) {
        let tables = self.tables_idents.take();
        let namespaces = self.namespace_idents.take();

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                if let Some(tables) = tables {
                    for t in tables {
                        self.catalog.drop_table(&t).await.unwrap();
                    }
                }

                if let Some(namespaces) = namespaces {
                    for ns in namespaces {
                        self.catalog.drop_namespace(&ns).await.unwrap();
                    }
                }
            });
        })
    }
}
