use crate::storage::iceberg::catalog_test_utils::create_test_table_schema;
use crate::storage::iceberg::rest_catalog::RestCatalog;
/// A RAII-style test guard, which creates namespace ident, table ident at construction, and deletes at destruction.
use crate::storage::iceberg::rest_catalog_test_utils::*;
use iceberg::{Catalog, NamespaceIdent, Result, TableIdent};
use std::collections::HashMap;

pub(crate) struct RestCatalogTestGuard {
    pub(crate) namespace: NamespaceIdent,
    pub(crate) table: Option<TableIdent>,
}

impl RestCatalogTestGuard {
    pub(crate) async fn new(namespace: String, table: Option<String>) -> Result<Self> {
        let rest_catalog_config = default_rest_catalog_config();
        let accessor_config = default_accessor_config();
        let catalog = RestCatalog::new(
            rest_catalog_config,
            accessor_config,
            create_test_table_schema().unwrap(),
        )
        .await
        .unwrap();
        let ns_ident = NamespaceIdent::new(namespace);
        catalog.create_namespace(&ns_ident, HashMap::new()).await?;
        let table_ident = if let Some(t) = table {
            let tc = default_table_creation(t.clone());
            catalog.create_table(&ns_ident, tc).await?;
            Some(TableIdent {
                namespace: ns_ident.clone(),
                name: t,
            })
        } else {
            None
        };
        Ok(Self {
            namespace: ns_ident,
            table: table_ident,
        })
    }
}

impl Drop for RestCatalogTestGuard {
    fn drop(&mut self) {
        let table = self.table.take();
        let rest_catalog_config = default_rest_catalog_config();
        let accessor_config = default_accessor_config();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let catalog = RestCatalog::new(
                    rest_catalog_config,
                    accessor_config,
                    create_test_table_schema().unwrap(),
                )
                .await
                .unwrap();
                if let Some(t) = table {
                    catalog.drop_table(&t).await.unwrap();
                }
                catalog.drop_namespace(&self.namespace).await.unwrap();
            });
        })
    }
}
