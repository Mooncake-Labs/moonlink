/// A RAII-style test guard, which creates namespace ident, table ident at construction, and deletes at destruction.
use crate::storage::iceberg::rest_catalog_test_utils::*;
use iceberg::{Catalog, NamespaceIdent, Result, TableIdent};
use std::collections::HashMap;

pub(crate) struct CatalogTestGuard<'a> {
    pub(crate) catalog: &'a dyn Catalog,
    pub(crate) namespace: NamespaceIdent,
    pub(crate) table: Option<TableIdent>,
}

impl<'a> CatalogTestGuard<'a> {
    pub(crate) async fn new(
        catalog: &'a dyn Catalog,
        namespace: String,
        table: Option<String>,
    ) -> Result<Self> {
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
            catalog,
            namespace: ns_ident,
            table: table_ident,
        })
    }
}

impl<'a> Drop for CatalogTestGuard<'a> {
    fn drop(&mut self) {
        let table = self.table.take();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                if let Some(t) = &table {
                    self.catalog.drop_table(t).await.unwrap();
                }
                self.catalog.drop_namespace(&self.namespace).await.unwrap();
            });
        })
    }
}
