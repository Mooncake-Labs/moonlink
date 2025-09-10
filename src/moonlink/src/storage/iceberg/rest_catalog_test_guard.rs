use crate::storage::iceberg::catalog_test_utils::create_test_table_schema;
use crate::storage::iceberg::rest_catalog::RestCatalog;
/// A RAII-style test guard, which creates namespace ident, table ident at construction, and deletes at destruction.
use crate::storage::iceberg::rest_catalog_test_utils::*;
use iceberg::{Catalog, NamespaceIdent, Result, TableIdent};
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) struct RestCatalogTestGuard {
    pub(crate) catalog: Arc<RwLock<RestCatalog>>,
    pub(crate) namespace_idents: Option<Vec<NamespaceIdent>>,
    pub(crate) tables_idents: Option<Vec<TableIdent>>,
}

impl RestCatalogTestGuard {
    pub(crate) async fn new() -> Result<Self> {
        let rest_catalog_config = default_rest_catalog_config();
        let accessor_config = default_accessor_config();
        let catalog = RestCatalog::new(
            rest_catalog_config,
            accessor_config,
            create_test_table_schema().unwrap(),
        )
        .await
        .expect("error: fail to create rest catlog");
        Ok(Self {
            catalog: Arc::new(RwLock::new(catalog)),
            namespace_idents: None,
            tables_idents: None,
        })
    }
}

impl Drop for RestCatalogTestGuard {
    fn drop(&mut self) {
        let catalog = self.catalog.clone();
        let table_idents = self.tables_idents.take();
        let namespace_idents = self.namespace_idents.take();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let writer = catalog.write().await;

                if let Some(t_idents) = table_idents {
                    for t in t_idents {
                        writer
                            .catalog
                            .drop_table(&t)
                            .await
                            .expect("error: fail to drop the table on drop method");
                    }
                }

                if let Some(ns_idents) = namespace_idents {
                    for ns_ident in ns_idents {
                        writer
                            .catalog
                            .drop_namespace(&ns_ident)
                            .await
                            .expect("error: fail to drop the namespace on drop method");
                    }
                }
            });
        })
    }
}
