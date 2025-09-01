use crate::storage::iceberg::iceberg_table_config::RestCatalogConfig;
use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::CatalogBuilder;
use iceberg::Result as IcebergResult;
use iceberg::{Catalog, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent};
use iceberg_catalog_rest::{
    RestCatalog as IcebergRestCatalog, RestCatalogBuilder as IcebergRestCatalogBuilder,
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use std::collections::HashMap;

#[derive(Debug)]
pub struct RestCatalog {
    pub(crate) catalog: IcebergRestCatalog,
}

impl RestCatalog {
    pub async fn new(mut config: RestCatalogConfig) -> Result<Self> {
        let builder = IcebergRestCatalogBuilder::default();
        config
            .props
            .insert(REST_CATALOG_PROP_URI.to_string(), config.uri);
        config
            .props
            .insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), config.warehouse);
        let catalog = builder.load(config.name, config.props).await?;
        Ok(Self { catalog })
    }
}

#[async_trait]
impl Catalog for RestCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        todo!("list namespaces is not supported");
    }
    async fn create_namespace(
        &self,
        namespace_ident: &iceberg::NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<iceberg::Namespace> {
        self.catalog
            .create_namespace(namespace_ident, properties)
            .await
    }

    async fn get_namespace(&self, _namespace_ident: &NamespaceIdent) -> IcebergResult<Namespace> {
        todo!("get namespace is not supported");
    }

    async fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<bool> {
        self.catalog.namespace_exists(namespace_ident).await
    }

    async fn drop_namespace(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<()> {
        self.catalog.drop_namespace(namespace_ident).await
    }

    async fn list_tables(
        &self,
        namespace_ident: &NamespaceIdent,
    ) -> IcebergResult<Vec<TableIdent>> {
        self.catalog.list_tables(namespace_ident).await
    }

    async fn update_namespace(
        &self,
        _namespace_ident: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        todo!("Update namespace is not supported");
    }

    async fn create_table(
        &self,
        namespace_ident: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        self.catalog.create_table(namespace_ident, creation).await
    }

    async fn load_table(&self, table_ident: &TableIdent) -> IcebergResult<Table> {
        self.catalog.load_table(table_ident).await
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.catalog.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.catalog.table_exists(table).await
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        todo!("rename table is not supported");
    }

    async fn update_table(&self, mut _commit: TableCommit) -> IcebergResult<Table> {
        todo!("update table is not supported");
    }

    async fn register_table(
        &self,
        __table: &TableIdent,
        _metadata_location: String,
    ) -> IcebergResult<Table> {
        todo!("register existing table is not supported")
    }
}
