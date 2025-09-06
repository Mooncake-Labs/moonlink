use std::collections::HashMap;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{Catalog, Namespace, NamespaceIdent};
use iceberg::{Result as IcebergResult, TableCommit, TableCreation, TableIdent};
use iceberg_catalog_glue::GlueCatalog as IcebergGlueCatalog;

#[derive(Debug)]
pub struct GlueCatalog {
    pub(crate) _catalog: IcebergGlueCatalog,
}

#[async_trait]
impl Catalog for GlueCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        todo!("list namespaces is not supported");
    }
    async fn create_namespace(
        &self,
        _namespace_ident: &iceberg::NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<iceberg::Namespace> {
        todo!("create namespace is not supported");
    }

    async fn get_namespace(&self, _namespace_ident: &NamespaceIdent) -> IcebergResult<Namespace> {
        todo!("get namespace is not supported");
    }

    async fn namespace_exists(&self, _namespace_ident: &NamespaceIdent) -> IcebergResult<bool> {
        todo!("namespace exists is not supported");
    }

    async fn drop_namespace(&self, _namespace_ident: &NamespaceIdent) -> IcebergResult<()> {
        todo!("drop namespace is not supported");
    }

    async fn list_tables(
        &self,
        _namespace_ident: &NamespaceIdent,
    ) -> IcebergResult<Vec<TableIdent>> {
        todo!("list tables is not supported");
    }

    async fn update_namespace(
        &self,
        _namespace_ident: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        todo!("update namespace is not supported");
    }

    async fn create_table(
        &self,
        _namespace_ident: &NamespaceIdent,
        _creation: TableCreation,
    ) -> IcebergResult<Table> {
        todo!("create table is not supported");
    }

    async fn load_table(&self, _table_ident: &TableIdent) -> IcebergResult<Table> {
        todo!("load table is not supported");
    }

    async fn drop_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        todo!("drop table is not supported");
    }

    async fn table_exists(&self, _table: &TableIdent) -> IcebergResult<bool> {
        todo!("table exists is not supported");
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
