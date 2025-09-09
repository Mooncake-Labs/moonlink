use super::moonlink_catalog::{CatalogAccess, PuffinBlobType, PuffinWrite};
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::iceberg::iceberg_table_config::RestCatalogConfig;
use crate::storage::iceberg::moonlink_catalog::SchemaUpdate;
use async_trait::async_trait;
use iceberg::puffin::PuffinWriter;
use iceberg::spec::Schema as IcebergSchema;
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::CatalogBuilder;
use iceberg::Result as IcebergResult;
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};
use iceberg_catalog_rest::{
    RestCatalog as IcebergRestCatalog, RestCatalogBuilder as IcebergRestCatalogBuilder,
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct RestCatalog {
    pub(crate) catalog: IcebergRestCatalog,
    file_io: FileIO,
    warehouse_location: String,
    ///
    /// Maps from "puffin filepath" to "puffin blob metadata".
    deletion_vector_blobs_to_add: HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    file_index_blobs_to_add: HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    /// A vector of "puffin filepath"s.
    puffin_blobs_to_remove: HashSet<String>,
    /// A set of data files to remove, along with their corresponding deletion vectors and file indices.
    data_files_to_remove: HashSet<String>,
}

impl RestCatalog {
    #[allow(dead_code)]
    pub async fn new(
        mut config: RestCatalogConfig,
        _accessor_config: AccessorConfig,
    ) -> IcebergResult<Self> {
        let builder = IcebergRestCatalogBuilder::default();
        config
            .props
            .insert(REST_CATALOG_PROP_URI.to_string(), config.uri);
        config.props.insert(
            REST_CATALOG_PROP_WAREHOUSE.to_string(),
            config.warehouse.clone(),
        );
        let warehouse_location = config.warehouse.clone();
        let catalog = builder.load(config.name, config.props).await?;
        let file_io = iceberg_io_utils::create_file_io(&accessor_config)?;
        Ok(Self {
            catalog,
            file_io,
            warehouse_location,
            deletion_vector_blobs_to_add: HashMap::new(),
            file_index_blobs_to_add: HashMap::new(),
            puffin_blobs_to_remove: HashSet::new(),
            data_files_to_remove: HashSet::new(),
        })
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

#[async_trait]
impl PuffinWrite for RestCatalog {
    async fn record_puffin_metadata_and_close(
        &mut self,
        _puffin_filepath: String,
        _puffin_writer: PuffinWriter,
        _puffin_blob_type: PuffinBlobType,
    ) -> IcebergResult<()> {
        todo!("record puffin metadata and close is not supported")
    }

    fn set_data_files_to_remove(&mut self, _data_files: HashSet<String>) {
        todo!("set data files to remove is not supported")
    }

    fn set_index_puffin_files_to_remove(&mut self, _puffin_filepaths: HashSet<String>) {
        todo!("set index puffin files to remove is not supported")
    }

    fn clear_puffin_metadata(&mut self) {
        todo!("clear puffin metadata is not supported")
    }
}

#[async_trait]
impl SchemaUpdate for RestCatalog {
    async fn update_table_schema(
        &mut self,
        _new_schema: IcebergSchema,
        _table_ident: TableIdent,
    ) -> IcebergResult<Table> {
        todo!("update table schema is not supported")
    }
}

#[async_trait]
impl CatalogAccess for RestCatalog {
    fn get_warehouse_location(&self) -> &str {
        &self.warehouse_location
    }

    async fn load_metadata(
        &self,
        table_ident: &TableIdent,
    ) -> IcebergResult<(String /*metadata_filepath*/, TableMetadata)> {
        let table = self.catalog.load_table(table_ident).await?;
        let metadata = table.metadata().clone();
        let metadata_location = table
            .metadata_location()
            .map(|s| s.to_string())
            .unwrap_or_default();
        Ok((metadata_location, metadata))
    }
}
