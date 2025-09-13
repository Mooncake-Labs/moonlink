use super::moonlink_catalog::{CatalogAccess, PuffinBlobType, PuffinWrite, SchemaUpdate};
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::iceberg::catalog_utils::{
    close_puffin_writer_and_record_metadata, create_table_impl, update_table_impl,
};
use crate::storage::iceberg::iceberg_table_config::RestCatalogConfig;
use crate::storage::iceberg::io_utils as iceberg_io_utils;
use crate::storage::iceberg::puffin_writer_proxy::PuffinBlobMetadataProxy;
use crate::storage::iceberg::table_commit_proxy::TableCommitProxy;
use crate::storage::iceberg::table_update_proxy::TableUpdateProxy;
use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::puffin::PuffinWriter;
use iceberg::spec::{Schema as IcebergSchema, TableMetadata};
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
    /// Similar to opendal operator, which also provides an abstraction above different storage backends.
    file_io: FileIO,
    /// Table location.
    warehouse_location: String,
    /// Used to overwrite iceberg metadata at table creation.
    iceberg_schema: Option<IcebergSchema>,
    /// Buffered table updates, which will be reflect to iceberg snapshot at transaction commit.
    table_update_proxy: TableUpdateProxy,
}

impl RestCatalog {
    pub async fn new(
        mut config: RestCatalogConfig,
        accessor_config: AccessorConfig,
        iceberg_schema: IcebergSchema,
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
            iceberg_schema: Some(iceberg_schema),
            table_update_proxy: TableUpdateProxy::default(),
        })
    }

    /// Create a rest catalog, which get initialized lazily with no schema populated.
    pub async fn new_without_schema(
        mut config: RestCatalogConfig,
        accessor_config: AccessorConfig,
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
            iceberg_schema: None,
            table_update_proxy: TableUpdateProxy::default(),
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
        let iceberg_schema = self.iceberg_schema.as_ref().unwrap().clone();
        let table =
            create_table_impl(&self.catalog, namespace_ident, creation, iceberg_schema).await?;
        Ok(table)
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

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        let updated_table = update_table_impl(
            &self.catalog,
            &self.file_io,
            &self.table_update_proxy,
            commit,
        )
        .await?;
        Ok(updated_table)
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
    fn record_puffin_metadata(
        &mut self,
        puffin_filepath: String,
        puffin_metadata: Vec<PuffinBlobMetadataProxy>,
        puffin_blob_type: PuffinBlobType,
    ) {
        self.table_update_proxy.record_puffin_metadata(
            puffin_filepath,
            puffin_metadata,
            puffin_blob_type,
        );
    }
    async fn record_puffin_metadata_and_close(
        &mut self,
        puffin_filepath: String,
        puffin_writer: PuffinWriter,
        puffin_blob_type: PuffinBlobType,
    ) -> IcebergResult<()> {
        close_puffin_writer_and_record_metadata(
            puffin_filepath,
            puffin_writer,
            puffin_blob_type,
            &mut self.table_update_proxy,
        )
        .await?;
        Ok(())
    }

    fn set_data_files_to_remove(&mut self, data_files: HashSet<String>) {
        self.table_update_proxy.set_data_files_to_remove(data_files);
    }

    fn set_index_puffin_files_to_remove(&mut self, puffin_filepaths: HashSet<String>) {
        self.table_update_proxy
            .set_index_puffin_files_to_remove(puffin_filepaths);
    }

    fn clear_puffin_metadata(&mut self) {
        self.table_update_proxy.clear();
    }
}

#[async_trait]
impl SchemaUpdate for RestCatalog {
    async fn update_table_schema(
        &mut self,
        new_schema: IcebergSchema,
        table_ident: TableIdent,
    ) -> IcebergResult<Table> {
        let (_, old_metadata) = self.load_metadata(&table_ident).await?;
        let mut metadata_builder = old_metadata.into_builder(/*current_file_location=*/ None);
        metadata_builder = metadata_builder.add_current_schema(new_schema)?;
        let metadata_builder_result = metadata_builder.build()?;

        let table_commit_proxy = TableCommitProxy {
            ident: table_ident,
            updates: metadata_builder_result.changes,
            requirements: vec![],
        };
        let table_commit = table_commit_proxy.take_as_table_commit();
        self.update_table(table_commit).await
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
