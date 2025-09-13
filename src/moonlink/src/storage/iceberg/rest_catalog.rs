use super::moonlink_catalog::{CatalogAccess, PuffinBlobType, PuffinWrite, SchemaUpdate};
use super::puffin_writer_proxy::{
    append_puffin_metadata_and_rewrite, get_puffin_metadata_and_close, PuffinBlobMetadataProxy,
};
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::iceberg::catalog_utils::{reflect_table_updates, validate_table_requirements};
use crate::storage::iceberg::iceberg_table_config::RestCatalogConfig;
use crate::storage::iceberg::io_utils as iceberg_io_utils;
use crate::storage::iceberg::table_commit_proxy::TableCommitProxy;
use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::puffin::PuffinWriter;
use iceberg::spec::TableMetadataBuilder;
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
    /// Used to record puffin blob metadata in one transaction, and cleaned up after transaction commits.
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
            deletion_vector_blobs_to_add: HashMap::new(),
            file_index_blobs_to_add: HashMap::new(),
            puffin_blobs_to_remove: HashSet::new(),
            data_files_to_remove: HashSet::new(),
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
        let old_table = self.catalog.create_table(namespace_ident, creation).await?;
        let old_metadata = old_table.metadata();

        // Craft a new schema with a new schema id, which has to be different from the existing one.
        let old_schema = self.iceberg_schema.as_ref().unwrap().clone();
        let new_schema_id = old_schema.schema_id() + 1;
        let mut new_schema_builder = old_schema.into_builder();
        new_schema_builder = new_schema_builder.with_schema_id(new_schema_id);
        let new_schema = new_schema_builder.build()?;

        // On table creation, iceberg-rust normalize field id, which breaks the mapping between mooncake table arrow schema and iceberg schema.
        // Intentionally perform a schema evolution to reflect desired schema.
        let mut builder = TableMetadataBuilder::new_from_metadata(
            old_metadata.clone(),
            /*current_file_location=*/ None,
        );
        builder = builder.add_current_schema(new_schema)?;
        let build_result = builder.build()?;
        let normalized_updates = build_result.changes;
        let new_commit = TableCommitProxy {
            ident: old_table.identifier().clone(),
            requirements: Vec::new(),
            updates: normalized_updates,
        }
        .take_as_table_commit();

        let updated_table = self.catalog.update_table(new_commit).await?;
        Ok(updated_table)
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

    /// Transaction commit:
    /// - sdk write metadata file and manifest file
    /// - catalog check requirement
    /// - catalog craft request body
    /// - catalog commit
    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        let table = self.load_table(commit.identifier()).await?;
        let metadata = table.metadata();
        let requirements = commit.take_requirements();
        validate_table_requirements(requirements.clone(), metadata)?;

        let builder = TableMetadataBuilder::new_from_metadata(metadata.clone(), None);
        let updates = commit.take_updates();
        let builder = reflect_table_updates(builder, updates)?;
        let build_result = builder.build()?;

        // Rewrite manifest list/entries to append puffin metadata and handle removals before commit
        append_puffin_metadata_and_rewrite(
            &build_result.metadata,
            &self.file_io,
            &self.deletion_vector_blobs_to_add,
            &self.file_index_blobs_to_add,
            &self.data_files_to_remove,
            &self.puffin_blobs_to_remove,
        )
        .await?;

        let normalized_updates = build_result.changes;

        // Repackage normalized updates and original requirements into a TableCommit
        // and delegate to the underlying REST catalog implementation.
        let new_commit = TableCommitProxy {
            ident: commit.identifier().clone(),
            requirements,
            updates: normalized_updates,
        }
        .take_as_table_commit();

        let table = self.catalog.update_table(new_commit).await?;
        Ok(table)
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
        puffin_filepath: String,
        puffin_writer: PuffinWriter,
        puffin_blob_type: PuffinBlobType,
    ) -> IcebergResult<()> {
        let puffin_metadata = get_puffin_metadata_and_close(puffin_writer).await?;
        match &puffin_blob_type {
            PuffinBlobType::DeletionVector => self
                .deletion_vector_blobs_to_add
                .insert(puffin_filepath, puffin_metadata),
            PuffinBlobType::FileIndex => self
                .file_index_blobs_to_add
                .insert(puffin_filepath, puffin_metadata),
        };
        Ok(())
    }

    fn set_data_files_to_remove(&mut self, data_files: HashSet<String>) {
        assert!(self.data_files_to_remove.is_empty());
        self.data_files_to_remove = data_files;
    }

    fn set_index_puffin_files_to_remove(&mut self, puffin_filepaths: HashSet<String>) {
        assert!(self.puffin_blobs_to_remove.is_empty());
        self.puffin_blobs_to_remove = puffin_filepaths;
    }

    fn clear_puffin_metadata(&mut self) {
        self.deletion_vector_blobs_to_add.clear();
        self.file_index_blobs_to_add.clear();
        self.puffin_blobs_to_remove.clear();
        self.data_files_to_remove.clear();
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
