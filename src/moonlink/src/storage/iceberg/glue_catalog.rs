use super::moonlink_catalog::{CatalogAccess, PuffinBlobType, PuffinWrite, SchemaUpdate};
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::iceberg::catalog_utils::{
    close_puffin_writer_and_record_metadata, create_table_impl, update_table_impl,
};
use crate::storage::iceberg::iceberg_table_config::GlueCatalogConfig;
use crate::storage::iceberg::io_utils as iceberg_io_utils;
use crate::storage::iceberg::puffin_writer_proxy::PuffinBlobMetadataProxy;
use crate::storage::iceberg::table_commit_proxy::TableCommitProxy;
use crate::storage::iceberg::table_update_proxy::TableUpdateProxy;
use crate::StorageConfig;
use async_trait::async_trait;
use iceberg::io::{FileIO, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::puffin::PuffinWriter;
use iceberg::spec::{Schema as IcebergSchema, TableMetadata};
use iceberg::table::Table;
use iceberg::CatalogBuilder;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};
use iceberg_catalog_glue::{
    GlueCatalog as IcebergGlueCatalog, GlueCatalogBuilder as IcebergGlueCatalogBuilder,
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, GLUE_CATALOG_PROP_CATALOG_ID,
    GLUE_CATALOG_PROP_URI, GLUE_CATALOG_PROP_WAREHOUSE,
};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct GlueCatalog {
    pub(crate) catalog: IcebergGlueCatalog,
    /// Similar to opendal operator, which also provides an abstraction above different storage backends.
    file_io: FileIO,
    /// Table location.
    warehouse_location: String,
    /// Used to overwrite iceberg metadata at table creation.
    iceberg_schema: Option<IcebergSchema>,
    /// Buffered table updates, which will be reflect to iceberg snapshot at transaction commit.
    table_update_proxy: TableUpdateProxy,
}

/// Util function to get config properties from iceberg table config.
/// If not S3 storage config, return error.
fn extract_glue_config_properties(
    glue_config: &GlueCatalogConfig,
    storage_config: &StorageConfig,
) -> IcebergResult<HashMap<String, String>> {
    if !matches!(storage_config, StorageConfig::S3 { .. }) {
        return Err(IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Glue catalog expects S3 storage config, but gets {storage_config:?}"),
        ));
    }

    let s3_region = storage_config.get_region().unwrap();
    let aws_security_config = glue_config
        .cloud_secret_config
        .get_aws_security_config()
        .unwrap();

    let mut config_props = HashMap::from([
        // AWS configs.
        (
            AWS_ACCESS_KEY_ID.to_string(),
            aws_security_config.access_key_id.clone(),
        ),
        (
            AWS_SECRET_ACCESS_KEY.to_string(),
            aws_security_config.security_access_key.clone(),
        ),
        (
            AWS_REGION_NAME.to_string(),
            aws_security_config.region.clone(),
        ),
        // Glue configs.
        (GLUE_CATALOG_PROP_URI.to_string(), glue_config.uri.clone()),
        (
            GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
            glue_config.warehouse.clone(),
        ),
        // S3 configs.
        (S3_REGION.to_string(), s3_region.clone()),
        (
            S3_ACCESS_KEY_ID.to_string(),
            storage_config.get_access_key_id().unwrap(),
        ),
        (
            S3_SECRET_ACCESS_KEY.to_string(),
            storage_config.get_secret_access_key().unwrap(),
        ),
    ]);

    // Optionally assign catalog id.
    if let Some(catalog_id) = &glue_config.catalog_id {
        config_props.insert(GLUE_CATALOG_PROP_CATALOG_ID.to_string(), catalog_id.clone());
    }
    // Set S3 endpoint
    let s3_endpoint = if let Some(s3_endpoint) = &glue_config.s3_endpoint {
        s3_endpoint.to_string()
    } else {
        format!("https://s3.{s3_region}.amazonaws.com")
    };
    config_props.insert(S3_ENDPOINT.to_string(), s3_endpoint);

    Ok(config_props)
}

impl GlueCatalog {
    #[allow(dead_code)]
    pub async fn new(
        glue_config: GlueCatalogConfig,
        accessor_config: AccessorConfig,
        iceberg_schema: IcebergSchema,
    ) -> IcebergResult<Self> {
        let config_props =
            extract_glue_config_properties(&glue_config, &accessor_config.storage_config)?;
        let warehouse_location = accessor_config.get_root_path();
        let builder = IcebergGlueCatalogBuilder::default();
        let catalog = builder.load(glue_config.name, config_props).await?;
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
    #[allow(unused)]
    pub async fn new_without_schema(
        glue_config: GlueCatalogConfig,
        accessor_config: AccessorConfig,
    ) -> IcebergResult<Self> {
        let config_props =
            extract_glue_config_properties(&glue_config, &accessor_config.storage_config)?;
        let warehouse_location = accessor_config.get_root_path();
        let builder = IcebergGlueCatalogBuilder::default();
        let catalog = builder.load(glue_config.name, config_props).await?;
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
impl Catalog for GlueCatalog {
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
impl PuffinWrite for GlueCatalog {
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
impl SchemaUpdate for GlueCatalog {
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
impl CatalogAccess for GlueCatalog {
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
