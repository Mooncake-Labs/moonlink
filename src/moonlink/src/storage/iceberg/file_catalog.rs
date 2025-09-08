use super::puffin_writer_proxy::append_puffin_metadata_and_rewrite;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::factory::create_filesystem_accessor;
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::iceberg::catalog_utils::reflect_table_updates;
use crate::storage::iceberg::io_utils as iceberg_io_utils;
use crate::storage::iceberg::moonlink_catalog::{
    CatalogAccess, PuffinBlobType, PuffinWrite, SchemaUpdate,
};
use crate::storage::iceberg::puffin_writer_proxy::{
    get_puffin_metadata_and_close, PuffinBlobMetadataProxy,
};
use crate::storage::iceberg::table_commit_proxy::TableCommitProxy;

use futures::future::join_all;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
/// This module contains the file-based catalog implementation, which relies on version hint file to decide current version snapshot.
/// Despite a few limitation (i.e. atomic rename for local filesystem), it's not a problem for moonlink, which guarantees at most one writer at the same time (for nows).
/// It leverages `opendal` and iceberg `FileIO` as an abstraction layer to operate on all possible storage backends.
///
/// Iceberg table format from object storage's perspective:
/// - namespace_indicator.txt
///   - An empty file, indicates it's a valid namespace
/// - data
///   - parquet files
/// - metadata
///   - version hint file
///     + version-hint.text
///     + contains the latest version number for metadata
///   - metadata file
///     + v0.metadata.json ... vn.metadata.json
///     + records table schema
///   - snapshot file / manifest list
///     + snap-<snapshot-id>-<attempt-id>-<commit-uuid>.avro
///     + points to manifest files and record actions
///   - manifest files
///     + <commit-uuid>-m<manifest-counter>.avro
///     + which points to data files and stats
///
/// TODO(hjiang):
/// 1. Before release we should support not only S3, but also R2, GCS, etc; necessary change should be minimal, only need to setup configuration like secret id and secret key.
/// 2. Add integration test to actual object storage before pg_mooncake release.
use std::path::PathBuf;
use std::sync::Arc;
use std::vec;
use tokio::sync::Mutex;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::puffin::PuffinWriter;
use iceberg::spec::{
    Schema as IcebergSchema, TableMetadata, TableMetadataBuildResult, TableMetadataBuilder,
};
use iceberg::table::Table;
use iceberg::Result as IcebergResult;
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};
use iceberg::{Error as IcebergError, TableRequirement};

/// Object storage usually doesn't have "folder" concept, when creating a new namespace, we create an indicator file under certain folder.
pub(super) const NAMESPACE_INDICATOR_OBJECT_NAME: &str = "indicator.text";
/// Metadata directory, which stores all metadata files, including manifest files, metadata files, version hint files, etc.
pub(super) const METADATA_DIRECTORY: &str = "metadata";
/// Version hint file which indicates the latest version for the table, the file exists for all valid iceberg tables.
pub(super) const VERSION_HINT_FILENAME: &str = "version-hint.text";

#[derive(Debug)]
pub struct FileCatalog {
    /// Filesystem operator.
    filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
    /// Similar to opendal operator, which also provides an abstraction above different storage backends.
    file_io: FileIO,
    /// Table location.
    warehouse_location: String,
    /// Used to overwrite iceberg metadata at table creation.
    iceberg_schema: Option<IcebergSchema>,
    /// Used for atomic write to commit transaction.
    etag: Mutex<RefCell<Option<String>>>,
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

impl FileCatalog {
    /// Create a file catalog, which gets initialized lazily.
    pub fn new(
        accessor_config: AccessorConfig,
        iceberg_schema: IcebergSchema,
    ) -> IcebergResult<Self> {
        let file_io = iceberg_io_utils::create_file_io(&accessor_config)?;
        let warehouse_location = accessor_config.get_root_path();
        Ok(Self {
            filesystem_accessor: create_filesystem_accessor(accessor_config),
            file_io,
            warehouse_location,
            iceberg_schema: Some(iceberg_schema),
            etag: Mutex::new(RefCell::new(None)),
            deletion_vector_blobs_to_add: HashMap::new(),
            file_index_blobs_to_add: HashMap::new(),
            puffin_blobs_to_remove: HashSet::new(),
            data_files_to_remove: HashSet::new(),
        })
    }

    /// Create a file catalog, which get initialized lazily with no schema populated.
    pub fn new_without_schema(accessor_config: AccessorConfig) -> IcebergResult<Self> {
        let file_io = iceberg_io_utils::create_file_io(&accessor_config)?;
        let warehouse_location = accessor_config.get_root_path();
        Ok(Self {
            filesystem_accessor: create_filesystem_accessor(accessor_config),
            file_io,
            warehouse_location,
            iceberg_schema: None,
            etag: Mutex::new(RefCell::new(None)),
            deletion_vector_blobs_to_add: HashMap::new(),
            file_index_blobs_to_add: HashMap::new(),
            puffin_blobs_to_remove: HashSet::new(),
            data_files_to_remove: HashSet::new(),
        })
    }

    /// Create a file catalog with the provided filesystem accessor.
    #[cfg(test)]
    pub fn new_with_filesystem_accessor(
        filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
        iceberg_schema: IcebergSchema,
    ) -> IcebergResult<Self> {
        use iceberg::io::FileIOBuilder;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        Ok(Self {
            filesystem_accessor,
            file_io,
            iceberg_schema: Some(iceberg_schema),
            etag: Mutex::new(RefCell::new(None)),
            warehouse_location: String::new(),
            deletion_vector_blobs_to_add: HashMap::new(),
            file_index_blobs_to_add: HashMap::new(),
            puffin_blobs_to_remove: HashSet::new(),
            data_files_to_remove: HashSet::new(),
        })
    }

    /// Get object name of the indicator object for the given namespace.
    fn get_namespace_indicator_name(namespace: &iceberg::NamespaceIdent) -> String {
        let mut path = PathBuf::new();
        for part in namespace.as_ref() {
            path.push(part);
        }
        path.push(NAMESPACE_INDICATOR_OBJECT_NAME);
        path.to_str().unwrap().to_string()
    }

    /// Validate table commit requirements.
    fn validate_table_requirements(
        table_requirements: Vec<TableRequirement>,
        table_metadata: &TableMetadata,
    ) -> IcebergResult<()> {
        for cur_requirement in table_requirements.into_iter() {
            cur_requirement.check(Some(table_metadata))?;
        }
        Ok(())
    }

    /// This is a hack function to work-around iceberg-rust.
    /// iceberg-rust somehow reassign field id at table creation, which means it leads to inconsistency between iceberg table metadata and parquet metadata; query engines is possible to suffer schema inconsistency error.
    /// Here we overwrite iceberg schema with correctly populated field id.
    fn get_iceberg_table_metadata(
        &self,
        table_metadata: TableMetadataBuildResult,
    ) -> IcebergResult<TableMetadata> {
        let metadata = table_metadata.metadata;
        let mut metadata_builder = metadata.into_builder(/*current_file_location=*/ None);
        metadata_builder =
            metadata_builder.add_current_schema(self.iceberg_schema.as_ref().unwrap().clone())?;
        let new_table_metadata = metadata_builder.build()?;
        Ok(new_table_metadata.metadata)
    }

    /// Load and assign etag, which is used to commit transaction.
    async fn load_version_hint_etag(&self, version_hint_filepath: &str) -> IcebergResult<()> {
        let version_hint_metadata = self
            .filesystem_accessor
            .stats_object(version_hint_filepath)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Failed to stats version hint file {version_hint_filepath} on load table"
                    ),
                )
                .with_retryable(true)
                .with_source(e)
            })?;
        let etag = version_hint_metadata.etag().map(|etag| etag.to_string());
        {
            let guard = self.etag.lock().await;
            *guard.borrow_mut() = etag;
        }
        Ok(())
    }

    /// Read current version from version hint filepath.
    async fn load_current_version(&self, version_hint_filepath: &str) -> IcebergResult<u32> {
        let version_str = self
            .filesystem_accessor
            .read_object_as_string(version_hint_filepath)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Failed to read version hint file {version_hint_filepath} on load table"
                    ),
                )
                .with_retryable(true)
                .with_source(e)
            })?;
        let version = version_str.trim().parse::<u32>().map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::DataInvalid,
                "Failed to parse version hint string".to_string(),
            )
            .with_source(e)
        })?;
        Ok(version)
    }

    /// Load iceberg table metadata.
    async fn load_table_metadata(&self, metadata_filepath: &str) -> IcebergResult<TableMetadata> {
        let metadata_bytes = self
            .filesystem_accessor
            .read_object(metadata_filepath)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Failed to read table metadata file at {metadata_filepath} on load table"
                    ),
                )
                .with_retryable(true)
                .with_source(e)
            })?;
        let metadata = serde_json::from_slice::<TableMetadata>(&metadata_bytes).map_err(|e| {
            IcebergError::new(iceberg::ErrorKind::DataInvalid, e.to_string()).with_source(e)
        })?;
        Ok(metadata)
    }
}

#[async_trait]
impl PuffinWrite for FileCatalog {
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
impl Catalog for FileCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        // Check parent namespace existence.
        if let Some(namespace_ident) = parent {
            let exists = self.namespace_exists(namespace_ident).await?;
            if !exists {
                return Err(IcebergError::new(
                    iceberg::ErrorKind::NamespaceNotFound,
                    format!(
                        "When list namespace, parent namespace {namespace_ident:?} doesn't exist."
                    ),
                ));
            }
        }

        let parent_directory = if let Some(namespace_ident) = parent {
            namespace_ident.to_url_string()
        } else {
            "/".to_string()
        };
        let subdirectories = self
            .filesystem_accessor
            .list_direct_subdirectories(&parent_directory)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to list namespaces {parent:?}"),
                )
                .with_retryable(true)
                .with_source(e)
            })?;

        // Start multiple async functions in parallel to check whether namespace.
        let mut futures = Vec::with_capacity(subdirectories.len());
        for cur_subdir in subdirectories.iter() {
            let cur_namespace_ident = if let Some(parent_namespace_ident) = parent {
                let mut parent_namespace_segments = parent_namespace_ident.clone().to_vec();
                parent_namespace_segments.push(cur_subdir.to_string());
                NamespaceIdent::from_vec(parent_namespace_segments).unwrap()
            } else {
                NamespaceIdent::new(cur_subdir.to_string())
            };
            futures.push(async move { self.namespace_exists(&cur_namespace_ident).await });
        }

        // Wait for all operations to complete and collect results.
        let exists_results = join_all(futures).await;
        let mut res: Vec<NamespaceIdent> = Vec::new();
        for (exists_result, cur_subdir) in exists_results.into_iter().zip(subdirectories.iter()) {
            let is_namespace = exists_result?;
            if is_namespace {
                let cur_namespace_ident = if let Some(parent_namespace_ident) = parent {
                    let mut parent_namespace_segments = parent_namespace_ident.clone().to_vec();
                    parent_namespace_segments.push(cur_subdir.to_string());
                    NamespaceIdent::from_vec(parent_namespace_segments).unwrap()
                } else {
                    NamespaceIdent::new(cur_subdir.to_string())
                };
                res.push(cur_namespace_ident);
            }
        }

        Ok(res)
    }

    /// Create a new namespace inside the catalog, return error if namespace already exists, or any parent namespace doesn't exist.
    ///
    /// TODO(hjiang): Implement properties handling.
    async fn create_namespace(
        &self,
        namespace_ident: &iceberg::NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<iceberg::Namespace> {
        let segments = namespace_ident.clone().inner();
        let mut segment_vec = vec![];
        for cur_segment in &segments[..segments.len().saturating_sub(1)] {
            segment_vec.push(cur_segment.clone());
            let parent_namespace_ident = NamespaceIdent::from_vec(segment_vec.clone())?;
            let exists = self.namespace_exists(&parent_namespace_ident).await?;
            if !exists {
                return Err(IcebergError::new(
                    iceberg::ErrorKind::NamespaceNotFound,
                    format!("Parent Namespace {parent_namespace_ident:?} doesn't exists"),
                ));
            }
        }
        self.filesystem_accessor
            .write_object(
                &FileCatalog::get_namespace_indicator_name(namespace_ident),
                /*content=*/ vec![],
            )
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Failed to write metadata file at namespace creation for {namespace_ident:?}"
                    ),
                )
                .with_retryable(true)
                .with_source(e)
            })?;

        Ok(Namespace::new(namespace_ident.clone()))
    }

    /// Get a namespace information from the catalog, return error if requested namespace doesn't exist.
    async fn get_namespace(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<Namespace> {
        let exists = self.namespace_exists(namespace_ident).await?;
        if exists {
            return Ok(Namespace::new(namespace_ident.clone()));
        }
        Err(IcebergError::new(
            iceberg::ErrorKind::NamespaceNotFound,
            format!("Namespace {namespace_ident:?} does not exist"),
        ))
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<bool> {
        let key = FileCatalog::get_namespace_indicator_name(namespace_ident);
        let exists = self
            .filesystem_accessor
            .object_exists(&key)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to check namespace {namespace_ident:?} existence"),
                )
                .with_retryable(true)
                .with_source(e)
            })?;
        Ok(exists)
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<()> {
        let key = FileCatalog::get_namespace_indicator_name(namespace_ident);
        self.filesystem_accessor
            .delete_object(&key)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to drop namespace {namespace_ident:?} existence"),
                )
                .with_retryable(true)
                .with_source(e)
            })?;
        Ok(())
    }

    /// List tables from namespace, return error if the given namespace doesn't exist.
    async fn list_tables(
        &self,
        namespace_ident: &NamespaceIdent,
    ) -> IcebergResult<Vec<TableIdent>> {
        // Check if the given namespace exists.
        let exists = self.namespace_exists(namespace_ident).await?;
        if !exists {
            return Err(IcebergError::new(
                iceberg::ErrorKind::NamespaceNotFound,
                format!("Namespace {namespace_ident:?} doesn't exist when list tables within."),
            ));
        }

        let parent_directory = namespace_ident.to_url_string();
        let subdirectories = self
            .filesystem_accessor
            .list_direct_subdirectories(&parent_directory)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to list tables for namespace {namespace_ident:?}"),
                )
                .with_source(e)
            })?;

        let mut table_idents: Vec<TableIdent> = Vec::with_capacity(subdirectories.len());
        for cur_subdir in subdirectories.iter() {
            let cur_table_ident = TableIdent::new(namespace_ident.clone(), cur_subdir.clone());
            let exists = self.table_exists(&cur_table_ident).await?;
            if exists {
                table_idents.push(cur_table_ident);
            }
        }
        Ok(table_idents)
    }

    async fn update_namespace(
        &self,
        _namespace_ident: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        todo!("Update namespace is not supported yet!")
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace_ident: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        let directory = namespace_ident.to_url_string();
        let table_ident = TableIdent::new(namespace_ident.clone(), creation.name.clone());

        // Create version hint file.
        let version_hint_filepath = format!(
            "{}/{}/{}/version-hint.text",
            directory, creation.name, METADATA_DIRECTORY,
        );
        self.filesystem_accessor
            .write_object(
                &version_hint_filepath,
                /*content=*/ "0".as_bytes().to_vec(),
            )
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write version hint file at table {} creation under namespace {:?} ", creation.name, namespace_ident),
                )
                .with_retryable(true)
                .with_source(e)
            })?;

        // Create metadata file.
        let metadata_filepath = format!(
            "{}/{}/metadata/v0.metadata.json",
            directory,
            creation.name.clone()
        );

        let table_metadata = TableMetadataBuilder::from_table_creation(creation)?.build()?;
        let metadata = self.get_iceberg_table_metadata(table_metadata)?;
        let metadata_json = serde_json::to_vec(&metadata)?;
        self.filesystem_accessor
            .write_object(&metadata_filepath, /*content=*/ metadata_json)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write metadata file {metadata_filepath} at table creation"),
                )
                .with_retryable(true)
                .with_source(e)
            })?;

        let table = Table::builder()
            .metadata(metadata)
            .identifier(table_ident)
            .file_io(self.file_io.clone())
            .build()?;
        Ok(table)
    }

    /// Load table from the catalog.
    async fn load_table(&self, table_ident: &TableIdent) -> IcebergResult<Table> {
        let (metadata_filepath, metadata) = self.load_metadata(table_ident).await?;

        // Build and return the table.
        let metadata_path = format!("{}/{}", self.warehouse_location, metadata_filepath);
        let table = Table::builder()
            .metadata_location(metadata_path)
            .metadata(metadata)
            .identifier(table_ident.clone())
            .file_io(self.file_io.clone())
            .build()?;
        Ok(table)
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        let directory = format!("{}/{}", table.namespace().to_url_string(), table.name());
        self.filesystem_accessor
            .remove_directory(&directory)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to delete directory {directory}"),
                )
                .with_retryable(true)
                .with_source(e)
            })?;
        Ok(())
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        let mut version_hint_filepath = PathBuf::from(table.namespace.to_url_string());
        version_hint_filepath.push(table.name());
        version_hint_filepath.push("metadata");
        version_hint_filepath.push("version-hint.text");

        let exists = self
            .filesystem_accessor
            .object_exists(version_hint_filepath.to_str().unwrap())
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Failed to check version hint file existence {version_hint_filepath:?}"
                    ),
                )
                .with_retryable(true)
                .with_source(e)
            })?;
        Ok(exists)
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        todo!("rename table is not supported yet!")
    }

    /// Update a table to the catalog, which writes metadata file and version hint file.
    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        let (metadata_filepath, metadata) = self.load_metadata(commit.identifier()).await?;
        let version = metadata.next_sequence_number();
        let builder = TableMetadataBuilder::new_from_metadata(
            metadata.clone(),
            /*current_file_location=*/ Some(metadata_filepath.clone()),
        );

        // Validate existing table metadata with requirements.
        Self::validate_table_requirements(commit.take_requirements(), &metadata)?;

        // Construct new metadata with updates.
        let updates = commit.take_updates();
        let builder = reflect_table_updates(builder, updates)?;
        let metadata = builder.build()?.metadata;

        // Write metadata file.
        let metadata_directory = format!(
            "{}/{}/metadata",
            commit.identifier().namespace().to_url_string(),
            commit.identifier().name()
        );
        let new_metadata_filepath = format!("{metadata_directory}/v{version}.metadata.json",);
        let metadata_json = serde_json::to_vec(&metadata)?;
        self.filesystem_accessor
            .write_object(&new_metadata_filepath, metadata_json)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    "Failed to write metadata file at table update".to_string(),
                )
                .with_retryable(true)
                .with_source(e)
            })?;

        // Manifest files and manifest list has persisted into storage, make modifications based on puffin blobs.
        //
        // TODO(hjiang): Add unit test for update and check manifest population.
        append_puffin_metadata_and_rewrite(
            &metadata,
            &self.file_io,
            &self.deletion_vector_blobs_to_add,
            &self.file_index_blobs_to_add,
            &self.data_files_to_remove,
            &self.puffin_blobs_to_remove,
        )
        .await?;

        // Write version hint file.
        let version_hint_path = format!("{metadata_directory}/version-hint.text");
        let etag = {
            let guard = self.etag.lock().await;
            let etag = guard.borrow().as_ref().cloned();
            etag
        };
        let object_metadata = self
            .filesystem_accessor
            .conditional_write_object(
                &version_hint_path,
                format!("{version}").as_bytes().to_vec(),
                etag,
            )
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write version hint file existence {version_hint_path}"),
                )
                .with_retryable(true)
                .with_source(e)
            })?;
        {
            let guard = self.etag.lock().await;
            *guard.borrow_mut() = object_metadata.etag().map(|etag| etag.to_string());
        }

        Table::builder()
            .identifier(commit.identifier().clone())
            .file_io(self.file_io.clone())
            .metadata(metadata)
            .metadata_location(metadata_filepath)
            .build()
    }

    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> IcebergResult<Table> {
        todo!("register existing table is not supported")
    }
}

#[async_trait]
impl SchemaUpdate for FileCatalog {
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
impl CatalogAccess for FileCatalog {
    /// Get warehouse uri.
    #[allow(dead_code)]
    fn get_warehouse_location(&self) -> &str {
        &self.warehouse_location
    }

    /// Load metadata and its location foe the given table.
    async fn load_metadata(
        &self,
        table_ident: &TableIdent,
    ) -> IcebergResult<(String /*metadata_filepath*/, TableMetadata)> {
        // Read version hint for the table to get latest version.
        let version_hint_filepath = format!(
            "{}/{}/{}/{}",
            table_ident.namespace().to_url_string(),
            table_ident.name(),
            METADATA_DIRECTORY,
            VERSION_HINT_FILENAME,
        );

        // Load and assign etag.
        self.load_version_hint_etag(&version_hint_filepath).await?;

        // Read version hint file.
        let version = self.load_current_version(&version_hint_filepath).await?;

        // Read and parse table metadata.
        let metadata_filepath = format!(
            "{}/{}/{}/v{}.metadata.json",
            table_ident.namespace().to_url_string(),
            table_ident.name(),
            METADATA_DIRECTORY,
            version,
        );
        let metadata = self.load_table_metadata(&metadata_filepath).await?;

        Ok((metadata_filepath, metadata))
    }
}
