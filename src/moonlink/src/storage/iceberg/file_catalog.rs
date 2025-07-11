use super::puffin_writer_proxy::append_puffin_metadata_and_rewrite;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;
use crate::storage::filesystem::utils::path_utils::get_root_path;
use crate::storage::iceberg::io_utils as iceberg_io_utils;
use crate::storage::iceberg::moonlink_catalog::PuffinWrite;
use crate::storage::iceberg::puffin_writer_proxy::{
    get_puffin_metadata_and_close, PuffinBlobMetadataProxy,
};

use futures::future::join_all;
use std::collections::{HashMap, HashSet};
/// This module contains the file-based catalog implementation, which relies on version hint file to decide current version snapshot.
/// Dispite a few limitation (i.e. atomic rename for local filesystem), it's not a problem for moonlink, which guarantees at most one writer at the same time (for nows).
/// It leverages `opendal` and iceberg `FileIO` as an abstraction layer to operate on all possible storage backends.
///
/// Iceberg table format from object storage's perspective:
/// - namespace_indicator.txt
///   - An empty file, indicates it's a valid namespace
/// - data
///   - parquet files
/// - metdata
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

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::puffin::PuffinWriter;
use iceberg::spec::{
    Schema as IcebergSchema, TableMetadata, TableMetadataBuildResult, TableMetadataBuilder,
};
use iceberg::table::Table;
use iceberg::Result as IcebergResult;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableUpdate,
};
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
    iceberg_schema: IcebergSchema,
    /// Used to record puffin blob metadata in one transaction, and cleaned up after transaction commits.
    ///
    /// Maps from "puffin filepath" to "puffin blob metadata".
    puffin_blobs_to_add: HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    /// A vector of "puffin filepath"s.
    puffin_blobs_to_remove: HashSet<String>,
    /// A set of data files to remove, along with their corresponding deletion vectors and file indices.
    data_files_to_remove: HashSet<String>,
}

impl FileCatalog {
    /// Create a file catalog, which gets initialized lazily.
    pub fn new(config: FileSystemConfig, iceberg_schema: IcebergSchema) -> IcebergResult<Self> {
        let file_io = iceberg_io_utils::create_file_io(&config)?;
        let warehouse_location = get_root_path(&config);
        Ok(Self {
            filesystem_accessor: Arc::new(FileSystemAccessor::new(config)),
            file_io,
            warehouse_location,
            iceberg_schema,
            puffin_blobs_to_add: HashMap::new(),
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
            iceberg_schema,
            warehouse_location: String::new(),
            puffin_blobs_to_add: HashMap::new(),
            puffin_blobs_to_remove: HashSet::new(),
            data_files_to_remove: HashSet::new(),
        })
    }

    /// Get warehouse uri.
    #[allow(dead_code)]
    pub(super) fn get_warehouse_location(&self) -> &str {
        &self.warehouse_location
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

    /// Load metadata and its location foe the given table.
    pub(super) async fn load_metadata(
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
        let version_str = self
            .filesystem_accessor
            .read_object_as_string(&version_hint_filepath)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to read version hint file on load table: {e}"),
                )
            })?;
        let version = version_str
            .trim()
            .parse::<u32>()
            .map_err(|e| IcebergError::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        // Read and parse table metadata.
        let metadata_filepath = format!(
            "{}/{}/{}/v{}.metadata.json",
            table_ident.namespace().to_url_string(),
            table_ident.name(),
            METADATA_DIRECTORY,
            version,
        );
        let metadata_bytes = self
            .filesystem_accessor
            .read_object(&metadata_filepath)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to read table metadata file on load table: {e}"),
                )
            })?;
        let metadata = serde_json::from_slice::<TableMetadata>(&metadata_bytes)
            .map_err(|e| IcebergError::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        Ok((metadata_filepath, metadata))
    }

    /// Validate table commit requirements.
    fn validate_table_requirements(
        table_requirements: Vec<TableRequirement>,
        table_metadata: &TableMetadata,
    ) -> IcebergResult<()> {
        for cur_requirment in table_requirements.into_iter() {
            cur_requirment.check(Some(table_metadata))?;
        }
        Ok(())
    }

    /// Reflect table updates to table metadata builder.
    fn reflect_table_updates(
        mut builder: TableMetadataBuilder,
        table_updates: Vec<TableUpdate>,
    ) -> IcebergResult<TableMetadataBuilder> {
        for update in &table_updates {
            match update {
                TableUpdate::AddSnapshot { snapshot } => {
                    builder = builder.add_snapshot(snapshot.clone())?;
                }
                TableUpdate::SetSnapshotRef {
                    ref_name,
                    reference,
                } => {
                    builder = builder.set_ref(ref_name, reference.clone())?;
                }
                TableUpdate::SetProperties { updates } => {
                    builder = builder.set_properties(updates.clone())?;
                }
                TableUpdate::RemoveProperties { removals } => {
                    builder = builder.remove_properties(removals)?;
                }
                _ => {
                    unreachable!("Only snapshot updates are expected in this implementation");
                }
            }
        }
        Ok(builder)
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
        metadata_builder = metadata_builder.add_current_schema(self.iceberg_schema.clone())?;
        let new_table_metadata = metadata_builder.build()?;
        Ok(new_table_metadata.metadata)
    }
}

#[async_trait]
impl PuffinWrite for FileCatalog {
    async fn record_puffin_metadata_and_close(
        &mut self,
        puffin_filepath: String,
        puffin_writer: PuffinWriter,
    ) -> IcebergResult<()> {
        let puffin_metadata = get_puffin_metadata_and_close(puffin_writer).await?;
        self.puffin_blobs_to_add
            .insert(puffin_filepath, puffin_metadata);
        Ok(())
    }

    fn set_data_files_to_remove(&mut self, data_files: HashSet<String>) {
        assert!(self.data_files_to_remove.is_empty());
        self.data_files_to_remove = data_files;
    }

    fn set_puffin_files_to_remove(&mut self, puffin_filepaths: HashSet<String>) {
        assert!(self.puffin_blobs_to_remove.is_empty());
        self.puffin_blobs_to_remove = puffin_filepaths;
    }

    fn clear_puffin_metadata(&mut self) {
        self.puffin_blobs_to_add.clear();
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
                    format!("Failed to list namespaces: {e}"),
                )
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
                    format!("Failed to write metadata file at namespace creation: {e}"),
                )
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
                    format!("Failed to check namespace {namespace_ident:?} existence: {e:?}"),
                )
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
                    format!("Failed to drop namespace {namespace_ident:?} existence: {e:?}"),
                )
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
                    format!("Failed to list tables: {e}"),
                )
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
        todo!()
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
                    format!("Failed to write version hint file at table creation: {e}"),
                )
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
                    format!("Failed to write metadata file at table creation: {e}"),
                )
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
                    format!("Failed to delete directory {directory}: {e:?}"),
                )
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
                        "Failed to check version hint file existence {version_hint_filepath:?}: {e:?}"
                    ),
                )
            })?;
        Ok(exists)
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        todo!()
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
        let builder = Self::reflect_table_updates(builder, updates)?;
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
                    format!("Failed to write metadata file at table update: {e}"),
                )
            })?;

        // Manifest files and manifest list has persisted into storage, make modifications based on puffin blobs.
        //
        // TODO(hjiang): Add unit test for update and check manifest population.
        append_puffin_metadata_and_rewrite(
            &metadata,
            &self.file_io,
            &self.data_files_to_remove,
            &self.puffin_blobs_to_add,
            &self.puffin_blobs_to_remove,
        )
        .await?;

        // Write version hint file.
        let version_hint_path = format!("{metadata_directory}/version-hint.text");
        self.filesystem_accessor
            .write_object(&version_hint_path, format!("{version}").as_bytes().to_vec())
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Failed to write version hint file existencee {version_hint_path}: {e:?}"
                    ),
                )
            })?;

        Table::builder()
            .identifier(commit.identifier().clone())
            .file_io(self.file_io.clone())
            .metadata(metadata)
            .metadata_location(metadata_filepath)
            .build()
    }
}
