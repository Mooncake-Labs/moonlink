use super::puffin_writer_proxy::append_puffin_metadata_and_rewrite;
use crate::storage::iceberg::moonlink_catalog::PuffinWrite;
use crate::storage::iceberg::puffin_writer_proxy::{
    get_puffin_metadata_and_close, PuffinBlobMetadataProxy,
};

use futures::future::join_all;
use futures::StreamExt;
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
use std::error::Error;
use std::path::PathBuf;
use std::vec;

use async_trait::async_trait;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::puffin::PuffinWriter;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableUpdate,
};
use opendal::layers::RetryLayer;
use opendal::services;
use opendal::Operator;
use tokio::sync::OnceCell;

// Object storage usually doesn't have "folder" concept, when creating a new namespace, we create an indicator file under certain folder.
static NAMESPACE_INDICATOR_OBJECT_NAME: &str = "indicator.text";

// Retry related constants.
static MIN_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
static MAX_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(3);
static RETRY_DELAY_FACTOR: f32 = 1.5;
static MAX_RETRY_COUNT: usize = 5;

// opendal requires local filesystem directory to end with "/".
fn normalize_directory(mut path: PathBuf) -> String {
    let mut os_string = path.as_mut_os_str().to_os_string();
    if os_string.to_str().unwrap().ends_with("/") {
        return os_string.to_str().unwrap().to_string();
    }
    os_string.push("/");
    os_string.to_str().unwrap().to_string()
}

#[derive(Debug)]
#[warn(dead_code)]
pub enum CatalogConfig {
    #[cfg(feature = "storage-fs")]
    FileSystem,
    #[cfg(feature = "storage-s3")]
    S3 {
        access_key_id: String,
        secret_access_key: String,
        region: String,
        bucket: String,
        endpoint: String,
    },
}

/// Create `FileIO` object based on the catalog config.
fn create_file_io(config: &CatalogConfig) -> IcebergResult<FileIO> {
    match config {
        #[cfg(feature = "storage-fs")]
        CatalogConfig::FileSystem => FileIOBuilder::new_fs_io().build(),
        #[cfg(feature = "storage-s3")]
        CatalogConfig::S3 {
            access_key_id,
            secret_access_key,
            region,
            endpoint,
            ..
        } => FileIOBuilder::new("s3")
            .with_prop(iceberg::io::S3_REGION, region)
            .with_prop(iceberg::io::S3_ENDPOINT, endpoint)
            .with_prop(iceberg::io::S3_ACCESS_KEY_ID, access_key_id)
            .with_prop(iceberg::io::S3_SECRET_ACCESS_KEY, secret_access_key)
            .build(),
    }
}

#[derive(Debug)]
pub struct FileCatalog {
    /// Catalog configurations.
    config: CatalogConfig,
    /// Similar to opendal operator, which also provides an abstraction above different storage backends.
    file_io: FileIO,
    /// Operator to manager all IO operations.
    operator: OnceCell<Operator>,
    /// Table location.
    warehouse_location: String,
    /// Used to record puffin blob metadata in one transaction, and cleaned up after transaction commits.
    ///
    /// Maps from "puffin filepath" to "puffin blob metadata".
    puffin_blobs_to_add: HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    /// A vector of "puffin filepath"s.
    puffin_blobs_to_remove: HashSet<String>,
}

impl FileCatalog {
    pub fn new(warehouse_location: String, config: CatalogConfig) -> IcebergResult<Self> {
        let file_io = create_file_io(&config)?;
        Ok(Self {
            config,
            file_io,
            operator: OnceCell::new(),
            warehouse_location,
            puffin_blobs_to_add: HashMap::new(),
            puffin_blobs_to_remove: HashSet::new(),
        })
    }

    /// Get IO operator from the catalog.
    pub(crate) async fn get_operator(&self) -> IcebergResult<&Operator> {
        let retry_layer = RetryLayer::new()
            .with_max_times(MAX_RETRY_COUNT)
            .with_jitter()
            .with_factor(RETRY_DELAY_FACTOR)
            .with_min_delay(MIN_RETRY_DELAY)
            .with_max_delay(MAX_RETRY_DELAY);

        self.operator
            .get_or_try_init(|| async {
                match &self.config {
                    #[cfg(feature = "storage-fs")]
                    CatalogConfig::FileSystem => {
                        let builder = services::Fs::default().root(&self.warehouse_location);
                        let op = Operator::new(builder)
                            .expect("failed to create fs operator")
                            .layer(retry_layer)
                            .finish();
                        op.create_dir(&normalize_directory(PathBuf::from(
                            &self.warehouse_location,
                        )))
                        .await?;
                        Ok(op)
                    }
                    #[cfg(feature = "storage-s3")]
                    CatalogConfig::S3 {
                        access_key_id,
                        secret_access_key,
                        region,
                        bucket,
                        endpoint,
                        ..
                    } => {
                        let builder = services::S3::default()
                            .bucket(bucket)
                            .region(region)
                            .endpoint(endpoint)
                            .access_key_id(access_key_id)
                            .secret_access_key(secret_access_key);
                        let op = Operator::new(builder)
                            .expect("failed to create s3 operator")
                            .layer(retry_layer)
                            .finish();
                        Ok(op)
                    }
                }
            })
            .await
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

    async fn object_exists(&self, object: &str) -> Result<bool, Box<dyn Error>> {
        match self.get_operator().await?.stat(object).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// List all objects under the given directory.
    async fn list_objects_under_directory(
        &self,
        folder: &str,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let prefix = format!("{}/", folder);
        let mut objects = Vec::new();
        let mut lister = self
            .get_operator()
            .await?
            .lister_with(&prefix)
            .recursive(true)
            .await?;
        while let Some(entry) = lister.next().await {
            let entry = entry?;
            let path = entry.path();
            objects.push(path.to_string());
        }
        Ok(objects)
    }

    /// List all direct sub-directory under the given directory.
    ///
    /// For example, we have directory "a", "a/b", "a/b/c", listing direct subdirectories for "a" will return "a/b".
    async fn list_direct_subdirectories(
        &self,
        folder: &str,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let prefix = format!("{}/", folder);
        let mut dirs = Vec::new();
        let lister = self.get_operator().await?.list(&prefix).await?;

        let entries = lister;
        for cur_entry in entries.iter() {
            // Both directories and objects will be returned, here we only care about sub-directories.
            if !cur_entry.path().ends_with('/') {
                continue;
            }
            let dir_name = cur_entry
                .path()
                .trim_start_matches(&prefix)
                .trim_end_matches('/')
                .to_string();
            if !dir_name.is_empty() {
                dirs.push(dir_name);
            }
        }

        Ok(dirs)
    }

    /// Read the whole content for the given object.
    /// Notice, it's not suitable to read large files; as of now it's made for metadata files.
    async fn read_object(&self, object: &str) -> Result<String, Box<dyn Error>> {
        let content = self.get_operator().await?.read(object).await?;
        Ok(String::from_utf8(content.to_vec())?)
    }

    /// Write the whole content to the given file.
    async fn write_object(
        &self,
        object_filepath: &str,
        content: &str,
    ) -> Result<(), Box<dyn Error>> {
        let data = content.as_bytes().to_vec();
        self.get_operator()
            .await?
            .write(object_filepath, data)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write content: {}", e),
                )
            })?;
        Ok(())
    }

    /// Delete all given objects.
    /// TODO(hjiang): Add test and documentation on partial failure.
    async fn delete_all_objects(
        &self,
        objects_to_delete: Vec<String>,
    ) -> Result<(), Box<dyn Error>> {
        let futures = objects_to_delete.into_iter().map(|object| async move {
            let op = self.get_operator().await.unwrap().clone();
            op.delete(&object).await
        });

        let results = join_all(futures).await;
        for result in results {
            result?;
        }

        Ok(())
    }

    /// Load metadata and its location foe the given table.
    async fn load_metadata(
        &self,
        table_ident: &TableIdent,
    ) -> IcebergResult<(String /*metadata_filepath*/, TableMetadata)> {
        // Read version hint for the table to get latest version.
        let version_hint_filepath = format!(
            "{}/{}/metadata/version-hint.text",
            table_ident.namespace().to_url_string(),
            table_ident.name(),
        );
        let version_str = self
            .read_object(&version_hint_filepath)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to read version hint file on load table: {}", e),
                )
            })?;
        let version = version_str
            .trim()
            .parse::<u32>()
            .map_err(|e| IcebergError::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        // Read and parse table metadata.
        let metadata_filepath = format!(
            "{}/{}/metadata/v{}.metadata.json",
            table_ident.namespace().to_url_string(),
            table_ident.name(),
            version,
        );
        let metadata_str = self.read_object(&metadata_filepath).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to read table metadata file on load table: {}", e),
            )
        })?;
        let metadata = serde_json::from_slice::<TableMetadata>(metadata_str.as_bytes())
            .map_err(|e| IcebergError::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        Ok((metadata_filepath, metadata))
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

    fn set_puffin_file_to_remove(&mut self, puffin_filepaths: HashSet<String>) {
        assert!(self.puffin_blobs_to_remove.is_empty());
        self.puffin_blobs_to_remove = puffin_filepaths;
    }

    fn clear_puffin_metadata(&mut self) {
        self.puffin_blobs_to_add.clear();
        self.puffin_blobs_to_remove.clear();
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
                        "When list namespace, parent namespace {:?} doesn't exist.",
                        namespace_ident
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
            .list_direct_subdirectories(&parent_directory)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to list namespaces: {}", e),
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
                    format!(
                        "Parent Namespace {:?} doesn't exists",
                        parent_namespace_ident
                    ),
                ));
            }
        }
        self.write_object(
            &FileCatalog::get_namespace_indicator_name(namespace_ident),
            /*content=*/ "",
        )
        .await
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to write metadata file at namespace creation: {}", e),
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
            format!("Namespace {:?} does not exist", namespace_ident),
        ))
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<bool> {
        match self
            .get_operator()
            .await?
            .stat(&FileCatalog::get_namespace_indicator_name(namespace_ident))
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => return Ok(false),
        }
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<()> {
        let key = FileCatalog::get_namespace_indicator_name(namespace_ident);

        self.get_operator().await?.delete(&key).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to drop namespace: {}", e),
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
                format!(
                    "Namespace {:?} doesn't exist when list tables within.",
                    namespace_ident
                ),
            ));
        }

        let parent_directory = namespace_ident.to_url_string();
        let subdirectories = self
            .list_direct_subdirectories(&parent_directory)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to list tables: {}", e),
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
        let version_hint_filepath =
            format!("{}/{}/metadata/version-hint.text", directory, creation.name);
        self.write_object(&version_hint_filepath, /*content=*/ "0")
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write version hint file at table creation: {}", e),
                )
            })?;

        // Create metadata file.
        let metadata_filepath = format!(
            "{}/{}/metadata/v0.metadata.json",
            directory,
            creation.name.clone()
        );

        let table_metadata = TableMetadataBuilder::from_table_creation(creation)?.build()?;
        let metadata_json = serde_json::to_string(&table_metadata.metadata)?;
        self.write_object(&metadata_filepath, /*content=*/ &metadata_json)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write metadata file at table creation: {}", e),
                )
            })?;

        let table = Table::builder()
            .metadata(table_metadata.metadata)
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
        let objects = self
            .list_objects_under_directory(&directory)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to get objects under {}: {}", directory, e),
                )
            })?;
        self.delete_all_objects(objects).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to delete objects: {}", e),
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
            .object_exists(version_hint_filepath.to_str().unwrap())
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Failed to check object existence when get table existence: {}",
                        e
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
    ///
    /// TODO(hjiang): Implement table requirements, which indicates user-defined compare-and-swap logic.
    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        let (metadata_filepath, metadata) = self.load_metadata(commit.identifier()).await?;
        let version = metadata.next_sequence_number();
        let mut builder = TableMetadataBuilder::new_from_metadata(
            metadata.clone(),
            /*current_file_location=*/ Some(metadata_filepath.clone()),
        );

        let updates = commit.take_updates();
        for update in &updates {
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
                _ => {
                    unreachable!("Only snapshot updates are expected in this implementation");
                }
            }
        }

        // Construct new metadata with updates.
        let metadata = builder.build()?.metadata;

        // Write metadata file.
        let metadata_directory = format!(
            "{}/{}/metadata",
            commit.identifier().namespace().to_url_string(),
            commit.identifier().name()
        );
        let new_metadata_filepath = format!("{}/v{}.metadata.json", metadata_directory, version,);
        let metadata_json = serde_json::to_string(&metadata)?;
        self.write_object(&new_metadata_filepath, &metadata_json)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write metadata file at table update: {}", e),
                )
            })?;

        // Manifest files and manifest list has persisted into storage, make modifications based on puffin blobs.
        //
        // TODO(hjiang): Add unit test for update and check manifest population.
        append_puffin_metadata_and_rewrite(
            &metadata,
            &self.file_io,
            &self.puffin_blobs_to_add,
            &self.puffin_blobs_to_remove,
        )
        .await?;

        // Write version hint file.
        let version_hint_path = format!("{}/version-hint.text", metadata_directory);
        self.write_object(&version_hint_path, &format!("{version}"))
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write version hint file at table update: {}", e),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::iceberg::catalog_test_utils;
    #[cfg(feature = "storage-s3")]
    use crate::storage::iceberg::s3_test_utils;

    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;

    use iceberg::spec::{
        NestedField, PrimitiveType, Schema, SnapshotReference, SnapshotRetention,
        Type as IcebergType, MAIN_BRANCH,
    };
    use iceberg::NamespaceIdent;
    use iceberg::Result as IcebergResult;

    // Create S3 catalog with local minio deployment and a random bucket.
    #[cfg(feature = "storage-s3")]
    async fn create_s3_catalog() -> FileCatalog {
        let (bucket_name, warehouse_uri) = s3_test_utils::get_test_minio_bucket_and_warehouse();
        s3_test_utils::object_store_test_utils::create_test_s3_bucket(bucket_name.clone())
            .await
            .unwrap();
        s3_test_utils::create_minio_s3_catalog(&bucket_name, &warehouse_uri)
    }

    // Test util function to get iceberg schema,
    async fn get_test_schema() -> IcebergResult<Schema> {
        let field = NestedField::required(
            /*id=*/ 1,
            "field_name".to_string(),
            IcebergType::Primitive(PrimitiveType::Int),
        );
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(field)])
            .build()?;

        Ok(schema)
    }

    // Test util function to create a new table.
    async fn create_test_table(catalog: &FileCatalog) -> IcebergResult<()> {
        // Define namespace and table.
        let namespace = NamespaceIdent::from_strs(["default"])?;
        let table_name = "test_table".to_string();

        let schema = get_test_schema().await?;
        let table_creation = TableCreation::builder()
            .name(table_name.clone())
            .location(format!(
                "{}/{}/{}",
                catalog.warehouse_location,
                namespace.to_url_string(),
                table_name
            ))
            .schema(schema.clone())
            .build();

        catalog
            .create_namespace(&namespace, /*properties=*/ HashMap::new())
            .await?;
        catalog.create_table(&namespace, table_creation).await?;

        Ok(())
    }

    async fn test_catalog_namespace_operations_impl(catalog: FileCatalog) -> IcebergResult<()> {
        let namespace = NamespaceIdent::from_strs(vec!["default", "ns"])?;

        // Ensure namespace does not exist.
        let exists = catalog.namespace_exists(&namespace).await?;
        assert!(!exists, "Namespace should not exist before creation");

        // Create parent namespace.
        catalog
            .create_namespace(
                &NamespaceIdent::from_strs(vec!["default"]).unwrap(),
                /*properties=*/ HashMap::new(),
            )
            .await?;

        // Create namespace and check.
        catalog
            .create_namespace(&namespace, /*properties=*/ HashMap::new())
            .await?;

        let exists = catalog.namespace_exists(&namespace).await?;
        assert!(exists, "Namespace should exist after creation");

        // Get the namespace and check.
        let ns = catalog.get_namespace(&namespace).await?;
        assert_eq!(ns.name(), &namespace, "Namespace should match created one");

        // Drop the namespace and check.
        catalog.drop_namespace(&namespace).await?;
        let exists = catalog.namespace_exists(&namespace).await?;
        assert!(!exists, "Namespace should not exist after drop");

        Ok(())
    }

    async fn test_catalog_table_operations_impl(catalog: FileCatalog) -> IcebergResult<()> {
        // Define namespace and table.
        let namespace = NamespaceIdent::from_strs(vec!["default"])?;
        let table_name = "test_table".to_string();
        let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

        // Ensure table does not exist.
        let table_already_exists = catalog.table_exists(&table_ident).await?;
        assert!(
            !table_already_exists,
            "Table should not exist before creation"
        );

        // TODO(hjiang): Add testcase to check list table here.

        create_test_table(&catalog).await?;
        let table_already_exists = catalog.table_exists(&table_ident).await?;
        assert!(table_already_exists, "Table should exist after creation");

        let tables = catalog.list_tables(&namespace).await?;
        assert_eq!(tables.len(), 1);
        assert!(tables.contains(&table_ident));

        // Load table and check.
        let table = catalog.load_table(&table_ident).await?;
        let expected_schema = get_test_schema().await?;
        assert_eq!(
            table.identifier(),
            &table_ident,
            "Loaded table identifier should match"
        );
        assert_eq!(
            *table.metadata().current_schema().as_ref(),
            expected_schema,
            "Loaded table schema should match"
        );

        // Drop the table and check.
        catalog.drop_table(&table_ident).await?;
        let table_already_exists = catalog.table_exists(&table_ident).await?;
        assert!(!table_already_exists, "Table should not exist after drop");

        Ok(())
    }

    async fn test_list_operation_impl(catalog: FileCatalog) -> IcebergResult<()> {
        // List namespaces with non-existent parent namespace.
        let res = catalog
            .list_namespaces(Some(
                &NamespaceIdent::from_strs(["non-existent-ns"]).unwrap(),
            ))
            .await;
        assert!(
            res.is_err(),
            "List namespace under a non-existent namespace should fail"
        );
        let err = res.err().unwrap();
        assert_eq!(
            err.kind(),
            iceberg::ErrorKind::NamespaceNotFound,
            "List namespace under a non-existent namespace gets error {:?}",
            err
        );

        // List tables with non-existent parent namespace.
        let res = catalog
            .list_tables(&NamespaceIdent::from_strs(["non-existent-ns"]).unwrap())
            .await;
        assert!(
            res.is_err(),
            "List tables under a non-existent namespace should fail"
        );
        let err = res.err().unwrap();
        assert_eq!(
            err.kind(),
            iceberg::ErrorKind::NamespaceNotFound,
            "List namespace under a non-existent namespace gets error {:?}",
            err
        );

        // Create default namespace.
        let default_namespace = NamespaceIdent::from_strs(["default"])?;
        catalog
            .create_namespace(&default_namespace, /*properties=*/ HashMap::new())
            .await?;

        // Create two children namespaces under default namespace.
        let child_namespace_1 = NamespaceIdent::from_strs(["default", "child1"])?;
        catalog
            .create_namespace(&child_namespace_1, /*properties=*/ HashMap::new())
            .await?;
        let child_namespace_2 = NamespaceIdent::from_strs(["default", "child2"])?;
        catalog
            .create_namespace(&child_namespace_2, /*properties=*/ HashMap::new())
            .await?;

        // Create two tables under default namespace.
        let table_creation_1 =
            catalog_test_utils::create_test_table_creation(&default_namespace, "child_table_1")?;
        catalog
            .create_table(&default_namespace, table_creation_1)
            .await?;

        let table_creation_2 =
            catalog_test_utils::create_test_table_creation(&default_namespace, "child_table_2")?;
        catalog
            .create_table(&default_namespace, table_creation_2)
            .await?;

        // List default namespace and check.
        let res = catalog.list_namespaces(/*parent=*/ None).await?;
        assert_eq!(
            res.len(),
            1,
            "Only default namespace expected under root, but actually there're {:?}",
            res
        );
        assert_eq!(res[0].to_url_string(), "default");

        // List namespaces under default namespace and check.
        let res = catalog
            .list_namespaces(Some(&NamespaceIdent::from_strs(["default"]).unwrap()))
            .await?;
        assert_eq!(
            res.len(),
            2,
            "Expect two children namespaces, actually there're {:?}",
            res
        );
        assert!(
            res.contains(&child_namespace_1),
            "Expects children namespace {:?}, but actually {:?}",
            child_namespace_1,
            res
        );
        assert!(
            res.contains(&child_namespace_2),
            "Expects children namespace {:?}, but actually {:?}",
            child_namespace_2,
            res
        );

        // List tables under default namespace and check.
        let child_table_1 = TableIdent::new(default_namespace.clone(), "child_table_1".to_string());
        let child_table_2 = TableIdent::new(default_namespace.clone(), "child_table_2".to_string());

        let res = catalog
            .list_tables(&NamespaceIdent::from_strs(["default"]).unwrap())
            .await?;
        assert_eq!(
            res.len(),
            2,
            "Expect two children tables, actually there're {:?}",
            res
        );
        assert!(
            res.contains(&child_table_1),
            "Expects children table {:?}, but actually {:?}",
            child_table_1,
            res
        );
        assert!(
            res.contains(&child_table_2),
            "Expects children table {:?}, but actually {:?}",
            child_table_2,
            res
        );

        Ok(())
    }

    async fn test_update_table_impl(mut catalog: FileCatalog) -> IcebergResult<()> {
        create_test_table(&catalog).await?;

        let namespace = NamespaceIdent::from_strs(["default"])?;
        let table_name = "test_table".to_string();
        let table_ident = TableIdent::new(namespace.clone(), table_name.clone());
        catalog.load_metadata(&table_ident).await?;

        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let mut table_updates = vec![];
        table_updates.append(&mut vec![
            TableUpdate::AddSnapshot {
                snapshot: iceberg::spec::Snapshot::builder()
                    .with_snapshot_id(1)
                    .with_sequence_number(1)
                    .with_timestamp_ms(millis as i64)
                    .with_schema_id(0)
                    .with_manifest_list(format!(
                        "s3://{}/{}/snap-8161620281254644995-0-01966b87-6e93-7bc1-9e12-f1980d9737d3.avro",
                        namespace.to_url_string(),
                        table_name
                    ))
                    .with_parent_snapshot_id(None)
                    .with_summary(iceberg::spec::Summary {
                        operation: iceberg::spec::Operation::Append,
                        additional_properties: HashMap::new(),
                    })
                    .build(),
            },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference {
                    snapshot_id: 1,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            }
        ]);

        // TODO(hjiang): This is a hack to create `TableCommit`, because its builder is only exposed to crate instead of public.
        #[repr(C)]
        struct TableCommitProxy {
            ident: TableIdent,
            requirements: Vec<iceberg::TableRequirement>,
            updates: Vec<TableUpdate>,
        }
        let table_commit_proxy = TableCommitProxy {
            ident: table_ident.clone(),
            requirements: vec![],
            updates: table_updates,
        };
        let table_commit =
            unsafe { std::mem::transmute::<TableCommitProxy, TableCommit>(table_commit_proxy) };

        // Check table metadata.
        let table = catalog.update_table(table_commit).await?;
        catalog.clear_puffin_metadata();

        let table_metadata = table.metadata();
        assert_eq!(
            **table_metadata.current_schema(),
            get_test_schema().await.unwrap(),
            "Schema should match"
        );
        assert_eq!(
            table.identifier(),
            &table_ident,
            "Updated table identifier should match"
        );
        assert_eq!(
            table_metadata.current_snapshot_id(),
            Some(1),
            "Current snapshot ID should be 1"
        );

        Ok(())
    }

    /// -------------------------
    /// Namespace operations test.
    #[tokio::test]
    async fn test_catalog_namespace_operations_filesystem() -> IcebergResult<()> {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let warehouse_path = temp_dir.path().to_str().unwrap();
        let catalog = FileCatalog::new(warehouse_path.to_string(), CatalogConfig::FileSystem {})?;
        test_catalog_namespace_operations_impl(catalog).await
    }
    #[tokio::test]
    #[cfg(feature = "storage-s3")]
    async fn test_catalog_namespace_operations_s3() -> IcebergResult<()> {
        let catalog = create_s3_catalog().await;
        test_catalog_namespace_operations_impl(catalog).await
    }

    /// Table operations test.
    #[tokio::test]
    async fn test_catalog_table_operations_filesystem() -> IcebergResult<()> {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let warehouse_path = temp_dir.path().to_str().unwrap();
        let catalog = FileCatalog::new(warehouse_path.to_string(), CatalogConfig::FileSystem {})?;
        test_catalog_table_operations_impl(catalog).await
    }
    #[tokio::test]
    #[cfg(feature = "storage-s3")]
    async fn test_catalog_table_operations_s3() -> IcebergResult<()> {
        let catalog = create_s3_catalog().await;
        test_catalog_table_operations_impl(catalog).await
    }

    /// List operation test.
    #[tokio::test]
    async fn test_list_operation_filesystem() -> IcebergResult<()> {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let warehouse_path = temp_dir.path().to_str().unwrap();
        let catalog = FileCatalog::new(warehouse_path.to_string(), CatalogConfig::FileSystem {})?;
        test_list_operation_impl(catalog).await
    }
    #[tokio::test]
    #[cfg(feature = "storage-s3")]
    async fn test_list_operation_s3() -> IcebergResult<()> {
        let catalog = create_s3_catalog().await;
        test_list_operation_impl(catalog).await
    }

    /// Update table test.
    #[tokio::test]
    async fn test_update_table_filesystem() -> IcebergResult<()> {
        let temp_dir = TempDir::new().expect("tempdir failed");
        let warehouse_path = temp_dir.path().to_str().unwrap();
        let catalog = FileCatalog::new(warehouse_path.to_string(), CatalogConfig::FileSystem {})?;
        test_update_table_impl(catalog).await
    }
    #[tokio::test]
    #[cfg(feature = "storage-s3")]
    async fn test_update_table_s3() -> IcebergResult<()> {
        let catalog = create_s3_catalog().await;
        create_test_table(&catalog).await?;
        test_update_table_impl(catalog).await
    }
}
