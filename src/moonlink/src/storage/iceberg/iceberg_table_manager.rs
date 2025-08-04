use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::iceberg::catalog_utils;
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::table_manager::{
    PersistenceFileParams, PersistenceResult, TableManager,
};
use crate::storage::iceberg::utils;
use crate::storage::index::FileIndex as MooncakeFileIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::storage_utils::FileId;
use crate::{IcebergTableConfig, ObjectStorageCache};

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::spec::DataFile;
use iceberg::table::Table as IcebergTable;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::writer::file_writer::location_generator::LocationGenerator;
use iceberg::{NamespaceIdent, Result as IcebergResult, TableIdent};
use uuid::Uuid;

/// TODO(hjiang): store snapshot property in snapshot summary, instead of table property.
///
/// Key for iceberg snapshot property, to record flush lsn.
pub(super) const MOONCAKE_TABLE_FLUSH_LSN: &str = "moonlink.table-flush-lsn";
/// Key for iceberg snapshot property, to record accompanying WAL metadata.
pub(super) const MOONCAKE_WAL_METADATA: &str = "moonlink.wal-metadata";
/// Used to represent uninitialized deletion vector.
/// TODO(hjiang): Consider using `Option<>` to represent uninitialized, which is more rust-idiometic.
pub(super) const UNINITIALIZED_BATCH_DELETION_VECTOR_MAX_ROW: usize = 0;

#[derive(Clone, Debug)]
pub(crate) struct DataFileEntry {
    /// Iceberg data file, used to decide what to persist at new commit requests.
    pub(crate) data_file: DataFile,
    /// In-memory deletion vector.
    pub(crate) deletion_vector: BatchDeletionVector,
}

#[derive(Debug)]
pub struct IcebergTableManager {
    /// Iceberg snapshot should be loaded only once at recovery, this boolean records whether recovery has attempted.
    pub(super) snapshot_loaded: bool,

    /// Iceberg table configuration.
    pub(crate) config: IcebergTableConfig,

    /// Mooncake table metadata.
    pub(crate) mooncake_table_metadata: Arc<MooncakeTableMetadata>,

    /// Iceberg catalog, which interacts with the iceberg table.
    pub(crate) catalog: Box<dyn MoonlinkCatalog>,

    /// The iceberg table it's managing.
    pub(crate) iceberg_table: Option<IcebergTable>,

    /// Object storage cache.
    pub(crate) object_storage_cache: ObjectStorageCache,

    /// Filesystem accessor.
    pub(crate) filesystem_accessor: Arc<dyn BaseFileSystemAccess>,

    /// Maps from already persisted data file filepath to its deletion vector, and iceberg `DataFile`.
    pub(crate) persisted_data_files: HashMap<FileId, DataFileEntry>,

    /// Maps from mooncake file index to remote puffin filepath.
    pub(crate) persisted_file_indices: HashMap<MooncakeFileIndex, String>,

    /// Maps from remote data file path to its file id.
    pub(crate) remote_data_file_to_file_id: HashMap<String, FileId>,
}

impl IcebergTableManager {
    pub fn new(
        mooncake_table_metadata: Arc<MooncakeTableMetadata>,
        object_storage_cache: ObjectStorageCache,
        filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
        config: IcebergTableConfig,
    ) -> IcebergResult<IcebergTableManager> {
        let iceberg_schema =
            iceberg::arrow::arrow_schema_to_schema(mooncake_table_metadata.schema.as_ref())?;
        let catalog =
            catalog_utils::create_catalog(config.accessor_config.clone(), iceberg_schema)?;
        Ok(Self {
            snapshot_loaded: false,
            config,
            mooncake_table_metadata,
            catalog,
            iceberg_table: None,
            object_storage_cache,
            filesystem_accessor,
            persisted_data_files: HashMap::new(),
            persisted_file_indices: HashMap::new(),
            remote_data_file_to_file_id: HashMap::new(),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_with_filesystem_accessor(
        mooncake_table_metadata: Arc<MooncakeTableMetadata>,
        object_storage_cache: ObjectStorageCache,
        filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
        config: IcebergTableConfig,
    ) -> IcebergResult<IcebergTableManager> {
        let iceberg_schema =
            iceberg::arrow::arrow_schema_to_schema(mooncake_table_metadata.schema.as_ref())?;
        let catalog = catalog_utils::create_catalog_with_filesystem_accessor(
            filesystem_accessor.clone(),
            iceberg_schema,
        )?;
        Ok(Self {
            snapshot_loaded: false,
            config,
            mooncake_table_metadata,
            catalog,
            iceberg_table: None,
            object_storage_cache,
            filesystem_accessor,
            persisted_data_files: HashMap::new(),
            persisted_file_indices: HashMap::new(),
            remote_data_file_to_file_id: HashMap::new(),
        })
    }

    /// Get table identity.
    pub(super) fn get_table_ident(&self) -> TableIdent {
        TableIdent {
            namespace: NamespaceIdent::from_strs(&self.config.namespace).unwrap(),
            name: self.config.table_name.clone(),
        }
    }

    /// Get a unique puffin filepath under table warehouse uri.
    pub(super) fn get_unique_deletion_vector_filepath(&self) -> String {
        let location_generator =
            DefaultLocationGenerator::new(self.iceberg_table.as_ref().unwrap().metadata().clone())
                .unwrap();
        location_generator
            .generate_location(&format!("{}-deletion-vector-v1-puffin.bin", Uuid::now_v7()))
    }
    pub(super) fn get_unique_hash_index_v1_filepath(&self) -> String {
        let location_generator =
            DefaultLocationGenerator::new(self.iceberg_table.as_ref().unwrap().metadata().clone())
                .unwrap();
        location_generator
            .generate_location(&format!("{}-hash-index-v1-puffin.bin", Uuid::now_v7()))
    }

    /// Get or create an iceberg table based on the iceberg manager config.
    ///
    /// This function is executed in a lazy style, so no iceberg table will get created if
    /// (1) It doesn't exist before any mooncake table events
    /// (2) Iceberg snapshot is not requested to create
    pub(crate) async fn initialize_iceberg_table_for_once(&mut self) -> IcebergResult<()> {
        if self.iceberg_table.is_none() {
            let table = utils::get_or_create_iceberg_table(
                &*self.catalog,
                &self.config.accessor_config.get_root_path(),
                &self.config.namespace,
                &self.config.table_name,
                self.mooncake_table_metadata.schema.as_ref(),
            )
            .await?;
            self.iceberg_table = Some(table);
        }
        Ok(())
    }

    /// Initialize table if it exists.
    pub(crate) async fn initialize_iceberg_table_if_exists(&mut self) -> IcebergResult<()> {
        assert!(self.iceberg_table.is_none());
        self.iceberg_table = utils::get_table_if_exists(
            &*self.catalog,
            &self.config.namespace,
            &self.config.table_name,
        )
        .await?;
        Ok(())
    }
}

/// TODO(hjiang): Parallelize all IO operations.
#[async_trait]
impl TableManager for IcebergTableManager {
    fn get_warehouse_location(&self) -> String {
        self.config.accessor_config.get_root_path()
    }

    async fn sync_snapshot(
        &mut self,
        mut snapshot_payload: IcebergSnapshotPayload,
        file_params: PersistenceFileParams,
    ) -> IcebergResult<PersistenceResult> {
        // Persist data files, deletion vectors, and file indices.
        let new_table_schema = std::mem::take(&mut snapshot_payload.new_table_schema);
        let persistence_result = self
            .sync_snapshot_impl(snapshot_payload, file_params)
            .await?;

        // Perform schema evolution if necessary.
        if let Some(new_table_schema) = new_table_schema {
            self.alter_table_schema_impl(new_table_schema).await?;
        }
        Ok(persistence_result)
    }

    async fn load_snapshot_from_table(&mut self) -> IcebergResult<(u32, MooncakeSnapshot)> {
        self.load_snapshot_from_table_impl().await
    }

    async fn drop_table(&mut self) -> IcebergResult<()> {
        let table_ident = TableIdent::new(
            NamespaceIdent::from_strs(&self.config.namespace).unwrap(),
            self.config.table_name.clone(),
        );
        self.catalog.drop_table(&table_ident).await
    }
}
