use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
};
use crate::storage::iceberg::index::FileIndexBlob;
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::iceberg::utils;
use crate::storage::iceberg::validation as IcebergValidation;
use crate::storage::index::{FileIndex as MooncakeFileIndex, MooncakeIndex};
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::DiskFileDeletionVector;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::storage_utils::{create_data_file, MooncakeDataFileRef};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::vec;

use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::puffin::CompressionCodec;
use iceberg::spec::DataFileFormat;
use iceberg::spec::{DataFile, ManifestEntry};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::Transaction;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::writer::file_writer::location_generator::LocationGenerator;
use iceberg::{NamespaceIdent, Result as IcebergResult, TableIdent};
use typed_builder::TypedBuilder;
use uuid::Uuid;

#[cfg(test)]
use mockall::*;

/// Key for iceberg table property, to record flush lsn.
const MOONCAKE_TABLE_FLUSH_LSN: &str = "mooncake-table-flush-lsn";
/// Used to represent uninitialized deletion vector.
/// TODO(hjiang): Consider using `Option<>` to represent uninitialized, which is more rust-idiometic.
const UNINITIALIZED_BATCH_DELETION_VECTOR_MAX_ROW: usize = 0;

#[derive(Clone, Debug, TypedBuilder)]
pub struct IcebergTableConfig {
    /// Table warehouse location.
    #[builder(default = "/tmp/moonlink_iceberg".to_string())]
    pub warehouse_uri: String,
    /// Namespace for the iceberg table.
    #[builder(default = vec!["default".to_string()])]
    pub namespace: Vec<String>,
    /// Iceberg table name.
    #[builder(default = "table".to_string())]
    pub table_name: String,
}

#[async_trait]
#[cfg_attr(test, automock)]
pub trait TableManager: Send {
    /// Write a new snapshot to iceberg table.
    /// It could be called for multiple times to write and commit multiple snapshots.
    ///
    /// - Apart from data files, it also supports deletion vector (which is introduced in v3) and self-defined hash index,
    ///   both of which are stored in puffin files.
    /// - For deletion vectors, we store one blob in one puffin file.
    /// - For hash index, we store one mooncake file index in one puffin file.
    #[allow(async_fn_in_trait)]
    async fn sync_snapshot(
        &mut self,
        snapshot_payload: IcebergSnapshotPayload,
    ) -> IcebergResult<HashMap<MooncakeDataFileRef, PuffinBlobRef>>;

    /// Load latest snapshot from iceberg table. Used for recovery and initialization.
    /// Notice this function is supposed to call **only once**.
    #[allow(async_fn_in_trait)]
    async fn load_snapshot_from_table(&mut self) -> IcebergResult<MooncakeSnapshot>;

    /// Drop the current iceberg table.
    #[allow(async_fn_in_trait)]
    async fn drop_table(&mut self) -> IcebergResult<()>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DataFileEntry {
    /// Iceberg data file, used to decide what to persist at new commit requests.
    pub(crate) data_file: DataFile,
    /// In-memory deletion vector.
    pub(crate) deletion_vector: BatchDeletionVector,
    /// Puffin blob for its deletion vector.
    persisted_deletion_vector: Option<PuffinBlobRef>,
}

/// TODO(hjiang):
/// 1. Support a data file handle, which is a remote file path, plus an optional local cache filepath.
/// 2. Support a deletion vector handle, which is a remote file path, with an optional in-memory buffer and a local cache filepath.
#[derive(Debug)]
pub struct IcebergTableManager {
    /// Iceberg snapshot should be loaded only once at recovery, this boolean records whether recovery has attempted.
    snapshot_loaded: bool,

    /// Iceberg table configuration.
    pub(crate) config: IcebergTableConfig,

    /// Mooncake table metadata.
    pub(crate) mooncake_table_metadata: Arc<MooncakeTableMetadata>,

    /// Iceberg catalog, which interacts with the iceberg table.
    pub(crate) catalog: Box<dyn MoonlinkCatalog>,

    /// The iceberg table it's managing.
    pub(crate) iceberg_table: Option<IcebergTable>,

    /// Maps from already persisted data file filepath to its deletion vector, and iceberg `DataFile`.
    ///
    /// TODO(hjiang): Consider using `MooncakeDataFileRef` as map key.
    pub(crate) persisted_data_files: HashMap<String, DataFileEntry>,

    /// Maps from mooncake file index to remote puffin filepath.
    pub(crate) persisted_file_indices: HashMap<MooncakeFileIndex, String>,
}

impl IcebergTableManager {
    pub fn new(
        mooncake_table_metadata: Arc<MooncakeTableMetadata>,
        config: IcebergTableConfig,
    ) -> IcebergResult<IcebergTableManager> {
        let catalog = utils::create_catalog(&config.warehouse_uri)?;
        Ok(Self {
            snapshot_loaded: false,
            config,
            mooncake_table_metadata,
            catalog,
            iceberg_table: None,
            persisted_data_files: HashMap::new(),
            persisted_file_indices: HashMap::new(),
        })
    }

    /// Get a unique puffin filepath under table warehouse uri.
    fn get_unique_deletion_vector_filepath(&self) -> String {
        let location_generator =
            DefaultLocationGenerator::new(self.iceberg_table.as_ref().unwrap().metadata().clone())
                .unwrap();
        location_generator
            .generate_location(&format!("{}-deletion-vector-v1-puffin.bin", Uuid::new_v4()))
    }
    fn get_unique_hash_index_v1_filepath(&self) -> String {
        let location_generator =
            DefaultLocationGenerator::new(self.iceberg_table.as_ref().unwrap().metadata().clone())
                .unwrap();
        location_generator
            .generate_location(&format!("{}-hash-index-v1-puffin.bin", Uuid::new_v4()))
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
                &self.config.warehouse_uri,
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

    /// ---------- load snapshot ----------
    ///
    /// Load index file into table manager from the current manifest entry.
    async fn load_file_indices_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
        file_io: &FileIO,
    ) -> IcebergResult<Vec<MooncakeFileIndex>> {
        if !utils::is_file_index(entry) {
            return Ok(vec![]);
        }

        let mut file_index_blob =
            FileIndexBlob::load_from_index_blob(file_io.clone(), entry.data_file()).await?;
        let file_index_futures = file_index_blob
            .file_indices
            .iter_mut()
            .map(|cur_file_index| cur_file_index.as_mooncake_file_index());
        let file_indices = futures::future::join_all(file_index_futures).await;

        self.persisted_file_indices.reserve(file_indices.len());
        file_indices.iter().for_each(|cur_file_index| {
            self.persisted_file_indices.insert(
                cur_file_index.clone(),
                entry.data_file().file_path().to_string(),
            );
        });

        Ok(file_indices)
    }

    /// Load data file into table manager from the current manifest entry.
    async fn load_data_file_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
    ) -> IcebergResult<()> {
        if !utils::is_data_file_entry(entry) {
            return Ok(());
        }

        let data_file = entry.data_file();
        let file_path = PathBuf::from(data_file.file_path().to_string());
        assert_eq!(data_file.file_format(), DataFileFormat::Parquet);
        let new_data_file_entry = DataFileEntry {
            data_file: data_file.clone(),
            deletion_vector: BatchDeletionVector::new(UNINITIALIZED_BATCH_DELETION_VECTOR_MAX_ROW),
            persisted_deletion_vector: None,
        };
        let old_entry = self
            .persisted_data_files
            .insert(file_path.to_string_lossy().to_string(), new_data_file_entry);
        assert!(old_entry.is_none());
        Ok(())
    }

    /// Load deletion vector into table manager from the current manifest entry.
    async fn load_deletion_vector_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
        file_io: &FileIO,
    ) -> IcebergResult<()> {
        // Skip data files and file indices.
        if !utils::is_deletion_vector_entry(entry) {
            return Ok(());
        }

        let data_file = entry.data_file();
        let referenced_filepath = data_file.referenced_data_file().unwrap();
        let mut data_file_entry = self.persisted_data_files.get_mut(&referenced_filepath);
        assert!(
            data_file_entry.is_some(),
            "At recovery, the data file path for {:?} doesn't exist",
            &referenced_filepath
        );

        IcebergValidation::validate_puffin_manifest_entry(entry)?;
        let deletion_vector = DeletionVector::load_from_dv_blob(file_io.clone(), data_file).await?;
        let batch_deletion_vector = deletion_vector
            .take_as_batch_delete_vector(self.mooncake_table_metadata.config.batch_size());
        data_file_entry.as_mut().unwrap().deletion_vector = batch_deletion_vector;
        data_file_entry.as_mut().unwrap().persisted_deletion_vector = Some(PuffinBlobRef {
            puffin_filepath: data_file.file_path().to_string(),
            start_offset: data_file.content_offset().unwrap() as u32,
            blob_size: data_file.content_size_in_bytes().unwrap() as u32,
        });

        Ok(())
    }

    /// Util function to transform iceberg table status to mooncake table snapshot.
    fn transform_to_mooncake_snapshot(
        &self,
        persisted_file_indices: Vec<MooncakeFileIndex>,
        flush_lsn: Option<u64>,
    ) -> MooncakeSnapshot {
        let mut mooncake_snapshot = MooncakeSnapshot::new(self.mooncake_table_metadata.clone());

        // Assign snapshot version.
        let iceberg_table_metadata = self.iceberg_table.as_ref().unwrap().metadata();
        mooncake_snapshot.snapshot_version =
            if let Some(ver) = iceberg_table_metadata.current_snapshot_id() {
                ver as u64
            } else {
                0
            };
        // Fill in disk files.
        mooncake_snapshot.disk_files = HashMap::with_capacity(self.persisted_data_files.len());
        for (file_id, (data_filepath, data_file_entry)) in
            self.persisted_data_files.iter().enumerate()
        {
            mooncake_snapshot.disk_files.insert(
                create_data_file(file_id as u64, data_filepath.to_string()),
                DiskFileDeletionVector {
                    puffin_deletion_blob: data_file_entry.persisted_deletion_vector.clone(),
                    batch_deletion_vector: data_file_entry.deletion_vector.clone(),
                },
            );
        }
        // UNDONE:
        // 1. Update file id in persisted_file_indices.

        // Fill in indices.
        mooncake_snapshot.indices = MooncakeIndex {
            in_memory_index: HashSet::new(),
            file_indices: persisted_file_indices,
        };

        // Fill in flush LSN.
        mooncake_snapshot.data_file_flush_lsn = flush_lsn;

        mooncake_snapshot
    }

    /// ---------- store snapshot ----------
    ///
    /// Write deletion vector to puffin file.
    /// Precondition: batch deletion vector is not empty.
    ///
    /// Puffin blob write condition:
    /// 1. No compression is performed, otherwise it's hard to get blob size without another read operation.
    /// 2. We put one deletion vector within one puffin file.
    async fn write_deletion_vector(
        &mut self,
        data_file: String,
        deletion_vector: BatchDeletionVector,
    ) -> IcebergResult<PuffinBlobRef> {
        let deleted_rows = deletion_vector.collect_deleted_rows();
        assert!(!deleted_rows.is_empty());

        let deleted_row_count = deleted_rows.len();
        let mut iceberg_deletion_vector = DeletionVector::new();
        iceberg_deletion_vector.mark_rows_deleted(deleted_rows);
        let blob_properties = HashMap::from([
            (DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(), data_file),
            (
                DELETION_VECTOR_CADINALITY.to_string(),
                deleted_row_count.to_string(),
            ),
        ]);
        let blob = iceberg_deletion_vector.serialize(blob_properties);
        let blob_size = blob.data().len();
        let puffin_filepath = self.get_unique_deletion_vector_filepath();
        let mut puffin_writer = puffin_utils::create_puffin_writer(
            self.iceberg_table.as_ref().unwrap().file_io(),
            &puffin_filepath,
        )
        .await?;
        puffin_writer.add(blob, CompressionCodec::None).await?;

        self.catalog
            .record_puffin_metadata_and_close(puffin_filepath.clone(), puffin_writer)
            .await?;

        Ok(PuffinBlobRef {
            puffin_filepath,
            start_offset: 4_u32, // Puffin file starts with 4 magic bytes.
            blob_size: blob_size as u32,
        })
    }

    /// Dump local data files into iceberg table.
    /// Return new iceberg data files for append transaction, and local data filepath to remote data filepath for index block remapping.
    async fn sync_data_files(
        &mut self,
        new_data_files: Vec<MooncakeDataFileRef>,
        new_deletion_vector: &HashMap<MooncakeDataFileRef, BatchDeletionVector>,
    ) -> IcebergResult<(
        Vec<DataFile>,
        HashMap<String, String>, /*local to remote datafile mapping*/
    )> {
        // Maps from local data filepath to remote filepath.
        let mut local_data_file_to_remote = HashMap::with_capacity(new_data_files.len());

        let mut new_iceberg_data_files = Vec::with_capacity(new_data_files.len());
        for local_data_file in new_data_files.into_iter() {
            let iceberg_data_file = utils::write_record_batch_to_iceberg(
                self.iceberg_table.as_ref().unwrap(),
                local_data_file.file_path(),
                self.iceberg_table.as_ref().unwrap().metadata(),
            )
            .await?;
            local_data_file_to_remote.insert(
                local_data_file.file_path().clone(),
                iceberg_data_file.file_path().to_string(),
            );

            // Try get deletion vector batch size.
            let max_rows = new_deletion_vector
                .get(&local_data_file)
                .map(|dv| dv.get_max_rows());

            let old_entry = self.persisted_data_files.insert(
                local_data_file.file_path().to_string(),
                DataFileEntry {
                    data_file: iceberg_data_file.clone(),
                    deletion_vector: BatchDeletionVector::new(
                        max_rows.unwrap_or(UNINITIALIZED_BATCH_DELETION_VECTOR_MAX_ROW),
                    ),
                    persisted_deletion_vector: None,
                },
            );
            assert!(old_entry.is_none());
            new_iceberg_data_files.push(iceberg_data_file);
        }
        Ok((new_iceberg_data_files, local_data_file_to_remote))
    }

    /// Dump committed deletion logs into iceberg table, only the changed part will be persisted.
    async fn sync_deletion_vector(
        &mut self,
        new_deletion_logs: HashMap<MooncakeDataFileRef, BatchDeletionVector>,
    ) -> IcebergResult<HashMap<MooncakeDataFileRef, PuffinBlobRef>> {
        let mut puffin_deletion_blobs = HashMap::new();
        for (local_data_file, new_deletion_vector) in new_deletion_logs.into_iter() {
            let mut entry = self
                .persisted_data_files
                .get(local_data_file.file_path())
                .unwrap()
                .clone();

            if entry.deletion_vector.get_max_rows() == UNINITIALIZED_BATCH_DELETION_VECTOR_MAX_ROW {
                entry.deletion_vector =
                    BatchDeletionVector::new(new_deletion_vector.get_max_rows());
            }
            entry.deletion_vector.merge_with(&new_deletion_vector);

            // Data filepath in iceberg table.
            let iceberg_data_file = entry.data_file.file_path();
            let puffin_blob = self
                .write_deletion_vector(iceberg_data_file.to_string(), entry.deletion_vector.clone())
                .await?;
            self.persisted_data_files
                .insert(local_data_file.file_path().to_string(), entry);
            puffin_deletion_blobs.insert(local_data_file, puffin_blob);
        }
        Ok(puffin_deletion_blobs)
    }

    /// Process file indices to import.
    /// [`local_data_file_to_remote`] should contain all local data filepath to remote data filepath mapping.
    async fn import_file_indices(
        &mut self,
        file_indices_to_import: &[MooncakeFileIndex],
        local_data_file_to_remote: HashMap<String, String>,
    ) -> IcebergResult<()> {
        let puffin_filepath = self.get_unique_hash_index_v1_filepath();
        let mut puffin_writer = puffin_utils::create_puffin_writer(
            self.iceberg_table.as_ref().unwrap().file_io(),
            &puffin_filepath,
        )
        .await?;

        // TODO(hjiang): Maps from local filepath to remote filepath.
        // After sync, file index still stores local index file location.
        // After cache design, we should be able to provide a "handle" abstraction, which could be either local or remote.
        // The hash map here is merely a workaround to pass remote path to iceberg file index structure.
        let mut local_index_file_to_remote = HashMap::new();

        let mut new_file_indices: Vec<&MooncakeFileIndex> =
            Vec::with_capacity(file_indices_to_import.len());
        for cur_file_index in file_indices_to_import.iter() {
            new_file_indices.push(cur_file_index);
            // Upload new index file to iceberg table.
            for cur_index_block in cur_file_index.index_blocks.iter() {
                let remote_index_block = utils::upload_index_file(
                    self.iceberg_table.as_ref().unwrap(),
                    &cur_index_block.file_path,
                )
                .await?;
                local_index_file_to_remote
                    .insert(cur_index_block.file_path.clone(), remote_index_block);
            }
            self.persisted_file_indices
                .insert(cur_file_index.clone(), puffin_filepath.clone());
        }

        let file_index_blob = FileIndexBlob::new(
            new_file_indices,
            local_index_file_to_remote,
            local_data_file_to_remote,
        );
        let puffin_blob = file_index_blob.as_blob()?;
        puffin_writer
            .add(puffin_blob, iceberg::puffin::CompressionCodec::None)
            .await?;
        self.catalog
            .record_puffin_metadata_and_close(puffin_filepath, puffin_writer)
            .await?;

        Ok(())
    }

    /// Dump file indices into the iceberg table, only new file indices will be persisted into the table.
    /// Return file index ids which should be added into iceberg table.
    ///
    /// TODO(hjiang): Need to configure (1) the number of blobs in a puffin file; and (2) the number of file index in a puffin blob.
    /// For implementation simpicity, put everything in a single file and a single blob.
    async fn sync_file_indices(
        &mut self,
        file_indices_to_import: &[MooncakeFileIndex],
        file_indices_to_remove: &[MooncakeFileIndex],
        local_data_file_to_remote: HashMap<String, String>,
    ) -> IcebergResult<()> {
        // Invariant assertion: file indices to remove should only come from compaction, so there must be new file indices to import.
        if !file_indices_to_remove.is_empty() {
            assert!(!file_indices_to_import.is_empty());
        }

        if file_indices_to_import.is_empty() {
            return Ok(());
        }

        // Process file indices to import.
        self.import_file_indices(file_indices_to_import, local_data_file_to_remote)
            .await?;

        // Process file indices to remove.
        self.catalog.set_puffin_file_to_remove(
            file_indices_to_remove
                .iter()
                .map(|cur_index| self.persisted_file_indices.remove(cur_index).unwrap())
                .collect::<HashSet<String>>(),
        );

        Ok(())
    }

    /// Util function to merge local to remote data filepath mapping, with already persisted one.
    fn merge_local_to_remote_data_file_to_remote(
        &self,
        mut local_data_file_to_remote: HashMap<String, String>,
    ) -> HashMap<String, String> {
        local_data_file_to_remote
            .reserve(local_data_file_to_remote.len() + self.persisted_data_files.len());
        for (cur_local_filepath, cur_entry) in self.persisted_data_files.iter() {
            local_data_file_to_remote.insert(
                cur_local_filepath.clone(),
                cur_entry.data_file.file_path().to_string(),
            );
        }
        local_data_file_to_remote
    }
}

/// TODO(hjiang): Parallelize all IO operations.
#[async_trait]
impl TableManager for IcebergTableManager {
    async fn sync_snapshot(
        &mut self,
        mut snapshot_payload: IcebergSnapshotPayload,
    ) -> IcebergResult<HashMap<MooncakeDataFileRef, PuffinBlobRef>> {
        // Initialize iceberg table on access.
        self.initialize_iceberg_table_for_once().await?;

        // Persist data files.
        let (new_iceberg_data_files, local_data_file_to_remote) = self
            .sync_data_files(
                std::mem::take(&mut snapshot_payload.data_files),
                &snapshot_payload.new_deletion_vector,
            )
            .await?;

        // Update local data file to remote mapping.
        let local_data_file_to_remote =
            self.merge_local_to_remote_data_file_to_remote(local_data_file_to_remote);

        // Persist committed deletion logs.
        let deletion_puffin_blobs = self
            .sync_deletion_vector(std::mem::take(&mut snapshot_payload.new_deletion_vector))
            .await?;

        // Persist file index changes.
        self.sync_file_indices(
            &snapshot_payload.file_indices_to_import,
            &snapshot_payload.file_indices_to_remove,
            local_data_file_to_remote,
        )
        .await?;

        // Only start append action when there're new data files.
        let mut txn = Transaction::new(self.iceberg_table.as_ref().unwrap());
        if !new_iceberg_data_files.is_empty() {
            let mut action =
                txn.fast_append(/*commit_uuid=*/ None, /*key_metadata=*/ vec![])?;
            action.add_data_files(new_iceberg_data_files)?;
            txn = action.apply().await?;
        }

        // Persist flush lsn at table property, it's just a workaround.
        // The ideal solution is to store at snapshot summary additional properties.
        // Issue: https://github.com/apache/iceberg-rust/issues/1329
        let mut prop = HashMap::new();
        prop.insert(
            MOONCAKE_TABLE_FLUSH_LSN.to_string(),
            snapshot_payload.flush_lsn.to_string(),
        );
        txn = txn.set_properties(prop)?;

        self.iceberg_table = Some(txn.commit(&*self.catalog).await?);
        self.catalog.clear_puffin_metadata();

        Ok(deletion_puffin_blobs)
    }

    async fn load_snapshot_from_table(&mut self) -> IcebergResult<MooncakeSnapshot> {
        assert!(!self.snapshot_loaded);
        self.snapshot_loaded = true;

        // Handle cases which iceberg table doesn't exist.
        self.initialize_iceberg_table_if_exists().await?;
        if self.iceberg_table.is_none() {
            return Ok(MooncakeSnapshot::new(self.mooncake_table_metadata.clone()));
        }

        let table_metadata = self.iceberg_table.as_ref().unwrap().metadata();
        let mut flush_lsn: Option<u64> = None;
        if let Some(lsn) = table_metadata.properties().get(MOONCAKE_TABLE_FLUSH_LSN) {
            flush_lsn = Some(lsn.parse().unwrap());
        }

        // There's nothing stored in iceberg table.
        if table_metadata.current_snapshot().is_none() {
            return Ok(MooncakeSnapshot::new(self.mooncake_table_metadata.clone()));
        }

        // Load table state into iceberg table manager.
        let snapshot_meta = table_metadata.current_snapshot().unwrap();
        let manifest_list = snapshot_meta
            .load_manifest_list(
                self.iceberg_table.as_ref().unwrap().file_io(),
                table_metadata,
            )
            .await?;

        let file_io = self.iceberg_table.as_ref().unwrap().file_io().clone();
        let mut loaded_file_indices = vec![];
        for manifest_file in manifest_list.entries().iter() {
            let manifest = manifest_file.load_manifest(&file_io).await?;
            let (manifest_entries, _) = manifest.into_parts();
            assert!(
                !manifest_entries.is_empty(),
                "Shouldn't have empty manifest file"
            );

            // On load, we do two pass on all entries, to check whether all deletion vector has a corresponding data file.
            for entry in manifest_entries.iter() {
                self.load_data_file_from_manifest_entry(entry.as_ref())
                    .await?;
                let file_indices = self
                    .load_file_indices_from_manifest_entry(entry.as_ref(), &file_io)
                    .await?;
                loaded_file_indices.extend(file_indices);
            }
            for entry in manifest_entries.into_iter() {
                self.load_deletion_vector_from_manifest_entry(entry.as_ref(), &file_io)
                    .await?;
            }
        }

        let mooncake_snapshot = self.transform_to_mooncake_snapshot(loaded_file_indices, flush_lsn);
        Ok(mooncake_snapshot)
    }

    async fn drop_table(&mut self) -> IcebergResult<()> {
        let table_ident = TableIdent::new(
            NamespaceIdent::from_strs(&self.config.namespace).unwrap(),
            self.config.table_name.clone(),
        );
        self.catalog.drop_table(&table_ident).await
    }
}
