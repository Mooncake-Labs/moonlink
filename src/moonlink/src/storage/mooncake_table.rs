mod batch_id_counter;
mod data_batches;
pub(crate) mod delete_vector;
mod disk_slice;
mod iceberg_persisted_records;
mod mem_slice;
mod persistence_buffer;
mod shared_array;
mod snapshot;
mod snapshot_cache_utils;
mod snapshot_maintenance;
mod snapshot_persistence;
mod snapshot_read;
pub mod snapshot_read_output;
mod snapshot_validation;
pub mod table_config;
pub mod table_secret;
mod table_snapshot;
pub mod table_status;
pub mod table_status_reader;
mod transaction_stream;
use more_asserts as ma;

use super::iceberg::puffin_utils::PuffinBlobRef;
use super::index::{FileIndex, MemIndex, MooncakeIndex};
use super::storage_utils::{MooncakeDataFileRef, RawDeletionRecord, RecordLocation};
use crate::error::Result;
use crate::row::{IdentityProp, MoonlinkRow};
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
use crate::storage::compaction::compactor::{CompactionBuilder, CompactionFileParams};
pub(crate) use crate::storage::compaction::table_compaction::{
    DataCompactionPayload, DataCompactionResult,
};
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::iceberg::iceberg_table_config::IcebergTableConfig;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::storage::iceberg::table_manager::{PersistenceFileParams, TableManager};
use crate::storage::index::persisted_bucket_hash_map::GlobalIndexBuilder;
use crate::storage::mooncake_table::batch_id_counter::BatchIdCounter;
use crate::storage::mooncake_table::iceberg_persisted_records::IcebergPersistedRecords;
use crate::storage::mooncake_table::shared_array::SharedRowBufferSnapshot;
pub use crate::storage::mooncake_table::snapshot_read_output::ReadOutput as SnapshotReadOutput;
#[cfg(test)]
pub(crate) use crate::storage::mooncake_table::table_snapshot::IcebergSnapshotDataCompactionPayload;
pub(crate) use crate::storage::mooncake_table::table_snapshot::{
    take_data_files_to_import, take_data_files_to_remove, take_file_indices_to_import,
    take_file_indices_to_remove, FileIndiceMergePayload, FileIndiceMergeResult,
    IcebergSnapshotDataCompactionResult, IcebergSnapshotImportPayload,
    IcebergSnapshotIndexMergePayload, IcebergSnapshotPayload, IcebergSnapshotResult,
};
use crate::storage::mooncake_table_config::MooncakeTableConfig;
use crate::storage::storage_utils::{FileId, TableId};
use crate::storage::wal::iceberg_corresponding_wal_metadata::IcebergCorrespondingWalMetadata;
use crate::storage::wal::{
    IcebergSnapshotWalInfo, WalConfig, WalManager, WalPersistenceUpdateResult,
};
use crate::table_notify::{EvictedFiles, TableEvent};
use crate::NonEvictableHandle;
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use delete_vector::BatchDeletionVector;
pub(crate) use disk_slice::DiskSliceWriter;
use mem_slice::MemSlice;
pub(crate) use snapshot::{PuffinDeletionBlobAtRead, SnapshotTableState};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use table_snapshot::{IcebergSnapshotImportResult, IcebergSnapshotIndexMergeResult};
#[cfg(test)]
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::{watch, RwLock};
use tracing::Instrument;
use tracing::{error, info_span};
use transaction_stream::{TransactionStreamOutput, TransactionStreamState};

/// Special transaction id used for initial copy append operation.
/// `0` is designated as `InvalidTransactionId` in the postgres implementation, so we can be sure this will never collide with a valid transaction id.
/// [https://github.com/postgres/postgres/blob/d5b9b2d40262f57f58322ad49f8928fd4a492adb/src/include/access/transam.h#L31]
pub(crate) const INITIAL_COPY_XACT_ID: u32 = 0;

#[derive(Debug)]
pub struct TableMetadata {
    /// table name
    pub(crate) name: String,
    /// table id
    pub(crate) table_id: u32,
    /// table schema
    pub(crate) schema: Arc<Schema>,
    /// table config
    pub(crate) config: MooncakeTableConfig,
    /// storage path
    pub(crate) path: PathBuf,
    /// function to get lookup key from row
    pub(crate) identity: IdentityProp,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AlterTableRequest {
    pub(crate) new_columns: Vec<arrow_schema::FieldRef>,
    pub(crate) dropped_columns: Vec<String>,
}

impl TableMetadata {
    pub fn new_for_alter_table(
        previous_metadata: Arc<TableMetadata>,
        alter_table_request: AlterTableRequest,
    ) -> Self {
        let mut new_columns = vec![];
        for field in previous_metadata.schema.fields.iter() {
            if !alter_table_request.dropped_columns.contains(field.name()) {
                new_columns.push(field.clone());
            }
        }
        new_columns.extend(alter_table_request.new_columns);
        let new_schema =
            Schema::new_with_metadata(new_columns, previous_metadata.schema.metadata.clone());
        Self {
            name: previous_metadata.name.clone(),
            table_id: previous_metadata.table_id,
            schema: Arc::new(new_schema),
            config: previous_metadata.config.clone(),
            path: previous_metadata.path.clone(),
            identity: previous_metadata.identity.clone(),
        }
    }
}
#[derive(Clone, Debug)]
pub(crate) struct DiskFileEntry {
    /// Cache handle. If assigned, it's pinned in object storage cache.
    pub(crate) cache_handle: Option<NonEvictableHandle>,
    /// Number of rows.
    pub(crate) num_rows: usize,
    /// File size.
    pub(crate) file_size: usize,
    /// In-memory deletion vector, used for new deletion records in-memory processing.
    pub(crate) batch_deletion_vector: BatchDeletionVector,
    /// Persisted iceberg deletion vector puffin blob.
    pub(crate) puffin_deletion_blob: Option<PuffinBlobRef>,
}

/// Snapshot contains state of the table at a given time.
/// A snapshot maps directly to an iceberg snapshot.
///
#[derive(Clone)]
pub struct Snapshot {
    /// table metadata
    pub(crate) metadata: Arc<TableMetadata>,
    /// datafile and their deletion vector.
    ///
    /// TODO(hjiang):
    /// 1. For the initial release and before we figure out a cache design, disk files are always local ones.
    /// 2. Add corresponding file indices into the value part, so when data file gets compacted, we make sure all related file indices get rewritten and compacted as well.
    pub(crate) disk_files: HashMap<MooncakeDataFileRef, DiskFileEntry>,
    /// Current snapshot version, which is the mooncake table commit point.
    pub(crate) snapshot_version: u64,
    /// LSN which last data file flush operation happens.
    ///
    /// There're two important time points: commit and flush.
    /// - Data files are persisted at flush point, which could span across multiple commit points;
    /// - Batch deletion vector, which is the value for `Snapshot::disk_files` updates at commit points.
    ///   So likely they are not consistent from LSN's perspective.
    ///
    /// At iceberg snapshot creation, we should only dump consistent data files and deletion logs.
    /// Data file flush LSN is recorded here, to get corresponding deletion logs from "committed deletion logs".
    pub(crate) flush_lsn: Option<u64>,
    /// WAL data corresponding to the iceberg snapshot.
    ///
    /// Note that we only currently use this for recovery.
    pub(crate) iceberg_corresponding_wal_metadata: IcebergCorrespondingWalMetadata,
    /// indices
    pub(crate) indices: MooncakeIndex,
}

impl Snapshot {
    pub(crate) fn new(metadata: Arc<TableMetadata>) -> Self {
        Self {
            metadata,
            disk_files: HashMap::new(),
            snapshot_version: 0,
            flush_lsn: None,
            iceberg_corresponding_wal_metadata: IcebergCorrespondingWalMetadata {
                earliest_wal_file_num: 0,
            },
            indices: MooncakeIndex::new(),
        }
    }

    pub fn get_name_for_inmemory_file(&self) -> PathBuf {
        let mut directory = PathBuf::from(&self.metadata.config.temp_files_directory);
        directory.push(format!(
            "inmemory_{}_{}_{}.parquet",
            self.metadata.name, self.metadata.table_id, self.snapshot_version
        ));
        directory
    }
}

#[derive(Default)]
pub struct SnapshotTask {
    /// Mooncake table config.
    mooncake_table_config: MooncakeTableConfig,

    /// ---- States not recorded by mooncake snapshot ----
    ///
    new_disk_slices: Vec<DiskSliceWriter>,
    disk_file_lsn_map: HashMap<FileId, u64>,
    new_deletions: Vec<RawDeletionRecord>,
    /// Pair of <batch id, record batch>.
    new_record_batches: Vec<(u64, Arc<RecordBatch>)>,
    new_rows: Option<SharedRowBufferSnapshot>,
    new_mem_indices: Vec<Arc<MemIndex>>,
    /// Assigned (non-zero) after a commit event.
    new_commit_lsn: u64,
    /// Assigned at a flush operation.
    new_flush_lsn: Option<u64>,
    new_commit_point: Option<RecordLocation>,

    /// WAL metadata corresponding to the iceberg snapshot. This will always be populated at the time of update_snapshot.
    iceberg_corresponding_wal_metadata: Option<IcebergCorrespondingWalMetadata>,

    /// streaming xact
    new_streaming_xact: Vec<TransactionStreamOutput>,

    /// Schema change.
    force_empty_iceberg_payload: bool,

    /// Committed deletion records, which have been persisted into iceberg, and should be pruned from mooncake snapshot.
    committed_deletion_logs: HashSet<(FileId, usize /*row idx*/)>,

    /// --- States related to file indices merge operation ---
    /// These persisted items will be reflected to mooncake snapshot in the next invocation of periodic mooncake snapshot operation.
    index_merge_result: FileIndiceMergeResult,

    /// --- States related to data compaction operation ---
    /// These persisted items will be reflected to mooncake snapshot in the next invocation of periodic mooncake snapshot operation.
    data_compaction_result: DataCompactionResult,

    /// ---- States have been recorded by mooncake snapshot, and persisted into iceberg table ----
    /// These persisted items will be reflected to mooncake snapshot in the next invocation of periodic mooncake snapshot operation.
    iceberg_persisted_records: IcebergPersistedRecords,
}

impl SnapshotTask {
    pub fn new(mooncake_table_config: MooncakeTableConfig) -> Self {
        Self {
            mooncake_table_config,
            new_disk_slices: Vec::new(),
            disk_file_lsn_map: HashMap::new(),
            new_deletions: Vec::new(),
            new_record_batches: Vec::new(),
            new_rows: None,
            new_mem_indices: Vec::new(),
            new_commit_lsn: 0,
            new_flush_lsn: None,
            new_commit_point: None,
            iceberg_corresponding_wal_metadata: None,
            new_streaming_xact: Vec::new(),
            force_empty_iceberg_payload: false,
            // Committed deletion logs which have been persisted, and should be pruned from mooncake snapshot.
            committed_deletion_logs: HashSet::new(),
            // Index merge related fields.
            index_merge_result: FileIndiceMergeResult::default(),
            // Data compaction related fields.
            data_compaction_result: DataCompactionResult::default(),
            // Iceberg persistence result.
            iceberg_persisted_records: IcebergPersistedRecords::default(),
        }
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        if !self.new_disk_slices.is_empty() {
            assert!(!self.disk_file_lsn_map.is_empty());
            assert!(self.new_flush_lsn.is_some());
            return false;
        }
        if !self.new_deletions.is_empty() {
            return false;
        }
        if !self.new_mem_indices.is_empty() {
            return false;
        }
        if !self.new_streaming_xact.is_empty() {
            return false;
        }
        if !self.index_merge_result.is_empty() {
            return false;
        }
        if !self.data_compaction_result.is_empty() {
            return false;
        }
        if !self.iceberg_persisted_records.is_empty() {
            return false;
        }
        true
    }

    pub fn should_create_snapshot(&self) -> bool {
        // If mooncake has new transaction commits.
        self.new_commit_lsn > 0
            || self.force_empty_iceberg_payload
        // If mooncake table accumulated large enough writes.
            || !self.new_disk_slices.is_empty()
            || self.new_deletions.len()
                >= self.mooncake_table_config.snapshot_deletion_record_count()
            // If iceberg snapshot is already performed, update mooncake snapshot accordingly.
            // On local filesystem, potentially we could double storage as soon as possible.
            || !self.iceberg_persisted_records.import_result.is_empty()
            || !self.iceberg_persisted_records.index_merge_result.is_empty()
            || !self.iceberg_persisted_records.data_compaction_result.is_empty()
    }

    /// Get newly created data files, including both batch write ones and stream write ones.
    pub(crate) fn get_new_data_files(&self) -> Vec<MooncakeDataFileRef> {
        let mut new_files = vec![];

        // Batch write data files.
        for cur_disk_slice in self.new_disk_slices.iter() {
            new_files.extend(
                cur_disk_slice
                    .output_files()
                    .iter()
                    .map(|(file, _)| file.clone()),
            );
        }

        // Stream write data files.
        for cur_stream_xact in self.new_streaming_xact.iter() {
            if let TransactionStreamOutput::Commit(cur_stream_commit) = cur_stream_xact {
                new_files.extend(cur_stream_commit.get_flushed_data_files());
            }
        }

        new_files
    }

    /// Get newly created file indices, including both batch write ones and stream write ones.
    pub(crate) fn get_new_file_indices(&self) -> Vec<FileIndex> {
        let mut new_file_indices = vec![];

        // Batch write file indices.
        for cur_disk_slice in self.new_disk_slices.iter() {
            let file_index = cur_disk_slice.get_file_index();
            if let Some(file_index) = file_index {
                new_file_indices.push(file_index);
            }
        }

        // Stream write file indices.
        for cur_stream_xact in self.new_streaming_xact.iter() {
            if let TransactionStreamOutput::Commit(cur_stream_commit) = cur_stream_xact {
                new_file_indices.extend(cur_stream_commit.get_file_indices());
            }
        }

        new_file_indices
    }
}

/// Option for a maintenance option.
///
/// For all types of maintaince tasks, we have two basic dimensions:
/// - Selection criteria: for full-mode maintenance task, all files will take part in, however big it is; for non-full-mode, only those meet certain threshold will be selected.
///   For example, for non-full-mode, only small files will be compacted.
/// - Trigger criteria: to avoid overly frequent background maintaince task, it's only triggered when selected files reaches certain threshold.
///   While for force maintenance request, as long as there're at least two files, task will be triggered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceOption {
    /// Regular maintenance task, which perform a best effort attempt.
    /// This is the default option, which is used for background task.
    BestEffort,
    /// Force a regular maintenance attempt.
    ForceRegular,
    /// Force a full maintaince attempt.
    ForceFull,
    /// Skip maintenance attempt.
    Skip,
}

/// Options to create mooncake snapshot.
#[derive(Clone, Debug)]
pub struct SnapshotOption {
    /// UUID for the current mooncake snapshot operation.
    pub(crate) uuid: uuid::Uuid,
    /// Whether to force create snapshot.
    /// When specified, mooncake snapshot will be created with snapshot threshold ignored.
    pub(crate) force_create: bool,
    /// Whether to skip iceberg snapshot creation.
    pub(crate) skip_iceberg_snapshot: bool,
    /// Index merge operation option.
    pub(crate) index_merge_option: MaintenanceOption,
    /// Data compaction operation option.
    pub(crate) data_compaction_option: MaintenanceOption,
}

impl SnapshotOption {
    pub fn default() -> SnapshotOption {
        Self {
            uuid: uuid::Uuid::new_v4(),
            force_create: false,
            skip_iceberg_snapshot: false,
            index_merge_option: MaintenanceOption::BestEffort,
            data_compaction_option: MaintenanceOption::BestEffort,
        }
    }
}

/// MooncakeTable is a disk table + mem slice.
/// Transactions will append data to the mem slice.
///
/// And periodically disk slices will be merged and compacted.
/// Single thread is used to write to the table.
///
/// LSN is used for visiblity control of mooncake table.
/// Currently it has following rules:
/// For read at lsn X, any record committed at lsn <= X is visible.
/// For commit at lsn X, any record whose lsn < X is committed.
///
/// COMMIT_LSN_xact_1 <= DELETE_LSN_xact_2 < COMMIT_LSN_xact_2
///
pub struct MooncakeTable {
    /// Current metadata of the table.
    ///
    metadata: Arc<TableMetadata>,

    /// The mem slice
    ///
    mem_slice: MemSlice,

    /// Current snapshot of the table
    snapshot: Arc<RwLock<SnapshotTableState>>,
    /// Whether there's ongoing mooncake snapshot.
    mooncake_snapshot_ongoing: bool,

    table_snapshot_watch_sender: watch::Sender<u64>,
    table_snapshot_watch_receiver: watch::Receiver<u64>,

    /// Records all the write operations since last snapshot.
    next_snapshot_task: SnapshotTask,

    /// Stream state per transaction, keyed by xact-id.
    transaction_stream_states: HashMap<u32, TransactionStreamState>,

    /// Auto increment id for generating unique file ids.
    /// Note, these ids is only used locally, and not persisted.
    next_file_id: u32,

    /// Batch ID counters for the two-counter allocation strategy
    non_streaming_batch_id_counter: Arc<BatchIdCounter>,
    streaming_batch_id_counter: Arc<BatchIdCounter>,

    /// Iceberg table manager, used to sync snapshot to the corresponding iceberg table.
    iceberg_table_manager: Option<Box<dyn TableManager>>,

    /// LSN of the latest iceberg snapshot.
    last_iceberg_snapshot_lsn: Option<u64>,

    /// Metadata for latest WAL file that accompanies the latest iceberg snapshot.
    /// The lowest relevant WAL file number is calculated when triggering an iceberg snapshot,
    /// and this is updated when the iceberg snapshot is completed.
    /// The WAL manager will truncate up to this file number on the next periodic WAL truncation.
    iceberg_corresponding_wal_metadata: IcebergCorrespondingWalMetadata,

    /// Table notifier, which is used to sent multiple types of event completion information.
    table_notify: Option<Sender<TableEvent>>,

    /// WAL manager, used to persist WAL events.
    wal_manager: WalManager,
}

impl MooncakeTable {
    /// foreground functions
    ///
    /// TODO(hjiang): Provide a struct to hold all paramters.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        schema: Schema,
        name: String,
        table_id: u32,
        base_path: PathBuf,
        identity: IdentityProp,
        iceberg_table_config: IcebergTableConfig,
        table_config: MooncakeTableConfig,
        wal_config: WalConfig,
        object_storage_cache: ObjectStorageCache,
        table_filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
    ) -> Result<Self> {
        let metadata = Arc::new(TableMetadata {
            name,
            table_id,
            schema: Arc::new(schema.clone()),
            config: table_config.clone(),
            path: base_path,
            identity,
        });
        let iceberg_table_manager = Box::new(IcebergTableManager::new(
            metadata.clone(),
            object_storage_cache.clone(),
            table_filesystem_accessor.clone(),
            iceberg_table_config,
        )?);

        let wal_manager = WalManager::new(&wal_config);
        Self::new_with_table_manager(
            metadata,
            iceberg_table_manager,
            object_storage_cache,
            table_filesystem_accessor,
            wal_manager,
        )
        .await
    }

    pub(crate) async fn new_with_table_manager(
        table_metadata: Arc<TableMetadata>,
        mut table_manager: Box<dyn TableManager>,
        object_storage_cache: ObjectStorageCache,
        table_filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
        wal_manager: WalManager,
    ) -> Result<Self> {
        table_metadata.config.validate();
        let (table_snapshot_watch_sender, table_snapshot_watch_receiver) = watch::channel(u64::MAX);
        let (next_file_id, current_snapshot) = table_manager.load_snapshot_from_table().await?;
        let last_iceberg_snapshot_lsn = current_snapshot.flush_lsn;
        // TODO(Paul): Change wal manager to pick up to latest WAL file number on recovery
        let iceberg_snapshot_wal_metadata =
            current_snapshot.iceberg_corresponding_wal_metadata.clone();
        if let Some(persistence_lsn) = last_iceberg_snapshot_lsn {
            table_snapshot_watch_sender.send(persistence_lsn).unwrap();
        }

        let non_streaming_batch_id_counter = Arc::new(BatchIdCounter::new(false));
        let streaming_batch_id_counter = Arc::new(BatchIdCounter::new(true));

        Ok(Self {
            mem_slice: MemSlice::new(
                table_metadata.schema.clone(),
                table_metadata.config.batch_size,
                table_metadata.identity.clone(),
                Arc::clone(&non_streaming_batch_id_counter),
            ),
            metadata: table_metadata.clone(),
            snapshot: Arc::new(RwLock::new(
                SnapshotTableState::new(
                    table_manager.get_warehouse_location(),
                    table_metadata.clone(),
                    object_storage_cache,
                    table_filesystem_accessor,
                    current_snapshot,
                    Arc::clone(&non_streaming_batch_id_counter),
                )
                .await?,
            )),
            mooncake_snapshot_ongoing: false,
            next_snapshot_task: SnapshotTask::new(table_metadata.as_ref().config.clone()),
            transaction_stream_states: HashMap::new(),
            table_snapshot_watch_sender,
            table_snapshot_watch_receiver,
            next_file_id,
            non_streaming_batch_id_counter,
            streaming_batch_id_counter,
            iceberg_table_manager: Some(table_manager),
            last_iceberg_snapshot_lsn,
            iceberg_corresponding_wal_metadata: iceberg_snapshot_wal_metadata,
            table_notify: None,
            wal_manager,
        })
    }

    pub(crate) fn alter_table(
        &mut self,
        alter_table_request: AlterTableRequest,
    ) -> Arc<TableMetadata> {
        assert!(
            self.mem_slice.is_empty(),
            "Cannot alter table with non-empty mem slice"
        );
        assert!(
            self.next_snapshot_task.is_empty(),
            "Cannot alter table with pending snapshot task"
        );

        // Create new table metadata.
        let new_metadata = Arc::new(TableMetadata::new_for_alter_table(
            self.metadata.clone(),
            alter_table_request,
        ));

        let mut guard = self.snapshot.try_write().unwrap();
        guard.reset_for_alter(new_metadata.clone());
        assert!(
            self.metadata.schema.fields.len() != new_metadata.schema.fields.len(),
            "Only support alter table with add/drop fields"
        );
        self.mem_slice = MemSlice::new(
            new_metadata.schema.clone(),
            new_metadata.config.batch_size,
            new_metadata.identity.clone(),
            Arc::clone(&self.non_streaming_batch_id_counter),
        );
        self.metadata = new_metadata.clone();
        new_metadata
    }

    /// Register event completion notifier.
    /// Notice it should be registered only once, which could be used to notify multiple events.
    pub(crate) async fn register_table_notify(&mut self, table_notify: Sender<TableEvent>) {
        assert!(self.table_notify.is_none());
        self.table_notify = Some(table_notify.clone());
        self.snapshot
            .write()
            .await
            .register_table_notify(table_notify);
    }

    /// Assert flush LSN doesn't regress.
    /// There're several cases for equal flush LSN, for example, force snapshot, table maintenance, etc.
    fn assert_flush_lsn_on_iceberg_snapshot_res(
        persistence_lsn: Option<u64>,
        iceberg_snapshot_res: &IcebergSnapshotResult,
    ) {
        let flush_lsn = iceberg_snapshot_res.flush_lsn;
        assert!(
                persistence_lsn.is_none()
                    || persistence_lsn.unwrap() <= flush_lsn,
                "Last iceberg snapshot LSN is {:?}, flush LSN is {:?}, imported data file number is {}, imported puffin file number is {}",
                persistence_lsn,
                flush_lsn,
                iceberg_snapshot_res.import_result.new_data_files.len(),
                iceberg_snapshot_res.import_result.puffin_blob_ref.len(),
            );
    }

    /// Set iceberg snapshot flush LSN, called after a snapshot operation.
    pub(crate) fn set_iceberg_snapshot_res(&mut self, iceberg_snapshot_res: IcebergSnapshotResult) {
        // ---- Update mooncake table fields ----
        let flush_lsn = iceberg_snapshot_res.flush_lsn;
        Self::assert_flush_lsn_on_iceberg_snapshot_res(
            self.last_iceberg_snapshot_lsn,
            &iceberg_snapshot_res,
        );
        self.last_iceberg_snapshot_lsn = Some(flush_lsn);

        // Update mooncake table metadata if necessary.
        if let Some(new_table_schema) = iceberg_snapshot_res.new_table_schema {
            // Assert table is at a clean state.
            assert!(self.mem_slice.is_empty());
            assert!(self.next_snapshot_task.is_empty());
            self.metadata = new_table_schema;
        }

        self.iceberg_corresponding_wal_metadata =
            iceberg_snapshot_res.corresponding_wal_metadata.clone();

        assert!(self.iceberg_table_manager.is_none());
        self.iceberg_table_manager = Some(iceberg_snapshot_res.table_manager.unwrap());

        // ---- Buffer iceberg persisted content to next snapshot task ---
        assert!(self.next_snapshot_task.committed_deletion_logs.is_empty());
        self.next_snapshot_task.committed_deletion_logs =
            iceberg_snapshot_res.committed_deletion_logs;

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .flush_lsn
            .is_none());
        self.next_snapshot_task.iceberg_persisted_records.flush_lsn = Some(flush_lsn);

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .import_result
            .is_empty());
        self.next_snapshot_task
            .iceberg_persisted_records
            .import_result = iceberg_snapshot_res.import_result;

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .index_merge_result
            .is_empty());
        self.next_snapshot_task
            .iceberg_persisted_records
            .index_merge_result = iceberg_snapshot_res.index_merge_result;

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .data_compaction_result
            .is_empty());
        self.next_snapshot_task
            .iceberg_persisted_records
            .data_compaction_result = iceberg_snapshot_res.data_compaction_result;
    }

    /// Set file indices merge result, which will be sync-ed to mooncake and iceberg snapshot in the next periodic snapshot iteration.
    pub(crate) fn set_file_indices_merge_res(&mut self, file_indices_res: FileIndiceMergeResult) {
        // TODO(hjiang): Should be able to use HashSet at beginning so no need to convert.
        assert!(self.next_snapshot_task.index_merge_result.is_empty());
        self.next_snapshot_task.index_merge_result = file_indices_res;
    }

    /// Set data compaction result, which will be sync-ed to mooncake and iceberg snapshot in the next periodic snapshot iteration.
    pub(crate) fn set_data_compaction_res(&mut self, data_compaction_res: DataCompactionResult) {
        assert!(self.next_snapshot_task.data_compaction_result.is_empty());
        self.next_snapshot_task.data_compaction_result = data_compaction_res;
    }

    /// Get iceberg snapshot flush LSN.
    pub(crate) fn get_iceberg_snapshot_lsn(&self) -> Option<u64> {
        self.last_iceberg_snapshot_lsn
    }

    pub(crate) fn get_state_for_reader(
        &self,
    ) -> (Arc<RwLock<SnapshotTableState>>, watch::Receiver<u64>) {
        (
            self.snapshot.clone(),
            self.table_snapshot_watch_receiver.clone(),
        )
    }

    pub fn append(&mut self, row: MoonlinkRow) -> Result<()> {
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let identity_for_key = self.metadata.identity.extract_identity_for_key(&row);
        if let Some(batch) = self.mem_slice.append(lookup_key, row, identity_for_key)? {
            self.next_snapshot_task.new_record_batches.push(batch);
        }
        Ok(())
    }

    pub async fn delete(&mut self, row: MoonlinkRow, lsn: u64) {
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let mut record = RawDeletionRecord {
            lookup_key,
            lsn,
            pos: None,
            row_identity: self.metadata.identity.extract_identity_columns(row),
        };
        let pos = self
            .mem_slice
            .delete(&record, &self.metadata.identity)
            .await;
        record.pos = pos;
        self.next_snapshot_task.new_deletions.push(record);
    }

    pub fn commit(&mut self, lsn: u64) {
        self.next_snapshot_task.new_commit_lsn = lsn;
        self.next_snapshot_task.new_commit_point = Some(self.mem_slice.get_commit_check_point());
        assert!(
            self.next_snapshot_task.new_deletions.is_empty()
                || self.next_snapshot_task.new_deletions.last().unwrap().lsn >= transaction_stream::LSN_START_FOR_STREAMING_XACT
                || self.next_snapshot_task.new_deletions.last().unwrap().lsn < lsn,
            "We expect commit LSN to be strictly greater than the last deletion LSN, but got commit LSN {} and last deletion LSN {}",
            lsn,
            self.next_snapshot_task.new_deletions.last().unwrap().lsn,
        );
    }

    /// Shutdown the current table, which unpins all referenced data files in the global data file.
    pub async fn shutdown(&mut self) -> Result<()> {
        let evicted_files_to_delete = {
            let mut guard = self.snapshot.write().await;
            guard.unreference_and_delete_all_cache_handles().await
        };

        for cur_file in evicted_files_to_delete.into_iter() {
            tokio::fs::remove_file(cur_file).await?;
        }

        Ok(())
    }

    pub fn should_flush(&self) -> bool {
        self.mem_slice.get_num_rows() >= self.metadata.config.mem_slice_size
    }

    /// Flush `mem_slice` into parquet files and return the resulting `DiskSliceWriter`.
    ///
    /// When `snapshot_task` is provided, new batches and indices are recorded so
    /// that they can be included in the next snapshot.  The `lsn` parameter
    /// specifies the commit LSN for the flushed data.  When `lsn` is `None` the
    /// caller is responsible for setting the final LSN on the returned
    /// `DiskSliceWriter`.
    async fn flush_mem_slice(
        mem_slice: &mut MemSlice,
        metadata: &Arc<TableMetadata>,
        next_file_id: u32,
        lsn: Option<u64>,
        snapshot_task: Option<&mut SnapshotTask>,
    ) -> Result<DiskSliceWriter> {
        // Finalize the current batch (if needed)
        let (new_batch, batches, index) = mem_slice.drain().unwrap();

        let index = Arc::new(index);
        if let Some(task) = snapshot_task {
            if let Some(batch) = new_batch {
                task.new_record_batches.push(batch);
            }
            task.new_mem_indices.push(index.clone());
        }

        let path = metadata.path.clone();
        let parquet_flush_threshold_size = metadata.config.disk_slice_parquet_file_size;

        let mut disk_slice = DiskSliceWriter::new(
            metadata.schema.clone(),
            path,
            batches,
            lsn,
            next_file_id,
            index,
            parquet_flush_threshold_size,
        );

        disk_slice.write().await?;
        Ok(disk_slice)
    }

    // UNDONE(BATCH_INSERT):
    // Flush uncommitted batches from big batch insert, whether how much record batch there is.
    //
    // This function
    // - tracks all record batches by current snapshot task
    // - persists all full batch records to local filesystem
    pub async fn flush(&mut self, lsn: u64) -> Result<()> {
        // Sanith check flush LSN doesn't regress.
        assert!(
            self.next_snapshot_task.new_flush_lsn.is_none()
                || self.next_snapshot_task.new_flush_lsn.unwrap() <= lsn,
            "Current flush LSN is {:?}, new flush LSN is {}",
            self.next_snapshot_task.new_flush_lsn,
            lsn,
        );
        self.next_snapshot_task.new_flush_lsn = Some(lsn);

        if self.mem_slice.is_empty() {
            return Ok(());
        }

        let next_file_id = self.next_file_id;
        self.next_file_id += 1;
        let disk_slice = Self::flush_mem_slice(
            &mut self.mem_slice,
            &self.metadata,
            next_file_id,
            Some(lsn),
            Some(&mut self.next_snapshot_task),
        )
        .await?;
        self.next_snapshot_task.new_disk_slices.push(disk_slice);

        Ok(())
    }

    // Create a snapshot of the last committed version, return current snapshot's version and payload to perform iceberg snapshot.
    fn create_snapshot_impl(&mut self, opt: SnapshotOption) {
        // Check invariant: there should be at most one ongoing mooncake snapshot.
        assert!(!self.mooncake_snapshot_ongoing);
        self.mooncake_snapshot_ongoing = true;

        self.next_snapshot_task.new_rows = Some(self.mem_slice.get_latest_rows());
        let mut next_snapshot_task = std::mem::take(&mut self.next_snapshot_task);

        let lowest_wal_file_to_keep = if let Some(flush_lsn) = next_snapshot_task.new_flush_lsn {
            self.wal_manager.get_lowest_file_to_keep(flush_lsn)
        } else {
            self.iceberg_corresponding_wal_metadata
                .earliest_wal_file_num
        };
        ma::assert_le!(
            self.iceberg_corresponding_wal_metadata
                .earliest_wal_file_num,
            lowest_wal_file_to_keep
        );
        let iceberg_corresponding_wal_metadata = IcebergCorrespondingWalMetadata {
            earliest_wal_file_num: lowest_wal_file_to_keep,
        };

        next_snapshot_task.iceberg_corresponding_wal_metadata =
            Some(iceberg_corresponding_wal_metadata);

        // Re-initialize mooncake table fields.
        self.next_snapshot_task = SnapshotTask::new(self.metadata.config.clone());

        let cur_snapshot = self.snapshot.clone();
        // Create a detached task, whose completion will be notified separately.
        tokio::task::spawn(
            Self::create_snapshot_async(
                cur_snapshot,
                next_snapshot_task,
                opt,
                self.table_notify.as_ref().unwrap().clone(),
            )
            .instrument(info_span!("create_snapshot_async")),
        );
    }

    /// If a mooncake snapshot is not going to be created, return false immediately.
    #[must_use]
    pub fn create_snapshot(&mut self, opt: SnapshotOption) -> bool {
        if !self.next_snapshot_task.should_create_snapshot() && !opt.force_create {
            return false;
        }
        self.create_snapshot_impl(opt);
        true
    }

    /// Notify mooncake snapshot as completed.
    pub fn mark_mooncake_snapshot_completed(&mut self) {
        assert!(self.mooncake_snapshot_ongoing);
        self.mooncake_snapshot_ongoing = false;
    }

    /// Perform index merge, whose completion will be notified separately in async style.
    pub(crate) fn perform_index_merge(
        &mut self,
        file_indice_merge_payload: FileIndiceMergePayload,
    ) {
        let cur_file_id = self.next_file_id as u64;
        self.next_file_id += 1;
        let table_directory = std::path::PathBuf::from(self.metadata.path.to_str().unwrap());
        let table_notify_tx_copy = self.table_notify.as_ref().unwrap().clone();

        // Create a detached task, whose completion will be notified separately.
        tokio::task::spawn(async move {
            let mut builder = GlobalIndexBuilder::new();
            builder.set_directory(table_directory);
            let merged = builder
                .build_from_merge(file_indice_merge_payload.file_indices.clone(), cur_file_id)
                .await;
            let index_merge_result = FileIndiceMergeResult {
                uuid: file_indice_merge_payload.uuid,
                old_file_indices: file_indice_merge_payload.file_indices,
                new_file_indices: vec![merged],
            };
            table_notify_tx_copy
                .send(TableEvent::IndexMergeResult { index_merge_result })
                .await
                .unwrap();
        });
    }

    /// Perform data compaction, whose completion will be notified separately in async style.
    pub(crate) fn perform_data_compaction(&mut self, compaction_payload: DataCompactionPayload) {
        let data_compaction_new_file_ids =
            compaction_payload.get_new_compacted_data_file_ids_number();
        let table_auto_incr_ids =
            self.next_file_id..(self.next_file_id + data_compaction_new_file_ids);
        self.next_file_id += data_compaction_new_file_ids;
        let file_params = CompactionFileParams {
            dir_path: self.metadata.path.clone(),
            table_auto_incr_ids,
            data_file_final_size: self
                .metadata
                .config
                .data_compaction_config
                .data_file_final_size,
        };
        let schema_ref = self.metadata.schema.clone();
        let table_notify_tx_copy = self.table_notify.as_ref().unwrap().clone();

        // Create a detached task, whose completion will be notified separately.
        tokio::task::spawn(
            async move {
                let builder = CompactionBuilder::new(compaction_payload, schema_ref, file_params);
                let data_compaction_result = builder.build().await;
                table_notify_tx_copy
                    .send(TableEvent::DataCompactionResult {
                        data_compaction_result,
                    })
                    .await
                    .unwrap();
            }
            .instrument(info_span!("data_compaction")),
        );
    }

    /// Update table schema to the provided [`updated_table_metadata`].
    /// To synchronize on its completion, caller should trigger a force snapshot and block wait iceberg snapshot complet
    pub(crate) fn force_empty_iceberg_payload(&mut self) {
        assert!(!self.next_snapshot_task.force_empty_iceberg_payload);
        self.next_snapshot_task.force_empty_iceberg_payload = true;
    }

    pub(crate) fn notify_snapshot_reader(&self, lsn: u64) {
        self.table_snapshot_watch_sender.send(lsn).unwrap();
    }

    /// Persist an iceberg snapshot.
    async fn persist_iceberg_snapshot_impl(
        mut iceberg_table_manager: Box<dyn TableManager>,
        snapshot_payload: IcebergSnapshotPayload,
        table_notify: Sender<TableEvent>,
        table_auto_incr_ids: std::ops::Range<u32>,
    ) {
        let uuid = snapshot_payload.uuid;
        let flush_lsn = snapshot_payload.flush_lsn;
        let new_table_schema = snapshot_payload.new_table_schema.clone();
        let committed_deletion_logs = snapshot_payload.committed_deletion_logs.clone();

        let new_imported_data_files_count = snapshot_payload.import_payload.data_files.len();
        let new_compacted_data_files_count = snapshot_payload
            .data_compaction_payload
            .new_data_files_to_import
            .len();

        let new_new_file_indices_count = snapshot_payload.import_payload.file_indices.len();
        let new_merged_file_indices_count = snapshot_payload
            .index_merge_payload
            .new_file_indices_to_import
            .len();
        let new_compacted_file_indices_count = snapshot_payload
            .data_compaction_payload
            .new_file_indices_to_import
            .len();

        let old_file_indices_to_remove_by_index_merge = snapshot_payload
            .index_merge_payload
            .old_file_indices_to_remove
            .clone();
        let old_data_files_to_remove_by_compaction = snapshot_payload
            .data_compaction_payload
            .old_data_files_to_remove
            .clone();
        let old_file_indices_to_remove_by_compaction = snapshot_payload
            .data_compaction_payload
            .old_file_indices_to_remove
            .clone();

        let persistence_file_params = PersistenceFileParams {
            table_auto_incr_ids,
        };

        let wal_persisted_metadata = snapshot_payload.iceberg_corresponding_wal_metadata.clone();

        let iceberg_persistence_res = iceberg_table_manager
            .sync_snapshot(snapshot_payload, persistence_file_params)
            .await;

        // Notify on event error.
        if iceberg_persistence_res.is_err() {
            table_notify
                .send(TableEvent::IcebergSnapshotResult {
                    iceberg_snapshot_result: Err(iceberg_persistence_res.unwrap_err().into()),
                })
                .await
                .unwrap();
            return;
        }

        // Notify on event success.
        let iceberg_persistence_res = iceberg_persistence_res.unwrap();

        // Persisted data files and file indices will be cut into multiple sections: imported part, index merge part, data compaction part.
        // Get the cut-off indices to slice from returned iceberg persistence result.
        let new_data_files_cutoff_index_1 = new_imported_data_files_count;
        let new_data_files_cutoff_index_2 =
            new_data_files_cutoff_index_1 + new_compacted_data_files_count;
        assert_eq!(
            new_data_files_cutoff_index_2,
            iceberg_persistence_res.remote_data_files.len()
        );

        let new_file_indices_cutoff_index_1 = new_new_file_indices_count;
        let new_file_indices_cutoff_index_2 =
            new_file_indices_cutoff_index_1 + new_merged_file_indices_count;
        let new_file_indices_cutoff_index_3 =
            new_file_indices_cutoff_index_2 + new_compacted_file_indices_count;
        assert_eq!(
            new_file_indices_cutoff_index_3,
            iceberg_persistence_res.remote_file_indices.len()
        );

        let snapshot_result = IcebergSnapshotResult {
            uuid,
            table_manager: Some(iceberg_table_manager),
            flush_lsn,
            corresponding_wal_metadata: wal_persisted_metadata,
            new_table_schema,
            committed_deletion_logs,
            import_result: IcebergSnapshotImportResult {
                new_data_files: iceberg_persistence_res.remote_data_files
                    [0..new_data_files_cutoff_index_1]
                    .to_vec(),
                puffin_blob_ref: iceberg_persistence_res.puffin_blob_ref,
                new_file_indices: iceberg_persistence_res.remote_file_indices
                    [0..new_file_indices_cutoff_index_1]
                    .to_vec(),
            },
            index_merge_result: IcebergSnapshotIndexMergeResult {
                new_file_indices_imported: iceberg_persistence_res.remote_file_indices
                    [new_file_indices_cutoff_index_1..new_file_indices_cutoff_index_2]
                    .to_vec(),
                old_file_indices_removed: old_file_indices_to_remove_by_index_merge,
            },
            data_compaction_result: IcebergSnapshotDataCompactionResult {
                new_data_files_imported: iceberg_persistence_res.remote_data_files
                    [new_data_files_cutoff_index_1..new_data_files_cutoff_index_2]
                    .to_vec(),
                old_data_files_removed: old_data_files_to_remove_by_compaction,
                new_file_indices_imported: iceberg_persistence_res.remote_file_indices
                    [new_file_indices_cutoff_index_2..new_file_indices_cutoff_index_3]
                    .to_vec(),
                old_file_indices_removed: old_file_indices_to_remove_by_compaction,
            },
        };
        table_notify
            .send(TableEvent::IcebergSnapshotResult {
                iceberg_snapshot_result: Ok(snapshot_result),
            })
            .await
            .unwrap();
    }

    /// Create an iceberg snapshot.
    pub(crate) fn persist_iceberg_snapshot(&mut self, snapshot_payload: IcebergSnapshotPayload) {
        // Check invariant: there's at most one ongoing iceberg snapshot.
        let iceberg_table_manager = self.iceberg_table_manager.take().unwrap();

        // Create a detached task, whose completion will be notified separately.
        let new_file_ids_to_create = snapshot_payload.get_new_file_ids_num();
        let table_auto_incr_ids = self.next_file_id..(self.next_file_id + new_file_ids_to_create);
        self.next_file_id += new_file_ids_to_create;
        tokio::task::spawn(
            Self::persist_iceberg_snapshot_impl(
                iceberg_table_manager,
                snapshot_payload,
                self.table_notify.as_ref().unwrap().clone(),
                table_auto_incr_ids,
            )
            .instrument(info_span!("persist_iceberg_snapshot")),
        );
    }

    /// Drop a mooncake table.
    pub(crate) async fn drop_mooncake_table(&mut self) -> Result<()> {
        tokio::fs::remove_dir_all(&self.metadata.path).await?;
        Ok(())
    }

    /// Drop an iceberg table.
    pub(crate) async fn drop_iceberg_table(&mut self) -> Result<()> {
        assert!(self.iceberg_table_manager.is_some());
        self.iceberg_table_manager
            .as_mut()
            .unwrap()
            .drop_table()
            .await?;
        Ok(())
    }

    async fn create_snapshot_async(
        snapshot: Arc<RwLock<SnapshotTableState>>,
        next_snapshot_task: SnapshotTask,
        mut opt: SnapshotOption,
        table_notify: Sender<TableEvent>,
    ) {
        let uuid = std::mem::take(&mut opt.uuid);
        let snapshot_result = snapshot
            .write()
            .await
            .update_snapshot(next_snapshot_task, opt)
            .await;
        table_notify
            .send(TableEvent::MooncakeTableSnapshotResult {
                uuid,
                lsn: snapshot_result.commit_lsn,
                iceberg_snapshot_payload: snapshot_result.iceberg_snapshot_payload,
                data_compaction_payload: snapshot_result.data_compaction_payload,
                file_indice_merge_payload: snapshot_result.file_indices_merge_payload,
                evicted_files_to_delete: EvictedFiles {
                    files: snapshot_result.evicted_data_files_to_delete,
                },
            })
            .await
            .unwrap();
    }

    /// Uses the latest wal metadata and iceberg LSN from the latest iceberg snapshot
    /// to determine the files to truncate.
    ///
    /// # Arguments
    ///
    /// * uuid: WAL persistence event unique id.
    #[must_use]
    pub(crate) fn do_wal_persistence_update(&mut self, uuid: uuid::Uuid) -> bool {
        let latest_iceberg_snapshot_lsn = self.get_iceberg_snapshot_lsn();
        let last_iceberg_wal_metadata = self.iceberg_corresponding_wal_metadata.clone();

        let iceberg_snapshot_wal_info =
            if let Some(latest_iceberg_snapshot_lsn) = latest_iceberg_snapshot_lsn {
                Some(IcebergSnapshotWalInfo::new(
                    last_iceberg_wal_metadata.earliest_wal_file_num,
                    latest_iceberg_snapshot_lsn,
                ))
            } else {
                if last_iceberg_wal_metadata.earliest_wal_file_num > 0 {
                    error!(
                    "latest iceberg flush LSN is not set even though WAL has been truncated to > 0"
                );
                }
                None
            };

        let files_to_truncate = self
            .wal_manager
            .get_files_to_truncate(last_iceberg_wal_metadata);

        let file_to_persist = self.wal_manager.extract_next_persistence_file();

        if !files_to_truncate.is_empty() || file_to_persist.is_some() {
            let event_sender_clone = self.table_notify.as_ref().unwrap().clone();
            let file_system_accessor = self.wal_manager.get_file_system_accessor();
            tokio::spawn(async move {
                WalManager::wal_persist_truncate_async(
                    uuid,
                    files_to_truncate,
                    file_to_persist,
                    file_system_accessor,
                    event_sender_clone,
                    iceberg_snapshot_wal_info,
                )
                .await;
            });
            true
        } else {
            false
        }
    }

    /// Handles the result of a persist and truncate operation.
    /// Returns the highest LSN that has been persisted into WAL.
    pub(crate) fn handle_completed_wal_persistence_update(
        &mut self,
        result: &WalPersistenceUpdateResult,
    ) -> Option<u64> {
        self.wal_manager
            .handle_complete_wal_persistence_update(result)
    }

    /// Drop the WAL files. Note that at the moment this drops WAL files in the mooncake table's local filesystem
    /// and so behaves the same as MooncakeTable.drop_table(),
    /// but in the future this is needed to support object storage
    pub(crate) async fn drop_wal(&mut self) -> Result<()> {
        self.wal_manager.drop_wal().await
    }

    pub fn push_wal_event(&mut self, event: &TableEvent) {
        self.wal_manager.push(event);
    }
}

#[cfg(test)]
mod mooncake_tests {
    use super::*;
    use crate::storage::storage_utils::create_data_file;

    #[test]
    fn test_flush_lsn_assertion() {
        // Only iceberg imported result.
        let iceberg_snapshot_result = IcebergSnapshotResult {
            uuid: uuid::Uuid::new_v4(),
            table_manager: None,
            flush_lsn: 1,
            corresponding_wal_metadata: IcebergCorrespondingWalMetadata {
                earliest_wal_file_num: 0,
            },
            new_table_schema: None,
            committed_deletion_logs: HashSet::new(),
            import_result: IcebergSnapshotImportResult {
                new_data_files: vec![create_data_file(
                    /*file_id=*/ 0,
                    "file_path".to_string(),
                )],
                puffin_blob_ref: HashMap::new(),
                new_file_indices: vec![],
            },
            index_merge_result: IcebergSnapshotIndexMergeResult::default(),
            data_compaction_result: IcebergSnapshotDataCompactionResult::default(),
        };
        // Valid snapshot result.
        MooncakeTable::assert_flush_lsn_on_iceberg_snapshot_res(
            /*persistence_lsn=*/ None,
            &iceberg_snapshot_result,
        );
        // Invalid snapshot result.
        let res_copy = iceberg_snapshot_result.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            MooncakeTable::assert_flush_lsn_on_iceberg_snapshot_res(Some(2), &res_copy);
        }));
        assert!(result.is_err());

        // Only data compaction result.
        let mut res_copy = iceberg_snapshot_result.clone();
        res_copy.import_result = IcebergSnapshotImportResult::default();
        res_copy.data_compaction_result = IcebergSnapshotDataCompactionResult {
            old_data_files_removed: vec![create_data_file(
                /*file_id=*/ 0,
                "file_path".to_string(),
            )],
            ..Default::default()
        };
        // Valid snapshot result.
        MooncakeTable::assert_flush_lsn_on_iceberg_snapshot_res(
            /*persistence_lsn=*/ Some(1),
            &res_copy,
        );
        // Invalid snapshot result.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            MooncakeTable::assert_flush_lsn_on_iceberg_snapshot_res(Some(2), &res_copy);
        }));
        assert!(result.is_err());

        // Contain both import and data compaction result.
        let mut res_copy = iceberg_snapshot_result.clone();
        res_copy.data_compaction_result = IcebergSnapshotDataCompactionResult {
            old_data_files_removed: vec![create_data_file(
                /*file_id=*/ 0,
                "file_path".to_string(),
            )],
            ..Default::default()
        };
        // Valid snapshot result.
        MooncakeTable::assert_flush_lsn_on_iceberg_snapshot_res(
            /*persistence_lsn=*/ Some(1),
            &res_copy,
        );
    }
}

#[cfg(test)]
impl MooncakeTable {
    pub fn get_table_id(&self) -> u32 {
        self.metadata.table_id
    }

    pub(crate) fn get_snapshot_watch_sender(&self) -> watch::Sender<u64> {
        self.table_snapshot_watch_sender.clone()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_utils;

#[cfg(test)]
mod data_file_state_tests;

#[cfg(test)]
mod deletion_vector_puffin_state_tests;

#[cfg(test)]
mod file_index_state_tests;

#[cfg(test)]
pub(crate) mod table_accessor_test_utils;

#[cfg(test)]
pub(crate) mod table_creation_test_utils;

#[cfg(test)]
pub(crate) mod validation_test_utils;

#[cfg(test)]
pub(crate) mod table_operation_test_utils;

#[cfg(test)]
pub(crate) mod test_utils_commons;

#[cfg(test)]
pub(crate) mod cache_test_utils;
