mod data_batches;
pub(crate) mod delete_vector;
mod disk_slice;
mod mem_slice;
mod persistence_buffer;
mod shared_array;
mod snapshot;
pub mod snapshot_read_output;
mod table_snapshot;
mod transaction_stream;

use super::iceberg::puffin_utils::PuffinBlobRef;
use super::index::index_merge_config::FileIndexMergeConfig;
use super::index::{FileIndex, MemIndex, MooncakeIndex};
use super::storage_utils::{MooncakeDataFileRef, RawDeletionRecord, RecordLocation};
use crate::error::{Error, Result};
use crate::row::{IdentityProp, MoonlinkRow};
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::compaction::compactor::{CompactionBuilder, CompactionFileParams};
pub(crate) use crate::storage::compaction::table_compaction::{
    DataCompactionPayload, DataCompactionResult,
};
use crate::storage::iceberg::iceberg_table_manager::{IcebergTableConfig, IcebergTableManager};
use crate::storage::iceberg::table_manager::{PersistenceFileParams, TableManager};
use crate::storage::index::persisted_bucket_hash_map::GlobalIndexBuilder;
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
#[cfg(test)]
use crate::storage::storage_utils::ProcessedDeletionRecord;
use crate::storage::storage_utils::{FileId, TableId};
use crate::table_notify::TableNotify;
use crate::NonEvictableHandle;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use table_snapshot::{IcebergSnapshotImportResult, IcebergSnapshotIndexMergeResult};
#[cfg(test)]
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tracing::info_span;
use tracing::Instrument;
use transaction_stream::{TransactionStreamOutput, TransactionStreamState};

#[cfg(test)]
use crate::storage::io_utils;
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use delete_vector::BatchDeletionVector;
pub(crate) use disk_slice::DiskSliceWriter;
use mem_slice::MemSlice;
pub(crate) use snapshot::{PuffinDeletionBlobAtRead, SnapshotTableState};
use tokio::sync::{watch, RwLock};

// TODO(hjiang): Add another threshold for merged file indices to trigger iceberg snapshot.
#[derive(Clone, Debug)]
pub struct TableConfig {
    /// Number of batch records which decides when to flush records from MemSlice to disk.
    pub mem_slice_size: usize,
    /// Number of new deletion records which decides whether to create a new mooncake table snapshot.
    pub snapshot_deletion_record_count: usize,
    /// Max number of rows in MemSlice.
    pub batch_size: usize,
    /// Disk slice parquet file flush threshold.
    pub disk_slice_parquet_file_size: usize,
    /// Number of new data files to trigger an iceberg snapshot.
    pub iceberg_snapshot_new_data_file_count: usize,
    /// Number of unpersisted committed delete logs to trigger an iceberg snapshot.
    pub iceberg_snapshot_new_committed_deletion_log: usize,
    /// Config for data compaction.
    pub data_compaction_config: DataCompactionConfig,
    /// Config for index merge.
    pub file_index_config: FileIndexMergeConfig,
    /// Filesystem directory to store temporary files, used for union read.
    pub temp_files_directory: String,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self::new(Self::DEFAULT_TEMP_FILE_DIRECTORY.to_string())
    }
}

impl TableConfig {
    #[cfg(debug_assertions)]
    pub(crate) const DEFAULT_MEM_SLICE_SIZE: usize = TableConfig::DEFAULT_BATCH_SIZE * 8;
    #[cfg(debug_assertions)]
    pub(super) const DEFAULT_SNAPSHOT_DELETION_RECORD_COUNT: usize = 1000;
    #[cfg(debug_assertions)]
    pub(crate) const DEFAULT_BATCH_SIZE: usize = 128;
    #[cfg(debug_assertions)]
    pub(crate) const DEFAULT_ICEBERG_NEW_DATA_FILE_COUNT: usize = 1;
    #[cfg(debug_assertions)]
    pub(crate) const DEFAULT_ICEBERG_SNAPSHOT_NEW_COMMITTED_DELETION_LOG: usize = 1000;
    #[cfg(debug_assertions)]
    pub(crate) const DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE: usize = 1024 * 1024 * 2; // 2MiB

    #[cfg(not(debug_assertions))]
    pub(crate) const DEFAULT_MEM_SLICE_SIZE: usize = TableConfig::DEFAULT_BATCH_SIZE * 32;
    #[cfg(not(debug_assertions))]
    pub(super) const DEFAULT_SNAPSHOT_DELETION_RECORD_COUNT: usize = 1000;
    #[cfg(not(debug_assertions))]
    pub(crate) const DEFAULT_BATCH_SIZE: usize = 4096;
    #[cfg(not(debug_assertions))]
    pub(crate) const DEFAULT_ICEBERG_NEW_DATA_FILE_COUNT: usize = 1;
    #[cfg(not(debug_assertions))]
    pub(crate) const DEFAULT_ICEBERG_SNAPSHOT_NEW_COMMITTED_DELETION_LOG: usize = 1000;
    #[cfg(not(debug_assertions))]
    pub(crate) const DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE: usize = 1024 * 1024 * 128; // 128MiB

    /// Default local directory to hold temporary files for union read.
    pub(crate) const DEFAULT_TEMP_FILE_DIRECTORY: &str = "/tmp/moonlink_temp_file";

    pub fn new(temp_files_directory: String) -> Self {
        Self {
            mem_slice_size: Self::DEFAULT_MEM_SLICE_SIZE,
            snapshot_deletion_record_count: Self::DEFAULT_SNAPSHOT_DELETION_RECORD_COUNT,
            batch_size: Self::DEFAULT_BATCH_SIZE,
            disk_slice_parquet_file_size: Self::DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE,
            iceberg_snapshot_new_data_file_count: Self::DEFAULT_ICEBERG_NEW_DATA_FILE_COUNT,
            iceberg_snapshot_new_committed_deletion_log:
                Self::DEFAULT_ICEBERG_SNAPSHOT_NEW_COMMITTED_DELETION_LOG,
            data_compaction_config: DataCompactionConfig::default(),
            file_index_config: FileIndexMergeConfig::default(),
            temp_files_directory,
        }
    }
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
    pub fn iceberg_snapshot_new_data_file_count(&self) -> usize {
        self.iceberg_snapshot_new_data_file_count
    }
    pub fn snapshot_deletion_record_count(&self) -> usize {
        self.snapshot_deletion_record_count
    }
    pub fn iceberg_snapshot_new_committed_deletion_log(&self) -> usize {
        self.iceberg_snapshot_new_committed_deletion_log
    }
}

#[derive(Debug)]
pub struct TableMetadata {
    /// table name
    pub(crate) name: String,
    /// table id
    pub(crate) id: u64,
    /// table schema
    pub(crate) schema: Arc<Schema>,
    /// table config
    pub(crate) config: TableConfig,
    /// storage path
    pub(crate) path: PathBuf,
    /// function to get lookup key from row
    pub(crate) identity: IdentityProp,
}

#[derive(Clone, Debug)]
pub(crate) struct DiskFileEntry {
    /// Cache handle. If assigned, it's pinned in object storage cache.
    pub(crate) cache_handle: Option<NonEvictableHandle>,
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
    pub(crate) data_file_flush_lsn: Option<u64>,
    /// indices
    pub(crate) indices: MooncakeIndex,
}

impl Snapshot {
    pub(crate) fn new(metadata: Arc<TableMetadata>) -> Self {
        Self {
            metadata,
            disk_files: HashMap::new(),
            snapshot_version: 0,
            data_file_flush_lsn: None,
            indices: MooncakeIndex::new(),
        }
    }

    pub fn get_name_for_inmemory_file(&self) -> PathBuf {
        let mut directory = PathBuf::from(&self.metadata.config.temp_files_directory);
        directory.push(format!(
            "inmemory_{}_{}_{}.parquet",
            self.metadata.name, self.metadata.id, self.snapshot_version
        ));
        directory
    }
}

/// Record iceberg persisted records, used to sync to mooncake snapshot.
#[derive(Debug, Default)]
pub(crate) struct IcebergPersistedRecords {
    /// Flush LSN for iceberg snapshot.
    flush_lsn: Option<u64>,
    /// Persisted new data files.
    data_files: Vec<MooncakeDataFileRef>,
    /// Puffin blobs which have been persisted into iceberg snapshot.
    puffin_blob: HashMap<FileId, PuffinBlobRef>,
    /// Persisted new file indices generated by new write operations.
    file_indices: Vec<FileIndex>,
    /// Persisted new merged file indices.
    new_merged_file_indices: Vec<FileIndex>,
    /// Persisted old merged file indices.
    old_merged_file_indices: Vec<FileIndex>,
    /// Persisted new compacted data files.
    new_compacted_data_files: Vec<MooncakeDataFileRef>,
    /// Persisted old compacted data files.
    old_compacted_data_files: Vec<MooncakeDataFileRef>,
    /// Persisted new compacted file indices.
    new_compacted_file_indices: Vec<FileIndex>,
    /// Persisted old compacted file indices.
    old_compacted_file_indices: Vec<FileIndex>,
}

#[derive(Default)]
pub struct SnapshotTask {
    /// ---- States not recorded by mooncake snapshot ----
    ///
    /// Mooncake table config.
    mooncake_table_config: TableConfig,
    /// Current task
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

    /// streaming xact
    new_streaming_xact: Vec<TransactionStreamOutput>,

    /// --- States related to read operation ---
    read_cache_handles: Vec<NonEvictableHandle>,

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
    pub fn new(mooncake_table_config: TableConfig) -> Self {
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
            new_streaming_xact: Vec::new(),
            // Read request related fields.
            read_cache_handles: Vec::new(),
            // Index merge related fields.
            index_merge_result: FileIndiceMergeResult::default(),
            // Data compaction related fields.
            data_compaction_result: DataCompactionResult::default(),
            // Iceberg persistence result.
            iceberg_persisted_records: IcebergPersistedRecords::default(),
        }
    }

    pub fn should_create_snapshot(&self) -> bool {
        self.new_commit_lsn > 0
            || !self.new_disk_slices.is_empty()
            || self.new_deletions.len()
                >= self.mooncake_table_config.snapshot_deletion_record_count()
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

/// Options to create mooncake snapshot.
#[derive(Clone, Debug)]
pub struct SnapshotOption {
    /// Whether to force create snapshot.
    /// When specified, mooncake snapshot will be created with snapshot threshold ignored.
    pub(crate) force_create: bool,
    /// Whether to skip iceberg snapshot creation.
    pub(crate) skip_iceberg_snapshot: bool,
    /// Whether to skip file indices merge payload creation.
    pub(crate) skip_file_indices_merge: bool,
    /// Whether to skip data file compaction payload creation.
    pub(crate) skip_data_file_compaction: bool,
}

impl SnapshotOption {
    pub fn default() -> SnapshotOption {
        Self {
            force_create: false,
            skip_iceberg_snapshot: false,
            skip_file_indices_merge: false,
            skip_data_file_compaction: false,
        }
    }
}

/// MooncakeTable is a disk table + mem slice.
/// Transactions will append data to the mem slice.
///
/// And periodically disk slices will be merged and compacted.
/// Single thread is used to write to the table.
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

    table_snapshot_watch_sender: watch::Sender<u64>,
    table_snapshot_watch_receiver: watch::Receiver<u64>,

    /// Records all the write operations since last snapshot.
    next_snapshot_task: SnapshotTask,

    /// Stream state per transaction, keyed by xact-id.
    transaction_stream_states: HashMap<u32, TransactionStreamState>,

    /// Auto increment id for generating unique file ids.
    /// Note, these ids is only used locally, and not persisted.
    next_file_id: u32,

    /// Iceberg table manager, used to sync snapshot to the corresponding iceberg table.
    iceberg_table_manager: Option<Box<dyn TableManager>>,

    /// LSN of the latest iceberg snapshot.
    last_iceberg_snapshot_lsn: Option<u64>,

    /// Table notifier, which is used to sent multiple types of event completion information.
    table_notify: Option<Sender<TableNotify>>,
}

impl MooncakeTable {
    /// foreground functions
    ///
    /// TODO(hjiang): Provide a struct to hold all paramters.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        schema: Schema,
        name: String,
        version: u64,
        base_path: PathBuf,
        identity: IdentityProp,
        iceberg_table_config: IcebergTableConfig,
        table_config: TableConfig,
        object_storage_cache: ObjectStorageCache,
    ) -> Result<Self> {
        let metadata = Arc::new(TableMetadata {
            name,
            id: version,
            schema: Arc::new(schema.clone()),
            config: table_config.clone(),
            path: base_path,
            identity,
        });
        let iceberg_table_manager = Box::new(IcebergTableManager::new(
            metadata.clone(),
            object_storage_cache.clone(),
            iceberg_table_config,
        )?);
        Self::new_with_table_manager(metadata, iceberg_table_manager, object_storage_cache).await
    }

    pub(crate) async fn new_with_table_manager(
        table_metadata: Arc<TableMetadata>,
        mut table_manager: Box<dyn TableManager>,
        object_storage_cache: ObjectStorageCache,
    ) -> Result<Self> {
        let (table_snapshot_watch_sender, table_snapshot_watch_receiver) = watch::channel(0);
        Ok(Self {
            mem_slice: MemSlice::new(
                table_metadata.schema.clone(),
                table_metadata.config.batch_size,
                table_metadata.identity.clone(),
            ),
            metadata: table_metadata.clone(),
            snapshot: Arc::new(RwLock::new(
                SnapshotTableState::new(
                    table_metadata.clone(),
                    object_storage_cache,
                    &mut *table_manager,
                )
                .await?,
            )),
            next_snapshot_task: SnapshotTask::new(table_metadata.as_ref().config.clone()),
            transaction_stream_states: HashMap::new(),
            table_snapshot_watch_sender,
            table_snapshot_watch_receiver,
            next_file_id: 0,
            iceberg_table_manager: Some(table_manager),
            last_iceberg_snapshot_lsn: None,
            table_notify: None,
        })
    }

    /// Register event completion notifier.
    /// Notice it should be registered only once, which could be used to notify multiple events.
    pub(crate) async fn register_table_notify(&mut self, table_notify: Sender<TableNotify>) {
        assert!(self.table_notify.is_none());
        self.table_notify = Some(table_notify.clone());
        self.snapshot
            .write()
            .await
            .register_table_notify(table_notify);
    }

    /// Set iceberg snapshot flush LSN, called after a snapshot operation.
    pub(crate) fn set_iceberg_snapshot_res(&mut self, iceberg_snapshot_res: IcebergSnapshotResult) {
        // ---- Update mooncake table fields ----
        let flush_lsn = iceberg_snapshot_res.flush_lsn;

        // Whether the iceberg snapshot result contains new write operations from mooncake table (append/delete).
        let contains_new_writes = |res: &IcebergSnapshotResult| {
            if !res.import_result.new_data_files.is_empty() {
                assert!(!res.import_result.imported_file_indices.is_empty());
                return true;
            }
            if !res.import_result.puffin_blob_ref.is_empty() {
                return true;
            }

            false
        };

        // There're two types of operations could trigger iceberg snapshot: (1) index merge / data compaction; (2) table writes, including append and delete.
        // The first type is safe to import to iceberg at any time, with no flush LSN advancement.
        if contains_new_writes(&iceberg_snapshot_res) {
            assert!(
                self.last_iceberg_snapshot_lsn.is_none()
                    || self.last_iceberg_snapshot_lsn.unwrap() < flush_lsn
            );
        } else {
            assert!(
                self.last_iceberg_snapshot_lsn.is_none()
                    || self.last_iceberg_snapshot_lsn.unwrap() <= flush_lsn
            );
        }
        self.last_iceberg_snapshot_lsn = Some(flush_lsn);

        assert!(self.iceberg_table_manager.is_none());
        self.iceberg_table_manager = Some(iceberg_snapshot_res.table_manager);

        // ---- Buffer iceberg persisted content to next snapshot task ---
        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .flush_lsn
            .is_none());
        self.next_snapshot_task.iceberg_persisted_records.flush_lsn = Some(flush_lsn);

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .data_files
            .is_empty());
        self.next_snapshot_task.iceberg_persisted_records.data_files =
            iceberg_snapshot_res.import_result.new_data_files;

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .puffin_blob
            .is_empty());
        self.next_snapshot_task
            .iceberg_persisted_records
            .puffin_blob = iceberg_snapshot_res.import_result.puffin_blob_ref;

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .file_indices
            .is_empty());
        self.next_snapshot_task
            .iceberg_persisted_records
            .file_indices = iceberg_snapshot_res.import_result.imported_file_indices;

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .new_merged_file_indices
            .is_empty());
        self.next_snapshot_task
            .iceberg_persisted_records
            .new_merged_file_indices = iceberg_snapshot_res
            .index_merge_result
            .new_file_indices_to_import;

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_records
            .old_merged_file_indices
            .is_empty());
        self.next_snapshot_task
            .iceberg_persisted_records
            .old_merged_file_indices = iceberg_snapshot_res
            .index_merge_result
            .old_file_indices_to_remove;
    }

    /// Set read request completion result, which will be sync-ed to mooncake table snapshot in the next periodic snapshot iteration.
    pub(crate) fn set_read_request_res(&mut self, cache_handles: Vec<NonEvictableHandle>) {
        self.next_snapshot_task
            .read_cache_handles
            .extend(cache_handles);
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
    }

    /// Shutdown the current table, which unpins all referenced data files in the global data file.
    pub async fn shutdown(&mut self) -> Result<()> {
        let evicted_files_to_delete = {
            let mut guard = self.snapshot.write().await;
            guard.unreference_all_cache_handles().await
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
        self.next_snapshot_task.new_flush_lsn = Some(lsn);

        if self.mem_slice.is_empty() {
            return Ok(());
        }

        let next_file_id = self.next_file_id;
        self.next_file_id += 1;
        // Flush data files into iceberb table.
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
        self.next_snapshot_task.new_rows = Some(self.mem_slice.get_latest_rows());
        let next_snapshot_task = std::mem::take(&mut self.next_snapshot_task);
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
    pub fn create_snapshot(&mut self, opt: SnapshotOption) -> bool {
        if !self.next_snapshot_task.should_create_snapshot() && !opt.force_create {
            return false;
        }
        self.create_snapshot_impl(opt);
        true
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
                old_file_indices: file_indice_merge_payload.file_indices,
                new_file_indices: vec![merged],
            };
            table_notify_tx_copy
                .send(TableNotify::IndexMerge { index_merge_result })
                .await
                .unwrap();
        });
    }

    /// Perform data compaction, whose completion will be notified separately in async style.
    pub(crate) fn perform_data_compaction(&mut self, compaction_payload: DataCompactionPayload) {
        let next_file_id = self.next_file_id;
        self.next_file_id += 1;
        let file_params = CompactionFileParams {
            dir_path: self.metadata.path.clone(),
            table_auto_incr_id: next_file_id,
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
                    .send(TableNotify::DataCompaction {
                        data_compaction_result,
                    })
                    .await
                    .unwrap();
            }
            .instrument(info_span!("data_compaction")),
        );
    }

    pub(crate) fn notify_snapshot_reader(&self, lsn: u64) {
        self.table_snapshot_watch_sender.send(lsn).unwrap();
    }

    /// Persist an iceberg snapshot.
    async fn persist_iceberg_snapshot_impl(
        mut iceberg_table_manager: Box<dyn TableManager>,
        snapshot_payload: IcebergSnapshotPayload,
        table_notify: Sender<TableNotify>,
        table_auto_incr_id: u32,
    ) {
        let flush_lsn = snapshot_payload.flush_lsn;

        let new_imported_data_files_count = snapshot_payload.import_payload.data_files.len();
        let new_compacted_data_files_count = snapshot_payload
            .data_compaction_payload
            .new_data_files_to_import
            .len();

        let new_imported_file_indices_count = snapshot_payload.import_payload.file_indices.len();
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

        let persistence_file_params = PersistenceFileParams { table_auto_incr_id };
        let iceberg_persistence_res = iceberg_table_manager
            .sync_snapshot(snapshot_payload, persistence_file_params)
            .await;

        // Notify on event error.
        if iceberg_persistence_res.is_err() {
            table_notify
                .send(TableNotify::IcebergSnapshot {
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

        let new_file_indices_cutoff_index_1 = new_imported_file_indices_count;
        let new_file_indices_cutoff_index_2 =
            new_file_indices_cutoff_index_1 + new_merged_file_indices_count;
        let new_file_indices_cutoff_index_3 =
            new_file_indices_cutoff_index_2 + new_compacted_file_indices_count;
        assert_eq!(
            new_file_indices_cutoff_index_3,
            iceberg_persistence_res.remote_file_indices.len()
        );

        let snapshot_result = IcebergSnapshotResult {
            table_manager: iceberg_table_manager,
            flush_lsn,
            import_result: IcebergSnapshotImportResult {
                new_data_files: iceberg_persistence_res.remote_data_files
                    [0..new_data_files_cutoff_index_1]
                    .to_vec(),
                puffin_blob_ref: iceberg_persistence_res.puffin_blob_ref,
                imported_file_indices: iceberg_persistence_res.remote_file_indices
                    [0..new_file_indices_cutoff_index_1]
                    .to_vec(),
            },
            index_merge_result: IcebergSnapshotIndexMergeResult {
                new_file_indices_to_import: iceberg_persistence_res.remote_file_indices
                    [new_file_indices_cutoff_index_1..new_file_indices_cutoff_index_2]
                    .to_vec(),
                old_file_indices_to_remove: old_file_indices_to_remove_by_index_merge,
            },
            data_compaction_result: IcebergSnapshotDataCompactionResult {
                new_data_files_to_import: iceberg_persistence_res.remote_data_files
                    [new_data_files_cutoff_index_1..new_data_files_cutoff_index_2]
                    .to_vec(),
                old_data_files_to_remove: old_data_files_to_remove_by_compaction,
                new_file_indices_to_import: iceberg_persistence_res.remote_file_indices
                    [new_file_indices_cutoff_index_2..new_file_indices_cutoff_index_3]
                    .to_vec(),
                old_file_indices_to_remove: old_file_indices_to_remove_by_compaction,
            },
        };
        table_notify
            .send(TableNotify::IcebergSnapshot {
                iceberg_snapshot_result: Ok(snapshot_result),
            })
            .await
            .unwrap();
    }

    /// Create an iceberg snapshot.
    pub(crate) fn persist_iceberg_snapshot(&mut self, snapshot_payload: IcebergSnapshotPayload) {
        let iceberg_table_manager = self.iceberg_table_manager.take().unwrap();
        // Create a detached task, whose completion will be notified separately.
        let table_auto_incre_id = self.next_file_id;
        self.next_file_id += 1;
        tokio::task::spawn(
            Self::persist_iceberg_snapshot_impl(
                iceberg_table_manager,
                snapshot_payload,
                self.table_notify.as_ref().unwrap().clone(),
                table_auto_incre_id,
            )
            .instrument(info_span!("persist_iceberg_snapshot")),
        );
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
        opt: SnapshotOption,
        table_notify: Sender<TableNotify>,
    ) {
        let snapshot_result = snapshot
            .write()
            .await
            .update_snapshot(next_snapshot_task, opt)
            .await;
        table_notify
            .send(TableNotify::MooncakeTableSnapshot {
                lsn: snapshot_result.commit_lsn,
                iceberg_snapshot_payload: snapshot_result.iceberg_snapshot_payload,
                data_compaction_payload: snapshot_result.data_compaction_payload,
                file_indice_merge_payload: snapshot_result.file_indices_merge_payload,
                evicted_data_files_to_delete: snapshot_result.evicted_data_files_to_delete,
            })
            .await
            .unwrap();
    }

    // ================================
    // Test util functions
    // ================================
    //
    // Test util function to block wait and get iceberg / file indices merge payload.
    #[cfg(test)]
    async fn sync_mooncake_snapshot(
        receiver: &mut Receiver<TableNotify>,
    ) -> (
        Option<IcebergSnapshotPayload>,
        Option<FileIndiceMergePayload>,
        Option<DataCompactionPayload>,
        Vec<String>,
    ) {
        let notification = receiver.recv().await.unwrap();
        if let TableNotify::MooncakeTableSnapshot {
            iceberg_snapshot_payload,
            file_indice_merge_payload,
            data_compaction_payload,
            evicted_data_files_to_delete,
            ..
        } = notification
        {
            (
                iceberg_snapshot_payload,
                file_indice_merge_payload,
                data_compaction_payload,
                evicted_data_files_to_delete,
            )
        } else {
            panic!("Expected mooncake snapshot completion notification, but get others.");
        }
    }

    /// Test util function to sync on completed read request.
    #[cfg(test)]
    pub(crate) async fn sync_read_request(&mut self, receiver: &mut Receiver<TableNotify>) {
        let notification = receiver.recv().await.unwrap();
        if let TableNotify::ReadRequest { cache_handles } = notification {
            self.set_read_request_res(cache_handles);
        }
    }

    /// Test util function to block wait evicted data file to delete request, and check whether matches expected data files.
    #[cfg(test)]
    pub(crate) async fn sync_delete_evicted_files(
        &mut self,
        receiver: &mut Receiver<TableNotify>,
        mut expected_files_to_delete: Vec<String>,
    ) {
        let notification = receiver.recv().await.unwrap();
        if let TableNotify::EvictedDataFilesToDelete {
            mut evicted_data_files,
        } = notification
        {
            evicted_data_files.sort();
            expected_files_to_delete.sort();
            assert_eq!(evicted_data_files, expected_files_to_delete);
        }
    }

    #[cfg(test)]
    async fn sync_iceberg_snapshot(
        receiver: &mut Receiver<TableNotify>,
    ) -> Result<IcebergSnapshotResult> {
        let notification = receiver.recv().await.unwrap();
        if let TableNotify::IcebergSnapshot {
            iceberg_snapshot_result,
        } = notification
        {
            iceberg_snapshot_result
        } else {
            panic!("Expected iceberg completion snapshot notification, but get mooncake one.");
        }
    }

    #[cfg(test)]
    async fn sync_index_merge(receiver: &mut Receiver<TableNotify>) -> FileIndiceMergeResult {
        let notification = receiver.recv().await.unwrap();
        if let TableNotify::IndexMerge { index_merge_result } = notification {
            index_merge_result
        } else {
            panic!("Expected index merge completion notification, but get another one.");
        }
    }

    #[cfg(test)]
    async fn sync_data_compaction(
        receiver: &mut Receiver<TableNotify>,
    ) -> Result<DataCompactionResult> {
        let notification = receiver.recv().await.unwrap();
        if let TableNotify::DataCompaction {
            data_compaction_result,
        } = notification
        {
            data_compaction_result
        } else {
            panic!("Expected data compaction completion notification, but get mooncake one.");
        }
    }

    // Test util function, which creates mooncake snapshot for testing.
    #[cfg(test)]
    pub(crate) async fn create_mooncake_snapshot_for_test(
        &mut self,
        receiver: &mut Receiver<TableNotify>,
    ) -> (
        Option<IcebergSnapshotPayload>,
        Option<FileIndiceMergePayload>,
        Option<DataCompactionPayload>,
        Vec<String>,
    ) {
        let mooncake_snapshot_created = self.create_snapshot(SnapshotOption {
            force_create: true,
            skip_iceberg_snapshot: false,
            skip_data_file_compaction: false,
            skip_file_indices_merge: false,
        });
        assert!(mooncake_snapshot_created);
        Self::sync_mooncake_snapshot(receiver).await
    }

    // Test util function, which updates mooncake table snapshot and create iceberg snapshot in a serial fashion.
    #[cfg(test)]
    pub(crate) async fn create_mooncake_and_iceberg_snapshot_for_test(
        &mut self,
        receiver: &mut Receiver<TableNotify>,
    ) -> Result<()> {
        // Create mooncake snapshot.
        let mooncake_snapshot_created = self.create_snapshot(SnapshotOption {
            force_create: true,
            skip_iceberg_snapshot: false,
            skip_file_indices_merge: false,
            skip_data_file_compaction: false,
        });
        if !mooncake_snapshot_created {
            return Ok(());
        }
        let (iceberg_snapshot_payload, _, _, evicted_data_files_to_delete) =
            Self::sync_mooncake_snapshot(receiver).await;

        // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
        io_utils::delete_local_files(evicted_data_files_to_delete)
            .await
            .unwrap();

        // Create iceberg snapshot if possible.
        if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
            self.persist_iceberg_snapshot(iceberg_snapshot_payload);
            let iceberg_snapshot_result = Self::sync_iceberg_snapshot(receiver).await;
            self.set_iceberg_snapshot_res(iceberg_snapshot_result.unwrap());
        }

        Ok(())
    }

    // Test util function to perform index merge, block wait its completion and reflect index merge result to mooncake table.
    // Return evicted files for assertion.
    #[cfg(test)]
    pub(crate) async fn perform_index_merge_for_test(
        &mut self,
        receiver: &mut Receiver<TableNotify>,
        index_merge_payload: FileIndiceMergePayload,
    ) -> Vec<String> {
        // Perform and block wait index merge.
        self.perform_index_merge(index_merge_payload);
        let index_merge_result = Self::sync_index_merge(receiver).await;

        self.set_file_indices_merge_res(index_merge_result);
        assert!(self.create_snapshot(SnapshotOption {
            force_create: true,
            skip_iceberg_snapshot: false,
            skip_file_indices_merge: false,
            skip_data_file_compaction: true,
        }));
        let (_, _, _, evicted_files_to_delete) = Self::sync_mooncake_snapshot(receiver).await;
        // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
        io_utils::delete_local_files(evicted_files_to_delete.clone())
            .await
            .unwrap();

        evicted_files_to_delete
    }

    // Test util function to perform data compaction, block wait its completion and reflect compaction result to mooncake table.
    // Return evicted files for assertion.
    #[cfg(test)]
    pub(crate) async fn perform_data_compaction_for_test(
        &mut self,
        receiver: &mut Receiver<TableNotify>,
        data_compaction_payload: DataCompactionPayload,
    ) -> Vec<String> {
        // Perform and block wait data compaction.
        self.perform_data_compaction(data_compaction_payload);
        let data_compaction_result = Self::sync_data_compaction(receiver).await;
        let data_compaction_result = data_compaction_result.unwrap();

        self.set_data_compaction_res(data_compaction_result);
        assert!(self.create_snapshot(SnapshotOption {
            force_create: true,
            skip_iceberg_snapshot: false,
            skip_file_indices_merge: true,
            skip_data_file_compaction: false,
        }));
        let (_, _, _, evicted_data_files_to_delete) = Self::sync_mooncake_snapshot(receiver).await;
        // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
        for cur_data_file in evicted_data_files_to_delete.iter() {
            tokio::fs::remove_file(cur_data_file).await.unwrap();
        }
        evicted_data_files_to_delete
    }

    // Test util function, which does the following things in serial fashion.
    // (1) updates mooncake table snapshot, (2) create iceberg snapshot, (3) trigger data compaction, (4) perform data compaction, (5) another mooncake and iceberg snapshot.
    //
    // # Arguments
    //
    // * injected_committed_deletion_rows: rows to delete and commit in between data compaction initiation and snapshot creation
    // * injected_uncommitted_deletion_rows: rows to delete but not commit in between data compaction initiation and snapshot creation
    #[cfg(test)]
    pub(crate) async fn create_mooncake_and_iceberg_snapshot_for_data_compaction_for_test(
        &mut self,
        receiver: &mut Receiver<TableNotify>,
        injected_committed_deletion_rows: Vec<(MoonlinkRow, u64 /*lsn*/)>,
        injected_uncommitted_deletion_rows: Vec<(MoonlinkRow, u64 /*lsn*/)>,
    ) -> Result<()> {
        // Create mooncake snapshot.
        let force_snapshot_option = SnapshotOption {
            force_create: true,
            skip_iceberg_snapshot: false,
            skip_file_indices_merge: true,
            skip_data_file_compaction: false,
        };
        assert!(self.create_snapshot(force_snapshot_option.clone()));

        // Create iceberg snapshot.
        let (iceberg_snapshot_payload, _, _, evicted_data_files_to_delete) =
            Self::sync_mooncake_snapshot(receiver).await;
        // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
        io_utils::delete_local_files(evicted_data_files_to_delete)
            .await
            .unwrap();

        if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
            self.persist_iceberg_snapshot(iceberg_snapshot_payload);
            let iceberg_snapshot_result = Self::sync_iceberg_snapshot(receiver).await;
            self.set_iceberg_snapshot_res(iceberg_snapshot_result.unwrap());
        }

        // Get data compaction payload.
        assert!(self.create_snapshot(force_snapshot_option.clone()));
        let (iceberg_snapshot_payload, _, data_compaction_payload, evicted_data_files_to_delete) =
            Self::sync_mooncake_snapshot(receiver).await;
        // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
        io_utils::delete_local_files(evicted_data_files_to_delete)
            .await
            .unwrap();

        assert!(iceberg_snapshot_payload.is_none());
        let data_compaction_payload = data_compaction_payload.unwrap();

        // Perform and block wait data compaction.
        self.perform_data_compaction(data_compaction_payload);
        let data_compaction_result = Self::sync_data_compaction(receiver).await;
        let data_compaction_result = data_compaction_result.unwrap();

        // Before create snapshot for compaction results, perform another deletion operations.
        for (cur_row, lsn) in injected_committed_deletion_rows {
            self.delete(cur_row, lsn).await;
            self.commit(/*lsn=*/ lsn);
        }
        for (cur_row, lsn) in injected_uncommitted_deletion_rows {
            self.delete(cur_row, lsn).await;
        }

        // Set data compaction result and trigger another iceberg snapshot.
        self.set_data_compaction_res(data_compaction_result);
        assert!(self.create_snapshot(SnapshotOption {
            force_create: true,
            skip_iceberg_snapshot: false,
            skip_file_indices_merge: true,
            skip_data_file_compaction: false,
        }));
        let (iceberg_snapshot_payload, _, _, evicted_data_files_to_delete) =
            Self::sync_mooncake_snapshot(receiver).await;
        // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
        io_utils::delete_local_files(evicted_data_files_to_delete)
            .await
            .unwrap();

        let iceberg_snapshot_payload = iceberg_snapshot_payload.unwrap();
        self.persist_iceberg_snapshot(iceberg_snapshot_payload);
        let iceberg_snapshot_result = Self::sync_iceberg_snapshot(receiver).await;
        self.set_iceberg_snapshot_res(iceberg_snapshot_result.unwrap());

        // Create mooncake snapshot after buffering iceberg snapshot result, to make sure mooncake snapshot is at a consistent state.
        assert!(self.create_snapshot(SnapshotOption {
            force_create: true,
            skip_iceberg_snapshot: true,
            skip_file_indices_merge: true,
            skip_data_file_compaction: true,
        }));

        Ok(())
    }

    // Test util function, which does the following things in serial fashion.
    // (1) updates mooncake table snapshot, (2) create iceberg snapshot, (3) trigger index merge, (4) perform index merge, (5) another mooncake and iceberg snapshot.
    #[cfg(test)]
    pub(crate) async fn create_mooncake_and_iceberg_snapshot_for_index_merge_for_test(
        &mut self,
        receiver: &mut Receiver<TableNotify>,
    ) -> Result<()> {
        // Create mooncake snapshot.
        let force_snapshot_option = SnapshotOption {
            force_create: true,
            skip_iceberg_snapshot: false,
            skip_file_indices_merge: false,
            skip_data_file_compaction: false,
        };
        assert!(self.create_snapshot(force_snapshot_option.clone()));

        // Create iceberg snapshot.
        let (iceberg_snapshot_payload, _, _, evicted_data_files_to_delete) =
            Self::sync_mooncake_snapshot(receiver).await;
        // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
        io_utils::delete_local_files(evicted_data_files_to_delete)
            .await
            .unwrap();

        if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
            self.persist_iceberg_snapshot(iceberg_snapshot_payload);
            let iceberg_snapshot_result = Self::sync_iceberg_snapshot(receiver).await;
            self.set_iceberg_snapshot_res(iceberg_snapshot_result.unwrap());
        }

        // Perform index merge.
        assert!(self.create_snapshot(force_snapshot_option.clone()));
        let (iceberg_snapshot_payload, file_indice_merge_payload, _, evicted_data_files_to_delete) =
            Self::sync_mooncake_snapshot(receiver).await;
        // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
        io_utils::delete_local_files(evicted_data_files_to_delete)
            .await
            .unwrap();

        assert!(iceberg_snapshot_payload.is_none());
        let file_indice_merge_payload = file_indice_merge_payload.unwrap();

        self.perform_index_merge(file_indice_merge_payload);
        let index_merge_result = Self::sync_index_merge(receiver).await;
        self.set_file_indices_merge_res(index_merge_result);
        assert!(self.create_snapshot(SnapshotOption {
            force_create: true,
            skip_iceberg_snapshot: false,
            skip_file_indices_merge: false,
            skip_data_file_compaction: false,
        }));
        let (iceberg_snapshot_payload, _, _, evicted_data_files_to_delete) =
            Self::sync_mooncake_snapshot(receiver).await;
        // Delete evicted object storage cache entries immediately to make sure later accesses all happen on persisted files.
        io_utils::delete_local_files(evicted_data_files_to_delete)
            .await
            .unwrap();

        let iceberg_snapshot_payload = iceberg_snapshot_payload.unwrap();
        self.persist_iceberg_snapshot(iceberg_snapshot_payload);
        let iceberg_snapshot_result = Self::sync_iceberg_snapshot(receiver).await;
        self.set_iceberg_snapshot_res(iceberg_snapshot_result.unwrap());

        // Create mooncake snapshot after buffering iceberg snapshot result, to make sure mooncake snapshot is at a consistent state.
        assert!(self.create_snapshot(SnapshotOption {
            force_create: true,
            skip_iceberg_snapshot: true,
            skip_file_indices_merge: true,
            skip_data_file_compaction: true,
        }));

        Ok(())
    }

    /// Test util function to get committed and uncommitted deletion logs states.
    #[cfg(test)]
    pub(crate) async fn get_deletion_logs_for_snapshot(
        &mut self,
    ) -> (
        Vec<ProcessedDeletionRecord>,
        Vec<Option<ProcessedDeletionRecord>>,
    ) {
        let guard = self.snapshot.read().await;
        (
            guard.committed_deletion_log.clone(),
            guard.uncommitted_deletion_log.clone(),
        )
    }

    /// Test util function to get all disk files and their deletion vector.
    #[cfg(test)]
    pub(crate) async fn get_disk_files_for_snapshot(
        &mut self,
    ) -> HashMap<MooncakeDataFileRef, DiskFileEntry> {
        let guard = self.snapshot.read().await;
        guard.current_snapshot.disk_files.clone()
    }

    /// Test util function to get all index block files.
    #[cfg(test)]
    pub(crate) async fn get_index_block_files(&mut self) -> Vec<String> {
        let guard = self.snapshot.read().await;
        let mut index_block_files = vec![];
        for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
            for cur_index_block in cur_file_index.index_blocks.iter() {
                index_block_files.push(cur_index_block.index_file.file_path().clone());
            }
        }
        index_block_files
    }

    /// Test util function to get all index block files size.
    #[cfg(test)]
    pub(crate) async fn get_index_block_files_size(&mut self) -> u64 {
        let guard = self.snapshot.read().await;
        let mut index_blocks_file_size = 0;
        for cur_file_index in guard.current_snapshot.indices.file_indices.iter() {
            for cur_index_block in cur_file_index.index_blocks.iter() {
                index_blocks_file_size += cur_index_block.file_size;
            }
        }
        index_blocks_file_size
    }

    /// Test util function to get snapshot read output.
    #[cfg(test)]
    pub(crate) async fn request_read(&mut self) -> Result<SnapshotReadOutput> {
        let mut guard = self.snapshot.write().await;
        guard.request_read().await
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_utils;

#[cfg(test)]
pub(crate) mod state_test_utils;

#[cfg(test)]
mod data_file_state_tests;

#[cfg(test)]
mod deletion_vector_puffin_state_tests;

#[cfg(test)]
mod file_index_state_tests;
