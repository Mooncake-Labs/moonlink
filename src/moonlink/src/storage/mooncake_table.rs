mod data_batches;
pub(crate) mod delete_vector;
mod disk_slice;
mod mem_slice;
mod shared_array;
mod snapshot;
mod table_snapshot;
mod transaction_stream;

use super::iceberg::puffin_utils::PuffinBlobRef;
use super::index::{FileIndex, MemIndex, MooncakeIndex};
use super::storage_utils::{MooncakeDataFileRef, RawDeletionRecord, RecordLocation};
use crate::error::{Error, Result};
use crate::row::{IdentityProp, MoonlinkRow};
use crate::storage::iceberg::iceberg_table_manager::{
    IcebergTableConfig, IcebergTableManager, TableManager,
};
use crate::storage::mooncake_table::shared_array::SharedRowBufferSnapshot;
pub(crate) use crate::storage::mooncake_table::table_snapshot::{
    IcebergSnapshotPayload, IcebergSnapshotResult,
};
use crate::storage::storage_utils::FileId;
use std::collections::HashMap;
use std::mem::take;
use std::path::PathBuf;
use std::sync::Arc;
use transaction_stream::{TransactionStreamOutput, TransactionStreamState};

use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use delete_vector::BatchDeletionVector;
pub(crate) use disk_slice::DiskSliceWriter;
use mem_slice::MemSlice;
pub(crate) use snapshot::{PuffinDeletionBlobAtRead, SnapshotTableState};
use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;

#[derive(Clone, Debug)]
pub struct TableConfig {
    /// Number of batch records which decides when to flush records from MemSlice to disk.
    pub mem_slice_size: usize,
    /// Number of new deletion records which decides whether to create a new mooncake table snapshot.
    pub snapshot_deletion_record_count: usize,
    /// Max number of rows in MemSlice.
    pub batch_size: usize,
    /// Filesystem directory to store temporary files, used for union read.
    pub temp_files_directory: String,
    /// Disk slice parquet file flush threshold.
    pub disk_slice_parquet_file_size: usize,
    /// Number of new data files to trigger an iceberg snapshot.
    pub iceberg_snapshot_new_data_file_count: usize,
    /// Number of unpersisted committed delete logs to trigger an iceberg snapshot.
    pub iceberg_snapshot_new_committed_deletion_log: usize,
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
            temp_files_directory,
            iceberg_snapshot_new_data_file_count: Self::DEFAULT_ICEBERG_NEW_DATA_FILE_COUNT,
            iceberg_snapshot_new_committed_deletion_log:
                Self::DEFAULT_ICEBERG_SNAPSHOT_NEW_COMMITTED_DELETION_LOG,
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
pub(crate) struct DiskFileDeletionVector {
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
    /// TODO(hjiang): For the initial release and before we figure out a cache design, disk files are always local ones.
    pub(crate) disk_files: HashMap<MooncakeDataFileRef, DiskFileDeletionVector>,
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

    /// ---- States have been recorded by mooncake snapshot, and persisted into iceberg table ----
    /// These persisted items will be reflected to mooncake snapshot in the next invocation of periodic mooncake snapshot operation.
    ///
    /// Flush LSN for iceberg snapshot.
    iceberg_flush_lsn: Option<u64>,
    /// Persisted new data files.
    iceberg_persisted_data_files: Vec<MooncakeDataFileRef>,
    /// Puffin blobs which have been persisted into iceberg snapshot.
    iceberg_persisted_puffin_blob: HashMap<MooncakeDataFileRef, PuffinBlobRef>,
    /// Persisted new file indices.
    iceberg_persisted_file_indices: Vec<FileIndex>,
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
            iceberg_flush_lsn: None,
            iceberg_persisted_data_files: Vec::new(),
            iceberg_persisted_puffin_blob: HashMap::new(),
            iceberg_persisted_file_indices: Vec::new(),
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
            let file_indice = cur_disk_slice.get_file_indice();
            if let Some(file_indice) = file_indice {
                new_file_indices.push(file_indice);
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
}

impl MooncakeTable {
    /// foreground functions
    ///
    pub async fn new(
        schema: Schema,
        name: String,
        version: u64,
        base_path: PathBuf,
        identity: IdentityProp,
        iceberg_table_config: IcebergTableConfig,
        table_config: TableConfig,
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
            iceberg_table_config,
        )?);
        Self::new_with_table_manager(metadata, iceberg_table_manager, table_config).await
    }

    pub(crate) async fn new_with_table_manager(
        table_metadata: Arc<TableMetadata>,
        mut table_manager: Box<dyn TableManager>,
        table_config: TableConfig,
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
                SnapshotTableState::new(table_metadata, &mut *table_manager).await?,
            )),
            next_snapshot_task: SnapshotTask::new(table_config),
            transaction_stream_states: HashMap::new(),
            table_snapshot_watch_sender,
            table_snapshot_watch_receiver,
            next_file_id: 0,
            iceberg_table_manager: Some(table_manager),
            last_iceberg_snapshot_lsn: None,
        })
    }

    /// Set iceberg snapshot flush LSN, called after a snapshot operation.
    pub(crate) fn set_iceberg_snapshot_res(&mut self, iceberg_snapshot_res: IcebergSnapshotResult) {
        // ---- Update mooncake table fields ----
        let iceberg_flush_lsn = iceberg_snapshot_res.flush_lsn;
        assert!(
            self.last_iceberg_snapshot_lsn.is_none()
                || self.last_iceberg_snapshot_lsn.unwrap() < iceberg_flush_lsn
        );
        self.last_iceberg_snapshot_lsn = Some(iceberg_flush_lsn);

        assert!(self.iceberg_table_manager.is_none());
        self.iceberg_table_manager = Some(iceberg_snapshot_res.table_manager);

        // ---- Update next snapshot task fields ---
        assert!(self.next_snapshot_task.iceberg_flush_lsn.is_none());
        self.next_snapshot_task.iceberg_flush_lsn = Some(iceberg_flush_lsn);

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_data_files
            .is_empty());
        self.next_snapshot_task.iceberg_persisted_data_files = iceberg_snapshot_res.new_data_files;

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_puffin_blob
            .is_empty());
        self.next_snapshot_task.iceberg_persisted_puffin_blob =
            iceberg_snapshot_res.puffin_blob_ref;

        assert!(self
            .next_snapshot_task
            .iceberg_persisted_file_indices
            .is_empty());
        self.next_snapshot_task.iceberg_persisted_file_indices =
            iceberg_snapshot_res.imported_file_indices;
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
    fn create_snapshot_impl(
        &mut self,
        force_create: bool,
    ) -> Option<JoinHandle<(u64, Option<IcebergSnapshotPayload>)>> {
        self.next_snapshot_task.new_rows = Some(self.mem_slice.get_latest_rows());
        let next_snapshot_task = take(&mut self.next_snapshot_task);
        self.next_snapshot_task = SnapshotTask::new(self.metadata.config.clone());
        let cur_snapshot = self.snapshot.clone();
        Some(tokio::task::spawn(Self::create_snapshot_async(
            cur_snapshot,
            next_snapshot_task,
            force_create,
        )))
    }

    pub fn create_snapshot(&mut self) -> Option<JoinHandle<(u64, Option<IcebergSnapshotPayload>)>> {
        if !self.next_snapshot_task.should_create_snapshot() {
            return None;
        }
        self.create_snapshot_impl(/*force_create=*/ false)
    }

    pub fn force_create_snapshot(
        &mut self,
    ) -> Option<JoinHandle<(u64, Option<IcebergSnapshotPayload>)>> {
        self.create_snapshot_impl(/*force_snapshot=*/ true)
    }

    pub(crate) fn notify_snapshot_reader(&self, lsn: u64) {
        self.table_snapshot_watch_sender.send(lsn).unwrap();
    }

    /// Persist an iceberg snapshot.
    async fn persist_iceberg_snapshot_impl(
        mut iceberg_table_manager: Box<dyn TableManager>,
        snapshot_payload: IcebergSnapshotPayload,
    ) -> Result<IcebergSnapshotResult> {
        let flush_lsn = snapshot_payload.flush_lsn;
        let new_data_files = snapshot_payload.data_files.clone();
        let imported_file_indices = snapshot_payload.file_indices_to_import.clone();
        let removed_file_indices = snapshot_payload.file_indices_to_remove.clone();
        let puffin_blob_ref = iceberg_table_manager
            .sync_snapshot(snapshot_payload)
            .await?;
        Ok(IcebergSnapshotResult {
            table_manager: iceberg_table_manager,
            flush_lsn,
            new_data_files,
            imported_file_indices,
            removed_file_indices,
            puffin_blob_ref,
        })
    }
    pub(crate) fn persist_iceberg_snapshot(
        &mut self,
        snapshot_payload: IcebergSnapshotPayload,
    ) -> JoinHandle<Result<IcebergSnapshotResult>> {
        let iceberg_table_manager = self.iceberg_table_manager.take().unwrap();
        tokio::task::spawn(Self::persist_iceberg_snapshot_impl(
            iceberg_table_manager,
            snapshot_payload,
        ))
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
        force_create: bool,
    ) -> (u64, Option<IcebergSnapshotPayload>) {
        snapshot
            .write()
            .await
            .update_snapshot(next_snapshot_task, force_create)
            .await
    }

    // ================================
    // Test util functions
    // ================================
    //
    // Test util function, which updates mooncake table snapshot and create iceberg snapshot in a serial fashion.
    #[cfg(test)]
    pub(crate) async fn create_mooncake_and_iceberg_snapshot_for_test(&mut self) -> Result<()> {
        if let Some(mooncake_join_handle) = self.create_snapshot() {
            // Wait for the snapshot async task to complete.
            match mooncake_join_handle.await {
                Ok((lsn, payload)) => {
                    // Notify readers that the mooncake snapshot has been created.
                    self.notify_snapshot_reader(lsn);

                    // Create iceberg snapshot if possible
                    if let Some(payload) = payload {
                        let iceberg_join_handle = self.persist_iceberg_snapshot(payload);
                        match iceberg_join_handle.await {
                            Ok(iceberg_snapshot_res) => {
                                self.set_iceberg_snapshot_res(iceberg_snapshot_res?);
                            }
                            Err(e) => {
                                panic!("Iceberg snapshot task gets cancelled: {:?}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    panic!("failed to join snapshot handle: {:?}", e);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_utils;
