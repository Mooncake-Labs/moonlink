mod column_array_builder;
mod data_batches;
pub(crate) mod delete_vector;
mod disk_slice;
mod mem_slice;
mod snapshot;

use super::index::{get_lookup_key, MemIndex, MooncakeIndex};
use super::storage_utils::{RawDeletionRecord, RecordLocation};
use crate::error::{Error, Result};
use crate::row::MoonlinkRow;

use std::collections::HashMap;
use std::mem::take;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use delete_vector::BatchDeletionVector;
pub(crate) use disk_slice::DiskSliceWriter;
use mem_slice::MemSlice;
pub(crate) use snapshot::SnapshotTableState;
use tokio::spawn;
use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;
pub(crate) struct TableConfig {
    /// mem slice size
    ///
    _mem_slice_size: usize,
    batch_size: usize,
}

impl TableConfig {
    #[cfg(debug_assertions)]
    const DEFAULT_MEM_SLICE_SIZE: usize = 2048 * 2;
    #[cfg(debug_assertions)]
    const DEFAULT_BATCH_SIZE: usize = 4;

    #[cfg(not(debug_assertions))]
    const DEFAULT_MEM_SLICE_SIZE: usize = 2048 * 16;
    #[cfg(not(debug_assertions))]
    const DEFAULT_BATCH_SIZE: usize = 2048;

    pub fn new() -> Self {
        Self {
            _mem_slice_size: Self::DEFAULT_MEM_SLICE_SIZE,
            batch_size: Self::DEFAULT_BATCH_SIZE,
        }
    }
}

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
    pub(crate) get_lookup_key: fn(&MoonlinkRow) -> u64,
}

/// Snapshot contains state of the table at a given time.
/// A snapshot maps directly to an iceberg snapshot.
///
pub struct Snapshot {
    /// Warehouse URI for the catalog.
    pub(crate) warehouse_uri: String,
    /// table metadata
    pub(crate) metadata: Arc<TableMetadata>,
    /// datafile and their deletion vectors
    pub(crate) disk_files: HashMap<PathBuf, BatchDeletionVector>,
    /// Current snapshot version
    snapshot_version: u64,
    /// indices
    indices: MooncakeIndex,
}

impl Snapshot {
    pub(crate) fn new(metadata: Arc<TableMetadata>) -> Self {
        Self {
            // Provide default warehouse location at filesystem.
            warehouse_uri: "/tmp/moonlink_iceberg_warehouse".to_string(),
            metadata,
            disk_files: HashMap::new(),
            snapshot_version: 0,
            indices: MooncakeIndex::new(),
        }
    }

    // TODO(hjiang): Currently development between mooncake table and iceberg is independent, this interface is left for unit test purpose.
    // After end-to-end integration, warehouse information should be passed down from postgres at `Snapshot` initialization.
    pub fn _set_warehouse_info(&mut self, warehouse_uri: String) {
        self.warehouse_uri = warehouse_uri;
    }

    pub fn get_name_for_inmemory_file(&self) -> PathBuf {
        Path::join(
            &self.metadata.path,
            format!(
                "inmemory_{}_{}_{}.parquet",
                self.metadata.name, self.metadata.id, self.snapshot_version
            ),
        )
    }
}

#[derive(Default)]
pub struct SnapshotTask {
    /// Current task
    ///
    new_disk_slices: Vec<DiskSliceWriter>,
    new_deletions: Vec<RawDeletionRecord>,
    new_record_batches: Vec<(u64, Arc<RecordBatch>)>,
    new_rows: Vec<MoonlinkRow>,
    new_mem_indices: Vec<Arc<MemIndex>>,
    new_lsn: u64,
    new_commit_point: Option<RecordLocation>,
    flushed_xacts: HashMap<u32, u64>,
    aborted_xacts: Vec<u32>,
}

impl SnapshotTask {
    pub fn new() -> Self {
        Self {
            new_disk_slices: Vec::new(),
            new_deletions: Vec::new(),
            new_record_batches: Vec::new(),
            new_rows: Vec::new(),
            new_mem_indices: Vec::new(),
            new_lsn: 0,
            new_commit_point: None,
            flushed_xacts: HashMap::new(),
            aborted_xacts: Vec::new(),
        }
    }

    pub fn should_create_snapshot(&self) -> bool {
        self.new_lsn > 0 || !self.new_disk_slices.is_empty()
    }
}

/// Used to track the state of a streamed transaction
/// Holds the memslice and pending deletes
struct TransactionStreamState {
    mem_slice: MemSlice,
    new_deletions: Vec<RawDeletionRecord>,
}

impl TransactionStreamState {
    fn new(schema: Arc<Schema>, batch_size: usize) -> Self {
        Self {
            mem_slice: MemSlice::new(schema, batch_size),
            new_deletions: Vec::new(),
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

    // Current snapshot of the table
    snapshot: Arc<RwLock<SnapshotTableState>>,

    table_snapshot_watch_sender: watch::Sender<u64>,
    table_snapshot_watch_receiver: watch::Receiver<u64>,
    next_snapshot_task: SnapshotTask,

    // Stream state per transaction
    transaction_stream_states: HashMap<u32, TransactionStreamState>,
}

impl MooncakeTable {
    /// foreground functions
    ///
    pub fn new(schema: Schema, name: String, version: u64, base_path: PathBuf) -> Self {
        let table_config = TableConfig::new();
        let schema = Arc::new(schema);
        let metadata = Arc::new(TableMetadata {
            name,
            id: version,
            schema,
            config: table_config,
            path: base_path,
            get_lookup_key,
        });
        let (table_snapshot_watch_sender, table_snapshot_watch_receiver) = watch::channel(0);

        Self {
            mem_slice: MemSlice::new(metadata.schema.clone(), metadata.config.batch_size),
            metadata: metadata.clone(),
            snapshot: Arc::new(RwLock::new(SnapshotTableState::new(metadata))),
            next_snapshot_task: SnapshotTask::new(),
            transaction_stream_states: HashMap::new(),
            table_snapshot_watch_sender,
            table_snapshot_watch_receiver,
        }
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
        let lookup_key = (self.metadata.get_lookup_key)(&row);
        if let Some(batch) = self.mem_slice.append(lookup_key, &row)? {
            self.next_snapshot_task.new_record_batches.push(batch);
            self.next_snapshot_task.new_rows = vec![];
        }
        self.next_snapshot_task.new_rows.push(row);
        Ok(())
    }

    pub fn delete(&mut self, row: MoonlinkRow, lsn: u64) {
        let lookup_key = (self.metadata.get_lookup_key)(&row);
        let mut record = RawDeletionRecord {
            lookup_key,
            lsn,
            pos: None,
            _row_identity: None,
            xact_id: None,
        };
        let pos = self.mem_slice.delete(&record);
        record.pos = pos;
        self.next_snapshot_task.new_deletions.push(record);
    }

    pub fn commit(&mut self, lsn: u64) {
        self.next_snapshot_task.new_lsn = lsn;
        self.next_snapshot_task.new_commit_point = Some(self.mem_slice.get_commit_check_point());
    }

    pub fn should_flush(&self) -> bool {
        self.mem_slice.get_num_rows() >= self.metadata.config.batch_size
    }

    fn get_or_create_stream_state(&mut self, xact_id: u32) -> &mut TransactionStreamState {
        self.transaction_stream_states
            .entry(xact_id)
            .or_insert_with(|| {
                TransactionStreamState::new(
                    self.metadata.schema.clone(),
                    self.metadata.config.batch_size,
                )
            })
    }

    pub fn append_in_stream_batch(&mut self, row: MoonlinkRow, xact_id: u32) -> Result<()> {
        let lookup_key = (self.metadata.get_lookup_key)(&row);
        let stream_state = self.get_or_create_stream_state(xact_id);

        stream_state.mem_slice.append(lookup_key, &row)?;

        Ok(())
    }

    pub fn delete_in_stream_batch(&mut self, row: MoonlinkRow, xact_id: u32) {
        let lookup_key = (self.metadata.get_lookup_key)(&row);
        let mut record = RawDeletionRecord {
            lookup_key,
            lsn: u64::MAX, // Updated at commit time
            pos: None,
            _row_identity: None,
            xact_id: Some(xact_id),
        };

        let stream_state = self.get_or_create_stream_state(xact_id);
        let pos = stream_state.mem_slice.delete(&record);
        if pos.is_none() {
            // Edge‑case: txn deletes a row that's still in the main mem_slice
            // NOTE: There is still a remaining edge case that is not yet supported:
            // In the event that we have two identical, rows A and B, with no primary key (using full row as
            // identifier). We may have a situation where we delete A during some streaming
            // transaction, and then delete A in a non-streaming transaction. In this case, we will
            // delete A twice instead of deleting A then B. A potential solution is to have the
            // main mem slice delete from the top of the matches rows and the streamin transaction
            // to delete from the bottom, but this needs a closer look.
            record.pos = self.mem_slice.find_non_deleted_position(&record);
            self.next_snapshot_task.new_deletions.push(record);
        }
    }

    pub fn abort_in_stream_batch(&mut self, xact_id: u32) {
        // Record abortion in snapshot task so we can remove any uncomitted deletions
        self.next_snapshot_task.aborted_xacts.push(xact_id);
        self.transaction_stream_states.remove(&xact_id);
    }

    fn inner_flush(
        mem_slice: &mut MemSlice,
        snapshot_task: &mut SnapshotTask,
        metadata: &Arc<TableMetadata>,
        lsn: u64,
    ) -> JoinHandle<Result<DiskSliceWriter>> {
        // Finalize the current batch (if needed)
        let (new_batch, batches, index) = mem_slice.drain().unwrap();

        if let Some(batch) = new_batch {
            snapshot_task.new_record_batches.push(batch);
            snapshot_task.new_rows.clear();
        }

        let index = Arc::new(index);
        snapshot_task.new_mem_indices.push(index.clone());

        let mut disk_slice = DiskSliceWriter::new(
            metadata.schema.clone(),
            metadata.path.clone(),
            batches,
            lsn,
            index,
        );
        // Spawn a task to build the disk slice asynchronously
        spawn(async move {
            disk_slice.write()?;
            Ok(disk_slice)
        })
    }

    pub(crate) fn flush_transaction_stream(
        &mut self,
        xact_id: u32,
        lsn: u64,
    ) -> Result<JoinHandle<Result<DiskSliceWriter>>> {
        if let Some(mut stream_state) = self.transaction_stream_states.remove(&xact_id) {
            let mem_slice = &mut stream_state.mem_slice;

            // We update our delete records with the last lsn of the transaction
            // Note that in the stream case we dont have this until commit time
            for deletion in stream_state.new_deletions.iter_mut() {
                deletion.lsn = lsn;
            }

            let snapshot_task = &mut self.next_snapshot_task;

            snapshot_task.new_lsn = lsn;

            for deletion in snapshot_task.new_deletions.iter_mut() {
                if deletion.xact_id == Some(xact_id) {
                    deletion.lsn = lsn;
                }
            }

            snapshot_task.flushed_xacts.insert(xact_id, lsn);

            Ok(Self::inner_flush(
                mem_slice,
                snapshot_task,
                &self.metadata,
                lsn,
            ))
        } else {
            Err(Error::TransactionNotFound(xact_id))
        }
    }

    // UNDONE(BATCH_INSERT):
    // flush uncommitted batches from big batch insert
    pub(crate) fn flush(&mut self, lsn: u64) -> JoinHandle<Result<DiskSliceWriter>> {
        Self::inner_flush(
            &mut self.mem_slice,
            &mut self.next_snapshot_task,
            &self.metadata,
            lsn,
        )
    }

    pub(crate) fn commit_flush(&mut self, disk_slice: DiskSliceWriter) -> Result<()> {
        self.next_snapshot_task.new_disk_slices.push(disk_slice);
        Ok(())
    }

    // Create a snapshot of the last committed version
    //
    pub(crate) fn create_snapshot(&mut self) -> Option<JoinHandle<u64>> {
        if !self.next_snapshot_task.should_create_snapshot() {
            return None;
        }
        let next_snapshot_task = take(&mut self.next_snapshot_task);
        Some(spawn(Self::create_snapshot_async(
            self.snapshot.clone(),
            next_snapshot_task,
        )))
    }

    pub(crate) fn notify_snapshot_reader(&self, lsn: u64) {
        self.table_snapshot_watch_sender.send(lsn).unwrap();
    }

    async fn create_snapshot_async(
        snapshot: Arc<RwLock<SnapshotTableState>>,
        next_snapshot_task: SnapshotTask,
    ) -> u64 {
        snapshot.write().await.update_snapshot(next_snapshot_task)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_utils;
