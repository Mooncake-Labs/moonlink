mod column_array_builder;
mod data_batches;
mod delete_vector;
mod disk_slice;
mod mem_slice;
mod snapshot;

use super::index::{get_lookup_key, MemIndex, MooncakeIndex};
use super::storage_utils::{RawDeletionRecord, RecordLocation};
use crate::error::Result;
use crate::row::MoonlinkRow;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use delete_vector::BatchDeletionVector;
pub(crate) use disk_slice::DiskSliceWriter;
use mem_slice::MemSlice;
use snapshot::SnapshotTableState;
use std::collections::HashMap;
use std::mem::take;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::spawn;
use tokio::task::JoinHandle;
struct TableConfig {
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

struct TableMetadata {
    /// table name
    name: String,
    /// table id
    id: u64,
    /// table schema
    schema: Arc<Schema>,
    /// table config
    config: TableConfig,
    /// storage path
    path: PathBuf,
    /// function to get lookup key from row
    pub(crate) get_lookup_key: fn(&MoonlinkRow) -> i64,
}

/// Snapshot contains state of the table at a given time.
/// A snapshot maps directly to an iceberg snapshot.
///
pub struct Snapshot {
    /// table metadata
    metadata: Arc<TableMetadata>,
    /// datafile and their deletion vectors
    disk_files: HashMap<PathBuf, BatchDeletionVector>,
    /// Current snapshot version
    snapshot_version: u64,
    /// indices
    indices: MooncakeIndex,
}

impl Snapshot {
    fn new(metadata: Arc<TableMetadata>) -> Self {
        Self {
            metadata,
            disk_files: HashMap::new(),
            snapshot_version: 0,
            indices: MooncakeIndex::new(),
        }
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
        }
    }

    pub fn should_create_snapshot(&self) -> bool {
        self.new_lsn > 0 || self.new_disk_slices.len() > 0
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
    next_snapshot_task: SnapshotTask,

    // UNDONE(BATCH_INSERT):
    // a memslice per transaction?
    _stream_write_mem_slices: HashMap<u64, MemSlice>,
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
        let table = Self {
            mem_slice: MemSlice::new(metadata.schema.clone(), metadata.config.batch_size),
            metadata: metadata.clone(),
            snapshot: Arc::new(RwLock::new(SnapshotTableState::new(metadata))),
            next_snapshot_task: SnapshotTask::new(),
            _stream_write_mem_slices: HashMap::new(),
        };

        table
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

    pub fn _append_in_stream_batch(&mut self, _row: MoonlinkRow, _xact_id: u64) -> Result<()> {
        todo!("Implement append in stream batch");
    }

    pub fn _delete_in_stream_batch(&mut self, _row: MoonlinkRow, _xact_id: u64) -> Result<()> {
        todo!("Implement delete in stream batch");
    }

    pub fn _commit_in_stream_batch(&mut self, _xact_id: u64) -> Result<()> {
        todo!("Implement commit in stream batch");
    }

    pub fn _abort_in_stream_batch(&mut self, _xact_id: u64) -> Result<()> {
        todo!("Implement abort in stream batch");
    }

    // UNDONE(BATCH_INSERT):
    // flush uncommitted batches from big batch insert
    pub fn flush(&mut self, lsn: u64) -> JoinHandle<Result<DiskSliceWriter>> {
        // finalize the current batch
        let (new_batch, batches, index) = self.mem_slice.drain().unwrap();
        if let Some(batch) = new_batch {
            self.next_snapshot_task.new_record_batches.push(batch);
            self.next_snapshot_task.new_rows = vec![];
        }
        let old_index = Arc::new(index);
        self.next_snapshot_task
            .new_mem_indices
            .push(old_index.clone());
        let mut disk_slice = DiskSliceWriter::new(
            self.metadata.schema.clone(),
            self.metadata.path.clone(),
            batches,
            lsn,
            old_index,
        );
        // Spawn a task to build the disk slice asynchronously
        spawn(async move {
            disk_slice.write()?;
            Ok(disk_slice)
        })
    }

    pub fn commit_flush(&mut self, disk_slice: DiskSliceWriter) -> Result<()> {
        self.next_snapshot_task.new_disk_slices.push(disk_slice);
        Ok(())
    }

    // Create a snapshot of the last committed version
    //
    pub fn create_snapshot(&mut self) -> Option<JoinHandle<()>> {
        if !self.next_snapshot_task.should_create_snapshot() {
            return None;
        }
        let next_snapshot_task = take(&mut self.next_snapshot_task);
        Some(spawn(Self::create_snapshot_async(
            self.snapshot.clone(),
            next_snapshot_task,
        )))
    }

    async fn create_snapshot_async(
        snapshot: Arc<RwLock<SnapshotTableState>>,
        next_snapshot_task: SnapshotTask,
    ) {
        snapshot
            .write()
            .unwrap()
            .update_snapshot(next_snapshot_task);
    }

    pub fn request_read(&self) -> Result<(Vec<PathBuf>, Vec<(usize, usize)>)> {
        let snapshot = self.snapshot.read().unwrap();
        snapshot.request_read()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_utils;
