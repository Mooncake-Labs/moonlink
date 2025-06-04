use super::data_batches::{create_batch_from_rows, InMemoryBatch};
use super::delete_vector::BatchDeletionVector;
use super::{
    DiskFileDeletionVector, IcebergSnapshotPayload, Snapshot, SnapshotTask, TableConfig,
    TableMetadata,
};
use crate::error::Result;
use crate::storage::iceberg::iceberg_table_manager::TableManager;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::index::{FileIndex, Index};
use crate::storage::mooncake_table::shared_array::SharedRowBufferSnapshot;
use crate::storage::mooncake_table::MoonlinkRow;
use crate::storage::storage_utils::FileId;
use crate::storage::storage_utils::{
    MooncakeDataFile, MooncakeDataFileRef, ProcessedDeletionRecord, RawDeletionRecord,
    RecordLocation,
};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use std::collections::{BTreeMap, HashMap};
use std::mem::take;
use std::sync::Arc;

pub(crate) struct SnapshotTableState {
    /// Mooncake table config.
    mooncake_table_config: TableConfig,

    /// Current snapshot
    pub(super) current_snapshot: Snapshot,

    /// In memory RecordBatches, maps from batch id to in-memory batch.
    batches: BTreeMap<u64, InMemoryBatch>,

    /// Latest rows
    rows: Option<SharedRowBufferSnapshot>,

    // UNDONE(BATCH_INSERT):
    // Track uncommitted disk files/ batches from big batch insert

    // There're three types of deletion records:
    // 1. Uncommitted deletion logs
    // 2. Committed and persisted deletion logs, which are reflected at `snapshot::disk_files` along with the corresponding data files
    // 3. Committed but not yet persisted deletion logs
    //
    // Type-3, committed but not yet persisted deletion logs.
    pub(super) committed_deletion_log: Vec<ProcessedDeletionRecord>,
    // Type-1: uncommitted deletion logs.
    pub(super) uncommitted_deletion_log: Vec<Option<ProcessedDeletionRecord>>,

    /// Last commit point
    last_commit: RecordLocation,

    /// ---- Items not persisted to iceberg snapshot ----
    ///
    /// Iceberg snapshot is created in an async style, which means it doesn't correspond 1-1 to mooncake snapshot, so we need to ensure idempotency for iceberg snapshot payload.
    /// The following fields record unpersisted content, which will be placed in iceberg payload everytime.
    unpersisted_iceberg_records: UnpersistedIcebergSnapshotRecords,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PuffinDeletionBlobAtRead {
    /// Index of local data files.
    pub data_file_index: u32,
    /// Index of puffin filepaths.
    pub puffin_file_index: u32,
    pub start_offset: u32,
    pub blob_size: u32,
}

#[derive(Clone, Debug)]
struct UnpersistedIcebergSnapshotRecords {
    /// Unpersisted data files, new data files are appended to the end.
    unpersisted_data_files: Vec<MooncakeDataFileRef>,
    /// Unpersisted file indices, new file indices are appended to the end.
    unpersisted_file_indices: Vec<FileIndex>,
}

pub struct ReadOutput {
    /// Contains two parts:
    /// 1. Committed and persisted data files.
    /// 2. Associated files, which include committed but un-persisted records.
    pub data_file_paths: Vec<String>,
    /// Puffin file paths.
    pub puffin_file_paths: Vec<String>,
    /// Deletion vectors persisted in puffin files.
    pub deletion_vectors: Vec<PuffinDeletionBlobAtRead>,
    /// Committed but un-persisted positional deletion records.
    pub position_deletes: Vec<(u32 /*file_index*/, u32 /*row_index*/)>,
    /// Contains committed but non-persisted record batches, which are persisted as temporary data files on local filesystem.
    pub associated_files: Vec<String>,
}

impl SnapshotTableState {
    pub(super) async fn new(
        metadata: Arc<TableMetadata>,
        // TODO(hjiang): Used when recovery enabled.
        _iceberg_table_manager: &mut dyn TableManager,
    ) -> Result<Self> {
        let mut batches = BTreeMap::new();
        batches.insert(0, InMemoryBatch::new(metadata.config.batch_size));

        Ok(Self {
            mooncake_table_config: metadata.config.clone(),
            current_snapshot: Snapshot::new(metadata.clone()),
            batches,
            rows: None,
            last_commit: RecordLocation::MemoryBatch(0, 0),
            committed_deletion_log: Vec::new(),
            uncommitted_deletion_log: Vec::new(),
            unpersisted_iceberg_records: UnpersistedIcebergSnapshotRecords {
                unpersisted_data_files: Vec::new(),
                unpersisted_file_indices: Vec::new(),
            },
        })
    }

    /// Aggregate committed deletion logs, which could be persisted into iceberg snapshot.
    /// Return a mapping from local data filepath to its batch deletion vector.
    ///
    /// Precondition: all disk files have been integrated into snapshot.
    fn aggregate_committed_deletion_logs(
        &self,
        flush_lsn: u64,
    ) -> HashMap<MooncakeDataFileRef, BatchDeletionVector> {
        let mut aggregated_deletion_logs = HashMap::new();
        for cur_deletion_log in self.committed_deletion_log.iter() {
            assert!(
                cur_deletion_log.lsn <= self.current_snapshot.snapshot_version,
                "Committed deletion log {:?} is later than current snapshot LSN {}",
                cur_deletion_log,
                self.current_snapshot.snapshot_version
            );
            if cur_deletion_log.lsn > flush_lsn {
                continue;
            }
            if let RecordLocation::DiskFile(file_id, row_idx) = &cur_deletion_log.pos {
                let batch_deletion_vector = self.current_snapshot.disk_files.get(file_id).unwrap();
                let max_rows = batch_deletion_vector.batch_deletion_vector.get_max_rows();

                let cur_data_file = self
                    .current_snapshot
                    .disk_files
                    .get_key_value(file_id)
                    .unwrap()
                    .0
                    .clone();

                let deletion_vector = aggregated_deletion_logs
                    .entry(cur_data_file)
                    .or_insert_with(|| BatchDeletionVector::new(max_rows));
                assert!(deletion_vector.delete_row(*row_idx));
            }
        }
        aggregated_deletion_logs
    }

    /// Prune committed deletion logs for the given persisted records.
    fn prune_committed_deletion_logs(&mut self, task: &SnapshotTask) {
        // No iceberg snapshot persisted between two mooncake snapshot.
        if task.iceberg_flush_lsn.is_none() {
            return;
        }

        // Keep two types of committed logs: (1) in-memory committed deletion logs; (2) commit point after flush LSN.
        // All on-disk committed deletion logs, which are <= iceberg snapshot flush LSN could be pruned.
        let mut new_committed_deletion_log = vec![];
        let flush_point_lsn = task.iceberg_flush_lsn.unwrap();

        let old_committed_deletion_logs = std::mem::take(&mut self.committed_deletion_log);
        for cur_deletion_log in old_committed_deletion_logs.into_iter() {
            assert!(
                cur_deletion_log.lsn <= self.current_snapshot.snapshot_version,
                "Committed deletion log {:?} is later than current snapshot LSN {}",
                cur_deletion_log,
                self.current_snapshot.snapshot_version
            );
            if cur_deletion_log.lsn > flush_point_lsn {
                new_committed_deletion_log.push(cur_deletion_log);
                continue;
            }
            if let RecordLocation::MemoryBatch(_, _) = &cur_deletion_log.pos {
                new_committed_deletion_log.push(cur_deletion_log);
            }
        }

        self.committed_deletion_log = new_committed_deletion_log;
    }

    /// Update current mooncake snapshot with persisted deletion vector.
    fn update_current_snapshot_with_iceberg_snapshot(
        &mut self,
        puffin_blob_ref: HashMap<MooncakeDataFileRef, PuffinBlobRef>,
    ) {
        for (local_disk_file, puffin_blob_ref) in puffin_blob_ref.into_iter() {
            let entry = self
                .current_snapshot
                .disk_files
                .get_mut(&local_disk_file)
                .unwrap();
            entry.puffin_deletion_blob = Some(puffin_blob_ref);
        }
    }

    /// Update unpersisted data files from successful iceberg snapshot operation.
    fn prune_persisted_data_files(&mut self, persisted_new_data_files: Vec<MooncakeDataFileRef>) {
        assert!(self.unpersisted_iceberg_records.unpersisted_data_files.len() >= persisted_new_data_files.len(),
            "There're in total {} unpersisted data files, but successful iceberg snapshot shows {} data file persisted.",
            self.unpersisted_iceberg_records.unpersisted_data_files.len(),
            persisted_new_data_files.len());

        self.unpersisted_iceberg_records
            .unpersisted_data_files
            .drain(0..persisted_new_data_files.len());
    }

    /// Update unpersisted file indices from successful iceberg snapshot operation.
    fn prune_persisted_file_indices(&mut self, persisted_new_file_indices: Vec<FileIndex>) {
        assert!(self.unpersisted_iceberg_records.unpersisted_file_indices.len() >= persisted_new_file_indices.len(),
            "There're in total {} unpersisted file indices, but successful iceberg snapshot shows {} file indices persisted.",
            self.unpersisted_iceberg_records.unpersisted_file_indices.len(),
            persisted_new_file_indices.len());

        self.unpersisted_iceberg_records
            .unpersisted_file_indices
            .drain(0..persisted_new_file_indices.len());
    }

    /// Util function to decide whether to create iceberg snapshot by new data files.
    fn create_iceberg_snapshot_by_data_files(
        &self,
        new_data_files: &[Arc<MooncakeDataFile>],
        force_create: bool,
    ) -> bool {
        let data_file_snapshot_threshold = if !force_create {
            self.mooncake_table_config
                .iceberg_snapshot_new_data_file_count()
        } else {
            1
        };
        new_data_files.len() >= data_file_snapshot_threshold
    }
    /// Util function to decide whether to create iceberg snapshot by deletion vectors.
    fn create_iceberg_snapshot_by_committed_logs(&self, force_create: bool) -> bool {
        let deletion_record_snapshot_threshold = if !force_create {
            self.mooncake_table_config
                .iceberg_snapshot_new_committed_deletion_log()
        } else {
            1
        };
        self.committed_deletion_log.len() >= deletion_record_snapshot_threshold
    }

    pub(super) async fn update_snapshot(
        &mut self,
        mut task: SnapshotTask,
        force_create: bool,
    ) -> (u64, Option<IcebergSnapshotPayload>) {
        // Reflect iceberg snapshot to mooncake snapshot.
        self.prune_committed_deletion_logs(&task);
        self.prune_persisted_data_files(std::mem::take(&mut task.iceberg_persisted_data_files));
        self.prune_persisted_file_indices(std::mem::take(&mut task.iceberg_persisted_file_indices));
        self.update_current_snapshot_with_iceberg_snapshot(std::mem::take(
            &mut task.iceberg_persisted_puffin_blob,
        ));

        // Sync buffer snapshot states into current mooncake snapshot.
        //
        // To reduce iceberg write frequency, only create new iceberg snapshot when there're new data files.
        let new_data_files = task.get_new_data_files();
        let new_file_indices = task.get_new_file_indices();

        self.apply_transaction_stream(&mut task);
        self.merge_mem_indices(&mut task);
        self.finalize_batches(&mut task);
        self.integrate_disk_slices(&mut task);

        self.rows = take(&mut task.new_rows);
        self.process_deletion_log(&mut task).await;

        if let Some(flush_lsn) = task.new_flush_lsn {
            self.current_snapshot.data_file_flush_lsn = Some(flush_lsn);
        }
        if task.new_commit_lsn != 0 {
            self.current_snapshot.snapshot_version = task.new_commit_lsn;
        }
        if let Some(cp) = task.new_commit_point {
            self.last_commit = cp;
        }

        // Batch new data files, whether we decide to create an iceberg snapshot.
        self.unpersisted_iceberg_records
            .unpersisted_data_files
            .extend(new_data_files);
        self.unpersisted_iceberg_records
            .unpersisted_file_indices
            .extend(new_file_indices);

        // Till this point, committed changes have been reflected to current snapshot; sync the latest change to iceberg.
        // To reduce iceberg persistence overhead, we only snapshot when (1) there're persisted data files, or (2) accumulated unflushed deletion vector exceeds threshold.
        //
        // TODO(hjiang): Error handling for snapshot sync-up.
        let mut iceberg_snapshot_payload: Option<IcebergSnapshotPayload> = None;
        let flush_by_data_files = self.create_iceberg_snapshot_by_data_files(
            self.unpersisted_iceberg_records
                .unpersisted_data_files
                .as_slice(),
            force_create,
        );
        let flush_by_deletion_logs = self.create_iceberg_snapshot_by_committed_logs(force_create);

        if self.current_snapshot.data_file_flush_lsn.is_some()
            && (flush_by_data_files || flush_by_deletion_logs)
        {
            // Getting persistable committed deletion logs is not cheap, which requires iterating through all logs,
            // so we only aggregate when there's committed deletion.
            let flush_lsn = self.current_snapshot.data_file_flush_lsn.unwrap();
            let aggregated_committed_deletion_logs =
                self.aggregate_committed_deletion_logs(flush_lsn);

            // Only create iceberg snapshot when there's something to import.
            if !aggregated_committed_deletion_logs.is_empty() || flush_by_data_files {
                iceberg_snapshot_payload = Some(IcebergSnapshotPayload {
                    flush_lsn,
                    data_files: self
                        .unpersisted_iceberg_records
                        .unpersisted_data_files
                        .to_vec(),
                    new_deletion_vector: aggregated_committed_deletion_logs,
                    file_indices_to_import: self
                        .unpersisted_iceberg_records
                        .unpersisted_file_indices
                        .to_vec(),
                    file_indices_to_remove: vec![],
                });
            }
        }

        (
            self.current_snapshot.snapshot_version,
            iceberg_snapshot_payload,
        )
    }

    fn merge_mem_indices(&mut self, task: &mut SnapshotTask) {
        for idx in take(&mut task.new_mem_indices) {
            self.current_snapshot.indices.insert_memory_index(idx);
        }
    }

    fn finalize_batches(&mut self, task: &mut SnapshotTask) {
        if task.new_record_batches.is_empty() {
            return;
        }

        let incoming = take(&mut task.new_record_batches);
        // close previously‐open batch
        assert!(self.batches.values().last().unwrap().data.is_none());
        self.batches.last_entry().unwrap().get_mut().data = Some(incoming[0].1.clone());

        // start a fresh empty batch after the newest data
        let batch_size = self.current_snapshot.metadata.config.batch_size;
        let next_id = incoming.last().unwrap().0 + 1;
        self.batches.insert(next_id, InMemoryBatch::new(batch_size));

        // add completed batches
        self.batches
            .extend(incoming.into_iter().skip(1).map(|(id, rb)| {
                (
                    id,
                    InMemoryBatch {
                        data: Some(rb.clone()),
                        deletions: BatchDeletionVector::new(rb.num_rows()),
                    },
                )
            }));
    }

    fn integrate_disk_slices(&mut self, task: &mut SnapshotTask) {
        for mut slice in take(&mut task.new_disk_slices) {
            // register new files
            self.current_snapshot
                .disk_files
                .extend(slice.output_files().iter().map(|(f, rows)| {
                    (
                        f.clone(),
                        DiskFileDeletionVector {
                            batch_deletion_vector: BatchDeletionVector::new(*rows),
                            puffin_deletion_blob: None,
                        },
                    )
                }));
            let write_lsn = slice.lsn();
            let lsn = write_lsn.expect("commited datafile should have a valid LSN");
            for (f, _) in slice.output_files().iter() {
                task.disk_file_lsn_map.insert(f.file_id(), lsn);
            }
            // remap deletions written *after* this slice’s LSN
            let cut = self.committed_deletion_log.partition_point(|d| {
                d.lsn
                    <= write_lsn.expect(
                        "Critical: LSN is None after it should have been updated by commit process",
                    )
            });

            self.committed_deletion_log[cut..]
                .iter_mut()
                .for_each(|d| slice.remap_deletion_if_needed(d));

            self.uncommitted_deletion_log
                .iter_mut()
                .flatten()
                .for_each(|d| slice.remap_deletion_if_needed(d));

            // swap indices and drop in-memory batches that were flushed
            if let Some(on_disk_index) = slice.take_index() {
                self.current_snapshot
                    .indices
                    .insert_file_index(on_disk_index);
            }
            self.current_snapshot
                .indices
                .delete_memory_index(slice.old_index());

            slice.input_batches().iter().for_each(|b| {
                self.batches.remove(&b.id);
            });
        }
    }

    async fn process_delete_record(
        &mut self,
        deletion: RawDeletionRecord,
        file_id_to_lsn: &HashMap<FileId, u64>,
    ) -> ProcessedDeletionRecord {
        // Fast-path: The row we are deleting was in the mem slice so we already have the position
        if let Some(pos) = deletion.pos {
            return Self::build_processed_deletion(deletion, pos.into());
        }

        // Locate all candidate positions for this record that have **not** yet been deleted.
        let mut candidates: Vec<RecordLocation> = self
            .current_snapshot
            .indices
            .find_record(&deletion)
            .await
            .into_iter()
            .filter(|loc| {
                !self.is_deleted(loc) && self.is_visible(loc, file_id_to_lsn, deletion.lsn)
            })
            .collect();

        match candidates.len() {
            0 => panic!("can't find deletion record {:?}", deletion),
            1 => Self::build_processed_deletion(deletion, candidates.pop().unwrap()),
            _ => {
                // Multiple candidates → disambiguate via full row identity comparison.
                let identity = deletion
                    .row_identity
                    .as_ref()
                    .expect("row_identity required when multiple matches");

                let mut target_position: Option<RecordLocation> = None;
                for loc in candidates.into_iter() {
                    let matches = self.matches_identity(&loc, identity).await;
                    if matches {
                        target_position = Some(loc);
                        break;
                    }
                }
                Self::build_processed_deletion(deletion, target_position.unwrap())
            }
        }
    }

    #[inline]
    fn build_processed_deletion(
        deletion: RawDeletionRecord,
        pos: RecordLocation,
    ) -> ProcessedDeletionRecord {
        ProcessedDeletionRecord {
            pos,
            lsn: deletion.lsn,
        }
    }

    /// Returns `true` if the location has already been marked deleted.
    fn is_deleted(&mut self, loc: &RecordLocation) -> bool {
        match loc {
            RecordLocation::MemoryBatch(batch_id, row_id) => self
                .batches
                .get_mut(batch_id)
                .expect("missing batch")
                .deletions
                .is_deleted(*row_id),

            RecordLocation::DiskFile(file_id, row_id) => self
                .current_snapshot
                .disk_files
                .get_mut(file_id)
                .expect("missing disk file")
                .batch_deletion_vector
                .is_deleted(*row_id),
        }
    }

    fn is_visible(
        &self,
        loc: &RecordLocation,
        file_id_to_lsn: &HashMap<FileId, u64>,
        lsn: u64,
    ) -> bool {
        match loc {
            RecordLocation::MemoryBatch(_, _) => true,
            RecordLocation::DiskFile(file_id, _) => {
                file_id_to_lsn.get(file_id).is_none() || file_id_to_lsn.get(file_id).unwrap() < &lsn
            }
        }
    }

    /// Verifies that `loc` matches the provided `identity`.
    async fn matches_identity(&self, loc: &RecordLocation, identity: &MoonlinkRow) -> bool {
        match loc {
            RecordLocation::MemoryBatch(batch_id, row_id) => {
                let batch = self.batches.get(batch_id).expect("missing batch");
                identity.equals_record_batch_at_offset(
                    batch.data.as_ref().expect("batch missing data"),
                    *row_id,
                    &self.current_snapshot.metadata.identity,
                )
            }
            RecordLocation::DiskFile(file_id, row_id) => {
                let (file, _) = self
                    .current_snapshot
                    .disk_files
                    .get_key_value(file_id)
                    .expect("missing disk file");
                identity
                    .equals_parquet_at_offset(
                        file.file_path(),
                        *row_id,
                        &self.current_snapshot.metadata.identity,
                    )
                    .await
            }
        }
    }

    /// Commit a row deletion record.
    fn commit_deletion(&mut self, deletion: ProcessedDeletionRecord) {
        match &deletion.pos {
            RecordLocation::MemoryBatch(batch_id, row_id) => {
                if self.batches.contains_key(batch_id) {
                    // Possible we deleted an in memory row that was flushed
                    let res = self
                        .batches
                        .get_mut(batch_id)
                        .unwrap()
                        .deletions
                        .delete_row(*row_id);
                    assert!(res);
                }
            }
            RecordLocation::DiskFile(file_name, row_id) => {
                let res = self
                    .current_snapshot
                    .disk_files
                    .get_mut(file_name)
                    .unwrap()
                    .batch_deletion_vector
                    .delete_row(*row_id);
                assert!(res);
            }
        }
        self.committed_deletion_log.push(deletion);
    }

    async fn process_deletion_log(&mut self, task: &mut SnapshotTask) {
        self.advance_pending_deletions(task);
        self.apply_new_deletions(task).await;
    }

    /// Update, commit, or re-queue previously seen deletions.
    fn advance_pending_deletions(&mut self, task: &SnapshotTask) {
        let mut still_uncommitted = Vec::new();

        for mut entry in take(&mut self.uncommitted_deletion_log) {
            let deletion = entry.take().unwrap();
            if deletion.lsn <= task.new_commit_lsn {
                self.commit_deletion(deletion);
            } else {
                still_uncommitted.push(Some(deletion));
            }
        }

        self.uncommitted_deletion_log = still_uncommitted;
    }

    /// Convert raw deletions discovered by the snapshot task and either commit
    /// them or defer until their LSN becomes visible.
    async fn apply_new_deletions(&mut self, task: &mut SnapshotTask) {
        for raw in take(&mut task.new_deletions) {
            let processed = self
                .process_delete_record(raw, &task.disk_file_lsn_map)
                .await;
            if processed.lsn <= task.new_commit_lsn {
                self.commit_deletion(processed);
            } else {
                self.uncommitted_deletion_log.push(Some(processed));
            }
        }
    }

    /// Get committed deletion record for current snapshot.
    fn get_deletion_records(
        &self,
    ) -> (
        Vec<String>,                   /*puffin filepaths*/
        Vec<PuffinDeletionBlobAtRead>, /*deletion vector puffin*/
        Vec<(
            u32, /*index of disk file in snapshot*/
            u32, /*row id*/
        )>,
    ) {
        // Get puffin blobs for deletion vector.
        let mut puffin_filepaths = vec![];
        let mut deletion_vector_blob_at_read = vec![];
        for (idx, (_, disk_deletion_vector)) in self.current_snapshot.disk_files.iter().enumerate()
        {
            if disk_deletion_vector.puffin_deletion_blob.is_none() {
                continue;
            }
            let puffin_deletion_blob = disk_deletion_vector.puffin_deletion_blob.as_ref().unwrap();
            puffin_filepaths.push(puffin_deletion_blob.puffin_filepath.clone());
            let puffin_file_index = puffin_filepaths.len() - 1;
            deletion_vector_blob_at_read.push(PuffinDeletionBlobAtRead {
                data_file_index: idx as u32,
                puffin_file_index: puffin_file_index as u32,
                start_offset: puffin_deletion_blob.start_offset,
                blob_size: puffin_deletion_blob.blob_size,
            });
        }

        // Get committed but un-persisted deletion vector.
        let mut ret = Vec::new();
        for deletion in self.committed_deletion_log.iter() {
            if let RecordLocation::DiskFile(file_id, row_id) = &deletion.pos {
                for (id, (file, _)) in self.current_snapshot.disk_files.iter().enumerate() {
                    if file.file_id() == *file_id {
                        ret.push((id as u32, *row_id as u32));
                        break;
                    }
                }
            }
        }
        (puffin_filepaths, deletion_vector_blob_at_read, ret)
    }

    pub(crate) async fn request_read(&self) -> Result<ReadOutput> {
        let mut data_file_paths = Vec::with_capacity(self.current_snapshot.disk_files.len());
        let mut associated_files = Vec::new();
        let (puffin_file_paths, deletion_vectors_at_read, position_deletes) =
            self.get_deletion_records();
        data_file_paths.extend(
            self.current_snapshot
                .disk_files
                .keys()
                .map(|path| path.file_path().clone()),
        );

        // For committed but not persisted records, we create a temporary file for them, which gets deleted after query completion.
        let file_path = self.current_snapshot.get_name_for_inmemory_file();
        let filepath_exists = tokio::fs::try_exists(&file_path).await?;
        if filepath_exists {
            data_file_paths.push(file_path.to_string_lossy().to_string());
            associated_files.push(file_path.to_string_lossy().to_string());
            return Ok(ReadOutput {
                data_file_paths,
                puffin_file_paths,
                deletion_vectors: deletion_vectors_at_read,
                position_deletes,
                associated_files,
            });
        }

        assert!(matches!(
            self.last_commit,
            RecordLocation::MemoryBatch(_, _)
        ));
        let (batch_id, row_id) = self.last_commit.clone().into();
        if batch_id > 0 || row_id > 0 {
            // add all batches
            let mut filtered_batches = Vec::new();
            let schema = self.current_snapshot.metadata.schema.clone();
            for (id, batch) in self.batches.iter() {
                if *id < batch_id {
                    if let Some(filtered_batch) = batch.get_filtered_batch()? {
                        filtered_batches.push(filtered_batch);
                    }
                } else if *id == batch_id && row_id > 0 {
                    if batch.data.is_some() {
                        if let Some(filtered_batch) = batch.get_filtered_batch_with_limit(row_id)? {
                            filtered_batches.push(filtered_batch);
                        }
                    } else {
                        let rows = self.rows.as_ref().unwrap().get_buffer(row_id);
                        let deletions = &self
                            .batches
                            .values()
                            .last()
                            .expect("batch not found")
                            .deletions;
                        let batch = create_batch_from_rows(rows, schema.clone(), deletions);
                        filtered_batches.push(batch);
                    }
                }
            }

            if !filtered_batches.is_empty() {
                // Build a parquet file from current record batches
                let temp_file = tokio::fs::File::create(&file_path).await?;
                let props = WriterProperties::builder()
                    .set_compression(Compression::UNCOMPRESSED)
                    .set_dictionary_enabled(false)
                    .set_encoding(Encoding::PLAIN)
                    .build();
                let mut parquet_writer = AsyncArrowWriter::try_new(temp_file, schema, Some(props))?;
                for batch in filtered_batches.iter() {
                    parquet_writer.write(batch).await?;
                }
                parquet_writer.close().await?;
                data_file_paths.push(file_path.to_string_lossy().to_string());
                associated_files.push(file_path.to_string_lossy().to_string());
            }
        }
        Ok(ReadOutput {
            data_file_paths,
            puffin_file_paths,
            deletion_vectors: deletion_vectors_at_read,
            position_deletes,
            associated_files,
        })
    }
}
