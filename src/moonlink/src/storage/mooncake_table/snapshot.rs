use super::data_batches::{create_batch_from_rows, InMemoryBatch};
use super::delete_vector::BatchDeletionVector;
use super::{Snapshot, SnapshotTask, TableMetadata};
use crate::error::Result;
use crate::storage::iceberg::iceberg_table_manager::{
    IcebergOperation, IcebergTableManager, IcebergTableManagerConfig,
};
use crate::storage::index::Index;
use crate::storage::mooncake_table::shared_array::SharedRowBufferSnapshot;
use crate::storage::mooncake_table::MoonlinkRow;
use crate::storage::storage_utils::RawDeletionRecord;
use crate::storage::storage_utils::{ProcessedDeletionRecord, RecordLocation};
use parquet::arrow::ArrowWriter;
use std::collections::BTreeMap;
use std::mem::take;
use std::sync::Arc;
pub(crate) struct SnapshotTableState {
    /// Current snapshot
    current_snapshot: Snapshot,

    /// In memory RecordBatches, maps from batch to in-memory batch.
    batches: BTreeMap<u64, InMemoryBatch>,

    /// Latest rows
    rows: Option<SharedRowBufferSnapshot>,

    // UNDONE(BATCH_INSERT):
    // Track uncommitted disk files/ batches from big batch insert

    // Track a log of position deletions on disk_files,
    // since last iceberg snapshot
    committed_deletion_log: Vec<ProcessedDeletionRecord>,
    uncommitted_deletion_log: Vec<Option<ProcessedDeletionRecord>>,

    /// Last commit point
    last_commit: RecordLocation,

    /// Iceberg table manager, used to sync snapshot to the corresponding iceberg table.
    ///
    /// TODO(hjiang): Figure out a way to store dynamic trait for mock-based unit test.
    iceberg_table_manager: Option<IcebergTableManager>,
}

pub struct ReadOutput {
    pub file_paths: Vec<String>,
    pub deletions: Vec<(u32, u32)>,
    pub associated_files: Vec<String>,
}

impl SnapshotTableState {
    pub(super) fn new(
        metadata: Arc<TableMetadata>,
        iceberg_table_config: Option<IcebergTableManagerConfig>,
    ) -> Self {
        let mut batches = BTreeMap::new();
        batches.insert(0, InMemoryBatch::new(metadata.config.batch_size));

        let mut iceberg_table_manager = None;
        if iceberg_table_config.is_some() {
            iceberg_table_manager = Some(IcebergTableManager::new(iceberg_table_config.unwrap()));
        }

        Self {
            current_snapshot: Snapshot::new(metadata),
            batches,
            rows: None,
            last_commit: RecordLocation::MemoryBatch(0, 0),
            committed_deletion_log: Vec::new(),
            uncommitted_deletion_log: Vec::new(),
            iceberg_table_manager,
        }
    }

    pub(super) async fn update_snapshot(&mut self, mut task: SnapshotTask) -> u64 {
        self.merge_mem_indices(&mut task);
        self.finalize_batches(&mut task);
        self.integrate_disk_slices(&mut task);

        self.rows = take(&mut task.new_rows);
        Self::process_deletion_log(self, &mut task);

        if task.new_lsn != 0 {
            self.current_snapshot.snapshot_version = task.new_lsn;
        }
        if let Some(cp) = task.new_commit_point {
            self.last_commit = cp;
        }

        // Sync the latest change to iceberg.
        // TODO(hjiang): Error handling for snapshot sync-up.
        if self.iceberg_table_manager.is_some() {
            self.iceberg_table_manager
                .as_mut()
                .unwrap()
                .sync_snapshot(self.current_snapshot.disk_files.clone())
                .await
                .unwrap();
        }

        self.current_snapshot.snapshot_version
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
            self.current_snapshot.disk_files.extend(
                slice
                    .output_files()
                    .iter()
                    .map(|(f, rows)| (f.clone(), BatchDeletionVector::new(*rows))),
            );

            // remap deletions written *after* this slice’s LSN
            let write_lsn = slice.lsn();
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
            self.current_snapshot
                .indices
                .insert_file_index(slice.take_index().unwrap());
            self.current_snapshot
                .indices
                .delete_memory_index(slice.old_index());

            slice.input_batches().iter().for_each(|b| {
                self.batches.remove(&b.id);
            });
        }
    }

    fn process_delete_record(&mut self, deletion: RawDeletionRecord) -> ProcessedDeletionRecord {
        // Fast-path: The row we are deleting was in the mem slice so we already have the position
        if let Some(pos) = deletion.pos {
            return Self::build_processed_deletion(deletion, pos.into());
        }

        // Locate all candidate positions for this record that have **not** yet been deleted.
        let mut candidates: Vec<RecordLocation> = self
            .current_snapshot
            .indices
            .find_record(&deletion)
            .into_iter()
            .filter(|loc| !self.is_deleted(loc))
            .collect();

        match candidates.len() {
            0 => panic!("can't find deletion record"),
            1 => Self::build_processed_deletion(deletion, candidates.pop().unwrap()),
            _ => {
                // Multiple candidates → disambiguate via full row identity comparison.
                let identity = deletion
                    .row_identity
                    .as_ref()
                    .expect("row_identity required when multiple matches");

                let pos = candidates
                    .into_iter()
                    .find(|loc| self.matches_identity(loc, identity))
                    .expect("can't find valid record to delete");

                Self::build_processed_deletion(deletion, pos)
            }
        }
    }

    #[inline]
    fn build_processed_deletion(
        deletion: RawDeletionRecord,
        pos: RecordLocation,
    ) -> ProcessedDeletionRecord {
        ProcessedDeletionRecord {
            _lookup_key: deletion.lookup_key,
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

            RecordLocation::DiskFile(file_name, row_id) => self
                .current_snapshot
                .disk_files
                .get_mut(file_name.0.as_ref())
                .expect("missing disk file")
                .is_deleted(*row_id),
        }
    }

    /// Verifies that `loc` matches the provided `identity`.
    fn matches_identity(&self, loc: &RecordLocation, identity: &MoonlinkRow) -> bool {
        match loc {
            RecordLocation::MemoryBatch(batch_id, row_id) => {
                let batch = self.batches.get(batch_id).expect("missing batch");
                identity.equals_record_batch_at_offset(
                    batch.data.as_ref().expect("batch missing data"),
                    *row_id,
                    &self.current_snapshot.metadata.identity,
                )
            }
            RecordLocation::DiskFile(file_name, row_id) => {
                let name = file_name.0.to_string_lossy();
                identity.equals_parquet_at_offset(
                    &name,
                    *row_id,
                    &self.current_snapshot.metadata.identity,
                )
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
                    .get_mut(file_name.0.as_ref())
                    .unwrap()
                    .delete_row(*row_id);
                assert!(res);
            }
        }
        self.committed_deletion_log.push(deletion);
    }

    fn process_deletion_log(&mut self, task: &mut SnapshotTask) {
        self.advance_pending_deletions(task);
        self.apply_new_deletions(task);
    }

    /// Update, commit, or re-queue previously seen deletions.
    fn advance_pending_deletions(&mut self, task: &SnapshotTask) {
        let mut still_uncommitted = Vec::new();

        for mut entry in take(&mut self.uncommitted_deletion_log) {
            let deletion = entry.take().unwrap();

            if deletion.lsn <= task.new_lsn {
                Self::commit_deletion(self, deletion);
            } else {
                still_uncommitted.push(Some(deletion));
            }
        }

        self.uncommitted_deletion_log = still_uncommitted;
    }

    /// Convert raw deletions discovered by the snapshot task and either commit
    /// them or defer until their LSN becomes visible.
    fn apply_new_deletions(&mut self, task: &mut SnapshotTask) {
        for raw in take(&mut task.new_deletions) {
            let processed = Self::process_delete_record(self, raw);
            if processed.lsn <= task.new_lsn {
                Self::commit_deletion(self, processed);
            } else {
                self.uncommitted_deletion_log.push(Some(processed));
            }
        }
    }

    /// Get committed deletion record for current snapshot.
    fn get_deletion_records(
        &self,
    ) -> Vec<(
        u32, /*index of disk file in snapshot*/
        u32, /*row id*/
    )> {
        let mut ret = Vec::new();
        for deletion in self.committed_deletion_log.iter() {
            if let RecordLocation::DiskFile(file_name, row_id) = &deletion.pos {
                for (id, (file, _)) in self.current_snapshot.disk_files.iter().enumerate() {
                    if *file == *file_name.0 {
                        ret.push((id as u32, *row_id as u32));
                        break;
                    }
                }
            }
        }
        ret
    }

    pub(crate) fn request_read(&self) -> Result<ReadOutput> {
        let mut file_paths: Vec<String> = Vec::new();
        let mut associated_files = Vec::new();
        let deletions = self.get_deletion_records();
        file_paths.extend(
            self.current_snapshot
                .disk_files
                .keys()
                .map(|path| path.to_string_lossy().to_string()),
        );
        let file_path = self.current_snapshot.get_name_for_inmemory_file();
        if file_path.exists() {
            file_paths.push(file_path.to_string_lossy().to_string());
            associated_files.push(file_path.to_string_lossy().to_string());
            return Ok(ReadOutput {
                file_paths,
                deletions,
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
                //
                let mut parquet_writer =
                    ArrowWriter::try_new(std::fs::File::create(&file_path).unwrap(), schema, None)?;
                for batch in filtered_batches.iter() {
                    parquet_writer.write(batch)?;
                }
                parquet_writer.close()?;
                file_paths.push(file_path.to_string_lossy().to_string());
                associated_files.push(file_path.to_string_lossy().to_string());
            }
        }
        Ok(ReadOutput {
            file_paths,
            deletions,
            associated_files,
        })
    }
}
