use super::data_batches::{create_batch_from_rows, InMemoryBatch};
use super::delete_vector::BatchDeletionVector;
use super::{Snapshot, SnapshotTask, TableMetadata};
use crate::error::Result;
use crate::row::MoonlinkRow;
use crate::storage::index::Index;
use crate::storage::storage_utils::RawDeletionRecord;
use crate::storage::storage_utils::{ProcessedDeletionRecord, RecordLocation};
use parquet::arrow::ArrowWriter;
use std::collections::BTreeMap;
use std::mem::take;
use std::path::PathBuf;
use std::sync::Arc;
pub(crate) struct SnapshotTableState {
    /// Current snapshot
    current_snapshot: Snapshot,

    /// In memory RecordBatches
    batches: BTreeMap<u64, InMemoryBatch>,

    /// Latest rows
    rows: Vec<MoonlinkRow>,

    // UNDONE(BATCH_INSERT):
    // Track uncommited disk files/ batches from big batch insert

    // Track a log of position deletions on disk_files,
    // since last iceberg snapshot
    committed_deletion_log: Vec<ProcessedDeletionRecord>,
    uncommitted_deletion_log: Vec<Option<ProcessedDeletionRecord>>,

    /// Last commit point
    last_commit: RecordLocation,
}

impl SnapshotTableState {
    pub(super) fn new(metadata: Arc<TableMetadata>) -> Self {
        let mut batches = BTreeMap::new();
        batches.insert(0, InMemoryBatch::new(metadata.config.batch_size));
        Self {
            current_snapshot: Snapshot::new(metadata),
            batches,
            rows: Vec::new(),
            last_commit: RecordLocation::MemoryBatch(0, 0),
            committed_deletion_log: Vec::new(),
            uncommitted_deletion_log: Vec::new(),
        }
    }

    pub(super) fn update_snapshot(&mut self, mut next_snapshot_task: SnapshotTask) -> u64 {
        let batch_size = self.current_snapshot.metadata.config.batch_size;
        if !next_snapshot_task.new_mem_indices.is_empty() {
            let new_mem_indices = take(&mut next_snapshot_task.new_mem_indices);
            for mem_index in new_mem_indices {
                self.current_snapshot.indices.insert_memory_index(mem_index);
            }
        }
        if !next_snapshot_task.new_record_batches.is_empty() {
            let new_batches = take(&mut next_snapshot_task.new_record_batches);
            // previous unfinished batch is finished
            assert!(self.batches.values().last().unwrap().data.is_none());
            assert!(self.batches.keys().last().unwrap() == &new_batches.first().unwrap().0);
            self.batches.last_entry().unwrap().get_mut().data =
                Some(new_batches.first().unwrap().1.clone());
            // insert the last unfinished batch
            let last_batch_id = new_batches.last().unwrap().0 + 1;
            self.batches
                .insert(last_batch_id, InMemoryBatch::new(batch_size));
            // copy the rest of the batches
            self.batches
                .extend(new_batches.iter().skip(1).map(|(id, batch)| {
                    (
                        *id,
                        InMemoryBatch {
                            data: Some(batch.clone()),
                            deletions: BatchDeletionVector::new(batch.num_rows()),
                        },
                    )
                }));
            self.rows.clear();
        }
        if !next_snapshot_task.new_disk_slices.is_empty() {
            let mut new_disk_slices = take(&mut next_snapshot_task.new_disk_slices);
            for slice in new_disk_slices.iter_mut() {
                self.current_snapshot.disk_files.extend(
                    slice.output_files().iter().map(|(file, row_count)| {
                        (file.clone(), BatchDeletionVector::new(*row_count))
                    }),
                );
                let write_lsn = slice.lsn();
                let pos = self
                    .committed_deletion_log
                    .partition_point(|deletion| deletion.lsn <= write_lsn);
                for entry in self.committed_deletion_log.iter_mut().skip(pos) {
                    slice.remap_deletion_if_needed(entry);
                }
                for entry in self.uncommitted_deletion_log.iter_mut().flatten() {
                    slice.remap_deletion_if_needed(entry);
                }
                self.current_snapshot
                    .indices
                    .insert_file_index(slice.take_index().unwrap());
                self.current_snapshot
                    .indices
                    .delete_memory_index(slice.old_index());
                slice.input_batches().iter().for_each(|batch| {
                    self.batches.remove(&batch.id);
                });
            }
        }
        if !next_snapshot_task.new_rows.is_empty() {
            let new_rows = take(&mut next_snapshot_task.new_rows);
            self.rows.extend(new_rows);
        }
        Self::process_deletion_log(self, &mut next_snapshot_task);
        if next_snapshot_task.new_lsn != 0 {
            self.current_snapshot.snapshot_version = next_snapshot_task.new_lsn;
        }
        if next_snapshot_task.new_commit_point.is_some() {
            self.last_commit = next_snapshot_task.new_commit_point.unwrap();
        }
        self.current_snapshot.snapshot_version
    }

    fn process_delete_record(&mut self, deletion: RawDeletionRecord) -> ProcessedDeletionRecord {
        if let Some(pos) = deletion.pos {
            ProcessedDeletionRecord {
                _lookup_key: deletion.lookup_key,
                pos: pos.into(),
                lsn: deletion.lsn,
                xact_id: deletion.xact_id,
            }
        } else {
            let locations = self.current_snapshot.indices.find_record(&deletion);
            for location in locations.unwrap().into_iter() {
                match &location {
                    RecordLocation::MemoryBatch(batch_id, row_id) => {
                        if !self
                            .batches
                            .get_mut(batch_id)
                            .unwrap()
                            .deletions
                            .is_deleted(*row_id)
                        {
                            return ProcessedDeletionRecord {
                                _lookup_key: deletion.lookup_key,
                                pos: location.clone(),
                                lsn: deletion.lsn,
                                xact_id: deletion.xact_id,
                            };
                        }
                    }
                    RecordLocation::DiskFile(file_name, row_id) => {
                        if !self
                            .current_snapshot
                            .disk_files
                            .get_mut(file_name.0.as_ref())
                            .unwrap()
                            .is_deleted(*row_id)
                        {
                            return ProcessedDeletionRecord {
                                _lookup_key: deletion.lookup_key,
                                pos: location,
                                lsn: deletion.lsn,
                                xact_id: deletion.xact_id,
                            };
                        }
                    }
                }
            }
            panic!("can't find deletion record");
        }
    }

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

    fn process_deletion_log(&mut self, next_snapshot_task: &mut SnapshotTask) {
        let mut new_commited_deletion = vec![];
        self.uncommitted_deletion_log.retain_mut(|deletion| {
            let mut should_keep = true;

            // First update LSN if it's from a flushed transaction
            if let Some(xact_id) = deletion.as_ref().unwrap().xact_id {
                if let Some(lsn) = next_snapshot_task.flushed_xacts.get(&xact_id) {
                    deletion.as_mut().unwrap().lsn = *lsn;
                }

                // Check if this is from an aborted transaction
                if next_snapshot_task.aborted_xacts.contains(&xact_id) {
                    should_keep = false;
                }
            }

            // After potentially updating LSN, check if it's now committed
            if should_keep && deletion.as_ref().unwrap().lsn <= next_snapshot_task.new_lsn {
                new_commited_deletion.push(deletion.take().unwrap());
                should_keep = false;
            }

            should_keep
        });
        for deletion in new_commited_deletion {
            Self::commit_deletion(self, deletion);
        }
        // Move committed deletions (lsn <= new_lsn) to committed deletion log
        // add raw deletion records, use index to find position and add to deletion buffer
        let new_deletions = take(&mut next_snapshot_task.new_deletions);
        // apply deletion records to deletion vectors
        for deletion in new_deletions {
            let processed_deletion = Self::process_delete_record(self, deletion);
            if processed_deletion.lsn <= next_snapshot_task.new_lsn {
                Self::commit_deletion(self, processed_deletion);
            } else {
                self.uncommitted_deletion_log.push(Some(processed_deletion));
            }
        }
    }

    fn get_deletion_records(&self) -> Vec<(usize, usize)> {
        let mut ret = Vec::new();
        for deletion in self.committed_deletion_log.iter() {
            if let RecordLocation::DiskFile(file_name, row_id) = &deletion.pos {
                for (id, (file, _)) in self.current_snapshot.disk_files.iter().enumerate() {
                    if *file == *file_name.0 {
                        ret.push((id, *row_id));
                        break;
                    }
                }
            }
        }
        ret
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn request_read(&self) -> Result<(Vec<PathBuf>, Vec<(usize, usize)>)> {
        let mut file_paths: Vec<PathBuf> = Vec::new();
        let deletions = self.get_deletion_records();
        file_paths.extend(self.current_snapshot.disk_files.keys().cloned());
        let file_path = self.current_snapshot.get_name_for_inmemory_file();
        if file_path.exists() {
            file_paths.push(file_path);
            return Ok((file_paths, deletions));
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
                    if let Some(batch) = batch.get_filtered_batch().unwrap() {
                        filtered_batches.push(batch);
                    }
                } else if *id == batch_id && row_id > 0 {
                    if batch.data.is_some() {
                        let filtered_batch = batch
                            .get_filtered_batch_with_limit(row_id)
                            .unwrap()
                            .unwrap();
                        filtered_batches.push(filtered_batch);
                    } else {
                        let rows = &self.rows[..row_id];
                        let deletions = &self.batches.values().last().unwrap().deletions;
                        let batch = create_batch_from_rows(rows, schema.clone(), deletions);
                        filtered_batches.push(batch);
                    }
                }
            }

            if !filtered_batches.is_empty() {
                // Build a parquet file from current record batches
                //
                let mut parquet_writer =
                    ArrowWriter::try_new(std::fs::File::create(&file_path).unwrap(), schema, None)
                        .unwrap();
                for batch in filtered_batches.iter() {
                    parquet_writer.write(batch)?;
                }
                parquet_writer.close()?;
                file_paths.push(file_path);
            }
        }

        Ok((file_paths, deletions))
    }
}
