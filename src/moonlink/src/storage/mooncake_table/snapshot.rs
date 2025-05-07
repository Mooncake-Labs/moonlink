use super::data_batches::{create_batch_from_rows, InMemoryBatch};
use super::delete_vector::BatchDeletionVector;
use super::{Snapshot, SnapshotTask, TableMetadata};
use crate::error::Result;
use crate::storage::index::Index;
use crate::storage::mooncake_table::shared_array::SharedRowBufferSnapshot;
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
}

pub struct ReadOutput {
    pub file_paths: Vec<String>,
    pub deletions: Vec<(u32, u32)>,
    pub associated_files: Vec<String>,
}

impl SnapshotTableState {
    pub(super) fn new(metadata: Arc<TableMetadata>) -> Self {
        let mut batches = BTreeMap::new();
        batches.insert(0, InMemoryBatch::new(metadata.config.batch_size));
        Self {
            current_snapshot: Snapshot::new(metadata),
            batches,
            rows: None,
            last_commit: RecordLocation::MemoryBatch(0, 0),
            committed_deletion_log: Vec::new(),
            uncommitted_deletion_log: Vec::new(),
        }
    }

    /// Return current snapshot's version.
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
            // assert!(self.batches.keys().last().unwrap() == &new_batches.first().unwrap().0);
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
        self.rows = take(&mut next_snapshot_task.new_rows);
        self.process_deletion_log(&mut next_snapshot_task);
        if next_snapshot_task.new_lsn != 0 {
            self.current_snapshot.snapshot_version = next_snapshot_task.new_lsn;
        }
        if next_snapshot_task.new_commit_point.is_some() {
            self.last_commit = next_snapshot_task.new_commit_point.unwrap();
        }
        // TODO(hjiang): now all committed changes are sync-ed to current snapshot, sync the snapshot to iceberg table as well.
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
            let mut locations_not_deleted = Vec::new();
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
                            locations_not_deleted.push(location);
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
                            locations_not_deleted.push(location);
                        }
                    }
                }
            }
            if locations_not_deleted.is_empty() {
                panic!("can't find deletion record");
            } else if locations_not_deleted.len() > 1 {
                assert!(deletion.row_identity.is_some());
                for location in locations_not_deleted.into_iter() {
                    match &location {
                        RecordLocation::MemoryBatch(batch_id, row_id) => {
                            let batch = self.batches.get_mut(batch_id).unwrap();
                            if deletion
                                .row_identity
                                .as_ref()
                                .unwrap()
                                .equals_record_batch_at_offset(
                                    batch.data.as_ref().unwrap(),
                                    *row_id,
                                    &self.current_snapshot.metadata.identity,
                                )
                            {
                                return ProcessedDeletionRecord {
                                    _lookup_key: deletion.lookup_key,
                                    pos: location,
                                    lsn: deletion.lsn,
                                    xact_id: deletion.xact_id,
                                };
                            }
                        }
                        RecordLocation::DiskFile(file_name, row_id) => {
                            let name = file_name.0.to_string_lossy().to_string();
                            if deletion
                                .row_identity
                                .as_ref()
                                .unwrap()
                                .equals_parquet_at_offset(
                                    &name,
                                    *row_id,
                                    &self.current_snapshot.metadata.identity,
                                )
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
                panic!("can't find valid record to delete");
            } else {
                ProcessedDeletionRecord {
                    _lookup_key: deletion.lookup_key,
                    pos: locations_not_deleted.first().unwrap().clone(),
                    lsn: deletion.lsn,
                    xact_id: deletion.xact_id,
                }
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

    /// Commit a few uncommitted valid deletion logs, with lsn <= new_lsn.
    fn process_deletion_log(&mut self, next_snapshot_task: &mut SnapshotTask) {
        let mut deletion_to_commit = vec![];
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
                deletion_to_commit.push(deletion.take().unwrap());
                should_keep = false;
            }

            should_keep
        });
        for deletion in deletion_to_commit.into_iter() {
            self.commit_deletion(deletion);
        }

        // Move committed deletions (lsn <= new_lsn) to committed deletion log
        // add raw deletion records, use index to find position and add to deletion buffer
        let new_deletions = take(&mut next_snapshot_task.new_deletions);
        // apply deletion records to deletion vectors
        for deletion in new_deletions {
            let processed_deletion = self.process_delete_record(deletion);
            if processed_deletion.lsn <= next_snapshot_task.new_lsn {
                self.commit_deletion(processed_deletion);
            } else {
                self.uncommitted_deletion_log.push(Some(processed_deletion));
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
