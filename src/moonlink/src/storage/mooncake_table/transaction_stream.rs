use super::*;
use crate::storage::index::Index;
use crate::storage::storage_utils::ProcessedDeletionRecord;
/// Used to track the state of a streamed transaction
/// Holds appending rows in memslice and files.
/// Deletes are more complex,
/// 1. row belong to stream state memslice, directly delete it.
/// 2. row belong to stream state flushed file, add to `local_deletions`
/// 3. row belong to main table's flushed files, directly pushed to snapshot_task.new_deletions and let snapshot handle it.
/// 4. row belong to main table's memslice, add to `pending_deletions_in_main_mem_slice`, and handle at commit time`
///
pub(super) struct TransactionStreamState {
    mem_slice: MemSlice,
    local_deletions: Vec<ProcessedDeletionRecord>,
    pending_deletions_in_main_mem_slice: Vec<RawDeletionRecord>,
    flushed_file_index: MooncakeIndex,
    flushed_files: hashbrown::HashMap<MooncakeDataFileRef, BatchDeletionVector>,
}

pub enum TransactionStreamOutput {
    Commit(TransactionStreamCommit),
    Abort(u32),
}

pub struct TransactionStreamCommit {
    xact_id: u32,
    commit_lsn: u64,
    flushed_file_index: MooncakeIndex,
    flushed_files: hashbrown::HashMap<MooncakeDataFileRef, BatchDeletionVector>,
    local_deletions: Vec<ProcessedDeletionRecord>,
    pending_deletions: Vec<RawDeletionRecord>,
}

impl TransactionStreamCommit {
    /// Get flushed data files for the current streaming commit.
    pub(crate) fn get_flushed_data_files(&self) -> Vec<MooncakeDataFileRef> {
        self.flushed_files.keys().cloned().collect::<Vec<_>>()
    }
    /// Get flushed file indices for the current streaming commit.
    pub(crate) fn get_file_indices(&self) -> Vec<FileIndex> {
        self.flushed_file_index.file_indices.clone()
    }
}

impl TransactionStreamState {
    fn new(schema: Arc<Schema>, batch_size: usize, identity: IdentityProp) -> Self {
        Self {
            mem_slice: MemSlice::new(schema, batch_size, identity),
            local_deletions: Vec::new(),
            pending_deletions_in_main_mem_slice: Vec::new(),
            flushed_file_index: MooncakeIndex::new(),
            flushed_files: hashbrown::HashMap::new(),
        }
    }
}

// DevNote:
// This is a trick to track xact of uncommitted deletions
// we set first 32 bits to 1, so it will be 'uncommitted' as the value is larger than any possible lsn.
// And we use the last 32 bits to store the xact_id, so we can find deletion for a given xact_id.
fn get_lsn_for_pending_xact(xact_id: u32) -> u64 {
    0xFFFF_FFFF_0000_0000 | xact_id as u64
}

impl MooncakeTable {
    fn get_or_create_stream_state<'a>(
        transaction_stream_states: &'a mut HashMap<u32, TransactionStreamState>,
        metadata: &Arc<TableMetadata>,
        xact_id: u32,
    ) -> &'a mut TransactionStreamState {
        transaction_stream_states.entry(xact_id).or_insert_with(|| {
            TransactionStreamState::new(
                metadata.schema.clone(),
                metadata.config.batch_size,
                metadata.identity.clone(),
            )
        })
    }

    pub fn should_transaction_flush(&self, xact_id: u32) -> bool {
        self.transaction_stream_states
            .get(&xact_id)
            .unwrap()
            .mem_slice
            .get_num_rows()
            >= self.metadata.config.mem_slice_size
    }

    pub fn append_in_stream_batch(&mut self, row: MoonlinkRow, xact_id: u32) -> Result<()> {
        let stream_state = Self::get_or_create_stream_state(
            &mut self.transaction_stream_states,
            &self.metadata,
            xact_id,
        );

        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let identity_for_key = self.metadata.identity.extract_identity_for_key(&row);
        stream_state
            .mem_slice
            .append(lookup_key, row, identity_for_key)?;

        Ok(())
    }

    pub async fn delete_in_stream_batch(&mut self, row: MoonlinkRow, xact_id: u32) {
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let mut record = RawDeletionRecord {
            lookup_key,
            lsn: get_lsn_for_pending_xact(xact_id), // at commit time we will update this with the actual lsn
            pos: None,
            row_identity: self.metadata.identity.extract_identity_columns(row),
        };
        let stream_state = Self::get_or_create_stream_state(
            &mut self.transaction_stream_states,
            &self.metadata,
            xact_id,
        );

        // Delete from stream mem slice
        if stream_state
            .mem_slice
            .delete(&record, &self.metadata.identity)
            .await
            .is_some()
        {
            return;
        }
        // Delete from stream flushed files
        let matches = stream_state.flushed_file_index.find_record(&record).await;
        if !matches.is_empty() {
            for loc in matches {
                let RecordLocation::DiskFile(file_id, row_id) = loc else {
                    panic!("Unexpected record location: {:?}", record);
                };
                let (file, dv) = stream_state
                    .flushed_files
                    .get_key_value_mut(&file_id)
                    .expect("missing disk file");
                if dv.is_deleted(row_id) {
                    continue;
                }
                if record.row_identity.is_none()
                    || record
                        .row_identity
                        .as_ref()
                        .unwrap()
                        .equals_parquet_at_offset(file.file_path(), row_id, &self.metadata.identity)
                        .await
                {
                    stream_state.local_deletions.push(ProcessedDeletionRecord {
                        pos: loc,
                        lsn: record.lsn,
                    });
                    dv.delete_row(row_id);
                    return;
                }
            }
        }

        // Delete from main table
        record.pos = self
            .mem_slice
            .find_non_deleted_position(&record, &self.metadata.identity)
            .await;
        if record.pos.is_some() {
            stream_state
                .pending_deletions_in_main_mem_slice
                .push(record);
        } else {
            self.next_snapshot_task.new_deletions.push(record);
        }
    }

    pub fn abort_in_stream_batch(&mut self, xact_id: u32) {
        // Record abortion in snapshot task so we can remove any uncomitted deletions
        self.transaction_stream_states.remove(&xact_id);
        self.next_snapshot_task
            .new_streaming_xact
            .push(TransactionStreamOutput::Abort(xact_id));
    }

    pub async fn flush_transaction_stream(&mut self, xact_id: u32) -> Result<()> {
        if let Some(stream_state) = self.transaction_stream_states.get_mut(&xact_id) {
            let next_file_id = self.next_file_id;
            self.next_file_id += 1;
            let mut disk_slice = Self::flush_mem_slice(
                &mut stream_state.mem_slice,
                &self.metadata,
                next_file_id,
                None,
                None,
            )
            .await?;

            for (file, num_rows) in disk_slice.output_files().iter() {
                stream_state
                    .flushed_files
                    .insert(file.clone(), BatchDeletionVector::new(*num_rows));
            }
            let index = disk_slice.take_index();
            if let Some(index) = index {
                stream_state.flushed_file_index.insert_file_index(index);
            }
            return Ok(());
        }
        Ok(())
    }

    pub async fn commit_transaction_stream(&mut self, xact_id: u32, lsn: u64) -> Result<()> {
        self.flush_transaction_stream(xact_id).await?;
        if let Some(mut stream_state) = self.transaction_stream_states.remove(&xact_id) {
            let snapshot_task = &mut self.next_snapshot_task;
            snapshot_task.new_commit_lsn = lsn;

            // We update our delete records with the last lsn of the transaction
            // Note that in the stream case we dont have this until commit time
            for deletion in stream_state.pending_deletions_in_main_mem_slice.iter_mut() {
                let pos = deletion.pos.unwrap();
                // If the row is no longer in memslice, it must be flushed, let snapshot task find it.
                if !self.mem_slice.try_delete_at_pos(pos) {
                    deletion.pos = None;
                }
            }

            for deletion in stream_state.local_deletions.iter_mut() {
                deletion.lsn = lsn;
            }

            let commit = TransactionStreamCommit {
                xact_id,
                commit_lsn: lsn,
                flushed_file_index: stream_state.flushed_file_index,
                flushed_files: stream_state.flushed_files,
                local_deletions: stream_state.local_deletions,
                pending_deletions: stream_state.pending_deletions_in_main_mem_slice,
            };
            snapshot_task
                .new_streaming_xact
                .push(TransactionStreamOutput::Commit(commit));
            snapshot_task.new_flush_lsn = Some(lsn);
            Ok(())
        } else {
            Err(Error::TransactionNotFound(xact_id))
        }
    }
}

impl SnapshotTableState {
    pub(super) fn apply_transaction_stream(&mut self, task: &mut SnapshotTask) {
        let new_streaming_xact = task.new_streaming_xact.drain(..);
        for output in new_streaming_xact {
            match output {
                TransactionStreamOutput::Commit(commit) => {
                    // add files
                    commit.flushed_files.into_iter().for_each(|(file, dv)| {
                        task.disk_file_lsn_map
                            .insert(file.file_id(), commit.commit_lsn);
                        self.current_snapshot.disk_files.insert(
                            file,
                            DiskFileDeletionVector {
                                batch_deletion_vector: dv,
                                puffin_deletion_blob: None,
                            },
                        );
                    });
                    // add index
                    commit
                        .flushed_file_index
                        .file_indices
                        .into_iter()
                        .for_each(|file_index| {
                            self.current_snapshot.indices.insert_file_index(file_index);
                        });
                    // add local deletions
                    self.committed_deletion_log
                        .extend(commit.local_deletions.into_iter());
                    // add pending deletions
                    task.new_deletions
                        .extend(commit.pending_deletions.into_iter());
                    // set lsn for pending deletions
                    self.uncommitted_deletion_log.iter_mut().for_each(|row| {
                        if let Some(deletion) = row {
                            if deletion.lsn == get_lsn_for_pending_xact(commit.xact_id) {
                                deletion.lsn = commit.commit_lsn;
                            }
                        }
                    });
                    task.new_deletions.iter_mut().for_each(|deletion| {
                        if deletion.lsn == get_lsn_for_pending_xact(commit.xact_id) {
                            deletion.lsn = commit.commit_lsn;
                        }
                    });
                }
                TransactionStreamOutput::Abort(xact_id) => {
                    for row in self.uncommitted_deletion_log.iter_mut() {
                        if let Some(deletion) = row {
                            if deletion.lsn == get_lsn_for_pending_xact(xact_id) {
                                *row = None;
                            }
                        }
                    }
                    task.new_deletions
                        .retain(|deletion| deletion.lsn != get_lsn_for_pending_xact(xact_id));
                }
            }
        }
    }
}
