use super::*;
use crate::storage::cache::object_storage::base_cache::{CacheEntry, CacheTrait, FileMetadata};
use crate::storage::index::cache_utils as index_cache_utils;
use crate::storage::mooncake_table::data_batches::InMemoryBatch;
use crate::storage::mooncake_table::DiskFileEntry;
use crate::storage::storage_utils::{ProcessedDeletionRecord, TableUniqueFileId};
use fastbloom::BloomFilter;
use more_asserts as ma;
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
    index_bloom_filter: BloomFilter,
    /// Both in memory and on disk indices for this transaction.
    stream_indices: MooncakeIndex,
    flushed_files: hashbrown::HashMap<MooncakeDataFileRef, DiskFileEntry>,
    new_record_batches: hashbrown::HashMap<u64, InMemoryBatch>,
    status: TransactionStreamStatus,
    /// Number of pending flushes for this transaction.
    /// Only safe to remove transaction stream state when there are no pending flushes.
    pending_flush_count: u32,
}

/// Determines the state of a transaction stream.
/// Transaction can be safely removed when it is no longer `Pending` and has no pending flushes.
#[derive(PartialEq)]
pub enum TransactionStreamStatus {
    Pending,
    Committed,
    Aborted,
}

pub enum TransactionStreamOutput {
    Commit(TransactionStreamCommit),
    Abort(u32),
}

impl TransactionStreamOutput {
    /// Get committed persisted disk files count.
    pub fn get_committed_persisted_disk_count(&self) -> usize {
        match &self {
            TransactionStreamOutput::Abort(_) => 0,
            TransactionStreamOutput::Commit(commit) => commit.flushed_files.len(),
        }
    }
}

pub struct TransactionStreamCommit {
    xact_id: u32,
    commit_lsn: u64,
    flushed_file_index: MooncakeIndex,
    flushed_files: hashbrown::HashMap<MooncakeDataFileRef, DiskFileEntry>,
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
    /// Import file index into cache.
    /// Return evicted files to delete.
    pub(crate) async fn import_file_index_into_cache(
        &mut self,
        object_storage_cache: ObjectStorageCache,
        table_id: TableId,
    ) -> Vec<String> {
        let file_indices = &mut self.flushed_file_index.file_indices;
        index_cache_utils::import_file_indices_to_cache(
            file_indices,
            object_storage_cache,
            table_id,
        )
        .await
    }
}

impl TransactionStreamState {
    fn new(
        schema: Arc<Schema>,
        batch_size: usize,
        identity: IdentityProp,
        streaming_counter: Arc<BatchIdCounter>,
    ) -> Self {
        Self {
            mem_slice: MemSlice::new(schema, batch_size, identity, streaming_counter),
            local_deletions: Vec::new(),
            pending_deletions_in_main_mem_slice: Vec::new(),
            index_bloom_filter: BloomFilter::with_num_bits(1 << 24).expected_items(1_000_000),
            stream_indices: MooncakeIndex::new(),
            flushed_files: hashbrown::HashMap::new(),
            new_record_batches: hashbrown::HashMap::new(),
            status: TransactionStreamStatus::Pending,
            pending_flush_count: 0,
        }
    }
}

pub(crate) const LSN_START_FOR_STREAMING_XACT: u64 = 0xFFFF_FFFF_0000_0000;
// DevNote:
// This is a trick to track xact of uncommitted deletions
// we set first 32 bits to 1, so it will be 'uncommitted' as the value is larger than any possible lsn.
// And we use the last 32 bits to store the xact_id, so we can find deletion for a given xact_id.
fn get_lsn_for_pending_xact(xact_id: u32) -> u64 {
    LSN_START_FOR_STREAMING_XACT | xact_id as u64
}

impl MooncakeTable {
    fn get_or_create_stream_state(&mut self, xact_id: u32) -> &mut TransactionStreamState {
        let metadata = self.metadata.clone();

        self.transaction_stream_states
            .entry(xact_id)
            .or_insert_with(|| {
                TransactionStreamState::new(
                    metadata.schema.clone(),
                    metadata.config.batch_size,
                    metadata.identity.clone(),
                    Arc::clone(&self.streaming_batch_id_counter),
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
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let identity_for_key = self.metadata.identity.extract_identity_for_key(&row);

        let stream_state = self.get_or_create_stream_state(xact_id);
        stream_state
            .mem_slice
            .append(lookup_key, row, identity_for_key)?;
        stream_state.index_bloom_filter.insert(&lookup_key);
        Ok(())
    }

    pub async fn delete_in_stream_batch(&mut self, row: MoonlinkRow, xact_id: u32) {
        let lookup_key = self.metadata.identity.get_lookup_key(&row);
        let metadata_identity = self.metadata.identity.clone();
        let mut record = RawDeletionRecord {
            lookup_key,
            lsn: get_lsn_for_pending_xact(xact_id), // at commit time we will update this with the actual lsn
            pos: None,
            row_identity: metadata_identity.extract_identity_columns(row),
        };

        let stream_state = self.get_or_create_stream_state(xact_id);

        // it is very unlikely to delete a row in current transaction,
        // only very weird query shape could do it.
        // use a bloom filter to skip any index lookup (which could be costly)
        let bloom_filter_pass = stream_state.index_bloom_filter.contains(&lookup_key);
        // skip any index lookup if bloom filter don't pass
        if bloom_filter_pass {
            // Delete from stream mem slice
            if stream_state
                .mem_slice
                .delete(&record, &metadata_identity)
                .await
                .is_some()
            {
                return;
            }
            // Delete from stream state
            let matches = stream_state.stream_indices.find_record(&record).await;
            if !matches.is_empty() {
                for loc in matches {
                    match loc {
                        RecordLocation::MemoryBatch(batch_id, row_id) => {
                            let batch = stream_state
                                .new_record_batches
                                .get(&batch_id)
                                .expect("Attempting to delete batch that doesn't exist");

                            if batch.deletions.is_deleted(row_id) {
                                continue;
                            }
                            if record.row_identity.is_some()
                                && metadata_identity.requires_identity_check_in_mem_slice()
                                && !record
                                    .row_identity
                                    .as_ref()
                                    .unwrap()
                                    .equals_record_batch_at_offset(
                                        batch.data.as_ref().unwrap(),
                                        row_id,
                                        &metadata_identity,
                                    )
                            {
                                continue;
                            }
                            // Push the deletion record to stream state
                            stream_state.local_deletions.push(ProcessedDeletionRecord {
                                pos: loc,
                                lsn: record.lsn,
                            });
                            // Mark the row as deleted in the batch
                            assert!(stream_state
                                .new_record_batches
                                .get_mut(&batch_id)
                                .unwrap()
                                .deletions
                                .delete_row(row_id));
                            return;
                        }
                        RecordLocation::DiskFile(file_id, row_id) => {
                            let (file, disk_file_entry) = stream_state
                                .flushed_files
                                .get_key_value_mut(&file_id)
                                .expect("missing disk file");
                            if disk_file_entry.batch_deletion_vector.is_deleted(row_id) {
                                continue;
                            }
                            if record.row_identity.is_none()
                                || record
                                    .row_identity
                                    .as_ref()
                                    .unwrap()
                                    .equals_parquet_at_offset(
                                        file.file_path(),
                                        row_id,
                                        &metadata_identity,
                                    )
                                    .await
                            {
                                stream_state.local_deletions.push(ProcessedDeletionRecord {
                                    pos: loc,
                                    lsn: record.lsn,
                                });
                                assert!(disk_file_entry.batch_deletion_vector.delete_row(row_id));
                                return;
                            }
                        }
                    }
                }
            }
        }

        // Scope the main table deletion lookup
        record.pos = {
            self.mem_slice
                .find_non_deleted_position(&record, &metadata_identity)
                .await
        };

        let stream_state = self.get_or_create_stream_state(xact_id);
        if record.pos.is_some() {
            stream_state
                .pending_deletions_in_main_mem_slice
                .push(record);
        } else {
            self.next_snapshot_task.new_deletions.push(record);
        }
    }

    pub fn abort_in_stream_batch(&mut self, xact_id: u32) {
        // Record abortion in snapshot task so we can remove any uncommitted deletions
        let stream_state = self
            .transaction_stream_states
            .get_mut(&xact_id)
            .expect("Stream state not found for xact_id: {xact_id}");

        stream_state.status = TransactionStreamStatus::Aborted;

        // If there are no pending flushes, we can remove the stream state immediately
        // Otherwise, let `apply_stream_flush_result` handle the abortion
        if stream_state.pending_flush_count == 0 {
            self.transaction_stream_states.remove(&xact_id);
        }

        self.next_snapshot_task
            .new_streaming_xact
            .push(TransactionStreamOutput::Abort(xact_id));
    }

    /// Drains the current mem slice and prepares a disk slice for flushing.
    /// Adds current mem slice batches and indices to the stream state.
    pub fn prepare_stream_disk_slice(
        &mut self,
        xact_id: u32,
        lsn: Option<u64>,
    ) -> Result<DiskSliceWriter> {
        let stream_state = self
            .transaction_stream_states
            .get_mut(&xact_id)
            .expect("Stream state not found for xact_id: {xact_id}");
        let next_file_id = self.next_file_id;
        self.next_file_id += 1;

        // Add filtered record batches to stream state
        // We filter here since in the stream case we delete from the current mem slice directly instead of adding to `new_deletions`
        let (_, mut batches, index) = stream_state.mem_slice.drain()?;
        for batch in batches.iter_mut() {
            let filtered_batch = batch.batch.get_filtered_batch()?;
            if let Some(filtered_batch) = filtered_batch {
                let num_rows = filtered_batch.num_rows();
                stream_state.new_record_batches.insert(
                    batch.id,
                    InMemoryBatch {
                        data: Some(Arc::new(filtered_batch)),
                        deletions: BatchDeletionVector::new(num_rows),
                    },
                );
            }
        }

        // Add mem index to stream state
        let index = Arc::new(index);
        stream_state
            .stream_indices
            .insert_memory_index(index.clone());

        let path = self.metadata.path.clone();
        let parquet_flush_threshold_size = self.metadata.config.disk_slice_parquet_file_size;

        let disk_slice = DiskSliceWriter::new(
            self.metadata.schema.clone(),
            path,
            batches,
            lsn,
            next_file_id,
            index,
            parquet_flush_threshold_size,
        );

        Ok(disk_slice)
    }

    /// Flushes a disk slice for streaming transaction.
    /// Increments the pending flush count for this transaction.
    pub async fn flush_stream_disk_slice(
        &mut self,
        xact_id: u32,
        disk_slice: &mut DiskSliceWriter,
    ) -> Result<()> {
        let stream_state = self
            .transaction_stream_states
            .get_mut(&xact_id)
            .expect("Stream state not found for xact_id: {xact_id}");
        stream_state.pending_flush_count += 1;
        disk_slice.write().await?;
        Ok(())
    }

    /// Applies the result of a streaming flush to the stream state.
    /// Decrements the pending flush count for this transaction.
    /// Handles commit and abort cleanup.
    /// Removes in memory indices and record batches from the stream state.
    pub fn apply_stream_flush_result(&mut self, xact_id: u32, mut disk_slice: DiskSliceWriter) {
        let stream_state = self
            .transaction_stream_states
            .get_mut(&xact_id)
            .expect("Stream state not found for xact_id: {xact_id}");
        stream_state.pending_flush_count -= 1;

        // Transaction committed while stream was flushing. Add disk slice to snapshot task and let snapshot handle it.
        // Drop the stream state since the transaction is over.
        if stream_state.status == TransactionStreamStatus::Committed {
            self.next_snapshot_task.new_disk_slices.push(disk_slice);
            if stream_state.pending_flush_count == 0 {
                self.transaction_stream_states.remove(&xact_id);
            }
            return;
        }

        // Transaction aborted while stream was flushing. Remove stream state and do nothing.
        // Drop the stream state since the transaction is over.
        if stream_state.status == TransactionStreamStatus::Aborted {
            if stream_state.pending_flush_count == 0 {
                self.transaction_stream_states.remove(&xact_id);
            }
            return;
        }

        // Append state so we can find deletes during tx
        for (file, file_attrs) in disk_slice.output_files().iter() {
            ma::assert_gt!(file_attrs.file_size, 0);
            let disk_file_entry = DiskFileEntry {
                num_rows: file_attrs.row_num,
                file_size: file_attrs.file_size,
                cache_handle: None,
                batch_deletion_vector: BatchDeletionVector::new(file_attrs.row_num),
                puffin_deletion_blob: None,
            };
            // Add now flushed files to stream state
            stream_state
                .flushed_files
                .insert(file.clone(), disk_file_entry);
        }

        // Add flushed file index to stream state
        let index = disk_slice.take_index();
        if let Some(index) = index {
            stream_state.stream_indices.insert_file_index(index);
        }
        // Remove now flushed in mem batches
        for batch in disk_slice.input_batches().iter() {
            stream_state.new_record_batches.remove(&batch.id);
        }
        // Remove now flushed in mem indices
        let old_index = disk_slice.old_index();
        stream_state.stream_indices.delete_memory_index(old_index);

        // Remap local in mem deletions to disk deletions
        for deletion in stream_state.local_deletions.iter_mut() {
            disk_slice.remap_deletion_if_needed(deletion);
        }
    }

    pub async fn flush_stream(&mut self, xact_id: u32, lsn: Option<u64>) -> Result<()> {
        let mut disk_slice = self.prepare_stream_disk_slice(xact_id, lsn)?;
        self.flush_stream_disk_slice(xact_id, &mut disk_slice)
            .await?;
        if let Some(lsn) = lsn {
            self.next_snapshot_task.new_flush_lsn = Some(lsn);
        }
        self.apply_stream_flush_result(xact_id, disk_slice);

        Ok(())
    }

    /// Commit a transaction stream.
    /// Flushes any remaining rows from stream mem slice
    /// Adds all in mem batches and indices to next snapshot task
    /// Updates deletion records
    /// Enqueues `TransactionStreamOutput::Commit` for snapshot task
    pub async fn commit_transaction_stream(&mut self, xact_id: u32, lsn: u64) -> Result<()> {
        self.flush_stream(xact_id, Some(lsn)).await?;

        // Now remove and process the state
        let stream_state = self
            .transaction_stream_states
            .get_mut(&xact_id)
            .expect("Stream state not found for xact_id: {xact_id}");

        // Add state from current mem slice to stream first
        let (_, mut batches, index) = stream_state.mem_slice.drain()?;
        for batch in batches.iter_mut() {
            let filtered_batch = batch.batch.get_filtered_batch()?;
            if let Some(filtered_batch) = filtered_batch {
                let num_rows = filtered_batch.num_rows();
                stream_state.new_record_batches.insert(
                    batch.id,
                    InMemoryBatch {
                        data: Some(Arc::new(filtered_batch)),
                        deletions: BatchDeletionVector::new(num_rows),
                    },
                );
            }
        }
        stream_state
            .stream_indices
            .insert_memory_index(Arc::new(index));

        // Add stream record batches to next snapshot task
        for (id, batch) in stream_state.new_record_batches.iter() {
            self.next_snapshot_task
                .new_record_batches
                .push((*id, batch.data.as_ref().unwrap().clone()));
        }
        // Add stream in mem indices to next snapshot task
        self.next_snapshot_task.new_mem_indices.extend(
            stream_state
                .stream_indices
                .in_memory_index
                .iter()
                .map(|ptr| ptr.arc_ptr()),
        );
        self.next_snapshot_task.new_commit_lsn = lsn;

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
            deletion.lsn = lsn - 1;
        }

        let commit = TransactionStreamCommit {
            xact_id,
            commit_lsn: lsn,
            flushed_file_index: stream_state.stream_indices.clone(),
            flushed_files: stream_state.flushed_files.clone(),
            local_deletions: std::mem::take(&mut stream_state.local_deletions),
            pending_deletions: std::mem::take(
                &mut stream_state.pending_deletions_in_main_mem_slice,
            ),
        };
        self.next_snapshot_task
            .new_streaming_xact
            .push(TransactionStreamOutput::Commit(commit));

        stream_state.status = TransactionStreamStatus::Committed;

        Ok(())
    }
}

impl SnapshotTableState {
    /// Return files evicted from object storage cache.
    pub(super) async fn apply_transaction_stream(
        &mut self,
        task: &mut SnapshotTask,
    ) -> Vec<String> {
        // Aggregate evicted data cache files to delete.
        let mut evicted_files = vec![];

        let new_streaming_xact = task.new_streaming_xact.drain(..);
        for output in new_streaming_xact {
            match output {
                TransactionStreamOutput::Commit(commit) => {
                    // Integrate files into current snapshot and import into object storage cache.
                    for (file, mut disk_file_entry) in commit.flushed_files.into_iter() {
                        task.disk_file_lsn_map
                            .insert(file.file_id(), commit.commit_lsn);

                        // Import data files into cache.
                        let file_id = TableUniqueFileId {
                            table_id: TableId(self.mooncake_table_metadata.table_id),
                            file_id: file.file_id(),
                        };
                        let (cache_handle, cur_evicted_files) = self
                            .object_storage_cache
                            .import_cache_entry(
                                file_id,
                                CacheEntry {
                                    cache_filepath: file.file_path().clone(),
                                    file_metadata: FileMetadata {
                                        file_size: disk_file_entry.file_size as u64,
                                    },
                                },
                            )
                            .await;
                        disk_file_entry.cache_handle = Some(cache_handle);
                        evicted_files.extend(cur_evicted_files);
                        self.current_snapshot
                            .disk_files
                            .insert(file, disk_file_entry);
                    }

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
                                deletion.lsn = commit.commit_lsn - 1;
                            }
                        }
                    });
                    task.new_deletions.iter_mut().for_each(|deletion| {
                        if deletion.lsn == get_lsn_for_pending_xact(commit.xact_id) {
                            deletion.lsn = commit.commit_lsn - 1;
                        }
                    });
                }
                TransactionStreamOutput::Abort(xact_id) => {
                    self.uncommitted_deletion_log.retain(|deletion| {
                        deletion.as_ref().unwrap().lsn != get_lsn_for_pending_xact(xact_id)
                    });
                    task.new_deletions
                        .retain(|deletion| deletion.lsn != get_lsn_for_pending_xact(xact_id));
                }
            }
        }

        evicted_files
    }
}
