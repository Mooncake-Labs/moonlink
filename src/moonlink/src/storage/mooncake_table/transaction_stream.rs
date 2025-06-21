use super::*;
use crate::storage::cache::object_storage::base_cache::{CacheEntry, CacheTrait, FileMetadata};
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
    flushed_file_index: MooncakeIndex,
    flushed_files: hashbrown::HashMap<MooncakeDataFileRef, DiskFileEntry>,
}

pub enum TransactionStreamOutput {
    Commit(TransactionStreamCommit),
    Abort(u32),
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
        mut object_storage_cache: ObjectStorageCache,
        table_id: TableId,
    ) -> Vec<String> {
        let mut evicted_files_to_delete = vec![];

        for cur_file_index in self.flushed_file_index.file_indices.iter_mut() {
            for cur_index_block in cur_file_index.index_blocks.iter_mut() {
                let table_unique_file_id = TableUniqueFileId {
                    table_id,
                    file_id: cur_index_block.index_file.file_id(),
                };
                let cache_entry = CacheEntry {
                    cache_filepath: cur_index_block.index_file.file_path().clone(),
                    file_metadata: FileMetadata {
                        file_size: cur_index_block.file_size,
                    },
                };
                let (cache_handle, cur_evicted_files) = object_storage_cache
                    .import_cache_entry(table_unique_file_id, cache_entry)
                    .await;
                cur_index_block.cache_handle = Some(cache_handle);
                evicted_files_to_delete.extend(cur_evicted_files);
            }
        }

        evicted_files_to_delete
    }
}

impl TransactionStreamState {
    fn new(schema: Arc<Schema>, batch_size: usize, identity: IdentityProp) -> Self {
        Self {
            mem_slice: MemSlice::new(schema, batch_size, identity),
            local_deletions: Vec::new(),
            pending_deletions_in_main_mem_slice: Vec::new(),
            index_bloom_filter: BloomFilter::with_num_bits(1 << 24).expected_items(1_000_000),
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
        stream_state.index_bloom_filter.insert(&lookup_key);
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
        // it is very unlikely to delete a row in current transaction,
        // only very weird query shape could do it.
        // use a bloom filter to skip any index lookup (which could be costly)
        let bloom_filter_pass = stream_state.index_bloom_filter.contains(&lookup_key);
        // skip any index lookup if bloom filter don't pass
        if bloom_filter_pass {
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
                                &self.metadata.identity,
                            )
                            .await
                    {
                        stream_state.local_deletions.push(ProcessedDeletionRecord {
                            pos: loc,
                            lsn: record.lsn,
                        });
                        disk_file_entry.batch_deletion_vector.delete_row(row_id);
                        return;
                    }
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

            for (file, file_attrs) in disk_slice.output_files().iter() {
                ma::assert_gt!(file_attrs.file_size, 0);
                let disk_file_entry = DiskFileEntry {
                    file_size: file_attrs.file_size,
                    cache_handle: None,
                    batch_deletion_vector: BatchDeletionVector::new(file_attrs.row_num),
                    puffin_deletion_blob: None,
                };
                stream_state
                    .flushed_files
                    .insert(file.clone(), disk_file_entry);
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
                            table_id: TableId(self.mooncake_table_metadata.id),
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

        evicted_files
    }
}
