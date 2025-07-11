use super::data_batches::{create_batch_from_rows, InMemoryBatch};
use super::delete_vector::BatchDeletionVector;
use super::{
    DiskFileEntry, IcebergSnapshotPayload, Snapshot, SnapshotTask,
    TableMetadata as MooncakeTableMetadata,
};
use crate::error::Result;
use crate::storage::cache::object_storage::base_cache::{
    CacheEntry as DataFileCacheEntry, CacheTrait, FileMetadata,
};
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
use crate::storage::compaction::table_compaction::{
    CompactedDataEntry, DataCompactionPayload, RemappedRecordLocation, SingleFileToCompact,
};
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::index::{cache_utils as index_cache_utils, FileIndex};
use crate::storage::mooncake_table::persistence_buffer::UnpersistedRecords;
use crate::storage::mooncake_table::shared_array::SharedRowBufferSnapshot;
use crate::storage::mooncake_table::snapshot_read_output::{
    DataFileForRead, ReadOutput as SnapshotReadOutput,
};
use crate::storage::mooncake_table::table_snapshot::{
    FileIndiceMergePayload, IcebergSnapshotDataCompactionPayload,
};
use crate::storage::mooncake_table::transaction_stream::TransactionStreamOutput;
use crate::storage::mooncake_table::SnapshotOption;
use crate::storage::mooncake_table::{
    IcebergSnapshotImportPayload, IcebergSnapshotIndexMergePayload, MoonlinkRow,
};
use crate::storage::storage_utils::{FileId, TableId, TableUniqueFileId};
use crate::storage::storage_utils::{
    MooncakeDataFileRef, ProcessedDeletionRecord, RawDeletionRecord, RecordLocation,
};
use crate::table_notify::TableEvent;
use crate::{create_data_file, NonEvictableHandle};
use more_asserts as ma;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem::take;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
pub(crate) struct SnapshotTableState {
    /// Mooncake table metadata.
    pub(super) mooncake_table_metadata: Arc<MooncakeTableMetadata>,

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
    pub(crate) committed_deletion_log: Vec<ProcessedDeletionRecord>,
    // Type-1: uncommitted deletion logs.
    pub(crate) uncommitted_deletion_log: Vec<Option<ProcessedDeletionRecord>>,

    /// Last commit point
    last_commit: RecordLocation,

    /// Object storage cache.
    pub(super) object_storage_cache: ObjectStorageCache,

    /// Filesystem accessor.
    pub(super) filesystem_accessor: Arc<dyn BaseFileSystemAccess>,

    /// Table notifier.
    table_notify: Option<Sender<TableEvent>>,

    /// ---- Items not persisted to iceberg snapshot ----
    ///
    /// Iceberg snapshot is created in an async style, which means it doesn't correspond 1-1 to mooncake snapshot, so we need to ensure idempotency for iceberg snapshot payload.
    /// The following fields record unpersisted content, which will be placed in iceberg payload everytime.
    pub(super) unpersisted_records: UnpersistedRecords,
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

pub(crate) struct MooncakeSnapshotOutput {
    /// Committed LSN for mooncake snapshot.
    pub(crate) commit_lsn: u64,
    /// Iceberg snapshot payload.
    pub(crate) iceberg_snapshot_payload: Option<IcebergSnapshotPayload>,
    /// Data compaction payload.
    pub(crate) data_compaction_payload: Option<DataCompactionPayload>,
    /// File indice merge payload.
    pub(crate) file_indices_merge_payload: Option<FileIndiceMergePayload>,
    /// Evicted local data cache files to delete.
    pub(crate) evicted_data_files_to_delete: Vec<String>,
}

impl SnapshotTableState {
    pub(super) async fn new(
        metadata: Arc<MooncakeTableMetadata>,
        object_storage_cache: ObjectStorageCache,
        filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
        current_snapshot: Snapshot,
    ) -> Result<Self> {
        let mut batches = BTreeMap::new();
        batches.insert(0, InMemoryBatch::new(metadata.config.batch_size));

        let table_config = metadata.config.clone();
        Ok(Self {
            mooncake_table_metadata: metadata.clone(),
            current_snapshot,
            batches,
            rows: None,
            last_commit: RecordLocation::MemoryBatch(0, 0),
            object_storage_cache,
            filesystem_accessor,
            table_notify: None,
            committed_deletion_log: Vec::new(),
            uncommitted_deletion_log: Vec::new(),
            unpersisted_records: UnpersistedRecords::new(table_config),
        })
    }

    /// Util function to get table unique file id.
    fn get_table_unique_file_id(&self, file_id: FileId) -> TableUniqueFileId {
        TableUniqueFileId {
            table_id: TableId(self.mooncake_table_metadata.table_id),
            file_id,
        }
    }

    /// Register event completion notifier.
    /// Notice it should be registered only once, which could be used to notify multiple events.
    pub(crate) fn register_table_notify(&mut self, table_notify: Sender<TableEvent>) {
        assert!(self.table_notify.is_none());
        self.table_notify = Some(table_notify);
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
            ma::assert_le!(cur_deletion_log.lsn, self.current_snapshot.snapshot_version);
            if cur_deletion_log.lsn >= flush_lsn {
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
        if task.iceberg_persisted_records.flush_lsn.is_none() {
            return;
        }

        // Keep two types of committed logs: (1) in-memory committed deletion logs; (2) commit point after flush LSN.
        // All on-disk committed deletion logs, which are <= iceberg snapshot flush LSN could be pruned.
        let mut new_committed_deletion_log = vec![];
        let flush_point_lsn = task.iceberg_persisted_records.flush_lsn.unwrap();

        let old_committed_deletion_logs = std::mem::take(&mut self.committed_deletion_log);
        for cur_deletion_log in old_committed_deletion_logs.into_iter() {
            ma::assert_le!(cur_deletion_log.lsn, self.current_snapshot.snapshot_version);
            if cur_deletion_log.lsn >= flush_point_lsn {
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
    /// Return the evicted files to delete.
    async fn update_deletion_vector_to_persisted(
        &mut self,
        puffin_blob_ref: HashMap<FileId, PuffinBlobRef>,
    ) -> Vec<String> {
        // Aggregate the evicted files to delete.
        let mut evicted_files_to_delete = vec![];

        for (file_id, puffin_blob_ref) in puffin_blob_ref.into_iter() {
            let entry = self.current_snapshot.disk_files.get_mut(&file_id).unwrap();
            // Unreference and delete old cache handle if any.
            let old_puffin_blob = entry.puffin_deletion_blob.take();
            if let Some(mut old_puffin_blob) = old_puffin_blob {
                let cur_evicted_files = old_puffin_blob
                    .puffin_file_cache_handle
                    .unreference_and_delete()
                    .await;
                evicted_files_to_delete.extend(cur_evicted_files);
            }
            entry.puffin_deletion_blob = Some(puffin_blob_ref);
        }

        evicted_files_to_delete
    }

    /// Update disk files in the current snapshot from local data files to remote ones, meanwile unpin write-through cache file from object storage cache.
    /// Provide [`persisted_data_files`] could come from imported new files, or maintainance jobs like compaction.
    /// Return cache evicted files to delete.
    async fn update_data_files_to_persisted(
        &mut self,
        persisted_data_files: Vec<MooncakeDataFileRef>,
    ) -> Vec<String> {
        // Aggregate evicted files to delete.
        let mut evicted_files_to_delete = vec![];

        if persisted_data_files.is_empty() {
            return evicted_files_to_delete;
        }

        // Update disk file from local write through cache to iceberg persisted remote path.
        // TODO(hjiang): We should be able to save some copies here.
        for cur_data_file in persisted_data_files.iter() {
            // Removing entry with [`cur_data_file`] and insert with the same key might be confusing, but here we're only using file id as key, but not filepath.
            // So the real operation is: remove the entry with <old filepath> and insert with <new filepath>.
            let mut disk_file_entry = self
                .current_snapshot
                .disk_files
                .remove(cur_data_file)
                .unwrap();
            let cur_evicted_files = disk_file_entry
                .cache_handle
                .as_mut()
                .unwrap()
                .unreference_and_replace_with_remote(cur_data_file.file_path())
                .await;
            evicted_files_to_delete.extend(cur_evicted_files);
            disk_file_entry.cache_handle = None;

            self.current_snapshot
                .disk_files
                .insert(cur_data_file.clone(), disk_file_entry);
        }

        evicted_files_to_delete
    }

    /// Update file indices in the current snapshot from local data files to remote ones.
    /// Return evicted files to delete.
    ///
    /// # Arguments
    ///
    /// * updated_file_ids: file ids which are updated by data files update, used to identify which file indices to remove.
    /// * new_file_indices: newly persisted file indices, need to reflect the update to mooncake snapshot.
    async fn update_file_indices_to_persisted(
        &mut self,
        mut new_file_indices: Vec<FileIndex>,
        updated_file_ids: HashSet<FileId>,
    ) -> Vec<String> {
        if new_file_indices.is_empty() && updated_file_ids.is_empty() {
            return vec![];
        }

        // Update file indice from local write through cache to iceberg persisted remote path.
        // TODO(hjiang): For better update performance, we might need to use hash set instead vector to store file indices.
        let cur_file_indices = std::mem::take(&mut self.current_snapshot.indices.file_indices);
        let mut updated_file_indices = Vec::with_capacity(cur_file_indices.len());
        for cur_file_index in cur_file_indices.into_iter() {
            let mut skip = false;
            let referenced_data_files = &cur_file_index.files;
            for cur_data_file in referenced_data_files.iter() {
                if updated_file_ids.contains(&cur_data_file.file_id()) {
                    skip = true;
                    break;
                }
            }

            // If one referenced file gets updated, all others should get updated.
            #[cfg(test)]
            if skip {
                for cur_data_file in referenced_data_files.iter() {
                    assert!(updated_file_ids.contains(&cur_data_file.file_id()));
                }
            }

            if !skip {
                updated_file_indices.push(cur_file_index);
            }
        }

        // Aggregate evicted files to delete.
        let mut evicted_files_to_delete = vec![];

        // For newly persisted index block files, attempt local filesystem optimization to replace local cache filepath to remote if applicable.
        // At this point, all index block files are at an inconsistent state, which have their
        // - file path pointing to remote path
        // - cache handle pinned and refers to local cache file path
        for cur_file_index in new_file_indices.iter_mut() {
            for cur_index_block in cur_file_index.index_blocks.iter_mut() {
                // All index block files have their cache handle pinned in cache.
                let cur_evicted_files = cur_index_block
                    .cache_handle
                    .as_mut()
                    .unwrap()
                    .replace_with_remote(cur_index_block.index_file.file_path())
                    .await;
                evicted_files_to_delete.extend(cur_evicted_files);

                // Reset the index block to be local cache file, to keep it consistent.
                cur_index_block.index_file = create_data_file(
                    cur_index_block.index_file.file_id().0,
                    cur_index_block
                        .cache_handle
                        .as_ref()
                        .unwrap()
                        .cache_entry
                        .cache_filepath
                        .to_string(),
                );
            }
        }
        updated_file_indices.extend(new_file_indices);
        self.current_snapshot.indices.file_indices = updated_file_indices;

        evicted_files_to_delete
    }

    /// Update current snapshot with iceberg persistence result.
    /// Before iceberg snapshot, mooncake snapshot records local write through cache in disk file (which is local filepath).
    /// After a successful iceberg snapshot, update current snapshot's disk files and file indices to reference to remote paths,
    /// also import local write through cache to globally managed object storage cache, so they could be pinned and evicted when necessary.
    ///
    /// Return evicted data files to delete when unreference existing disk file entries.
    async fn update_snapshot_by_iceberg_snapshot(&mut self, task: &SnapshotTask) -> Vec<String> {
        // Aggregate evicted files to delete.
        let mut evicted_files_to_delete = vec![];

        // Get persisted data files and file indices.
        // TODO(hjiang): Revisit whether we need separate fields in snapshot task.
        let persisted_data_files = task
            .iceberg_persisted_records
            .get_data_files_to_reflect_persistence();
        let (index_blocks_to_remove, persisted_file_indices) = task
            .iceberg_persisted_records
            .get_file_indices_to_reflect_persistence();

        // Record data files number and file indices number for persistence reflection, which is not supposed to change.
        let old_data_files_count = self.current_snapshot.disk_files.len();
        let old_file_indices_count = self.current_snapshot.indices.file_indices.len();

        // Step-1: Handle persisted data files.
        let cur_evicted_files = self
            .update_data_files_to_persisted(persisted_data_files)
            .await;
        evicted_files_to_delete.extend(cur_evicted_files);

        // Step-2: Handle persisted file indices.
        let cur_evicted_files = self
            .update_file_indices_to_persisted(persisted_file_indices, index_blocks_to_remove)
            .await;
        evicted_files_to_delete.extend(cur_evicted_files);

        // Step-3: Handle persisted deletion vector.
        let cur_evicted_files = self
            .update_deletion_vector_to_persisted(
                task.iceberg_persisted_records
                    .import_result
                    .puffin_blob_ref
                    .clone(),
            )
            .await;
        evicted_files_to_delete.extend(cur_evicted_files);

        // Check data files number and file indices number don't change after persistence reflection.
        let new_data_files_count = self.current_snapshot.disk_files.len();
        let new_file_indices_count = self.current_snapshot.indices.file_indices.len();
        assert_eq!(old_data_files_count, new_data_files_count);
        assert_eq!(old_file_indices_count, new_file_indices_count);

        evicted_files_to_delete
    }

    /// Util function to decide whether to create iceberg snapshot by deletion vectors.
    fn create_iceberg_snapshot_by_committed_logs(&self, force_create: bool) -> bool {
        let deletion_record_snapshot_threshold = if !force_create {
            self.mooncake_table_metadata
                .config
                .iceberg_snapshot_new_committed_deletion_log()
        } else {
            1
        };
        self.committed_deletion_log.len() >= deletion_record_snapshot_threshold
    }

    /// Util function to decide whether and what to compact data files.
    /// To simplify states (aka, avoid data compaction already in iceberg with those not), only merge those already persisted.
    fn get_payload_to_compact(&self) -> Option<DataCompactionPayload> {
        // Fast-path: not enough data files to trigger compaction.
        let all_disk_files = &self.current_snapshot.disk_files;
        if all_disk_files.len()
            < self
                .mooncake_table_metadata
                .config
                .data_compaction_config
                .data_file_to_compact as usize
        {
            return None;
        }

        // To simplify state management, only compact data files which have been persisted into iceberg table.
        let unpersisted_data_files = self.unpersisted_records.get_unpersisted_data_files_set();
        let mut tentative_data_files_to_compact = vec![];

        // TODO(hjiang): We should be able to early exit, if left items are not enough to reach the compaction threshold.
        for (cur_data_file, disk_file_entry) in all_disk_files.iter() {
            // Doesn't compact those unpersisted files.
            if unpersisted_data_files.contains(cur_data_file) {
                continue;
            }

            // Skip compaction if the file size exceeds threshold, AND it has no persisted deletion vectors.
            if disk_file_entry.file_size
                >= self
                    .mooncake_table_metadata
                    .config
                    .data_compaction_config
                    .data_file_final_size as usize
                && disk_file_entry.batch_deletion_vector.is_empty()
            {
                continue;
            }

            // Tentatively decide data file to compact.
            let single_file_to_compact = SingleFileToCompact {
                file_id: TableUniqueFileId {
                    table_id: TableId(self.mooncake_table_metadata.table_id),
                    file_id: cur_data_file.file_id(),
                },
                filepath: cur_data_file.file_path().to_string(),
                deletion_vector: disk_file_entry.puffin_deletion_blob.clone(),
            };
            tentative_data_files_to_compact.push(single_file_to_compact);
        }

        if tentative_data_files_to_compact.len()
            < self
                .mooncake_table_metadata
                .config
                .data_compaction_config
                .data_file_to_compact as usize
        {
            return None;
        }

        // Calculate related file indices to compact.
        let mut file_indices_to_compact = vec![];
        let file_ids_to_compact = tentative_data_files_to_compact
            .iter()
            .map(|single_file_to_compact| single_file_to_compact.file_id.file_id)
            .collect::<HashSet<_>>();
        for cur_file_index in self.current_snapshot.indices.file_indices.iter() {
            for cur_file in cur_file_index.files.iter() {
                if file_ids_to_compact.contains(&cur_file.file_id()) {
                    file_indices_to_compact.push(cur_file_index.clone());
                    break;
                }
            }
        }

        Some(DataCompactionPayload {
            object_storage_cache: self.object_storage_cache.clone(),
            filesystem_accessor: self.filesystem_accessor.clone(),
            disk_files: tentative_data_files_to_compact,
            file_indices: file_indices_to_compact,
        })
    }

    /// Util function to decide whether and what to merge index.
    /// To simplify states (aka, avoid merging file indices already in iceberg with those not), only merge those already persisted.
    #[allow(clippy::mutable_key_type)]
    fn get_file_indices_to_merge(&self) -> Option<FileIndiceMergePayload> {
        // Fast-path: not enough file indices to trigger index merge.
        let mut file_indices_to_merge = HashSet::new();
        let all_file_indices = &self.current_snapshot.indices.file_indices;
        if all_file_indices.len()
            < self
                .mooncake_table_metadata
                .config
                .file_index_config
                .file_indices_to_merge as usize
        {
            return None;
        }

        // To simplify state management, only compact data files which have been persisted into iceberg table.
        let unpersisted_file_indices = self.unpersisted_records.get_unpersisted_file_indices_set();

        for cur_file_index in all_file_indices.iter() {
            // Don't merge unpersisted file indices.
            if unpersisted_file_indices.contains(cur_file_index) {
                continue;
            }

            if cur_file_index.get_index_blocks_size()
                >= self
                    .mooncake_table_metadata
                    .config
                    .file_index_config
                    .index_block_final_size
            {
                continue;
            }
            assert!(file_indices_to_merge.insert(cur_file_index.clone()));
        }
        // To avoid too many small IO operations, only attempt an index merge when accumulated small indices exceeds the threshold.
        if file_indices_to_merge.len()
            >= self
                .mooncake_table_metadata
                .config
                .file_index_config
                .file_indices_to_merge as usize
        {
            return Some(FileIndiceMergePayload {
                file_indices: file_indices_to_merge,
            });
        }
        None
    }

    /// Update current snapshot's file indices by adding and removing a few.
    #[allow(clippy::mutable_key_type)]
    async fn update_file_indices_to_mooncake_snapshot_impl(
        &mut self,
        mut old_file_indices: HashSet<FileIndex>,
        new_file_indices: Vec<FileIndex>,
    ) -> Vec<String> {
        // Aggregate evicted files to delete.
        let mut evicted_files_to_delete = vec![];

        if old_file_indices.is_empty() {
            assert!(new_file_indices.is_empty());
            return evicted_files_to_delete;
        }

        // Unreference all old file indices.
        for cur_file_index in old_file_indices.iter() {
            let mut file_index_copy = cur_file_index.clone();
            let cur_evicted_files =
                index_cache_utils::unreference_and_delete_file_index_from_cache(
                    &mut file_index_copy,
                )
                .await;
            evicted_files_to_delete.extend(cur_evicted_files);
        }

        // Update current snapshot's file indices.
        let file_indices = std::mem::take(&mut self.current_snapshot.indices.file_indices);
        ma::assert_le!(old_file_indices.len(), file_indices.len());
        let updated_file_indices_len =
            file_indices.len() - old_file_indices.len() + new_file_indices.len();
        let mut updated_file_indices = Vec::with_capacity(updated_file_indices_len);

        for cur_file_indice in file_indices.into_iter() {
            if old_file_indices.remove(&cur_file_indice) {
                continue;
            }
            updated_file_indices.push(cur_file_indice);
        }
        updated_file_indices.extend(new_file_indices);
        self.current_snapshot.indices.file_indices = updated_file_indices;

        // Check all file indices to remove comes from current file indices.
        assert!(old_file_indices.is_empty());

        evicted_files_to_delete
    }

    // Update current snapshot's data files by adding and removing a few, generated by data compaction.
    // Here new data files are all local data files, which will be uploaded to remote by importing into iceberg table.
    // Return evicted data files from the object storage cache to delete.
    async fn update_data_files_to_mooncake_snapshot_impl(
        &mut self,
        old_data_files: HashSet<MooncakeDataFileRef>,
        new_data_files: Vec<(MooncakeDataFileRef, CompactedDataEntry)>,
        remapped_data_files_after_compaction: HashMap<RecordLocation, RemappedRecordLocation>,
    ) -> Vec<String> {
        if old_data_files.is_empty() {
            assert!(new_data_files.is_empty());
            assert!(remapped_data_files_after_compaction.is_empty());
            return vec![];
        }

        // Aggregate evicted files to delete.
        let mut evicted_files_to_delete = vec![];

        // Process new data files to import.
        ma::assert_ge!(self.current_snapshot.disk_files.len(), old_data_files.len());
        for (cur_new_data_file, cur_entry) in new_data_files.iter() {
            ma::assert_gt!(cur_entry.file_size, 0);
            let unique_file_id = self.get_table_unique_file_id(cur_new_data_file.file_id());
            let cache_entry = DataFileCacheEntry {
                cache_filepath: cur_new_data_file.file_path().clone(),
                file_metadata: FileMetadata {
                    file_size: cur_entry.file_size as u64,
                },
            };
            let (cache_handle, cur_evicted_files) = self
                .object_storage_cache
                .import_cache_entry(unique_file_id, cache_entry)
                .await;
            evicted_files_to_delete.extend(cur_evicted_files);

            self.current_snapshot.disk_files.insert(
                cur_new_data_file.clone(),
                DiskFileEntry {
                    file_size: cur_entry.file_size,
                    cache_handle: Some(cache_handle),
                    batch_deletion_vector: BatchDeletionVector::new(
                        /*max_rows=*/ cur_entry.num_rows,
                    ),
                    puffin_deletion_blob: None,
                },
            );
        }

        // Process old data files to remove.
        for cur_old_data_file in old_data_files.into_iter() {
            let old_entry = self.current_snapshot.disk_files.remove(&cur_old_data_file);
            assert!(old_entry.is_some());
            let unique_file_id = self.get_table_unique_file_id(cur_old_data_file.file_id());

            // ====================================
            // Process data file
            // ====================================
            //
            // If the old entry is pinned cache handle, unreference.
            let old_entry = old_entry.unwrap();
            if let Some(mut cache_handle) = old_entry.cache_handle {
                // The old entry is no longer needed for mooncake table, directly mark it deleted from cache, so we could reclaim the disk space back ASAP.
                let cur_evicted_files = cache_handle.unreference_and_delete().await;
                evicted_files_to_delete.extend(cur_evicted_files);
            }
            // Even if there's no pinned cache handle within current snapshot (since it's persisted), still try to delete it from cache if exists.
            else {
                let cur_evicted_files = self
                    .object_storage_cache
                    .try_delete_cache_entry(unique_file_id)
                    .await;
                evicted_files_to_delete.extend(cur_evicted_files);
            }

            // ====================================
            // Process deletion vector
            // ====================================
            //
            // If no deletion record for this file, directly remove it, no need to do remapping.
            if old_entry.batch_deletion_vector.is_empty() {
                assert!(old_entry.puffin_deletion_blob.is_none());
                continue;
            }

            // Unpin and request to delete all cached puffin files.
            if let Some(mut puffin_deletion_blob) = old_entry.puffin_deletion_blob {
                let cur_evicted_files = puffin_deletion_blob
                    .puffin_file_cache_handle
                    .unreference_and_delete()
                    .await;
                evicted_files_to_delete.extend(cur_evicted_files);
            }

            // If there's deletion record, try remap to the new compacted data file.
            let deleted_rows = old_entry.batch_deletion_vector.collect_deleted_rows();
            for cur_deleted_row in deleted_rows {
                let old_record_location =
                    RecordLocation::DiskFile(cur_old_data_file.file_id(), cur_deleted_row as usize);
                let new_record_location =
                    remapped_data_files_after_compaction.get(&old_record_location);
                // Case-1: The old record still exists, need to remap.
                if let Some(new_record_location) = new_record_location {
                    let new_deletion_entry = self
                        .current_snapshot
                        .disk_files
                        .get_mut(&new_record_location.new_data_file)
                        .unwrap();
                    new_deletion_entry
                        .batch_deletion_vector
                        .delete_row(new_record_location.record_location.get_row_idx());
                }
                // Case-2: The old record has already been compacted, directly skip.
            }
        }

        evicted_files_to_delete
    }

    /// Return evicted files to delete.
    async fn update_file_indices_merge_to_mooncake_snapshot(
        &mut self,
        task: &SnapshotTask,
    ) -> Vec<String> {
        self.update_file_indices_to_mooncake_snapshot_impl(
            task.index_merge_result.old_file_indices.clone(),
            task.index_merge_result.new_file_indices.clone(),
        )
        .await
    }

    /// Reflect data compaction results to mooncake snapshot.
    /// Return evicted data files to delete due to data compaction.
    async fn update_data_compaction_to_mooncake_snapshot(
        &mut self,
        task: &SnapshotTask,
    ) -> Vec<String> {
        // Aggregate evicted files to delete.
        let mut evicted_files_to_delete = vec![];

        if task.data_compaction_result.is_empty() {
            return vec![];
        }

        // NOTICE: Update data files before file indices, so when update file indices, data files for new file indices already exist in disk files map.
        let data_compaction_res = task.data_compaction_result.clone();
        let cur_evicted_files = self
            .update_data_files_to_mooncake_snapshot_impl(
                data_compaction_res.old_data_files,
                data_compaction_res.new_data_files,
                data_compaction_res.remapped_data_files,
            )
            .await;
        evicted_files_to_delete.extend(cur_evicted_files);

        let cur_evicted_files = self
            .update_file_indices_to_mooncake_snapshot_impl(
                data_compaction_res.old_file_indices,
                data_compaction_res.new_file_indices,
            )
            .await;
        evicted_files_to_delete.extend(cur_evicted_files);

        // Apply evicted data files to delete within data compaction process.
        evicted_files_to_delete.extend(
            task.data_compaction_result
                .evicted_files_to_delete
                .iter()
                .cloned()
                .to_owned(),
        );

        evicted_files_to_delete
    }

    /// Remap single record location after compaction.
    /// Return if remap succeeds.
    fn remap_record_location_after_compaction(
        deletion_log: &mut ProcessedDeletionRecord,
        task: &mut SnapshotTask,
    ) -> bool {
        if task.data_compaction_result.is_empty() {
            return false;
        }

        let old_record_location = &deletion_log.pos;
        let remapped_data_files_after_compaction = &mut task.data_compaction_result;
        let new_record_location = remapped_data_files_after_compaction
            .remapped_data_files
            .remove(old_record_location);
        if new_record_location.is_none() {
            return false;
        }
        deletion_log.pos = new_record_location.unwrap().record_location;
        true
    }

    /// Data compaction might delete existing persisted files, which invalidates record locations and requires a record location remap.
    fn remap_and_prune_deletion_logs_after_compaction(&mut self, task: &mut SnapshotTask) {
        // No need to prune and remap if no compaction happening.
        if task.data_compaction_result.is_empty() {
            return;
        }

        // Remap and prune committed deletion log.
        let mut new_committed_deletion_log = vec![];
        let old_committed_deletion_log = std::mem::take(&mut self.committed_deletion_log);
        for mut cur_deletion_log in old_committed_deletion_log.into_iter() {
            if let Some(file_id) = cur_deletion_log.get_file_id() {
                // Case-1: the deletion log doesn't indicate a compacted data file.
                if !task
                    .data_compaction_result
                    .old_data_files
                    .contains(&file_id)
                {
                    new_committed_deletion_log.push(cur_deletion_log);
                    continue;
                }
                // Case-2: the deletion log exists in the compacted new data file, perform a remap.
                let remap_succ =
                    Self::remap_record_location_after_compaction(&mut cur_deletion_log, task);
                if remap_succ {
                    new_committed_deletion_log.push(cur_deletion_log);
                    continue;
                }
                // Case-3: the deletion log doesn't exist in the compacted new file, directly remove it.
            } else {
                new_committed_deletion_log.push(cur_deletion_log);
            }
        }
        self.committed_deletion_log = new_committed_deletion_log;

        // Remap uncommitted deletion log.
        for cur_deletion_log in &mut self.uncommitted_deletion_log {
            if cur_deletion_log.is_none() {
                continue;
            }
            Self::remap_record_location_after_compaction(cur_deletion_log.as_mut().unwrap(), task);
        }
    }

    fn get_iceberg_snapshot_payload(
        &self,
        flush_lsn: u64,
        new_committed_deletion_logs: HashMap<MooncakeDataFileRef, BatchDeletionVector>,
    ) -> IcebergSnapshotPayload {
        IcebergSnapshotPayload {
            flush_lsn,
            import_payload: IcebergSnapshotImportPayload {
                data_files: self.unpersisted_records.get_unpersisted_data_files(),
                new_deletion_vector: new_committed_deletion_logs,
                file_indices: self.unpersisted_records.get_unpersisted_file_indices(),
            },
            index_merge_payload: IcebergSnapshotIndexMergePayload {
                new_file_indices_to_import: self
                    .unpersisted_records
                    .get_merged_file_indices_to_add(),
                old_file_indices_to_remove: self
                    .unpersisted_records
                    .get_merged_file_indices_to_remove(),
            },
            data_compaction_payload: IcebergSnapshotDataCompactionPayload {
                new_data_files_to_import: self
                    .unpersisted_records
                    .get_compacted_data_files_to_add(),
                old_data_files_to_remove: self
                    .unpersisted_records
                    .get_compacted_data_files_to_remove(),
                new_file_indices_to_import: self
                    .unpersisted_records
                    .get_compacted_file_indices_to_add(),
                old_file_indices_to_remove: self
                    .unpersisted_records
                    .get_compacted_file_indices_to_remove(),
            },
        }
    }

    /// Unreference pinned cache handles used in read operations.
    /// Return evicted data files to delete.
    async fn unreference_read_cache_handles(&mut self, task: &mut SnapshotTask) -> Vec<String> {
        // Aggregate evicted data files to delete.
        let mut evicted_files_to_delete = vec![];

        for cur_cache_handle in task.read_cache_handles.iter_mut() {
            let cur_evicted_files = cur_cache_handle.unreference().await;
            evicted_files_to_delete.extend(cur_evicted_files);
        }

        evicted_files_to_delete
    }

    /// Take read request result and update mooncake snapshot.
    /// Return evicted data files to delete.
    async fn update_snapshot_by_read_request_results(
        &mut self,
        task: &mut SnapshotTask,
    ) -> Vec<String> {
        // Unpin cached files used in the read request.
        self.unreference_read_cache_handles(task).await
    }

    /// Unreference all pinned data files.
    /// Return all evicted files to evict
    pub(crate) async fn unreference_and_delete_all_cache_handles(&mut self) -> Vec<String> {
        // Aggregate evicted files to delete.
        let mut evicted_files_to_delete = vec![];

        // Unreference and delete data files and puffin files.
        for (_, disk_file_entry) in self.current_snapshot.disk_files.iter_mut() {
            // Handle data files.
            let cache_handle = &mut disk_file_entry.cache_handle;
            if let Some(cache_handle) = cache_handle {
                let cur_evicted_files = cache_handle.unreference_and_delete().await;
                evicted_files_to_delete.extend(cur_evicted_files);
            }
            // Handle puffin files.
            if let Some(puffin_file) = &mut disk_file_entry.puffin_deletion_blob {
                let cur_evicted_files = puffin_file
                    .puffin_file_cache_handle
                    .unreference_and_delete()
                    .await;
                evicted_files_to_delete.extend(cur_evicted_files);
            }
        }

        // Unreference and delete file indices.
        for cur_file_index in self.current_snapshot.indices.file_indices.iter_mut() {
            for cur_index_block in cur_file_index.index_blocks.iter_mut() {
                let cur_evicted_files = cur_index_block
                    .cache_handle
                    .as_mut()
                    .unwrap()
                    .unreference_and_delete()
                    .await;
                evicted_files_to_delete.extend(cur_evicted_files);
            }
        }

        evicted_files_to_delete
    }

    /// Import batch write and stream file indices into cache.
    /// Return evicted files to delete.
    async fn import_file_indices_into_cache(&mut self, task: &mut SnapshotTask) -> Vec<String> {
        let table_id = TableId(self.mooncake_table_metadata.table_id);

        // Aggregate evicted files to delete.
        let mut evicted_files_to_delete = vec![];

        // Import batch write file indices.
        for cur_disk_slice in task.new_disk_slices.iter_mut() {
            let cur_evicted_files = cur_disk_slice
                .import_file_indices_to_cache(self.object_storage_cache.clone(), table_id)
                .await;
            evicted_files_to_delete.extend(cur_evicted_files);
        }

        // Import stream write file indices.
        for cur_stream in task.new_streaming_xact.iter_mut() {
            if let TransactionStreamOutput::Commit(commit) = cur_stream {
                let cur_evicted_files = commit
                    .import_file_index_into_cache(self.object_storage_cache.clone(), table_id)
                    .await;
                evicted_files_to_delete.extend(cur_evicted_files);
            }
        }

        // Import new compacted file indices.
        let new_file_indices_by_index_merge = &mut task.index_merge_result.new_file_indices;
        let cur_evicted_files = index_cache_utils::import_file_indices_to_cache(
            new_file_indices_by_index_merge,
            self.object_storage_cache.clone(),
            table_id,
        )
        .await;
        evicted_files_to_delete.extend(cur_evicted_files);

        // Import new merged file indices.
        let new_file_indices_by_data_compaction = &mut task.data_compaction_result.new_file_indices;
        let cur_evicted_files = index_cache_utils::import_file_indices_to_cache(
            new_file_indices_by_data_compaction,
            self.object_storage_cache.clone(),
            table_id,
        )
        .await;
        evicted_files_to_delete.extend(cur_evicted_files);

        evicted_files_to_delete
    }

    /// Validate mooncake table invariants.
    fn validate_mooncake_table_invariants(&self, task: &SnapshotTask, opt: &SnapshotOption) {
        if let Some(new_flush_lsn) = task.new_flush_lsn {
            if self.current_snapshot.data_file_flush_lsn.is_some() {
                // Invariant-1: flush LSN doesn't regress.
                //
                // Force snapshot not change table states, it's possible to use the latest flush LSN.
                if opt.force_create {
                    ma::assert_le!(
                        self.current_snapshot.data_file_flush_lsn.unwrap(),
                        new_flush_lsn
                    );
                }
                // Otherwise, flush LSN always progresses.
                else {
                    ma::assert_lt!(
                        self.current_snapshot.data_file_flush_lsn.unwrap(),
                        new_flush_lsn
                    );
                }

                // Invariant-2: flush must follow a commit, but commit doesn't need to be followed by a flush.
                //
                // Force snapshot could flush as long as the table at a clean state (aka, no uncommitted states), possible to go without commit at current snapshot iteration.
                if opt.force_create {
                    assert!(
                        task.new_commit_lsn == 0 || task.new_commit_lsn >= new_flush_lsn,
                        "New commit LSN is {}, new flush LSN is {}",
                        task.new_commit_lsn,
                        new_flush_lsn
                    );
                } else {
                    ma::assert_ge!(task.new_commit_lsn, new_flush_lsn);
                }
            }
        }
    }

    pub(super) async fn update_snapshot(
        &mut self,
        mut task: SnapshotTask,
        opt: SnapshotOption,
    ) -> MooncakeSnapshotOutput {
        // Validate mooncake table operation invariants.
        self.validate_mooncake_table_invariants(&task, &opt);

        // All evicted data files by the object storage cache.
        let mut evicted_data_files_to_delete = vec![];

        // Reflect read request result to mooncake snapshot.
        let completed_read_evicted_files = self
            .update_snapshot_by_read_request_results(&mut task)
            .await;
        evicted_data_files_to_delete.extend(completed_read_evicted_files);

        // Reflect iceberg snapshot to mooncake snapshot.
        let persistence_evicted_files = self.update_snapshot_by_iceberg_snapshot(&task).await;
        evicted_data_files_to_delete.extend(persistence_evicted_files);

        // Import all new file indices, including newly imported ones, merged ones, and compacted ones into cache.
        // So it should happen before reflecting index merge and data compaction result into mooncake snapshot, and before integrating stream transactions and disk slices.
        //
        // TODO(hjiang): double check why we cannot apply disk slice/stream transaction before append unpersisted records.
        // Import file indices into cache.
        let file_indices_evicted_files_to_delete =
            self.import_file_indices_into_cache(&mut task).await;
        evicted_data_files_to_delete.extend(file_indices_evicted_files_to_delete);

        // Update disk files' disk entries and file indices from merged indices.
        let index_merge_evicted_files = self
            .update_file_indices_merge_to_mooncake_snapshot(&task)
            .await;
        evicted_data_files_to_delete.extend(index_merge_evicted_files);

        // Update disk file's disk entries and file indices from compacted data files and file indices.
        // Also remap committed deletion logs if applicable.
        let compaction_evicted_files = self
            .update_data_compaction_to_mooncake_snapshot(&task)
            .await;
        evicted_data_files_to_delete.extend(compaction_evicted_files);

        // Prune unpersisted records.
        self.prune_committed_deletion_logs(&task);
        self.unpersisted_records.prune_persisted_records(&task);

        // Sync buffer snapshot states into unpersisted iceberg content.
        self.unpersisted_records.buffer_unpersisted_records(&task);

        // Apply buffered change to current mooncake snapshot.
        let stream_evicted_cache_files = self.apply_transaction_stream(&mut task).await;
        self.merge_mem_indices(&mut task);
        self.finalize_batches(&mut task);
        let batch_evicted_cache_files = self.integrate_disk_slices(&mut task).await;
        evicted_data_files_to_delete.extend(stream_evicted_cache_files);
        evicted_data_files_to_delete.extend(batch_evicted_cache_files);

        // Apply data compaction to committed deletion logs.
        self.remap_and_prune_deletion_logs_after_compaction(&mut task);

        // After data compaction and index merge changes have been applied to snapshot, processed deletion record will point to the new record location.
        self.rows = take(&mut task.new_rows);
        self.process_deletion_log(&mut task).await;

        // Assert and update flush LSN.
        if let Some(new_flush_lsn) = task.new_flush_lsn {
            // Assert flush LSN doesn't regress, if not force snapshot.
            if self.current_snapshot.data_file_flush_lsn.is_some() && !opt.force_create {
                ma::assert_lt!(
                    self.current_snapshot.data_file_flush_lsn.unwrap(),
                    new_flush_lsn
                );
            }
            // Update flush LSN.
            self.current_snapshot.data_file_flush_lsn = Some(new_flush_lsn);
        }
        if task.new_commit_lsn != 0 {
            self.current_snapshot.snapshot_version = task.new_commit_lsn;
        }
        if let Some(cp) = task.new_commit_point {
            self.last_commit = cp;
        }

        // Till this point, committed changes have been reflected to current snapshot; sync the latest change to iceberg.
        // To reduce iceberg persistence overhead, we only snapshot when (1) there're persisted data files, or (2) accumulated unflushed deletion vector exceeds threshold.
        let mut iceberg_snapshot_payload: Option<IcebergSnapshotPayload> = None;
        let flush_by_deletion = self.create_iceberg_snapshot_by_committed_logs(opt.force_create);
        let flush_by_new_files_or_maintainence = self
            .unpersisted_records
            .if_persist_by_new_files_or_maintainence(opt.force_create);

        // Decide whether to perform a data compaction.
        let mut data_compaction_payload: Option<DataCompactionPayload> = None;
        if !opt.skip_data_file_compaction {
            // No need to pin puffin file during compaction:
            // - only compaction deletes puffin file
            // - there's no two ongoing compaction
            data_compaction_payload = self.get_payload_to_compact();
        }

        // Decide whether to merge an index merge, which cannot be performed together with data compaction.
        let mut file_indices_merge_payload: Option<FileIndiceMergePayload> = None;
        if !opt.skip_file_indices_merge && data_compaction_payload.is_none() {
            file_indices_merge_payload = self.get_file_indices_to_merge();
        }

        // TODO(hjiang): Add whether to flush based on merged file indices.
        if !opt.skip_iceberg_snapshot
            && self.current_snapshot.data_file_flush_lsn.is_some()
            && (flush_by_new_files_or_maintainence || flush_by_deletion)
        {
            // Getting persistable committed deletion logs is not cheap, which requires iterating through all logs,
            // so we only aggregate when there's committed deletion.
            let flush_lsn = self.current_snapshot.data_file_flush_lsn.unwrap();
            let aggregated_committed_deletion_logs =
                self.aggregate_committed_deletion_logs(flush_lsn);

            // Only create iceberg snapshot when there's something to import.
            if !aggregated_committed_deletion_logs.is_empty() || flush_by_new_files_or_maintainence
            {
                iceberg_snapshot_payload =
                    Some(self.get_iceberg_snapshot_payload(
                        flush_lsn,
                        aggregated_committed_deletion_logs,
                    ));
            }
        }

        // Expensive assertion, which is only enabled in unit tests.
        #[cfg(test)]
        {
            self.assert_current_snapshot_consistent().await;
        }

        MooncakeSnapshotOutput {
            commit_lsn: self.current_snapshot.snapshot_version,
            iceberg_snapshot_payload,
            data_compaction_payload,
            file_indices_merge_payload,
            evicted_data_files_to_delete,
        }
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

    /// Return files evicted from object storage cache.
    async fn integrate_disk_slices(&mut self, task: &mut SnapshotTask) -> Vec<String> {
        // Aggregate evicted data cache files to delete.
        let mut evicted_files = vec![];

        for mut slice in take(&mut task.new_disk_slices) {
            let write_lsn = slice.lsn();
            let lsn = write_lsn.expect("commited datafile should have a valid LSN");

            // Register new files into mooncake snapshot, add it into cache, and record LSN map.
            for (file, file_attrs) in slice.output_files().iter() {
                ma::assert_gt!(file_attrs.file_size, 0);
                task.disk_file_lsn_map.insert(file.file_id(), lsn);
                let unique_file_id = self.get_table_unique_file_id(file.file_id());
                let (cache_handle, cur_evicted_files) = self
                    .object_storage_cache
                    .import_cache_entry(
                        unique_file_id,
                        DataFileCacheEntry {
                            cache_filepath: file.file_path().clone(),
                            file_metadata: FileMetadata {
                                file_size: file_attrs.file_size as u64,
                            },
                        },
                    )
                    .await;
                evicted_files.extend(cur_evicted_files);
                self.current_snapshot.disk_files.insert(
                    file.clone(),
                    DiskFileEntry {
                        file_size: file_attrs.file_size,
                        cache_handle: Some(cache_handle),
                        batch_deletion_vector: BatchDeletionVector::new(file_attrs.row_num),
                        puffin_deletion_blob: None,
                    },
                );
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

        evicted_files
    }

    async fn match_deletions_with_identical_key_and_lsn(
        &self,
        deletions: &[RawDeletionRecord],
        index_lookup_result: Vec<RecordLocation>,
        file_id_to_lsn: &HashMap<FileId, u64>,
    ) -> Vec<ProcessedDeletionRecord> {
        let mut candidates: Vec<RecordLocation> = index_lookup_result
            .into_iter()
            .filter(|loc| {
                !self.is_deleted(loc)
                    && Self::is_visible(loc, file_id_to_lsn, deletions.first().unwrap().lsn)
            })
            .collect();
        // This optimization is important when working with table without primary key.
        // Postgres never distinguish row with same value, so they will almost always be processed together.
        // Thus we can avoid full row identity comparison if we also process them together.
        match candidates.len().cmp(&deletions.len()) {
            Ordering::Equal => candidates
                .into_iter()
                .zip(deletions.iter())
                .map(|(loc, deletion)| Self::build_processed_deletion(deletion, loc))
                .collect(),
            Ordering::Less => {
                panic!("find less than expected candidates to deletions {deletions:?}")
            }
            Ordering::Greater => {
                let mut processed_deletions = Vec::new();
                // multiple candidates → disambiguate via full row identity comparison.
                for deletion in deletions.iter() {
                    let identity = deletion
                        .row_identity
                        .as_ref()
                        .expect("row_identity required when multiple matches");
                    let mut target_position: Option<RecordLocation> = None;
                    for (idx, loc) in candidates.iter().enumerate() {
                        let matches = self.matches_identity(loc, identity).await;
                        if matches {
                            target_position = Some(candidates.swap_remove(idx));
                            break;
                        }
                    }
                    processed_deletions.push(Self::build_processed_deletion(
                        deletion,
                        target_position.unwrap(),
                    ));
                }
                processed_deletions
            }
        }
    }

    #[inline]
    fn build_processed_deletion(
        deletion: &RawDeletionRecord,
        pos: RecordLocation,
    ) -> ProcessedDeletionRecord {
        ProcessedDeletionRecord {
            pos,
            lsn: deletion.lsn,
        }
    }

    /// Returns `true` if the location has already been marked deleted.
    fn is_deleted(&self, loc: &RecordLocation) -> bool {
        match loc {
            RecordLocation::MemoryBatch(batch_id, row_id) => self
                .batches
                .get(batch_id)
                .expect("missing batch")
                .deletions
                .is_deleted(*row_id),

            RecordLocation::DiskFile(file_id, row_id) => self
                .current_snapshot
                .disk_files
                .get(file_id)
                .expect("missing disk file")
                .batch_deletion_vector
                .is_deleted(*row_id),
        }
    }

    fn is_visible(loc: &RecordLocation, file_id_to_lsn: &HashMap<FileId, u64>, lsn: u64) -> bool {
        match loc {
            RecordLocation::MemoryBatch(_, _) => true,
            RecordLocation::DiskFile(file_id, _) => {
                file_id_to_lsn.get(file_id).is_none()
                    || file_id_to_lsn.get(file_id).unwrap() <= &lsn
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
            if deletion.lsn < task.new_commit_lsn {
                self.commit_deletion(deletion);
            } else {
                still_uncommitted.push(Some(deletion));
            }
        }

        self.uncommitted_deletion_log = still_uncommitted;
    }

    fn add_processed_deletion(
        &mut self,
        deletions: Vec<ProcessedDeletionRecord>,
        new_commit_lsn: u64,
    ) {
        for deletion in deletions.into_iter() {
            if deletion.lsn < new_commit_lsn {
                self.commit_deletion(deletion);
            } else {
                self.uncommitted_deletion_log.push(Some(deletion));
            }
        }
    }

    /// Convert raw deletions discovered by the snapshot task and either commit
    /// them or defer until their LSN becomes visible.
    async fn apply_new_deletions(&mut self, task: &mut SnapshotTask) {
        let mut new_deletions = take(&mut task.new_deletions);
        let mut already_processed = Vec::new();
        new_deletions.retain(|deletion| {
            if let Some(pos) = deletion.pos {
                already_processed.push(Self::build_processed_deletion(deletion, pos.into()));
                false
            } else {
                true
            }
        });
        self.add_processed_deletion(already_processed, task.new_commit_lsn);
        new_deletions.sort_by_key(|deletion| deletion.lookup_key);
        if new_deletions.is_empty() {
            return;
        }
        let mut index_lookup_result = self
            .current_snapshot
            .indices
            .find_records(&new_deletions)
            .await;
        index_lookup_result.sort_by_key(|(key, _)| *key);
        let mut i = 0;
        let mut j = 0;
        while i < new_deletions.len() {
            let start_i = i;
            while i < new_deletions.len()
                && new_deletions[i].lookup_key == new_deletions[start_i].lookup_key
                && new_deletions[i].lsn == new_deletions[start_i].lsn
            {
                i += 1;
            }
            let deletions = &new_deletions[start_i..i];
            let mut lookup_result = Vec::new();
            while index_lookup_result[j].0 != new_deletions[start_i].lookup_key {
                j += 1;
            }
            let mut j_end = j;
            while j_end < index_lookup_result.len()
                && index_lookup_result[j_end].0 == new_deletions[start_i].lookup_key
            {
                lookup_result.push(index_lookup_result[j_end].1.clone());
                j_end += 1;
            }
            let processed_deletions = self
                .match_deletions_with_identical_key_and_lsn(
                    deletions,
                    lookup_result,
                    &task.disk_file_lsn_map,
                )
                .await;
            self.add_processed_deletion(processed_deletions, task.new_commit_lsn);
        }
    }

    /// Get committed deletion record for current snapshot.
    async fn get_deletion_records(
        &mut self,
    ) -> (
        Vec<NonEvictableHandle>,       /*puffin file cache handles*/
        Vec<PuffinDeletionBlobAtRead>, /*deletion vector puffin*/
        Vec<(
            u32, /*index of disk file in snapshot*/
            u32, /*row id*/
        )>,
    ) {
        // Get puffin blobs for deletion vector.
        let mut puffin_cache_handles = vec![];
        let mut deletion_vector_blob_at_read = vec![];
        for (idx, (_, disk_deletion_vector)) in self.current_snapshot.disk_files.iter().enumerate()
        {
            if disk_deletion_vector.puffin_deletion_blob.is_none() {
                continue;
            }
            let puffin_deletion_blob = disk_deletion_vector.puffin_deletion_blob.as_ref().unwrap();

            // Add one more reference for puffin cache handle.
            // There'll be no IO operations during cache access, thus no failure or evicted files expected.
            let (new_puffin_cache_handle, cur_evicted) = self
                .object_storage_cache
                .get_cache_entry(
                    puffin_deletion_blob.puffin_file_cache_handle.file_id,
                    /*remote_filepath=*/ "",
                    /*filesystem_accessor*/ self.filesystem_accessor.as_ref(),
                )
                .await
                .unwrap();
            assert!(cur_evicted.is_empty());
            puffin_cache_handles.push(new_puffin_cache_handle.unwrap());

            let puffin_file_index = puffin_cache_handles.len() - 1;
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
        (puffin_cache_handles, deletion_vector_blob_at_read, ret)
    }

    /// Util function to get read state, which returns all current data files information.
    /// If a data file already has a pinned reference, increment the reference count directly to avoid unnecessary IO.
    async fn get_read_files_for_read(&mut self) -> Vec<DataFileForRead> {
        let mut data_files_for_read = Vec::with_capacity(self.current_snapshot.disk_files.len());
        for (file, _) in self.current_snapshot.disk_files.iter() {
            let unique_table_file_id = self.get_table_unique_file_id(file.file_id());
            data_files_for_read.push(DataFileForRead::RemoteFilePath((
                unique_table_file_id,
                file.file_path().to_string(),
            )));
        }

        data_files_for_read
    }

    pub(crate) async fn request_read(&mut self) -> Result<SnapshotReadOutput> {
        let mut data_file_paths = self.get_read_files_for_read().await;
        let mut associated_files = Vec::new();
        let (puffin_cache_handles, deletion_vectors_at_read, position_deletes) =
            self.get_deletion_records().await;

        // For committed but not persisted records, we create a temporary file for them, which gets deleted after query completion.
        let file_path = self.current_snapshot.get_name_for_inmemory_file();
        let filepath_exists = tokio::fs::try_exists(&file_path).await?;
        if filepath_exists {
            data_file_paths.push(DataFileForRead::TemporaryDataFile(
                file_path.to_string_lossy().to_string(),
            ));
            associated_files.push(file_path.to_string_lossy().to_string());
            return Ok(SnapshotReadOutput {
                data_file_paths,
                puffin_cache_handles,
                deletion_vectors: deletion_vectors_at_read,
                position_deletes,
                associated_files,
                object_storage_cache: Some(self.object_storage_cache.clone()),
                filesystem_accessor: Some(self.filesystem_accessor.clone()),
                table_notifier: Some(self.table_notify.as_ref().unwrap().clone()),
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

            // TODO(hjiang): Check whether we could avoid IO operation inside of critical section.
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
                data_file_paths.push(DataFileForRead::TemporaryDataFile(
                    file_path.to_string_lossy().to_string(),
                ));
                associated_files.push(file_path.to_string_lossy().to_string());
            }
        }
        Ok(SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            deletion_vectors: deletion_vectors_at_read,
            position_deletes,
            associated_files,
            object_storage_cache: Some(self.object_storage_cache.clone()),
            filesystem_accessor: Some(self.filesystem_accessor.clone()),
            table_notifier: Some(self.table_notify.as_ref().unwrap().clone()),
        })
    }
}
