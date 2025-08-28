use super::data_batches::InMemoryBatch;
use super::delete_vector::BatchDeletionVector;
use super::{
    DiskFileEntry, IcebergSnapshotPayload, Snapshot, SnapshotTask,
    TableMetadata as MooncakeTableMetadata,
};
use crate::error::Result;
use crate::storage::cache::object_storage::base_cache::{
    CacheEntry as DataFileCacheEntry, CacheTrait, FileMetadata,
};
use crate::storage::compaction::table_compaction::{CompactedDataEntry, RemappedRecordLocation};
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::index::{cache_utils as index_cache_utils, FileIndex};
use crate::storage::mooncake_table::persistence_buffer::UnpersistedRecords;
use crate::storage::mooncake_table::shared_array::SharedRowBufferSnapshot;
use crate::storage::mooncake_table::BatchIdCounter;
use crate::storage::mooncake_table::MoonlinkRow;
use crate::storage::mooncake_table::SnapshotOption;
use crate::storage::snapshot_options::IcebergSnapshotOption;
use crate::storage::storage_utils::{FileId, TableId, TableUniqueFileId};
use crate::storage::storage_utils::{
    MooncakeDataFileRef, ProcessedDeletionRecord, RawDeletionRecord, RecordLocation,
};
use crate::table_notify::{
    DataCompactionMaintenanceStatus, IndexMergeMaintenanceStatus, TableEvent,
};
use more_asserts as ma;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem::take;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
pub(crate) struct SnapshotTableState {
    /// Iceberg warehouse location.
    pub(super) iceberg_warehouse_location: String,

    /// Mooncake table metadata.
    pub(super) mooncake_table_metadata: Arc<MooncakeTableMetadata>,

    /// Current snapshot
    pub(super) current_snapshot: Snapshot,

    /// In memory RecordBatches, maps from batch id to in-memory batch.
    pub(super) batches: BTreeMap<u64, InMemoryBatch>,

    /// Latest rows
    pub(super) rows: Option<SharedRowBufferSnapshot>,

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
    //
    // Invariant: all processed deletion records are valid, here we use `Option` simply for an easy way to `move` the record out.
    pub(crate) uncommitted_deletion_log: Vec<Option<ProcessedDeletionRecord>>,

    /// Last commit point
    pub(super) last_commit: RecordLocation,

    /// Object storage cache.
    pub(super) object_storage_cache: Arc<dyn CacheTrait>,

    /// Filesystem accessor.
    pub(super) filesystem_accessor: Arc<dyn BaseFileSystemAccess>,

    /// Table notifier.
    pub(super) table_notify: Option<Sender<TableEvent>>,

    /// ---- Items not persisted to iceberg snapshot ----
    ///
    /// Iceberg snapshot is created in an async style, which means it doesn't correspond 1-1 to mooncake snapshot, so we need to ensure idempotency for iceberg snapshot payload.
    /// The following fields record unpersisted content, which will be placed in iceberg payload everytime.
    pub(super) unpersisted_records: UnpersistedRecords,

    /// Batch ID counter for non-streaming operations
    pub(super) non_streaming_batch_id_counter: Arc<BatchIdCounter>,
}

#[derive(Clone)]
pub struct MooncakeSnapshotOutput {
    /// Table event id.
    pub(crate) uuid: uuid::Uuid,
    /// Committed LSN for mooncake snapshot.
    pub(crate) commit_lsn: u64,
    /// Iceberg snapshot payload.
    pub(crate) iceberg_snapshot_payload: Option<IcebergSnapshotPayload>,
    /// File indice merge payload.
    pub(crate) file_indices_merge_payload: IndexMergeMaintenanceStatus,
    /// Data compaction payload.
    pub(crate) data_compaction_payload: DataCompactionMaintenanceStatus,
    /// Evicted local data cache files to delete.
    pub(crate) evicted_data_files_to_delete: Vec<String>,
    /// Optional mooncake snapshot dump.
    pub(crate) current_snapshot: Option<Snapshot>,
}

impl std::fmt::Debug for MooncakeSnapshotOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MooncakeSnapshotOutput")
            .field("uuid", &self.uuid)
            .field("commit", &self.commit_lsn)
            .field("iceberg_snapshot_payload", &self.iceberg_snapshot_payload)
            .field(
                "file_indices_merge_payload",
                &self.file_indices_merge_payload,
            )
            .field("data_compaction_payload", &self.data_compaction_payload)
            .field(
                "evicted data files count",
                &self.evicted_data_files_to_delete.len(),
            )
            .field("current_snapshot", &self.current_snapshot)
            .finish()
    }
}

/// Committed deletion record to persist.
pub(super) struct CommittedDeletionToPersist {
    /// Commit deletion log included in the current iceberg persistence operation, which is used to prune snapshot deletion record after persistence finished.
    pub(super) committed_deletion_logs: HashSet<(FileId, usize /*row idx*/)>,
    /// Maps from data file to its changed deletion vector (which only contains newly deleted rows).
    pub(super) new_deletions_to_persist: HashMap<MooncakeDataFileRef, BatchDeletionVector>,
}

impl CommittedDeletionToPersist {
    /// Validate it's valid.
    fn validate(&self) {
        let len1 = self.committed_deletion_logs.len();
        let len2 = self.new_deletions_to_persist.len();
        ma::assert_ge!(len1, len2);
    }
}

impl SnapshotTableState {
    pub(super) async fn new(
        iceberg_warehouse_location: String,
        metadata: Arc<MooncakeTableMetadata>,
        object_storage_cache: Arc<dyn CacheTrait>,
        filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
        current_snapshot: Snapshot,
        non_streaming_batch_id_counter: Arc<BatchIdCounter>,
    ) -> Result<Self> {
        let mut batches = BTreeMap::new();
        // Properly load a batch ID from the counter to ensure correspondence with MemSlice.
        let initial_batch_id = non_streaming_batch_id_counter.load();
        batches.insert(
            initial_batch_id,
            InMemoryBatch::new(metadata.config.batch_size),
        );

        let table_config = metadata.config.clone();
        Ok(Self {
            iceberg_warehouse_location,
            mooncake_table_metadata: metadata.clone(),
            current_snapshot,
            batches,
            rows: None,
            last_commit: RecordLocation::MemoryBatch(initial_batch_id, 0),
            object_storage_cache,
            filesystem_accessor,
            table_notify: None,
            committed_deletion_log: Vec::new(),
            uncommitted_deletion_log: Vec::new(),
            unpersisted_records: UnpersistedRecords::new(table_config),
            non_streaming_batch_id_counter,
        })
    }

    pub(crate) fn reset_for_alter(&mut self, new_metadata: Arc<MooncakeTableMetadata>) {
        self.batches = BTreeMap::new();
        // Initialize with a proper batch ID from the counter to avoid collisions
        let initial_batch_id = self.non_streaming_batch_id_counter.load();
        assert!(self
            .batches
            .insert(
                initial_batch_id,
                InMemoryBatch::new(new_metadata.config.batch_size)
            )
            .is_none());
        self.rows = None;
        self.last_commit = RecordLocation::MemoryBatch(initial_batch_id, 0);
        self.current_snapshot.metadata = new_metadata.clone();
        self.mooncake_table_metadata = new_metadata;
    }
    /// Util function to get table unique file id.
    pub(super) fn get_table_unique_file_id(&self, file_id: FileId) -> TableUniqueFileId {
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
    /// Return committed deletion records to delete.
    ///
    /// Precondition: all disk files have been integrated into snapshot.
    fn aggregate_committed_deletion_logs(&self, flush_lsn: u64) -> CommittedDeletionToPersist {
        let mut new_deletions_to_persist = HashMap::new();
        let mut committed_deletion_logs = HashSet::new();

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

                let deletion_vector = new_deletions_to_persist
                    .entry(cur_data_file)
                    .or_insert_with(|| BatchDeletionVector::new(max_rows));
                assert!(deletion_vector.delete_row(*row_idx));
                assert!(committed_deletion_logs.insert((*file_id, *row_idx)));
            }
        }

        CommittedDeletionToPersist {
            committed_deletion_logs,
            new_deletions_to_persist,
        }
    }

    /// Prune committed deletion logs for the given persisted records.
    fn prune_committed_deletion_logs(&mut self, task: &SnapshotTask) {
        // No iceberg snapshot persisted between two mooncake snapshot.
        if task.iceberg_persisted_records.flush_lsn.is_none() {
            return;
        }

        // Committed deletion logs, which have been persisted into iceberg.
        let committed_deletion_logs = &task.committed_deletion_logs;

        // Keep two types of committed logs: (1) in-memory committed deletion logs; (2) commit point after flush LSN.
        // All on-disk committed deletion logs, which are <= iceberg snapshot flush LSN could be pruned.
        let mut new_committed_deletion_log = vec![];

        let old_committed_deletion_logs = std::mem::take(&mut self.committed_deletion_log);
        for cur_deletion_log in old_committed_deletion_logs.into_iter() {
            ma::assert_le!(cur_deletion_log.lsn, self.current_snapshot.snapshot_version);
            match &cur_deletion_log.pos {
                // Keep memory batch based committed deletion logs.
                RecordLocation::MemoryBatch(_, _) => {
                    new_committed_deletion_log.push(cur_deletion_log);
                }
                // Check whether committed deletion logs have been persisted, and prune if persisted.
                RecordLocation::DiskFile(file_id, row_idx) => {
                    if !committed_deletion_logs.contains(&(*file_id, *row_idx)) {
                        new_committed_deletion_log.push(cur_deletion_log);
                    }
                }
            }
        }

        self.committed_deletion_log = new_committed_deletion_log;
    }

    /// Update current snapshot's file indices by adding and removing a few.
    #[allow(clippy::mutable_key_type)]
    pub(super) async fn update_file_indices_to_mooncake_snapshot_impl(
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
    pub(super) async fn update_data_files_to_mooncake_snapshot_impl(
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
                    num_rows: cur_entry.num_rows,
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
                    assert!(new_deletion_entry
                        .batch_deletion_vector
                        .delete_row(new_record_location.record_location.get_row_idx()));
                }
                // Case-2: The old record has already been compacted, directly skip.
            }
        }

        evicted_files_to_delete
    }

    fn get_expected_disk_files_count(&self, task: &SnapshotTask) -> usize {
        self.current_snapshot.disk_files.len()
            + task.new_disk_slices.iter().map(|cur_disk_slice| cur_disk_slice.output_files().len()).sum::<usize>() // new persisted files by non-streaming committed transactions
            + task.new_streaming_xact.iter().map(|cur_txn| cur_txn.get_committed_persisted_disk_count()).sum::<usize>() // new persisted files files by streaming committed transactions
            - task.data_compaction_result.old_data_files.len() // old data files before compaction
            + task.data_compaction_result.new_data_files.len() // new data files after compaction
    }
    fn get_expected_file_indices_count(&self, task: &SnapshotTask) -> usize {
        self.current_snapshot.indices.file_indices.len()
            + task.get_new_file_indices().len() // committed file indices, including streaming and non-streaming
            - task.index_merge_result.old_file_indices.len() // old file indices before index merge
            + task.index_merge_result.new_file_indices.len() // new file indices after index merge
            - task.data_compaction_result.old_file_indices.len() // old file indices before index merge
            + task.data_compaction_result.new_file_indices.len() // new file indices after index merge
    }

    pub(super) async fn update_snapshot(
        &mut self,
        mut task: SnapshotTask,
        opt: SnapshotOption,
    ) -> MooncakeSnapshotOutput {
        // Validate mooncake table operation invariants.
        self.validate_mooncake_table_invariants(&task, &opt);
        // Validate persistence results.
        task.iceberg_persisted_records
            .validate_imported_files_remote(&self.iceberg_warehouse_location);

        // Calculate the expected disk files number after current snapshot update.
        let expected_disk_files_count = self.get_expected_disk_files_count(&task);
        // Calculate the expected file indices number after current snapshot update.
        let expected_file_indices_count = self.get_expected_file_indices_count(&task);

        // All evicted data files by the object storage cache.
        let mut evicted_data_files_to_delete = vec![];

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

        // Remap deletion logs.
        self.remap_uncommitted_deletion_logs_after_compaction(&mut task);
        self.remap_committed_deletion_logs_after_compaction(&mut task);

        // Prune unpersisted records.
        //
        // Precondition: All remapping for old committed deletion logs should finish beforehand.
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

        // After data compaction and index merge changes have been applied to snapshot, processed deletion record will point to the new record location.
        self.rows = take(&mut task.new_rows);
        self.process_deletion_log(&mut task).await;

        // Assert and update flush LSN.
        if let Some(new_flush_lsn) = task.new_flush_lsn {
            // Assert flush LSN doesn't regress, if not force snapshot.
            if self.current_snapshot.flush_lsn.is_some() && !opt.force_create {
                ma::assert_lt!(self.current_snapshot.flush_lsn.unwrap(), new_flush_lsn);
            }
            // Update flush LSN.
            self.current_snapshot.flush_lsn = Some(new_flush_lsn);
        }

        if task.commit_lsn_baseline != 0 {
            self.current_snapshot.snapshot_version = task.commit_lsn_baseline;
        }
        if let Some(cp) = task.new_commit_point {
            self.last_commit = cp;
        }

        // Till this point, committed changes have been reflected to current snapshot; sync the latest change to iceberg.
        // To reduce iceberg persistence overhead, there're certain cases an iceberg snapshot will be triggered:
        // (1) there're persisted data files
        // or (2) accumulated unflushed deletion vector exceeds threshold
        // or (3) there're unpersisted table maintenance results
        // or (4) there's pending table schema update
        let mut iceberg_snapshot_payload: Option<IcebergSnapshotPayload> = None;
        let flush_by_deletion = self.create_iceberg_snapshot_by_committed_logs(opt.force_create);
        let flush_by_new_files_or_maintenance = self
            .unpersisted_records
            .if_persist_by_new_files_or_maintenance(opt.force_create);
        let force_empty_iceberg_payload = task.force_empty_iceberg_payload;

        // Decide whether to perform a data compaction.
        //
        // No need to pin puffin file during compaction:
        // - only compaction deletes puffin file
        // - there's no two ongoing compaction
        let data_compaction_payload = self.get_payload_to_compact(&opt.data_compaction_option);

        // Decide whether to merge an index merge, which cannot be performed together with data compaction.
        let mut file_indices_merge_payload = IndexMergeMaintenanceStatus::Unknown;
        if !data_compaction_payload.has_payload() {
            file_indices_merge_payload = self.get_file_indices_to_merge(&opt.index_merge_option);
        }

        let flush_by_table_write = self.current_snapshot.flush_lsn.is_some()
            && (flush_by_new_files_or_maintenance || flush_by_deletion);

        // TODO(hjiang): When there's only schema evolution, we should also flush even no flush.
        let flush_lsn = self.current_snapshot.flush_lsn.unwrap_or(0);
        if opt.iceberg_snapshot_option != IcebergSnapshotOption::Skip
            && (force_empty_iceberg_payload || flush_by_table_write)
            && flush_lsn < task.min_ongoing_flush_lsn
        {
            // Getting persistable committed deletion logs is not cheap, which requires iterating through all logs,
            // so we only aggregate when there's committed deletion.
            let committed_deletion_logs = self.aggregate_committed_deletion_logs(flush_lsn);
            committed_deletion_logs.validate();

            // Only create iceberg snapshot when there's something to import.
            if !committed_deletion_logs.new_deletions_to_persist.is_empty()
                || flush_by_new_files_or_maintenance
                || force_empty_iceberg_payload
            {
                iceberg_snapshot_payload = Some(self.get_iceberg_snapshot_payload(
                    &opt.iceberg_snapshot_option,
                    flush_lsn,
                    committed_deletion_logs,
                ));
            }
        }

        // Validate disk files count is as expected.
        let actual_disk_files_count = self.current_snapshot.disk_files.len();
        assert_eq!(expected_disk_files_count, actual_disk_files_count);
        // Validate file indices count is as expected.
        let actual_file_indices_count = self.current_snapshot.indices.file_indices.len();
        assert_eq!(expected_file_indices_count, actual_file_indices_count);

        // Expensive assertion, which is only enabled in unit tests.
        #[cfg(any(test, debug_assertions))]
        {
            self.assert_current_snapshot_consistent().await;
        }

        MooncakeSnapshotOutput {
            uuid: opt.uuid,
            commit_lsn: self.current_snapshot.snapshot_version,
            iceberg_snapshot_payload,
            data_compaction_payload,
            file_indices_merge_payload,
            evicted_data_files_to_delete,
            current_snapshot: opt.dump_snapshot.then(|| self.current_snapshot.clone()),
        }
    }

    fn merge_mem_indices(&mut self, task: &mut SnapshotTask) {
        for idx in take(&mut task.new_mem_indices) {
            self.current_snapshot.indices.insert_memory_index(idx);
        }
    }

    fn finalize_batches(&mut self, task: &mut SnapshotTask) {
        let incoming = take(&mut task.new_record_batches);
        let (streaming_batches, mut non_streaming_batches): (Vec<_>, Vec<_>) = incoming
            .into_iter()
            .partition(|batch| batch.batch_id < (1u64 << 63));

        if !non_streaming_batches.is_empty() {
            // close previously‐open batch
            assert!(self.batches.values().last().unwrap().data.is_none());
            // Use the ID from the incoming batches rather than the counter, since the counter may have been further advanced elsewhere.
            let next_id = non_streaming_batches.last().unwrap().batch_id + 1;
            self.batches.last_entry().unwrap().get_mut().data =
                Some(non_streaming_batches.remove(0).record_batch.clone());

            // start a fresh empty batch after the newest data
            let batch_size = self.current_snapshot.metadata.config.batch_size;

            // Add to batch and assert that the batch is not already in the map.
            assert!(self
                .batches
                .insert(next_id, InMemoryBatch::new(batch_size))
                .is_none());
        }

        // Add completed batches
        // Assert that no incoming batch ID is already present in the map.
        for batch in streaming_batches
            .into_iter()
            .chain(non_streaming_batches.into_iter())
        {
            let deletions = match batch.deletion_vector {
                Some(dv) => dv, // Use the deletion vector from streaming transaction
                None => BatchDeletionVector::new(batch.record_batch.num_rows()), // Create fresh deletion vector for non-streaming
            };

            assert!(
                self.batches
                    .insert(
                        batch.batch_id,
                        InMemoryBatch {
                            data: Some(batch.record_batch.clone()),
                            deletions,
                        }
                    )
                    .is_none(),
                "Batch ID {} already exists in self.batches",
                batch.batch_id
            );
        }
    }

    /// Return files evicted from object storage cache.
    async fn integrate_disk_slices(&mut self, task: &mut SnapshotTask) -> Vec<String> {
        // Aggregate evicted data cache files to delete.
        let mut evicted_files = vec![];

        for mut slice in take(&mut task.new_disk_slices) {
            let write_lsn = slice
                .lsn()
                .expect("committed datafile should have a valid LSN");

            // Register new files into mooncake snapshot, add it into cache, and record LSN map.
            for (file, file_attrs) in slice.output_files().iter() {
                ma::assert_gt!(file_attrs.file_size, 0);
                assert!(task
                    .new_disk_file_lsn_map
                    .insert(file.file_id(), write_lsn)
                    .is_none());
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
                assert!(self
                    .current_snapshot
                    .disk_files
                    .insert(
                        file.clone(),
                        DiskFileEntry {
                            num_rows: file_attrs.row_num,
                            file_size: file_attrs.file_size,
                            cache_handle: Some(cache_handle),
                            batch_deletion_vector: BatchDeletionVector::new(file_attrs.row_num),
                            puffin_deletion_blob: None,
                        },
                    )
                    .is_none());
            }

            // If a committed deletion still points to a MemoryBatch that is being flushed
            // in this slice, remap it to the corresponding DiskFile location and apply it.
            for deletion in self.committed_deletion_log.iter_mut() {
                if let Some(RecordLocation::DiskFile(file_id, row_idx)) =
                    slice.remap_deletion_if_needed(deletion)
                {
                    // Find the disk file entry and apply the deletion
                    // Precondition: all new data files have been reflected to disk file map
                    for (file_ref, disk_entry) in self.current_snapshot.disk_files.iter_mut() {
                        if file_ref.file_id() == file_id {
                            assert!(disk_entry.batch_deletion_vector.delete_row(row_idx));
                            break;
                        }
                    }
                }
            }

            self.uncommitted_deletion_log
                .iter_mut()
                .flatten()
                .for_each(|d| {
                    slice.remap_deletion_if_needed(d);
                });

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
                // Remove from batch and assert that the batch is in the map.
                assert!(self.batches.remove(&b.id).is_some());
            });
        }

        evicted_files
    }

    async fn match_deletions_with_identical_key_and_lsn(
        &self,
        deletions: &[RawDeletionRecord],
        index_lookup_result: Vec<RecordLocation>,
        file_id_to_lsn: &HashMap<FileId, u64>,
        batch_id_to_lsn: &HashMap<u64, u64>,
    ) -> Vec<ProcessedDeletionRecord> {
        let mut candidates: Vec<RecordLocation> = index_lookup_result
            .into_iter()
            .filter(|loc| {
                !self.is_deleted(loc)
                    && Self::is_visible(
                        loc,
                        file_id_to_lsn,
                        batch_id_to_lsn,
                        deletions.first().unwrap().lsn,
                    )
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
                panic!("find less than expected candidates to deletions {deletions:?}, candidates: {candidates:?}, batch_id_to_lsn: {batch_id_to_lsn:?}, file_id_to_lsn: {file_id_to_lsn:?}")
            }
            Ordering::Greater => {
                let mut processed_deletions = Vec::new();
                // multiple candidates → disambiguate via full row identity comparison.
                for deletion in deletions.iter() {
                    assert!(deletion.row_identity.is_some(), "deletion: {deletion:?}, candidates: {candidates:?}, batch_id_to_lsn: {batch_id_to_lsn:?}, file_id_to_lsn: {file_id_to_lsn:?}");
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

    fn is_visible(
        loc: &RecordLocation,
        file_id_to_lsn: &HashMap<FileId, u64>,
        batch_id_to_lsn: &HashMap<u64, u64>,
        lsn: u64,
    ) -> bool {
        match loc {
            RecordLocation::MemoryBatch(batch_id, _) => {
                batch_id_to_lsn.get(batch_id).is_none()
                    || batch_id_to_lsn.get(batch_id).unwrap() <= &lsn
            }
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
                    &self.current_snapshot.metadata.config.row_identity,
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
                        &self.current_snapshot.metadata.config.row_identity,
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
            if deletion.lsn < task.commit_lsn_baseline {
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
        self.add_processed_deletion(already_processed, task.commit_lsn_baseline);
        new_deletions.sort_by(|a, b| a.lookup_key.cmp(&b.lookup_key).then(a.lsn.cmp(&b.lsn)));
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
                    &task.new_disk_file_lsn_map,
                    &task.flushing_batch_lsn_map,
                )
                .await;
            self.add_processed_deletion(processed_deletions, task.commit_lsn_baseline);
        }
    }
}
