use std::collections::HashSet;

/// This file contains maintenance related features for mooncake snapshot.
use crate::storage::compaction::table_compaction::SingleFileToCompact;
use crate::storage::mooncake_table::snapshot::SnapshotTableState;
use crate::storage::mooncake_table::{
    DataCompactionPayload, FileIndiceMergePayload, MaintenanceOption, SnapshotTask,
};
use crate::storage::storage_utils::{ProcessedDeletionRecord, TableId, TableUniqueFileId};
use crate::table_notify::{DataCompactionMaintenanceStatus, IndexMergeMaintenanceStatus};

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

impl SnapshotTableState {
    /// ===============================
    /// Get maintenance payload
    /// ===============================
    ///
    /// Util function to decide whether and what to compact data files.
    /// To simplify states (aka, avoid data compaction already in iceberg with those not), only merge those already persisted.
    #[allow(clippy::mutable_key_type)]
    pub(super) fn get_payload_to_compact(
        &self,
        data_compaction_option: &MaintenanceOption,
    ) -> DataCompactionMaintenanceStatus {
        if *data_compaction_option == MaintenanceOption::Skip {
            return DataCompactionMaintenanceStatus::Unknown;
        }

        let config = &self.mooncake_table_metadata.config.data_compaction_config;
        let (
            min_data_compaction_file_num_threshold,
            max_data_compaction_file_num_threshold,
            data_file_deletion_percentage_threshold,
            data_compaction_file_size_threshold,
        ) = match data_compaction_option {
            MaintenanceOption::Skip => (usize::MAX, usize::MAX, 100, 0),
            MaintenanceOption::ForceRegular => (
                2,
                config.max_data_file_to_compact as usize,
                config.data_file_deletion_percentage as usize,
                config.data_file_final_size as usize,
            ),
            MaintenanceOption::ForceFull => (2, usize::MAX, 1, usize::MAX),
            MaintenanceOption::BestEffort => (
                config.min_data_file_to_compact as usize,
                config.max_data_file_to_compact as usize,
                config.data_file_deletion_percentage as usize,
                config.data_file_final_size as usize,
            ),
        };

        // Fast-path: not enough data files to trigger compaction.
        let all_disk_files = &self.current_snapshot.disk_files;
        if all_disk_files.len() < min_data_compaction_file_num_threshold {
            return DataCompactionMaintenanceStatus::Nothing;
        }

        // To simplify state management, only compact data files which have been persisted into iceberg table.
        let unpersisted_data_files = self.unpersisted_records.get_unpersisted_data_files_set();
        let mut tentative_data_files_to_compact = HashSet::new();

        // Number of data files rejected to merge due to unpersistence.
        let mut reject_by_unpersistence = 0;

        // TODO(hjiang): We should be able to early exit, if left items are not enough to reach the compaction threshold.
        for (cur_data_file, disk_file_entry) in all_disk_files.iter() {
            // Doesn't compact those unpersisted files.
            if unpersisted_data_files.contains(cur_data_file) {
                reject_by_unpersistence += 1;
                continue;
            }

            // Skip compaction if the file size exceeds threshold, AND deleted rows are below config thresholds.
            if disk_file_entry.file_size >= data_compaction_file_size_threshold {
                // Compaction by deletion is skipped.
                if data_file_deletion_percentage_threshold == 0 {
                    continue;
                }
                let deletion_percentage =
                    disk_file_entry.batch_deletion_vector.get_num_rows_deleted() * 100
                        / disk_file_entry.num_rows;
                if deletion_percentage < data_file_deletion_percentage_threshold {
                    continue;
                }
            }

            // Break early if tentative data files to compact already reaches upper limit.
            if tentative_data_files_to_compact.len() >= max_data_compaction_file_num_threshold {
                break;
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
            assert!(tentative_data_files_to_compact.insert(single_file_to_compact));
        }

        if tentative_data_files_to_compact.len() < min_data_compaction_file_num_threshold {
            // There're two possibilities here:
            // 1. If due to unpersistence, data compaction should wait until persistence completion.
            if tentative_data_files_to_compact.len() + reject_by_unpersistence
                >= min_data_compaction_file_num_threshold
            {
                return DataCompactionMaintenanceStatus::Unknown;
            }
            // 2. There're not enough number of small data files to merge.
            else {
                return DataCompactionMaintenanceStatus::Nothing;
            }
        }

        // Calculate related file indices to compact.
        let mut file_indices_to_compact = HashSet::new();
        let file_ids_to_compact = tentative_data_files_to_compact
            .iter()
            .map(|single_file_to_compact| single_file_to_compact.file_id.file_id)
            .collect::<HashSet<_>>();
        for cur_file_index in self.current_snapshot.indices.file_indices.iter() {
            for cur_file in cur_file_index.files.iter() {
                if file_ids_to_compact.contains(&cur_file.file_id()) {
                    assert!(file_indices_to_compact.insert(cur_file_index.clone()));
                    break;
                }
            }
        }

        // Skip data files to compact if their corresponding file indices haven't been persisted.
        let mut file_indices_to_remove = vec![];
        let unpersisted_file_indices = self.unpersisted_records.get_unpersisted_file_indices_set();
        for cur_file_index in file_indices_to_compact.iter() {
            if unpersisted_file_indices.contains(cur_file_index) {
                file_indices_to_remove.push(cur_file_index.clone());
            }
        }
        for cur_file_index in file_indices_to_remove.iter() {
            for cur_data_file in cur_file_index.files.iter() {
                let table_unique_file_id = self.get_table_unique_file_id(cur_data_file.file_id());
                assert!(tentative_data_files_to_compact.remove(&table_unique_file_id));
                reject_by_unpersistence += 1;
            }
            assert!(file_indices_to_compact.remove(cur_file_index));
        }

        // Check again whether need to compact.
        if tentative_data_files_to_compact.len() < min_data_compaction_file_num_threshold {
            return DataCompactionMaintenanceStatus::Unknown;
        }

        let payload = DataCompactionPayload {
            uuid: uuid::Uuid::new_v4(),
            object_storage_cache: self.object_storage_cache.clone(),
            filesystem_accessor: self.filesystem_accessor.clone(),
            disk_files: tentative_data_files_to_compact
                .into_iter()
                .collect::<Vec<_>>(),
            file_indices: file_indices_to_compact.into_iter().collect::<Vec<_>>(),
        };

        #[cfg(any(test, debug_assertions))]
        {
            Self::validate_compaction_payload(&payload);
        }
        DataCompactionMaintenanceStatus::Payload(payload)
    }

    /// Util function to validate the consistency of data compaction payload.
    #[cfg(any(test, debug_assertions))]
    fn validate_compaction_payload(payload: &DataCompactionPayload) {
        // Data files to compact.
        let data_files = payload
            .disk_files
            .iter()
            .map(|f| f.file_id.file_id)
            .collect::<HashSet<_>>();

        // Data files indicated by file indices.
        let mut data_files_by_file_indices = HashSet::new();
        for cur_file_index in payload.file_indices.iter() {
            data_files_by_file_indices.extend(cur_file_index.files.iter().map(|f| f.file_id()));
        }

        assert_eq!(data_files, data_files_by_file_indices);
    }

    /// Util function to decide whether and what to merge index.
    /// To simplify states (aka, avoid merging file indices already in iceberg with those not), only merge those already persisted.
    #[allow(clippy::mutable_key_type)]
    pub(super) fn get_file_indices_to_merge(
        &self,
        index_merge_option: &MaintenanceOption,
    ) -> IndexMergeMaintenanceStatus {
        if *index_merge_option == MaintenanceOption::Skip {
            return IndexMergeMaintenanceStatus::Unknown;
        }
        let max_index_merge_file_num_threshold = self
            .mooncake_table_metadata
            .config
            .file_index_config
            .max_file_indices_to_merge as usize;
        let min_index_merge_file_num_threshold = match index_merge_option {
            MaintenanceOption::Skip => usize::MAX,
            MaintenanceOption::ForceRegular => 2,
            MaintenanceOption::ForceFull => 2,
            MaintenanceOption::BestEffort => {
                self.mooncake_table_metadata
                    .config
                    .file_index_config
                    .min_file_indices_to_merge as usize
            }
        };
        let default_final_file_size = self
            .mooncake_table_metadata
            .config
            .file_index_config
            .index_block_final_size;
        let index_merge_file_size_threshold = match index_merge_option {
            MaintenanceOption::Skip => u64::MAX,
            MaintenanceOption::ForceRegular => default_final_file_size,
            MaintenanceOption::ForceFull => 0,
            MaintenanceOption::BestEffort => default_final_file_size,
        };

        // Fast-path: not enough file indices to trigger index merge.
        let mut file_indices_to_merge = HashSet::new();
        let all_file_indices = &self.current_snapshot.indices.file_indices;
        if all_file_indices.len() < min_index_merge_file_num_threshold {
            return IndexMergeMaintenanceStatus::Nothing;
        }

        // To simplify state management, only compact data files which have been persisted into iceberg table.
        let unpersisted_file_indices = self.unpersisted_records.get_unpersisted_file_indices_set();

        // Number of index blocks rejected to merge due to unpersistence.
        let mut reject_by_unpersistence = 0;
        for cur_file_index in all_file_indices.iter() {
            if cur_file_index.get_index_blocks_size() >= index_merge_file_size_threshold {
                continue;
            }

            // Don't merge unpersisted file indices.
            if unpersisted_file_indices.contains(cur_file_index) {
                reject_by_unpersistence += 1;
                continue;
            }

            assert!(file_indices_to_merge.insert(cur_file_index.clone()));
        }

        // To avoid too many small IO operations, only attempt an index merge when accumulated small indices exceeds the threshold.
        if file_indices_to_merge.len() >= min_index_merge_file_num_threshold {
            let payload = FileIndiceMergePayload {
                uuid: uuid::Uuid::new_v4(),
                file_indices: file_indices_to_merge
                    .into_iter()
                    .take(max_index_merge_file_num_threshold)
                    .collect::<HashSet<_>>(),
            };
            return IndexMergeMaintenanceStatus::Payload(payload);
        }

        // There're two possibilities here:
        // 1. If due to unpersistence, index merge should wait until persistence completion.
        if file_indices_to_merge.len() + reject_by_unpersistence
            >= min_index_merge_file_num_threshold
        {
            return IndexMergeMaintenanceStatus::Unknown;
        }

        // 2. There're not enough number of small index blocks to merge.
        IndexMergeMaintenanceStatus::Nothing
    }

    /// ===============================
    /// Reflect maintenance result
    /// ===============================
    ///
    /// Reflect data compaction results to mooncake snapshot.
    /// Return evicted data files to delete due to data compaction.
    pub(super) async fn update_data_compaction_to_mooncake_snapshot(
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

    /// Return evicted files to delete.
    pub(super) async fn update_file_indices_merge_to_mooncake_snapshot(
        &mut self,
        task: &SnapshotTask,
    ) -> Vec<String> {
        self.update_file_indices_to_mooncake_snapshot_impl(
            task.index_merge_result.old_file_indices.clone(),
            task.index_merge_result.new_file_indices.clone(),
        )
        .await
    }

    /// Get remapped committed deletion log after compaction.
    pub(super) fn remap_committed_deletion_logs_after_compaction(
        &mut self,
        task: &mut SnapshotTask,
    ) {
        // No need to remap if no compaction happening.
        if task.data_compaction_result.is_empty() {
            return;
        }

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
                //
                // Committed deletion log only contains unpersisted records, so remap should succeed.
                let remap_succ =
                    remap_record_location_after_compaction(&mut cur_deletion_log, task);
                assert!(
                    remap_succ,
                    "Deletion log {cur_deletion_log:?} fails to remap"
                );
                new_committed_deletion_log.push(cur_deletion_log);
                continue;
            } else {
                // Keep deletion record for in-memory batches.
                new_committed_deletion_log.push(cur_deletion_log);
            }
        }
        self.committed_deletion_log = new_committed_deletion_log;
    }

    /// Remap uncommitted deletion log after compaction.
    pub(super) fn remap_uncommitted_deletion_logs_after_compaction(
        &mut self,
        task: &mut SnapshotTask,
    ) {
        // No need to remap if no compaction happening.
        if task.data_compaction_result.is_empty() {
            return;
        }
        for cur_deletion_log in &mut self.uncommitted_deletion_log {
            if cur_deletion_log.is_some() {
                remap_record_location_after_compaction(cur_deletion_log.as_mut().unwrap(), task);
            }
        }
    }
}
