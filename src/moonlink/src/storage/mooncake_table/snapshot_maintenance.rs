use std::collections::{HashMap, HashSet};

/// This file contains maintenance related features for mooncake snapshot.
use crate::storage::compaction::size_tiering::{pick_similar_sized, SizedCandidate};
use crate::storage::compaction::table_compaction::SingleFileToCompact;
use crate::storage::index::FileIndex;
use crate::storage::mooncake_table::snapshot::SnapshotTableState;
use crate::storage::mooncake_table::{
    DataCompactionPayload, FileIndiceMergePayload, MaintenanceOption, SnapshotTask,
};
use crate::storage::storage_utils::{
    FileId, MooncakeDataFileRef, ProcessedDeletionRecord, TableId, TableUniqueFileId,
};
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
            event_id,
            min_data_compaction_file_num_threshold,
            max_data_compaction_file_num_threshold,
            data_file_deletion_percentage_threshold,
            data_compaction_file_size_threshold,
        ) = match data_compaction_option {
            MaintenanceOption::Skip => (None, usize::MAX, usize::MAX, 100, 0),
            MaintenanceOption::ForceRegular(event_id) => (
                Some(event_id),
                2,
                config.max_data_file_to_compact as usize,
                config.data_file_deletion_percentage as usize,
                config.data_file_final_size as usize,
            ),
            MaintenanceOption::ForceFull(event_id) => {
                (Some(event_id), 2, usize::MAX, 1, usize::MAX)
            }
            MaintenanceOption::BestEffort(event_id) => (
                Some(event_id),
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

        // Compaction rewrites whole file indices, so data files sharing an index are selected
        // together or not at all; otherwise the rows of the unselected files would lose their
        // index entries.
        let unpersisted_data_files = self.unpersisted_records.get_unpersisted_data_files_set();
        let unpersisted_file_indices = self.unpersisted_records.get_unpersisted_file_indices_set();
        let mut reject_by_unpersistence = 0;
        let mut never_compacted_files = 0;
        let mut eligible_groups = Vec::new();
        for group in self.build_compaction_groups() {
            // To simplify state management, only compact data files which have been persisted into iceberg table.
            let unpersisted = group
                .files
                .iter()
                .any(|f| unpersisted_data_files.contains(&f.data_file))
                || group
                    .file_indices
                    .iter()
                    .any(|i| unpersisted_file_indices.contains(i));
            if unpersisted {
                reject_by_unpersistence += group.files.len();
                continue;
            }
            // Every file must be small enough, or deleted enough, to be worth rewriting.
            let all_files_eligible = group.files.iter().all(|f| {
                f.file_size < data_compaction_file_size_threshold
                    || f.is_deletion_heavy(data_file_deletion_percentage_threshold)
            });
            if all_files_eligible {
                if group.files.len() > max_data_compaction_file_num_threshold {
                    never_compacted_files += group.files.len();
                }
                eligible_groups.push(group);
            } else if group.files.len() > 1 {
                // Small files sharing an index with a large one are only compacted together.
                never_compacted_files += group.files.len();
            }
        }
        if never_compacted_files > 0
            && !self
                .logged_uncompactable_groups
                .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            tracing::info!(
                table = self.mooncake_table_metadata.mooncake_table_id,
                files = never_compacted_files,
                "some data files share a file index with files too large to rewrite together, so data compaction skips them"
            );
        }

        // Background compaction only merges groups of similar size, so earlier outputs are not
        // rewritten again with every few new small files; explicit requests merge everything
        // eligible.
        let chosen_groups = if matches!(data_compaction_option, MaintenanceOption::BestEffort(_)) {
            choose_similar_sized_compaction_groups(
                eligible_groups,
                min_data_compaction_file_num_threshold,
                max_data_compaction_file_num_threshold,
                data_file_deletion_percentage_threshold,
            )
        } else {
            choose_compaction_groups(
                eligible_groups,
                min_data_compaction_file_num_threshold,
                max_data_compaction_file_num_threshold,
            )
        };
        let chosen_file_count: usize = chosen_groups.iter().map(|g| g.files.len()).sum();
        if chosen_file_count < min_data_compaction_file_num_threshold {
            // There're two possibilities here:
            // 1. If due to unpersistence, data compaction should wait until persistence completion.
            if chosen_file_count + reject_by_unpersistence >= min_data_compaction_file_num_threshold
            {
                return DataCompactionMaintenanceStatus::Unknown;
            }
            // 2. There're not enough number of small data files to merge.
            return DataCompactionMaintenanceStatus::Nothing;
        }

        let mut tentative_data_files_to_compact = HashSet::new();
        let mut file_indices_to_compact = HashSet::new();
        for group in chosen_groups {
            for f in group.files {
                let disk_file_entry = &all_disk_files[&f.data_file];
                assert!(tentative_data_files_to_compact.insert(SingleFileToCompact {
                    file_id: TableUniqueFileId {
                        table_id: TableId(self.mooncake_table_metadata.table_id),
                        file_id: f.data_file.file_id(),
                    },
                    data_file_cache_handle: disk_file_entry.cache_handle.clone(),
                    filepath: f.data_file.file_path().to_string(),
                    deletion_vector: disk_file_entry.puffin_deletion_blob.clone(),
                }));
            }
            file_indices_to_compact.extend(group.file_indices);
        }

        let payload = DataCompactionPayload {
            uuid: *event_id.unwrap(),
            object_storage_cache: self.object_storage_cache.clone(),
            filesystem_accessor: self.filesystem_accessor.clone(),
            disk_files: tentative_data_files_to_compact
                .into_iter()
                .collect::<Vec<_>>(),
            file_indices: file_indices_to_compact.into_iter().collect::<Vec<_>>(),
        };

        if !self.compaction_payload_is_consistent(&payload) {
            tracing::error!(
                table = self.mooncake_table_metadata.mooncake_table_id,
                "skipping data compaction: selected data files do not match their file indices"
            );
            return DataCompactionMaintenanceStatus::Nothing;
        }
        DataCompactionMaintenanceStatus::Payload(payload)
    }

    /// Partition data files into groups connected by shared file indices.
    fn build_compaction_groups(&self) -> Vec<CompactionGroup> {
        let disk_files = &self.current_snapshot.disk_files;
        let files: Vec<&MooncakeDataFileRef> = disk_files.keys().collect();
        let position: HashMap<FileId, usize> = files
            .iter()
            .enumerate()
            .map(|(i, f)| (f.file_id(), i))
            .collect();
        let mut parent: Vec<usize> = (0..files.len()).collect();
        fn find(parent: &mut [usize], mut x: usize) -> usize {
            while parent[x] != x {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            x
        }
        let file_indices = &self.current_snapshot.indices.file_indices;
        for index in file_indices.iter() {
            let members: Vec<usize> = index
                .files
                .iter()
                .filter_map(|f| position.get(&f.file_id()).copied())
                .collect();
            for pair in members.windows(2) {
                let (a, b) = (find(&mut parent, pair[0]), find(&mut parent, pair[1]));
                parent[a] = b;
            }
        }
        let mut groups: HashMap<usize, CompactionGroup> = HashMap::new();
        for (i, f) in files.iter().enumerate() {
            let entry = &disk_files[*f];
            let deletion_percentage = if entry.num_rows == 0 {
                0
            } else {
                entry.committed_deletion_vector.get_num_rows_deleted() * 100 / entry.num_rows
            };
            groups
                .entry(find(&mut parent, i))
                .or_default()
                .files
                .push(CompactionGroupFile {
                    data_file: (*f).clone(),
                    file_size: entry.file_size,
                    deletion_percentage,
                });
        }
        for index in file_indices.iter() {
            if let Some(first) = index
                .files
                .iter()
                .find_map(|f| position.get(&f.file_id()).copied())
            {
                if let Some(group) = groups.get_mut(&find(&mut parent, first)) {
                    group.file_indices.push(index.clone());
                }
            }
        }
        groups.into_values().collect()
    }

    /// Whether the payload's data files are exactly the files covered by its file indices.
    fn compaction_payload_is_consistent(&self, payload: &DataCompactionPayload) -> bool {
        if self.mooncake_table_metadata.config.append_only {
            return true;
        }

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

        data_files == data_files_by_file_indices
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
        let default_final_file_size = self
            .mooncake_table_metadata
            .config
            .file_index_config
            .index_block_final_size;
        let (event_id, min_index_merge_file_num_threshold, index_merge_file_size_threshold) =
            match index_merge_option {
                MaintenanceOption::Skip => (None, usize::MAX, u64::MAX),
                MaintenanceOption::ForceRegular(event_id) => {
                    (Some(event_id), 2, default_final_file_size)
                }
                MaintenanceOption::ForceFull(event_id) => (Some(event_id), 2, 0),
                MaintenanceOption::BestEffort(event_id) => (
                    Some(event_id),
                    self.mooncake_table_metadata
                        .config
                        .file_index_config
                        .min_file_indices_to_merge as usize,
                    default_final_file_size,
                ),
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
        // Merging the index of a large data file with those of small files would tie the small
        // files to it: compaction only takes data files sharing an index together, so they could
        // no longer be compacted.
        let large_data_file_size = self
            .mooncake_table_metadata
            .config
            .data_compaction_config
            .data_file_final_size;
        let skip_large_files = !matches!(index_merge_option, MaintenanceOption::ForceFull(_));
        for cur_file_index in all_file_indices.iter() {
            if cur_file_index.get_index_blocks_size() >= index_merge_file_size_threshold {
                continue;
            }
            if skip_large_files
                && cur_file_index.files.iter().any(|f| {
                    self.current_snapshot
                        .disk_files
                        .get(f)
                        .map(|e| e.file_size as u64 >= large_data_file_size)
                        .unwrap_or(false)
                })
            {
                continue;
            }

            // Don't merge unpersisted file indices.
            if unpersisted_file_indices.contains(cur_file_index) {
                reject_by_unpersistence += 1;
                continue;
            }

            assert!(file_indices_to_merge.insert(cur_file_index.clone()));
        }

        // Background index merges only combine indices of similar size, so a merged index is not
        // rewritten again with every few new small ones.
        if matches!(index_merge_option, MaintenanceOption::BestEffort(_)) {
            let candidates = file_indices_to_merge
                .into_iter()
                .map(|file_index| SizedCandidate {
                    bytes: file_index.get_index_blocks_size(),
                    weight: 1,
                    item: file_index,
                })
                .collect();
            file_indices_to_merge = pick_similar_sized(
                candidates,
                min_index_merge_file_num_threshold,
                max_index_merge_file_num_threshold,
            )
            .into_iter()
            .collect();
        }

        // To avoid too many small IO operations, only attempt an index merge when accumulated small indices exceeds the threshold.
        if file_indices_to_merge.len() >= min_index_merge_file_num_threshold {
            let payload = FileIndiceMergePayload {
                uuid: *event_id.unwrap(),
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

/// One data file of a [`CompactionGroup`].
#[derive(Clone, Debug)]
pub(crate) struct CompactionGroupFile {
    pub(crate) data_file: MooncakeDataFileRef,
    pub(crate) file_size: usize,
    /// Percentage (0..=100) of the file's rows that are deleted.
    pub(crate) deletion_percentage: usize,
}

impl CompactionGroupFile {
    /// Whether enough of the file's rows are deleted that rewriting it reclaims space; a threshold
    /// of 0 disables deletion-triggered compaction.
    fn is_deletion_heavy(&self, deletion_percentage_threshold: usize) -> bool {
        deletion_percentage_threshold != 0
            && self.deletion_percentage >= deletion_percentage_threshold
    }

    /// Bytes of the file's rows that are not deleted.
    fn live_bytes(&self) -> u64 {
        (self.file_size * (100 - self.deletion_percentage.min(100)) / 100) as u64
    }
}

/// Data files connected through shared file indices; compaction takes a group as a whole.
#[derive(Clone, Debug, Default)]
pub(crate) struct CompactionGroup {
    pub(crate) files: Vec<CompactionGroupFile>,
    pub(crate) file_indices: Vec<FileIndex>,
}

/// Like [`choose_compaction_groups`], restricted to groups of similar live size.
///
/// Groups with a deletion-heavy file (see `deletion_percentage_threshold`) are taken regardless of
/// size, because rewriting them reclaims the deleted rows; a large, mostly deleted file would
/// otherwise wait until enough files of its live size accumulate. Like the unrestricted choice,
/// they are only compacted together with at least `min_files` files in total, topped up with the
/// smallest other groups.
pub(crate) fn choose_similar_sized_compaction_groups(
    eligible: Vec<CompactionGroup>,
    min_files: usize,
    max_files: usize,
    deletion_percentage_threshold: usize,
) -> Vec<CompactionGroup> {
    let (deletion_heavy, others): (Vec<_>, Vec<_>) = eligible.into_iter().partition(|group| {
        group
            .files
            .iter()
            .any(|f| f.is_deletion_heavy(deletion_percentage_threshold))
    });
    let mut candidates = others
        .into_iter()
        .map(|group| SizedCandidate {
            bytes: group
                .files
                .iter()
                .map(CompactionGroupFile::live_bytes)
                .sum(),
            weight: group.files.len(),
            item: group,
        })
        .collect::<Vec<_>>();
    if deletion_heavy.is_empty() {
        return pick_similar_sized(candidates, min_files, max_files);
    }
    candidates.sort_by_key(|c| c.bytes);
    let smallest_first = candidates.into_iter().map(|c| c.item);
    choose_compaction_groups(
        deletion_heavy.into_iter().chain(smallest_first).collect(),
        min_files,
        max_files,
    )
}

/// Pick whole groups to compact, up to `max_files` data files in total. Returns an empty list if
/// fewer than `min_files` files can be chosen.
pub(crate) fn choose_compaction_groups(
    eligible: Vec<CompactionGroup>,
    min_files: usize,
    max_files: usize,
) -> Vec<CompactionGroup> {
    let mut chosen = Vec::new();
    let mut total = 0;
    for group in eligible {
        let size = group.files.len();
        if total + size > max_files {
            continue;
        }
        total += size;
        chosen.push(group);
    }
    if total < min_files {
        return Vec::new();
    }
    chosen
}
