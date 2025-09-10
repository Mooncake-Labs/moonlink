use crate::create_data_file;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::index::FileIndex;
use crate::storage::mooncake_table::snapshot::CommittedDeletionToPersist;
use crate::storage::mooncake_table::table_snapshot::IcebergSnapshotDataCompactionPayload;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::SnapshotTask;
use crate::storage::mooncake_table::{
    IcebergSnapshotImportPayload, IcebergSnapshotIndexMergePayload,
};
use crate::storage::snapshot_options::IcebergSnapshotOption;
use crate::storage::storage_utils::FileId;
use crate::storage::storage_utils::MooncakeDataFileRef;
/// This file stores snapshot persistence related features.
use crate::storage::SnapshotTableState;
use std::collections::{HashMap, HashSet};

impl SnapshotTableState {
    /// Util function to decide whether to create iceberg snapshot by deletion vectors.
    pub(super) fn create_iceberg_snapshot_by_committed_logs(&self, force_create: bool) -> bool {
        let deletion_record_snapshot_threshold = if !force_create {
            self.mooncake_table_metadata
                .config
                .iceberg_snapshot_new_committed_deletion_log()
        } else {
            1
        };
        self.committed_deletion_log.len() >= deletion_record_snapshot_threshold
    }

    pub(super) fn get_iceberg_snapshot_payload(
        &self,
        opt: &IcebergSnapshotOption,
        flush_lsn: u64,
        committed_deletion_to_persist: CommittedDeletionToPersist,
    ) -> IcebergSnapshotPayload {
        IcebergSnapshotPayload {
            uuid: opt.get_event_id().unwrap(),
            flush_lsn,
            new_table_schema: None,
            committed_deletion_logs: committed_deletion_to_persist.committed_deletion_logs,
            import_payload: IcebergSnapshotImportPayload {
                data_files: self.unpersisted_records.get_unpersisted_data_files(),
                new_deletion_vector: committed_deletion_to_persist.new_deletions_to_persist,
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
                data_file_records_remap: self.unpersisted_records.get_compacted_data_file_remap(),
            },
        }
    }

    /// Update disk files in the current snapshot from local data files to remote ones, meanwile unpin write-through cache file from object storage cache.
    /// Provide [`persisted_data_files`] could come from imported new files, or maintenance jobs like compaction.
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
        for cur_data_file in persisted_data_files.into_iter() {
            // Removing entry with [`cur_data_file`] and insert with the same key might be confusing, but here we're only using file id as key, but not filepath.
            // So the real operation is: remove the entry with <old filepath> and insert with <new filepath>.
            let mut disk_file_entry = self
                .current_snapshot
                .disk_files
                .remove(&cur_data_file)
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
                .insert(cur_data_file, disk_file_entry);
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

    /// Update current mooncake snapshot with persisted deletion vector.
    /// Return the evicted files to delete.
    async fn update_deletion_vector_to_persisted(
        &mut self,
        puffin_blob_ref: HashMap<FileId, PuffinBlobRef>,
    ) -> Vec<String> {
        // Aggregate the evicted files to delete.
        let mut evicted_files_to_delete = vec![];

        for (file_id, puffin_blob_ref) in puffin_blob_ref.into_iter() {
            // The data file referenced by puffin blob still exist.
            if let Some(entry) = self.current_snapshot.disk_files.get_mut(&file_id) {
                // Unreference and delete old cache handle if any.
                let old_puffin_blob = entry.puffin_deletion_blob.take();
                if let Some(old_puffin_blob) = old_puffin_blob {
                    let cur_evicted_files = old_puffin_blob
                        .puffin_file_cache_handle
                        .unreference_and_delete()
                        .await;
                    evicted_files_to_delete.extend(cur_evicted_files);
                }
                entry.puffin_deletion_blob = Some(puffin_blob_ref);
            }
            // The referenced data file has been removed, for example, by completed data compaction; directly discard the puffin blob.
            // The committed deletion record included by the puffin deletion blob is still contained current snapshot's committed deletion records.
            else {
                let cur_evicted_files = puffin_blob_ref
                    .puffin_file_cache_handle
                    .unreference_and_delete()
                    .await;
                evicted_files_to_delete.extend(cur_evicted_files);
            }
        }

        evicted_files_to_delete
    }

    /// Update current snapshot with iceberg persistence result.
    /// Before iceberg snapshot, mooncake snapshot records local write through cache in disk file (which is local filepath).
    /// After a successful iceberg snapshot, update current snapshot's disk files and file indices to reference to remote paths,
    /// also import local write through cache to globally managed object storage cache, so they could be pinned and evicted when necessary.
    ///
    /// Return evicted data files to delete when unreference existing disk file entries.
    pub(super) async fn update_snapshot_by_iceberg_snapshot(
        &mut self,
        task: &SnapshotTask,
    ) -> Vec<String> {
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
}
