use crate::storage::index::cache_utils as index_cache_utils;
/// This file contains cache related snapshot functions.
use crate::storage::mooncake_table::transaction_stream::TransactionStreamOutput;
use crate::storage::mooncake_table::SnapshotTableState;
use crate::storage::mooncake_table::SnapshotTask;
use crate::storage::storage_utils::TableId;

impl SnapshotTableState {
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
    pub(super) async fn import_file_indices_into_cache(
        &mut self,
        task: &mut SnapshotTask,
    ) -> Vec<String> {
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
}
