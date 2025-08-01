use crate::storage::index::FileIndex;
use crate::storage::mooncake_table::table_snapshot::IcebergSnapshotDataCompactionResult;
use crate::storage::mooncake_table::table_snapshot::{
    IcebergSnapshotImportResult, IcebergSnapshotIndexMergeResult,
};
use crate::storage::storage_utils::FileId;
use crate::storage::storage_utils::MooncakeDataFileRef;

use std::collections::HashSet;

/// Record iceberg persisted records, used to sync to mooncake snapshot.
#[derive(Debug, Default)]
pub(crate) struct IcebergPersistedRecords {
    /// Flush LSN for iceberg snapshot.
    pub(crate) flush_lsn: Option<u64>,
    /// New data file, puffin file and file indices result.
    pub(crate) import_result: IcebergSnapshotImportResult,
    /// Index merge persistence result.
    pub(crate) index_merge_result: IcebergSnapshotIndexMergeResult,
    /// Data compaction persistence result.
    pub(crate) data_compaction_result: IcebergSnapshotDataCompactionResult,
}

impl IcebergPersistedRecords {
    /// Return whether persistence result is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        if self.flush_lsn.is_none() {
            assert!(self.import_result.is_empty());
            assert!(self.index_merge_result.is_empty());
            assert!(self.data_compaction_result.is_empty());
            return true;
        }

        false
    }

    /// Get persisted data files.
    pub fn get_data_files_to_reflect_persistence(&self) -> Vec<MooncakeDataFileRef> {
        let mut persisted_data_files = vec![];
        persisted_data_files.extend(self.import_result.new_data_files.iter().cloned());
        persisted_data_files.extend(
            self.data_compaction_result
                .new_data_files_imported
                .iter()
                .cloned(),
        );
        persisted_data_files
    }

    /// Get persisted file indices, and files id for data files referenced by index blocks to delete.
    ///
    /// Notice, we don't need to reflect file indices persistence for index merge and data compaction, since file indices are always cached on-disk, thus mooncake snapshot only access local cache files.
    ///
    /// TODO(hjiang): It's actually better not to assume certain cache implementation, and only apply what iceberg table manager returns.
    pub fn get_file_indices_to_reflect_persistence(&self) -> (HashSet<FileId>, Vec<FileIndex>) {
        let mut persisted_file_indices = vec![];
        persisted_file_indices.extend(self.import_result.new_file_indices.iter().cloned());
        persisted_file_indices.extend(
            self.index_merge_result
                .new_file_indices_imported
                .iter()
                .cloned(),
        );
        persisted_file_indices.extend(
            self.data_compaction_result
                .new_file_indices_imported
                .iter()
                .cloned(),
        );

        let mut index_blocks_to_delete = HashSet::new();
        for cur_file_index in self.import_result.new_file_indices.iter() {
            index_blocks_to_delete.extend(cur_file_index.files.iter().map(|f| f.file_id()));
        }
        for cur_file_index in self.index_merge_result.new_file_indices_imported.iter() {
            index_blocks_to_delete.extend(cur_file_index.files.iter().map(|f| f.file_id()));
        }
        for cur_file_index in self.data_compaction_result.new_file_indices_imported.iter() {
            index_blocks_to_delete.extend(cur_file_index.files.iter().map(|f| f.file_id()));
        }

        (index_blocks_to_delete, persisted_file_indices)
    }

    /// Util function to validate all data files referenced by file indices are remote files.
    fn validate_file_indices_remote(&self, file_index: &FileIndex, warehouse_uri: &str) {
        for cur_index_block in file_index.index_blocks.iter() {
            assert!(cur_index_block
                .index_file
                .file_path()
                .starts_with(warehouse_uri));
        }
    }

    /// Validate all imported data files, file indices and index blocks point to remote files.
    pub fn validate_imported_files_remote(&self, warehouse_uri: &str) {
        #[cfg(any(test, debug_assertions))]
        {
            let import_result = &self.import_result;

            // Validate persisted data files point to remote.
            for cur_data_file in import_result.new_data_files.iter() {
                assert!(cur_data_file.file_path().starts_with(warehouse_uri));
            }

            // Validate persisted file indices and index blocks point to remote.
            for cur_file_index in import_result.new_file_indices.iter() {
                self.validate_file_indices_remote(cur_file_index, warehouse_uri);
            }
        }

        #[cfg(any(test, debug_assertions))]
        {
            let index_merge_results = &self.index_merge_result;
            for cur_file_index in index_merge_results.new_file_indices_imported.iter() {
                self.validate_file_indices_remote(cur_file_index, warehouse_uri);
            }
        }

        #[cfg(any(test, debug_assertions))]
        {
            let data_compaction_results = &self.data_compaction_result;

            // Validate persisted data files point to remote.
            for cur_data_file in data_compaction_results.new_data_files_imported.iter() {
                assert!(cur_data_file.file_path().starts_with(warehouse_uri));
            }

            // Validate persisted file indices and index blocks point to remote.
            for cur_file_index in data_compaction_results.new_file_indices_imported.iter() {
                self.validate_file_indices_remote(cur_file_index, warehouse_uri);
            }
        }
    }
}
