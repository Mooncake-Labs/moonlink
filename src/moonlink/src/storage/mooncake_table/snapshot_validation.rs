use crate::storage::mooncake_table::SnapshotTableState;
use crate::storage::mooncake_table::{SnapshotOption, SnapshotTask};
use more_asserts as ma;
#[cfg(test)]
use std::collections::HashSet;

impl SnapshotTableState {
    /// Validate mooncake table invariants.
    pub(super) fn validate_mooncake_table_invariants(
        &self,
        task: &SnapshotTask,
        opt: &SnapshotOption,
    ) {
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

    /// Test util functions to assert current snapshot is at a consistent state.
    #[cfg(test)]
    pub(super) async fn assert_current_snapshot_consistent(&self) {
        // Check data files and file indices match each other.
        self.assert_data_files_and_file_indices_match();
        // Check one data file is only pointed by one file index.
        self.assert_file_indices_no_duplicate();
        // Check file ids don't have duplicate.
        self.assert_file_ids_no_duplicate();
        // Check index blocks are all cached.
        self.assert_index_blocks_cached().await;
        // Check persistence buffer.
        self.unpersisted_records.validate_invariants();
    }

    /// Test util function to validate data files and file indices match each other.
    #[cfg(test)]
    fn assert_data_files_and_file_indices_match(&self) {
        let mut all_data_files_1 = HashSet::new();
        let mut all_data_files_2 = HashSet::new();
        for (cur_data_file, _) in self.current_snapshot.disk_files.iter() {
            all_data_files_1.insert(cur_data_file.file_id());
        }
        for cur_file_index in self.current_snapshot.indices.file_indices.iter() {
            for cur_data_file in cur_file_index.files.iter() {
                all_data_files_2.insert(cur_data_file.file_id());
            }
        }
        assert_eq!(all_data_files_1, all_data_files_2);
    }

    /// Test util function to validate all index block files are cached, and cache handle filepath matches index file path.
    #[cfg(test)]
    async fn assert_index_blocks_cached(&self) {
        for cur_file_index in self.current_snapshot.indices.file_indices.iter() {
            for cur_index_block in cur_file_index.index_blocks.iter() {
                assert!(cur_index_block.cache_handle.is_some());
                assert_eq!(
                    cur_index_block
                        .cache_handle
                        .as_ref()
                        .unwrap()
                        .get_cache_filepath(),
                    cur_index_block.index_file.file_path()
                );
                assert!(
                    tokio::fs::try_exists(cur_index_block.index_file.file_path())
                        .await
                        .unwrap()
                );
            }
        }
    }

    /// Test util function to validate one data file is referenced by exactly one file index.
    #[cfg(test)]
    fn assert_file_indices_no_duplicate(&self) {
        // Get referenced data files by file indices.
        let mut referenced_data_files = HashSet::new();
        for cur_file_index in self.current_snapshot.indices.file_indices.iter() {
            for cur_data_file in cur_file_index.files.iter() {
                assert!(referenced_data_files.insert(cur_data_file.file_id()));
            }
        }

        // Get all data files, and assert they're equal.
        let data_files = self
            .current_snapshot
            .disk_files
            .keys()
            .map(|f| f.file_id())
            .collect::<HashSet<_>>();
        assert_eq!(data_files, referenced_data_files);
    }

    /// Test util function to validate file ids don't have duplicates.
    #[cfg(test)]
    fn assert_file_ids_no_duplicate(&self) {
        let mut file_ids = HashSet::new();
        for (cur_data_file, cur_disk_file_entry) in self.current_snapshot.disk_files.iter() {
            assert!(file_ids.insert(cur_data_file.file_id()));
            if let Some(puffin_blob_file) = &cur_disk_file_entry.puffin_deletion_blob {
                assert!(file_ids.insert(puffin_blob_file.puffin_file_cache_handle.file_id.file_id));
            }
        }
        for cur_file_index in &self.current_snapshot.indices.file_indices {
            for cur_index_block in &cur_file_index.index_blocks {
                assert!(file_ids.insert(cur_index_block.index_file.file_id()));
            }
        }
    }
}
