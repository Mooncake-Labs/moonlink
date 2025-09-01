#[cfg(any(test, debug_assertions))]
use crate::row::IdentityProp;
use crate::storage::mooncake_table::SnapshotTableState;
use crate::storage::mooncake_table::{SnapshotOption, SnapshotTask};
use more_asserts as ma;
#[cfg(any(test, debug_assertions))]
use std::collections::HashSet;

impl SnapshotTableState {
    /// Validate mooncake table invariants.
    pub(super) fn validate_mooncake_table_invariants(
        &self,
        task: &SnapshotTask,
        opt: &SnapshotOption,
    ) {
        if let Some(new_flush_lsn) = task.new_flush_lsn {
            if self.current_snapshot.flush_lsn.is_some() {
                // Invariant-1: flush LSN doesn't regress.
                //
                // Force snapshot not change table states, it's possible to use the latest flush LSN.
                if opt.force_create {
                    ma::assert_le!(self.current_snapshot.flush_lsn.unwrap(), new_flush_lsn);
                }
                // Otherwise, flush LSN always progresses.
                else {
                    ma::assert_lt!(self.current_snapshot.flush_lsn.unwrap(), new_flush_lsn);
                }

                // Invariant-2: flush must follow a commit, but commit doesn't need to be followed by a flush.
                //
                // Force snapshot could flush as long as the table at a clean state (aka, no uncommitted states), possible to go without commit at current snapshot iteration.
                if opt.force_create {
                    assert!(
                        task.commit_lsn_baseline == 0
                            || task.commit_lsn_baseline >= new_flush_lsn
                            || task.commit_lsn_baseline == task.prev_commit_lsn_baseline,
                        "New commit LSN is {}, new flush LSN is {}",
                        task.commit_lsn_baseline,
                        new_flush_lsn
                    );
                } else {
                    ma::assert_ge!(task.commit_lsn_baseline, new_flush_lsn);
                }
            }
        }
    }

    /// Test util functions to assert current snapshot is at a consistent state.
    #[cfg(any(test, debug_assertions))]
    pub(super) async fn assert_current_snapshot_consistent(&self) {
        // Check data files and file indices match each other.
        self.assert_data_files_and_file_indices_match();
        // Check deletion record for disk files.
        self.assert_deletion_records_for_disk_files();
        // Check one data file is only pointed by one file index.
        self.assert_file_indices_no_duplicate();
        // Check file ids don't have duplicate.
        self.assert_file_ids_no_duplicate();
        // Check index blocks are all cached.
        self.assert_index_blocks_cached().await;
        // Check all uncommitted deletion logs are valid.
        self.assert_uncommitted_deletion_logs_valid();
        // Check persistence buffer.
        self.unpersisted_records.validate_invariants();
    }

    /// Util function to validate uncommitted deletion logs are valid.
    #[cfg(any(test, debug_assertions))]
    fn assert_uncommitted_deletion_logs_valid(&self) {
        for cur_log in self.uncommitted_deletion_log.iter() {
            assert!(cur_log.is_some());
        }
    }

    /// Util function to validate deleted rows for batch deletion vector and puffin deletion vector blob.
    #[cfg(any(test, debug_assertions))]
    fn assert_deletion_records_for_disk_files(&self) {
        for (_, cur_disk_file_entry) in self.current_snapshot.disk_files.iter() {
            // Puffin blob is a subset of batch deletion vector.
            ma::assert_le!(
                cur_disk_file_entry
                    .puffin_deletion_blob
                    .as_ref()
                    .map_or(0, |cur_puffin_blob| cur_puffin_blob.num_rows),
                cur_disk_file_entry
                    .committed_deletion_vector
                    .get_num_rows_deleted()
            );
        }
    }

    /// Util function to validate data files and file indices match each other.
    #[cfg(any(test, debug_assertions))]
    fn assert_data_files_and_file_indices_match(&self) {
        // Skip validation for append-only tables since they don't have file indices.
        if matches!(
            self.mooncake_table_metadata.config.row_identity,
            IdentityProp::None
        ) {
            return;
        }

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

    /// Util function to validate all index block files are cached, and cache handle filepath matches index file path.
    #[cfg(any(test, debug_assertions))]
    async fn assert_index_blocks_cached(&self) {
        // Skip validation for append-only tables since they don't have file indices
        if matches!(
            self.mooncake_table_metadata.config.row_identity,
            IdentityProp::None
        ) {
            return;
        }

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

    /// Util function to validate one data file is referenced by exactly one file index.
    #[cfg(any(test, debug_assertions))]
    fn assert_file_indices_no_duplicate(&self) {
        // Skip validation for append-only tables since they don't have file indices
        if matches!(
            self.mooncake_table_metadata.config.row_identity,
            IdentityProp::None
        ) {
            return;
        }

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

    /// Util function to validate file ids don't have duplicates.
    #[cfg(any(test, debug_assertions))]
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
