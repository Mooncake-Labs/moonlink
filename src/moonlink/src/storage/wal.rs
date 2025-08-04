pub mod iceberg_corresponding_wal_metadata;
use crate::row::MoonlinkRow;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::factory::create_filesystem_accessor;
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::filesystem::storage_config::StorageConfig;
use crate::storage::wal::iceberg_corresponding_wal_metadata::IcebergCorrespondingWalMetadata;
use crate::table_notify::TableEvent;
use crate::Result;
use futures::stream::{self, Stream};
use futures::{future, StreamExt};
use more_asserts as ma;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub const DEFAULT_WAL_FOLDER: &str = "wal";

#[derive(Debug, Clone)]
pub struct WalConfig {
    accessor_config: AccessorConfig,
}

impl WalConfig {
    /// Create a default WAL config for local storage. Should take in the mooncake table ID,
    /// a unique identifier for a table in mooncake. Note that something like just postgres table ID
    /// is not guaranteed to be unique, so we need to use the mooncake table ID which is unique
    /// within moonlink.
    pub fn default_wal_config_local(mooncake_table_id: &str, base_path: &Path) -> WalConfig {
        let wal_storage_config = StorageConfig::FileSystem {
            root_directory: base_path
                .join(DEFAULT_WAL_FOLDER)
                .join(mooncake_table_id)
                .to_str()
                .unwrap()
                .to_string(),
        };
        Self {
            accessor_config: AccessorConfig::new_with_storage_config(wal_storage_config),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum WalEvent {
    Append {
        row: MoonlinkRow,
        xact_id: Option<u32>,
        lsn: u64,
        is_copied: bool,
    },
    Delete {
        row: MoonlinkRow,
        lsn: u64,
        xact_id: Option<u32>,
    },
    Commit {
        lsn: u64,
        xact_id: Option<u32>,
    },
    StreamAbort {
        xact_id: u32,
    },
    StreamFlush {
        xact_id: u32,
    },
}

#[derive(Debug, Clone)]
/// Information about the last iceberg snapshot that is used to determine how the WAL was truncated.
pub struct IcebergSnapshotWalInfo {
    iceberg_snapshot_wal_file_num: u64,
    iceberg_snapshot_lsn: u64,
}

impl IcebergSnapshotWalInfo {
    pub fn new(iceberg_snapshot_wal_file_num: u64, iceberg_snapshot_lsn: u64) -> Self {
        Self {
            iceberg_snapshot_wal_file_num,
            iceberg_snapshot_lsn,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalPersistenceUpdateResult {
    /// UUID for current persistence operation.
    #[allow(dead_code)]
    uuid: uuid::Uuid,
    file_persisted: Option<WalFileInfo>,
    /// Only None if there has not been an iceberg snapshot yet.
    iceberg_snapshot_wal_info: Option<IcebergSnapshotWalInfo>,
}

impl WalPersistenceUpdateResult {
    pub fn new(
        uuid: uuid::Uuid,
        file_persisted: Option<WalFileInfo>,
        iceberg_snapshot_wal_info: Option<IcebergSnapshotWalInfo>,
    ) -> Self {
        Self {
            uuid,
            file_persisted,
            iceberg_snapshot_wal_info,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalFileInfo {
    file_number: u64,
    highest_lsn: u64,
}
impl WalEvent {
    pub fn new(table_event: &TableEvent) -> Self {
        match table_event {
            TableEvent::Append {
                row,
                xact_id,
                lsn,
                is_copied,
            } => WalEvent::Append {
                row: row.clone(),
                xact_id: *xact_id,
                lsn: *lsn,
                is_copied: *is_copied,
            },
            TableEvent::Delete { row, lsn, xact_id } => WalEvent::Delete {
                row: row.clone(),
                lsn: *lsn,
                xact_id: *xact_id,
            },
            TableEvent::Commit { lsn, xact_id } => WalEvent::Commit {
                lsn: *lsn,
                xact_id: *xact_id,
            },
            TableEvent::StreamAbort { xact_id } => WalEvent::StreamAbort { xact_id: *xact_id },
            TableEvent::CommitFlush { lsn, xact_id } => WalEvent::Commit {
                lsn: *lsn,
                xact_id: *xact_id,
            },
            TableEvent::StreamFlush { xact_id } => WalEvent::StreamFlush { xact_id: *xact_id },
            _ => unimplemented!(
                "TableEvent variant not supported for WAL: {:?}",
                table_event
            ),
        }
    }

    pub fn into_table_event(self) -> TableEvent {
        match self {
            WalEvent::Append {
                row,
                xact_id,
                lsn,
                is_copied,
            } => TableEvent::Append {
                row,
                xact_id,
                lsn,
                is_copied,
            },
            WalEvent::Delete { row, lsn, xact_id } => TableEvent::Delete { row, lsn, xact_id },
            WalEvent::Commit { lsn, xact_id } => TableEvent::Commit { lsn, xact_id },
            WalEvent::StreamAbort { xact_id } => TableEvent::StreamAbort { xact_id },
            WalEvent::StreamFlush { xact_id } => TableEvent::StreamFlush { xact_id },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum WalTransactionState {
    Commit {
        start_file: u64,
        completion_lsn: u64,
        file_end: u64,
    },
    Abort {
        start_file: u64,
        completion_lsn: u64,
        file_end: u64,
    },
    Open {
        start_file: u64,
    },
}

impl WalTransactionState {
    fn get_start_file(&self) -> u64 {
        match self {
            WalTransactionState::Open { start_file } => *start_file,
            WalTransactionState::Commit { start_file, .. } => *start_file,
            WalTransactionState::Abort { start_file, .. } => *start_file,
        }
    }

    fn get_completion_lsn_and_file(&self) -> Option<(u64, u64)> {
        match self {
            WalTransactionState::Open { .. } => None,
            WalTransactionState::Commit {
                completion_lsn,
                file_end,
                ..
            } => Some((*completion_lsn, *file_end)),
            WalTransactionState::Abort {
                completion_lsn,
                file_end,
                ..
            } => Some((*completion_lsn, *file_end)),
        }
    }

    fn is_closed(&self) -> bool {
        self.get_completion_lsn_and_file().is_some()
    }

    /// Checks if a transaction is captured in the iceberg snapshot. Sometimes this may be called after we have calculated the lowest file to keep
    /// for the snapshot, so we check for consistency that an xact capture in the iceberg snapshot has both its completion LSN <= iceberg snapshot lsn
    /// and its completion file number < lowest_file_kept.
    fn is_captured_in_iceberg_snapshot(
        &self,
        iceberg_snapshot_lsn: u64,
        iceberg_snapshot_wal_file_num: Option<u64>,
    ) -> bool {
        let completion_lsn_and_file = self.get_completion_lsn_and_file();

        // the xact has a known completion lsn by the iceberg snapshot lsn,
        // so it is captured in the iceberg snapshot
        if let Some((completion_lsn, completion_file_number)) = completion_lsn_and_file {
            // here we do the check for consistency
            if let Some(iceberg_snapshot_wal_file_num) = iceberg_snapshot_wal_file_num {
                self.check_completed_xact_consistent_with_iceberg_snapshot(
                    completion_lsn,
                    completion_file_number,
                    iceberg_snapshot_lsn,
                    iceberg_snapshot_wal_file_num,
                );
            }
            if completion_lsn <= iceberg_snapshot_lsn {
                return true;
            }
        }
        false
    }

    fn check_completed_xact_consistent_with_iceberg_snapshot(
        &self,
        completion_lsn: u64,
        completion_file_number: u64,
        iceberg_snapshot_lsn: u64,
        lowest_file_kept: u64,
    ) {
        if completion_lsn > iceberg_snapshot_lsn {
            // If the transaction completed after the iceberg snapshot LSN,
            // its completion file HAS to be newer than or equal to the lowest file we're keeping
            // to prevent data loss.
            assert!(
                completion_file_number >= lowest_file_kept,
                "Transaction completed at LSN {completion_lsn} (after iceberg snapshot LSN {iceberg_snapshot_lsn}), \
                but its completion file {completion_file_number} is older than lowest file to keep {lowest_file_kept}"
            );
        }
        // Note that the reverse case is not always true.
        // If the transaction completed before or at the iceberg snapshot LSN,
        // its completion file may not be older than the lowest file we're keeping because we may have to
        // keep that file around because of other transactions in those files not yet captured in the iceberg snapshot.
    }
}

/// Wal tracks both the in-memory WAL and the flushed WALs.
/// Note that wal manager is meant to be used in a single thread. While
/// persist and delete_files can be called asynchronously, their results returned
/// from those operations have to be handled serially.
/// There is one instance of WalManager per table.
pub struct WalManager {
    /// In Mem Wal that gets appended to. When we need to flush, we call take on the buffer inside.
    pub in_mem_buf: Vec<WalEvent>,
    /// highest last seen lsn
    highest_seen_lsn: u64,
    /// The wal file numbers that are still live. Tracked in ascending order of file number.
    live_wal_files_tracker: Vec<WalFileInfo>,
    /// Tracks the file number to be assigned to the next flushed file.
    /// All events currently in the in_mem_buf will be flushed to a file with this file number.
    curr_file_number: u64,
    /// Tracks any transactions that may not have been flushed to an iceberg snapshot yet,
    /// and therefore need to live in the WAL.
    active_transactions: HashMap<u32, WalTransactionState>,
    /// Similar to active_transactions, but for the main transaction.
    /// Tracks the commits and aborts of the main transaction. Note that
    /// the events from the main transaction may also be spread across multiple files.
    /// This is in ascending order of completion LSN.
    main_transaction_tracker: Vec<WalTransactionState>,

    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
}

impl WalManager {
    pub fn new(config: &WalConfig) -> Self {
        // TODO(Paul): Add a more robust constructor when implementing recovery
        let accessor_config = config.accessor_config.clone();
        Self {
            in_mem_buf: Vec::new(),
            highest_seen_lsn: 0,
            live_wal_files_tracker: Vec::new(),
            curr_file_number: 0,
            active_transactions: HashMap::new(),
            main_transaction_tracker: Vec::new(),
            // TODO(Paul): Implement object storage
            file_system_accessor: create_filesystem_accessor(accessor_config),
        }
    }

    pub fn get_file_name(file_number: u64) -> String {
        format!("wal_{file_number}.json")
    }

    pub fn get_file_system_accessor(&self) -> Arc<dyn BaseFileSystemAccess> {
        self.file_system_accessor.clone()
    }

    // ------------------------------
    // Inserting events
    // ------------------------------

    fn get_updated_xact_state(
        table_event: &TableEvent,
        xact_state: WalTransactionState,
        highest_seen_lsn: u64,
        curr_file_number: u64,
    ) -> WalTransactionState {
        match table_event {
            TableEvent::Append { .. }
            | TableEvent::Delete { .. }
            | TableEvent::StreamFlush { .. } => WalTransactionState::Open {
                start_file: xact_state.get_start_file(),
            },
            TableEvent::Commit { lsn, .. } | TableEvent::CommitFlush { lsn, .. } => {
                WalTransactionState::Commit {
                    start_file: xact_state.get_start_file(),
                    completion_lsn: *lsn,
                    file_end: curr_file_number,
                }
            }
            TableEvent::StreamAbort { .. } => WalTransactionState::Abort {
                start_file: xact_state.get_start_file(),
                completion_lsn: highest_seen_lsn,
                file_end: curr_file_number,
            },
            _ => unimplemented!(
                "TableEvent variant not supported for WAL: {:?}",
                table_event
            ),
        }
    }

    /// Update transaction tracking when a new event is inserted
    fn update_transaction_tracking(&mut self, table_event: &TableEvent) {
        let xact_id = match table_event {
            TableEvent::Append { xact_id, .. } => *xact_id,
            TableEvent::Delete { xact_id, .. } => *xact_id,
            TableEvent::Commit { xact_id, .. } => *xact_id,
            TableEvent::StreamAbort { xact_id } => Some(*xact_id),
            _ => None, // Other events don't have xact_id
        };

        if let Some(xact_id) = xact_id {
            // Case: streaming xact
            // Extract the transaction state as an owned value, or create a new one if not present
            let old_state =
                self.active_transactions
                    .remove(&xact_id)
                    .unwrap_or(WalTransactionState::Open {
                        start_file: self.curr_file_number,
                    });

            let updated_state = Self::get_updated_xact_state(
                table_event,
                old_state,
                self.highest_seen_lsn,
                self.curr_file_number,
            );
            self.active_transactions.insert(xact_id, updated_state);
        } else {
            // Case: main transaction
            // if  there isn't currently a state tracking the main transaction, add one
            let old_state = if self.main_transaction_tracker.is_empty()
                || self.main_transaction_tracker.last().unwrap().is_closed()
            {
                WalTransactionState::Open {
                    start_file: self.curr_file_number,
                }
            } else {
                self.main_transaction_tracker.pop().unwrap()
            };
            let updated_state = Self::get_updated_xact_state(
                table_event,
                old_state,
                self.highest_seen_lsn,
                self.curr_file_number,
            );
            // TODO(Paul): This could get very long and might have many commits in a single file. We can
            // coalesce all main xacts that share the same start_file and end_file into a single state.
            self.main_transaction_tracker.push(updated_state);
        };
    }

    pub fn push(&mut self, table_event: &TableEvent) {
        // add to in_mem_buf
        let wal_event = WalEvent::new(table_event);
        self.in_mem_buf.push(wal_event);

        // Update highest_lsn if this event has a higher LSN
        if let Some(lsn) = table_event.get_lsn_for_ingest_event() {
            self.highest_seen_lsn = self.highest_seen_lsn.max(lsn);
        }

        // update transaction tracking
        self.update_transaction_tracking(table_event);
    }

    // ------------------------------
    // Preparing for truncation
    // ------------------------------

    /// Returns the lowest file number that needs to be kept. Is called while preparing for an iceberg snapshot,
    /// to be stored in the iceberg snapshot metadata.
    ///
    /// An event can only be dropped if the completion LSN of its transaction commit/abort is
    /// less than truncate_from_lsn. In this function, we represent this completion LSN of any event as
    /// completion_lsn.
    ///
    /// If a transaction is not yet committed by truncate_from_lsn, its completion_lsn is None,
    /// indicating that it is to be determined at a point in the future > truncate_from_lsn, and
    /// therefore it cannot be dropped.
    ///
    /// A WAL file can only be dropped if all its events are captured in the iceberg snapshot,
    /// as per the criteria above.
    ///
    /// if no files need to be kept (all can be truncated), then it returns the next file number to be assigned.
    pub fn get_lowest_file_to_keep(&self, truncate_from_lsn: u64) -> u64 {
        let xacts_still_incomplete_after_truncate = self
            .active_transactions
            .values()
            .filter(|state| !state.is_captured_in_iceberg_snapshot(truncate_from_lsn, None))
            .collect::<Vec<&WalTransactionState>>();

        let mut files_to_keep = xacts_still_incomplete_after_truncate
            .iter()
            .map(|state| state.get_start_file())
            .collect::<Vec<u64>>();

        // now we look through the main transaction tracker and find the first transaction that
        // is not yet captured in the iceberg snapshot (ie has a completion_lsn greater than truncate_from_lsn)
        // we also need to find the first file that has a highest_lsn less than truncate_from_lsn
        let main_xact_file_to_keep = self
            .main_transaction_tracker
            .iter()
            .find(|state| !state.is_captured_in_iceberg_snapshot(truncate_from_lsn, None))
            .map(|state| state.get_start_file());

        if let Some(file_to_keep) = main_xact_file_to_keep {
            // if there is a main transaction that is not yet captured in the iceberg snapshot
            files_to_keep.push(file_to_keep);
        }

        // get the min of the files_to_keep and the main_xact_file_to_keep
        // if this is None, then we do not need to keep any files
        let lowest_file_to_keep = files_to_keep.iter().min().copied();
        if let Some(lowest_file_to_keep) = lowest_file_to_keep {
            lowest_file_to_keep
        } else {
            self.curr_file_number
        }
    }

    /// Returns a list of WAL files to be truncated, following an iceberg snapshot where we already
    ///  determine the lowest file number to be kept.
    /// List of files returned is sorted in ascending order of file number.
    /// Should be called in preparation to asynchronously delete the files.
    pub fn get_files_to_truncate(
        &self,
        iceberg_corresponding_wal_metadata: IcebergCorrespondingWalMetadata,
    ) -> Vec<WalFileInfo> {
        // get all file numbers less than the lowest file to keep as we can then delete them
        let lowest_file_to_keep = iceberg_corresponding_wal_metadata.earliest_wal_file_num;

        if !self.live_wal_files_tracker.is_empty() {
            ma::assert_ge!(
                lowest_file_to_keep,
                self.live_wal_files_tracker.first().unwrap().file_number,
                "We must be keeping a file that is at least as old as the oldest live WAL file"
            );
        }

        self.live_wal_files_tracker
            .iter()
            .filter(|wal_file_info| wal_file_info.file_number < lowest_file_to_keep)
            .cloned()
            .collect()
    }

    // ------------------------------
    // Preparing for persistence
    // ------------------------------

    /// Takes all events from the in-memory buffer and prepare metadata for the next WAL file.
    /// Resets the in-mem_buf and increments the curr_file_number.
    ///
    /// This function is called when we periodically persist the WAL, in preparation for a
    /// flush in the background.
    pub fn extract_next_persistence_file(&mut self) -> Option<(Vec<WalEvent>, WalFileInfo)> {
        let events_to_persist = std::mem::take(&mut self.in_mem_buf);

        if events_to_persist.is_empty() {
            return None;
        }

        let file_info = WalFileInfo {
            file_number: self.curr_file_number,
            highest_lsn: self.highest_seen_lsn,
        };
        self.curr_file_number += 1;
        Some((events_to_persist, file_info))
    }

    // ------------------------------
    // Async persist / truncate
    // ------------------------------
    /// Delete a list of wal files from the file system.
    /// Should be called asynchronously using the results from get_files_to_truncate.
    /// TODO(Paul): This should be moved to the file system level.
    pub async fn delete_files(
        file_system_accessor: Arc<dyn BaseFileSystemAccess>,
        wal_file_numbers: &[WalFileInfo],
    ) -> Result<()> {
        let file_names = wal_file_numbers
            .iter()
            .map(|wal_file_info| WalManager::get_file_name(wal_file_info.file_number))
            .collect::<Vec<String>>();
        let delete_futures = file_names
            .iter()
            .map(|file_name| file_system_accessor.delete_object(file_name));
        let delete_results = future::join_all(delete_futures).await;
        for result in delete_results {
            result?;
        }
        Ok(())
    }

    /// Persist a series of wal events to the file system.
    /// Should be called asynchronously using the most recent wal data extracted form the
    /// in-memory buffer.
    pub async fn persist(
        file_system_accessor: Arc<dyn BaseFileSystemAccess>,
        wal_to_persist: &Vec<WalEvent>,
        wal_file_info: &WalFileInfo,
    ) -> Result<()> {
        if !wal_to_persist.is_empty() {
            let wal_json = serde_json::to_vec(&wal_to_persist)?;

            let wal_file_path = WalManager::get_file_name(wal_file_info.file_number);
            file_system_accessor
                .write_object(&wal_file_path, wal_json)
                .await?;
        }
        Ok(())
    }

    /// This function is called when we periodically persist the WAL.
    /// It persists any new events in the WAL, and deletes any old WAL files following an iceberg snapshot.
    ///
    /// iceberg snapshot info is None only if there has not been an iceberg snapshot yet.
    pub async fn wal_persist_truncate_async(
        uuid: uuid::Uuid,
        files_to_delete: Vec<WalFileInfo>,
        file_to_persist: Option<(Vec<WalEvent>, WalFileInfo)>,
        file_system_accessor: Arc<dyn BaseFileSystemAccess>,
        table_notify: Sender<TableEvent>,
        iceberg_snapshot_wal_info: Option<IcebergSnapshotWalInfo>,
    ) {
        // Execute WAL operations
        let result = async {
            let file_system_accessor_persist = file_system_accessor.clone();

            // Delete old WAL files
            if !files_to_delete.is_empty() {
                assert!(
                    iceberg_snapshot_wal_info.is_some(),
                    "iceberg_snapshot_wal_info must be set when deleting files, because it means that an iceberg snapshot has been taken"
                );
                assert!(
                    files_to_delete.last().unwrap().file_number
                        == iceberg_snapshot_wal_info.as_ref().unwrap().iceberg_snapshot_wal_file_num - 1,
                    "the last file to delete must be lower than the lowest file to keep"
                );
                WalManager::delete_files(file_system_accessor, &files_to_delete).await?;
            }

            // Persist new WAL file
            if let Some((wal_events, wal_file_info)) = &file_to_persist {
                WalManager::persist(file_system_accessor_persist, wal_events, wal_file_info)
                    .await?;
            }

            Ok(())
        }
        .await;

        // Create result and notify
        let persistence_update_result = result.map(|_| {
            WalPersistenceUpdateResult::new(
                uuid,
                file_to_persist.map(|(_, wal_file_info)| wal_file_info),
                iceberg_snapshot_wal_info,
            )
        });

        table_notify
            .send(TableEvent::PeriodicalWalPersistenceUpdateResult {
                result: persistence_update_result,
            })
            .await
            .unwrap();
    }

    // ------------------------------
    // Handling completed persistence and truncation
    // ------------------------------
    /// Handles the persistence side of a WAL persistence update.
    fn handle_complete_persistence(
        &mut self,
        persistence_update_result: &WalPersistenceUpdateResult,
    ) {
        if let Some(file_persisted) = &persistence_update_result.file_persisted {
            assert_eq!(file_persisted.file_number, self.curr_file_number - 1);
            // Add the persisted file to the live tracker
            self.live_wal_files_tracker.push(file_persisted.clone());
        }
    }

    /// Remove all xacts that have been captured in the most recent iceberg snapshot.
    fn cleanup_xacts(&mut self, iceberg_snapshot_lsn: u64, iceberg_snapshot_wal_file_num: u64) {
        // remove all xacts that are captured in the iceberg snapshot
        self.active_transactions.retain(|_, state| {
            !state.is_captured_in_iceberg_snapshot(
                iceberg_snapshot_lsn,
                Some(iceberg_snapshot_wal_file_num),
            )
        });
        // remove all main xacts that are captured in the iceberg snapshot
        self.main_transaction_tracker.retain(|state| {
            !state.is_captured_in_iceberg_snapshot(
                iceberg_snapshot_lsn,
                Some(iceberg_snapshot_wal_file_num),
            )
        });
    }

    /// Handles the truncation side of a WAL persistence update.
    /// This cleans up any files that no longer need to be tracked, and also any xacts that no longer need to
    /// be tracked as a result.
    fn handle_complete_truncate(&mut self, persistence_update_result: &WalPersistenceUpdateResult) {
        if let Some(iceberg_snapshot_wal_info) =
            &persistence_update_result.iceberg_snapshot_wal_info
        {
            self.live_wal_files_tracker.retain(|wal_file_info| {
                wal_file_info.file_number >= iceberg_snapshot_wal_info.iceberg_snapshot_wal_file_num
            });

            self.cleanup_xacts(
                iceberg_snapshot_wal_info.iceberg_snapshot_lsn,
                iceberg_snapshot_wal_info.iceberg_snapshot_wal_file_num,
            );
        }
    }

    /// Should be called after a WAL persistence update operation has completed. Updates
    /// tracked files and transactions. Returns the highest LSN that has been persisted into WAL.
    ///
    /// For internal tracking, we do truncates before persistence.
    pub fn handle_complete_wal_persistence_update(
        // For now, we handle the persist and truncate results together.
        &mut self,
        wal_persistence_update_result: &WalPersistenceUpdateResult,
    ) -> Option<u64> {
        self.handle_complete_truncate(wal_persistence_update_result);
        self.handle_complete_persistence(wal_persistence_update_result);

        wal_persistence_update_result
            .file_persisted
            .as_ref()
            .map(|wal_file_info| wal_file_info.highest_lsn)
    }

    // ------------------------------
    // Drop WAL files
    // ------------------------------
    /// Drops all WAL files by removing the entire WAL directory for this table.
    pub async fn drop_wal(&mut self) -> Result<()> {
        self.file_system_accessor.remove_directory("").await?;
        Ok(())
    }

    // ------------------------------
    // Recovery
    // ------------------------------

    /// Recover the flushed WALs from the file system. Start file number and begin_from_lsn are
    /// both inclusive.
    #[allow(dead_code)]
    fn recover_flushed_wals(
        file_system_accessor: Arc<dyn BaseFileSystemAccess>,
        start_file_number: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<TableEvent>>> + Send>> {
        Box::pin(stream::unfold(start_file_number, move |file_number| {
            let file_system_accessor = file_system_accessor.clone();
            async move {
                let file_name = WalManager::get_file_name(file_number);
                let exists = file_system_accessor.object_exists(&file_name).await;
                match exists {
                    Ok(exists) => {
                        // If file not found, we have reached the end of the WAL files
                        if !exists {
                            return None;
                        }
                    }
                    Err(e) => {
                        return Some((Err(e), file_number + 1));
                    }
                }

                match file_system_accessor.read_object(&file_name).await {
                    Ok(bytes) => {
                        let wal_events: Vec<WalEvent> = match serde_json::from_slice(&bytes) {
                            Ok(events) => events,
                            Err(e) => return Some((Err(e.into()), file_number + 1)),
                        };
                        let table_events = wal_events
                            .into_iter()
                            .map(|wal| wal.into_table_event())
                            .collect();
                        Some((Ok(table_events), file_number + 1))
                    }
                    Err(e) => Some((Err(e), file_number + 1)),
                }
            }
        }))
    }

    /// Recover the flushed WALs from the file system as a flat stream. Start file number and
    /// begin_from_lsn are both inclusive.
    #[allow(dead_code)]
    pub fn recover_flushed_wals_flat(
        file_system_accessor: Arc<dyn BaseFileSystemAccess>,
        start_file_number: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<TableEvent>> + Send>> {
        WalManager::recover_flushed_wals(file_system_accessor, start_file_number)
            .flat_map(|result| match result {
                Ok(events) => stream::iter(events.into_iter().map(Ok).collect::<Vec<_>>()),
                Err(e) => stream::iter(vec![Err(e)]),
            })
            .boxed()
    }
}

#[cfg(test)]
impl WalConfig {
    pub fn get_accessor_config(&self) -> AccessorConfig {
        self.accessor_config.clone()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub mod test_utils;
