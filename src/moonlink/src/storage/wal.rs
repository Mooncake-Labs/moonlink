use crate::row::MoonlinkRow;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::factory::create_filesystem_accessor;
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::filesystem::storage_config::StorageConfig;
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
            // TODO(paul): evaluate atomic write option.
            atomic_write_dir: None,
        };
        Self {
            accessor_config: AccessorConfig::new_with_storage_config(wal_storage_config),
        }
    }

    pub fn get_accessor_config(&self) -> &AccessorConfig {
        &self.accessor_config
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
pub struct WalPersistenceUpdateResult {
    /// UUID for current persistence operation.
    #[allow(dead_code)]
    uuid: uuid::Uuid,
    prepare_persistent_update: PreparePersistentUpdate,
}

impl WalPersistenceUpdateResult {
    pub fn new(uuid: uuid::Uuid, prepare_persistent_update: PreparePersistentUpdate) -> Self {
        Self {
            uuid,
            prepare_persistent_update,
        }
    }

    pub fn get_prepare_persistent_update(&self) -> &PreparePersistentUpdate {
        &self.prepare_persistent_update
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalFileInfo {
    pub file_number: u64,
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
                ..
            } => WalEvent::Append {
                row: row.clone(),
                xact_id: *xact_id,
                lsn: *lsn,
                is_copied: *is_copied,
            },
            TableEvent::Delete {
                row, lsn, xact_id, ..
            } => WalEvent::Delete {
                row: row.clone(),
                lsn: *lsn,
                xact_id: *xact_id,
            },
            TableEvent::Commit { lsn, xact_id, .. } => WalEvent::Commit {
                lsn: *lsn,
                xact_id: *xact_id,
            },
            TableEvent::StreamAbort { xact_id, .. } => WalEvent::StreamAbort { xact_id: *xact_id },
            TableEvent::CommitFlush { lsn, xact_id, .. } => WalEvent::Commit {
                lsn: *lsn,
                xact_id: *xact_id,
            },
            TableEvent::StreamFlush { xact_id, .. } => WalEvent::StreamFlush { xact_id: *xact_id },
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
                is_recovery: false,
            },
            WalEvent::Delete { row, lsn, xact_id } => TableEvent::Delete {
                row,
                lsn,
                xact_id,
                is_recovery: false,
            },
            WalEvent::Commit { lsn, xact_id } => TableEvent::Commit {
                lsn,
                xact_id,
                is_recovery: false,
            },
            WalEvent::StreamAbort { xact_id } => TableEvent::StreamAbort {
                xact_id,
                is_recovery: false,
                closes_incomplete_wal_transaction: false,
            },
            WalEvent::StreamFlush { xact_id } => TableEvent::StreamFlush {
                xact_id,
                is_recovery: false,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WalTransactionState {
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
        lowest_file_kept: Option<u64>,
    ) -> bool {
        let completion_lsn_and_file = self.get_completion_lsn_and_file();

        // the xact has a known completion lsn by the iceberg snapshot lsn,
        // so it is captured in the iceberg snapshot
        if let Some((completion_lsn, completion_file_number)) = completion_lsn_and_file {
            #[cfg(any(debug_assertions, test))]
            {
                // here we do the check for consistency
                if let Some(iceberg_snapshot_wal_file_num) = lowest_file_kept {
                    self.check_completed_xact_consistent_with_iceberg_snapshot(
                        completion_lsn,
                        completion_file_number,
                        iceberg_snapshot_lsn,
                        iceberg_snapshot_wal_file_num,
                    );
                }
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

/// Metadata for the WAL that is persisted to the file system.
/// This is used as the single source of truth for any persistent WAL upon recovery.
/// It is meant to be persisted after a new WAL log file is flushed, and before any deletion of unused WAL log files, as this
/// file captures all these changes.
/// This captures the state of [WalManager] at the time of persistence.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct PersistentWalMetadata {
    /// The file number of the next WAL log file to be flushed.
    /// Note that this is usually the file number of the last flushed WAL log file + 1,
    /// unless the WAL is completely empty, in which case it is 0.
    curr_file_number: u64,
    /// The highest completion LSN of all completed transactions in the WAL that is persisted.
    highest_completion_lsn: u64,
    /// The list of all live WAL log files that are persisted.
    live_wal_files_tracker: Vec<WalFileInfo>,
    /// The list of all active transactions in the WAL that is persisted.
    active_transactions: HashMap<u32, WalTransactionState>,
    /// The list of all main transactions in the WAL that is persisted.
    main_transaction_tracker: Vec<WalTransactionState>,
    /// The LSN of the last iceberg snapshot that was persisted just before the WAL was persisted.
    iceberg_snapshot_lsn: Option<u64>,
}

impl PersistentWalMetadata {
    pub fn new(
        curr_file_number: u64,
        highest_completion_lsn: u64,
        live_wal_files_tracker: Vec<WalFileInfo>,
        active_transactions: HashMap<u32, WalTransactionState>,
        main_transaction_tracker: Vec<WalTransactionState>,
        iceberg_snapshot_lsn: Option<u64>,
    ) -> Self {
        Self {
            curr_file_number,
            highest_completion_lsn,
            live_wal_files_tracker,
            active_transactions,
            main_transaction_tracker,
            iceberg_snapshot_lsn,
        }
    }

    pub fn get_live_wal_files_tracker(&self) -> &Vec<WalFileInfo> {
        &self.live_wal_files_tracker
    }

    pub fn get_highest_completion_lsn(&self) -> u64 {
        self.highest_completion_lsn
    }

    pub fn get_curr_file_number(&self) -> u64 {
        self.curr_file_number
    }

    pub fn get_active_transactions(&self) -> &HashMap<u32, WalTransactionState> {
        &self.active_transactions
    }

    pub fn get_main_transaction_tracker(&self) -> &Vec<WalTransactionState> {
        &self.main_transaction_tracker
    }

    pub fn get_iceberg_snapshot_lsn(&self) -> Option<u64> {
        self.iceberg_snapshot_lsn
    }
}

/// Used to prepare all the information needed to update the persisted WAL.
/// This is prepared synchronously before being passed to the a background task.
/// Upon completion of the background task, the [WalManager] will be updated with
/// the information contained in this struct.
#[derive(Clone, PartialEq)]
pub struct PreparePersistentUpdate {
    persistent_wal_metadata: PersistentWalMetadata,
    files_to_delete: Vec<WalFileInfo>,
    accompanying_iceberg_snapshot_lsn: Option<u64>,
    files_to_persist: Option<(Vec<WalEvent>, WalFileInfo)>,
}

impl PreparePersistentUpdate {
    pub fn new(
        persistent_wal_metadata: PersistentWalMetadata,
        files_to_delete: Vec<WalFileInfo>,
        accompanying_iceberg_snapshot_lsn: Option<u64>,
        files_to_persist: Option<(Vec<WalEvent>, WalFileInfo)>,
    ) -> Self {
        Self {
            persistent_wal_metadata,
            files_to_delete,
            accompanying_iceberg_snapshot_lsn,
            files_to_persist,
        }
    }

    pub fn should_do_persistence(&self) -> bool {
        self.files_to_persist.is_some() || !self.files_to_delete.is_empty()
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
    highest_completion_lsn: u64,
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
            highest_completion_lsn: 0,
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

    pub fn get_metadata_file_name() -> String {
        "metadata_wal.json".to_string()
    }

    pub fn get_file_system_accessor(&self) -> Arc<dyn BaseFileSystemAccess> {
        self.file_system_accessor.clone()
    }

    pub fn get_highest_completion_lsn(&self) -> u64 {
        self.highest_completion_lsn
    }

    pub fn get_curr_file_number(&self) -> u64 {
        self.curr_file_number
    }

    // ------------------------------
    // Helpers to maintain WAL tracking data structures
    // ------------------------------

    fn compute_updated_live_wal_file_tracker(
        &self,
        files_to_delete: &[WalFileInfo],
        files_to_persist: &Option<WalFileInfo>,
    ) -> Vec<WalFileInfo> {
        let mut updated_live_wal_file_tracker_copy = if files_to_delete.is_empty() {
            self.live_wal_files_tracker.clone()
        } else {
            let lowest_file_to_keep = files_to_delete.last().unwrap().file_number + 1;
            let mut updated_live_wal_file_tracker_copy = self.live_wal_files_tracker.clone();
            updated_live_wal_file_tracker_copy
                .retain(|wal_file_info| wal_file_info.file_number >= lowest_file_to_keep);
            updated_live_wal_file_tracker_copy
        };

        if let Some(files_to_persist) = files_to_persist {
            updated_live_wal_file_tracker_copy.push(files_to_persist.clone());
        }
        updated_live_wal_file_tracker_copy
    }

    /// Remove all xacts that have been captured in the most recent iceberg snapshot.
    fn compute_cleanedup_xacts(
        &self,
        iceberg_snapshot_lsn: Option<u64>,
        files_to_delete: &[WalFileInfo],
    ) -> (HashMap<u32, WalTransactionState>, Vec<WalTransactionState>) {
        if iceberg_snapshot_lsn.is_none() {
            return (
                self.active_transactions.clone(),
                self.main_transaction_tracker.clone(),
            );
        }

        let iceberg_snapshot_lsn = iceberg_snapshot_lsn.unwrap();

        let lowest_file_kept = files_to_delete.last().map(|file| file.file_number + 1);
        let mut cleanedup_xacts = self.active_transactions.clone();
        // remove all xacts that are captured in the iceberg snapshot
        cleanedup_xacts.retain(|_, state| {
            !state.is_captured_in_iceberg_snapshot(iceberg_snapshot_lsn, lowest_file_kept)
        });
        let mut cleanedup_main_xacts = self.main_transaction_tracker.clone();
        // remove all main xacts that are captured in the iceberg snapshot
        cleanedup_main_xacts.retain(|state| {
            !state.is_captured_in_iceberg_snapshot(iceberg_snapshot_lsn, lowest_file_kept)
        });

        (cleanedup_xacts, cleanedup_main_xacts)
    }

    // ------------------------------
    // Inserting events
    // ------------------------------

    fn get_updated_xact_state(
        table_event: &TableEvent,
        xact_state: WalTransactionState,
        highest_completion_lsn: u64,
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
                completion_lsn: highest_completion_lsn,
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
            TableEvent::StreamAbort { xact_id, .. } => Some(*xact_id),
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
                self.highest_completion_lsn,
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
                self.highest_completion_lsn,
                self.curr_file_number,
            );
            // TODO(Paul): This could get very long and might have many commits in a single file. We can
            // coalesce all main xacts that share the same start_file and end_file into a single state.
            self.main_transaction_tracker.push(updated_state);
        };
    }

    pub fn push(&mut self, table_event: &TableEvent) {
        assert!(
            !table_event.is_recovery(),
            "Recovery events should not be added to the WAL"
        );
        // add to in_mem_buf
        let wal_event = WalEvent::new(table_event);
        self.in_mem_buf.push(wal_event);

        // Update highest_lsn if this event has a higher LSN
        if let TableEvent::Commit { lsn, .. } | TableEvent::CommitFlush { lsn, .. } = table_event {
            if *lsn > 0 {
                ma::assert_le!(self.highest_completion_lsn, *lsn, "Highest seen LSN was more than a new event's commit LSN, but incoming LSN should be monotonically increasing");
            }
            self.highest_completion_lsn = *lsn;
        }

        // update transaction tracking
        self.update_transaction_tracking(table_event);
    }

    // ------------------------------
    // Preparing for truncate
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
    pub fn get_files_to_truncate(&self, iceberg_snapshot_lsn: u64) -> Vec<WalFileInfo> {
        // get all file numbers less than the lowest file to keep as we can then delete them
        let lowest_file_to_keep = self.get_lowest_file_to_keep(iceberg_snapshot_lsn);

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
            highest_lsn: self.highest_completion_lsn,
        };
        self.curr_file_number += 1;
        Some((events_to_persist, file_info))
    }

    /// ------------------------------
    /// Preparing metadata
    /// ------------------------------
    pub fn prepare_metadata(
        &self,
        iceberg_snapshot_lsn: Option<u64>,
        files_to_delete: Vec<WalFileInfo>,
        files_to_persist: Option<WalFileInfo>,
    ) -> PersistentWalMetadata {
        let live_wal_files_tracker =
            self.compute_updated_live_wal_file_tracker(&files_to_delete, &files_to_persist);

        let (cleanedup_xacts, cleanedup_main_xacts) =
            self.compute_cleanedup_xacts(iceberg_snapshot_lsn, &files_to_delete);
        PersistentWalMetadata::new(
            self.curr_file_number,
            self.highest_completion_lsn,
            live_wal_files_tracker,
            cleanedup_xacts,
            cleanedup_main_xacts,
            iceberg_snapshot_lsn,
        )
    }

    // ------------------------------
    // Prepare everything
    // ------------------------------
    pub fn prepare_persistent_update(
        &mut self,
        iceberg_snapshot_lsn: Option<u64>,
    ) -> PreparePersistentUpdate {
        let files_to_truncate = if let Some(iceberg_snapshot_lsn) = iceberg_snapshot_lsn {
            self.get_files_to_truncate(iceberg_snapshot_lsn)
        } else {
            vec![]
        };

        let next_files_to_persist = self.extract_next_persistence_file();

        let metadata_to_persist = self.prepare_metadata(
            iceberg_snapshot_lsn,
            files_to_truncate.clone(),
            next_files_to_persist
                .as_ref()
                .map(|(_, file_info)| file_info.clone()),
        );

        PreparePersistentUpdate::new(
            metadata_to_persist,
            files_to_truncate,
            iceberg_snapshot_lsn,
            next_files_to_persist,
        )
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
    pub async fn persist_new_wal_file(
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

    pub async fn persist_metadata(
        persistent_wal_metadata: &PersistentWalMetadata,
        file_system_accessor: Arc<dyn BaseFileSystemAccess>,
    ) -> Result<()> {
        let metadata_file_name = WalManager::get_metadata_file_name();
        let metadata_bytes = serde_json::to_vec(&persistent_wal_metadata).unwrap();
        file_system_accessor
            .write_object(&metadata_file_name, metadata_bytes)
            .await?;
        Ok(())
    }

    /// This function is called when we periodically persist the WAL.
    /// It persists any new events in the WAL, and deletes any old WAL files following an iceberg snapshot.
    ///
    /// iceberg snapshot info is None only if there has not been an iceberg snapshot yet.
    pub async fn wal_persist_truncate_async(
        uuid: uuid::Uuid,
        prepare_persistent_update: PreparePersistentUpdate,
        file_system_accessor: Arc<dyn BaseFileSystemAccess>,
        table_notify: Sender<TableEvent>,
    ) {
        // Execute WAL operations
        let result = async {
            let file_system_accessor_persist = file_system_accessor.clone();

            // Order matters here in case we fail in between. We need to persist the new WAL file first, then save the new metadata,
            // and finally truncate the old WAL files.

            // (1) Persist new WAL file
            if let Some((wal_events, wal_file_info)) = &prepare_persistent_update.files_to_persist {
                WalManager::persist_new_wal_file(
                    file_system_accessor_persist.clone(),
                    wal_events,
                    wal_file_info,
                )
                .await?;
            }

            // (2) Save new metadata
            WalManager::persist_metadata(
                &prepare_persistent_update.persistent_wal_metadata,
                file_system_accessor_persist.clone(),
            )
            .await?;

            // (3) Delete old WAL files
            if !prepare_persistent_update.files_to_delete.is_empty() {
                WalManager::delete_files(
                    file_system_accessor_persist,
                    &prepare_persistent_update.files_to_delete,
                )
                .await?;
            }

            Ok(())
        }
        .await;

        // Create result and notify
        let persistence_update_result =
            result.map(|_| WalPersistenceUpdateResult::new(uuid, prepare_persistent_update));

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

    fn clean_up_xacts(&mut self, iceberg_snapshot_lsn: u64, files_to_delete: &[WalFileInfo]) {
        let (cleanedup_xacts, cleanedup_main_xacts) =
            self.compute_cleanedup_xacts(Some(iceberg_snapshot_lsn), files_to_delete);
        self.active_transactions = cleanedup_xacts;
        self.main_transaction_tracker = cleanedup_main_xacts;
    }

    fn update_live_wal_file_tracker(
        &mut self,
        files_to_delete: &[WalFileInfo],
        files_to_persist: &Option<WalFileInfo>,
    ) {
        self.live_wal_files_tracker =
            self.compute_updated_live_wal_file_tracker(files_to_delete, files_to_persist);
    }

    /// Updates the trackers for a persistence update result.
    /// Under the hood, this should be calling the exact same functions using the same iceberg LSN
    ///  as the ones used when preparing for this persistence update job
    fn update_trackers_for_persistence_update_result(
        &mut self,
        persistence_update_result: &WalPersistenceUpdateResult,
    ) {
        let accompanying_iceberg_snapshot_lsn = persistence_update_result
            .prepare_persistent_update
            .accompanying_iceberg_snapshot_lsn;

        self.update_live_wal_file_tracker(
            &persistence_update_result
                .prepare_persistent_update
                .files_to_delete,
            &persistence_update_result
                .prepare_persistent_update
                .files_to_persist
                .as_ref()
                .map(|(_, file_info)| file_info.clone()),
        );
        if let Some(accompanying_iceberg_snapshot_lsn) = accompanying_iceberg_snapshot_lsn {
            self.clean_up_xacts(
                accompanying_iceberg_snapshot_lsn,
                &persistence_update_result
                    .prepare_persistent_update
                    .files_to_delete,
            );
        }

        #[cfg(any(debug_assertions, test))]
        {
            assert_eq!(
                self.live_wal_files_tracker,
                persistence_update_result
                    .prepare_persistent_update
                    .persistent_wal_metadata
                    .live_wal_files_tracker,
                "live wal files stored in metadata should match the live wal files tracker"
            );

            // test to check that the xacts in the metadata snapshot are a
            // subset of the xacts in the curr active transactions map.
            let xact_map = self.active_transactions.clone();
            let xact_map_from_metadata = persistence_update_result
                .prepare_persistent_update
                .persistent_wal_metadata
                .active_transactions
                .clone();
            // we check that the persisted xact map contains all xacts that should have been kept
            for (xact_id, xact_state) in xact_map.iter() {
                // we skip aborted xacts, they may be dropped in the middle of the persistence update
                if !matches!(xact_state, WalTransactionState::Abort { .. }) {
                    // if the xact has completed
                    if let Some((completion_lsn, _)) = xact_state.get_completion_lsn_and_file() {
                        // we first check that completion LSN has to be greater than the iceberg snapshot lsn
                        if let Some(iceberg_snapshot_lsn) = persistence_update_result
                            .prepare_persistent_update
                            .accompanying_iceberg_snapshot_lsn
                        {
                            ma::assert_gt!(
                                completion_lsn, iceberg_snapshot_lsn,
                                "completion lsn {completion_lsn} should be greater than the iceberg snapshot lsn {iceberg_snapshot_lsn}"
                            );
                        }
                        // now, if completion lsn is <= the persisted wal highest seen lsn, then it should be in the persisted metadata
                        if completion_lsn
                            <= persistence_update_result
                                .prepare_persistent_update
                                .persistent_wal_metadata
                                .highest_completion_lsn
                        {
                            assert!(xact_map_from_metadata.contains_key(xact_id), "xact_id {xact_id} with state {xact_state:?} should be in the persisted metadata, but is not. Recently persisted metadata: {xact_map_from_metadata:?} Recently updated Metadata: {xact_map:?}");
                        }
                    }
                }
            }
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
        self.update_trackers_for_persistence_update_result(wal_persistence_update_result);

        let highest_lsn = wal_persistence_update_result
            .prepare_persistent_update
            .files_to_persist
            .as_ref()
            .map(|(_, wal_file_info)| wal_file_info.highest_lsn);

        highest_lsn
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
    #[allow(dead_code)]
    pub async fn recover_from_persistent_wal_metadata(
        file_system_accessor: Arc<dyn BaseFileSystemAccess>,
    ) -> Option<PersistentWalMetadata> {
        let metadata_file_name = WalManager::get_metadata_file_name();
        if !file_system_accessor
            .object_exists(&metadata_file_name)
            .await
            .expect("failed to check if metadata file exists")
        {
            return None;
        }

        let metadata_bytes = file_system_accessor
            .read_object(&metadata_file_name)
            .await
            .unwrap();

        Some(serde_json::from_slice(&metadata_bytes).expect("failed to parse wal metadata"))
    }

    #[allow(dead_code)]
    pub fn from_persistent_wal_metadata(
        file_system_accessor: Arc<dyn BaseFileSystemAccess>,
        persistent_wal_metadata: PersistentWalMetadata,
    ) -> Self {
        Self {
            in_mem_buf: Vec::new(),
            highest_completion_lsn: persistent_wal_metadata.highest_completion_lsn,
            live_wal_files_tracker: persistent_wal_metadata.live_wal_files_tracker,
            curr_file_number: persistent_wal_metadata.curr_file_number,
            active_transactions: persistent_wal_metadata.active_transactions,
            main_transaction_tracker: persistent_wal_metadata.main_transaction_tracker,
            file_system_accessor,
        }
    }

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

    #[allow(dead_code)]
    pub async fn get_xact_map_from_wal_events(
        mut wal_events: Pin<Box<dyn Stream<Item = Result<TableEvent>> + Send>>,
    ) -> HashMap<u32, WalTransactionState> {
        let mut xact_map = HashMap::new();
        while let Some(event_result) = wal_events.next().await {
            match event_result {
                Ok(TableEvent::Commit {
                    xact_id: Some(xact_id),
                    lsn,
                    ..
                }) => {
                    xact_map.insert(
                        xact_id,
                        WalTransactionState::Commit {
                            start_file: 0,
                            completion_lsn: lsn,
                            file_end: 0,
                        },
                    );
                }
                Ok(TableEvent::StreamAbort { xact_id, .. }) => {
                    xact_map.insert(
                        xact_id,
                        WalTransactionState::Abort {
                            start_file: 0,
                            completion_lsn: 0,
                            file_end: 0,
                        },
                    );
                }
                Ok(TableEvent::Append { xact_id, .. }) | Ok(TableEvent::Delete { xact_id, .. }) => {
                    if let Some(xact_id) = xact_id {
                        xact_map.insert(xact_id, WalTransactionState::Open { start_file: 0 });
                    }
                }
                _ => {}
            }
        }
        xact_map
    }

    /// Returns true if the streaming xact event should be applied before the source replay lsn.
    /// Takes in an xact_map which contains the states of each streaming xact by the end of the WAL.
    #[allow(dead_code)]
    fn streaming_xact_event_should_be_applied_before_source_replay(
        xact_id: u32,
        xact_map: &HashMap<u32, WalTransactionState>,
        source_replay_lsn: u64,
    ) -> bool {
        // expect the xact id to be in the xact map
        let xact_state = xact_map
            .get(&xact_id)
            .expect("xact id should always be found in xact map tracking final lsns");
        match xact_state {
            WalTransactionState::Commit { completion_lsn, .. } => {
                // check if the completion lsn is before the source replay lsn
                *completion_lsn < source_replay_lsn
            }
            WalTransactionState::Abort { .. } => {
                // no-op
                false
            }
            WalTransactionState::Open { .. } => {
                // if still open, it means postgres will replay this event because we have not yet flushed it in the WAL
                false
            }
        }
    }

    /// Returns true if the lsn is before the last iceberg snapshot lsn.
    /// Assumes that there is no last iceberg snapshot lsn if the option is None.
    #[allow(dead_code)]
    fn event_already_captured_in_iceberg_snapshot(
        lsn: u64,
        last_iceberg_snapshot_lsn: Option<u64>,
    ) -> bool {
        if let Some(last_iceberg_snapshot_lsn) = last_iceberg_snapshot_lsn {
            lsn <= last_iceberg_snapshot_lsn
        } else {
            false
        }
    }

    /// We reapply all WAL events that are already committed transactions at the time of recovery.
    /// Meaning that any open transactions will not be reapplied.
    /// However, events that are already captured in the iceberg snapshot will not be reapplied.
    pub fn should_reapply_wal_event(
        event: &TableEvent,
        xact_map: &HashMap<u32, WalTransactionState>,
        highest_committed_lsn: u64,
        last_iceberg_snapshot_lsn: Option<u64>,
    ) -> bool {
        match event {
            // for everything, check if already in iceberg snapshot
            TableEvent::Append { lsn, xact_id, .. }
            | TableEvent::Delete { lsn, xact_id, .. }
            | TableEvent::Commit { lsn, xact_id, .. } => {
                // Streaming xacts
                if let Some(xact_id) = xact_id {
                    match xact_map.get(xact_id) {
                        Some(WalTransactionState::Commit { completion_lsn, .. })
                        | Some(WalTransactionState::Abort { completion_lsn, .. }) => {
                            // transaction was already committed, we should reapply it if it is NOT captured in the iceberg snapshot
                            !WalManager::event_already_captured_in_iceberg_snapshot(
                                *completion_lsn,
                                last_iceberg_snapshot_lsn,
                            )
                        }
                        Some(WalTransactionState::Open { .. }) => {
                            // if the xact is still open, it means postgres will replay this event because we have not yet flushed it in the WAL
                            false
                        }
                        // if the xact is not in the xact map, it means it is closed before the iceberg snapshot (ie we are already not tracking it)
                        None => {
                            #[cfg(any(debug_assertions, test))]
                            if let TableEvent::Commit { lsn, .. } = event {
                                assert!(WalManager::event_already_captured_in_iceberg_snapshot(
                                    *lsn,
                                    last_iceberg_snapshot_lsn,
                                ), "an untracked streaming xact should be captured in the iceberg snapshot, but it was not");
                            }
                            false
                        }
                    }
                } else {
                    // Main xact - if it is <= the iceberg snapshot lsn, it is already captured in the iceberg snapshot
                    let already_captured_in_iceberg_snapshot = last_iceberg_snapshot_lsn.is_some()
                        && *lsn <= last_iceberg_snapshot_lsn.unwrap();
                    // in the main xact, if the lsn is <= the lsn of the highest commit, it means the transaction has committed
                    let is_completed_transaction = *lsn <= highest_committed_lsn;
                    !already_captured_in_iceberg_snapshot && is_completed_transaction
                }
            }
            TableEvent::CommitFlush { .. } | TableEvent::StreamFlush { .. } => {
                // no-ops
                false
            }
            _ => unimplemented!("TableEvent variant not supported for WAL: {:?}", event),
        }
    }

    /// note that in recovery this is always called before we start replication,
    /// so WAL events get sent to the sink before we get replay events from the source itself,
    /// thus ensuring that we still receive events in the correct order
    pub async fn replay_recovery_from_wal(
        event_sender_clone: Sender<TableEvent>,
        persistent_wal_metadata: Option<PersistentWalMetadata>,
        wal_file_accessor: Arc<dyn BaseFileSystemAccess>,
        last_iceberg_snapshot_lsn: Option<u64>,
    ) -> Result<()> {
        if persistent_wal_metadata.is_none() {
            return Ok(());
        }
        let persistent_wal_metadata = persistent_wal_metadata.unwrap();

        let starting_wal = persistent_wal_metadata.get_live_wal_files_tracker().first();
        if starting_wal.is_none() {
            return Ok(());
        }
        let starting_wal_file_number = starting_wal.unwrap().file_number;

        let active_xacts = persistent_wal_metadata.get_active_transactions().clone();

        let mut wal_events_stream = WalManager::recover_flushed_wals_flat(
            wal_file_accessor.clone(),
            starting_wal_file_number,
        );

        while let Some(table_event) = wal_events_stream.next().await {
            if let Ok(mut table_event) = table_event {
                // we reapply any events that would come BEFORE the postgres replay LSN
                if WalManager::should_reapply_wal_event(
                    &table_event,
                    &active_xacts,
                    persistent_wal_metadata.get_highest_completion_lsn(),
                    last_iceberg_snapshot_lsn,
                ) {
                    table_event.set_is_recovery(true);
                    event_sender_clone
                        .send(table_event)
                        .await
                        .expect("failed to send table event during recovery");
                }
            }
        }

        // there are open transactions in the WAL that will be re-sent, so we mark them as aborted to avoid duplicate events
        for (xact_id, xact_state) in active_xacts {
            if let WalTransactionState::Open { .. } = xact_state {
                event_sender_clone
                    .send(TableEvent::StreamAbort {
                        xact_id,
                        is_recovery: false,
                        closes_incomplete_wal_transaction: true,
                    })
                    .await
                    .expect("failed to send StreamAbort event to closed xact during recovery");
            }
        }

        let highest_completion_lsn = persistent_wal_metadata.get_highest_completion_lsn();
        event_sender_clone
            .send(TableEvent::FinishRecovery {
                highest_completion_lsn,
            })
            .await
            .expect(
                "failed to send FinishRecovery event to close incomplete xacts during recovery",
            );

        Ok(())
    }
}

impl std::fmt::Debug for PersistentWalMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentWalMetadata")
            .field("curr_file_number", &self.curr_file_number)
            .field("highest_completion_lsn", &self.highest_completion_lsn)
            .field("iceberg_snapshot_lsn", &self.iceberg_snapshot_lsn)
            .field(
                "live wal files tracker number",
                &self.live_wal_files_tracker.len(),
            )
            .field(
                "active transactions number",
                &self.active_transactions.len(),
            )
            .field(
                "main transaction tracker number",
                &self.main_transaction_tracker.len(),
            )
            .finish()
    }
}

impl std::fmt::Debug for PreparePersistentUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (files_to_persist_num, wal_file_info) =
            if let Some((files_to_persist, wal_file_info)) = &self.files_to_persist {
                (files_to_persist.len(), Some(wal_file_info.clone()))
            } else {
                (0, None)
            };

        f.debug_struct("PreparePersistentUpdate")
            .field("persistent_wal_metadata", &self.persistent_wal_metadata)
            .field(
                "accompanying_iceberg_snapshot_lsn",
                &self.accompanying_iceberg_snapshot_lsn,
            )
            .field("files to delete number", &self.files_to_delete.len())
            .field("files to persist number", &files_to_persist_num)
            .field("wal file info", &wal_file_info)
            .finish()
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub mod test_utils;
