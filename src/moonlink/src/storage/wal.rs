pub mod wal_persistence_metadata;
use crate::row::MoonlinkRow;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;
use crate::table_notify::TableEvent;
use crate::Result;
use futures::stream::{self, Stream};
use futures::{future, StreamExt};
use more_asserts::assert_ge;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

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
}

pub struct PersistAndTruncateResult {
    file_persisted: Option<WalFileInfo>,
    highest_deleted_file: Option<WalFileInfo>,
    /// The LSN of the last seen iceberg snapshot which was used to
    /// determine how the WAL was truncated.
    iceberg_snapshot_lsn: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WalFileInfo {
    file_number: u64,
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
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum WalTransactionState {
    Commit {
        start_file: u64,
        completion_lsn: u64,
    },
    Abort {
        start_file: u64,
        completion_lsn: u64,
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

    fn get_completion_lsn(&self) -> Option<u64> {
        match self {
            WalTransactionState::Open { .. } => None,
            WalTransactionState::Commit { completion_lsn, .. } => Some(*completion_lsn),
            WalTransactionState::Abort { completion_lsn, .. } => Some(*completion_lsn),
        }
    }

    fn is_closed(&self) -> bool {
        self.get_completion_lsn().is_some()
    }

    fn is_captured_in_iceberg_snapshot(&self, truncate_from_lsn: u64) -> bool {
        let completion_lsn = self.get_completion_lsn();
        // the xact has a known completion lsn by the iceberg snapshot lsn,
        // so it is captured in the iceberg snapshot
        if let Some(completion_lsn) = completion_lsn {
            if completion_lsn <= truncate_from_lsn {
                return true;
            }
        }
        false
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
    pub fn new(config: FileSystemConfig) -> Self {
        // TODO(Paul): Add a more robust constructor when implementing recovery
        Self {
            in_mem_buf: Vec::new(),
            highest_seen_lsn: 0,
            live_wal_files_tracker: Vec::new(),
            curr_file_number: 0,
            active_transactions: HashMap::new(),
            main_transaction_tracker: Vec::new(),
            // TODO(Paul): Implement object storage
            file_system_accessor: Arc::new(FileSystemAccessor::new(config)),
        }
    }

    pub fn get_file_name(file_number: u64) -> String {
        format!("wal_{file_number}.json")
    }

    // ------------------------------
    // Inserting events
    // ------------------------------

    fn get_updated_xact_state(
        table_event: &TableEvent,
        xact_state: WalTransactionState,
        highest_seen_lsn: u64,
    ) -> WalTransactionState {
        match table_event {
            TableEvent::Append { .. } | TableEvent::Delete { .. } => WalTransactionState::Open {
                start_file: xact_state.get_start_file(),
            },
            TableEvent::Commit { lsn, .. } => WalTransactionState::Commit {
                start_file: xact_state.get_start_file(),
                completion_lsn: *lsn,
            },
            TableEvent::StreamAbort { .. } => WalTransactionState::Abort {
                start_file: xact_state.get_start_file(),
                completion_lsn: highest_seen_lsn,
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

            let updated_state =
                Self::get_updated_xact_state(table_event, old_state, self.highest_seen_lsn);
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
            let updated_state =
                Self::get_updated_xact_state(table_event, old_state, self.highest_seen_lsn);
            // TODO(Paul): This could get very long and might have many commits in a single file. We can
            // coalesce all main xacts that share the same start_file and end_file into a single state.
            self.main_transaction_tracker.push(updated_state);
        };
    }

    fn push(&mut self, table_event: &TableEvent) {
        // add to in_mem_buf
        let wal_event = WalEvent::new(table_event);
        self.in_mem_buf.push(wal_event);

        // Update highest_lsn if this event has a higher LSN
        if let Some(lsn) = table_event.get_lsn_for_ingest_event() {
            assert_ge!(lsn, self.highest_seen_lsn);
            self.highest_seen_lsn = lsn;
        }

        // update transaction tracking
        self.update_transaction_tracking(table_event);
    }

    // ------------------------------
    // Preparing for truncation
    // ------------------------------

    /// Returns a list of WAL files to be truncated, following an iceberg snapshot.
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
    /// List of files returned is sorted in ascending order of file number.
    /// Should be called in preparation to asynchronously delete the files.
    pub fn get_files_to_truncate(&self, truncate_from_lsn: u64) -> Vec<WalFileInfo> {
        let xacts_still_incomplete_after_truncate = self
            .active_transactions
            .values()
            .filter(|state| !state.is_captured_in_iceberg_snapshot(truncate_from_lsn))
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
            .find(|state| !state.is_captured_in_iceberg_snapshot(truncate_from_lsn))
            .map(|state| state.get_start_file());

        if let Some(file_to_keep) = main_xact_file_to_keep {
            // if there is a main transaction that is not yet captured in the iceberg snapshot
            files_to_keep.push(file_to_keep);
        }

        // get the min of the files_to_keep and the main_xact_file_to_keep
        // if this is None, then we do not need to keep any files
        let lowest_file_to_keep = files_to_keep.iter().min();

        // get all file numbers less than the lowest file to keep as we can then delete them
        if let Some(lowest_file) = lowest_file_to_keep {
            self.live_wal_files_tracker
                .iter()
                .filter(|wal_file_info| wal_file_info.file_number < *lowest_file)
                .cloned()
                .collect()
        } else {
            self.live_wal_files_tracker.clone()
        }
    }

    // ------------------------------
    // Preparing for persistence
    // ------------------------------

    /// Take all events currently in the in_mem_buf and prepare the metadata for the next file.
    /// Resets the in_mem_buf and increments the curr_file_number.
    fn take_for_next_file(&mut self) -> Option<(Vec<WalEvent>, WalFileInfo)> {
        let events_to_persist = std::mem::take(&mut self.in_mem_buf);

        if events_to_persist.is_empty() {
            return None;
        }

        let file_info = WalFileInfo {
            file_number: self.curr_file_number,
        };
        self.curr_file_number += 1;
        Some((events_to_persist, file_info))
    }

    /// Persist a series of wal events to the file system.
    /// Should be called asynchronously using the results of take_for_next_file.
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

    // ------------------------------
    // Async persist / truncate
    // ------------------------------
    /// Delete a list of wal files from the file system.
    /// Should be called asynchronously using the results from get_files_to_truncate.
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

    // ------------------------------
    // Handling completed persistence and truncation
    // ------------------------------
    fn handle_complete_persistence(
        &mut self,
        persist_and_truncate_result: &PersistAndTruncateResult,
    ) {
        if let Some(file_persisted) = &persist_and_truncate_result.file_persisted {
            assert_eq!(file_persisted.file_number, self.curr_file_number - 1);
            // Add the persisted file to the live tracker
            self.live_wal_files_tracker.push(file_persisted.clone());
        }
    }

    /// Remove all xacts that are captured in the iceberg snapshot.
    fn cleanup_xacts(&mut self, iceberg_snapshot_lsn: u64) {
        // remove all xacts that are captured in the iceberg snapshot
        self.active_transactions
            .retain(|_, state| !state.is_captured_in_iceberg_snapshot(iceberg_snapshot_lsn));
        // remove all main xacts that are captured in the iceberg snapshot
        self.main_transaction_tracker
            .retain(|state| !state.is_captured_in_iceberg_snapshot(iceberg_snapshot_lsn));
    }

    /// This cleans up any files that no longer need to be tracked, and also any xacts that no longer need to
    /// be tracked as a result.
    fn handle_complete_truncate(&mut self, persist_and_truncate_result: &PersistAndTruncateResult) {
        if let Some(highest_deleted_file) = &persist_and_truncate_result.highest_deleted_file {
            self.live_wal_files_tracker.retain(|wal_file_info| {
                wal_file_info.file_number > highest_deleted_file.file_number
            });
        }
        if let Some(iceberg_snapshot_lsn) = persist_and_truncate_result.iceberg_snapshot_lsn {
            self.cleanup_xacts(iceberg_snapshot_lsn);
        }
    }

    /// Should be called after a persist and truncate operation has completed. Updates
    /// tracked files and transactions.
    pub fn handle_completed_persist_and_truncate(
        // For now, we handle the persist and truncate results together.
        &mut self,
        persist_and_truncate_result: &PersistAndTruncateResult,
    ) {
        self.handle_complete_persistence(persist_and_truncate_result);
        self.handle_complete_truncate(persist_and_truncate_result);
    }

    // ------------------------------
    // Recovery
    // ------------------------------

    /// Recover the flushed WALs from the file system. Start file number and begin_from_lsn are
    /// both inclusive.
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
mod tests;

#[cfg(test)]
mod test_utils;
