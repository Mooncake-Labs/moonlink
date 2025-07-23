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
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum WalEventEnum {
    Append {
        row: MoonlinkRow,
        xact_id: Option<u32>,
        is_copied: bool,
    },
    Delete {
        row: MoonlinkRow,
        xact_id: Option<u32>,
    },
    Commit {
        xact_id: Option<u32>,
    },
    StreamAbort {
        xact_id: u32,
    },
    StreamFlush {
        xact_id: u32,
    },
}

pub struct PersistAndTruncateResult {
    file_persisted: Option<WalFileInfo>,
    highest_deleted_file: Option<WalFileInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WalFileInfo {
    file_number: u64,
    highest_lsn: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct WalEvent {
    lsn: u64,
    event: WalEventEnum,
}

impl WalEvent {
    fn new(table_event: &TableEvent, last_highest_lsn: u64) -> Self {
        // Try to get LSN from the table event, fallback to last_highest_lsn for events without LSN
        let lsn = table_event
            .get_lsn_for_ingest_event()
            .unwrap_or(last_highest_lsn);

        let event = match table_event {
            TableEvent::Append {
                row,
                xact_id,
                is_copied,
                ..
            } => WalEventEnum::Append {
                row: row.clone(),
                xact_id: *xact_id,
                is_copied: *is_copied,
            },
            TableEvent::Delete { row, xact_id, .. } => WalEventEnum::Delete {
                row: row.clone(),
                xact_id: *xact_id,
            },
            TableEvent::Commit { xact_id, .. } => WalEventEnum::Commit { xact_id: *xact_id },
            TableEvent::StreamAbort { xact_id } => WalEventEnum::StreamAbort { xact_id: *xact_id },
            TableEvent::StreamFlush { xact_id } => WalEventEnum::StreamFlush { xact_id: *xact_id },
            _ => {
                unimplemented!("Invalid table event for WAL: {:?}", table_event)
            }
        };

        Self { lsn, event }
    }

    fn into_table_event(self) -> TableEvent {
        match self.event {
            WalEventEnum::Append {
                row,
                xact_id,
                is_copied,
            } => TableEvent::Append {
                row,
                xact_id,
                is_copied,
                lsn: self.lsn,
            },
            WalEventEnum::Delete { row, xact_id } => TableEvent::Delete {
                row,
                xact_id,
                lsn: self.lsn,
            },
            WalEventEnum::Commit { xact_id } => TableEvent::Commit {
                xact_id,
                lsn: self.lsn,
            },
            WalEventEnum::StreamAbort { xact_id } => TableEvent::StreamAbort { xact_id },
            WalEventEnum::StreamFlush { xact_id } => TableEvent::StreamFlush { xact_id },
        }
    }
}
pub struct InMemWal {
    /// The in_mem_wal could have insertions done by incoming CDC events
    pub buf: Vec<WalEvent>,
    /// Tracks an LSN in case of a stream flush (or similar events) which has no accompanying LSN.
    /// A new instance will take the highest_lsn from the previous instance before it is populated.
    highest_lsn: u64,
}

impl InMemWal {
    fn new(highest_lsn: u64) -> Self {
        Self {
            buf: Vec::new(),
            highest_lsn,
        }
    }

    fn push(&mut self, table_event: &TableEvent) {
        let wal_event = WalEvent::new(table_event, self.highest_lsn);
        self.buf.push(wal_event);

        // Update highest_lsn if this event has a higher LSN
        if let Some(lsn) = table_event.get_lsn_for_ingest_event() {
            assert_ge!(lsn, self.highest_lsn);
            self.highest_lsn = lsn;
        }
    }
}

/// Wal tracks both the in-memory WAL and the flushed WALs.
/// Note that wal manager is meant to be used in a single thread. While
/// persist and delete_files can be called asynchronously, their results returned
/// from those operations have to be handled serially.
/// There is one instance of WalManager per table.
pub struct WalManager {
    /// In Mem Wal that gets appended to. When we need to flush, we call take on the buffer inside.
    pub in_mem_wal: InMemWal,
    /// The wal file numbers that are still live.
    live_wal_files_tracker: Vec<WalFileInfo>,
    /// Tracks the file number to be assigned to the next persisted wal file
    curr_file_number: u64,

    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
}

impl WalManager {
    pub fn new(config: FileSystemConfig) -> Self {
        // TODO(Paul): Add a more robust constructor when implementing recovery
        Self {
            in_mem_wal: InMemWal::new(0),
            live_wal_files_tracker: Vec::new(),
            curr_file_number: 0,
            file_system_accessor: Arc::new(FileSystemAccessor::new(config)),
        }
    }

    pub fn push(&mut self, table_event: &TableEvent) {
        self.in_mem_wal.push(table_event);
        // TODO(Paul): Implement streaming flush (if cross threshold, begin streaming write)
    }

    pub fn get_file_name(file_number: u64) -> String {
        format!("wal_{file_number}.json")
    }

    /// Get the file info for the next wal file to be persisted.
    pub fn get_to_persist_wal_file_info(&self) -> WalFileInfo {
        WalFileInfo {
            file_number: self.curr_file_number,
            highest_lsn: self.in_mem_wal.highest_lsn,
        }
    }

    /// Persist a series of wal events to the file system.
    /// Should be called asynchronously after we call take on the in_mem_wal.
    pub async fn persist(
        file_system_accessor: Arc<dyn BaseFileSystemAccess>,
        wal_to_persist: &Vec<WalEvent>,
        wal_file_info: &WalFileInfo,
    ) -> Result<()> {
        if !wal_to_persist.is_empty() {
            let wal_json = serde_json::to_vec(&wal_to_persist).unwrap();

            let wal_file_path = WalManager::get_file_name(wal_file_info.file_number);
            file_system_accessor
                .write_object(&wal_file_path, wal_json)
                .await?;
        }
        Ok(())
    }

    fn handle_complete_persistence(
        &mut self,
        persist_and_truncate_result: &PersistAndTruncateResult,
    ) {
        if let Some(file_persisted) = &persist_and_truncate_result.file_persisted {
            assert_eq!(file_persisted.file_number, self.curr_file_number);
            // Add the persisted file to the live tracker
            self.live_wal_files_tracker.push(file_persisted.clone());
            self.curr_file_number += 1;
        }
    }

    /// Returns a list of files to be dropped, i.e. all files with highest_lsn < truncate_from_lsn
    /// List of files returned is sorted in ascending order of file number.
    /// Should be called in preparation to asynchronously delete the files.
    pub fn get_files_to_truncate(&self, truncate_from_lsn: u64) -> Vec<WalFileInfo> {
        let last_truncate_idx = self
            .live_wal_files_tracker
            .iter()
            .rposition(|wal_file_info| wal_file_info.highest_lsn < truncate_from_lsn);

        if let Some(idx) = last_truncate_idx {
            self.live_wal_files_tracker
                .iter()
                .take(idx + 1)
                .cloned()
                .collect::<Vec<WalFileInfo>>()
        } else {
            Vec::new()
        }
    }

    /// Delete a list of wal files from the file system.
    /// Should be called asynchronously after we call get_files_to_truncate.
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

    fn handle_complete_truncate(&mut self, persist_and_truncate_result: &PersistAndTruncateResult) {
        if let Some(highest_deleted_file) = &persist_and_truncate_result.highest_deleted_file {
            let last_truncate_idx = self
                .live_wal_files_tracker
                .iter()
                .rposition(|wal_file_info| wal_file_info == highest_deleted_file);

            assert!(last_truncate_idx.is_some());
            // Remove all files up to and including the last truncated file
            self.live_wal_files_tracker
                .drain(0..=last_truncate_idx.unwrap());
        }
    }

    /// For now, we handle the persist and truncate results together.
    pub fn handle_completed_persist_and_truncate(
        &mut self,
        persist_and_truncate_result: &PersistAndTruncateResult,
    ) {
        self.handle_complete_persistence(persist_and_truncate_result);
        self.handle_complete_truncate(persist_and_truncate_result);
    }

    /// Recover the flushed WALs from the file system. Start file number and begin_from_lsn are
    /// both inclusive.
    fn recover_flushed_wals(
        file_system_accessor: Arc<dyn BaseFileSystemAccess>,
        start_file_number: u64,
        begin_from_lsn: u64,
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

                        let filtered_wal_events = {
                            if wal_events.first().unwrap().lsn >= begin_from_lsn {
                                wal_events
                            } else {
                                wal_events
                                    .into_iter()
                                    .filter(|event| event.lsn >= begin_from_lsn)
                                    .collect()
                            }
                        };

                        let table_events = filtered_wal_events
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
        begin_from_lsn: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<TableEvent>> + Send>> {
        WalManager::recover_flushed_wals(file_system_accessor, start_file_number, begin_from_lsn)
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
