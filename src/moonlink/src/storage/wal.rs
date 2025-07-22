pub(crate) mod wal_persistence_metadata;

#[cfg(test)]
mod test_utils;

use crate::row::MoonlinkRow;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::filesystem::filesystem_config::FileSystemConfig;
use crate::table_notify::TableEvent;
use crate::Result;
use futures::stream::{self, Stream};
use futures::{future, StreamExt};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;

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

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct WalEvent {
    pub lsn: u64,
    pub event: WalEventEnum,
}

impl WalEvent {
    pub fn new_from_table_event(table_event: &TableEvent, last_highest_lsn: u64) -> Self {
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

    pub fn into_table_event(self) -> TableEvent {
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
struct InMemWal {
    /// The in_mem_wal could have insertions done by incoming CDC events
    pub buf: Vec<WalEvent>,
    /// Tracks an LSN in case of a stream flush (or similar events) which has no accompanying LSN.
    /// A new instance will take the highest_lsn from the previous instance before it is populated.
    pub highest_lsn: u64,
}

impl InMemWal {
    pub fn new(highest_lsn: u64) -> Self {
        Self {
            buf: Vec::new(),
            highest_lsn,
        }
    }
}

/// Wal tracks both the in-memory WAL and the flushed WALs.
/// The in-memory WAL may be modified concurrently with persist_and_truncate, but
/// there is only meant to be one live call to persist_and_truncate at a time.
pub struct Wal {
    in_mem_wal: Mutex<InMemWal>,
    /// The wal file numbers that are still live. Each entry is (file_number, highest_lsn within file).
    /// Gets modified by persist and truncate, which are only called serially in the persist table handler.
    live_wal_file_tracker: Mutex<Vec<(u64, u64)>>,
    // Tracks the file number to be assigned to the next persisted wal file
    curr_file_number: Mutex<u64>,

    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
}

impl Wal {
    pub fn new(config: FileSystemConfig) -> Self {
        // TODO(Paul): Add a more robust constructor when implementing recovery
        Self {
            in_mem_wal: Mutex::new(InMemWal::new(0)),
            live_wal_file_tracker: Mutex::new(Vec::new()),
            curr_file_number: Mutex::new(0),
            file_system_accessor: Arc::new(FileSystemAccessor::new(config)),
        }
    }

    pub async fn insert(&self, row: &TableEvent) {
        let mut in_mem_wal = self.in_mem_wal.lock().await;
        let wal_event = WalEvent::new_from_table_event(row, in_mem_wal.highest_lsn);
        in_mem_wal.buf.push(wal_event);

        // Update highest_lsn if this event has a higher LSN
        if let Some(lsn) = row.get_lsn_for_ingest_event() {
            in_mem_wal.highest_lsn = std::cmp::max(in_mem_wal.highest_lsn, lsn);
        }

        // TODO(Paul): Implement streaming flush (if cross threshold, begin streaming write)
    }

    async fn take_and_replace(&self) -> (Option<Vec<WalEvent>>, u64) {
        let mut in_mem_wal = self.in_mem_wal.lock().await;
        if in_mem_wal.buf.is_empty() {
            return (None, in_mem_wal.highest_lsn);
        }
        let mut new_buf = Vec::new();
        std::mem::swap(&mut in_mem_wal.buf, &mut new_buf);
        (Some(new_buf), in_mem_wal.highest_lsn)
    }

    pub fn get_file_name(file_number: u64) -> String {
        format!("wal_{file_number}.json")
    }

    #[cfg(test)]
    /// Get the file system accessor for testing purposes
    pub(crate) fn get_file_system_accessor(&self) -> Arc<dyn BaseFileSystemAccess> {
        self.file_system_accessor.clone()
    }

    async fn persist(&self) -> Result<Option<u64>> {
        let (old_wal, highest_lsn) = self.take_and_replace().await;
        if let Some(old_wal) = old_wal {
            let wal_json = serde_json::to_vec(&old_wal).unwrap();

            let mut curr_file_number = self.curr_file_number.lock().await;
            let mut live_wal_file_tracker = self.live_wal_file_tracker.lock().await;

            let wal_file_path = Wal::get_file_name(*curr_file_number);
            self.file_system_accessor
                .write_object(&wal_file_path, wal_json)
                .await?;
            live_wal_file_tracker.push((*curr_file_number, highest_lsn));
            let latest_flushed_wal_file_number = *curr_file_number;

            *curr_file_number += 1;
            Ok(Some(latest_flushed_wal_file_number))
        } else {
            Ok(None)
        }
    }

    async fn truncate_flushed_wals(&self, truncate_from_lsn: u64) -> Result<()> {
        let mut live_wal_file_tracker = self.live_wal_file_tracker.lock().await;
        // Step 1: Find the last index to be truncated
        let last_truncate_idx = live_wal_file_tracker
            .iter()
            .rposition(|&(_, highest_lsn)| highest_lsn < truncate_from_lsn);

        if let Some(idx) = last_truncate_idx {
            // Step 2: Drop all the files <= that index
            let delete_file_names = live_wal_file_tracker
                .iter()
                .take(idx + 1)
                .map(|(file_number, _)| Wal::get_file_name(*file_number))
                .collect::<Vec<String>>();
            let delete_futures = delete_file_names
                .iter()
                .map(|file_name| self.file_system_accessor.delete_object(file_name));
            future::try_join_all(delete_futures).await?;
            // Step 3: Truncate the tracking vector
            live_wal_file_tracker.drain(0..=idx);
        }
        Ok(())
    }

    pub async fn persist_and_truncate(
        &self,
        last_iceberg_snapshot_lsn: Option<u64>,
    ) -> Result<Option<u64>> {
        let latest_flushed_wal_file_number = self.persist().await?;
        if let Some(last_iceberg_snapshot_lsn) = last_iceberg_snapshot_lsn {
            self.truncate_flushed_wals(last_iceberg_snapshot_lsn)
                .await?;
        }
        Ok(latest_flushed_wal_file_number)
    }

    fn recover_flushed_wals(
        &self,
        start_file_number: u64,
        begin_from_lsn: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<TableEvent>>> + Send>> {
        let file_system_accessor = self.file_system_accessor.clone();
        Box::pin(stream::unfold(start_file_number, move |file_number| {
            let file_system_accessor = file_system_accessor.clone();
            async move {
                let file_name = Wal::get_file_name(file_number);
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

    pub fn recover_flushed_wals_flat(
        &self,
        start_file_number: u64,
        begin_from_lsn: u64,
    ) -> Pin<Box<dyn Stream<Item = Result<TableEvent>> + Send>> {
        self.recover_flushed_wals(start_file_number, begin_from_lsn)
            .flat_map(|result| match result {
                Ok(events) => stream::iter(events.into_iter().map(Ok).collect::<Vec<_>>()),
                Err(e) => stream::iter(vec![Err(e)]),
            })
            .boxed()
    }
}

#[cfg(test)]
mod tests;
