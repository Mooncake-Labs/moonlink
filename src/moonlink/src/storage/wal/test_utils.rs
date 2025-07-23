use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::mooncake_table::test_utils::{test_row, TestContext};
use crate::storage::wal::{PersistAndTruncateResult, WalEvent, WalManager};
use crate::table_notify::TableEvent;
use crate::FileSystemConfig;
use crate::Result;
use futures::StreamExt;
use std::sync::Arc;
use tokio::fs;

impl WalManager {
    /// Get the file system accessor for testing purposes
    pub fn get_file_system_accessor(&self) -> Arc<dyn BaseFileSystemAccess> {
        self.file_system_accessor.clone()
    }

    /// Mock function for how this gets called in table handler. We retrieve some data from the wal,
    /// asynchronously update the file system,
    /// and then call handle_completed_persist_and_truncate to update the wal.
    pub async fn persist_and_truncate(
        &mut self,
        last_iceberg_snapshot_lsn: Option<u64>,
    ) -> Result<()> {
        // handle the persist side
        let wal_to_persist = std::mem::take(&mut self.in_mem_wal.buf);
        let file_info_to_persist = self.get_to_persist_wal_file_info();

        let persisted_wal_file = if wal_to_persist.is_empty() {
            None
        } else {
            WalManager::persist(
                self.file_system_accessor.clone(),
                &wal_to_persist,
                &file_info_to_persist,
            )
            .await?;
            Some(file_info_to_persist)
        };

        let mut highest_deleted_file = None;

        if let Some(last_iceberg_snapshot_lsn) = last_iceberg_snapshot_lsn {
            let files_to_truncate = self.get_files_to_truncate(last_iceberg_snapshot_lsn);
            highest_deleted_file = if files_to_truncate.is_empty() {
                None
            } else {
                WalManager::delete_files(self.file_system_accessor.clone(), &files_to_truncate)
                    .await?;
                files_to_truncate.last().cloned()
            };
        }

        let persist_and_truncate_result = PersistAndTruncateResult {
            file_persisted: persisted_wal_file,
            highest_deleted_file,
        };

        self.handle_completed_persist_and_truncate(&persist_and_truncate_result);
        Ok(())
    }
}

pub async fn extract_file_contents(
    file_path: &str,
    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
) -> Vec<WalEvent> {
    let file_content = file_system_accessor.read_object(file_path).await.unwrap();
    serde_json::from_slice(&file_content).unwrap()
}

pub fn convert_to_wal_events_vector(table_events: &[TableEvent]) -> Vec<WalEvent> {
    let mut highest_lsn = 0;
    table_events
        .iter()
        .map(|event| {
            if let Some(event_lsn) = event.get_lsn_for_ingest_event() {
                highest_lsn = std::cmp::max(highest_lsn, event_lsn);
            }
            WalEvent::new(event, highest_lsn)
        })
        .collect()
}

pub async fn get_wal_logs_from_files(
    file_paths: &[&str],
    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
) -> Vec<WalEvent> {
    let mut wal_events = Vec::new();
    for file_path in file_paths {
        let events = extract_file_contents(file_path, file_system_accessor.clone()).await;
        wal_events.extend(events);
    }
    wal_events
}

pub async fn check_wal_logs_equal(
    file_paths: &[&str],
    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
    expected_events: Vec<WalEvent>,
) {
    let wal_events = get_wal_logs_from_files(file_paths, file_system_accessor.clone()).await;
    assert_eq!(wal_events, expected_events);
}

/// Helper function to compare two ingestion events by their key properties - this exists because
/// PartialEq is not implemented for TableEvent, because only a subset of the fields are relevant for comparison.
pub fn assert_ingestion_events_equal(actual: &TableEvent, expected: &TableEvent) {
    match (actual, expected) {
        (
            TableEvent::Append {
                row: row1,
                lsn: lsn1,
                xact_id: xact1,
                is_copied: copied1,
                ..
            },
            TableEvent::Append {
                row: row2,
                lsn: lsn2,
                xact_id: xact2,
                is_copied: copied2,
                ..
            },
        ) => {
            assert_eq!(row1, row2, "Append events have different rows");
            assert_eq!(lsn1, lsn2, "Append events have different LSNs");
            assert_eq!(xact1, xact2, "Append events have different xact_ids");
            assert_eq!(
                copied1, copied2,
                "Append events have different is_copied flags"
            );
        }
        (
            TableEvent::Delete {
                row: row1,
                lsn: lsn1,
                xact_id: xact1,
                ..
            },
            TableEvent::Delete {
                row: row2,
                lsn: lsn2,
                xact_id: xact2,
                ..
            },
        ) => {
            assert_eq!(row1, row2, "Delete events have different rows");
            assert_eq!(lsn1, lsn2, "Delete events have different LSNs");
            assert_eq!(xact1, xact2, "Delete events have different xact_ids");
        }
        (
            TableEvent::Commit {
                lsn: lsn1,
                xact_id: xact1,
                ..
            },
            TableEvent::Commit {
                lsn: lsn2,
                xact_id: xact2,
                ..
            },
        ) => {
            assert_eq!(lsn1, lsn2, "Commit events have different LSNs");
            assert_eq!(xact1, xact2, "Commit events have different xact_ids");
        }
        (
            TableEvent::StreamAbort { xact_id: xact1, .. },
            TableEvent::StreamAbort { xact_id: xact2, .. },
        ) => {
            assert_eq!(xact1, xact2, "StreamAbort events have different xact_ids");
        }
        (
            TableEvent::StreamFlush { xact_id: xact1, .. },
            TableEvent::StreamFlush { xact_id: xact2, .. },
        ) => {
            assert_eq!(xact1, xact2, "StreamFlush events have different xact_ids");
        }
        _ => {
            panic!("Event types don't match: {actual:?} vs {expected:?}");
        }
    }
}

/// Helper function to compare vectors of ingestion events
pub fn assert_ingestion_events_vectors_equal(actual: &[TableEvent], expected: &[TableEvent]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "Event vectors have different lengths"
    );
    for (actual_event, expected_event) in actual.iter().zip(expected.iter()) {
        assert_ingestion_events_equal(actual_event, expected_event);
    }
}

pub async fn get_table_events_vector_recovery(
    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
    start_file_name: u64,
    begin_from_lsn: u64,
) -> Vec<TableEvent> {
    // Recover events using flat stream
    let mut recovered_events = Vec::new();
    let mut stream = WalManager::recover_flushed_wals_flat(
        file_system_accessor,
        start_file_name,
        begin_from_lsn,
    );
    while let Some(result) = stream.next().await {
        match result {
            Ok(event) => recovered_events.push(event),
            Err(e) => panic!("Recovery failed: {e:?}"),
        }
    }
    recovered_events
}

// Helper function to create a WAL with some test data
pub async fn create_test_wal(context: &TestContext) -> (WalManager, Vec<TableEvent>) {
    let mut wal = WalManager::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });
    let mut expected_events = Vec::new();
    let row = test_row(1, "Alice", 30);

    for i in 0..5 {
        let event = TableEvent::Append {
            row: row.clone(),
            xact_id: None,
            lsn: 100 + i,
            is_copied: false,
        };

        wal.push(&event);
        expected_events.push(event);
    }
    (wal, expected_events)
}

// Helper to check if a directory is empty
pub async fn local_dir_is_empty(path: &std::path::Path) -> bool {
    let mut entries = fs::read_dir(path).await.unwrap();
    entries.next_entry().await.unwrap().is_none()
}
