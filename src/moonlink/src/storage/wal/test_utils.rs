use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::mooncake_table::test_utils::test_row;
use crate::storage::wal::{WalEvent, WalManager, WalPersistAndTruncateResult};
use crate::table_notify::TableEvent;
use crate::{Result, WalConfig};
use futures::StreamExt;
use std::sync::Arc;
use tokio::fs;

/// The ID used to test the WAL. Note that for now this could be decoupled from the
/// tableID of the mooncake table that this may be embedded in during testing,
/// as it is just for testing purposes to govern the WAL folder.
pub const WAL_TEST_TABLE_ID: &str = "1";

impl WalManager {
    /// Mock function for how this gets called in table handler. We retrieve some data from the wal,
    /// asynchronously update the file system,
    /// and then call handle_completed_persist_and_truncate to update the wal.
    pub async fn persist_and_truncate(
        &mut self,
        last_iceberg_snapshot_lsn: Option<u64>,
    ) -> Result<()> {
        // handle the persist side
        let mut persisted_wal_file = None;
        let to_persist_wal_info = self.extract_next_persistent_file();
        if let Some((wal_to_persist, file_info_to_persist)) = to_persist_wal_info {
            WalManager::persist(
                self.file_system_accessor.clone(),
                &wal_to_persist,
                &file_info_to_persist,
            )
            .await?;
            persisted_wal_file = Some(file_info_to_persist);
        }

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

        let persist_and_truncate_result = WalPersistAndTruncateResult {
            file_persisted: persisted_wal_file,
            highest_deleted_file,
            iceberg_snapshot_lsn: last_iceberg_snapshot_lsn,
        };

        self.handle_completed_persistence_and_truncate(&persist_and_truncate_result);
        Ok(())
    }
}

// ================================================
// Helper functions for WAL file manipulation
// TODO(Paul): Rework these when implementing object storage WAL
// ================================================

pub async fn extract_file_contents(
    file_path: &str,
    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
) -> Vec<WalEvent> {
    let file_content = file_system_accessor.read_object(file_path).await.unwrap();
    serde_json::from_slice(&file_content).unwrap()
}

/// Helper to check if a directory is empty
pub async fn local_dir_is_empty(path: &std::path::Path) -> bool {
    // first check if the directory exists
    if !fs::try_exists(path).await.unwrap() {
        return true;
    }
    // then check if it's empty
    let mut entries = fs::read_dir(path).await.unwrap();
    entries.next_entry().await.unwrap().is_none()
}

pub async fn get_wal_logs_from_files(
    file_ids: &[u64],
    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
) -> Vec<WalEvent> {
    let file_paths = file_ids
        .iter()
        .map(|id| WalManager::get_file_name(*id))
        .collect::<Vec<String>>();
    let mut wal_events = Vec::new();
    for file_path in file_paths {
        let events = extract_file_contents(&file_path, file_system_accessor.clone()).await;
        wal_events.extend(events);
    }
    wal_events
}

pub async fn wal_file_exists(
    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
    file_number: u64,
) -> bool {
    let file_name = WalManager::get_file_name(file_number);
    file_system_accessor
        .object_exists(&file_name)
        .await
        .unwrap()
}

// ================================================
// End of WAL file manipulation helpers
// ================================================

pub fn convert_to_wal_events_vector(table_events: &[TableEvent]) -> Vec<WalEvent> {
    table_events.iter().map(WalEvent::new).collect()
}

/// Helper function to compare two ingestion events by their key properties - this exists because
/// PartialEq is not implemented for TableEvent, because only a subset of the fields are relevant for comparison.
/// Returns true if the events are equal, false otherwise.
pub fn ingestion_events_equal(actual: &TableEvent, expected: &TableEvent) -> bool {
    match (actual, expected) {
        (
            TableEvent::Append {
                row: row1,
                lsn: lsn1,
                xact_id: xact1,
                is_copied: copied1,
            },
            TableEvent::Append {
                row: row2,
                lsn: lsn2,
                xact_id: xact2,
                is_copied: copied2,
            },
        ) => row1 == row2 && lsn1 == lsn2 && xact1 == xact2 && copied1 == copied2,
        (
            TableEvent::Delete {
                row: row1,
                lsn: lsn1,
                xact_id: xact1,
            },
            TableEvent::Delete {
                row: row2,
                lsn: lsn2,
                xact_id: xact2,
            },
        ) => row1 == row2 && lsn1 == lsn2 && xact1 == xact2,
        (
            TableEvent::Commit {
                lsn: lsn1,
                xact_id: xact1,
            },
            TableEvent::Commit {
                lsn: lsn2,
                xact_id: xact2,
            },
        ) => lsn1 == lsn2 && xact1 == xact2,
        (
            TableEvent::StreamAbort { xact_id: xact1 },
            TableEvent::StreamAbort { xact_id: xact2 },
        ) => xact1 == xact2,
        (
            TableEvent::StreamFlush { xact_id: xact1 },
            TableEvent::StreamFlush { xact_id: xact2 },
        ) => xact1 == xact2,
        _ => false,
    }
}

/// Helper function to compare two ingestion events by their key properties - this exists because
/// PartialEq is not implemented for TableEvent, because only a subset of the fields are relevant for comparison.
pub fn assert_ingestion_events_equal(actual: &TableEvent, expected: &TableEvent) {
    if !ingestion_events_equal(actual, expected) {
        panic!("Events are not equal: {actual:?} vs {expected:?}");
    }
}

/// Helper function to assert that two ingestion events are not equal by their key properties.
pub fn assert_ingestion_events_not_equal(actual: &TableEvent, expected: &TableEvent) {
    if ingestion_events_equal(actual, expected) {
        panic!("Events should not be equal but they are: {actual:?} vs {expected:?}");
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

pub fn assert_wal_events_contains(from_wal_events: &[TableEvent], expected_events: &[TableEvent]) {
    for expected_event in expected_events {
        let mut found = false;
        for event in from_wal_events {
            if ingestion_events_equal(event, expected_event) {
                found = true;
                break;
            }
        }
        assert!(
            found,
            "Event {expected_event:?} not found in from_wal_events {from_wal_events:?}"
        );
    }
}

pub fn assert_wal_events_does_not_contain(
    from_wal_events: &[TableEvent],
    not_expected_events: &[TableEvent],
) {
    // just a naive double loop for now
    for event in from_wal_events {
        for not_expected_event in not_expected_events {
            assert_ingestion_events_not_equal(event, not_expected_event);
        }
    }
}

pub async fn get_table_events_vector_recovery(
    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
    start_file_name: u64,
) -> Vec<TableEvent> {
    // Recover events using flat stream
    let mut recovered_events = Vec::new();
    let mut stream = WalManager::recover_flushed_wals_flat(file_system_accessor, start_file_name);
    while let Some(result) = stream.next().await {
        match result {
            Ok(event) => recovered_events.push(event),
            Err(e) => panic!("Recovery failed: {e:?}"),
        }
    }
    recovered_events
}

/// Helper function to create a WAL with some test data
pub async fn create_test_wal(wal_config: WalConfig) -> (WalManager, Vec<TableEvent>) {
    let mut wal = WalManager::new(&wal_config);
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
    // commit the main transaction
    let commit_event = TableEvent::Commit {
        lsn: 100 + 5,
        xact_id: None,
    };
    wal.push(&commit_event);
    expected_events.push(commit_event);
    (wal, expected_events)
}

pub fn add_new_example_commit_event(
    lsn: u64,
    xact_id: Option<u32>,
    wal: &mut WalManager,
    expected_events: &mut Vec<TableEvent>,
) {
    let event = TableEvent::Commit { lsn, xact_id };
    wal.push(&event);
    expected_events.push(event);
}

pub fn add_new_example_append_event(
    lsn: u64,
    xact_id: Option<u32>,
    wal: &mut WalManager,
    expected_events: &mut Vec<TableEvent>,
) {
    let event = TableEvent::Append {
        row: test_row(1, "Alice", 30),
        lsn,
        xact_id,
        is_copied: false,
    };
    wal.push(&event);
    expected_events.push(event);
}

pub fn add_new_example_delete_event(
    lsn: u64,
    xact_id: Option<u32>,
    wal: &mut WalManager,
    expected_events: &mut Vec<TableEvent>,
) {
    let event = TableEvent::Delete {
        row: test_row(1, "Alice", 30),
        lsn,
        xact_id,
    };
    wal.push(&event);
    expected_events.push(event);
}

pub fn add_new_example_stream_abort_event(
    xact_id: u32,
    wal: &mut WalManager,
    expected_events: &mut Vec<TableEvent>,
) {
    let event = TableEvent::StreamAbort { xact_id };
    wal.push(&event);
    expected_events.push(event);
}

#[macro_export]
macro_rules! assert_wal_file_exists {
    ($file_system_accessor:expr, $file_number:expr) => {
        assert!(
            $crate::storage::wal::test_utils::wal_file_exists($file_system_accessor, $file_number)
                .await,
            "File {} should exist",
            $file_number
        );
    };
}

#[macro_export]
macro_rules! assert_wal_file_does_not_exist {
    ($file_system_accessor:expr, $file_number:expr) => {
        assert!(
            !$crate::storage::wal::test_utils::wal_file_exists($file_system_accessor, $file_number)
                .await,
            "File {} should not exist",
            $file_number
        );
    };
}

#[macro_export]
macro_rules! assert_wal_logs_equal {
    ($file_ids:expr, $file_system_accessor:expr, $expected_events:expr) => {
        let wal_events = $crate::storage::wal::test_utils::get_wal_logs_from_files(
            $file_ids,
            $file_system_accessor.clone(),
        )
        .await;
        assert_eq!(wal_events, $expected_events);
    };
}
