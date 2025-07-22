use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::mooncake_table::test_utils::{test_row, TestContext};
use crate::storage::wal::{Wal, WalEvent};
use crate::table_notify::TableEvent;
use crate::FileSystemConfig;
use futures::StreamExt;
use std::sync::Arc;

pub async fn extract_file_contents(
    file_path: &str,
    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
) -> Vec<WalEvent> {
    let file_content = file_system_accessor.read_object(file_path).await.unwrap();
    serde_json::from_slice(&file_content).unwrap()
}

pub fn convert_to_wal_events_vector(table_events: Vec<TableEvent>) -> Vec<WalEvent> {
    let mut highest_lsn = 0;
    table_events
        .into_iter()
        .map(|event| {
            if let Some(event_lsn) = event.get_lsn_for_ingest_event() {
                highest_lsn = std::cmp::max(highest_lsn, event_lsn);
            }
            WalEvent::new_from_table_event(&event, highest_lsn)
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
    wal: &Wal,
    start_file_name: u64,
    begin_from_lsn: u64,
) -> Vec<TableEvent> {
    // Recover events using flat stream
    let mut recovered_events = Vec::new();
    let mut stream = wal.recover_flushed_wals_flat(start_file_name, begin_from_lsn);
    while let Some(result) = stream.next().await {
        match result {
            Ok(event) => recovered_events.push(event),
            Err(e) => panic!("Recovery failed: {e:?}"),
        }
    }
    recovered_events
}

// Helper function to create a WAL with some test data
pub async fn create_test_wal(context: &TestContext) -> (Wal, Vec<TableEvent>) {
    let wal = Wal::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });
    let mut expected_events = Vec::new();
    let row = test_row(1, "Alice", 30);

    for i in 0..5 {
        wal.insert(&TableEvent::Append {
            row: row.clone(),
            xact_id: None,
            lsn: 100 + i,
            is_copied: false,
        })
        .await;
        expected_events.push(TableEvent::Append {
            row: row.clone(),
            xact_id: None,
            lsn: 100 + i,
            is_copied: false,
        });
    }
    (wal, expected_events)
}
