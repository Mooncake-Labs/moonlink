use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::gcs_test_utils;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::test_guard::TestGuard as GcsTestGuard;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::s3_test_utils;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::test_guard::TestGuard as S3TestGuard;
use crate::storage::mooncake_table::test_utils::test_row;
use crate::storage::wal::{WalEvent, WalManager};
use crate::storage::TestContext;
use crate::table_notify::TableEvent;
use crate::{PersistentWalMetadata, Result, WalConfig};
use futures::StreamExt;
use std::sync::Arc;

/// The ID used to test the WAL. Note that for now this could be decoupled from the
/// tableID of the mooncake table that this may be embedded in during testing,
/// as it is just for testing purposes to govern the WAL folder.
pub const WAL_TEST_TABLE_ID: &str = "1";

impl WalManager {
    /// Mock function for how this gets called in table handler. We retrieve some data from the wal,
    /// asynchronously update the file system,
    /// and then call handle_completed_wal_persistence_update to update the wal.
    pub async fn do_wal_persistence_update_for_test(
        &mut self,
        last_iceberg_snapshot_lsn: Option<u64>,
    ) -> Result<()> {
        let prepare_persistent_update = self.prepare_persistent_update(last_iceberg_snapshot_lsn);

        let (event_sender, mut event_receiver) = tokio::sync::mpsc::channel::<TableEvent>(100);

        WalManager::wal_persist_truncate_async(
            uuid::Uuid::new_v4(),
            prepare_persistent_update,
            self.file_system_accessor.clone(),
            event_sender,
        )
        .await;

        // receive the event
        while let Some(event) = event_receiver.recv().await {
            match event {
                TableEvent::PeriodicalWalPersistenceUpdateResult { result } => match result {
                    Ok(wal_persistence_update_result) => {
                        self.handle_complete_wal_persistence_update(&wal_persistence_update_result);
                    }
                    Err(e) => panic!("Error receiving wal persistence update result: {e:?}"),
                },
                _ => panic!("Unexpected event: {event:?}"),
            }
        }
        Ok(())
    }
}

// ================================================
// Helper functions for WAL file manipulation
// ================================================

// Used in conjunction with rstest to test WAL persistence in different environments.
pub enum WalTestEnv {
    // ownership needed for test guards even if unused
    #[allow(dead_code)]
    Local(WalConfig, TestContext),
    #[cfg(feature = "storage-gcs")]
    Gcs((WalConfig, GcsTestGuard)),
    #[cfg(feature = "storage-s3")]
    S3((WalConfig, S3TestGuard)),
}

impl WalTestEnv {
    pub async fn new_from_string(path_or_obj_store_indicator: &str) -> WalTestEnv {
        #[cfg(feature = "storage-gcs")]
        if path_or_obj_store_indicator == "gcs" {
            let (bucket, warehouse_uri) = gcs_test_utils::get_test_gcs_bucket_and_warehouse();
            let test_guard = GcsTestGuard::new(bucket.clone()).await;
            let gcs_storage_config = gcs_test_utils::create_gcs_storage_config(&warehouse_uri);
            let wal_config = WalConfig::new(gcs_storage_config, WAL_TEST_TABLE_ID);
            return WalTestEnv::Gcs((wal_config, test_guard));
        }

        #[cfg(feature = "storage-s3")]
        if path_or_obj_store_indicator == "s3" {
            let (bucket, warehouse_uri) = s3_test_utils::get_test_s3_bucket_and_warehouse();
            let test_guard = S3TestGuard::new(bucket.clone()).await;
            let s3_storage_config = s3_test_utils::create_s3_storage_config(&warehouse_uri);
            let wal_config = WalConfig::new(s3_storage_config, WAL_TEST_TABLE_ID);
            return WalTestEnv::S3((wal_config, test_guard));
        }

        let test_context = TestContext::new(path_or_obj_store_indicator);
        WalTestEnv::Local(
            WalConfig::default_wal_config_local(
                WAL_TEST_TABLE_ID,
                &test_context.path().to_path_buf(),
            ),
            test_context,
        )
    }

    pub fn get_wal_config(&self) -> WalConfig {
        match self {
            WalTestEnv::Local(wal_config, _) => wal_config.clone(),
            #[cfg(feature = "storage-gcs")]
            WalTestEnv::Gcs((wal_config, _)) => wal_config.clone(),
            #[cfg(feature = "storage-s3")]
            WalTestEnv::S3((wal_config, _)) => wal_config.clone(),
        }
    }
}

pub async fn extract_file_contents(
    file_path: &str,
    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
) -> Vec<WalEvent> {
    let file_content = file_system_accessor.read_object(file_path).await.unwrap();
    serde_json::from_slice(&file_content).unwrap()
}

pub async fn get_wal_logs_from_files(
    file_ids: &[u64],
    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
    wal_config: &WalConfig,
) -> Vec<WalEvent> {
    let file_paths = file_ids
        .iter()
        .map(|id| {
            WalManager::get_wal_file_path_for_mooncake_table(
                *id,
                wal_config.get_mooncake_table_id(),
            )
        })
        .collect::<Vec<String>>();
    let mut wal_events = Vec::new();
    for file_path in file_paths {
        let events = extract_file_contents(&file_path, file_system_accessor.clone()).await;
        wal_events.extend(events);
    }
    wal_events
}

pub async fn wal_file_exists(
    file_number: u64,
    file_system_accessor: Arc<dyn BaseFileSystemAccess>,
    wal_config: &WalConfig,
) -> bool {
    let file_name = WalManager::get_wal_file_path_for_mooncake_table(
        file_number,
        wal_config.get_mooncake_table_id(),
    );
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
                ..
            },
            TableEvent::Append {
                row: row2,
                lsn: lsn2,
                xact_id: xact2,
                ..
            },
        ) => row1 == row2 && lsn1 == lsn2 && xact1 == xact2,
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
        ) => row1 == row2 && lsn1 == lsn2 && xact1 == xact2,
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
        ) => lsn1 == lsn2 && xact1 == xact2,
        (
            TableEvent::StreamAbort { xact_id: xact1, .. },
            TableEvent::StreamAbort { xact_id: xact2, .. },
        ) => xact1 == xact2,
        (
            TableEvent::StreamFlush { xact_id: xact1, .. },
            TableEvent::StreamFlush { xact_id: xact2, .. },
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
    wal_metadata: &PersistentWalMetadata,
) -> Vec<TableEvent> {
    // Recover events using flat stream
    let mut recovered_events = Vec::new();
    let mut stream = WalManager::recover_flushed_wals_flat(file_system_accessor, wal_metadata);
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
            is_recovery: false,
        };

        wal.push(&event);
        expected_events.push(event);
    }
    // commit the main transaction
    let commit_event = TableEvent::Commit {
        lsn: 100 + 5,
        xact_id: None,
        is_recovery: false,
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
    let event = TableEvent::Commit {
        lsn,
        xact_id,
        is_recovery: false,
    };
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
        is_recovery: false,
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
        delete_if_exists: false,
        is_recovery: false,
    };
    wal.push(&event);
    expected_events.push(event);
}

pub fn add_new_example_stream_abort_event(
    xact_id: u32,
    wal: &mut WalManager,
    expected_events: &mut Vec<TableEvent>,
) {
    let event = TableEvent::StreamAbort {
        xact_id,
        is_recovery: false,
        closes_incomplete_wal_transaction: false,
    };
    wal.push(&event);
    expected_events.push(event);
}

#[macro_export]
macro_rules! assert_wal_file_exists {
    ($file_number:expr, $file_system_accessor:expr, $wal_config:expr) => {
        assert!(
            $crate::storage::wal::test_utils::wal_file_exists(
                $file_number,
                $file_system_accessor,
                $wal_config
            )
            .await,
            "File {} should exist",
            $file_number
        );
    };
}

#[macro_export]
macro_rules! assert_wal_file_does_not_exist {
    ($file_number:expr, $file_system_accessor:expr, $wal_config:expr) => {
        assert!(
            !$crate::storage::wal::test_utils::wal_file_exists(
                $file_number,
                $file_system_accessor,
                $wal_config
            )
            .await,
            "File {} should not exist",
            $file_number
        );
    };
}

#[macro_export]
macro_rules! assert_wal_logs_equal {
    ($file_ids:expr, $expected_events:expr, $file_system_accessor:expr, $wal_config:expr) => {
        let wal_events = $crate::storage::wal::test_utils::get_wal_logs_from_files(
            $file_ids,
            $file_system_accessor.clone(),
            $wal_config,
        )
        .await;
        assert_eq!(wal_events, $expected_events);
    };
}
