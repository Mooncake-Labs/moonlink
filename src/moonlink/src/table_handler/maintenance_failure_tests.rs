//! Failed background maintenance must not kill the table, leak state, or block later maintenance.
use super::test_utils::*;
use crate::table_handler::table_handler_state::{MaintenanceProcessStatus, TableHandlerState};
use crate::{Error, TableEventManager};

use iceberg::{Error as IcebergError, ErrorKind};
use std::os::unix::fs::PermissionsExt;

/// Background compaction has no subscriber on the maintenance channel; reporting its failure must
/// not panic.
#[tokio::test]
async fn test_background_compaction_error_without_subscriber_does_not_panic() {
    let (sender, _receiver) = crate::event_sync::create_table_event_syncer();
    let mut state = TableHandlerState::new(
        sender.table_maintenance_completion_tx.clone(),
        sender.force_snapshot_completion_tx.clone(),
        None,
        None,
    );
    state.table_maintenance_process_status = MaintenanceProcessStatus::InProcess;
    let err: crate::Result<crate::storage::mooncake_table::DataCompactionResult> =
        Err(Error::from(IcebergError::new(
            ErrorKind::Unexpected,
            "compaction failed",
        )));
    state.mark_data_compaction_completed(&err).await;
    assert_eq!(
        state.table_maintenance_process_status,
        MaintenanceProcessStatus::Unrequested
    );
}

/// A failed index merge returns maintenance to idle instead of leaving it "ready to persist".
#[tokio::test]
async fn test_failed_index_merge_returns_to_idle() {
    let (sender, _receiver) = crate::event_sync::create_table_event_syncer();
    let mut rx = sender.table_maintenance_completion_tx.subscribe();
    let mut state = TableHandlerState::new(
        sender.table_maintenance_completion_tx.clone(),
        sender.force_snapshot_completion_tx.clone(),
        None,
        None,
    );
    state.table_maintenance_process_status = MaintenanceProcessStatus::InProcess;
    let err: crate::Result<crate::storage::mooncake_table::FileIndiceMergeResult> = Err(
        Error::from(IcebergError::new(ErrorKind::Unexpected, "merge failed")),
    );
    state.mark_index_merge_completed(&err).await;
    assert_eq!(
        state.table_maintenance_process_status,
        MaintenanceProcessStatus::Unrequested
    );
    assert!(rx.recv().await.unwrap().is_err());
    assert!(state.can_start_new_maintenance());
}

struct RestorePermissions(std::path::PathBuf);
impl Drop for RestorePermissions {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o755));
    }
}

/// End to end: compaction fails (output directory not writable), the table keeps working, and a
/// later compaction succeeds with all rows intact.
#[tokio::test]
async fn test_compaction_failure_then_retry_succeeds() {
    let mut env = TestEnvironment::default().await;
    for i in 1..=2 {
        let lsn = i as u64 * 10;
        env.append_row(i, "row", i, lsn - 1, None).await;
        env.commit(lsn).await;
        env.flush_table_and_sync(lsn, None).await;
    }
    let rx = env.table_event_manager.initiate_snapshot(20).await;
    TableEventManager::synchronize_force_snapshot_request(rx, 20)
        .await
        .unwrap();

    let table_dir = env.temp_dir.path().to_path_buf();
    let guard = RestorePermissions(table_dir.clone());
    std::fs::set_permissions(&table_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        env.force_data_compaction_and_sync(),
    )
    .await
    .expect("compaction waiter hung");
    let writable_as_us = std::fs::File::create(table_dir.join("probe")).is_ok();
    drop(guard);
    if writable_as_us {
        // Running as root: permissions don't stop writes, nothing to test here.
        return;
    }
    assert!(
        res.is_err(),
        "compaction into a read-only directory should fail"
    );

    // The table still ingests and a second compaction works.
    env.append_row(3, "row", 3, 29, None).await;
    env.commit(30).await;
    env.flush_table_and_sync(30, None).await;
    let rx = env.table_event_manager.initiate_snapshot(30).await;
    TableEventManager::synchronize_force_snapshot_request(rx, 30)
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(20),
        env.force_data_compaction_and_sync(),
    )
    .await
    .expect("second compaction hung")
    .unwrap();
    env.set_readable_lsn(30);
    env.verify_snapshot(30, &[1, 2, 3]).await;
}
