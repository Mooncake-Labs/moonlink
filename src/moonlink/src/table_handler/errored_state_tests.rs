//! Per-table errored state: entered on request, rejects requests, drops CDC, always allows DROP.
use super::test_utils::*;
use crate::event_sync::TableStatus;
use crate::row::IdentityProp;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table_config::MooncakeTableConfig;
use crate::storage::MooncakeTable;
use crate::table_handler::table_handler_state::{SpecialTableState, TableHandlerState};
use crate::TableEventManager;

use tempfile::tempdir;
use tokio::sync::watch;

const REASON: &str = "unit test reason";

async fn wait_errored(rx: &mut watch::Receiver<TableStatus>) -> String {
    loop {
        if let TableStatus::Errored(message) = &*rx.borrow() {
            return message.clone();
        }
        tokio::time::timeout(std::time::Duration::from_secs(10), rx.changed())
            .await
            .expect("table never became errored")
            .unwrap();
    }
}

fn find_files_named(dir: &std::path::Path, name: &str, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            find_files_named(&p, name, out);
        } else if p.file_name().map(|n| n == name).unwrap_or(false) {
            out.push(p);
        }
    }
}

#[tokio::test]
async fn test_errored_table_rejects_requests_and_allows_drop() {
    let mut env = TestEnvironment::default().await;
    env.append_row(1, "a", 10, /*lsn=*/ 5, None).await;
    env.commit(10).await;
    let rx = env.table_event_manager.initiate_snapshot(10).await;
    TableEventManager::synchronize_force_snapshot_request(rx, 10)
        .await
        .unwrap();
    assert_eq!(env.table_event_manager.table_status(), TableStatus::Healthy);
    assert!(env.table_event_manager.check_healthy().is_ok());

    let mut status_rx = env.table_event_manager.subscribe_table_status();
    env.table_event_manager
        .enter_errored(REASON.to_string())
        .await
        .unwrap();
    let message = wait_errored(&mut status_rx).await;
    assert_eq!(
        message,
        "mirror table_name is out of sync: unit test reason; drop and re-create it with mooncake.create_table"
    );
    let err = env.table_event_manager.check_healthy().unwrap_err();
    assert!(
        err.to_string().contains("is out of sync: unit test reason"),
        "{err}"
    );

    // CDC is dropped without panicking.
    env.append_row(2, "b", 20, /*lsn=*/ 15, None).await;
    env.delete_row(1, "a", 10, /*lsn=*/ 16, None).await;
    env.commit(20).await;

    // Snapshot and maintenance waiters get the error instead of hanging.
    let rx = env.table_event_manager.initiate_snapshot(20).await;
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        TableEventManager::synchronize_force_snapshot_request(rx, 20),
    )
    .await
    .expect("create_snapshot hung on an errored table");
    assert!(res.unwrap_err().to_string().contains("out of sync"));
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        env.force_index_merge_and_sync(),
    )
    .await
    .expect("optimize hung on an errored table");
    assert!(res.unwrap_err().to_string().contains("out of sync"));

    // Idempotent.
    env.table_event_manager
        .enter_errored("second reason".to_string())
        .await
        .unwrap();
    assert_eq!(
        env.table_event_manager.table_status(),
        TableStatus::Errored(message)
    );

    // DROP works and removes the iceberg table, so a re-created mirror starts empty.
    env.drop_table().await.unwrap();
    let mut hints = Vec::new();
    find_files_named(env.temp_dir.path(), "version-hint.text", &mut hints);
    assert!(hints.is_empty(), "iceberg table left behind: {hints:?}");
}

#[tokio::test]
async fn test_recreate_after_errored_drop_starts_empty() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();
    let mut config = MooncakeTableConfig::new(path.to_str().unwrap().to_string());
    config.row_identity = IdentityProp::Keys(vec![0]);
    let iceberg_table_config =
        get_iceberg_manager_config("table_name".to_string(), path.to_str().unwrap().to_string());
    let open = || async {
        MooncakeTable::new(
            (*create_test_arrow_schema()).clone(),
            "table_name".to_string(),
            1,
            path.clone(),
            iceberg_table_config.clone(),
            config.clone(),
            create_test_object_storage_cache(&temp_dir),
            create_test_filesystem_accessor(&iceberg_table_config),
        )
        .await
        .unwrap()
    };
    let table = open().await;
    let mut env = TestEnvironment::new_with_mooncake_table(tempdir().unwrap(), table).await;
    env.append_row(1, "a", 10, 5, None).await;
    env.commit(10).await;
    let rx = env.table_event_manager.initiate_snapshot(10).await;
    TableEventManager::synchronize_force_snapshot_request(rx, 10)
        .await
        .unwrap();
    env.table_event_manager
        .enter_errored(REASON.to_string())
        .await
        .unwrap();
    env.drop_table().await.unwrap();
    drop(env);

    std::fs::create_dir_all(&path).unwrap();
    let recreated = open().await;
    assert_eq!(recreated.get_persistence_snapshot_lsn(), None);
}

#[tokio::test]
async fn test_errored_table_does_not_affect_other_tables() {
    let errored = TestEnvironment::default().await;
    let healthy = TestEnvironment::default().await;
    errored
        .table_event_manager
        .enter_errored(REASON.to_string())
        .await
        .unwrap();
    let mut rx = errored.table_event_manager.subscribe_table_status();
    wait_errored(&mut rx).await;

    healthy.append_row(1, "a", 10, 5, None).await;
    healthy.commit(10).await;
    healthy.set_readable_lsn(10);
    healthy.verify_snapshot(10, &[1]).await;
    assert_eq!(
        healthy.table_event_manager.table_status(),
        TableStatus::Healthy
    );
}

#[test]
fn test_enter_errored_reports_pending_drop() {
    let (sender, _receiver) = crate::event_sync::create_table_event_syncer();
    let mut state = TableHandlerState::new(
        sender.table_maintenance_completion_tx.clone(),
        sender.force_snapshot_completion_tx.clone(),
        None,
        None,
    )
    .with_table_status_tx(sender.table_status_tx.clone());
    state.mark_drop_table();
    assert_eq!(state.enter_errored("t", REASON.to_string()), Some(true));
    assert!(matches!(
        state.special_table_state,
        SpecialTableState::Errored { .. }
    ));
    // Already errored.
    assert_eq!(state.enter_errored("t", "other".to_string()), None);
}

/// A streamed transaction that commits a deletion of a row the mirror does not have errors the
/// mirror (it no longer matches its source) instead of stopping its snapshots.
#[tokio::test]
async fn test_deletion_without_a_row_errors_the_mirror() {
    let mut env = TestEnvironment::default().await;
    env.append_row(1, "a", 10, /*lsn=*/ 5, None).await;
    env.commit(10).await;
    env.flush_table_and_sync(10, None).await;
    let mut status_rx = env.table_event_manager.subscribe_table_status();

    let xact_id = 42;
    env.delete_row(1, "a", 10, /*lsn=*/ 0, Some(xact_id)).await;
    env.delete_row(1, "a", 10, /*lsn=*/ 0, Some(xact_id)).await;
    env.stream_commit(20, xact_id).await;
    let rx = env.table_event_manager.initiate_snapshot(20).await;
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        TableEventManager::synchronize_force_snapshot_request(rx, 20),
    )
    .await
    .expect("create_snapshot hung");
    assert!(res.unwrap_err().to_string().contains("out of sync"));
    let message = wait_errored(&mut status_rx).await;
    assert!(
        message.contains("1 deleted row(s) were not found in the mirror"),
        "{message}"
    );
    env.drop_table().await.unwrap();
}
