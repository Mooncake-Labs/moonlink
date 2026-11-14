//! Panics in table background tasks or in the event loop error the table instead of silently
//! stalling it; DROP keeps working.
use super::test_utils::*;
use crate::event_sync::TableStatus;
use crate::TableEventManager;

use std::time::Duration;

async fn wait_errored(env: &TestEnvironment) -> String {
    let mut rx = env.table_event_manager.subscribe_table_status();
    loop {
        if let TableStatus::Errored(m) = &*rx.borrow() {
            return m.clone();
        }
        tokio::time::timeout(Duration::from_secs(30), rx.changed())
            .await
            .expect("table never became errored")
            .unwrap();
    }
}

fn has_version_hint(dir: &std::path::Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|e| {
        let p = e.path();
        if p.is_dir() {
            has_version_hint(&p)
        } else {
            p.file_name()
                .map(|n| n == "version-hint.text")
                .unwrap_or(false)
        }
    })
}

/// A delete for a row the mirror does not have (source and mirror diverged) used to panic the
/// detached snapshot task, leaving the table silently stuck on its last snapshot.
#[tokio::test]
async fn test_snapshot_task_panic_errors_the_table() {
    let mut env = TestEnvironment::default().await;
    env.append_row(1, "a", 1, 5, None).await;
    env.commit(10).await;
    let rx = env.table_event_manager.initiate_snapshot(10).await;
    TableEventManager::synchronize_force_snapshot_request(rx, 10)
        .await
        .unwrap();

    env.delete_row(42, "missing", 42, 15, None).await;
    env.commit(20).await;
    let message = wait_errored(&env).await;
    assert!(message.contains("mooncake snapshot"), "{message}");

    // Waiters fail fast instead of hanging; DROP works and removes the iceberg table.
    let rx = env.table_event_manager.initiate_snapshot(20).await;
    let res = tokio::time::timeout(
        Duration::from_secs(10),
        TableEventManager::synchronize_force_snapshot_request(rx, 20),
    )
    .await
    .expect("create_snapshot hung");
    assert!(res.is_err());
    env.drop_table().await.unwrap();
    assert!(!has_version_hint(env.temp_dir.path()));
}

/// A panic in the event loop itself is caught; the table is errored and DROP still completes.
#[tokio::test]
async fn test_event_loop_panic_errors_the_table_and_drop_works() {
    let mut env = TestEnvironment::default().await;
    env.append_row(1, "a", 1, 5, None).await;
    env.commit(20).await;
    let rx = env.table_event_manager.initiate_snapshot(20).await;
    TableEventManager::synchronize_force_snapshot_request(rx, 20)
        .await
        .unwrap();
    env.send_event(crate::table_notify::TableEvent::PanicForTest)
        .await;
    let message = wait_errored(&env).await;
    assert!(message.contains("table event loop"), "{message}");
    assert!(env.table_event_manager.check_healthy().is_err());
    tokio::time::timeout(Duration::from_secs(60), env.drop_table())
        .await
        .expect("drop hung")
        .unwrap();
    assert!(!has_version_hint(env.temp_dir.path()));
}
