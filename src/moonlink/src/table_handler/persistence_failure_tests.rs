//! Iceberg persistence and flush failures: retried when safe, otherwise the table is errored.
//! Never a panic, never duplicated or lost rows.
use super::test_utils::*;
use crate::event_sync::TableStatus;
use crate::row::IdentityProp;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::PersistenceSnapshotPayload;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::mooncake_table_config::MooncakeTableConfig;
use crate::storage::MockTableManager;
use crate::storage::MooncakeTable;
use crate::storage::PersistenceResult;
use crate::table_handler::table_handler_state::MAX_CONSECUTIVE_PERSISTENCE_FAILURES;
use crate::{Error, TableEventManager};

use iceberg::{Error as IcebergError, ErrorKind};
use mockall::Sequence;
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};

fn transient_error() -> Error {
    Error::from(IcebergError::new(
        ErrorKind::Unexpected,
        "transient write error",
    ))
}

fn ok_result(payload: PersistenceSnapshotPayload) -> crate::Result<PersistenceResult> {
    Ok(PersistenceResult {
        remote_data_files: payload.import_payload.data_files.clone(),
        remote_file_indices: payload.import_payload.file_indices.clone(),
        puffin_blob_ref: HashMap::new(),
        evicted_files_to_delete: Vec::new(),
    })
}

/// Build a table backed by a mock table manager configured by `configure`.
async fn env_with_mock(configure: impl FnOnce(&mut MockTableManager)) -> TestEnvironment {
    let temp_dir = tempdir().unwrap();
    let config = MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
    let metadata = Arc::new(MooncakeTableMetadata {
        mooncake_table_id: "table_name".to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config,
        path: temp_dir.path().to_path_buf(),
    });
    let metadata_copy = metadata.clone();
    let mut mock = MockTableManager::new();
    mock.expect_get_warehouse_location()
        .returning(|| "".to_string());
    mock.expect_load_snapshot_from_table()
        .times(1)
        .returning(move || {
            let m = metadata_copy.clone();
            Box::pin(async move { Ok((0, MooncakeSnapshot::new(m))) })
        });
    mock.expect_drop_table()
        .returning(|| Box::pin(async move { Ok(()) }));
    configure(&mut mock);
    let table = MooncakeTable::new_with_table_manager(
        metadata,
        Box::new(mock),
        create_test_object_storage_cache(&temp_dir),
        FileSystemAccessor::default_for_test(&temp_dir),
    )
    .await
    .unwrap();
    TestEnvironment::new_with_mooncake_table(temp_dir, table).await
}

async fn snapshot(env: &mut TestEnvironment, lsn: u64) -> crate::Result<()> {
    let rx = env.table_event_manager.initiate_snapshot(lsn).await;
    tokio::time::timeout(
        Duration::from_secs(30),
        TableEventManager::synchronize_force_snapshot_request(rx, lsn),
    )
    .await
    .expect("create_snapshot hung")
}

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

/// One transient failure (think ENOSPC): the next attempt succeeds and the table keeps working.
#[tokio::test]
async fn test_table_survives_one_failed_iceberg_persistence() {
    let mut env = env_with_mock(|mock| {
        let mut seq = Sequence::new();
        mock.expect_sync_snapshot()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| Box::pin(async move { Err(transient_error()) }));
        mock.expect_sync_snapshot()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|payload, _| Box::pin(async move { ok_result(payload) }));
    })
    .await;

    env.append_row(1, "Alice", 10, 5, None).await;
    env.commit(10).await;
    assert!(snapshot(&mut env, 10).await.is_err());

    // A later request is not failed by the stale error, and the retry succeeds.
    env.append_row(2, "Bob", 20, 15, None).await;
    env.commit(20).await;
    snapshot(&mut env, 20).await.unwrap();
    assert_eq!(env.table_event_manager.table_status(), TableStatus::Healthy);
}

/// Persistent failures end in the errored state, not in an endless retry or a panic.
#[tokio::test]
async fn test_repeated_persistence_failures_error_the_table() {
    let mut env = env_with_mock(|mock| {
        mock.expect_sync_snapshot()
            .times(MAX_CONSECUTIVE_PERSISTENCE_FAILURES as usize)
            .returning(|_, _| Box::pin(async move { Err(transient_error()) }));
    })
    .await;
    env.append_row(1, "Alice", 10, 5, None).await;
    env.commit(10).await;
    let _ = snapshot(&mut env, 10).await;
    let message = wait_errored(&env).await;
    assert!(message.contains("iceberg persistence failed"), "{message}");
    env.drop_table().await.unwrap();
}

/// A failure whose effect on the committed table is unknown errors the table at once.
#[tokio::test]
async fn test_unknown_commit_outcome_errors_the_table() {
    let mut env = env_with_mock(|mock| {
        mock.expect_sync_snapshot().times(1).returning(|_, _| {
            Box::pin(async move {
                Err(Error::iceberg_commit_outcome_unknown(
                    "table changed".to_string(),
                ))
            })
        });
    })
    .await;
    env.append_row(1, "Alice", 10, 5, None).await;
    env.commit(10).await;
    assert!(snapshot(&mut env, 10).await.is_err());
    let message = wait_errored(&env).await;
    assert!(message.contains("outcome is unknown"), "{message}");
    env.drop_table().await.unwrap();
}

struct RestorePermissions(Vec<std::path::PathBuf>);
impl Drop for RestorePermissions {
    fn drop(&mut self) {
        for p in &self.0 {
            let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755));
        }
    }
}

fn find_dir_named(dir: &std::path::Path, name: &str) -> Option<std::path::PathBuf> {
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let p = entry.path();
        if p.is_dir() {
            if p.file_name().map(|n| n == name).unwrap_or(false) {
                return Some(p);
            }
            if let Some(found) = find_dir_named(&p, name) {
                return Some(found);
            }
        }
    }
    None
}

fn running_as_root(dir: &std::path::Path) -> bool {
    std::fs::File::create(dir.join(".perm_probe")).is_ok()
}

/// Real iceberg manager: a commit fails after the data files were already copied (metadata
/// directory not writable). After the directory is writable again the retry succeeds, and the
/// table reloaded from disk holds every row exactly once.
#[tokio::test]
async fn test_failed_commit_retry_does_not_duplicate_rows() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();
    let mut config = MooncakeTableConfig::new(path.to_str().unwrap().to_string());
    config.row_identity = IdentityProp::Keys(vec![0]);
    let iceberg_table_config =
        get_iceberg_manager_config("table_name".to_string(), path.to_str().unwrap().to_string());
    let open = |cache_dir: &TempDir| {
        let iceberg_table_config = iceberg_table_config.clone();
        let config = config.clone();
        let path = path.clone();
        let cache = create_test_object_storage_cache(cache_dir);
        async move {
            MooncakeTable::new(
                (*create_test_arrow_schema()).clone(),
                "table_name".to_string(),
                1,
                path,
                iceberg_table_config.clone(),
                config,
                cache,
                create_test_filesystem_accessor(&iceberg_table_config),
            )
            .await
            .unwrap()
        }
    };

    let cache_dir = tempdir().unwrap();
    let mut env =
        TestEnvironment::new_with_mooncake_table(tempdir().unwrap(), open(&cache_dir).await).await;
    env.append_row(1, "a", 1, 5, None).await;
    env.commit(10).await;
    snapshot(&mut env, 10).await.unwrap();

    let metadata_dir = find_dir_named(&path, "metadata").expect("iceberg metadata dir");
    let guard = RestorePermissions(vec![metadata_dir.clone()]);
    std::fs::set_permissions(&metadata_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    if running_as_root(&metadata_dir) {
        return;
    }
    env.append_row(2, "b", 2, 15, None).await;
    env.commit(20).await;
    assert!(snapshot(&mut env, 20).await.is_err());
    drop(guard);
    snapshot(&mut env, 20).await.unwrap();
    assert_eq!(env.table_event_manager.table_status(), TableStatus::Healthy);
    // Note: TestEnvironment::shutdown drops the table; just release the environment.
    drop(env);

    // Reload from disk: rows 1 and 2, once each.
    let cache_dir = tempdir().unwrap();
    let reloaded = open(&cache_dir).await;
    assert_eq!(reloaded.get_persistence_snapshot_lsn(), Some(20));
    let env = TestEnvironment::new_with_mooncake_table(tempdir().unwrap(), reloaded).await;
    env.set_readable_lsn(20);
    env.verify_snapshot(20, &[1, 2]).await;
}

/// A flush that keeps failing (table directory not writable) errors the table instead of
/// panicking the event loop; DROP still works afterwards.
#[tokio::test]
async fn test_persistent_flush_failure_errors_the_table() {
    let mut env = TestEnvironment::default().await;
    let table_dir = env.temp_dir.path().to_path_buf();
    let guard = RestorePermissions(vec![table_dir.clone()]);
    std::fs::set_permissions(&table_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    if running_as_root(&table_dir) {
        return;
    }
    env.append_row(1, "a", 1, 5, None).await;
    env.commit(10).await;
    env.flush_table(10).await;
    let message = wait_errored(&env).await;
    assert!(
        message.contains("writing table data to disk failed"),
        "{message}"
    );
    drop(guard);
    env.drop_table().await.unwrap();
}
