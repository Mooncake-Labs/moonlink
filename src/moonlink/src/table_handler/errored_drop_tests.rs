//! DROP of an errored table while background tasks (flush, mooncake snapshot, maintenance,
//! iceberg persistence) are still running.
use super::test_utils::*;
use crate::storage::filesystem::accessor::filesystem_accessor::FileSystemAccessor;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::mooncake_table_config::MooncakeTableConfig;
use crate::storage::MockTableManager;
use crate::storage::MooncakeTable;
use crate::{Error, TableEvent};

use iceberg::{Error as IcebergError, ErrorKind};
use tempfile::tempdir;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

static PANICS_BY_THREAD_NAME: Mutex<Option<HashMap<String, usize>>> = Mutex::new(None);
static PANIC_HOOK: Once = Once::new();

/// Run `test` on a runtime whose worker threads are named `thread_name`, and return how many
/// panics happened on those threads (background tasks spawned by the table run there too).
fn run_counting_panics<F: std::future::Future<Output = ()>>(thread_name: &str, test: F) -> usize {
    PANIC_HOOK.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if let Some(name) = std::thread::current().name() {
                let mut guard = PANICS_BY_THREAD_NAME.lock().unwrap();
                *guard
                    .get_or_insert_with(HashMap::new)
                    .entry(name.to_string())
                    .or_default() += 1;
            }
            prev(info);
        }));
    });
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name(thread_name)
        .enable_all()
        .build()
        .unwrap()
        .block_on(test);
    PANICS_BY_THREAD_NAME
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|m| m.get(thread_name).copied())
        .unwrap_or(0)
}

fn find_files(
    dir: &std::path::Path,
    pred: &dyn Fn(&std::path::Path) -> bool,
    out: &mut Vec<std::path::PathBuf>,
) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            find_files(&p, pred, out);
        } else if pred(&p) {
            out.push(p);
        }
    }
}

fn enter_errored() -> TableEvent {
    TableEvent::EnterErrored {
        reason: "unit test reason".to_string(),
    }
}

/// A flush still running when an errored table is dropped: DROP waits for it, and nothing is
/// written into the table directory afterwards.
#[test]
fn test_errored_drop_waits_for_inflight_flush() {
    let panics = run_counting_panics("errored-drop-flush", async {
        let mut env = TestEnvironment::default().await;
        for i in 0..200_000 {
            env.append_row(i, "some-longer-name-to-make-the-flush-slow", i, 5, None)
                .await;
        }
        env.flush_table(10).await;
        env.send_event(enter_errored()).await;
        tokio::time::timeout(Duration::from_secs(45), env.drop_table())
            .await
            .expect("drop hung")
            .expect("drop failed");
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut parquet = Vec::new();
        find_files(
            env.temp_dir.path(),
            &|p| p.extension().map(|e| e == "parquet").unwrap_or(false),
            &mut parquet,
        );
        assert!(parquet.is_empty(), "files written after drop: {parquet:?}");
    });
    assert_eq!(
        panics, 0,
        "a background task panicked after the errored drop"
    );
}

/// Enter errored + DROP raced against flush, mooncake snapshot and iceberg persistence at various
/// offsets: DROP finishes, nothing panics, and no iceberg table reappears afterwards.
#[test]
fn test_errored_drop_races_background_work() {
    let panics = run_counting_panics("errored-drop-race", async {
        for iter in 0..16u64 {
            let mut env = TestEnvironment::default().await;
            for i in 0..20_000 {
                env.append_row(i, "name", i, 5, None).await;
            }
            env.flush_table(10).await;
            env.send_event(TableEvent::ForceSnapshot { lsn: Some(10) })
                .await;
            tokio::time::sleep(Duration::from_millis(iter * 20)).await;
            env.send_event(enter_errored()).await;
            tokio::time::timeout(Duration::from_secs(45), env.drop_table())
                .await
                .unwrap_or_else(|_| panic!("iter {iter}: drop hung"))
                .unwrap_or_else(|e| panic!("iter {iter}: drop failed: {e}"));
            tokio::time::sleep(Duration::from_millis(500)).await;
            let mut hints = Vec::new();
            find_files(
                env.temp_dir.path(),
                &|p| {
                    p.file_name()
                        .map(|n| n == "version-hint.text")
                        .unwrap_or(false)
                },
                &mut hints,
            );
            assert!(
                hints.is_empty(),
                "iter {iter}: iceberg table left: {hints:?}"
            );
        }
    });
    assert_eq!(
        panics, 0,
        "a background task panicked after the errored drop"
    );
}

/// A compaction waiter registered before the table errored gets the errored error.
#[tokio::test]
async fn test_inflight_maintenance_waiter_fails_on_errored() {
    let mut env = TestEnvironment::default().await;
    env.append_row(1, "a", 1, 5, None).await;
    env.commit(10).await;
    let mut rx = env.table_event_manager.initiate_data_compaction().await;
    env.send_event(enter_errored()).await;
    let res = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("compaction waiter hung");
    assert!(matches!(res, Ok(Err(Error::TableErrored(_)))), "{res:?}");
    env.drop_table().await.unwrap();
}

/// Mock table manager whose `sync_snapshot` signals `entered` and then waits for `release` (forever
/// if it is never notified), failing afterwards.
async fn table_with_gated_persistence(
    temp_dir: &tempfile::TempDir,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    released: Arc<AtomicBool>,
) -> MooncakeTable {
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
    mock.expect_load_snapshot_from_table().returning(move || {
        let metadata = metadata_copy.clone();
        Box::pin(async move { Ok((0, MooncakeSnapshot::new(metadata))) })
    });
    mock.expect_sync_snapshot().returning(move |_, _| {
        let entered = entered.clone();
        let release = release.clone();
        let released = released.clone();
        Box::pin(async move {
            entered.notify_one();
            release.notified().await;
            released.store(true, Ordering::SeqCst);
            Err(Error::from(IcebergError::new(
                ErrorKind::Unexpected,
                "injected persistence failure",
            )))
        })
    });
    mock.expect_drop_table()
        .returning(|| Box::pin(async { Ok(()) }));
    MooncakeTable::new_with_table_manager(
        metadata,
        Box::new(mock),
        create_test_object_storage_cache(temp_dir),
        FileSystemAccessor::default_for_test(temp_dir),
    )
    .await
    .unwrap()
}

/// DROP of an errored table waits for an in-flight iceberg persistence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_errored_drop_waits_for_inflight_persistence() {
    let temp_dir = tempdir().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let released = Arc::new(AtomicBool::new(false));
    let table = table_with_gated_persistence(
        &temp_dir,
        entered.clone(),
        release.clone(),
        released.clone(),
    )
    .await;
    let mut env = TestEnvironment::new_with_mooncake_table(temp_dir, table).await;
    env.append_row(1, "a", 10, 5, None).await;
    env.commit(10).await;
    let _snapshot_rx = env.table_event_manager.initiate_snapshot(10).await;
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("iceberg persistence never started");

    env.send_event(enter_errored()).await;
    let drop_completion = env.table_event_manager.drop_table();
    tokio::pin!(drop_completion);
    assert!(
        tokio::time::timeout(Duration::from_millis(500), &mut drop_completion)
            .await
            .is_err(),
        "errored DROP did not wait for the in-flight persistence"
    );

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(10), drop_completion)
        .await
        .expect("drop hung after persistence finished")
        .unwrap();
    assert!(released.load(Ordering::SeqCst));
}

/// A persistence that never reports back does not block DROP of an errored table forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_errored_drop_times_out_on_stuck_persistence() {
    let temp_dir = tempdir().unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let table = table_with_gated_persistence(
        &temp_dir,
        entered.clone(),
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(AtomicBool::new(false)),
    )
    .await;
    let mut env = TestEnvironment::new_with_mooncake_table(temp_dir, table).await;
    env.send_event(TableEvent::SetErroredDropWaitForTest {
        wait: Duration::from_millis(300),
    })
    .await;
    env.append_row(1, "a", 10, 5, None).await;
    env.commit(10).await;
    let _snapshot_rx = env.table_event_manager.initiate_snapshot(10).await;
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("iceberg persistence never started");

    env.send_event(enter_errored()).await;
    tokio::time::timeout(Duration::from_secs(10), env.drop_table())
        .await
        .expect("drop hung on a stuck persistence")
        .unwrap();
}
