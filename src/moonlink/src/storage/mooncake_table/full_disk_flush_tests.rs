//! Flushes that hit a full disk (injected after the write created a data file).
use super::test_utils::*;
use super::*;
use crate::row::IdentityProp;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use tokio::sync::mpsc;

fn files_under(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = vec![];
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(files_under(&p));
            } else {
                out.push(p);
            }
        }
    }
    out
}

/// With the process-wide flush-wait accounting new tables get.
async fn table_on_process_wide_accounting(
    context: &TestContext,
    rows: i32,
) -> (MooncakeTable, mpsc::Receiver<TableEvent>) {
    let mut table = test_table(context, "full_disk", IdentityProp::FullRow).await;
    let (tx, rx) = mpsc::channel(100);
    table.register_table_notify(tx).await;
    for i in 0..rows {
        table.append(test_row(i, "row", i)).unwrap();
    }
    table.commit(10);
    (table, rx)
}

/// With its own flush-wait accounting, so the test cannot affect others running in parallel.
async fn table_with_committed_rows(
    context: &TestContext,
    rows: i32,
) -> (MooncakeTable, mpsc::Receiver<TableEvent>) {
    let (mut table, rx) = table_on_process_wide_accounting(context, rows).await;
    table.full_disk_flush_wait = Arc::default();
    (table, rx)
}

/// A full disk is waited out, and only the files of the attempt that succeeded remain.
#[tokio::test]
async fn test_flush_retried_after_full_disk_keeps_only_its_output() {
    let context = TestContext::new("full_disk");
    let (mut table, mut rx) = table_with_committed_rows(&context, 10).await;
    let before = files_under(context.path().as_path());
    table.flush_failure_injector.fail_next_writes(3);

    let disk_slice = flush_table_and_sync_no_apply(&mut table, &mut rx, 10)
        .await
        .expect("flush did not wait out the full disk");
    let mut new_files: Vec<_> = files_under(context.path().as_path())
        .into_iter()
        .filter(|p| !before.contains(p))
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .collect();
    let mut outputs: Vec<_> = disk_slice
        .output_files()
        .iter()
        .map(|(file, _)| std::path::PathBuf::from(file.file_path()))
        .collect();
    new_files.sort();
    outputs.sort();
    assert_eq!(new_files, outputs, "failed attempts left data files");
}

/// A disk that stays full fails the flush after the wait, without leaving partial data files.
#[tokio::test]
async fn test_flush_failing_on_full_disk_leaves_no_partial_files() {
    let context = TestContext::new("full_disk");
    let (mut table, mut rx) = table_with_committed_rows(&context, 10).await;
    let before = files_under(context.path().as_path());
    table.flush_failure_injector.fail_next_writes(usize::MAX);

    let started = std::time::Instant::now();
    let event_id = uuid::Uuid::new_v4();
    table.flush(10, event_id).unwrap();
    let TableEvent::FlushResult { flush_result, .. } = rx.recv().await.unwrap() else {
        panic!("expected a flush result");
    };
    let err = flush_result.unwrap().expect_err("the flush should fail");
    assert!(err.is_storage_full(), "{err:?}");
    assert!(
        started.elapsed() >= FLUSH_FULL_DISK_RETRY_LIMIT,
        "a full disk was not waited out"
    );
    let new_files: Vec<_> = files_under(context.path().as_path())
        .into_iter()
        .filter(|p| !before.contains(p))
        .collect();
    assert!(
        new_files.is_empty(),
        "failed flush left files: {new_files:?}"
    );
}

async fn next_flush_error(rx: &mut mpsc::Receiver<TableEvent>) -> crate::Error {
    let TableEvent::FlushResult { flush_result, .. } = rx.recv().await.unwrap() else {
        panic!("expected a flush result");
    };
    flush_result.unwrap().expect_err("the flush should fail")
}

/// Waiting out a full disk keeps the flushed changes in memory while replication goes on. Past
/// the memory limit the flush gives up (the table handler then errors the table) instead of
/// waiting for the whole retry period.
#[tokio::test]
async fn test_flush_over_the_memory_limit_gives_up_at_once() {
    let context = TestContext::new("full_disk");
    let (mut table, mut rx) = table_with_committed_rows(&context, 10).await;
    table.full_disk_flush_wait.set_limit_for_test(1);
    table.flush_failure_injector.fail_next_writes(usize::MAX);

    let started = std::time::Instant::now();
    table.flush(10, uuid::Uuid::new_v4()).unwrap();
    let err = next_flush_error(&mut rx).await;
    assert!(
        started.elapsed() < FLUSH_FULL_DISK_RETRY_LIMIT,
        "waited although over the limit"
    );
    assert!(format!("{err:?}").contains("waiting in memory"), "{err:?}");
    assert_eq!(table.full_disk_flush_wait.waiting_bytes(), 0);
}

/// A flush under the limit waits; one more waiting flush that crosses it makes both give up.
#[tokio::test]
async fn test_waiting_flushes_give_up_together_past_the_memory_limit() {
    let context = TestContext::new("full_disk");
    let (mut table, mut rx) = table_with_committed_rows(&context, 10).await;
    table.flush_failure_injector.fail_next_writes(usize::MAX);

    let started = std::time::Instant::now();
    table.flush(10, uuid::Uuid::new_v4()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while table.full_disk_flush_wait.waiting_bytes() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the flush never waited for disk space");
    let first = table.full_disk_flush_wait.waiting_bytes();
    table
        .full_disk_flush_wait
        .set_limit_for_test(first + first / 2);

    for i in 10..20 {
        table.append(test_row(i, "row", i)).unwrap();
    }
    table.commit(20);
    table.flush(20, uuid::Uuid::new_v4()).unwrap();
    for _ in 0..2 {
        let err = next_flush_error(&mut rx).await;
        assert!(format!("{err:?}").contains("waiting in memory"), "{err:?}");
    }
    assert!(
        started.elapsed() < FLUSH_FULL_DISK_RETRY_LIMIT,
        "the flushes waited for the whole retry period"
    );
    assert_eq!(table.full_disk_flush_wait.waiting_bytes(), 0);
}

async fn wait_until_waiting(wait: &FullDiskFlushWait, more_than: u64) -> u64 {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let bytes = wait.waiting_bytes();
            if bytes > more_than {
                return bytes;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the flush never waited for disk space")
}

/// All mirrors of the process share one accounting. When their waiting flushes together exceed
/// the limit, only the mirror holding the most gives up: a small mirror whose flush crosses the
/// limit keeps waiting, and flushes once there is space.
///
/// The only test that makes flushes wait on the process-wide instance.
#[tokio::test]
async fn test_only_the_mirror_holding_the_most_waiting_memory_gives_up() {
    let big_context = TestContext::new("full_disk_big");
    let (big, mut big_rx) = table_on_process_wide_accounting(&big_context, 2000).await;
    let small_context = TestContext::new("full_disk_small");
    let (small, mut small_rx) = table_on_process_wide_accounting(&small_context, 10).await;
    let shared = process_wide_full_disk_flush_wait();
    assert!(Arc::ptr_eq(&big.full_disk_flush_wait, &shared));
    assert!(Arc::ptr_eq(&small.full_disk_flush_wait, &shared));
    assert_eq!(shared.waiting_bytes(), 0);
    let (mut big, mut small) = (big, small);
    big.flush_failure_injector.fail_next_writes(usize::MAX);
    small.flush_failure_injector.fail_next_writes(usize::MAX);

    big.flush(10, uuid::Uuid::new_v4()).unwrap();
    let big_bytes = wait_until_waiting(&shared, 0).await;
    // The small mirror's flush is the one that crosses the limit.
    shared.set_limit_for_test(big_bytes + 1);
    small.flush(10, uuid::Uuid::new_v4()).unwrap();

    let err = next_flush_error(&mut big_rx).await;
    assert!(format!("{err:?}").contains("waiting in memory"), "{err:?}");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        small_rx.try_recv().is_err(),
        "the small mirror's flush gave up for the big one's memory"
    );
    let small_bytes = shared.waiting_bytes();
    assert!(small_bytes > 0 && small_bytes < big_bytes);

    small.flush_failure_injector.fail_next_writes(0);
    let TableEvent::FlushResult { flush_result, .. } = small_rx.recv().await.unwrap() else {
        panic!("expected a flush result");
    };
    assert!(
        flush_result.unwrap().is_ok(),
        "the small mirror's flush failed"
    );
    assert_eq!(shared.waiting_bytes(), 0);
    shared.set_limit_for_test(FULL_DISK_MAX_WAITING_FLUSH_BYTES);
}

/// A flush waiting for disk space gives up as soon as its table is errored, dropped or being
/// dropped, so it neither keeps its memory nor holds budget the other mirrors need.
#[tokio::test]
async fn test_waiting_flush_gives_up_when_its_table_no_longer_wants_it() {
    for drop_table in [false, true] {
        let context = TestContext::new("full_disk");
        let (mut table, mut rx) = table_with_committed_rows(&context, 10).await;
        table.flush_failure_injector.fail_next_writes(usize::MAX);
        let wait = table.full_disk_flush_wait.clone();
        table.flush(10, uuid::Uuid::new_v4()).unwrap();
        wait_until_waiting(&wait, 0).await;

        let started = std::time::Instant::now();
        if drop_table {
            drop(table);
        } else {
            table.cancel_flushes_waiting_for_disk_space();
        }
        let err = next_flush_error(&mut rx).await;
        assert!(
            format!("{err:?}").contains("errored or being dropped"),
            "{err:?}"
        );
        assert!(
            started.elapsed() < FLUSH_FULL_DISK_RETRY_LIMIT / 2,
            "the flush kept waiting"
        );
        assert_eq!(wait.waiting_bytes(), 0);
    }
}
