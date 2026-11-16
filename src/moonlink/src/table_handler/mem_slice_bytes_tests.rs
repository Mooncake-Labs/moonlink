//! Buffered rows are flushed once they reach `mem_slice_bytes`, well before the row-count limit
//! when rows are wide, both for regular and for streamed transactions.
use super::test_utils::*;
use crate::row::IdentityProp;
use crate::MooncakeTableConfig;

use std::path::Path;
use std::time::Duration;
use tempfile::tempdir;

const WIDE_NAME_LEN: usize = 4096;
const ROWS: i32 = 40;

fn count_parquet_files(dir: &Path) -> usize {
    let mut count = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                count += count_parquet_files(&path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                count += 1;
            }
        }
    }
    count
}

async fn wait_for_parquet(dir: &Path, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if count_parquet_files(dir) > 0 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

fn config_with_mem_slice_bytes(
    temp_files_dir: &Path,
    mem_slice_bytes: usize,
) -> MooncakeTableConfig {
    std::fs::create_dir_all(temp_files_dir).unwrap();
    let mut config = MooncakeTableConfig::new(temp_files_dir.to_str().unwrap().to_string());
    config.row_identity = IdentityProp::FullRow;
    config.mem_slice_bytes = mem_slice_bytes;
    config
}

/// The periodic force-snapshot timer ticks once right after the table handler starts; a
/// commit processed before that tick would be flushed by it. Let it pass on the empty table so
/// that only the mem slice thresholds can trigger a flush in these tests.
async fn new_env_after_startup_tick(
    temp_dir: tempfile::TempDir,
    config: MooncakeTableConfig,
) -> TestEnvironment {
    let env = TestEnvironment::new(temp_dir, config).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    env
}

/// 40 rows of ~4 KiB stay below the row limit but exceed a 64 KiB budget: the commit flushes.
#[tokio::test]
async fn test_wide_rows_flush_by_bytes_at_commit() {
    let temp_dir = tempdir().unwrap();
    let root = temp_dir.path().to_path_buf();
    let config = config_with_mem_slice_bytes(&root.join("temp"), 64 * 1024);
    assert!((ROWS as usize) < config.mem_slice_size);
    let mut env = new_env_after_startup_tick(temp_dir, config).await;
    let name = "x".repeat(WIDE_NAME_LEN);
    for id in 0..ROWS {
        env.append_row(id, &name, id, /*lsn=*/ 0, None).await;
    }
    env.commit(1).await;
    assert!(wait_for_parquet(&root, Duration::from_secs(10)).await);
    env.set_readable_lsn(1);
    env.verify_snapshot(1, &(0..ROWS).collect::<Vec<_>>()).await;
    env.shutdown().await;
}

/// With the default budget the same rows stay in memory until another trigger.
#[tokio::test]
async fn test_same_rows_stay_buffered_under_default_budget() {
    let temp_dir = tempdir().unwrap();
    let root = temp_dir.path().to_path_buf();
    let config = config_with_mem_slice_bytes(
        &root.join("temp"),
        MooncakeTableConfig::default_mem_slice_bytes(),
    );
    let mut env = new_env_after_startup_tick(temp_dir, config).await;
    let name = "x".repeat(WIDE_NAME_LEN);
    for id in 0..ROWS {
        env.append_row(id, &name, id, /*lsn=*/ 0, None).await;
    }
    env.commit(1).await;
    assert!(!wait_for_parquet(&root, Duration::from_secs(2)).await);
    env.shutdown().await;
}

/// A streamed transaction is flushed while it is still open once its rows exceed the budget.
#[tokio::test]
async fn test_wide_rows_flush_by_bytes_in_streamed_transaction() {
    let temp_dir = tempdir().unwrap();
    let root = temp_dir.path().to_path_buf();
    let config = config_with_mem_slice_bytes(&root.join("temp"), 64 * 1024);
    let mut env = new_env_after_startup_tick(temp_dir, config).await;
    let name = "x".repeat(WIDE_NAME_LEN);
    const XACT_ID: u32 = 7;
    for id in 0..ROWS {
        env.append_row(id, &name, id, /*lsn=*/ 1, Some(XACT_ID))
            .await;
    }
    assert!(wait_for_parquet(&root, Duration::from_secs(10)).await);
    env.stream_commit(/*lsn=*/ 2, XACT_ID).await;
    env.set_readable_lsn(2);
    env.verify_snapshot(2, &(0..ROWS).collect::<Vec<_>>()).await;
    env.shutdown().await;
}
