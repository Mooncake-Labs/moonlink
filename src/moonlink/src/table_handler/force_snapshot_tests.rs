//! A forced snapshot is answered only once its LSN is in the iceberg table.
use std::time::Duration;

use tempfile::tempdir;

use super::test_utils::*;
use crate::row::IdentityProp;
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::index::index_merge_config::FileIndexMergeConfig;
use crate::storage::mooncake_table_config::DiskSliceWriterConfig;
use crate::storage::mooncake_table_config::IcebergPersistenceConfig;
use crate::storage::mooncake_table_config::MooncakeTableConfig;
use crate::storage::TableManager;
use crate::TableEventManager;

/// No flush or persistence happens on its own, only when a forced snapshot needs it.
fn config_without_background_persistence(temp_dir: &tempfile::TempDir) -> MooncakeTableConfig {
    MooncakeTableConfig {
        mem_slice_bytes: usize::MAX,
        append_only: false,
        row_identity: IdentityProp::Keys(vec![0]),
        batch_size: MooncakeTableConfig::DEFAULT_BATCH_SIZE,
        mem_slice_size: usize::MAX,
        snapshot_deletion_record_count: usize::MAX,
        temp_files_directory: temp_dir.path().to_str().unwrap().to_string(),
        disk_slice_writer_config: DiskSliceWriterConfig::default(),
        data_compaction_config: DataCompactionConfig::default(),
        file_index_config: FileIndexMergeConfig::default(),
        persistence_config: IcebergPersistenceConfig {
            new_data_file_count: usize::MAX,
            new_committed_deletion_log: usize::MAX,
            new_compacted_data_file_count: usize::MAX,
            old_compacted_data_file_count: usize::MAX,
            old_merged_file_indices_count: usize::MAX,
        },
    }
}

/// The persisted table LSN the handler reports once it has processed every event sent so far. A
/// zero-LSN forced snapshot is answered at once. The first answer may come from the periodic
/// forced snapshot's first tick, which can run before the queued events; the second request is
/// sent after that one, so its answer comes after them.
async fn persisted_lsn_after_queued_events(env: &mut TestEnvironment) -> u64 {
    let mut reported_lsn = 0;
    for _ in 0..2 {
        let mut rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 0).await;
        tokio::time::timeout(Duration::from_secs(10), rx.changed())
            .await
            .expect("a zero-LSN forced snapshot is answered at once")
            .unwrap();
        reported_lsn = match &*rx.borrow() {
            Some(Ok(lsn)) => *lsn,
            other => panic!("unexpected forced snapshot answer {other:?}"),
        };
    }
    reported_lsn
}

/// A table that has committed but never persisted, and has received a write since its last
/// commit, has no consistent view. That used to count as "no activity at all", so the forced
/// snapshot was answered at once with the replication LSN, although iceberg held nothing.
#[tokio::test]
async fn test_force_snapshot_waits_for_first_persistence_after_uncommitted_write() {
    let temp_dir = tempdir().unwrap();
    let config = config_without_background_persistence(&temp_dir);
    let mut env = TestEnvironment::new(temp_dir, config.clone()).await;

    env.append_row(1, "John", 30, /*lsn=*/ 3, /*xact_id=*/ None)
        .await;
    env.commit(/*lsn=*/ 4).await;
    // A transaction that has not committed yet.
    env.append_row(2, "Bob", 20, /*lsn=*/ 5, /*xact_id=*/ None)
        .await;
    env.set_replication_lsn(5);

    let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 4).await;
    // Nothing can flush before the open transaction commits, so iceberg is empty.
    let reported_lsn = persisted_lsn_after_queued_events(&mut env).await;
    assert!(
        reported_lsn < 4,
        "forced snapshot at 4 answered with {reported_lsn} before anything was persisted"
    );

    // Committing the open transaction flushes, which persists the requested LSN.
    env.commit(/*lsn=*/ 6).await;
    tokio::time::timeout(
        Duration::from_secs(10),
        TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 4),
    )
    .await
    .expect("forced snapshot at 4 never completed after the commit")
    .unwrap();
    let mut iceberg_table_manager = env.create_iceberg_table_manager(config).await;
    let (_, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert!(snapshot.flush_lsn.is_some_and(|lsn| lsn >= 4));
    assert_eq!(snapshot.disk_files.len(), 1);

    env.shutdown().await;
}

/// A table that never committed anything has nothing to persist: a forced snapshot is answered
/// with the replication LSN right away.
#[tokio::test]
async fn test_force_snapshot_of_table_without_commits_is_answered_at_once() {
    let temp_dir = tempdir().unwrap();
    let config = config_without_background_persistence(&temp_dir);
    let mut env = TestEnvironment::new(temp_dir, config).await;

    // Uncommitted rows only.
    env.append_row(1, "John", 30, /*lsn=*/ 3, /*xact_id=*/ None)
        .await;
    env.set_replication_lsn(7);

    let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 7).await;
    tokio::time::timeout(
        Duration::from_secs(10),
        TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 7),
    )
    .await
    .expect("a table without commits has nothing to wait for")
    .unwrap();

    env.shutdown().await;
}

/// How a streamed transaction ends without changing the table.
#[derive(Clone, Copy)]
enum StreamEnd {
    Aborted,
    AbortedWithoutRows,
    /// Its commit is a re-send of one the table already applied, as after a reconnect.
    ResentCommit,
}

/// A streamed transaction that ends without changing the table leaves it at its latest commit.
/// A forced snapshot of that commit must flush and persist it on an idle table; it used to wait
/// for the next commit, because the stream's end left the table without a consistent view.
#[rstest::rstest]
#[case::aborted(StreamEnd::Aborted)]
#[case::aborted_without_rows(StreamEnd::AbortedWithoutRows)]
#[case::resent_commit(StreamEnd::ResentCommit)]
#[tokio::test]
async fn test_force_snapshot_of_latest_commit_completes_after_stream_ends(
    #[case] stream_end: StreamEnd,
) {
    let temp_dir = tempdir().unwrap();
    let config = config_without_background_persistence(&temp_dir);
    let mut env = TestEnvironment::new(temp_dir, config.clone()).await;

    env.append_row(1, "John", 30, /*lsn=*/ 10, /*xact_id=*/ None)
        .await;
    env.commit(/*lsn=*/ 20).await;
    let xact_id = 7;
    if !matches!(stream_end, StreamEnd::AbortedWithoutRows) {
        env.append_row(2, "Bob", 20, /*lsn=*/ 21, Some(xact_id))
            .await;
    }
    match stream_end {
        StreamEnd::Aborted | StreamEnd::AbortedWithoutRows => {
            env.stream_abort(xact_id).await;
        }
        StreamEnd::ResentCommit => {
            env.stream_commit(/*lsn=*/ 15, xact_id).await;
        }
    }
    env.set_replication_lsn(21);

    let rx = env.table_event_manager.initiate_snapshot(/*lsn=*/ 20).await;
    tokio::time::timeout(
        Duration::from_secs(10),
        TableEventManager::synchronize_force_snapshot_request(rx, /*requested_lsn=*/ 20),
    )
    .await
    .expect("forced snapshot of the latest commit never completed on an idle table")
    .unwrap();
    let mut iceberg_table_manager = env.create_iceberg_table_manager(config).await;
    let (_, snapshot) = iceberg_table_manager
        .load_snapshot_from_table()
        .await
        .unwrap();
    assert_eq!(snapshot.flush_lsn, Some(20));
    assert_eq!(snapshot.disk_files.len(), 1);
    env.set_readable_lsn(20);
    env.verify_snapshot(20, &[1]).await;

    env.shutdown().await;
}
