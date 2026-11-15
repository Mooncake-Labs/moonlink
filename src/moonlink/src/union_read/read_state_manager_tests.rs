//! ReadStateManager tests that drive the LSN watch channels by hand, to reproduce states that
//! production reaches through concurrency between the replication sink, the table handler and
//! readers.

use crate::union_read::always_healthy_table_status;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use crate::row::IdentityProp;

use crate::storage::cache::object_storage::base_cache::{
    CacheEntry, CacheTrait, InlineEvictedFiles,
};
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::mooncake_table::table_creation_test_utils::create_mooncake_table_and_notify_for_read;
use crate::storage::mooncake_table::table_operation_test_utils::{
    create_mooncake_snapshot_for_test, flush_table_and_sync, get_read_state_filepath_remap,
};
use crate::storage::mooncake_table::test_utils::{
    append_rows, test_row, test_table, verify_files_and_deletions, TestContext,
};
use crate::storage::storage_utils::TableUniqueFileId;
use crate::storage::MooncakeTable;
use crate::table_notify::TableEvent;
use crate::union_read::decode_read_state_for_testing;
use crate::{NonEvictableHandle, ObjectStorageCache, ReadState, ReadStateManager};

pub(super) struct Env {
    _context: Option<TestContext>,
    pub(super) table: MooncakeTable,
    notify_rx: mpsc::Receiver<TableEvent>,
    pub(super) replication_tx: watch::Sender<u64>,
    pub(super) commit_tx: watch::Sender<u64>,
    pub(super) rsm: Arc<ReadStateManager>,
}

impl Env {
    fn wrap(
        context: Option<TestContext>,
        table: MooncakeTable,
        notify_rx: mpsc::Receiver<TableEvent>,
    ) -> Self {
        let (replication_tx, replication_rx) = watch::channel(0u64);
        let (commit_tx, commit_rx) = watch::channel(0u64);
        let rsm = Arc::new(ReadStateManager::new(
            &table,
            replication_rx,
            commit_rx,
            always_healthy_table_status(),
            get_read_state_filepath_remap(),
        ));
        Self {
            _context: context,
            table,
            notify_rx,
            replication_tx,
            commit_tx,
            rsm,
        }
    }

    pub(super) async fn new(name: &str) -> Self {
        let context = TestContext::new(name);
        let mut table = test_table(&context, name, IdentityProp::FullRow).await;
        let (notify_tx, notify_rx) = mpsc::channel(100);
        table.register_table_notify(notify_tx).await;
        Self::wrap(Some(context), table, notify_rx)
    }

    /// Append rows and commit at `lsn`; optionally flush; then apply a mooncake snapshot WITHOUT
    /// notifying readers (the table handler publishes the snapshot LSN later).
    pub(super) async fn commit_and_snapshot(&mut self, rows: &[i32], lsn: u64, flush: bool) {
        let rows = rows.iter().map(|id| test_row(*id, "x", 1)).collect();
        append_rows(&mut self.table, rows).unwrap();
        self.table.commit(lsn);
        if flush {
            flush_table_and_sync(&mut self.table, &mut self.notify_rx, lsn)
                .await
                .unwrap();
        }
        create_mooncake_snapshot_for_test(&mut self.table, &mut self.notify_rx).await;
    }

    pub(super) fn publish(&self, snapshot_lsn: u64, commit_lsn: u64, replication_lsn: u64) {
        self.table.notify_snapshot_reader(snapshot_lsn);
        self.commit_tx.send(commit_lsn).unwrap();
        self.replication_tx.send(replication_lsn).unwrap();
    }
}

pub(super) async fn assert_rows(read_state: &ReadState, expected: &[i32]) {
    let (data_files, puffin_files, deletion_vectors, position_deletes) =
        decode_read_state_for_testing(read_state);
    for file in &data_files {
        assert!(
            std::path::Path::new(file).exists(),
            "read state references missing file {file}"
        );
    }
    verify_files_and_deletions(
        &data_files,
        &puffin_files,
        position_deletes,
        deletion_vectors,
        expected,
    )
    .await;
}

/// Object storage cache whose next `get_cache_entry` fails once when armed (transient IO error).
#[derive(Debug)]
struct FailOnceCache {
    inner: ObjectStorageCache,
    armed: AtomicBool,
}

#[async_trait::async_trait]
impl CacheTrait for FailOnceCache {
    async fn import_cache_entry(
        &self,
        file_id: TableUniqueFileId,
        cache_entry: CacheEntry,
    ) -> (NonEvictableHandle, InlineEvictedFiles) {
        self.inner.import_cache_entry(file_id, cache_entry).await
    }

    async fn try_delete_cache_entry(&self, file_id: TableUniqueFileId) -> InlineEvictedFiles {
        self.inner.try_delete_cache_entry(file_id).await
    }

    async fn get_cache_entry(
        &self,
        file_id: TableUniqueFileId,
        remote_filepath: &str,
        filesystem_accessor: &dyn BaseFileSystemAccess,
    ) -> crate::Result<(Option<NonEvictableHandle>, InlineEvictedFiles)> {
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected read-cache failure").into());
        }
        self.inner
            .get_cache_entry(file_id, remote_filepath, filesystem_accessor)
            .await
    }

    async fn increment_reference_count(&self, cache_handle: &NonEvictableHandle) {
        self.inner.increment_reference_count(cache_handle).await
    }
}

/// A failed file resolution must not leave the previous snapshot's read state cached under the
/// new LSN.
#[tokio::test]
async fn failed_read_resolution_does_not_cache_stale_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(FailOnceCache {
        inner: ObjectStorageCache::default_for_test(&temp_dir),
        armed: AtomicBool::new(false),
    });
    let (table, notify_rx) =
        create_mooncake_table_and_notify_for_read(&temp_dir, cache.clone()).await;
    let mut env = Env::wrap(None, table, notify_rx);

    env.commit_and_snapshot(&[1], 50, /*flush=*/ true).await;
    env.publish(50, 50, 50);
    assert_rows(&env.rsm.try_read(Some(50)).await.unwrap(), &[1]).await;

    env.commit_and_snapshot(&[2], 100, /*flush=*/ true).await;
    env.publish(100, 100, 100);
    cache.armed.store(true, Ordering::SeqCst);
    assert!(env.rsm.try_read(Some(100)).await.is_err());

    // The transient failure is gone; the read at 100 must include row 2.
    assert_rows(&env.rsm.try_read(Some(100)).await.unwrap(), &[1, 2]).await;
    drop(temp_dir);
}

/// The sink marks a table's commit LSN, then awaits the (bounded) table-event send, and only then
/// marks the replication LSN, so a reader can observe commit > replication. That must make the
/// read wait, never panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_lsn_ahead_of_replication_lsn_waits_without_panicking() {
    let mut env = Env::new("commit_ahead").await;
    env.commit_and_snapshot(&[1], 100, /*flush=*/ false).await;
    env.publish(100, 100, 100);
    assert_rows(&env.rsm.try_read(Some(100)).await.unwrap(), &[1]).await;

    env.commit_tx.send(200).unwrap();
    // Already-snapshotted LSNs stay readable.
    assert_rows(&env.rsm.try_read(Some(100)).await.unwrap(), &[1]).await;
    let rsm = env.rsm.clone();
    let waiting = tokio::spawn(async move { rsm.try_read(Some(150)).await.map(|_| ()) });
    if let Ok(res) = tokio::time::timeout(Duration::from_secs(1), waiting).await {
        panic!("read at 150 must wait for replication, got {res:?}");
    }
}

/// Even if a reader once labelled the cache past its snapshot, publishing a newer snapshot must
/// refresh it: the cache is keyed by the snapshot it was built from, not by an LSN watermark.
#[tokio::test]
async fn new_snapshot_always_refreshes_cached_read_state() {
    let mut env = Env::new("refresh").await;
    env.commit_and_snapshot(&[1], 50, /*flush=*/ false).await;
    env.publish(50, 50, 50);
    assert_rows(&env.rsm.try_read(Some(50)).await.unwrap(), &[1]).await;
    env.commit_and_snapshot(&[3], 100, /*flush=*/ false).await;
    env.publish(100, 100, 100);

    // A view where replication ran ahead of the commit watch labels the cache up to 200.
    env.replication_tx.send(200).unwrap();
    let _ = env.rsm.try_read(Some(200)).await.unwrap();

    env.commit_and_snapshot(&[2], 200, /*flush=*/ false).await;
    env.publish(200, 200, 200);
    assert_rows(&env.rsm.try_read(Some(200)).await.unwrap(), &[1, 2, 3]).await;
}

/// A read between "snapshot applied" and "snapshot LSN published" materializes the in-memory
/// rows for the new snapshot version. Each read state must own its temporary file: dropping the
/// first one must not delete a file that a cached read state still serves.
#[tokio::test]
async fn in_memory_temp_file_is_not_shared_between_read_states() {
    let mut env = Env::new("shared_tmp").await;
    env.commit_and_snapshot(&[1], 100, /*flush=*/ false).await;
    env.publish(100, 100, 100);

    env.commit_and_snapshot(&[2], 200, /*flush=*/ false).await;
    env.commit_tx.send(200).unwrap();
    env.replication_tx.send(200).unwrap();
    let first = env.rsm.try_read(Some(100)).await.unwrap();

    env.table.notify_snapshot_reader(200);
    let second = env.rsm.try_read(Some(200)).await.unwrap();
    drop(second);
    drop(first);
    // Read state cleanup runs on detached tasks.
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_rows(&env.rsm.try_read(Some(200)).await.unwrap(), &[1, 2]).await;
}

/// Writing the temporary parquet for unflushed rows must not hold the table snapshot lock: the
/// snapshot task, and with it ingestion, would stall behind reader disk IO.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_lock_not_held_during_temp_parquet_write() {
    use crate::union_read::read_state_manager::test_hooks::block_next_materialize;

    let mut env = Env::new("lock_scope").await;
    env.commit_and_snapshot(&[1, 2], 100, /*flush=*/ false)
        .await;
    env.publish(100, 100, 100);

    let (reached_rx, release_tx) = block_next_materialize(&env.rsm);
    let rsm = env.rsm.clone();
    let reader = tokio::spawn(async move { rsm.try_read(Some(100)).await.map(|_| ()) });
    reached_rx.await.unwrap();

    let snapshot = env.table.get_state_for_reader().0;
    assert!(
        snapshot.try_write().is_ok(),
        "snapshot lock is held while the in-memory parquet is being written"
    );
    release_tx.send(()).unwrap();
    reader.await.unwrap().unwrap();
}

/// Reads of an errored table fail at once, and a read waiting for an LSN ends as soon as the table
/// becomes errored instead of waiting out the scan timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn errored_table_fails_reads_immediately_and_wakes_waiters() {
    use crate::TableStatus;

    let mut env = Env::new("errored").await;
    env.commit_and_snapshot(&[1], 100, /*flush=*/ false).await;
    env.publish(100, 100, 100);
    let (status_tx, status_rx) = watch::channel(TableStatus::Healthy);
    let rsm = Arc::new(ReadStateManager::new(
        &env.table,
        env.replication_tx.subscribe(),
        env.commit_tx.subscribe(),
        status_rx,
        get_read_state_filepath_remap(),
    ));
    assert_rows(&rsm.try_read(Some(100)).await.unwrap(), &[1]).await;

    let waiting = tokio::spawn({
        let rsm = rsm.clone();
        async move { rsm.try_read(Some(500)).await.map(|_| ()) }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    status_tx
        .send(TableStatus::Errored("mirror t is out of sync".to_string()))
        .unwrap();
    let res = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("waiting read did not observe the errored state")
        .unwrap();
    let err = res.expect_err("waiting read must fail");
    assert!(err.to_string().contains("out of sync"), "{err}");

    // New reads fail immediately, even for already-snapshotted LSNs.
    let err = rsm.try_read(Some(100)).await.expect_err("errored table");
    assert!(err.to_string().contains("out of sync"), "{err}");
}

/// When CDC fails a table, the sink drops the table's replication watch before the table handler
/// marks the table errored. A snapshot request or scan waiting on that watch reports the errored
/// reason, not the closed channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_report_the_errored_reason_when_the_replication_watch_closes_first() {
    use crate::TableStatus;

    let env = Env::new("watch_closed").await;
    env.commit_tx.send(100).unwrap();
    for scan in [false, true] {
        let (replication_tx, replication_rx) = watch::channel(100u64);
        let (status_tx, status_rx) = watch::channel(TableStatus::Healthy);
        let rsm = Arc::new(ReadStateManager::new(
            &env.table,
            replication_rx,
            env.commit_tx.subscribe(),
            status_rx,
            get_read_state_filepath_remap(),
        ));
        let waiting = tokio::spawn(async move {
            if scan {
                rsm.try_read(Some(500)).await.map(|_| ())
            } else {
                rsm.wait_for_lsn_coverage(500).await.map(|_| ())
            }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(replication_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Fails only if the read already ended (and dropped the receiver); the assertion says how.
        let _ = status_tx.send(TableStatus::Errored(
            "mirror t is out of sync: source schema changed".to_string(),
        ));
        let err = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("read hung after its replication watch closed")
            .unwrap()
            .expect_err("read of a failed table must fail");
        assert!(
            matches!(err, crate::Error::TableErrored(_))
                && err.to_string().contains("source schema changed"),
            "scan={scan}: {err:?}"
        );
    }
}

/// A read whose replication watch closed while the table stays healthy fails after a bounded wait
/// with the closed-channel error; a table dropped meanwhile reports that it was dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_whose_replication_watch_closes_end_without_a_status_change() {
    use crate::TableStatus;

    let env = Env::new("watch_closed_healthy").await;
    env.commit_tx.send(100).unwrap();
    for drop_status in [false, true] {
        let (replication_tx, replication_rx) = watch::channel(100u64);
        let (status_tx, status_rx) = watch::channel(TableStatus::Healthy);
        let rsm = ReadStateManager::new(
            &env.table,
            replication_rx,
            env.commit_tx.subscribe(),
            status_rx,
            get_read_state_filepath_remap(),
        );
        drop(replication_tx);
        if drop_status {
            drop(status_tx);
        } else {
            std::mem::forget(status_tx);
        }
        let err = tokio::time::timeout(Duration::from_secs(5), rsm.wait_for_lsn_coverage(500))
            .await
            .expect("a read whose watch closed must not hang")
            .expect_err("replication can no longer reach the LSN");
        if drop_status {
            assert!(err.to_string().contains("dropped"), "{err:?}");
        } else {
            assert!(
                matches!(err, crate::Error::WatchChannelRecvError(_)),
                "{err:?}"
            );
        }
    }
}

/// Snapshot requests resolve the session LSN against replication progress: once replication is at
/// the LSN, the table's latest commit is the LSN a snapshot has to cover.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lsn_coverage_resolves_to_last_table_commit_once_replicated() {
    let env = Env::new("coverage").await;
    env.commit_tx.send(100).unwrap();
    env.replication_tx.send(100).unwrap();

    // A commit of this table at or past the LSN: the LSN itself.
    assert_eq!(env.rsm.wait_for_lsn_coverage(80).await.unwrap(), 80);
    assert_eq!(env.rsm.wait_for_lsn_coverage(100).await.unwrap(), 100);

    // WAL past the last table commit (no commit at 150): wait for replication, then the commit.
    let rsm = env.rsm.clone();
    let waiting = tokio::spawn(async move { rsm.wait_for_lsn_coverage(150).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !waiting.is_finished(),
        "must wait until replication reaches the LSN"
    );
    env.replication_tx.send(160).unwrap();
    let covered = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(covered, 100);
}

mod gc {
    use super::*;
    use crate::storage::mooncake_table::table_creation_test_utils::{
        create_test_arrow_schema, create_test_filesystem_accessor,
    };
    use crate::storage::mooncake_table::table_operation_test_utils::{
        create_mooncake_and_persist_for_test, perform_data_compaction_for_test,
        sync_iceberg_snapshot, sync_mooncake_snapshot,
    };
    use crate::storage::mooncake_table::test_utils::{
        test_iceberg_table_config, test_mooncake_table_config,
    };
    use crate::storage::snapshot_options::{
        IcebergSnapshotOption, MaintenanceOption, SnapshotOption,
    };
    use crate::ObjectStorageCacheConfig;

    fn data_parquet_count(env: &Env) -> usize {
        let dir = env
            ._context
            .as_ref()
            .unwrap()
            .path()
            .join("default")
            .join("gc")
            .join("data");
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// One flushed and persisted data file per row; readers are told about each snapshot.
    async fn write_files(env: &mut Env, ids: std::ops::RangeInclusive<i32>) {
        for id in ids {
            let lsn = id as u64 * 10;
            append_rows(&mut env.table, vec![test_row(id, "r", id)]).unwrap();
            env.table.commit(lsn);
            flush_table_and_sync(&mut env.table, &mut env.notify_rx, lsn)
                .await
                .unwrap();
            create_mooncake_and_persist_for_test(&mut env.table, &mut env.notify_rx).await;
            env.publish(lsn, lsn, lsn);
        }
    }

    /// Compact all data files into one and persist it; garbage collection runs on the commit.
    async fn compact_everything(env: &mut Env) {
        assert!(env.table.try_create_mooncake_snapshot(SnapshotOption {
            uuid: uuid::Uuid::new_v4(),
            force_create: true,
            dump_snapshot: false,
            iceberg_snapshot_option: IcebergSnapshotOption::Skip,
            index_merge_option: MaintenanceOption::Skip,
            data_compaction_option: MaintenanceOption::ForceFull(uuid::Uuid::new_v4()),
        }));
        let (_, _, _, compaction, _) =
            sync_mooncake_snapshot(&mut env.table, &mut env.notify_rx).await;
        let payload = compaction.take_payload().expect("compaction payload");
        perform_data_compaction_for_test(&mut env.table, &mut env.notify_rx, payload).await;
        env.table.force_empty_persistence_payload();
        create_mooncake_and_persist_for_test(&mut env.table, &mut env.notify_rx).await;
    }

    /// Files of an open read state survive compaction until the read state is dropped.
    #[tokio::test]
    async fn gc_keeps_files_of_open_read_states() {
        let mut env = Env::new("gc").await;
        write_files(&mut env, 1..=4).await;
        let open_scan = env.rsm.try_read(Some(40)).await.unwrap();
        assert_eq!(data_parquet_count(&env), 4);

        compact_everything(&mut env).await;
        assert_eq!(
            data_parquet_count(&env),
            5,
            "files deleted under an open read state"
        );
        assert_rows(&open_scan, &[1, 2, 3, 4]).await;

        // The scan ends; the next commit collects the superseded files.
        drop(open_scan);
        write_files(&mut env, 5..=5).await;
        assert_eq!(data_parquet_count(&env), 2);
        assert_rows(&env.rsm.try_read(Some(50)).await.unwrap(), &[1, 2, 3, 4, 5]).await;
    }

    /// On an idle table the cached read state is the only holder of the old files; it must not
    /// pin them forever.
    #[tokio::test]
    async fn idle_cached_read_state_does_not_pin_superseded_files() {
        let mut env = Env::new("gc").await;
        write_files(&mut env, 1..=4).await;
        drop(env.rsm.try_read(Some(40)).await.unwrap());

        compact_everything(&mut env).await;
        // The predecessor iceberg version stays loadable until the next commit.
        assert_eq!(data_parquet_count(&env), 5);
        write_files(&mut env, 5..=5).await;
        assert_eq!(
            data_parquet_count(&env),
            2,
            "the idle cached read state pinned superseded files"
        );
        // The next scan rebuilds from the compacted snapshot.
        assert_rows(&env.rsm.try_read(Some(50)).await.unwrap(), &[1, 2, 3, 4, 5]).await;
    }

    /// Table with the production cache setting (cache entries of persisted files point at the
    /// iceberg files in place) and a deletion-vector puffin per committed delete.
    async fn production_cache_env(name: &str) -> (Env, Arc<ObjectStorageCache>) {
        let context = TestContext::new(name);
        let iceberg_table_config = test_iceberg_table_config(&context, name);
        let mut table_config = test_mooncake_table_config(&context);
        table_config.batch_size = 2;
        table_config.row_identity = IdentityProp::FullRow;
        table_config.persistence_config.new_committed_deletion_log = 1;
        let cache_dir = context.temp_dir.path().join("read_cache");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let cache = Arc::new(ObjectStorageCache::new(ObjectStorageCacheConfig::new(
            1 << 30,
            cache_dir.to_str().unwrap().to_string(),
            /*optimize_local_filesystem=*/ true,
        )));
        let mut table = MooncakeTable::new(
            (*create_test_arrow_schema()).clone(),
            name.to_string(),
            1,
            context.path(),
            iceberg_table_config.clone(),
            table_config,
            cache.clone(),
            create_test_filesystem_accessor(&iceberg_table_config),
        )
        .await
        .unwrap();
        // Reader liveness alone must keep the files; no grace period to hide behind.
        table.set_gc_grace_for_test(Duration::ZERO);
        let (notify_tx, notify_rx) = mpsc::channel(100);
        table.register_table_notify(notify_tx).await;
        (Env::wrap(Some(context), table, notify_rx), cache)
    }

    async fn commit_flush_and_persist(env: &mut Env, lsn: u64) {
        env.table.commit(lsn);
        flush_table_and_sync(&mut env.table, &mut env.notify_rx, lsn)
            .await
            .unwrap();
        create_mooncake_and_persist_for_test(&mut env.table, &mut env.notify_rx).await;
        create_mooncake_and_persist_for_test(&mut env.table, &mut env.notify_rx).await;
    }

    fn puffins(read_state: &ReadState) -> Vec<String> {
        decode_read_state_for_testing(read_state).1
    }

    /// Applying an iceberg commit is a mooncake snapshot that keeps the snapshot LSN. A reader
    /// that starts after it must not get the cached read state from before it (with the replaced
    /// deletion-vector puffin), and the replaced puffin must survive later commits for as long as
    /// a read state that lists it is alive, however long ago the replacing commit was.
    #[tokio::test]
    async fn replaced_puffin_is_neither_served_to_new_readers_nor_deleted_under_old_ones() {
        let (mut env, _cache) = production_cache_env("gc_puffin").await;
        for id in 1..=3 {
            append_rows(&mut env.table, vec![test_row(id, "r", id)]).unwrap();
        }
        commit_flush_and_persist(&mut env, 10).await;
        env.table.delete(test_row(1, "r", 1), 15).await;
        commit_flush_and_persist(&mut env, 20).await;

        // An in-memory delete on top of persisted puffin A: the read state lists A.
        env.table.delete(test_row(2, "r", 2), 25).await;
        env.table.commit(30);
        flush_table_and_sync(&mut env.table, &mut env.notify_rx, 30)
            .await
            .unwrap();
        let (_, payload, _, _, _) =
            create_mooncake_snapshot_for_test(&mut env.table, &mut env.notify_rx).await;
        env.publish(30, 30, 30);
        let old_reader = env.rsm.try_read(None).await.unwrap();
        let old_puffins = puffins(&old_reader);
        assert_eq!(old_puffins.len(), 1, "{old_puffins:?}");
        assert_rows(&old_reader, &[3]).await;

        // The iceberg commit writes puffin B replacing A; the next mooncake snapshot applies it
        // without moving the snapshot LSN.
        env.table.persist_iceberg_snapshot(payload.unwrap());
        let result = sync_iceberg_snapshot(&mut env.notify_rx).await;
        env.table.set_persistence_snapshot_res(result);
        create_mooncake_snapshot_for_test(&mut env.table, &mut env.notify_rx).await;
        env.publish(30, 30, 30);

        let new_reader = env.rsm.try_read(None).await.unwrap();
        assert!(
            !Arc::ptr_eq(&old_reader, &new_reader),
            "a reader after the apply got the cached read state from before it"
        );
        let new_puffins = puffins(&new_reader);
        assert!(
            new_puffins.iter().all(|p| !old_puffins.contains(p)),
            "new reader still lists the replaced puffin: {new_puffins:?}"
        );
        assert_rows(&new_reader, &[3]).await;

        // The next commit runs garbage collection: A is kept for the old reader.
        append_rows(&mut env.table, vec![test_row(4, "r", 4)]).unwrap();
        commit_flush_and_persist(&mut env, 40).await;
        let exists = |paths: &[String]| paths.iter().all(|p| std::path::Path::new(p).exists());
        assert!(
            exists(&old_puffins),
            "puffin deleted under a live read state: {old_puffins:?}"
        );
        assert!(exists(&new_puffins), "{new_puffins:?}");
        assert_rows(&old_reader, &[3]).await;

        // Once both readers are gone (only the idle cached read state is left), the following
        // commit collects A.
        drop(old_reader);
        drop(new_reader);
        append_rows(&mut env.table, vec![test_row(5, "r", 5)]).unwrap();
        commit_flush_and_persist(&mut env, 50).await;
        assert!(
            old_puffins
                .iter()
                .all(|p| !std::path::Path::new(p).exists()),
            "replaced puffin pinned after its last reader ended: {old_puffins:?}"
        );
        env.publish(50, 50, 50);
        assert_rows(&env.rsm.try_read(Some(50)).await.unwrap(), &[3, 4, 5]).await;
    }

    /// Sum of the reference counts of all pinned (non-evictable) cache entries.
    async fn pinned_refs(cache: &ObjectStorageCache) -> u32 {
        let mut total = 0;
        for id in cache.get_non_evictable_filenames().await {
            total += cache.get_non_evictable_entry_ref_count(&id).await;
        }
        total
    }

    fn inmemory_temp_files(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| {
                        let path = e.path();
                        if path.is_dir() {
                            inmemory_temp_files(&path)
                        } else {
                            usize::from(e.file_name().to_string_lossy().starts_with("inmemory_"))
                        }
                    })
                    .sum()
            })
            .unwrap_or(0)
    }

    /// A scan cancelled after its read was prepared (RPC client disconnect, LSN-wait timeout)
    /// must not leak the cache pins and the temp parquet its read state build took.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_scan_does_not_leak_cache_pins_or_temp_files() {
        use crate::union_read::read_state_manager::test_hooks::block_next_materialize;

        let (mut env, cache) = production_cache_env("gc_cancel").await;
        for id in 1..=3 {
            append_rows(&mut env.table, vec![test_row(id, "r", id)]).unwrap();
        }
        commit_flush_and_persist(&mut env, 10).await;
        env.table.delete(test_row(1, "r", 1), 15).await;
        commit_flush_and_persist(&mut env, 20).await;
        // An unflushed row, so the read also writes a temp parquet.
        append_rows(&mut env.table, vec![test_row(4, "r", 4)]).unwrap();
        env.table.commit(30);
        create_mooncake_snapshot_for_test(&mut env.table, &mut env.notify_rx).await;
        env.publish(30, 30, 30);
        let baseline = pinned_refs(&cache).await;

        let (reached_rx, release_tx) = block_next_materialize(&env.rsm);
        let rsm = env.rsm.clone();
        let scan = tokio::spawn(async move { rsm.try_read(None).await.map(|_| ()) });
        reached_rx.await.unwrap();
        assert!(
            pinned_refs(&cache).await > baseline,
            "precondition: the prepared read holds a cache pin"
        );
        scan.abort();
        assert!(scan.await.unwrap_err().is_cancelled());
        // The slow materialization the cancelled scan started completes on its own (with the
        // build tied to the scan, the hook went away with it).
        let _ = release_tx.send(());
        assert_rows(&env.rsm.try_read(None).await.unwrap(), &[2, 3, 4]).await;

        // With no reader and no read state manager left, every pin and temp file is released.
        let temp_dir = env._context.as_ref().unwrap().temp_dir.path().to_path_buf();
        drop(env.rsm);
        let released = async {
            while pinned_refs(&cache).await != baseline || inmemory_temp_files(&temp_dir) != 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        if tokio::time::timeout(Duration::from_secs(5), released)
            .await
            .is_err()
        {
            panic!(
                "a cancelled scan leaked: pinned refs {} (baseline {baseline}), temp files {}",
                pinned_refs(&cache).await,
                inmemory_temp_files(&temp_dir)
            );
        }
    }

    /// A read state build that panics after its read was prepared releases the cache pins and
    /// temp parquet it took, the panic reaches the caller, and later reads work.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn panicking_read_state_build_does_not_leak_cache_pins_or_temp_files() {
        use crate::union_read::read_state_manager::test_hooks::panic_next_materialize;

        let (mut env, cache) = production_cache_env("gc_panic").await;
        for id in 1..=3 {
            append_rows(&mut env.table, vec![test_row(id, "r", id)]).unwrap();
        }
        commit_flush_and_persist(&mut env, 10).await;
        env.table.delete(test_row(1, "r", 1), 15).await;
        commit_flush_and_persist(&mut env, 20).await;
        // An unflushed row, so the read also has a temp parquet to write.
        append_rows(&mut env.table, vec![test_row(4, "r", 4)]).unwrap();
        env.table.commit(30);
        create_mooncake_snapshot_for_test(&mut env.table, &mut env.notify_rx).await;
        env.publish(30, 30, 30);
        let baseline = pinned_refs(&cache).await;

        panic_next_materialize(&env.rsm);
        let rsm = env.rsm.clone();
        let scan = tokio::spawn(async move { rsm.try_read(None).await.map(|_| ()) });
        assert!(
            scan.await.unwrap_err().is_panic(),
            "the build panic must reach the caller"
        );

        let temp_dir = env._context.as_ref().unwrap().temp_dir.path().to_path_buf();
        let released = async {
            while pinned_refs(&cache).await != baseline || inmemory_temp_files(&temp_dir) != 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        if tokio::time::timeout(Duration::from_secs(5), released)
            .await
            .is_err()
        {
            panic!(
                "a panicking build leaked: pinned refs {} (baseline {baseline}), temp files {}",
                pinned_refs(&cache).await,
                inmemory_temp_files(&temp_dir)
            );
        }
        assert_rows(&env.rsm.try_read(None).await.unwrap(), &[2, 3, 4]).await;
    }

    /// A read state build that is stuck holds the cache lock, so later reads of the table queue
    /// behind it; they stay bounded by the caller's timeout (scan_table's LSN wait), the timeout
    /// is diagnosed as a slow read rather than missing replication, and reads work once the build
    /// finishes, without leaked pins.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reads_behind_a_stuck_build_are_bounded_by_the_caller_timeout() {
        use crate::union_read::read_state_manager::test_hooks::block_next_materialize;

        let (mut env, cache) = production_cache_env("gc_stuck").await;
        for id in 1..=2 {
            append_rows(&mut env.table, vec![test_row(id, "r", id)]).unwrap();
        }
        commit_flush_and_persist(&mut env, 10).await;
        env.table.delete(test_row(1, "r", 1), 15).await;
        commit_flush_and_persist(&mut env, 20).await;
        env.publish(20, 20, 20);
        let baseline = pinned_refs(&cache).await;

        let (reached_rx, release_tx) = block_next_materialize(&env.rsm);
        let rsm = env.rsm.clone();
        let stuck = tokio::spawn(async move { rsm.try_read(Some(20)).await.map(|_| ()) });
        reached_rx.await.unwrap();

        // A newer snapshot, so the next read cannot reuse the stuck build's result.
        append_rows(&mut env.table, vec![test_row(3, "r", 3)]).unwrap();
        env.table.commit(30);
        create_mooncake_snapshot_for_test(&mut env.table, &mut env.notify_rx).await;
        env.publish(30, 30, 30);
        let queued =
            tokio::time::timeout(Duration::from_millis(300), env.rsm.try_read(Some(30))).await;
        assert!(queued.is_err(), "a read behind a stuck build must wait");
        assert!(
            env.rsm.lsn_reached(Some(30)),
            "the wait is the build, not replication"
        );

        release_tx.send(()).unwrap();
        stuck.await.unwrap().unwrap();
        assert_rows(&env.rsm.try_read(Some(30)).await.unwrap(), &[2, 3]).await;

        drop(env.rsm);
        let released = async {
            while pinned_refs(&cache).await != baseline {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(5), released)
            .await
            .expect("reads that timed out behind a stuck build leaked cache pins");
    }

    /// A materialize hook armed for one table is not taken by reads of another table, which run
    /// in parallel in the same process.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn materialize_hook_applies_to_its_table_only() {
        use crate::union_read::read_state_manager::test_hooks::block_next_materialize;

        let (mut armed, _) = production_cache_env("hook_armed").await;
        let (mut other, _) = production_cache_env("hook_other").await;
        for env in [&mut armed, &mut other] {
            append_rows(&mut env.table, vec![test_row(1, "r", 1)]).unwrap();
            env.table.commit(10);
            create_mooncake_snapshot_for_test(&mut env.table, &mut env.notify_rx).await;
            env.publish(10, 10, 10);
        }
        let (reached_rx, release_tx) = block_next_materialize(&armed.rsm);
        let read = tokio::time::timeout(Duration::from_secs(5), other.rsm.try_read(Some(10)))
            .await
            .expect("another table's read took the hook");
        assert_rows(&read.unwrap(), &[1]).await;

        let rsm = armed.rsm.clone();
        let blocked = tokio::spawn(async move { rsm.try_read(Some(10)).await.map(|_| ()) });
        tokio::time::timeout(Duration::from_secs(5), reached_rx)
            .await
            .expect("the armed table's read did not take its hook")
            .unwrap();
        release_tx.send(()).unwrap();
        blocked.await.unwrap().unwrap();
    }
}
