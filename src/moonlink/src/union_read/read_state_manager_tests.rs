//! ReadStateManager tests that drive the LSN watch channels by hand, to reproduce states that
//! production reaches through concurrency between the replication sink, the table handler and
//! readers.

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
    use crate::union_read::read_state_manager::test_hooks::BLOCK_MATERIALIZE;
    use tokio::sync::oneshot;

    let mut env = Env::new("lock_scope").await;
    env.commit_and_snapshot(&[1, 2], 100, /*flush=*/ false)
        .await;
    env.publish(100, 100, 100);

    let (reached_tx, reached_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    *BLOCK_MATERIALIZE.lock().unwrap() = Some((reached_tx, release_rx));
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
    let rsm = Arc::new(
        ReadStateManager::new(
            &env.table,
            env.replication_tx.subscribe(),
            env.commit_tx.subscribe(),
            get_read_state_filepath_remap(),
        )
        .with_table_status(status_rx),
    );
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

mod gc {
    use super::*;
    use crate::storage::mooncake_table::table_operation_test_utils::{
        create_mooncake_and_persist_for_test, perform_data_compaction_for_test,
        sync_mooncake_snapshot,
    };
    use crate::storage::snapshot_options::{
        IcebergSnapshotOption, MaintenanceOption, SnapshotOption,
    };

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
}
