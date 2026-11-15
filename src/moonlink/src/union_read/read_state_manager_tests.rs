//! ReadStateManager tests that drive the LSN watch channels by hand, to reproduce states that
//! production reaches through concurrency between the replication sink, the table handler and
//! readers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use crate::storage::cache::object_storage::base_cache::{
    CacheEntry, CacheTrait, InlineEvictedFiles,
};
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::mooncake_table::table_creation_test_utils::create_mooncake_table_and_notify_for_read;
use crate::storage::mooncake_table::table_operation_test_utils::{
    create_mooncake_snapshot_for_test, flush_table_and_sync, get_read_state_filepath_remap,
};
use crate::storage::mooncake_table::test_utils::{
    append_rows, test_row, verify_files_and_deletions, TestContext,
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
