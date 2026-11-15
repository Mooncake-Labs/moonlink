use crate::error::Result;
use crate::event_sync::TableStatus;
use crate::storage::table::iceberg::gc::LiveReadSeqProvider;
use crate::storage::MooncakeTable;
use crate::storage::SnapshotTableState;
use crate::union_read::live_reads::LiveReadSeqs;
use crate::ReadState;
use crate::ReadStateFilepathRemap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::{watch, RwLock};

/// Snapshot LSN, which indicates there's no snapshot LSN update.
const NO_SNAPSHOT_LSN: u64 = u64::MAX;
/// Commit LSN, which indicates there's no commit.
const NO_COMMIT_LSN: u64 = 0;

/// The latest materialized read, shared by all scans until the table snapshot changes.
struct CachedRead {
    /// Snapshot watch value observed when this read was materialized; `None` before the first read.
    snapshot_key: Option<u64>,
    /// Requests up to this LSN are answered by `state`: no commit of this table in
    /// `(snapshot, valid_upto]` existed when it was materialized.
    valid_upto: u64,
    /// Snapshot sequence the state was built from. Maintenance-only snapshots (compaction, index
    /// merge, iceberg import) bump it without moving the snapshot LSN.
    snapshot_seq: u64,
    state: Arc<ReadState>,
}

impl CachedRead {
    fn empty(read_state_filepath_remap: ReadStateFilepathRemap) -> Self {
        Self {
            snapshot_key: None,
            valid_upto: 0,
            snapshot_seq: 0,
            state: Arc::new(ReadState::new(
                /*data_files=*/ Vec::new(),
                /*puffin_cache_handles=*/ Vec::new(),
                /*deletion_vectors_at_read=*/ Vec::new(),
                /*position_deletes=*/ Vec::new(),
                /*associated_files=*/ Vec::new(),
                /*cache_handles=*/ Vec::new(),
                read_state_filepath_remap,
            )),
        }
    }

    /// Rows up to the snapshot are always covered. Beyond it, also require the CURRENT view to be
    /// clean: a commit at or below `valid_upto` that was marked late (e.g. buffered during initial
    /// copy) makes the view dirty and forces a wait for its snapshot.
    fn reusable(&self, view: &LsnView, snapshot_seq: u64, requested_lsn: Option<u64>) -> bool {
        if self.snapshot_key != Some(view.snapshot) || self.snapshot_seq != snapshot_seq {
            return false;
        }
        match requested_lsn {
            None => view.clean,
            Some(lsn) => {
                (view.snapshot_initialized() && lsn <= view.snapshot)
                    || (view.clean && lsn <= self.valid_upto)
            }
        }
    }
}

/// Orders snapshot watch values; [`NO_SNAPSHOT_LSN`] is older than any published snapshot.
fn snapshot_lsn_is_newer(lsn: u64, than: u64) -> bool {
    lsn != NO_SNAPSHOT_LSN && (than == NO_SNAPSHOT_LSN || lsn > than)
}

/// One consistent observation of the table's LSN watches.
///
/// Producers publish in a fixed order: moonlink_sink marks a table's commit LSN before the
/// replication LSN, and the table handler publishes a snapshot only after its commit was marked.
/// Reading replication -> commit -> snapshot therefore guarantees `commit` covers every commit
/// of this table at or below `replication`; later values may run ahead, which `normalize` clamps.
#[derive(Debug, Clone, Copy)]
struct LsnView {
    /// Raw snapshot watch value (may be [`NO_SNAPSHOT_LSN`]).
    snapshot: u64,
    replication: u64,
    clean: bool,
}

impl LsnView {
    fn observe(
        replication_rx: &mut watch::Receiver<u64>,
        commit_rx: &watch::Receiver<u64>,
        snapshot_rx: &mut watch::Receiver<u64>,
    ) -> Self {
        let replication = *replication_rx.borrow_and_update();
        let commit = *commit_rx.borrow();
        let snapshot = *snapshot_rx.borrow_and_update();
        Self::normalize(replication, commit, snapshot)
    }

    fn normalize(replication: u64, commit: u64, snapshot: u64) -> Self {
        let commit = if snapshot == NO_SNAPSHOT_LSN {
            commit
        } else {
            commit.max(snapshot)
        };
        let replication = replication.max(commit);
        Self {
            snapshot,
            replication,
            clean: ReadStateManager::snapshot_is_clean(snapshot, commit),
        }
    }

    fn snapshot_initialized(&self) -> bool {
        self.snapshot != NO_SNAPSHOT_LSN
    }

    fn can_satisfy(&self, requested_lsn: Option<u64>) -> bool {
        match requested_lsn {
            None => true,
            Some(lsn) => {
                (self.snapshot_initialized() && lsn <= self.snapshot)
                    || (lsn <= self.replication && self.clean)
            }
        }
    }

    fn valid_upto(&self) -> u64 {
        let snapshot = if self.snapshot_initialized() {
            self.snapshot
        } else {
            0
        };
        if self.clean {
            snapshot.max(self.replication)
        } else {
            snapshot
        }
    }
}

/// Status watch of a table without a table handler (test validation readers): healthy for as long
/// as the process runs.
#[cfg(test)]
pub(crate) fn always_healthy_table_status() -> watch::Receiver<TableStatus> {
    let (sender, receiver) = watch::channel(TableStatus::Healthy);
    // A dropped sender means a dropped table to readers; keep it alive.
    Box::leak(Box::new(sender));
    receiver
}

pub struct ReadStateManager {
    cache: Arc<RwLock<CachedRead>>,
    snapshot_seq: Arc<AtomicU64>,
    live_reads: Arc<LiveReadSeqs>,
    table_snapshot: Arc<RwLock<SnapshotTableState>>,
    table_snapshot_watch_receiver: watch::Receiver<u64>,
    replication_lsn_rx: watch::Receiver<u64>,
    last_commit_lsn_rx: watch::Receiver<u64>,
    /// Functor which maps local filepath to remote URI if possible, should be applied on all files within [`ReadState`].
    read_state_filepath_remap: ReadStateFilepathRemap,
    /// Table health; reads of an errored table fail instead of waiting or serving stale data.
    table_status_rx: watch::Receiver<TableStatus>,
}

impl ReadStateManager {
    pub fn new(
        table: &MooncakeTable,
        replication_lsn_rx: watch::Receiver<u64>,
        last_commit_lsn_rx: watch::Receiver<u64>,
        table_status_rx: watch::Receiver<TableStatus>,
        read_state_filepath_remap: ReadStateFilepathRemap,
    ) -> Self {
        let (table_snapshot, table_snapshot_watch_receiver) = table.get_state_for_reader();
        let cache = Arc::new(RwLock::new(CachedRead::empty(
            read_state_filepath_remap.clone(),
        )));
        let snapshot_seq = table.snapshot_seq_handle();
        let live_reads = Arc::new(LiveReadSeqs::default());
        table.set_live_read_seq_provider(Self::live_read_seq_provider(
            Arc::downgrade(&cache),
            snapshot_seq.clone(),
            live_reads.clone(),
            read_state_filepath_remap.clone(),
        ));
        ReadStateManager {
            cache,
            snapshot_seq,
            live_reads,
            table_snapshot,
            table_snapshot_watch_receiver,
            replication_lsn_rx,
            last_commit_lsn_rx,
            read_state_filepath_remap,
            table_status_rx,
        }
    }

    /// Oldest snapshot sequence any live read state was built from, for garbage collection.
    /// On an idle table the cached read state would pin superseded files forever, so a cached
    /// state that is outdated and not held by any scan is released first.
    fn live_read_seq_provider(
        cache: Weak<RwLock<CachedRead>>,
        snapshot_seq: Arc<AtomicU64>,
        live_reads: Arc<LiveReadSeqs>,
        read_state_filepath_remap: ReadStateFilepathRemap,
    ) -> LiveReadSeqProvider {
        Arc::new(move || {
            if let Some(cache) = cache.upgrade() {
                if let Ok(mut cached) = cache.try_write() {
                    if cached.snapshot_key.is_some()
                        && cached.snapshot_seq != snapshot_seq.load(Ordering::SeqCst)
                        && Arc::strong_count(&cached.state) == 1
                    {
                        *cached = CachedRead::empty(read_state_filepath_remap.clone());
                    }
                }
            }
            live_reads.min()
        })
    }

    /// Wait until replication reached `lsn`, then return the LSN a snapshot must cover so that it
    /// contains every commit of this table at or below `lsn`: `min(lsn, table commit LSN)`.
    ///
    /// Callers pass the end of the session's last transaction, which may be WAL without a commit
    /// for this table (an xid-less transaction that pruned pages, a logical message, a commit to
    /// another table); waiting for a table commit at that LSN would never end. Once replication is
    /// at `lsn`, the producer order (commit LSN marked before replication LSN, table events sent in
    /// between) guarantees every such commit was already handed to the table handler.
    pub async fn wait_for_lsn_coverage(&self, lsn: u64) -> Result<u64> {
        let mut replication_lsn_rx = self.replication_lsn_rx.clone();
        let mut table_status_rx = self.table_status_rx.clone();
        loop {
            Self::check_healthy(&mut table_status_rx)?;
            let replication = *replication_lsn_rx.borrow_and_update();
            let commit = *self.last_commit_lsn_rx.borrow();
            if commit >= lsn {
                return Ok(lsn);
            }
            if replication >= lsn {
                return Ok(commit);
            }
            tokio::select! {
                res = replication_lsn_rx.changed() => res?,
                res = Self::table_status_changed(&mut table_status_rx) => res?,
            }
        }
    }

    /// Whether the table's LSNs already allow a read at `lsn`, i.e. a read that has not finished
    /// is waiting for its read state build rather than for replication. For diagnostics.
    pub fn lsn_reached(&self, lsn: Option<u64>) -> bool {
        // Same order as `LsnView::observe`.
        let replication = *self.replication_lsn_rx.borrow();
        let commit = *self.last_commit_lsn_rx.borrow();
        let snapshot = *self.table_snapshot_watch_receiver.borrow();
        LsnView::normalize(replication, commit, snapshot).can_satisfy(lsn)
    }

    /// Current (snapshot, commit, replication) LSNs, for diagnostics.
    pub fn lsn_status(&self) -> (u64, u64, u64) {
        (
            *self.table_snapshot_watch_receiver.borrow(),
            *self.last_commit_lsn_rx.borrow(),
            *self.replication_lsn_rx.borrow(),
        )
    }

    fn check_healthy(status_rx: &mut watch::Receiver<TableStatus>) -> Result<()> {
        if let TableStatus::Errored(message) = &*status_rx.borrow_and_update() {
            return Err(crate::Error::table_errored(message));
        }
        Ok(())
    }

    async fn table_status_changed(status_rx: &mut watch::Receiver<TableStatus>) -> Result<()> {
        if status_rx.changed().await.is_err() {
            return Err(crate::Error::table_errored(
                "table dropped while a read was waiting for it",
            ));
        }
        Ok(())
    }

    #[inline]
    fn snapshot_is_clean(snapshot_lsn: u64, commit_lsn: u64) -> bool {
        // Snapshot clean when there's completely no updates to the snapshot.
        if snapshot_lsn == NO_SNAPSHOT_LSN && commit_lsn == NO_COMMIT_LSN {
            return true;
        }
        snapshot_lsn == commit_lsn && snapshot_lsn != NO_SNAPSHOT_LSN
    }

    /// Returns a snapshot whose commit LSN is:
    /// • >= `requested_lsn` when `requested_lsn` is supplied, or
    /// • the latest snapshot when `requested_lsn` is `None`.
    #[tracing::instrument(name = "read_state_try_read", skip_all)]
    pub async fn try_read(&self, requested_lsn: Option<u64>) -> Result<Arc<ReadState>> {
        let mut table_snapshot_rx = self.table_snapshot_watch_receiver.clone();
        let mut replication_lsn_rx = self.replication_lsn_rx.clone();
        let last_commit_lsn = self.last_commit_lsn_rx.clone();
        let mut table_status_rx = self.table_status_rx.clone();

        loop {
            Self::check_healthy(&mut table_status_rx)?;
            let view = LsnView::observe(
                &mut replication_lsn_rx,
                &last_commit_lsn,
                &mut table_snapshot_rx,
            );
            {
                let cache = self.cache.read().await;
                let snapshot_seq = self.snapshot_seq.load(Ordering::SeqCst);
                if cache.reusable(&view, snapshot_seq, requested_lsn) {
                    return Ok(cache.state.clone());
                }
            }
            if view.can_satisfy(requested_lsn) {
                return self.read_from_snapshot_and_update_cache(view).await;
            }
            // `observe` and `check_healthy` marked the receivers as seen, so these wait for the
            // next update.
            let wait_replication = requested_lsn.unwrap() > view.replication;
            tokio::select! {
                res = replication_lsn_rx.changed(), if wait_replication => res?,
                res = table_snapshot_rx.changed(), if !wait_replication => res?,
                res = Self::table_status_changed(&mut table_status_rx) => res?,
            }
        }
    }

    async fn read_from_snapshot_and_update_cache(&self, view: LsnView) -> Result<Arc<ReadState>> {
        // Between prepare_read and the finished ReadState, cache pins and the temp parquet are
        // owned by values only a complete ReadState releases. Build in a task of its own, so a
        // cancelled caller (client disconnect, LSN-wait timeout) cannot interrupt that window;
        // an abandoned build still publishes its state to the cache, which releases it normally.
        // The build's cleanup (error, panic) never waits for the table handler. A stuck build
        // holds the cache lock, so later reads of the table queue behind it; callers bound their
        // wait (scan_table's LSN-wait timeout), and a queued build that finally runs re-checks
        // the cache first.
        let build = Self::build_read_state(
            self.cache.clone(),
            self.snapshot_seq.clone(),
            self.live_reads.clone(),
            self.table_snapshot.clone(),
            self.read_state_filepath_remap.clone(),
            view,
        );
        match tokio::spawn(build).await {
            Ok(result) => result,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(e) => Err(e.into()),
        }
    }

    #[tracing::instrument(name = "update_read_state", skip_all)]
    async fn build_read_state(
        cache: Arc<RwLock<CachedRead>>,
        current_snapshot_seq: Arc<AtomicU64>,
        live_reads: Arc<LiveReadSeqs>,
        table_snapshot: Arc<RwLock<SnapshotTableState>>,
        read_state_filepath_remap: ReadStateFilepathRemap,
        view: LsnView,
    ) -> Result<Arc<ReadState>> {
        let mut cache = cache.write().await;
        // A reader whose view went stale while it waited for the lock: the cached state is newer
        // than its view, so it covers every LSN the view could satisfy.
        let cache_is_newer = cache
            .snapshot_key
            .is_some_and(|key| snapshot_lsn_is_newer(key, view.snapshot));
        if cache.snapshot_seq == current_snapshot_seq.load(Ordering::SeqCst) {
            if cache.snapshot_key == Some(view.snapshot) {
                // Same snapshot already materialized; only the label can grow.
                cache.valid_upto = cache.valid_upto.max(view.valid_upto());
                return Ok(cache.state.clone());
            }
            if cache_is_newer {
                return Ok(cache.state.clone());
            }
        }
        // Only collection happens under the snapshot lock; the temp parquet is written after it is
        // released. The cache write guard still serializes materialization per table.
        let (pending_read, live_read_guard) = {
            let mut table_state_snapshot = table_snapshot.write().await;
            let pending_read = table_state_snapshot.prepare_read().await?;
            // Register under the snapshot lock: from here on GC keeps this snapshot's files.
            let guard = live_reads.register(pending_read.snapshot_seq());
            (pending_read, guard)
        };
        let snapshot_seq = pending_read.snapshot_seq();
        #[cfg(test)]
        test_hooks::before_materialize(&table_snapshot).await;
        let snapshot_read_output = pending_read.materialize().await?;
        let state = snapshot_read_output
            .take_as_read_state(read_state_filepath_remap)
            .await?;
        state.attach_live_read_guard(live_read_guard);
        // The current snapshot is at least as new as both keys; keep the newer key and its label.
        let (snapshot_key, valid_upto) = if cache_is_newer {
            (cache.snapshot_key, cache.valid_upto)
        } else {
            (Some(view.snapshot), view.valid_upto())
        };
        // Publish only after the read state was built, so a failure keeps the previous key.
        *cache = CachedRead {
            snapshot_key,
            valid_upto,
            snapshot_seq,
            state: state.clone(),
        };
        Ok(state)
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use super::{ReadStateManager, RwLock, SnapshotTableState};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::sync::oneshot;

    enum Hook {
        Block(oneshot::Sender<()>, oneshot::Receiver<()>),
        Panic,
    }

    /// Armed hooks, keyed by the table snapshot they apply to, so that reads of other tables
    /// (tests run in parallel in one process) never take them.
    static BLOCK_MATERIALIZE: Mutex<Option<HashMap<usize, Hook>>> = Mutex::new(None);

    fn key(table_snapshot: &Arc<RwLock<SnapshotTableState>>) -> usize {
        Arc::as_ptr(table_snapshot) as usize
    }

    /// Make the next materialization of `manager`'s table report on the returned receiver and
    /// wait until the returned sender fires (or is dropped).
    pub(crate) fn block_next_materialize(
        manager: &ReadStateManager,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached_tx, reached_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        BLOCK_MATERIALIZE
            .lock()
            .unwrap()
            .get_or_insert_with(HashMap::new)
            .insert(
                key(&manager.table_snapshot),
                Hook::Block(reached_tx, release_rx),
            );
        (reached_rx, release_tx)
    }

    /// Make the next materialization of `manager`'s table panic.
    pub(crate) fn panic_next_materialize(manager: &ReadStateManager) {
        BLOCK_MATERIALIZE
            .lock()
            .unwrap()
            .get_or_insert_with(HashMap::new)
            .insert(key(&manager.table_snapshot), Hook::Panic);
    }

    pub(crate) async fn before_materialize(table_snapshot: &Arc<RwLock<SnapshotTableState>>) {
        let hook = BLOCK_MATERIALIZE
            .lock()
            .unwrap()
            .as_mut()
            .and_then(|hooks| hooks.remove(&key(table_snapshot)));
        match hook {
            Some(Hook::Block(reached, release)) => {
                let _ = reached.send(());
                let _ = release.await;
            }
            Some(Hook::Panic) => panic!("injected panic while building a read state"),
            None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_snapshot_clean() {
        struct Case {
            snapshot_lsn: u64,
            commit_lsn: u64,
            expected: bool,
        }

        let cases = [
            // Completely no updates for the snapshot.
            Case {
                snapshot_lsn: NO_SNAPSHOT_LSN,
                commit_lsn: NO_COMMIT_LSN,
                expected: true,
            },
            // All commits are sync-ed to the snapshot.
            Case {
                snapshot_lsn: 10,
                commit_lsn: 10,
                expected: true,
            },
            // Still commits not reflected to snapshot.
            Case {
                snapshot_lsn: 10,
                commit_lsn: 98,
                expected: false,
            },
        ];
        for (i, c) in cases.iter().enumerate() {
            assert_eq!(
                ReadStateManager::snapshot_is_clean(c.snapshot_lsn, c.commit_lsn),
                c.expected,
                "case {i} failed"
            );
        }
    }

    #[test]
    fn lsn_view_normalizes_without_panicking() {
        struct Case {
            replication: u64,
            commit: u64,
            snapshot: u64,
            requested: u64,
            satisfiable: bool,
            valid_upto: u64,
        }
        let cases = [
            // Steady state: everything at 100, replication ahead (keepalive).
            Case {
                replication: 150,
                commit: 100,
                snapshot: 100,
                requested: 150,
                satisfiable: true,
                valid_upto: 150,
            },
            // Sink marked commit 200, replication still 100 (moonlink_sink awaits the send): wait, no panic.
            Case {
                replication: 100,
                commit: 200,
                snapshot: 100,
                requested: 150,
                satisfiable: false,
                valid_upto: 100,
            },
            // ...but anything already in the snapshot is readable.
            Case {
                replication: 100,
                commit: 200,
                snapshot: 100,
                requested: 100,
                satisfiable: true,
                valid_upto: 100,
            },
            // Snapshot observed after a newer commit was applied (snapshot > commit read earlier).
            Case {
                replication: 100,
                commit: 100,
                snapshot: 300,
                requested: 300,
                satisfiable: true,
                valid_upto: 300,
            },
            // Unsnapshotted commit: not clean, label is the snapshot only.
            Case {
                replication: 300,
                commit: 300,
                snapshot: 100,
                requested: 300,
                satisfiable: false,
                valid_upto: 100,
            },
            // Fresh table: no snapshot, no commit.
            Case {
                replication: 50,
                commit: NO_COMMIT_LSN,
                snapshot: NO_SNAPSHOT_LSN,
                requested: 50,
                satisfiable: true,
                valid_upto: 50,
            },
            // No snapshot yet but a commit pending.
            Case {
                replication: 50,
                commit: 40,
                snapshot: NO_SNAPSHOT_LSN,
                requested: 10,
                satisfiable: false,
                valid_upto: 0,
            },
        ];
        for (i, c) in cases.iter().enumerate() {
            let view = LsnView::normalize(c.replication, c.commit, c.snapshot);
            assert_eq!(
                view.can_satisfy(Some(c.requested)),
                c.satisfiable,
                "case {i}"
            );
            assert_eq!(view.valid_upto(), c.valid_upto, "case {i}");
        }
    }

    #[tokio::test]
    async fn cached_read_reuse_is_keyed_by_snapshot() {
        let mk = |key: Option<u64>, valid_upto: u64| CachedRead {
            snapshot_key: key,
            valid_upto,
            snapshot_seq: 7,
            state: Arc::new(ReadState::new(
                vec![],
                vec![],
                vec![],
                vec![],
                vec![],
                vec![],
                Arc::new(|s| s),
            )),
        };
        let clean_at_100 = LsnView::normalize(150, 100, 100);
        let dirty_at_100 = LsnView::normalize(150, 120, 100);
        // Same snapshot, within label.
        assert!(mk(Some(100), 150).reusable(&clean_at_100, 7, Some(150)));
        // Same snapshot, beyond label.
        assert!(!mk(Some(100), 150).reusable(&clean_at_100, 7, Some(151)));
        // A later-marked commit at/below the label makes the view dirty: only the snapshot range is served.
        assert!(!mk(Some(100), 150).reusable(&dirty_at_100, 7, Some(120)));
        assert!(mk(Some(100), 150).reusable(&dirty_at_100, 7, Some(100)));
        // A newer snapshot was published: never reuse, even if the label is higher.
        assert!(!mk(Some(100), 200).reusable(&LsnView::normalize(200, 200, 200), 7, Some(200)));
        // Empty cache.
        assert!(!mk(None, u64::MAX).reusable(&clean_at_100, 7, Some(1)));
        // Latest read reuses only a clean current snapshot.
        assert!(mk(Some(100), 100).reusable(&clean_at_100, 7, None));
        assert!(!mk(Some(100), 100).reusable(&dirty_at_100, 7, None));
        // A maintenance-only snapshot (same LSN, new sequence) also forces a refresh.
        assert!(!mk(Some(100), 150).reusable(&clean_at_100, 8, Some(100)));
    }

    #[test]
    fn snapshot_lsn_order_treats_no_snapshot_as_oldest() {
        assert!(snapshot_lsn_is_newer(100, 50));
        assert!(!snapshot_lsn_is_newer(50, 100));
        assert!(!snapshot_lsn_is_newer(100, 100));
        assert!(snapshot_lsn_is_newer(0, NO_SNAPSHOT_LSN));
        assert!(!snapshot_lsn_is_newer(NO_SNAPSHOT_LSN, 100));
        assert!(!snapshot_lsn_is_newer(NO_SNAPSHOT_LSN, NO_SNAPSHOT_LSN));
    }

    #[tokio::test]
    async fn stale_view_reuses_newer_cached_read_state() {
        use crate::union_read::read_state_manager_tests::{assert_rows, Env};

        let mut env = Env::new("stale_view_reuses_newer_cached_read_state").await;
        env.commit_and_snapshot(&[1], 50, /*flush=*/ false).await;
        env.publish(50, 50, 50);
        // Reader A observes snapshot 50, then is delayed before taking the cache lock.
        let stale_view = LsnView::normalize(50, 50, 50);
        env.commit_and_snapshot(&[2], 100, /*flush=*/ false).await;
        env.publish(100, 100, 100);
        // Reader B materializes snapshot 100.
        let b = env.rsm.try_read(Some(100)).await.unwrap();
        assert_rows(&b, &[1, 2]).await;

        let a = env
            .rsm
            .read_from_snapshot_and_update_cache(stale_view)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&a, &b), "stale reader re-materialized");
        assert_eq!(env.rsm.cache.read().await.snapshot_key, Some(100));
        let next = env.rsm.try_read(Some(100)).await.unwrap();
        assert!(Arc::ptr_eq(&next, &b), "newer cache key was downgraded");
    }
}
