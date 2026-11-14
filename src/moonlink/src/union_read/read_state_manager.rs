use crate::error::Result;
use crate::event_sync::TableStatus;
use crate::storage::MooncakeTable;
use crate::storage::SnapshotTableState;
use crate::ReadState;
use crate::ReadStateFilepathRemap;
use std::sync::Arc;
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
    state: Arc<ReadState>,
}

impl CachedRead {
    /// Rows up to the snapshot are always covered. Beyond it, also require the CURRENT view to be
    /// clean: a commit at or below `valid_upto` that was marked late (e.g. buffered during initial
    /// copy) makes the view dirty and forces a wait for its snapshot.
    fn reusable(&self, view: &LsnView, requested_lsn: Option<u64>) -> bool {
        if self.snapshot_key != Some(view.snapshot) {
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

pub struct ReadStateManager {
    cache: RwLock<CachedRead>,
    table_snapshot: Arc<RwLock<SnapshotTableState>>,
    table_snapshot_watch_receiver: watch::Receiver<u64>,
    replication_lsn_rx: watch::Receiver<u64>,
    last_commit_lsn_rx: watch::Receiver<u64>,
    /// Functor which maps local filepath to remote URI if possible, should be applied on all files within [`ReadState`].
    read_state_filepath_remap: ReadStateFilepathRemap,
    /// Table health; reads of an errored table fail instead of waiting or serving stale data.
    table_status_rx: Option<watch::Receiver<TableStatus>>,
}

impl ReadStateManager {
    pub fn new(
        table: &MooncakeTable,
        replication_lsn_rx: watch::Receiver<u64>,
        last_commit_lsn_rx: watch::Receiver<u64>,
        read_state_filepath_remap: ReadStateFilepathRemap,
    ) -> Self {
        let (table_snapshot, table_snapshot_watch_receiver) = table.get_state_for_reader();
        ReadStateManager {
            cache: RwLock::new(CachedRead {
                snapshot_key: None,
                valid_upto: 0,
                state: Arc::new(ReadState::new(
                    /*data_files=*/ Vec::new(),
                    /*puffin_cache_handles=*/ Vec::new(),
                    /*deletion_vectors_at_read=*/ Vec::new(),
                    /*position_deletes=*/ Vec::new(),
                    /*associated_files=*/ Vec::new(),
                    /*cache_handles=*/ Vec::new(),
                    read_state_filepath_remap.clone(), // Unused
                )),
            }),
            table_snapshot,
            table_snapshot_watch_receiver,
            replication_lsn_rx,
            last_commit_lsn_rx,
            read_state_filepath_remap,
            table_status_rx: None,
        }
    }

    /// Fail reads once the table enters the errored state, including reads waiting for an LSN.
    pub fn with_table_status(mut self, table_status_rx: watch::Receiver<TableStatus>) -> Self {
        self.table_status_rx = Some(table_status_rx);
        self
    }

    /// Current (snapshot, commit, replication) LSNs, for diagnostics.
    pub fn lsn_status(&self) -> (u64, u64, u64) {
        (
            *self.table_snapshot_watch_receiver.borrow(),
            *self.last_commit_lsn_rx.borrow(),
            *self.replication_lsn_rx.borrow(),
        )
    }

    fn check_healthy(status_rx: &mut Option<watch::Receiver<TableStatus>>) -> Result<()> {
        if let Some(rx) = status_rx {
            if let TableStatus::Errored(message) = &*rx.borrow_and_update() {
                return Err(crate::Error::table_errored(message));
            }
        }
        Ok(())
    }

    async fn table_status_changed(
        status_rx: &mut Option<watch::Receiver<TableStatus>>,
    ) -> Result<()> {
        match status_rx {
            Some(rx) => {
                if rx.changed().await.is_err() {
                    return Err(crate::Error::table_errored(
                        "table dropped while a read was waiting for it",
                    ));
                }
                Ok(())
            }
            None => std::future::pending().await,
        }
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
                if cache.reusable(&view, requested_lsn) {
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

    #[tracing::instrument(name = "update_read_state", skip_all)]
    async fn read_from_snapshot_and_update_cache(&self, view: LsnView) -> Result<Arc<ReadState>> {
        let mut cache = self.cache.write().await;
        if cache.snapshot_key == Some(view.snapshot) {
            // Same snapshot already materialized; only the label can grow.
            cache.valid_upto = cache.valid_upto.max(view.valid_upto());
            return Ok(cache.state.clone());
        }
        // Only collection happens under the snapshot lock; the temp parquet is written after it is
        // released. The cache write guard still serializes materialization per table.
        let pending_read = {
            let mut table_state_snapshot = self.table_snapshot.write().await;
            table_state_snapshot.prepare_read().await?
        };
        #[cfg(test)]
        test_hooks::before_materialize().await;
        let snapshot_read_output = pending_read.materialize().await?;
        let state = snapshot_read_output
            .take_as_read_state(self.read_state_filepath_remap.clone())
            .await?;
        // Publish only after the read state was built, so a failure keeps the previous key.
        *cache = CachedRead {
            snapshot_key: Some(view.snapshot),
            valid_upto: view.valid_upto(),
            state: state.clone(),
        };
        Ok(state)
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    /// When set, the next materialization reports `reached` and waits for `release`.
    pub(crate) static BLOCK_MATERIALIZE: Mutex<
        Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
    > = Mutex::new(None);

    pub(crate) async fn before_materialize() {
        let hook = BLOCK_MATERIALIZE.lock().unwrap().take();
        if let Some((reached, release)) = hook {
            let _ = reached.send(());
            let _ = release.await;
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
        assert!(mk(Some(100), 150).reusable(&clean_at_100, Some(150)));
        // Same snapshot, beyond label.
        assert!(!mk(Some(100), 150).reusable(&clean_at_100, Some(151)));
        // A later-marked commit at/below the label makes the view dirty: only the snapshot range is served.
        assert!(!mk(Some(100), 150).reusable(&dirty_at_100, Some(120)));
        assert!(mk(Some(100), 150).reusable(&dirty_at_100, Some(100)));
        // A newer snapshot was published: never reuse, even if the label is higher.
        assert!(!mk(Some(100), 200).reusable(&LsnView::normalize(200, 200, 200), Some(200)));
        // Empty cache.
        assert!(!mk(None, u64::MAX).reusable(&clean_at_100, Some(1)));
        // Latest read reuses only a clean current snapshot.
        assert!(mk(Some(100), 100).reusable(&clean_at_100, None));
        assert!(!mk(Some(100), 100).reusable(&dirty_at_100, None));
    }
}
