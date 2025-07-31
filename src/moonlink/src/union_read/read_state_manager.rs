use crate::error::Error;
use crate::error::Result;
use crate::storage::MooncakeTable;
use crate::storage::SnapshotTableState;
use crate::ReadState;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, RwLock};

pub struct ReadStateManager {
    last_read_lsn: AtomicU64,
    last_read_state: RwLock<Arc<ReadState>>,
    table_snapshot: Arc<RwLock<SnapshotTableState>>,
    table_snapshot_watch_receiver: watch::Receiver<u64>,
    replication_lsn_rx: watch::Receiver<u64>,
    last_commit_lsn_rx: watch::Receiver<u64>,
}

impl ReadStateManager {
    pub fn new(
        table: &MooncakeTable,
        replication_lsn_rx: watch::Receiver<u64>,
        last_commit_lsn_rx: watch::Receiver<u64>,
    ) -> Self {
        let (table_snapshot, table_snapshot_watch_receiver) = table.get_state_for_reader();
        ReadStateManager {
            last_read_lsn: AtomicU64::new(u64::MAX),
            last_read_state: RwLock::new(Arc::new(ReadState::new(
                /*data_files=*/ Vec::new(),
                /*puffin_cache_handles=*/ Vec::new(),
                /*deletion_vectors_at_read=*/ Vec::new(),
                /*position_deletes=*/ Vec::new(),
                /*associated_files=*/ Vec::new(),
                /*cache_handles=*/ Vec::new(),
            ))),
            table_snapshot,
            table_snapshot_watch_receiver,
            replication_lsn_rx,
            last_commit_lsn_rx,
        }
    }

    #[inline]
    fn snapshot_is_clean(snapshot_lsn: u64, commit_lsn: u64) -> bool {
        // Assume dirty when uninitialized.
        snapshot_lsn == commit_lsn && snapshot_lsn != u64::MAX
    }

    #[inline]
    fn should_use_cache(
        requested: Option<u64>,
        cached_lsn: u64,
        snapshot_lsn: u64,
        commit_lsn: u64,
    ) -> bool {
        // Never use cache if it's uninitialized (cached_lsn = u64::MAX)
        if cached_lsn == u64::MAX {
            return false;
        }

        let snapshot_clean = Self::snapshot_is_clean(snapshot_lsn, commit_lsn);
        match requested {
            Some(bound) => cached_lsn == snapshot_lsn && cached_lsn >= bound && snapshot_clean,
            None => cached_lsn == snapshot_lsn && snapshot_clean,
        }
    }

    /// Returns a snapshot whose commit LSN is:
    /// • >= `requested_lsn` when `requested_lsn` is supplied, or
    /// • the latest snapshot when `requested_lsn` is `None`.
    #[tracing::instrument(name = "read_state_try_read", skip_all)]
    pub async fn try_read(&self, requested_lsn: Option<u64>) -> Result<Arc<ReadState>> {
        // fast-path: reuse cached snapshot only when its still the tables latest and not newer than the callers LSN
        let cached_lsn = self.last_read_lsn.load(Ordering::Relaxed);
        let snapshot_lsn_now = *self.table_snapshot_watch_receiver.borrow();
        let commit_lsn_now = *self.last_commit_lsn_rx.borrow();

        let use_cache =
            Self::should_use_cache(requested_lsn, cached_lsn, snapshot_lsn_now, commit_lsn_now);

        if use_cache {
            return Ok(self.last_read_state.read().await.clone());
        }

        let mut table_snapshot_rx = self.table_snapshot_watch_receiver.clone();
        let mut replication_lsn_rx = self.replication_lsn_rx.clone();
        let last_commit_lsn = self.last_commit_lsn_rx.clone();

        loop {
            let current_snapshot_lsn = *table_snapshot_rx.borrow();
            let current_replication_lsn = *replication_lsn_rx.borrow();

            let last_commit_lsn_val = *last_commit_lsn.borrow();
            if self.can_satisfy_read_from_snapshot(
                requested_lsn,
                current_snapshot_lsn,
                current_replication_lsn,
                last_commit_lsn_val,
            ) {
                return self
                    .read_from_snapshot_and_update_cache(
                        current_snapshot_lsn,
                        current_replication_lsn,
                        last_commit_lsn_val,
                    )
                    .await;
            }

            self.wait_for_relevant_lsn_change(
                requested_lsn.unwrap(),
                current_replication_lsn,
                &mut replication_lsn_rx,
                &mut table_snapshot_rx,
            )
            .await?;
        }
    }

    fn can_satisfy_read_from_snapshot(
        &self,
        requested_lsn: Option<u64>,
        snapshot_lsn: u64,
        replication_lsn: u64,
        commit_lsn: u64,
    ) -> bool {
        let is_snapshot_clean = Self::snapshot_is_clean(snapshot_lsn, commit_lsn);
        let is_snapshot_initialized = snapshot_lsn != u64::MAX;
        match requested_lsn {
            // If no specific LSN is requested, we can always try to read the latest.
            None => true,
            Some(req_lsn_val) => {
                // Request can be satisfied if:
                // 1. The requested LSN is already covered by the table snapshot.
                // OR
                // 2. The requested LSN is covered by replication, AND the snapshot is clean
                is_snapshot_initialized && req_lsn_val <= snapshot_lsn
                    || (req_lsn_val <= replication_lsn && is_snapshot_clean)
            }
        }
    }

    #[tracing::instrument(name = "update_read_state", skip_all)]
    async fn read_from_snapshot_and_update_cache(
        &self,
        current_snapshot_lsn: u64,
        current_replication_lsn: u64,
        current_commit_lsn: u64,
    ) -> Result<Arc<ReadState>> {
        let mut table_state_snapshot = self.table_snapshot.write().await;
        let mut last_read_state_guard = self.last_read_state.write().await;
        let is_snapshot_clean = current_snapshot_lsn == current_commit_lsn;

        let last_read_lsn = self.last_read_lsn.load(Ordering::Acquire);
        if last_read_lsn < current_snapshot_lsn || last_read_lsn == u64::MAX {
            // Only calculate effective_lsn if we're not uninitialized
            let effective_lsn = if last_read_lsn == u64::MAX {
                // For uninitialized cache, just use the current snapshot LSN
                current_snapshot_lsn
            } else {
                // If the snapshot is fully committed and replication has progressed further,
                // we can consider the state valid up to the replication LSN.
                if is_snapshot_clean && current_snapshot_lsn < current_replication_lsn {
                    current_replication_lsn
                } else {
                    current_snapshot_lsn
                }
            };

            let snapshot_read_output = table_state_snapshot.request_read().await?;

            self.last_read_lsn.store(effective_lsn, Ordering::Release);
            *last_read_state_guard = snapshot_read_output.take_as_read_state().await;
        }
        Ok(last_read_state_guard.clone())
    }

    #[tracing::instrument(name = "wait_for_lsn", skip_all)]
    async fn wait_for_relevant_lsn_change(
        &self,
        requested_lsn_val: u64,
        current_replication_lsn: u64,
        replication_lsn_rx: &mut watch::Receiver<u64>,
        table_snapshot_rx: &mut watch::Receiver<u64>,
    ) -> Result<()> {
        if requested_lsn_val > current_replication_lsn {
            replication_lsn_rx
                .changed()
                .await
                .map_err(|e| Error::WatchChannelRecvError { source: e })?;
        } else {
            table_snapshot_rx
                .changed()
                .await
                .map_err(|e| Error::WatchChannelRecvError { source: e })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_decision_matrix() {
        struct Case {
            requested: Option<u64>,
            cached: u64,
            snap: u64,
            commit: u64,
            expect: bool,
        }

        let cases = [
            // hit: bounded read, snapshot is latest and within bound
            Case {
                requested: Some(42),
                cached: 42,
                snap: 42,
                commit: 42,
                expect: true,
            },
            // hit: bounded read, cache newer than caller wants
            Case {
                requested: Some(10),
                cached: 20,
                snap: 20,
                commit: 20,
                expect: true,
            },
            // hit: latest read, snapshot clean
            Case {
                requested: None,
                cached: 100,
                snap: 100,
                commit: 100,
                expect: true,
            },
            // miss: latest read, table advanced since cache
            Case {
                requested: None,
                cached: 50,
                snap: 60,
                commit: 60,
                expect: false,
            },
            // miss: bounded read, dirty snapshot (snapshot behind commit)
            Case {
                requested: Some(20),
                cached: 10,
                snap: 10,
                commit: 20,
                expect: false,
            },
            // miss: bounded read, dirty snapshot (snapshot ahead of commit)
            Case {
                requested: Some(30),
                cached: 25,
                snap: 25,
                commit: 20,
                expect: false,
            },
            // miss: latest read, dirty snapshot (snapshot behind commit)
            Case {
                requested: None,
                cached: 50,
                snap: 50,
                commit: 60,
                expect: false,
            },
            // miss: latest read, dirty snapshot (snapshot ahead of commit)
            Case {
                requested: None,
                cached: 70,
                snap: 70,
                commit: 65,
                expect: false,
            },
            // miss: uninitialized cache (cached_lsn = u64::MAX)
            Case {
                requested: Some(10),
                cached: u64::MAX,
                snap: 10,
                commit: 10,
                expect: false,
            },
            // miss: uninitialized cache for latest read
            Case {
                requested: None,
                cached: u64::MAX,
                snap: 10,
                commit: 10,
                expect: false,
            },
            // miss: valid LSN 0 cache, bounded read outside of request
            Case {
                requested: Some(5),
                cached: 0,
                snap: 0,
                commit: 0,
                expect: false,
            },
            // hit: valid LSN 0 cache for latest read, snapshot clean
            Case {
                requested: None,
                cached: 0,
                snap: 0,
                commit: 0,
                expect: true,
            },
        ];

        for (i, c) in cases.iter().enumerate() {
            assert_eq!(
                ReadStateManager::should_use_cache(c.requested, c.cached, c.snap, c.commit),
                c.expect,
                "case {i} failed"
            );
        }
    }
}
