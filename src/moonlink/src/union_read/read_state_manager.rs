use crate::error::Result;
use crate::storage::MooncakeTable;
use crate::storage::ReadOutput;
use crate::storage::SnapshotTableState;
use crate::union_read::read_state::ReadState;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, RwLock};

pub struct ReadStateManager {
    last_read_lsn: AtomicU64,
    last_read_state: RwLock<Arc<ReadState>>,
    table_snapshot: Arc<RwLock<SnapshotTableState>>,
    table_snapshot_watch_receiver: watch::Receiver<u64>,
    replication_lsn_rx: watch::Receiver<u64>,
    table_commit_lsn_rx: watch::Receiver<u64>,
}

impl ReadStateManager {
    pub fn new(
        table: &MooncakeTable,
        replication_lsn_rx: watch::Receiver<u64>,
        table_commit_lsn_rx: watch::Receiver<u64>,
    ) -> Self {
        let (table_snapshot, table_snapshot_watch_receiver) = table.get_state_for_reader();
        ReadStateManager {
            last_read_lsn: AtomicU64::new(0),
            last_read_state: RwLock::new(Arc::new(ReadState::new((vec![], vec![]), vec![]))),
            table_snapshot,
            table_snapshot_watch_receiver,
            replication_lsn_rx,
            table_commit_lsn_rx,
        }
    }

    /// Read after a specific lsn
    pub async fn try_read(&self, lsn: Option<u64>) -> Result<Arc<ReadState>> {
        if lsn.is_some() && lsn.unwrap() < self.last_read_lsn.load(Ordering::Relaxed) {
            let last_state = self.last_read_state.read().await;
            return Ok(last_state.clone());
        }
        let mut table_snapshot_lsn = self.table_snapshot_watch_receiver.clone();
        let mut replication_lsn = self.replication_lsn_rx.clone();
        let table_commit_lsn = self.table_commit_lsn_rx.clone();
        loop {
            {
                let table_snapshot_lsn = *table_snapshot_lsn.borrow();
                let replication_lsn = *replication_lsn.borrow();
                let table_commit_lsn = *table_commit_lsn.borrow();
                if lsn.is_none()
                    || lsn.unwrap() <= table_snapshot_lsn
                    || (lsn.unwrap() <= replication_lsn && table_snapshot_lsn == table_commit_lsn)
                {
                    let table_state = self.table_snapshot.read().await;
                    let mut last_state = self.last_read_state.write().await;
                    if self.last_read_lsn.load(Ordering::Acquire) < table_snapshot_lsn {
                        // we have replicated and snapshot all changed on table before replication_Lsn
                        // then we can set lsn to replication_lsn instead of table_snapshot_lsn
                        let effective_lsn = if table_snapshot_lsn == table_commit_lsn
                            && table_snapshot_lsn < replication_lsn
                        {
                            replication_lsn
                        } else {
                            table_snapshot_lsn
                        };
                        let ReadOutput {
                            file_paths,
                            deletions,
                            associated_files,
                        } = table_state.request_read()?;
                        self.last_read_lsn.store(effective_lsn, Ordering::Release);
                        *last_state =
                            Arc::new(ReadState::new((file_paths, deletions), associated_files));
                        return Ok(last_state.clone());
                    } else {
                        return Ok(last_state.clone());
                    }
                }
            }
            if lsn.unwrap() > *replication_lsn.borrow() {
                replication_lsn
                    .changed()
                    .await
                    .expect("replacation lsn sender dropped unexpectedly");
            } else {
                table_snapshot_lsn
                    .changed()
                    .await
                    .expect("table snapshot lsn sender dropped unexpectedly");
            }
        }
    }
}
