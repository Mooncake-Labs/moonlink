use crate::storage::MooncakeTable;
use crate::storage::SnapshotTableState;
use crate::union_read::read_state::ReadState;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, RwLock};
pub struct ReadStateManager {
    last_read_lsn: AtomicU64,
    last_read_state: RwLock<Option<Arc<ReadState>>>,
    table_snapshot: Arc<RwLock<SnapshotTableState>>,
    table_snapshot_watch_receiver: watch::Receiver<u64>,
}

impl ReadStateManager {
    pub fn new(table: &MooncakeTable) -> Self {
        let (table_snapshot, table_snapshot_watch_receiver) = table.get_state_for_reader();
        ReadStateManager {
            last_read_lsn: AtomicU64::new(0),
            last_read_state: RwLock::new(None),
            table_snapshot,
            table_snapshot_watch_receiver: table_snapshot_watch_receiver,
        }
    }

    /// Read after a specific lsn
    pub async fn try_read(&self, lsn: Option<u64>) -> Option<Arc<ReadState>> {
        if lsn.is_some() && lsn.unwrap() < self.last_read_lsn.load(Ordering::Relaxed) {
            let last_state = self.last_read_state.read().await;
            return last_state.clone();
        }
        let mut table_lsn = self.table_snapshot_watch_receiver.clone();
        loop {
            {
                let table_lsn = *table_lsn.borrow();
                if lsn.is_none() || lsn.unwrap() <= table_lsn {
                    let table_state = self.table_snapshot.read().await;
                    let mut last_state = self.last_read_state.write().await;
                    if self.last_read_lsn.load(Ordering::Acquire) != table_lsn {
                        let ret = table_state.request_read().unwrap();
                        // TODO: avoid transformation
                        let formated = (
                            ret.0
                                .into_iter()
                                .map(|x| x.to_string_lossy().to_string())
                                .collect(),
                            ret.1
                                .into_iter()
                                .map(|x| (x.0 as u32, x.1 as u32))
                                .collect(),
                        );
                        self.last_read_lsn.store(table_lsn, Ordering::Release);
                        *last_state = Some(Arc::new(ReadState::new(formated)));
                        return last_state.clone();
                    } else {
                        return last_state.clone();
                    }
                }
            }
            table_lsn.changed().await.unwrap();
        }
    }
}
