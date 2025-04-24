use crate::storage::MooncakeTable;
use crate::storage::SnapshotTableState;
use crate::union_read::read_state::ReadState;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
pub struct ReadStateManager {
    last_read_state: Mutex<(u64, Option<Arc<ReadState>>)>,
    table_snapshot: Arc<RwLock<SnapshotTableState>>,
}

impl ReadStateManager {
    pub fn new(table: &MooncakeTable) -> Self {
        ReadStateManager {
            last_read_state: Mutex::new((0, None)),
            table_snapshot: table.get_table_state(),
        }
    }

    pub async fn try_read(&self) -> Option<Arc<ReadState>> {
        let table_state = self.table_snapshot.read().await;
        let table_version = table_state.get_version();
        let mut last_state = self.last_read_state.lock().await;
        if last_state.0 != table_version {
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
            *last_state = (table_version, Some(Arc::new(ReadState::new(formated))));
        }
        return last_state.1.clone();
    }
}
