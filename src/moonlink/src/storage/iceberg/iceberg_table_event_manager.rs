/// This module interacts with iceberg snapshot status.
use tokio::sync::mpsc;

use crate::Result;
use crate::TableEvent;

/// Contains a few receivers, which get notified after certain iceberg events completion.
pub struct IcebergEventSyncReceiver {
    /// Get notified when iceberg drop table completes.
    pub iceberg_drop_table_completion_rx: mpsc::Receiver<Result<()>>,
}

/// At most one outstanding snapshot request is allowed.
pub struct IcebergTableEventManager {
    /// Used to initiate a mooncake and iceberg snapshot operation.
    table_event_tx: mpsc::Sender<TableEvent>,
    /// Used to synchronize on the completion of an iceberg drop table.
    drop_table_completion_rx: mpsc::Receiver<Result<()>>,
}

impl IcebergTableEventManager {
    pub fn new(
        table_event_tx: mpsc::Sender<TableEvent>,
        iceberg_event_sync_rx: IcebergEventSyncReceiver,
    ) -> Self {
        Self {
            table_event_tx,
            drop_table_completion_rx: iceberg_event_sync_rx.iceberg_drop_table_completion_rx,
        }
    }

    /// Initiate an iceberg snapshot event, return the channel for synchronization.
    pub async fn initiate_snapshot(&mut self, lsn: u64) -> mpsc::Receiver<Result<()>> {
        let (tx, rx) = mpsc::channel(1);
        self.table_event_tx
            .send(TableEvent::ForceSnapshot { lsn, tx })
            .await
            .unwrap();
        rx
    }

    /// Drop an iceberg table.
    pub async fn drop_table(&mut self) -> Result<()> {
        self.table_event_tx
            .send(TableEvent::DropIcebergTable)
            .await
            .unwrap();
        self.drop_table_completion_rx.recv().await.unwrap()
    }
}
