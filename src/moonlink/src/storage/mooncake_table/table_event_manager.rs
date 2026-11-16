/// This module interacts with iceberg snapshot status, which corresponds to one mooncake table.
use tokio::sync::{broadcast, mpsc, oneshot, watch};

use crate::event_sync::{EventSyncReceiver, TableStatus};
use crate::Result;
use crate::TableEvent;

/// At most one outstanding snapshot request is allowed.
pub struct TableEventManager {
    /// Used to initiate a mooncake and iceberg snapshot operation.
    table_event_tx: mpsc::Sender<TableEvent>,
    /// Used to synchronize on the completion of a drop table operation.
    drop_table_completion_rx: Option<oneshot::Receiver<Result<()>>>,
    /// Channel to observe latest flush LSN reported by iceberg.
    flush_lsn_rx: watch::Receiver<u64>,
    /// Sender which is used to create notification at latest force snapshot completion.
    force_snapshot_completion_rx: watch::Receiver<Option<Result<u64>>>,
    /// Sender which is used to create notification at latest data compaction completion.
    table_maintenance_completion_tx: broadcast::Sender<Result<()>>,
    /// Table health; errored tables reject reads and requests.
    table_status_rx: watch::Receiver<TableStatus>,
}

impl TableEventManager {
    pub fn new(
        table_event_tx: mpsc::Sender<TableEvent>,
        table_event_sync_rx: EventSyncReceiver,
    ) -> Self {
        Self {
            table_event_tx,
            drop_table_completion_rx: Some(table_event_sync_rx.drop_table_completion_rx),
            flush_lsn_rx: table_event_sync_rx.flush_lsn_rx,
            force_snapshot_completion_rx: table_event_sync_rx.force_snapshot_completion_rx,
            table_maintenance_completion_tx: table_event_sync_rx.table_maintenance_completion_tx,
            table_status_rx: table_event_sync_rx.table_status_rx,
        }
    }

    /// Test-only: make the table's handler task die, like an unexpected crash.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn kill_table_handler_for_testing(&self) {
        let _ = self.table_event_tx.send(TableEvent::KillForTest).await;
    }

    /// Current table status.
    pub fn table_status(&self) -> TableStatus {
        self.table_status_rx.borrow().clone()
    }

    /// Return an error if the table has entered the errored state.
    pub fn check_healthy(&self) -> Result<()> {
        match &*self.table_status_rx.borrow() {
            TableStatus::Healthy => Ok(()),
            TableStatus::Errored(message) => Err(crate::Error::table_errored(message)),
        }
    }

    /// Request the table to enter the errored state (e.g. CDC saw a change it cannot represent).
    /// Idempotent; returns an error only if the table handler is gone.
    pub async fn enter_errored(&self, reason: String) -> Result<()> {
        self.table_event_tx
            .send(TableEvent::EnterErrored { reason })
            .await
            .map_err(|_| self_stopped_error())
    }

    /// Subscribe to table status changes.
    pub fn subscribe_table_status(&self) -> watch::Receiver<TableStatus> {
        self.table_status_rx.clone()
    }

    /// Subscribe to flush LSN updates.
    pub fn subscribe_flush_lsn(&self) -> watch::Receiver<u64> {
        self.flush_lsn_rx.clone()
    }

    /// Initiate an iceberg snapshot event, return the channel for synchronization.
    pub async fn initiate_snapshot(&mut self, lsn: u64) -> watch::Receiver<Option<Result<u64>>> {
        // Only results published after this request count; an error from an earlier, failed
        // attempt must not fail this one.
        let mut rx = self.force_snapshot_completion_rx.clone();
        rx.mark_unchanged();
        // If the handler is gone, the receiver reports it on the first wait.
        let _ = self
            .table_event_tx
            .send(TableEvent::ForceSnapshot { lsn: Some(lsn) })
            .await;
        rx
    }

    /// Util function to decide whether the current LSN satisfies the requested LSN.
    fn is_iceberg_snapshot_ready(
        current_lsn: &Option<Result<u64>>,
        requested_lsn: u64,
    ) -> Result<bool> {
        match current_lsn {
            Some(Ok(current_lsn)) => Ok(*current_lsn >= requested_lsn),
            Some(Err(e)) => Err(e.clone()),
            None => Ok(false),
        }
    }

    /// Synchronize on the requested LSN for force snapshot request.
    pub async fn synchronize_force_snapshot_request(
        mut rx: watch::Receiver<Option<Result<u64>>>,
        requested_lsn: u64,
    ) -> Result<()> {
        // Fast-path: the persisted table LSN already satisfies the request. A stale error from an
        // earlier attempt is ignored here; only errors published after the request fail it.
        if let Some(Ok(current_lsn)) = &*rx.borrow() {
            if *current_lsn >= requested_lsn {
                return Ok(());
            }
        }

        // Otherwise falls back to loop until requested LSN is met.
        loop {
            if rx.changed().await.is_err() {
                return Err(self_stopped_error());
            }
            if Self::is_iceberg_snapshot_ready(&rx.borrow_and_update(), requested_lsn)? {
                break;
            }
        }

        Ok(())
    }

    /// Initiate an index merge event, return the channel for synchronization.
    /// Fails at once if the table handler is gone; an errored table answers with its error.
    pub async fn initiate_index_merge(&mut self) -> Result<broadcast::Receiver<Result<()>>> {
        self.initiate_maintenance(TableEvent::ForceRegularIndexMerge)
            .await
    }

    /// Initiate a data compaction event, return the channel for synchronization.
    /// Fails at once if the table handler is gone; an errored table answers with its error.
    pub async fn initiate_data_compaction(&mut self) -> Result<broadcast::Receiver<Result<()>>> {
        self.initiate_maintenance(TableEvent::ForceRegularDataCompaction)
            .await
    }

    /// Initiate full table maintenance event, return the channel for synchronization.
    /// Fails at once if the table handler is gone; an errored table answers with its error.
    pub async fn initiate_full_compaction(&mut self) -> Result<broadcast::Receiver<Result<()>>> {
        self.initiate_maintenance(TableEvent::ForceFullMaintenance)
            .await
    }

    async fn initiate_maintenance(
        &mut self,
        event: TableEvent,
    ) -> Result<broadcast::Receiver<Result<()>>> {
        let subscriber = self.table_maintenance_completion_tx.subscribe();
        self.table_event_tx
            .send(event)
            .await
            .map_err(|_| self_stopped_error())?;
        Ok(subscriber)
    }

    /// Wait for the result of a maintenance request started with an `initiate_*` method.
    pub async fn synchronize_maintenance_request(
        mut rx: broadcast::Receiver<Result<()>>,
    ) -> Result<()> {
        loop {
            match rx.recv().await {
                Ok(result) => return result,
                // The handler answers maintenance requests in order, so a later result still
                // means this request has finished.
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return Err(self_stopped_error()),
            }
        }
    }

    /// Drop a mooncake table.
    /// Each table event manager correspond to one mooncake table, so this function should be called at most once.
    pub async fn drop_table(&mut self) -> Result<()> {
        if self
            .table_event_tx
            .send(TableEvent::DropTable)
            .await
            .is_err()
        {
            return Err(self_stopped_error());
        }
        let Some(rx) = self.drop_table_completion_rx.take() else {
            return Err(crate::Error::table_handler_stopped(
                "table is already being dropped",
            ));
        };
        match rx.await {
            Ok(res) => res,
            Err(_) => Err(self_stopped_error()),
        }
    }
}

#[track_caller]
fn self_stopped_error() -> crate::Error {
    crate::Error::table_handler_stopped(
        "table handler stopped unexpectedly; drop and re-create the mirror",
    )
}

#[cfg(test)]
mod maintenance_request_tests {
    use super::*;
    use crate::event_sync::create_table_event_syncer;

    fn manager_without_handler() -> TableEventManager {
        let (table_event_tx, table_event_rx) = mpsc::channel(1);
        drop(table_event_rx);
        let (_sender, receiver) = create_table_event_syncer();
        TableEventManager::new(table_event_tx, receiver)
    }

    /// A maintenance request to a table whose handler is gone fails instead of panicking.
    #[tokio::test]
    async fn initiate_without_handler_fails() {
        let mut manager = manager_without_handler();
        assert!(matches!(
            manager.initiate_index_merge().await,
            Err(crate::Error::TableHandlerStopped(_))
        ));
        assert!(matches!(
            manager.initiate_data_compaction().await,
            Err(crate::Error::TableHandlerStopped(_))
        ));
        assert!(matches!(
            manager.initiate_full_compaction().await,
            Err(crate::Error::TableHandlerStopped(_))
        ));
    }

    /// Waiting for a maintenance result returns the handler's error, and an error (not a panic)
    /// once the result channel is closed; a lagged receiver keeps waiting for a result.
    #[tokio::test]
    async fn synchronize_maintenance_request_results() {
        let (tx, rx) = broadcast::channel::<Result<()>>(1);
        tx.send(Err(crate::Error::table_errored("mirror t is out of sync")))
            .unwrap();
        assert!(matches!(
            TableEventManager::synchronize_maintenance_request(rx).await,
            Err(crate::Error::TableErrored(_))
        ));

        let rx = tx.subscribe();
        drop(tx);
        assert!(matches!(
            TableEventManager::synchronize_maintenance_request(rx).await,
            Err(crate::Error::TableHandlerStopped(_))
        ));

        let (tx, rx) = broadcast::channel::<Result<()>>(1);
        tx.send(Err(crate::Error::table_errored("lost"))).unwrap();
        tx.send(Ok(())).unwrap();
        assert!(TableEventManager::synchronize_maintenance_request(rx)
            .await
            .is_ok());
    }
}
