/// This module contains sender and receiver for table events synchronization.
use tokio::sync::{broadcast, oneshot, watch};

use crate::Result;

/// Health of one mirrored table. An errored table stops applying changes and serving reads, but
/// never affects other tables; the only remedy offered to users is drop + re-create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableStatus {
    Healthy,
    /// Carries the full user-facing message (see `Error::errored_table_message`).
    Errored(String),
}

/// Contains a few receivers, which get notified after certain iceberg events completion.
pub struct EventSyncReceiver {
    /// Get notified when drop table completes.
    pub drop_table_completion_rx: oneshot::Receiver<Result<()>>,
    /// Get notified when iceberg flush lsn advances.
    pub flush_lsn_rx: watch::Receiver<u64>,
    /// Get notified when force snapshot operation completes.
    pub force_snapshot_completion_rx: watch::Receiver<Option<Result<u64>>>,
    /// Used to create notifier when force table maintenance operation completes.
    pub table_maintenance_completion_tx: broadcast::Sender<Result<()>>,
    /// Current table status.
    pub table_status_rx: watch::Receiver<TableStatus>,
}

/// Contains a few senders, which notifies after certain iceberg events completion.
pub struct EventSyncSender {
    /// Notifies when drop table completes.
    pub drop_table_completion_tx: oneshot::Sender<Result<()>>,
    /// Notifies when iceberg flush LSN advances.
    pub flush_lsn_tx: watch::Sender<u64>,
    /// Notifies when force snapshot completes.
    /// There're a few states:
    /// - None: no completed iceberg snapshots
    /// - Ok(lsn): persisted table LSN
    /// - Err: iceberg snapshot fails
    pub force_snapshot_completion_tx: watch::Sender<Option<Result<u64>>>,
    /// Notifies when force table maintenance operation completes.
    pub table_maintenance_completion_tx: broadcast::Sender<Result<()>>,
    /// Publishes table status changes.
    pub table_status_tx: watch::Sender<TableStatus>,
}

/// Create table event manager sender and receiver.
pub fn create_table_event_syncer() -> (EventSyncSender, EventSyncReceiver) {
    let (drop_table_completion_tx, drop_table_completion_rx) = oneshot::channel();
    let (flush_lsn_tx, flush_lsn_rx) = watch::channel(0u64);
    let (force_snapshot_completion_tx, force_snapshot_completion_rx) = watch::channel(None);
    let (table_maintenance_completion_tx, _) = broadcast::channel(64usize);
    let (table_status_tx, table_status_rx) = watch::channel(TableStatus::Healthy);
    let event_sync_sender = EventSyncSender {
        drop_table_completion_tx,
        flush_lsn_tx,
        force_snapshot_completion_tx: force_snapshot_completion_tx.clone(),
        table_maintenance_completion_tx: table_maintenance_completion_tx.clone(),
        table_status_tx,
    };
    let event_sync_receiver = EventSyncReceiver {
        drop_table_completion_rx,
        flush_lsn_rx,
        force_snapshot_completion_rx,
        table_maintenance_completion_tx,
        table_status_rx,
    };
    (event_sync_sender, event_sync_receiver)
}
