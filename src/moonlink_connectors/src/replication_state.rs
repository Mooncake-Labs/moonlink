use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::watch;

/// Tracks replication progress and notifies listeners when the replicated
/// LSN advances.
pub struct ReplicationState {
    current: AtomicU64,
    tx: watch::Sender<u64>,
}

impl std::fmt::Debug for ReplicationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationState")
            .field("current", &self.current.load(Ordering::SeqCst))
            .finish()
    }
}

impl ReplicationState {
    /// Create a new state initialised to LSN 0.
    pub fn new() -> Arc<Self> {
        let (tx, _rx) = watch::channel(0);
        Arc::new(Self {
            current: AtomicU64::new(0),
            tx,
        })
    }

    /// Mark the replication position as `lsn` if it is newer than the current
    /// value.
    pub fn mark(&self, lsn: u64) {
        if lsn > self.current.load(Ordering::Relaxed) {
            self.current.store(lsn, Ordering::Release);
            self.tx.send(lsn).unwrap();
        }
    }

    /// Subscribe for async notifications when the replicated LSN advances.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.tx.subscribe()
    }

    /// Get the current replicated LSN value.
    pub fn now(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }
}
