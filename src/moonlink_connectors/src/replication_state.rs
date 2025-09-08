use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::watch;
use tracing::warn;

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
        if lsn > self.current.load(Ordering::SeqCst) {
            self.current.store(lsn, Ordering::SeqCst);
            // Ignore send error if there are no subscribers (e.g., during shutdown)
            if let Err(e) = self.tx.send(lsn) {
                warn!(error = ?e, "failed to send replication state for lsn {}", lsn);
            }
        }
    }

    /// Subscribe for async notifications when the replicated LSN advances.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.tx.subscribe()
    }

    /// Get the current replicated LSN value.
    pub fn now(&self) -> u64 {
        self.current.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_without_subscribers_does_not_panic_and_updates_state() {
        let state = ReplicationState::new();
        assert_eq!(state.now(), 0);
        // No subscribers have been created; this will panic without the fix.
        state.mark(42);
        assert_eq!(state.now(), 42);
    }

    #[test]
    fn mark_after_last_subscriber_dropped_does_not_panic() {
        let state = ReplicationState::new();
        // Create a subscriber and then drop it to simulate shutdown.
        let rx = state.subscribe();
        drop(rx);
        // This will panic without the fix because there are no receivers.
        state.mark(100);
        assert_eq!(state.now(), 100);
    }
}
