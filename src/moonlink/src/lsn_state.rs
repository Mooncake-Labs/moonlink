use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::watch;

/// Tracks replication progress and notifies listeners when the replicated
/// LSN advances.
pub struct LsnState {
    current: AtomicU64,
    tx: watch::Sender<u64>,
}

impl std::fmt::Debug for LsnState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LsnState")
            .field("current", &self.current.load(Ordering::SeqCst))
            .finish()
    }
}

impl LsnState {
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
            // `send` drops the value when nobody is subscribed yet, and later subscribers would
            // then start from a stale LSN.
            self.tx.send_replace(lsn);
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

pub type ReplicationState = LsnState;
pub type CommitState = LsnState;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_without_subscribers_does_not_panic_and_updates_state() {
        let state = LsnState::new();
        assert_eq!(state.now(), 0);
        // No subscribers have been created; this will panic without the fix.
        state.mark(42);
        assert_eq!(state.now(), 42);
    }

    #[test]
    fn mark_after_last_subscriber_dropped_does_not_panic() {
        let state = LsnState::new();
        // Create a subscriber and then drop it to simulate shutdown.
        let rx = state.subscribe();
        drop(rx);
        // This will panic without the fix because there are no receivers.
        state.mark(100);
        assert_eq!(state.now(), 100);
    }

    #[test]
    fn subscriber_created_after_mark_sees_latest_lsn() {
        let state = LsnState::new();
        state.mark(42);
        // Not newer than the current value, so nothing is sent.
        state.mark(42);
        assert_eq!(*state.subscribe().borrow(), 42);
    }
}
