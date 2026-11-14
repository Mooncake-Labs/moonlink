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
        // Concurrent marks (the replication loop, table setup, the initial copy) must never make
        // either value move backwards: readers assert LSN ordering across these watermarks.
        if self.current.fetch_max(lsn, Ordering::SeqCst) < lsn {
            // Unlike `send`, this also updates the value while nobody is subscribed yet.
            self.tx.send_if_modified(|current| {
                if lsn > *current {
                    *current = lsn;
                    true
                } else {
                    false
                }
            });
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

    #[test]
    fn concurrent_marks_never_move_the_watermark_backwards() {
        let state = LsnState::new();
        let rx = state.subscribe();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observer = {
            let (state, stop) = (state.clone(), stop.clone());
            std::thread::spawn(move || {
                let (mut last_watched, mut last_now) = (0, 0);
                while !stop.load(Ordering::SeqCst) {
                    let watched = *rx.borrow();
                    let now = state.now();
                    assert!(
                        watched >= last_watched,
                        "watch went from {last_watched} to {watched}"
                    );
                    assert!(now >= last_now, "now() went from {last_now} to {now}");
                    (last_watched, last_now) = (watched, now);
                }
            })
        };
        let markers: Vec<_> = (0..8u64)
            .map(|thread| {
                let state = state.clone();
                std::thread::spawn(move || {
                    for i in 0..20_000u64 {
                        state.mark(i * 8 + thread);
                    }
                })
            })
            .collect();
        for marker in markers {
            marker.join().unwrap();
        }
        stop.store(true, Ordering::SeqCst);
        observer.join().unwrap();
        let max = 19_999 * 8 + 7;
        assert_eq!(state.now(), max);
        assert_eq!(*state.subscribe().borrow(), max);
    }
}
