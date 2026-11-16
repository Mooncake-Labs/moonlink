//! Failure injection for tests of the crates built on moonlink, which cannot reach its internals.
use std::sync::atomic::{AtomicUsize, Ordering};

static PERSISTENCE_FAILURES_TO_INJECT: AtomicUsize = AtomicUsize::new(0);

/// Make the next `count` iceberg persistences in this process fail with a transient error, before
/// anything is written, as a storage outage would.
pub fn inject_persistence_failures(count: usize) {
    PERSISTENCE_FAILURES_TO_INJECT.store(count, Ordering::SeqCst);
}

/// Injected persistence failures not used yet.
pub fn injected_persistence_failures_left() -> usize {
    PERSISTENCE_FAILURES_TO_INJECT.load(Ordering::SeqCst)
}

/// Use up one injected persistence failure, if any is left.
pub(crate) fn take_injected_persistence_failure() -> bool {
    PERSISTENCE_FAILURES_TO_INJECT
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
            left.checked_sub(1)
        })
        .is_ok()
}
