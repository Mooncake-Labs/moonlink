//! Failure injection for tests of the crates built on moonlink, which cannot reach its internals.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// How an injected iceberg persistence failure fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InjectedPersistenceFailure {
    /// A transient storage error, retried with backoff.
    Transient,
    /// The disk is full (ENOSPC).
    StorageFull,
}

static PERSISTENCE_FAILURES_TO_INJECT: AtomicUsize = AtomicUsize::new(0);
static INJECT_STORAGE_FULL: AtomicBool = AtomicBool::new(false);
static PERSISTENCE_WAIT_RETRIES: AtomicUsize = AtomicUsize::new(0);

/// Make the next `count` iceberg persistences in this process fail before anything is written,
/// as a storage outage would.
pub fn inject_persistence_failures(count: usize, kind: InjectedPersistenceFailure) {
    INJECT_STORAGE_FULL.store(
        kind == InjectedPersistenceFailure::StorageFull,
        Ordering::SeqCst,
    );
    PERSISTENCE_FAILURES_TO_INJECT.store(count, Ordering::SeqCst);
}

/// Injected persistence failures not used yet.
pub fn injected_persistence_failures_left() -> usize {
    PERSISTENCE_FAILURES_TO_INJECT.load(Ordering::SeqCst)
}

/// Count a persistence failure that a caller waiting for persistence waited out.
pub fn record_persistence_wait_retry() {
    PERSISTENCE_WAIT_RETRIES.fetch_add(1, Ordering::SeqCst);
}

/// Persistence failures waited out so far in this process.
pub fn persistence_wait_retries() -> usize {
    PERSISTENCE_WAIT_RETRIES.load(Ordering::SeqCst)
}

/// Use up one injected persistence failure, if any is left, and return its error.
pub(crate) fn take_injected_persistence_failure() -> Option<crate::Error> {
    PERSISTENCE_FAILURES_TO_INJECT
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
            left.checked_sub(1)
        })
        .ok()?;
    Some(if INJECT_STORAGE_FULL.load(Ordering::SeqCst) {
        crate::Error::from(std::io::Error::from_raw_os_error(28))
    } else {
        crate::Error::from(iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            "injected transient persistence failure",
        ))
    })
}
