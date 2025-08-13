use std::sync::atomic::{AtomicU64, Ordering};

/// Global, process-wide counter.
static NEXT_EVENT_ID: AtomicU64 = AtomicU64::new(1);

/// Get the next unique, monotonically increasing ID.
#[inline]
pub fn get_next_event_id() -> u64 {
    // Returns the previous value; since we start at 1, returned IDs are 1,2,3,...
    NEXT_EVENT_ID.fetch_add(1, Ordering::Relaxed)
}
