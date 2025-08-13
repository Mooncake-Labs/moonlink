use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
/// Class which assigns monotonically increasing id for background events.
pub(crate) struct EventIdAssigner {
    counter: Arc<AtomicU64>,
}

impl EventIdAssigner {
    pub(crate) fn new() -> Self {
        Self {
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn get_next_event_id(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }
}
