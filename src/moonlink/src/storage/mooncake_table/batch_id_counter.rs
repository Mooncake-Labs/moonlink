use more_asserts as ma;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub const STREAMING_BATCH_ID_MAX: u64 = 1u64 << 63;

/// Batch ID counter for the two-counter allocation strategy.
///
/// The system uses two separate atomic counters to partition the 64-bit batch ID space:
/// - **Streaming Counter**: Range 0 -> 2^63-1, used for streaming transactions
/// - **Non-Streaming Counter**: Range 2^63+, used for regular operations
///
/// We give streaming batches the smaller range so that they are always behind the commit point, which points to the most recently added batch of the non-streaming batches.
/// This ensures batch IDs are always monotonically increasing and unique across all transactions.
pub(super) struct BatchIdCounter {
    counter: Arc<AtomicU64>,
    is_streaming: bool,
}

impl BatchIdCounter {
    pub fn new(is_streaming: bool) -> Self {
        Self {
            counter: Arc::new(AtomicU64::new(if is_streaming {
                0
            } else {
                STREAMING_BATCH_ID_MAX
            })),
            is_streaming,
        }
    }

    // Relaxed ordering is used here because the counter is only used for internal state tracking, not for synchronization.
    pub fn load(&self) -> u64 {
        self.counter.load(Ordering::Relaxed)
    }

    // Increment the id by 1, and return the id before change.
    //
    // Relaxed ordering is used here because the counter is only used for internal state tracking, not for synchronization.
    pub fn get_and_next(&self) -> u64 {
        let current = self.counter.load(Ordering::Relaxed);

        // Check limits before incrementing
        if self.is_streaming {
            ma::assert_lt!(
                current,
                STREAMING_BATCH_ID_MAX,
                "Streaming batch ID counter overflow: exceeded 2^63-1"
            );
        } else {
            ma::assert_lt!(current, u64::MAX, "Non-streaming batch ID counter overflow");
        }

        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_streaming_counter_creation() {
        let counter = BatchIdCounter::new(true);
        assert_eq!(counter.load(), 0);
        assert!(counter.is_streaming);
    }

    #[test]
    fn test_non_streaming_counter_creation() {
        let counter = BatchIdCounter::new(false);
        assert_eq!(counter.load(), STREAMING_BATCH_ID_MAX);
        assert!(!counter.is_streaming);
    }

    #[test]
    fn test_streaming_counter_next() {
        let counter = BatchIdCounter::new(true);

        // First call should return 0, then increment to 1
        assert_eq!(counter.get_and_next(), 0);
        assert_eq!(counter.load(), 1);

        // Second call should return 1, then increment to 2
        assert_eq!(counter.get_and_next(), 1);
        assert_eq!(counter.load(), 2);
    }

    #[test]
    fn test_non_streaming_counter_next() {
        let counter = BatchIdCounter::new(false);
        let expected_start = STREAMING_BATCH_ID_MAX;

        // First call should return 2^63, then increment to 2^63 + 1
        assert_eq!(counter.get_and_next(), expected_start);
        assert_eq!(counter.load(), expected_start + 1);

        // Second call should return 2^63 + 1, then increment to 2^63 + 2
        assert_eq!(counter.get_and_next(), expected_start + 1);
        assert_eq!(counter.load(), expected_start + 2);
    }

    #[test]
    #[should_panic(expected = "Streaming batch ID counter overflow: exceeded 2^63-1")]
    fn test_streaming_counter_overflow() {
        let counter = BatchIdCounter::new(true);

        // Manually set counter to the limit
        let limit = STREAMING_BATCH_ID_MAX;
        counter.counter.store(limit, Ordering::Relaxed);

        // This should panic
        counter.get_and_next();
    }

    #[test]
    #[should_panic(expected = "Non-streaming batch ID counter overflow")]
    fn test_non_streaming_counter_overflow() {
        let counter = BatchIdCounter::new(false);

        // Manually set counter to u64::MAX
        counter.counter.store(u64::MAX, Ordering::Relaxed);

        // This should panic
        counter.get_and_next();
    }

    #[test]
    fn test_streaming_counter_near_limit() {
        let counter = BatchIdCounter::new(true);
        let near_limit = STREAMING_BATCH_ID_MAX - 2;

        // Set counter near the limit
        counter.counter.store(near_limit, Ordering::Relaxed);

        // These should work
        assert_eq!(counter.get_and_next(), near_limit);
        assert_eq!(counter.get_and_next(), near_limit + 1);

        // The next call should panic - test this separately to ensure it panics
    }

    #[test]
    fn test_concurrent_access() {
        let counter = Arc::new(BatchIdCounter::new(true));
        let num_threads = 10;
        let increments_per_thread = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let counter_clone = Arc::clone(&counter);
                thread::spawn(move || {
                    let mut ids = Vec::new();
                    for _ in 0..increments_per_thread {
                        ids.push(counter_clone.get_and_next());
                    }
                    ids
                })
            })
            .collect();

        // Collect all IDs from all threads
        let mut all_ids = Vec::new();
        for handle in handles {
            all_ids.extend(handle.join().unwrap());
        }

        // All IDs should be unique
        all_ids.sort_unstable();
        let mut unique_ids = all_ids.clone();
        unique_ids.dedup();

        assert_eq!(all_ids.len(), unique_ids.len(), "All IDs should be unique");
        assert_eq!(all_ids.len(), num_threads * increments_per_thread);

        // All IDs should be in streaming range
        for id in &all_ids {
            assert!(
                *id < STREAMING_BATCH_ID_MAX,
                "ID {id} should be in streaming range"
            );
        }

        // IDs should be consecutive starting from 0
        for (i, &id) in all_ids.iter().enumerate() {
            assert_eq!(id, i as u64, "IDs should be consecutive");
        }
    }
}
