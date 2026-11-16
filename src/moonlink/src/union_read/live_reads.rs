//! Tracks which mooncake snapshot sequences live read states were built from, so garbage
//! collection never deletes a data file an open scan may still read.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub(crate) struct LiveReadSeqs {
    /// Snapshot sequence -> number of live read states built from it.
    counts: Mutex<BTreeMap<u64, usize>>,
}

impl LiveReadSeqs {
    /// Register a read state built from `seq`; it stays live until the guard is dropped.
    pub(crate) fn register(self: &Arc<Self>, seq: u64) -> LiveReadGuard {
        *self.counts.lock().unwrap().entry(seq).or_default() += 1;
        LiveReadGuard {
            seqs: self.clone(),
            seq,
        }
    }

    /// Oldest snapshot sequence still in use, or None if no read state is live.
    pub(crate) fn min(&self) -> Option<u64> {
        self.counts.lock().unwrap().keys().next().copied()
    }
}

#[derive(Debug)]
pub(crate) struct LiveReadGuard {
    seqs: Arc<LiveReadSeqs>,
    seq: u64,
}

impl Drop for LiveReadGuard {
    fn drop(&mut self) {
        let mut counts = self.seqs.counts.lock().unwrap();
        if let Some(count) = counts.get_mut(&self.seq) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.seq);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_tracks_live_guards() {
        let seqs = Arc::new(LiveReadSeqs::default());
        assert_eq!(seqs.min(), None);
        let a = seqs.register(3);
        let b = seqs.register(5);
        let c = seqs.register(3);
        assert_eq!(seqs.min(), Some(3));
        drop(a);
        assert_eq!(seqs.min(), Some(3));
        drop(c);
        assert_eq!(seqs.min(), Some(5));
        drop(b);
        assert_eq!(seqs.min(), None);
    }
}
