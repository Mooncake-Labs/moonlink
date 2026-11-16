//! Completion channel of the background tasks of one mooncake table.
use crate::table_notify::TableEvent;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

/// Counts the background tasks of one table that may still read or write its files.
#[derive(Clone, Debug, Default)]
pub(crate) struct InFlightTasks(Arc<AtomicUsize>);

impl InFlightTasks {
    pub(crate) fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }

    /// Register a new background task, which reports its result through the returned notifier.
    pub(crate) fn notifier(&self, tx: Sender<TableEvent>) -> TaskNotifier {
        self.0.fetch_add(1, Ordering::SeqCst);
        TaskNotifier {
            tx,
            in_flight: Some(self.0.clone()),
        }
    }
}

/// Handed to a background task to report its result. The task stops counting as in flight
/// right before its result is sent (or when the notifier is dropped, e.g. on panic), so the
/// table handler sees an up-to-date count whenever it processes a result.
pub(crate) struct TaskNotifier {
    tx: Sender<TableEvent>,
    in_flight: Option<Arc<AtomicUsize>>,
}

impl TaskNotifier {
    pub(crate) async fn send(mut self, event: TableEvent) {
        self.release();
        // The handler is gone once the table has been dropped or shut down; nobody needs the
        // result then.
        if self.tx.send(event).await.is_err() {
            tracing::debug!("table handler is gone; dropping background task result");
        }
    }

    fn release(&mut self) {
        if let Some(in_flight) = self.in_flight.take() {
            in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl Drop for TaskNotifier {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_task_counts_until_it_reports_or_is_dropped() {
        let tasks = InFlightTasks::default();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);

        let reported = tasks.notifier(tx.clone());
        let dropped = tasks.notifier(tx.clone());
        assert_eq!(tasks.count(), 2);

        reported.send(TableEvent::ErroredDropTimeout).await;
        assert_eq!(tasks.count(), 1);
        assert!(rx.recv().await.is_some());

        drop(dropped);
        assert_eq!(tasks.count(), 0);

        // A closed handler does not make the task panic.
        let late = tasks.notifier(tx);
        drop(rx);
        late.send(TableEvent::ErroredDropTimeout).await;
        assert_eq!(tasks.count(), 0);
    }
}
