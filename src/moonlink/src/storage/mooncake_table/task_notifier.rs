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

/// Handed to a background task to report its result. The task stops counting as in flight once
/// its result is in the handler's queue (or could not be delivered, or the notifier is dropped,
/// e.g. on panic): a result still waiting for queue space keeps the table from being dropped,
/// which would discard it together with the cache pins it carries.
pub(crate) struct TaskNotifier {
    tx: Sender<TableEvent>,
    in_flight: Option<Arc<AtomicUsize>>,
}

impl TaskNotifier {
    pub(crate) async fn send(mut self, event: TableEvent) {
        // The handler is gone once the table has been dropped or shut down; nobody needs the
        // result then.
        if self.tx.send(event).await.is_err() {
            tracing::debug!("table handler is gone; dropping background task result");
        }
        self.release();
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

        // A result waiting for queue space still counts as in flight.
        let (tx_full, mut rx_full) = tokio::sync::mpsc::channel(1);
        tx_full.send(TableEvent::ErroredDropTimeout).await.unwrap();
        let blocked = tasks.notifier(tx_full);
        let send = tokio::spawn(async move { blocked.send(TableEvent::ErroredDropTimeout).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(tasks.count(), 1);
        rx_full.recv().await.unwrap();
        send.await.unwrap();
        assert_eq!(tasks.count(), 0);

        // A closed handler does not make the task panic.
        let late = tasks.notifier(tx);
        drop(rx);
        late.send(TableEvent::ErroredDropTimeout).await;
        assert_eq!(tasks.count(), 0);
    }
}
