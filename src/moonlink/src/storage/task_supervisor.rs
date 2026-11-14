//! Background tasks of a table report a panic back to the table handler instead of dying
//! silently, so the table can be put into the errored state.
use crate::table_notify::TableEvent;
use futures::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use tokio::sync::mpsc::Sender;

/// Render a panic payload for logs and the errored-table reason.
pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Spawn a table background task; if it panics, send [`TableEvent::BackgroundTaskFailed`].
pub(crate) fn spawn_supervised<F>(task: &'static str, table_notify: Sender<TableEvent>, fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::task::spawn(async move {
        if let Err(payload) = AssertUnwindSafe(fut).catch_unwind().await {
            let message = panic_message(payload.as_ref());
            tracing::error!(task, message, "table background task panicked");
            let _ = table_notify
                .send(TableEvent::BackgroundTaskFailed { task, message })
                .await;
        }
    });
}
