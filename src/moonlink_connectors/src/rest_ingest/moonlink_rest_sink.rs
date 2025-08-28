use crate::replication_state::ReplicationState;
use crate::rest_ingest::event_request::RowEventOperation;
use crate::rest_ingest::rest_event::RestEvent;
use crate::rest_ingest::rest_source::SrcTableId;
use crate::{Error, Result};
use moonlink::TableEvent;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tracing::debug;

pub struct TableStatus {
    pub(crate) _wal_flush_lsn_rx: watch::Receiver<u64>,
    pub(crate) _flush_lsn_rx: watch::Receiver<u64>,
    pub(crate) event_sender: mpsc::Sender<TableEvent>,
    pub(crate) commit_lsn_tx: watch::Sender<u64>,
}

/// REST-specific sink for handling REST API table events
pub struct RestSink {
    table_status: HashMap<SrcTableId, TableStatus>,
    tables_in_progress: Option<SrcTableId>,
    replication_state: Arc<ReplicationState>,
}

impl RestSink {
    pub fn new(replication_state: Arc<ReplicationState>) -> Self {
        Self {
            table_status: HashMap::new(),
            tables_in_progress: None,
            replication_state,
        }
    }

    /// Add a table to the REST sink
    ///
    /// # Arguments
    ///
    /// * persist_lsn: only assigned at recovery, used to indicate and update commit LSN and replication LSN.
    pub fn add_table(
        &mut self,
        src_table_id: SrcTableId,
        table_status: TableStatus,
        persist_lsn: Option<u64>,
    ) -> Result<()> {
        // Update per-table commit LSN.
        if let Some(persist_lsn) = persist_lsn {
            table_status.commit_lsn_tx.send(persist_lsn).unwrap();
        }

        if self
            .table_status
            .insert(src_table_id, table_status)
            .is_some()
        {
            return Err(Error::rest_duplicate_table(src_table_id));
        }

        // Update per-database replication LSN.
        if let Some(persist_lsn) = persist_lsn {
            self.replication_state.mark(persist_lsn);
        }

        Ok(())
    }

    /// Remove a table from the REST sink
    pub fn drop_table(&mut self, src_table_id: SrcTableId) -> Result<()> {
        if self.table_status.remove(&src_table_id).is_none() {
            return Err(Error::rest_non_existent_table(src_table_id));
        }
        Ok(())
    }

    /// Update commit LSN and replication LSN for the given table.
    ///
    /// Difference on commit LSN and replication LSN:
    /// - Commit LSN is used per-table
    /// - Replication LSN is used per-database
    fn mark_commit(&self, src_table_id: SrcTableId, lsn: u64) -> Result<()> {
        if let Some(table_status) = self.table_status.get(&src_table_id) {
            table_status.commit_lsn_tx.send(lsn).unwrap();
        } else {
            return Err(crate::Error::rest_api(
                format!("No table status found for src_table_id: {src_table_id}"),
                None,
            ));
        }
        self.replication_state.mark(lsn);
        Ok(())
    }

    /// Process a REST event and send appropriate table events
    /// This is the main entry point for REST event processing, similar to moonlink_sink's process_cdc_event
    pub async fn process_rest_event(&mut self, rest_event: RestEvent) -> Result<()> {
        match rest_event {
            // ==================
            // Row events
            // ==================
            //
            RestEvent::RowEvent {
                src_table_id,
                operation,
                row,
                lsn,
                timestamp: _,
            } => {
                self.tables_in_progress = Some(src_table_id);
                self.process_row_event(src_table_id, operation, row, lsn)
                    .await?;
                Ok(())
            }
            RestEvent::Commit { lsn, timestamp } => {
                let src_table_id = self
                    .tables_in_progress
                    .take()
                    .expect("tables_in_progress not set");
                self.process_commit_event(lsn, src_table_id, timestamp)
                    .await?;
                self.mark_commit(src_table_id, lsn)?;
                Ok(())
            }
            // ==================
            // Table events
            // ==================
            //
            RestEvent::FileInsertEvent {
                src_table_id: _,
                table_events,
            } => {
                self.process_file_insertion_boxed(table_events).await?;
                Ok(())
            }
            RestEvent::FileUploadEvent {
                src_table_id,
                files,
                lsn,
            } => {
                self.process_file_upload(src_table_id, files, lsn).await?;
                self.mark_commit(src_table_id, lsn)?;
                Ok(())
            }
        }
    }

    /// Process a file event (upload files).
    fn process_file_insertion_boxed<'a>(
        &'a mut self,
        table_events: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<Result<RestEvent>>>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.process_file_insertion_impl(table_events).await })
    }
    async fn process_file_insertion_impl(
        &mut self,
        table_events: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<Result<RestEvent>>>>,
    ) -> Result<()> {
        let mut guard = table_events.lock().await;
        while let Some(event) = guard.recv().await {
            self.process_rest_event(event?).await?;
        }
        Ok(())
    }

    /// Process file upload event.
    async fn process_file_upload(
        &self,
        src_table_id: SrcTableId,
        files: Vec<String>,
        lsn: u64,
    ) -> Result<()> {
        let table_event = TableEvent::LoadFiles { files, lsn };
        self.send_table_event(src_table_id, table_event).await?;
        Ok(())
    }

    /// Process a row event (Insert, Update, Delete).
    async fn process_row_event(
        &self,
        src_table_id: SrcTableId,
        operation: RowEventOperation,
        row: moonlink::row::MoonlinkRow,
        lsn: u64,
    ) -> Result<()> {
        match operation {
            RowEventOperation::Insert => {
                let table_event = TableEvent::Append {
                    row,
                    lsn,
                    xact_id: None,
                    is_copied: false,
                    is_recovery: false,
                };

                self.send_table_event(src_table_id, table_event).await?;
                debug!(src_table_id, lsn, "processed REST insert event");
            }
            RowEventOperation::Update => {
                // For updates, we send both delete and append events
                // First send delete for the old row (using the same row for simplicity)
                let delete_event = TableEvent::Delete {
                    row: row.clone(),
                    lsn,
                    xact_id: None,
                    is_recovery: false,
                };

                self.send_table_event(src_table_id, delete_event).await?;
                debug!(src_table_id, lsn, "processed REST update delete event");

                // Then send append for the new row
                let append_event = TableEvent::Append {
                    row,
                    lsn,
                    xact_id: None,
                    is_copied: false,
                    is_recovery: false,
                };

                self.send_table_event(src_table_id, append_event).await?;
                debug!(src_table_id, lsn, "processed REST update append event");
            }
            RowEventOperation::Delete => {
                let table_event = TableEvent::Delete {
                    row,
                    lsn,
                    xact_id: None,
                    is_recovery: false,
                };

                self.send_table_event(src_table_id, table_event).await?;
                debug!(src_table_id, lsn, "processed REST delete event");
            }
        }
        Ok(())
    }

    /// Process a commit event
    async fn process_commit_event(
        &self,
        lsn: u64,
        src_table_id: SrcTableId,
        timestamp: std::time::SystemTime,
    ) -> Result<()> {
        debug!(
            "REST API commit event: LSN={}, timestamp={:?}",
            lsn, timestamp
        );
        let commit_event = TableEvent::Commit {
            lsn,
            xact_id: None,
            is_recovery: false,
        };
        self.send_table_event(src_table_id, commit_event).await?;
        Ok(())
    }

    /// Send a table event to the appropriate table handler (internal helper)
    async fn send_table_event(&self, src_table_id: SrcTableId, event: TableEvent) -> Result<()> {
        if let Some(table_status) = self.table_status.get(&src_table_id) {
            table_status.event_sender.send(event).await.map_err(|e| {
                crate::Error::rest_api(
                    format!("Failed to send event to table {src_table_id}: {e}"),
                    None,
                )
            })?;
            Ok(())
        } else {
            Err(crate::Error::rest_api(
                format!("No event sender found for src_table_id: {src_table_id}"),
                None,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moonlink::row::{MoonlinkRow, RowValue};
    use std::time::SystemTime;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_rest_sink_basic_operations() {
        let replication_state = ReplicationState::new();
        let _replication_state_rx = replication_state.subscribe();
        let mut sink = RestSink::new(replication_state.clone());

        // Create channels for testing
        let (event_tx, mut event_rx) = mpsc::channel::<TableEvent>(10);
        let (_commit_lsn_tx, _commit_lsn_rx) = watch::channel(0u64);
        let (_wal_flush_lsn_tx, _wal_flush_lsn_rx) = watch::channel(0u64);
        let (_flush_lsn_tx, _flush_lsn_rx) = watch::channel(0u64);
        let table_status = TableStatus {
            _wal_flush_lsn_rx,
            _flush_lsn_rx,
            event_sender: event_tx,
            commit_lsn_tx: _commit_lsn_tx,
        };

        // Add table to sink
        let src_table_id = 1;
        sink.add_table(src_table_id, table_status, /*persist_lsn=*/ None)
            .unwrap();

        // Create a test event
        let test_row = MoonlinkRow::new(vec![
            RowValue::Int32(42),
            RowValue::ByteArray(b"test".to_vec()),
        ]);

        let table_event = TableEvent::Append {
            row: test_row.clone(),
            lsn: 1,
            xact_id: None,
            is_copied: false,
            is_recovery: false,
        };

        // Send event through sink
        sink.send_table_event(src_table_id, table_event)
            .await
            .unwrap();

        // Verify event was received
        let received_event = event_rx.recv().await.unwrap();
        match received_event {
            TableEvent::Append {
                row,
                lsn,
                xact_id,
                is_copied,
                is_recovery,
            } => {
                assert_eq!(row.values, test_row.values);
                assert_eq!(lsn, 1);
                assert_eq!(xact_id, None);
                assert!(!is_copied);
                assert!(!is_recovery);
            }
            _ => panic!("Expected Append event"),
        }

        // Test sending to non-existent table
        let result = sink
            .send_table_event(
                999,
                TableEvent::Append {
                    row: test_row,
                    lsn: 2,
                    xact_id: None,
                    is_copied: false,
                    is_recovery: false,
                },
            )
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No event sender found"));

        // Test drop table
        sink.drop_table(src_table_id).unwrap();

        // Verify table was dropped by trying to send another event
        let result = sink
            .send_table_event(
                src_table_id,
                TableEvent::Append {
                    row: MoonlinkRow::new(vec![RowValue::Int32(99)]),
                    lsn: 3,
                    xact_id: None,
                    is_copied: false,
                    is_recovery: false,
                },
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rest_sink_process_rest_event() {
        let replication_state = ReplicationState::new();
        let _replication_state_rx = replication_state.subscribe();
        let mut sink = RestSink::new(replication_state.clone());

        // Create channels for testing
        let (event_tx, mut event_rx) = mpsc::channel::<TableEvent>(10);
        let (_commit_lsn_tx, _commit_lsn_rx) = watch::channel(0u64);
        let (_wal_flush_lsn_tx, _wal_flush_lsn_rx) = watch::channel(0u64);
        let (_flush_lsn_tx, _flush_lsn_rx) = watch::channel(0u64);
        let table_status = TableStatus {
            _wal_flush_lsn_rx,
            _flush_lsn_rx,
            event_sender: event_tx,
            commit_lsn_tx: _commit_lsn_tx,
        };

        let src_table_id = 1;
        sink.add_table(src_table_id, table_status, /*persist_lsn=*/ None)
            .unwrap();

        let test_row = MoonlinkRow::new(vec![RowValue::Int32(42)]);

        // Test Insert event
        let insert_event = RestEvent::RowEvent {
            src_table_id,
            operation: RowEventOperation::Insert,
            row: test_row.clone(),
            lsn: 10,
            timestamp: SystemTime::now(),
        };

        sink.process_rest_event(insert_event).await.unwrap();

        // Verify insert was processed
        let received = event_rx.recv().await.unwrap();
        match received {
            TableEvent::Append { lsn, .. } => assert_eq!(lsn, 10),
            _ => panic!("Expected Append event"),
        }

        // Test Update event (should produce both Delete and Append)
        let update_event = RestEvent::RowEvent {
            src_table_id,
            operation: RowEventOperation::Update,
            row: test_row.clone(),
            lsn: 20,
            timestamp: SystemTime::now(),
        };

        sink.process_rest_event(update_event).await.unwrap();

        // Verify delete was processed
        let delete_received = event_rx.recv().await.unwrap();
        match delete_received {
            TableEvent::Delete { lsn, .. } => assert_eq!(lsn, 20),
            _ => panic!("Expected Delete event"),
        }

        // Verify append was processed
        let append_received = event_rx.recv().await.unwrap();
        match append_received {
            TableEvent::Append { lsn, .. } => assert_eq!(lsn, 20),
            _ => panic!("Expected Append event"),
        }

        // Test Commit event
        let commit_event = RestEvent::Commit {
            lsn: 30,
            timestamp: SystemTime::now(),
        };

        sink.process_rest_event(commit_event).await.unwrap();
        // Commit event doesn't produce table events, just sends LSN to channels
    }

    #[tokio::test]
    async fn test_rest_sink_operations() {
        let replication_state = ReplicationState::new();
        let _replication_state_rx = replication_state.subscribe();
        let mut sink = RestSink::new(replication_state.clone());

        // Create channels for testing
        let (event_tx_1, mut event_rx_1) = mpsc::channel::<TableEvent>(10);
        let (_commit_lsn_tx_1, _commit_lsn_rx_1) = watch::channel(0u64);
        let (_wal_flush_lsn_tx_1, _wal_flush_lsn_rx_1) = watch::channel(0u64);
        let (_flush_lsn_tx_1, _flush_lsn_rx_1) = watch::channel(0u64);
        let table_status_1 = TableStatus {
            _wal_flush_lsn_rx: _wal_flush_lsn_rx_1,
            _flush_lsn_rx: _flush_lsn_rx_1,
            event_sender: event_tx_1,
            commit_lsn_tx: _commit_lsn_tx_1,
        };

        let (event_tx_2, mut event_rx_2) = mpsc::channel::<TableEvent>(10);
        let (_commit_lsn_tx_2, _commit_lsn_rx_2) = watch::channel(0u64);
        let (_wal_flush_lsn_tx_2, _wal_flush_lsn_rx_2) = watch::channel(0u64);
        let (_flush_lsn_tx_2, _flush_lsn_rx_2) = watch::channel(0u64);
        let table_status_2 = TableStatus {
            _wal_flush_lsn_rx: _wal_flush_lsn_rx_2,
            _flush_lsn_rx: _flush_lsn_rx_2,
            event_sender: event_tx_2,
            commit_lsn_tx: _commit_lsn_tx_2,
        };

        // Add two tables
        sink.add_table(1, table_status_1, /*persist_lsn=*/ None)
            .unwrap();
        sink.add_table(2, table_status_2, /*persist_lsn=*/ None)
            .unwrap();

        // Test different operation types
        let test_row = MoonlinkRow::new(vec![RowValue::Int32(1)]);

        // Test Insert (Append)
        let insert_event = TableEvent::Append {
            row: test_row.clone(),
            lsn: 10,
            xact_id: None,
            is_copied: false,
            is_recovery: false,
        };
        sink.send_table_event(1, insert_event).await.unwrap();

        // Test Delete
        let delete_event = TableEvent::Delete {
            row: test_row.clone(),
            lsn: 11,
            xact_id: None,
            is_recovery: false,
        };
        sink.send_table_event(2, delete_event).await.unwrap();

        // Verify events were received correctly
        let received1 = event_rx_1.recv().await.unwrap();
        match received1 {
            TableEvent::Append { lsn, .. } => assert_eq!(lsn, 10),
            _ => panic!("Expected Append event for table 1"),
        }

        let received2 = event_rx_2.recv().await.unwrap();
        match received2 {
            TableEvent::Delete { lsn, .. } => assert_eq!(lsn, 11),
            _ => panic!("Expected Delete event for table 2"),
        }
    }
}
