use crate::rest_ingest::rest_source::SrcTableId;
use crate::rest_ingest::rest_source::{EventOperation, RestEvent};
use crate::Result;
use moonlink::TableEvent;
use std::collections::HashMap;
use tokio::sync::{mpsc, watch};
use tracing::{debug, warn};

/// REST-specific sink for handling REST API table events
pub struct RestSink {
    event_senders: HashMap<SrcTableId, mpsc::Sender<TableEvent>>,
    commit_lsn_txs: HashMap<SrcTableId, watch::Sender<u64>>,
    tables_in_progress: Option<SrcTableId>,
}

impl Default for RestSink {
    fn default() -> Self {
        Self::new()
    }
}

impl RestSink {
    pub fn new() -> Self {
        Self {
            event_senders: HashMap::new(),
            commit_lsn_txs: HashMap::new(),
            tables_in_progress: None,
        }
    }

    /// Add a table to the REST sink
    pub fn add_table(
        &mut self,
        src_table_id: SrcTableId,
        event_sender: mpsc::Sender<TableEvent>,
        commit_lsn_tx: watch::Sender<u64>,
    ) {
        self.event_senders.insert(src_table_id, event_sender);
        self.commit_lsn_txs.insert(src_table_id, commit_lsn_tx);
    }

    /// Remove a table from the REST sink
    pub fn drop_table(&mut self, src_table_id: SrcTableId) {
        self.event_senders.remove(&src_table_id);
        self.commit_lsn_txs.remove(&src_table_id);
    }

    /// Process a REST event and send appropriate table events
    /// This is the main entry point for REST event processing, similar to moonlink_sink's process_cdc_event
    pub async fn process_rest_event(&mut self, rest_event: RestEvent) -> Result<()> {
        match rest_event {
            RestEvent::RowEvent {
                src_table_id,
                operation,
                row,
                lsn,
                timestamp: _,
            } => {
                self.tables_in_progress = Some(src_table_id);
                self.process_row_event(src_table_id, operation, row, lsn)
                    .await
            }
            RestEvent::Commit { lsn, timestamp } => {
                let src_table_id = self
                    .tables_in_progress
                    .take()
                    .expect("tables_in_progress not set");
                self.process_commit_event(lsn, src_table_id, timestamp)
                    .await
            }
        }
    }

    /// Process a row event (Insert, Update, Delete)
    async fn process_row_event(
        &self,
        src_table_id: SrcTableId,
        operation: EventOperation,
        row: moonlink::row::MoonlinkRow,
        lsn: u64,
    ) -> Result<()> {
        match operation {
            EventOperation::Insert => {
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
            EventOperation::Update => {
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
            EventOperation::Delete => {
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
        self.send_commit_lsn(lsn);
        Ok(())
    }

    /// Send a table event to the appropriate table handler (internal helper)
    async fn send_table_event(&self, src_table_id: SrcTableId, event: TableEvent) -> Result<()> {
        if let Some(event_sender) = self.event_senders.get(&src_table_id) {
            event_sender.send(event).await.map_err(|e| {
                crate::Error::RestApi(format!("Failed to send event to table {src_table_id}: {e}"))
            })?;
            Ok(())
        } else {
            Err(crate::Error::RestApi(format!(
                "No event sender found for src_table_id: {src_table_id}"
            )))
        }
    }

    /// Send commit LSN to all active tables
    fn send_commit_lsn(&self, lsn: u64) {
        for (src_table_id, commit_lsn_tx) in &self.commit_lsn_txs {
            if let Err(e) = commit_lsn_tx.send(lsn) {
                warn!(error = ?e, src_table_id, lsn, "failed to send commit LSN");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rest_ingest::rest_source::{EventOperation, RestEvent};
    use moonlink::row::{MoonlinkRow, RowValue};
    use std::time::SystemTime;
    use tokio::sync::{mpsc, watch};

    #[tokio::test]
    async fn test_rest_sink_basic_operations() {
        let mut sink = RestSink::new();

        // Create channels for testing
        let (event_tx, mut event_rx) = mpsc::channel::<TableEvent>(10);
        let (commit_lsn_tx, _commit_lsn_rx) = watch::channel(0u64);

        let src_table_id = 1;

        // Add table to sink
        sink.add_table(src_table_id, event_tx, commit_lsn_tx);

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
        sink.drop_table(src_table_id);

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

    #[test]
    fn test_rest_sink_commit_lsn() {
        let sink = RestSink::new();

        // Test sending commit LSN to empty sink (should not panic)
        sink.send_commit_lsn(100);

        // TODO: Could extend this test to verify commit LSN is actually sent
        // to tables, but that would require more complex test setup
    }

    #[tokio::test]
    async fn test_rest_sink_process_rest_event() {
        let mut sink = RestSink::new();

        // Create channels for testing
        let (event_tx, mut event_rx) = mpsc::channel::<TableEvent>(10);
        let (commit_lsn_tx, _commit_lsn_rx) = watch::channel(0u64);

        let src_table_id = 1;
        sink.add_table(src_table_id, event_tx, commit_lsn_tx);

        let test_row = MoonlinkRow::new(vec![RowValue::Int32(42)]);

        // Test Insert event
        let insert_event = RestEvent::RowEvent {
            src_table_id,
            operation: EventOperation::Insert,
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
            operation: EventOperation::Update,
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
        let mut sink = RestSink::new();

        // Create channels for testing
        let (event_tx1, mut event_rx1) = mpsc::channel::<TableEvent>(10);
        let (commit_lsn_tx1, _commit_lsn_rx1) = watch::channel(0u64);
        let (event_tx2, mut event_rx2) = mpsc::channel::<TableEvent>(10);
        let (commit_lsn_tx2, _commit_lsn_rx2) = watch::channel(0u64);

        // Add two tables
        sink.add_table(1, event_tx1, commit_lsn_tx1);
        sink.add_table(2, event_tx2, commit_lsn_tx2);

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
        let received1 = event_rx1.recv().await.unwrap();
        match received1 {
            TableEvent::Append { lsn, .. } => assert_eq!(lsn, 10),
            _ => panic!("Expected Append event for table 1"),
        }

        let received2 = event_rx2.recv().await.unwrap();
        match received2 {
            TableEvent::Delete { lsn, .. } => assert_eq!(lsn, 11),
            _ => panic!("Expected Delete event for table 2"),
        }
    }
}
