use crate::error::Result;
use crate::row::MoonlinkRow;
use crate::storage::DiskSliceWriter;
use crate::storage::MooncakeTable;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

/// Event types that can be processed by the TableHandler
#[derive(Debug)]
pub enum TableEvent {
    /// Append a row to the table
    Append {
        row: MoonlinkRow,
        xact_id: Option<u32>,
    },
    /// Delete a row from the table
    Delete {
        row: MoonlinkRow,
        lsn: u64,
        xact_id: Option<u32>,
    },
    /// Commit all pending operations with a given LSN
    Commit { lsn: u64 },
    /// Commit all pending operations with given LSN and xact_id
    StreamCommit { lsn: u64, xact_id: u32 },
    /// Abort current stream with given xact_id
    StreamAbort { xact_id: u32 },
    /// Flush the table to disk
    Flush { lsn: u64 },
    /// Shutdown the handler
    _Shutdown,
}

/// Handler for table operations
pub struct TableHandler {
    /// Handle to the event processing task
    _event_handle: Option<JoinHandle<()>>,

    /// Sender for the event queue
    event_sender: Sender<TableEvent>,
}

impl TableHandler {
    /// Create a new TableHandler for the given schema and table name
    pub fn new(table: MooncakeTable) -> Self {
        // Create channel for events
        let (event_sender, event_receiver) = mpsc::channel(100);

        // Spawn the task with the oneshot receiver
        let event_handle = Some(tokio::spawn(async move {
            Self::event_loop(event_receiver, table).await;
        }));

        // Create the handler
        Self {
            _event_handle: event_handle,
            event_sender,
        }
    }

    /// Get the event sender to send events to this handler
    pub fn get_event_sender(&self) -> Sender<TableEvent> {
        self.event_sender.clone()
    }

    /// Main event processing loop
    async fn event_loop(mut event_receiver: Receiver<TableEvent>, mut table: MooncakeTable) {
        let mut snapshot_handle: Option<JoinHandle<u64>> = None;
        let mut flush_handle: Option<JoinHandle<Result<DiskSliceWriter>>> = None;
        let mut periodic_snapshot_interval = time::interval(Duration::from_millis(500));

        // Process events until the receiver is closed or a Shutdown event is received
        loop {
            tokio::select! {
                // Process events from the queue
                Some(event) = event_receiver.recv() => {
                    match event {
                        TableEvent::Append { row, xact_id } => {
                            let result = match xact_id {
                                Some(xact_id) => table.append_in_stream_batch(row, xact_id),
                                None => table.append(row),
                            };

                            if let Err(e) = result {
                                println!("Failed to append row: {}", e);
                            }
                        }
                        TableEvent::Delete { row, lsn, xact_id } => {
                            match xact_id {
                                Some(xact_id) => table.delete_in_stream_batch(row, xact_id),
                                None => table.delete(row, lsn),
                            };
                        }
                        TableEvent::Commit { lsn } => {
                            table.commit(lsn);
                            if table.should_flush() {
                                flush_handle = Some(table.flush(lsn));
                            }
                        }

                        TableEvent::StreamCommit { lsn, xact_id } => {
                            match table.flush_transaction_stream(xact_id, lsn) {
                                Ok(writer) => {
                                    flush_handle = Some(writer);
                                }
                                Err(e) => {
                                    println!("Stream commit failed: {}", e);
                                }
                            }
                        }

                        TableEvent::StreamAbort { xact_id } => {
                            table.abort_in_stream_batch(xact_id);
                        }
                        TableEvent::Flush { lsn } => {
                            flush_handle = Some(table.flush(lsn));
                        }
                        TableEvent::_Shutdown => {
                            println!("Shutting down table handler");
                            break;
                        }
                    }
                }
                // wait for the snapshot to complete
                Some(()) = async {
                    if let Some(handle) = &mut snapshot_handle {
                        match handle.await {
                            Ok(lsn) => {
                                table.notify_snapshot_reader(lsn);
                            }
                            Err(e) => {
                                println!("Snapshot task was cancelled: {}", e);
                            }
                        }
                        Some(())
                    } else {
                        futures::future::pending::<Option<_>>().await
                    }
                } => {
                    snapshot_handle = None;
                }
                // wait for the flush to complete
                Some(writer) = async {
                    if let Some(handle) = &mut flush_handle {
                        match handle.await {
                            Ok(writer) => {
                                Some(writer)
                            }
                            Err(e) => {
                                println!("Flush task was cancelled: {}", e);
                                None
                            }
                        }
                    } else {
                        futures::future::pending::<Option<_>>().await
                    }
                } => {
                    table.commit_flush(writer.unwrap()).unwrap();
                    flush_handle = None;
                }
                // Periodic snapshot based on time
                _ = periodic_snapshot_interval.tick() => {
                    // Only create a periodic snapshot if there isn't already one in progress
                    if snapshot_handle.is_none() {
                        println!("Creating periodic snapshot");
                        snapshot_handle = table.create_snapshot();
                    }
                }
                // If all senders have been dropped, exit the loop
                else => {
                    println!("All event senders have been dropped, shutting down table handler");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::RowValue;
    use crate::storage::verify_files_and_deletions;
    use crate::union_read::ReadStateManager;
    use arrow::datatypes::{DataType, Field, Schema};

    pub async fn check_read_snapshot(
        read_manager: &ReadStateManager,
        target_lsn: u64,
        expected_ids: &[i32],
    ) {
        let read_state = read_manager.try_read(Some(target_lsn)).await.unwrap();
        if read_state.files.is_empty() {
            assert!(false, "No snapshot files returned");
        }
        verify_files_and_deletions(&read_state.files, &read_state.deletions, expected_ids);
    }

    #[tokio::test]
    async fn test_table_handler() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let table = MooncakeTable::new(schema, "test_table".to_string(), 1, path);
        let read_state_manager = ReadStateManager::new(&table);
        let handler = TableHandler::new(table);
        let event_sender = handler.get_event_sender();

        // Test append operation
        let row1 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("John".as_bytes().to_vec()),
            RowValue::Int32(30),
        ]);

        event_sender
            .send(TableEvent::Append {
                row: row1,
                xact_id: None,
            })
            .await
            .unwrap();

        // Test commit operation
        event_sender
            .send(TableEvent::Commit { lsn: 1 })
            .await
            .unwrap();

        check_read_snapshot(&read_state_manager, 1, &[1]).await;

        // Test shutdown
        event_sender.send(TableEvent::_Shutdown).await.unwrap();

        // Wait for event handler to exit
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }

        println!("All table handler tests passed!");
    }

    #[tokio::test]
    async fn test_table_handler_flush() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let table = MooncakeTable::new(schema, "test_table".to_string(), 1, path);
        let read_state_manager = ReadStateManager::new(&table);
        let handler = TableHandler::new(table);
        let event_sender = handler.get_event_sender();

        // Test append operations - add multiple rows
        let rows: Vec<MoonlinkRow> = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(1),
                RowValue::ByteArray("Alice".as_bytes().to_vec()),
                RowValue::Int32(25),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(2),
                RowValue::ByteArray("Bob".as_bytes().to_vec()),
                RowValue::Int32(30),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(3),
                RowValue::ByteArray("Charlie".as_bytes().to_vec()),
                RowValue::Int32(35),
            ]),
        ];

        // Append all rows
        for row in rows {
            event_sender
                .send(TableEvent::Append { row, xact_id: None })
                .await
                .unwrap();
        }

        // Commit the changes
        event_sender
            .send(TableEvent::Commit { lsn: 1 })
            .await
            .unwrap();

        // Wait for commit to complete
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Test flush operation
        event_sender
            .send(TableEvent::Flush { lsn: 1 })
            .await
            .unwrap();
        check_read_snapshot(&read_state_manager, 1, &[1, 2, 3]).await;

        // Test shutdown
        event_sender.send(TableEvent::_Shutdown).await.unwrap();

        // Wait for event handler to exit
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }

        println!("All table handler flush tests passed!");
    }

    #[tokio::test]
    async fn test_streaming_append_and_commit() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let table = MooncakeTable::new(schema, "test_table".to_string(), 1, path);
        let read_state_manager = ReadStateManager::new(&table);
        let handler = TableHandler::new(table);
        let event_sender = handler.get_event_sender();

        // Test transaction append with a specific xact_id
        let xact_id = 101;

        // Create rows to append in the transaction
        let row = MoonlinkRow::new(vec![
            RowValue::Int32(10),
            RowValue::ByteArray("Transaction-User".as_bytes().to_vec()),
            RowValue::Int32(25),
        ]);

        // Append row to the transaction
        event_sender
            .send(TableEvent::Append {
                row,
                xact_id: Some(xact_id),
            })
            .await
            .unwrap();

        // Commit the transaction
        event_sender
            .send(TableEvent::StreamCommit { lsn: 101, xact_id })
            .await
            .unwrap();

        check_read_snapshot(&read_state_manager, 101, &[10]).await;

        // Shutdown the handler
        event_sender.send(TableEvent::_Shutdown).await.unwrap();
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_streaming_delete() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let table = MooncakeTable::new(schema, "test_table".to_string(), 1, path);
        let read_state_manager = ReadStateManager::new(&table);
        let handler = TableHandler::new(table);
        let event_sender = handler.get_event_sender();

        let xact_id = 101;

        // Create rows to append in the transaction
        let rows_to_append = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(10),
                RowValue::ByteArray("Transaction-User1".as_bytes().to_vec()),
                RowValue::Int32(25),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(11),
                RowValue::ByteArray("Transaction-User2".as_bytes().to_vec()),
                RowValue::Int32(30),
            ]),
        ];

        // Append rows to the transaction
        for row in rows_to_append {
            event_sender
                .send(TableEvent::Append {
                    row,
                    xact_id: Some(xact_id),
                })
                .await
                .unwrap();
        }

        // Delete one of the rows within the same transaction
        let row_to_delete = MoonlinkRow::new(vec![
            RowValue::Int32(10),
            RowValue::ByteArray("Transaction-User1".as_bytes().to_vec()),
            RowValue::Int32(25),
        ]);

        event_sender
            .send(TableEvent::Delete {
                row: row_to_delete,
                lsn: 100,
                xact_id: Some(xact_id),
            })
            .await
            .unwrap();

        // Commit the transaction
        event_sender
            .send(TableEvent::StreamCommit { lsn: 101, xact_id })
            .await
            .unwrap();

        check_read_snapshot(&read_state_manager, 101, &[11]).await;
        // Shutdown the handler
        event_sender.send(TableEvent::_Shutdown).await.unwrap();
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_streaming_abort() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let table = MooncakeTable::new(schema, "test_table".to_string(), 1, path);
        let read_state_manager = ReadStateManager::new(&table);
        let handler = TableHandler::new(table);
        let event_sender = handler.get_event_sender();

        // First, add and commit a baseline row to verify against
        let baseline_xact_id = 100;
        let baseline_row = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("Baseline-User".as_bytes().to_vec()),
            RowValue::Int32(20),
        ]);

        event_sender
            .send(TableEvent::Append {
                row: baseline_row,
                xact_id: Some(baseline_xact_id),
            })
            .await
            .unwrap();

        event_sender
            .send(TableEvent::StreamCommit {
                lsn: 100,
                xact_id: baseline_xact_id,
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        // Now create a transaction that will be aborted
        let abort_xact_id = 102;

        let abort_row = MoonlinkRow::new(vec![
            RowValue::Int32(20),
            RowValue::ByteArray("Transaction-UserToAbort".as_bytes().to_vec()),
            RowValue::Int32(40),
        ]);

        // Append a row to the transaction that will be aborted
        event_sender
            .send(TableEvent::Append {
                row: abort_row,
                xact_id: Some(abort_xact_id),
            })
            .await
            .unwrap();

        // Abort the transaction
        event_sender
            .send(TableEvent::StreamAbort {
                xact_id: abort_xact_id,
            })
            .await
            .unwrap();

        check_read_snapshot(&read_state_manager, 100, &[1]).await;

        // Shutdown the handler
        event_sender.send(TableEvent::_Shutdown).await.unwrap();
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_concurrent_streaming_transactions() {
        // Create a schema for testing
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, false),
        ]);

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a TableHandler
        let table = MooncakeTable::new(schema, "test_table".to_string(), 1, path);
        let read_state_manager = ReadStateManager::new(&table);
        let handler = TableHandler::new(table);
        let event_sender = handler.get_event_sender();

        // Create two concurrent transactions
        let xact_id_1 = 103;
        let xact_id_2 = 104;

        // Transaction 1: add one row
        let tx1_row = MoonlinkRow::new(vec![
            RowValue::Int32(30),
            RowValue::ByteArray("Transaction1-User".as_bytes().to_vec()),
            RowValue::Int32(35),
        ]);

        event_sender
            .send(TableEvent::Append {
                row: tx1_row,
                xact_id: Some(xact_id_1),
            })
            .await
            .unwrap();

        // Transaction 2: add one row
        let tx2_row = MoonlinkRow::new(vec![
            RowValue::Int32(40),
            RowValue::ByteArray("Transaction2-User".as_bytes().to_vec()),
            RowValue::Int32(45),
        ]);

        event_sender
            .send(TableEvent::Append {
                row: tx2_row,
                xact_id: Some(xact_id_2),
            })
            .await
            .unwrap();

        // Commit transaction 1, abort transaction 2
        event_sender
            .send(TableEvent::StreamCommit {
                lsn: 103,
                xact_id: xact_id_1,
            })
            .await
            .unwrap();

        event_sender
            .send(TableEvent::StreamAbort { xact_id: xact_id_2 })
            .await
            .unwrap();

        check_read_snapshot(&read_state_manager, 103, &[30]).await;
        // Shutdown the handler
        event_sender.send(TableEvent::_Shutdown).await.unwrap();
        if let Some(handle) = handler._event_handle {
            handle.await.unwrap();
        }
    }
}
