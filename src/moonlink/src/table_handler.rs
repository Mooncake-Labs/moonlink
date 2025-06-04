use crate::row::MoonlinkRow;
use crate::storage::mooncake_table::SnapshotOption;
use crate::storage::MooncakeTable;
use crate::table_notify::TableNotify;
use crate::{Error, Result};
use more_asserts as ma;
use std::collections::BTreeMap;
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
    /// Commit all pending operations with a given LSN and xact_id
    Commit { lsn: u64, xact_id: Option<u32> },
    /// Abort current stream with given xact_id
    StreamAbort { xact_id: u32 },
    /// Flush the table to disk
    Flush { lsn: u64 },
    /// Flush the transaction stream with given xact_id
    StreamFlush { xact_id: u32 },
    /// Shutdown the handler
    _Shutdown,
    /// Force a mooncake and iceberg snapshot.
    ForceSnapshot { lsn: u64, tx: Sender<Result<()>> },
    /// Drop table.
    DropIcebergTable,
}

/// Handler for table operations
pub struct TableHandler {
    /// Handle to the event processing task
    _event_handle: Option<JoinHandle<()>>,

    /// Sender for the event queue
    event_sender: Sender<TableEvent>,
}

/// Contains a few senders, which notifies after certain iceberg events completion.
pub struct IcebergEventSyncSender {
    /// Notifies when iceberg drop table completes.
    pub iceberg_drop_table_completion_tx: mpsc::Sender<Result<()>>,
}

impl TableHandler {
    /// Create a new TableHandler for the given schema and table name
    pub fn new(
        mut table: MooncakeTable,
        iceberg_event_sync_sender: IcebergEventSyncSender,
    ) -> Self {
        // Create channel for events
        let (event_sender, event_receiver) = mpsc::channel(100);

        // Create channel for internal control events.
        let (table_notifier_tx, table_notify_rx) = mpsc::channel(100);
        table.register_table_notify(table_notifier_tx);

        // Spawn the task with the oneshot receiver
        let event_handle = Some(tokio::spawn(async move {
            Self::event_loop(
                iceberg_event_sync_sender,
                event_receiver,
                table_notify_rx,
                table,
            )
            .await;
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
    async fn event_loop(
        iceberg_event_sync_sender: IcebergEventSyncSender,
        mut event_receiver: Receiver<TableEvent>,
        mut table_notify_rx: Receiver<TableNotify>,
        mut table: MooncakeTable,
    ) {
        let mut periodic_snapshot_interval = time::interval(Duration::from_millis(500));

        // Requested minimum LSN for a force snapshot request.
        let mut force_snapshot_lsns: BTreeMap<u64, Vec<Sender<Result<()>>>> = BTreeMap::new();

        // Record LSN if the last handled table event is committed, which indicates mooncake table stays at a consistent view, so table could be flushed safely.
        let mut table_consistent_view_lsn: Option<u64> = None;

        // Whether there's an ongoing mooncake snapshot operation.
        let mut mooncake_snapshot_ongoing = false;

        // Whether there's an ongoing iceberg snapshot operation.
        let mut iceberg_snapshot_ongoing = false;

        // Whether current table receives any update events.
        let mut table_updated = false;

        // Whether iceberg snapshot result has been consumed by the latest mooncake snapshot, when creating a mooncake snapshot.
        //
        // There're three possible states for an iceberg snapshot:
        // - snapshot ongoing = false, result consumed = true: no active iceberg snapshot
        // - snapshot ongoing = true, result consumed = true: iceberg snapshot is ongoing
        // - snapshot ongoing = false, result consumed = false: iceberg snapshot completes, but wait for mooncake snapshot to consume the result
        //
        // Invariant of impossible state: snapshot ongoing = true, result consumed = false
        let mut iceberg_snapshot_result_consumed = true;

        // Invoked before mooncake snapshot creation, this function checks and resets iceberg snapshot state.
        let reset_iceberg_state_at_mooncake_snapshot =
            |iceberg_consumed: &mut bool, iceberg_ongoing: &mut bool| {
                // Validate iceberg snapshot state before mooncake snapshot creation.
                //
                // Assertion on impossible state.
                assert!(!*iceberg_ongoing || *iceberg_consumed);

                // If there's pending iceberg snapshot result unconsumed, the following mooncake snapshot will properly handle it.
                if !*iceberg_consumed {
                    *iceberg_consumed = true;
                    *iceberg_ongoing = false;
                }
            };

        // Used to decide whether we could create an iceberg snapshot.
        // The completion of an iceberg snapshot is **NOT** marked as the finish of snapshot thread, but the handling of its results.
        // We can only create a new iceberg snapshot when (1) there's no ongoing iceberg snapshot; and (2) previous snapshot results have been acknowledged.
        let can_initiate_iceberg_snapshot =
            |iceberg_consumed: bool, iceberg_ongoing: bool| iceberg_consumed && !iceberg_ongoing;

        // Process events until the receiver is closed or a Shutdown event is received
        loop {
            tokio::select! {
                // Process events from the queue
                Some(event) = event_receiver.recv() => {
                    table_consistent_view_lsn = match event {
                        TableEvent::Commit { lsn, .. } => Some(lsn),
                        // `ForceSnapshot` event doesn't affect whether mooncake is at a committed state.
                        TableEvent::ForceSnapshot { .. } => table_consistent_view_lsn,
                        _ => None,
                    };
                    table_updated = match event {
                        TableEvent::ForceSnapshot { .. } => table_updated,
                        _ => true,
                    };

                    match event {
                        TableEvent::Append { row, xact_id } => {
                            let result = match xact_id {
                                Some(xact_id) => {
                                    let res = table.append_in_stream_batch(row, xact_id);
                                    if table.should_transaction_flush(xact_id) {
                                        if let Err(e) = table.flush_transaction_stream(xact_id).await {
                                            println!("Flush failed in Append: {}", e);
                                        }
                                    }
                                    res
                                },
                                None => table.append(row),
                            };

                            if let Err(e) = result {
                                println!("Failed to append row: {}", e);
                            }
                        }
                        TableEvent::Delete { row, lsn, xact_id } => {
                            match xact_id {
                                Some(xact_id) => table.delete_in_stream_batch(row, xact_id).await,
                                None => table.delete(row, lsn).await,
                            };
                        }
                        TableEvent::Commit { lsn, xact_id } => {
                            // Force create snapshot if
                            // 1. force snapshot is requested
                            // and 2. LSN which meets force snapshot requirement has appeared, before that we still allow buffering
                            // and 3. there's no snapshot creation operation ongoing
                            let force_snapshot = !force_snapshot_lsns.is_empty()
                                && lsn >= *force_snapshot_lsns.iter().next().as_ref().unwrap().0
                                && !mooncake_snapshot_ongoing;

                            match xact_id {
                                Some(xact_id) => {
                                    if let Err(e) = table.commit_transaction_stream(xact_id, lsn).await {
                                        println!("Stream commit flush failed: {}", e);
                                    }
                                }
                                None => {
                                    table.commit(lsn);
                                    if table.should_flush() || force_snapshot {
                                        if let Err(e) = table.flush(lsn).await {
                                            println!("Flush failed in Commit: {}", e);
                                        }
                                    }
                                }
                            }

                            if force_snapshot {
                                reset_iceberg_state_at_mooncake_snapshot(&mut iceberg_snapshot_result_consumed, &mut iceberg_snapshot_ongoing);
                                table.create_snapshot(SnapshotOption {
                                    force_create: true,
                                    skip_iceberg_snapshot: iceberg_snapshot_ongoing,
                                });
                                mooncake_snapshot_ongoing = true;
                            }
                        }
                        TableEvent::StreamAbort { xact_id } => {
                            table.abort_in_stream_batch(xact_id);
                        }
                        TableEvent::Flush { lsn } => {
                            if let Err(e) = table.flush(lsn).await {
                                println!("Explicit Flush failed: {}", e);
                            }
                        }
                        TableEvent::StreamFlush { xact_id } => {
                            if let Err(e) = table.flush_transaction_stream(xact_id).await {
                                println!("Stream flush failed: {}", e);
                            }
                        }
                        TableEvent::_Shutdown => {
                            println!("Shutting down table handler");
                            break;
                        }
                        TableEvent::ForceSnapshot { lsn, tx } => {
                            // A workaround to avoid create snapshot call gets stuck, when there's no write operations to the table.
                            if !table_updated {
                                tx.send(Ok(())).await.unwrap();
                                continue;
                            }

                            // Fast-path: if iceberg snapshot requirement is already satisfied, notify directly.
                            let last_iceberg_snapshot_lsn = table.get_iceberg_snapshot_lsn();
                            if last_iceberg_snapshot_lsn.is_some() && lsn <= last_iceberg_snapshot_lsn.unwrap() {
                                tx.send(Ok(())).await.unwrap();
                            }
                            // Iceberg snapshot LSN requirement is not met, record the required LSN, so later commit will pick up.
                            else {
                                force_snapshot_lsns.entry(lsn).or_default().push(tx);
                            }
                        }
                        // Branch to drop the iceberg table, only used when the whole table requested to drop.
                        // So we block wait for asynchronous request completion.
                        TableEvent::DropIcebergTable => {
                            let res = table.drop_iceberg_table().await;
                            iceberg_event_sync_sender.iceberg_drop_table_completion_tx.send(res).await.unwrap();
                        }
                    }
                }
                // Wait for the mooncake table event notification.
                Some(event) = table_notify_rx.recv() => {
                    match event {
                        TableNotify::MooncakeTableSnapshot { lsn, iceberg_snapshot_payload } => {
                            // Notify read the mooncake table commit of LSN.
                            table.notify_snapshot_reader(lsn);

                            // Process iceberg snapshot and trigger iceberg snapshot if necessary.
                            if can_initiate_iceberg_snapshot(iceberg_snapshot_result_consumed, iceberg_snapshot_ongoing) {
                                if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
                                    table.persist_iceberg_snapshot(iceberg_snapshot_payload);
                                    iceberg_snapshot_ongoing = true;
                                }
                            }

                            mooncake_snapshot_ongoing = false;
                        }
                        TableNotify::IcebergSnapshot { iceberg_snapshot_result } => {
                            iceberg_snapshot_ongoing = false;

                            match iceberg_snapshot_result {
                                Ok(snapshot_res) => {
                                    let iceberg_flush_lsn = snapshot_res.flush_lsn;
                                    table.set_iceberg_snapshot_res(snapshot_res);
                                    iceberg_snapshot_result_consumed = false;

                                    // Notify all waiters with LSN satisfied.
                                    let new_map = force_snapshot_lsns.split_off(&(iceberg_flush_lsn + 1));
                                    for (requested_lsn, tx) in force_snapshot_lsns.iter() {
                                        ma::assert_le!(*requested_lsn, iceberg_flush_lsn);
                                        for cur_tx in tx {
                                            cur_tx.send(Ok(())).await.unwrap();
                                        }
                                    }
                                    force_snapshot_lsns = new_map;
                                }
                                Err(e) => {
                                    for (_, tx) in force_snapshot_lsns.iter() {
                                        for cur_tx in tx {
                                            let err = Error::IcebergMessage(format!("Failed to create iceberg snapshot: {:?}", e));
                                            cur_tx.send(Err(err)).await.unwrap();
                                        }
                                    }
                                    force_snapshot_lsns.clear();
                                }
                            }
                        }
                    }
                }
                // Periodic snapshot based on time
                _ = periodic_snapshot_interval.tick() => {
                    // Only create a periodic snapshot if there isn't already one in progress
                    if mooncake_snapshot_ongoing {
                        continue;
                    }

                    // Check whether a flush and force snapshot is needed.
                    if !force_snapshot_lsns.is_empty() {
                        if let Some(commit_lsn) = table_consistent_view_lsn {
                            if *force_snapshot_lsns.iter().next().as_ref().unwrap().0 <= commit_lsn {
                                table.flush(/*lsn=*/ commit_lsn).await.unwrap();
                                reset_iceberg_state_at_mooncake_snapshot(&mut iceberg_snapshot_result_consumed, &mut iceberg_snapshot_ongoing);
                                table.create_snapshot(SnapshotOption {
                                    force_create: true,
                                    skip_iceberg_snapshot: iceberg_snapshot_ongoing,
                                });
                                mooncake_snapshot_ongoing = true;
                                continue;
                            }
                        }
                    }

                    // Fallback to normal periodic snapshot.
                    reset_iceberg_state_at_mooncake_snapshot(&mut iceberg_snapshot_result_consumed, &mut iceberg_snapshot_ongoing);
                    mooncake_snapshot_ongoing = table.create_snapshot(SnapshotOption {
                        force_create: false,
                        skip_iceberg_snapshot: iceberg_snapshot_ongoing,
                    });
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
mod tests;

#[cfg(test)]
mod test_utils;
