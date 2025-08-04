/// There're a few LSN concepts used in the table handler:
/// - commit LSN: LSN for a streaming or a non-streaming LSN
/// - flush LSN: LSN for a flush operation
/// - iceberg snapshot LSN: LSN of the latest committed transaction, before which all updates have been persisted into iceberg
/// - table consistent view LSN: LSN if the last handled table event is a commit operation, which indicates mooncake table stays at a consistent view, so table could be flushed safely
/// - replication LSN: LSN come from replication.
///   It's worth noting that there's no guarantee on the numerical order for "replication LSN" and "commit LSN";
///   because if a table recovers from a clean state (aka, all committed messages have confirmed), it's possible to have iceberg snapshot LSN but no further replication LSN.
/// - persisted table LSN: the largest LSN where all updates have been persisted into iceberg
///   Suppose we have two tables, table-A has persisted all updated into iceberg; with table-B taking new updates. persisted table LSN for table-A grows with table-B.
use crate::event_sync::EventSyncSender;
use crate::storage::mooncake_table::AlterTableRequest;
use crate::storage::mooncake_table::MaintenanceOption;
use crate::storage::mooncake_table::SnapshotOption;
use crate::storage::mooncake_table::INITIAL_COPY_XACT_ID;
use crate::storage::{io_utils, MooncakeTable};
use crate::table_notify::TableEvent;
use crate::Error;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::Instrument;
use tracing::{debug, error, info_span};
mod table_handler_state;
use table_handler_state::{
    MaintenanceProcessStatus, MaintenanceRequestStatus, SpecialTableState, TableHandlerState,
};

/// Handler for table operations
pub struct TableHandler {
    /// Handle to periodical events.
    _periodic_event_handle: JoinHandle<()>,

    /// Handle to the event processing task
    _event_handle: Option<JoinHandle<()>>,

    /// Sender for the table event queue
    event_sender: Sender<TableEvent>,
}

impl TableHandler {
    /// Create a new TableHandler for the given schema and table name
    pub async fn new(
        mut table: MooncakeTable,
        event_sync_sender: EventSyncSender,
        replication_lsn_rx: watch::Receiver<u64>,
        event_replay_tx: Option<mpsc::UnboundedSender<TableEvent>>,
    ) -> Self {
        // Create channel for events
        let (event_sender, event_receiver) = mpsc::channel(100);

        // Create channel for internal control events.
        table.register_table_notify(event_sender.clone()).await;

        // Spawn the task to notify periodical events.
        let table_handler_event_sender = event_sender.clone();
        let event_sender_for_periodical_snapshot = event_sender.clone();
        let event_sender_for_periodical_force_snapshot = event_sender.clone();
        let event_sender_for_periodical_wal = event_sender.clone();
        let periodic_event_handle = tokio::spawn(async move {
            let mut periodic_snapshot_interval = tokio::time::interval(Duration::from_millis(500));
            let mut periodic_force_snapshot_interval =
                tokio::time::interval(Duration::from_secs(300));
            let mut periodic_wal_interval = tokio::time::interval(Duration::from_millis(500));

            loop {
                tokio::select! {
                    // Sending to channel fails only happens when eventloop exits, directly exit timer events.
                    _ = periodic_wal_interval.tick() => {
                        if event_sender_for_periodical_wal.send(TableEvent::PeriodicalPersistenceUpdateWal(uuid::Uuid::new_v4())).await.is_err() {
                            return;
                        }
                    }
                    _ = periodic_snapshot_interval.tick() => {
                        if event_sender_for_periodical_snapshot.send(TableEvent::PeriodicalMooncakeTableSnapshot(uuid::Uuid::new_v4())).await.is_err() {
                           return;
                        }
                    }
                    _ = periodic_force_snapshot_interval.tick() => {
                        if event_sender_for_periodical_force_snapshot.send(TableEvent::ForceSnapshot { lsn: None }).await.is_err() {
                            return;
                        }
                    }
                    else => {
                        break;
                    }
                }
            }
        });

        // Spawn the task with the oneshot receiver
        let event_handle = Some(tokio::spawn(
            async move {
                Self::event_loop(
                    table_handler_event_sender,
                    event_sync_sender,
                    event_receiver,
                    replication_lsn_rx,
                    event_replay_tx,
                    table,
                )
                .await;
            }
            .instrument(info_span!("table_event_loop")),
        ));

        // Create the handler
        Self {
            _event_handle: event_handle,
            _periodic_event_handle: periodic_event_handle,
            event_sender,
        }
    }

    /// Get the event sender to send events to this handler
    pub fn get_event_sender(&self) -> Sender<TableEvent> {
        self.event_sender.clone()
    }

    /// Main event processing loop
    #[tracing::instrument(name = "table_event_loop", skip_all)]
    async fn event_loop(
        table_handler_event_sender: Sender<TableEvent>,
        event_sync_sender: EventSyncSender,
        mut event_receiver: Receiver<TableEvent>,
        replication_lsn_rx: watch::Receiver<u64>,
        event_replay_tx: Option<mpsc::UnboundedSender<TableEvent>>,
        mut table: MooncakeTable,
    ) {
        let initial_persistence_lsn = table.get_iceberg_snapshot_lsn();
        let mut table_handler_state = TableHandlerState::new(
            event_sync_sender.table_maintenance_completion_tx.clone(),
            event_sync_sender.force_snapshot_completion_tx.clone(),
            initial_persistence_lsn,
        );

        // Used to clean up mooncake table status, and send completion notification.
        let drop_table = async |table: &mut MooncakeTable, event_sync_sender: EventSyncSender| {
            // Step-1: shutdown the table, which unreferences and deletes all cache files.
            if let Err(e) = table.shutdown().await {
                let _ = event_sync_sender.drop_table_completion_tx.send(Err(e));
                return;
            }

            // Step-2: delete the iceberg table.
            if let Err(e) = table.drop_iceberg_table().await {
                let _ = event_sync_sender.drop_table_completion_tx.send(Err(e));
                return;
            }

            // Step-3: delete the WAL files.
            if let Err(e) = table.drop_wal().await {
                let _ = event_sync_sender.drop_table_completion_tx.send(Err(e));
                return;
            }

            // Step-4: delete the mooncake table.
            if let Err(e) = table.drop_mooncake_table().await {
                let _ = event_sync_sender.drop_table_completion_tx.send(Err(e));
                return;
            }

            // Step-5: send back completion notification.
            let _ = event_sync_sender.drop_table_completion_tx.send(Ok(()));
        };

        // Util function to spawn a detached task to delete evicted data files.
        let start_task_to_delete_evicted = |evicted_file_to_delete: Vec<String>| {
            if evicted_file_to_delete.is_empty() {
                return;
            }
            tokio::task::spawn(async move {
                if let Err(err) = io_utils::delete_local_files(&evicted_file_to_delete).await {
                    error!(
                        "Failed to delete object storage cache {:?}: {:?}",
                        evicted_file_to_delete, err
                    );
                }
            });
        };

        // Process events until the receiver is closed or a Shutdown event is received
        loop {
            tokio::select! {
                // Process events from the queue
                Some(event) = event_receiver.recv() => {
                    // Record event if requested.
                    if let Some(replay_tx) = &event_replay_tx {
                        replay_tx.send(event.clone()).unwrap();
                    }

                    table_handler_state.update_table_lsns(&event);

                    match event {
                        event if event.is_ingest_event() => {
                            Self::process_cdc_table_event(event, &mut table, &mut table_handler_state).await;
                        }
                        // ==============================
                        // Interactive blocking events
                        // ==============================
                        //
                        TableEvent::ForceSnapshot { lsn } => {
                            let requested_lsn = if lsn.is_some() {
                                lsn
                            } else if table_handler_state.latest_commit_lsn.is_some() {
                                table_handler_state.latest_commit_lsn
                            } else {
                                None
                            };

                            // Fast-path: nothing to snapshot.
                            if requested_lsn.is_none() {
                                table_handler_state.force_snapshot_completion_tx.send(Some(Ok(/*lsn=*/0))).unwrap();
                                continue;
                            }

                            // Fast-path: if iceberg snapshot requirement is already satisfied, notify directly.
                            let requested_lsn = requested_lsn.unwrap();
                            let last_iceberg_snapshot_lsn = table.get_iceberg_snapshot_lsn();
                            let replication_lsn = *replication_lsn_rx.borrow();
                            let persisted_table_lsn = table_handler_state.get_persisted_table_lsn(last_iceberg_snapshot_lsn, replication_lsn);
                            if persisted_table_lsn >= requested_lsn {
                                table_handler_state.notify_persisted_table_lsn(persisted_table_lsn);
                                continue;
                            }

                            // Iceberg snapshot LSN requirement is not met, record the required LSN, so later commit will pick up.
                            else {
                                table_handler_state.largest_force_snapshot_lsn = Some(match table_handler_state.largest_force_snapshot_lsn {
                                    None => requested_lsn,
                                    Some(old_largest) => std::cmp::max(old_largest, requested_lsn),
                                });
                            }
                        }
                        // Branch to trigger a force regular index merge request.
                        TableEvent::ForceRegularIndexMerge => {
                            // TODO(hjiang): If there's already table maintenance ongoing, skip.
                            if !table_handler_state.can_start_new_maintenance() {
                                let _ = table_handler_state.table_maintenance_completion_tx.send(Ok(()));
                                continue;
                            }
                            // Otherwise queue a request.
                            assert_eq!(table_handler_state.index_merge_request_status, MaintenanceRequestStatus::Unrequested);
                            table_handler_state.index_merge_request_status = MaintenanceRequestStatus::ForceRegular;
                        }
                        // Branch to trigger a force regular data compaction request.
                        TableEvent::ForceRegularDataCompaction => {
                            if !table_handler_state.can_start_new_maintenance() {
                                let _ = table_handler_state.table_maintenance_completion_tx.send(Ok(()));
                                continue;
                            }
                            // Otherwise queue a request.
                            assert_eq!(table_handler_state.data_compaction_request_status, MaintenanceRequestStatus::Unrequested);
                            table_handler_state.data_compaction_request_status = MaintenanceRequestStatus::ForceRegular;
                        }
                        // Branch to trigger a force full index merge request.
                        TableEvent::ForceFullMaintenance => {
                            if !table_handler_state.can_start_new_maintenance() {
                                let _ = table_handler_state.table_maintenance_completion_tx.send(Ok(()));
                                continue;
                            }
                            // Otherwise queue a request.
                            assert_eq!(table_handler_state.index_merge_request_status, MaintenanceRequestStatus::Unrequested);
                            assert_eq!(table_handler_state.data_compaction_request_status, MaintenanceRequestStatus::Unrequested);
                            table_handler_state.data_compaction_request_status = MaintenanceRequestStatus::ForceFull;
                        }
                        // Branch to drop the iceberg table and clear pinned data files from the global object storage cache, only used when the whole table requested to drop.
                        // So we block wait for asynchronous request completion.
                        TableEvent::DropTable => {
                            // Fast-path: no other concurrent events, directly clean up states and ack back.
                            if table_handler_state.can_drop_table_now() {
                                drop_table(&mut table, event_sync_sender).await;
                                return;
                            }

                            // Otherwise, leave a drop marker to clean up states later.
                            table_handler_state.mark_drop_table();
                        }
                        TableEvent::AlterTable { columns_to_drop } => {
                            debug!("altering table, dropping columns: {:?}", columns_to_drop);
                            let alter_table_request = AlterTableRequest {
                                new_columns: vec![],
                                dropped_columns: columns_to_drop,
                            };
                            table_handler_state.start_alter_table(alter_table_request);
                        }
                        TableEvent::StartInitialCopy => {
                            debug!("starting initial copy");
                            table_handler_state.start_initial_copy();
                        }
                        TableEvent::FinishInitialCopy { start_lsn } => {
                            debug!("finishing initial copy");
                            if let Err(e) = table.commit_transaction_stream(INITIAL_COPY_XACT_ID, 0).await {
                                error!(error = %e, "failed to finish initial copy");
                            }
                            // Force create the snapshot with LSN 0
                            assert!(table.create_snapshot(SnapshotOption {
                                uuid: uuid::Uuid::new_v4(),
                                force_create: true,
                                skip_iceberg_snapshot: true,
                                index_merge_option: MaintenanceOption::Skip,
                                data_compaction_option: MaintenanceOption::Skip,
                            }));
                            table_handler_state.mooncake_snapshot_ongoing = true;
                            table_handler_state.finish_initial_copy();

                            // Drop any events that have LSN less than the start LSN during apply.
                            table_handler_state.initial_persistence_lsn = Some(start_lsn);
                            // Apply the buffered events.
                            Self::process_blocked_events(&mut table, &mut table_handler_state).await;
                        }
                        // ==============================
                        // Table internal events
                        // ==============================
                        //
                        TableEvent::PeriodicalMooncakeTableSnapshot(uuid) => {
                            // Only create a periodic snapshot if there isn't already one in progress
                            if table_handler_state.mooncake_snapshot_ongoing {
                                continue;
                            }

                            // Check whether a flush and force snapshot is needed.
                            if table_handler_state.has_pending_force_snapshot_request() && !table_handler_state.iceberg_snapshot_ongoing {
                                if let Some(commit_lsn) = table_handler_state.table_consistent_view_lsn {
                                    table.flush(commit_lsn).await.unwrap();
                                    table_handler_state.last_unflushed_commit_lsn = None;
                                    table_handler_state.reset_iceberg_state_at_mooncake_snapshot();
                                    if let SpecialTableState::AlterTable { .. } = table_handler_state.special_table_state {
                                        table.force_empty_iceberg_payload();
                                    }
                                    assert!(table.create_snapshot(table_handler_state.get_mooncake_snapshot_option(/*request_force=*/true, uuid)));
                                    table_handler_state.mooncake_snapshot_ongoing = true;
                                    continue;
                                }
                            }

                            // Fallback to normal periodic snapshot.
                            table_handler_state.reset_iceberg_state_at_mooncake_snapshot();
                            table_handler_state.mooncake_snapshot_ongoing = table.create_snapshot(table_handler_state.get_mooncake_snapshot_option(/*request_force=*/false, uuid));
                        }
                        TableEvent::RegularIcebergSnapshot { mut iceberg_snapshot_payload } => {
                            // Update table maintainence status.
                            if iceberg_snapshot_payload.contains_table_maintenance_payload() && table_handler_state.table_maintenance_process_status == MaintenanceProcessStatus::ReadyToPersist {
                                table_handler_state.table_maintenance_process_status = MaintenanceProcessStatus::InPersist;
                            }
                            table_handler_state.iceberg_snapshot_ongoing = true;
                            if table_handler_state.should_complete_alter_table(iceberg_snapshot_payload.flush_lsn) {
                                if let SpecialTableState::AlterTable { ref mut alter_table_request, .. } = table_handler_state.special_table_state {
                                    let new_table_metadata = table.alter_table(alter_table_request.take().unwrap());
                                    iceberg_snapshot_payload.new_table_schema = Some(new_table_metadata);
                                }
                                else {
                                    unreachable!("alter table request is not set");
                                }
                                table_handler_state.finish_alter_table();
                                Self::process_blocked_events(&mut table, &mut table_handler_state).await;
                            }
                            table.persist_iceberg_snapshot(iceberg_snapshot_payload);
                        }
                        TableEvent::MooncakeTableSnapshotResult { lsn, uuid: _, iceberg_snapshot_payload, data_compaction_payload, file_indice_merge_payload, evicted_files_to_delete } => {
                            // Spawn a detached best-effort task to delete evicted object storage cache.
                            start_task_to_delete_evicted(evicted_files_to_delete.files);

                            // Mark mooncake snapshot as completed.
                            table.mark_mooncake_snapshot_completed();
                            table_handler_state.mooncake_snapshot_ongoing = false;

                            // Drop table if requested, and table at a clean state.
                            if table_handler_state.special_table_state == SpecialTableState::DropTable && table_handler_state.can_drop_table_now() {
                                drop_table(&mut table, event_sync_sender).await;
                                return;
                            }

                            // Notify read the mooncake table commit of LSN.
                            table.notify_snapshot_reader(lsn);

                            // Process iceberg snapshot and trigger iceberg snapshot if necessary.
                            if table_handler_state.can_initiate_iceberg_snapshot() {
                                if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
                                    table_handler_event_sender.send(TableEvent::RegularIcebergSnapshot {
                                        iceberg_snapshot_payload,
                                    }).await.unwrap();
                                }
                            }

                            // Only attempt new maintenance when there's no ongoing one.
                            if table_handler_state.table_maintenance_process_status == MaintenanceProcessStatus::Unrequested {
                                // ==========================
                                // Data compaction
                                // ==========================
                                //
                                // Unlike snapshot, we can actually have multiple data compaction operations ongoing concurrently,
                                // to simplify workflow we limit at most one ongoing.
                                //
                                // If there's force compact request, and there's nothing to compact, directly ack back.
                                if table_handler_state.data_compaction_request_status.is_force_request() && data_compaction_payload.is_nothing() {
                                    let _ = table_handler_state.table_maintenance_completion_tx.send(Ok(()));
                                    table_handler_state.data_compaction_request_status = MaintenanceRequestStatus::Unrequested;
                                }

                                // Get payload and try perform maintenance operations.
                                if let Some(data_compaction_payload) = data_compaction_payload.take_payload() {
                                    table_handler_state.table_maintenance_process_status = MaintenanceProcessStatus::InProcess;
                                    table.perform_data_compaction(data_compaction_payload);
                                }

                                // ==========================
                                // Index merge
                                // ==========================
                                //
                                // Unlike snapshot, we can actually have multiple file index merge operations ongoing concurrently,
                                // to simplify workflow we limit at most one ongoing.
                                //
                                // If there's force merge request, and there's nothing to merge, directly ack back.
                                if table_handler_state.index_merge_request_status.is_force_request() && file_indice_merge_payload.is_nothing() {
                                    let _ = table_handler_state.table_maintenance_completion_tx.send(Ok(()));
                                    table_handler_state.index_merge_request_status = MaintenanceRequestStatus::Unrequested;
                                }

                                if let Some(file_indice_merge_payload) = file_indice_merge_payload.take_payload() {
                                    assert_eq!(table_handler_state.table_maintenance_process_status, MaintenanceProcessStatus::Unrequested);
                                    table_handler_state.table_maintenance_process_status = MaintenanceProcessStatus::InProcess;
                                    table.perform_index_merge(file_indice_merge_payload);
                                }
                            }
                        }
                        TableEvent::IcebergSnapshotResult { iceberg_snapshot_result } => {
                            table_handler_state.iceberg_snapshot_ongoing = false;
                            match iceberg_snapshot_result {
                                Ok(snapshot_res) => {
                                    // Update table maintenance operation status.
                                    if table_handler_state.table_maintenance_process_status == MaintenanceProcessStatus::InPersist && snapshot_res.contains_maintanence_result() {
                                        table_handler_state.table_maintenance_process_status = MaintenanceProcessStatus::Unrequested;
                                        // Table maintenance could come from table internal events, which doesn't have notification receiver.
                                        let _ = table_handler_state.table_maintenance_completion_tx.send(Ok(()));
                                    }

                                    // Buffer iceberg persistence result, which later will be reflected to mooncake snapshot.
                                    let iceberg_flush_lsn = snapshot_res.flush_lsn;
                                    event_sync_sender.flush_lsn_tx.send(iceberg_flush_lsn).unwrap();
                                    debug!("Iceberg snapshot result wal metadata: {:?}", snapshot_res.corresponding_wal_metadata);
                                    table.set_iceberg_snapshot_res(snapshot_res);
                                    table_handler_state.iceberg_snapshot_result_consumed = false;

                                    // Notify all waiters with LSN satisfied.
                                    let replication_lsn = *replication_lsn_rx.borrow();
                                    table_handler_state.update_iceberg_persisted_lsn(iceberg_flush_lsn, replication_lsn);
                                }
                                Err(e) => {
                                    let err = Err(Error::IcebergMessage(format!("Failed to create iceberg snapshot: {e:?}")));
                                    if table_handler_state.has_pending_force_snapshot_request() {
                                        if let Err(send_err) = table_handler_state.force_snapshot_completion_tx.send(Some(err.clone())) {
                                            error!(error = ?send_err, "failed to notify force snapshot, because receive end has closed channel");
                                        }
                                    }

                                    // Update table maintainence operation status.
                                    if table_handler_state.table_maintenance_process_status == MaintenanceProcessStatus::InPersist {
                                        table_handler_state.table_maintenance_process_status = MaintenanceProcessStatus::Unrequested;
                                        // Table maintenance could come from table internal events, which doesn't have notification receiver.
                                        let _ = table_handler_state.table_maintenance_completion_tx.send(Err(e));
                                    }

                                    // If iceberg snapshot fails, send error back to all broadcast subscribers and unset force snapshot requests.
                                    table_handler_state.largest_force_snapshot_lsn = None;
                                }
                            }

                            // Drop table if requested, and table at a clean state.
                            if table_handler_state.special_table_state == SpecialTableState::DropTable && table_handler_state.can_drop_table_now() {
                                drop_table(&mut table, event_sync_sender).await;
                                return;
                            }
                        }
                        TableEvent::IndexMergeResult { index_merge_result } => {
                            table.set_file_indices_merge_res(index_merge_result);
                            table_handler_state.mark_index_merge_completed().await;
                            // Check whether need to drop table.
                            if table_handler_state.special_table_state == SpecialTableState::DropTable && table_handler_state.can_drop_table_now() {
                                drop_table(&mut table, event_sync_sender).await;
                                return;
                            }
                        }
                        TableEvent::DataCompactionResult { data_compaction_result } => {
                            table_handler_state.mark_data_compaction_completed(&data_compaction_result).await;
                            match data_compaction_result {
                                Ok(data_compaction_res) => {
                                    table.set_data_compaction_res(data_compaction_res)
                                }
                                Err(err) => {
                                    error!(error = ?err, "failed to perform compaction");
                                }
                            }
                            // Check whether need to drop table.
                            if table_handler_state.special_table_state == SpecialTableState::DropTable && table_handler_state.can_drop_table_now() {
                                drop_table(&mut table, event_sync_sender).await;
                                return;
                            }
                        }
                        TableEvent::EvictedFilesToDelete { evicted_files } => {
                            start_task_to_delete_evicted(evicted_files.files);
                        }
                        TableEvent::PeriodicalPersistenceUpdateWal(uuid) => {
                            if !table_handler_state.wal_persist_ongoing {
                                table_handler_state.wal_persist_ongoing = true;
                                let ongoing_persist_truncate = table.do_wal_persistence_update(uuid);
                                table_handler_state.wal_persist_ongoing = ongoing_persist_truncate;
                            }
                        }
                        TableEvent::PeriodicalWalPersistenceUpdateResult { result } => {
                            match result {
                                Ok(result) => {
                                    table_handler_state.wal_persist_ongoing = false;
                                    if let Some(highest_lsn) = table.handle_completed_wal_persistence_update(&result) {
                                        event_sync_sender.wal_flush_lsn_tx.send(highest_lsn).unwrap();
                                    }

                                    // Check whether need to drop table.
                                    if table_handler_state.special_table_state == SpecialTableState::DropTable && table_handler_state.can_drop_table_now() {
                                        drop_table(&mut table, event_sync_sender).await;
                                        return;
                                    }
                                }
                                Err(e) => {
                                    error!(error = %e, "failed to persist wal");
                                }
                            }
                        }
                        // ==============================
                        // Replication events
                        // ==============================
                        //
                        _ => {
                            unreachable!("unexpected event: {:?}", event);
                        }
                    }
                }
                // If all senders have been dropped, exit the loop
                else => {
                    if let Err(e) = table.shutdown().await {
                        error!(error = %e, "failed to shutdown table");
                    }
                    debug!("all event senders dropped, shutting down table handler");
                    break;
                }
            }
        }
    }

    async fn process_cdc_table_event(
        event: TableEvent,
        table: &mut MooncakeTable,
        table_handler_state: &mut TableHandlerState,
    ) {
        // ==============================
        // Replication events
        // ==============================
        //
        let is_initial_copy_event = matches!(
            event,
            TableEvent::Append {
                is_copied: true,
                ..
            }
        );

        if table_handler_state.is_in_blocking_state() && !is_initial_copy_event {
            table_handler_state.initial_copy_buffered_events.push(event);
            return;
        }

        // In the case that this is an initial copy event we acutally expect the LSN to be less than the initial persistence LSN, hence we don't discard it.
        if table_handler_state.should_discard_event(&event) && !is_initial_copy_event {
            return;
        }
        assert_eq!(
            is_initial_copy_event,
            table_handler_state.special_table_state == SpecialTableState::InitialCopy
        );

        table.push_wal_event(&event);

        match event {
            TableEvent::Append {
                is_copied,
                row,
                xact_id,
                ..
            } => {
                if is_copied {
                    if let Err(e) = table.append_in_stream_batch(row, INITIAL_COPY_XACT_ID) {
                        error!(error = %e, "failed to append row");
                    }
                    return;
                }

                let result = match xact_id {
                    Some(xact_id) => {
                        let res = table.append_in_stream_batch(row, xact_id);
                        if table.should_transaction_flush(xact_id) {
                            if let Err(e) = table.flush_stream(xact_id, None).await {
                                error!(error = %e, "flush failed in append");
                            }
                        }
                        res
                    }
                    None => table.append(row),
                };

                if let Err(e) = result {
                    error!(error = %e, "failed to append row");
                }
            }
            TableEvent::Delete { row, lsn, xact_id } => {
                match xact_id {
                    Some(xact_id) => table.delete_in_stream_batch(row, xact_id).await,
                    None => table.delete(row, lsn).await,
                };
            }
            TableEvent::Commit { lsn, xact_id } => {
                Self::commit_and_attempt_flush(
                    lsn,
                    xact_id,
                    table_handler_state,
                    table,
                    /*force_flush_requested=*/ false,
                )
                .await;
            }
            TableEvent::StreamAbort { xact_id } => {
                table.abort_in_stream_batch(xact_id);
            }
            TableEvent::CommitFlush { lsn, xact_id } => {
                Self::commit_and_attempt_flush(
                    lsn,
                    xact_id,
                    table_handler_state,
                    table,
                    /*force_flush_requested=*/ true,
                )
                .await;
            }
            TableEvent::StreamFlush { xact_id } => {
                if let Err(e) = table.flush_stream(xact_id, None).await {
                    error!(error = %e, "stream flush failed");
                }
            }
            _ => {
                unreachable!("unexpected event: {:?}", event)
            }
        }
    }

    async fn process_blocked_events(
        table: &mut MooncakeTable,
        table_handler_state: &mut TableHandlerState,
    ) {
        let buffered_events = table_handler_state
            .initial_copy_buffered_events
            .drain(..)
            .collect::<Vec<_>>();
        for event in buffered_events {
            Self::process_cdc_table_event(event, table, table_handler_state).await;
        }
    }

    async fn commit_and_attempt_flush(
        lsn: u64,
        xact_id: Option<u32>,
        table_handler_state: &mut TableHandlerState,
        table: &mut MooncakeTable,
        force_flush_requested: bool,
    ) {
        // Force create snapshot if
        // 1. force snapshot is requested
        // and 2. LSN which meets force snapshot requirement has appeared, before that we still allow buffering
        // and 3. there's no snapshot creation operation ongoing

        let should_force_snapshot = table_handler_state.should_force_snapshot_by_commit_lsn(lsn);

        match xact_id {
            Some(xact_id) => {
                // Attempt to flush all preceding unflushed committed non-streaming writes.
                if let Some(last_unflushed_commit_lsn) =
                    table_handler_state.last_unflushed_commit_lsn
                {
                    if let Err(e) = table.flush(last_unflushed_commit_lsn).await {
                        error!(error = %e, "flush non-streaming writes failed in LSN {lsn}");
                    }
                    table_handler_state.last_unflushed_commit_lsn = None;
                }

                // For streaming writers, whose commit LSN is only finalized at commit phase, delay decision whether to discard now.
                // If commit LSN is no fresher than persistence LSN, it means already persisted, directly discard.
                if let Some(initial_persistence_lsn) = table_handler_state.initial_persistence_lsn {
                    if lsn <= initial_persistence_lsn {
                        table.abort_in_stream_batch(xact_id);
                        return;
                    }
                }
                if let Err(e) = table.commit_transaction_stream(xact_id, lsn).await {
                    error!(error = %e, "stream commit flush failed");
                }
            }
            None => {
                table.commit(lsn);
                if table.should_flush() || should_force_snapshot || force_flush_requested {
                    table_handler_state.last_unflushed_commit_lsn = None;
                    if let Err(e) = table.flush(lsn).await {
                        error!(error = %e, "flush failed in commit");
                    }
                }
            }
        }

        if should_force_snapshot {
            table_handler_state.reset_iceberg_state_at_mooncake_snapshot();
            assert!(table.create_snapshot(
                table_handler_state.get_mooncake_snapshot_option(
                    /*request_force=*/ true,
                    uuid::Uuid::new_v4()
                )
            ));
            table_handler_state.mooncake_snapshot_ongoing = true;
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod failure_tests;

#[cfg(test)]
#[cfg(feature = "chaos-test")]
mod chaos_test;
