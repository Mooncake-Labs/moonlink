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
use crate::storage::mooncake_table::replay::replay_events::MooncakeTableEvent;
use crate::storage::mooncake_table::AlterTableRequest;
use crate::storage::mooncake_table::TableDropper;
use crate::storage::snapshot_options::IcebergSnapshotOption;
use crate::storage::snapshot_options::MaintenanceOption;
use crate::storage::snapshot_options::SnapshotOption;
use crate::storage::task_supervisor::panic_message;
use crate::storage::{io_utils, MooncakeTable};
use crate::table_handler_timer::TableHandlerTimer;
use crate::table_notify::TableEvent;
use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::Instrument;
use tracing::{debug, error, info_span, warn};
pub(crate) mod table_handler_state;
use table_handler_state::{
    MaintenanceProcessStatus, MaintenanceRequestStatus, SpecialTableState, TableHandlerState,
    MAX_CONSECUTIVE_PERSISTENCE_FAILURES,
};

const MAX_BUFFERED_TABLE_EVENTS: usize = 32_768;

/// How long the DROP of an errored table waits for in-flight background tasks before deleting
/// the table files anyway.
const ERRORED_DROP_BACKGROUND_TASK_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// DROP of an errored table: requested once, completed when no background task can touch the
/// table files any more (or after a timeout).
struct ErroredDrop {
    pending: bool,
    background_task_wait: std::time::Duration,
}

impl Default for ErroredDrop {
    fn default() -> Self {
        Self {
            pending: false,
            background_task_wait: ERRORED_DROP_BACKGROUND_TASK_WAIT,
        }
    }
}

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
        mut table_handler_timer: TableHandlerTimer,
        replication_lsn_rx: watch::Receiver<u64>,
        handler_event_replay_tx: Option<mpsc::UnboundedSender<TableEvent>>,
        table_event_replay_tx: Option<mpsc::UnboundedSender<MooncakeTableEvent>>,
    ) -> Self {
        // Create channel for events
        let (event_sender, event_receiver) = mpsc::channel(MAX_BUFFERED_TABLE_EVENTS);

        // A recovered table has persisted everything up to its snapshot; report that before the
        // replication slot is first acknowledged, so the slot never moves past what it still needs.
        advance_flush_lsn(
            &event_sync_sender.flush_lsn_tx,
            table.get_persistence_snapshot_lsn().unwrap_or(0),
        );

        // Register channel for internal control events.
        table.register_table_notify(event_sender.clone()).await;
        // Register channel for mooncake table events replay.
        table.register_event_replay_tx(table_event_replay_tx);

        // Spawn the task to notify periodical events.
        let table_handler_event_sender = event_sender.clone();
        let event_sender_for_periodical_snapshot = event_sender.clone();
        let event_sender_for_periodical_force_snapshot = event_sender.clone();
        let periodic_event_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Sending to channel fails only happens when eventloop exits, directly exit timer events.
                    _ = table_handler_timer.mooncake_snapshot_timer.tick() => {
                        if event_sender_for_periodical_snapshot.send(TableEvent::PeriodicalMooncakeTableSnapshot(uuid::Uuid::new_v4())).await.is_err() {
                           return;
                        }
                    }
                    _ = table_handler_timer.force_snapshot_timer.tick() => {
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

        // If the event loop itself panics, a minimal loop keeps serving the table's channel: it
        // marks the table errored, fails waiters and still performs DROP.
        let fallback = EventLoopPanicFallback {
            dropper: table.dropper(),
            table_name: table.mooncake_table_id().to_string(),
            drop_completion_tx: event_sync_sender.drop_table_completion_tx.clone(),
            force_snapshot_completion_tx: event_sync_sender.force_snapshot_completion_tx.clone(),
            table_maintenance_completion_tx: event_sync_sender
                .table_maintenance_completion_tx
                .clone(),
            table_status_tx: event_sync_sender.table_status_tx.clone(),
        };

        // Spawn the task with the oneshot receiver
        let event_handle = Some(tokio::spawn(
            async move {
                let mut event_receiver = event_receiver;
                let result = AssertUnwindSafe(Self::event_loop(
                    table_handler_event_sender,
                    event_sync_sender,
                    &mut event_receiver,
                    replication_lsn_rx,
                    handler_event_replay_tx,
                    table,
                ))
                .catch_unwind()
                .await;
                if let Err(payload) = result {
                    fallback
                        .run(&mut event_receiver, panic_message(payload.as_ref()))
                        .await;
                }
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
        event_receiver: &mut Receiver<TableEvent>,
        replication_lsn_rx: watch::Receiver<u64>,
        handler_event_replay_tx: Option<mpsc::UnboundedSender<TableEvent>>,
        mut table: MooncakeTable,
    ) {
        // Postgres replays everything after the slot's confirmed LSN, which never exceeds the
        // iceberg snapshot LSN, so replayed events up to that LSN are already persisted.
        let persistence_snapshot_lsn = table.get_persistence_snapshot_lsn();
        let mut table_handler_state = TableHandlerState::new(
            event_sync_sender.table_maintenance_completion_tx.clone(),
            event_sync_sender.force_snapshot_completion_tx.clone(),
            /*initial_persistence_lsn=*/ persistence_snapshot_lsn,
            persistence_snapshot_lsn,
        )
        .with_table_status_tx(event_sync_sender.table_status_tx.clone());
        // DROP progress of an errored table.
        let mut errored_drop = ErroredDrop::default();

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

            // Step-3: delete the mooncake table.
            if let Err(e) = table.drop_mooncake_table().await {
                let _ = event_sync_sender.drop_table_completion_tx.send(Err(e));
                return;
            }

            // Step-4: send back completion notification.
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

        const MAX_EVENTS_PER_BATCH: usize = 128;
        let mut buf = Vec::<TableEvent>::with_capacity(MAX_EVENTS_PER_BATCH);
        loop {
            buf.clear();
            let n = event_receiver
                .recv_many(&mut buf, MAX_EVENTS_PER_BATCH)
                .await;
            if n == 0 {
                break;
            }

            // Process events until the receiver is closed or a Shutdown event is received
            for event in buf.drain(..) {
                // Record event if requested.
                if let Some(replay_tx) = &handler_event_replay_tx {
                    replay_tx.send(event.clone()).unwrap();
                }

                #[cfg(test)]
                if let TableEvent::SetErroredDropWaitForTest { wait } = event {
                    errored_drop.background_task_wait = wait;
                    continue;
                }
                #[cfg(test)]
                if let TableEvent::SetGcGraceForTest { grace } = event {
                    table.set_gc_grace_for_test(grace);
                    continue;
                }

                // ==============================
                // Errored state
                // ==============================
                //
                if let TableEvent::MooncakeTableSnapshotResult {
                    mooncake_snapshot_result,
                } = &event
                {
                    if let Some(reason) = &mooncake_snapshot_result.unmatched_deletions {
                        if Self::enter_errored(&mut table_handler_state, &table, reason.clone())
                            == Some(true)
                        {
                            Self::request_errored_drop(
                                &mut errored_drop,
                                &table_handler_event_sender,
                            );
                        }
                    }
                }
                if table_handler_state.is_errored()
                    || matches!(
                        event,
                        TableEvent::EnterErrored { .. } | TableEvent::BackgroundTaskFailed { .. }
                    )
                {
                    let drop_now = Self::process_errored_table_event(
                        event,
                        &mut table,
                        &mut table_handler_state,
                        &mut errored_drop,
                        &table_handler_event_sender,
                    )
                    .await;
                    if drop_now {
                        Self::drop_errored_table(&mut table, event_sync_sender).await;
                        return;
                    }
                    continue;
                }

                match event {
                    event if event.is_ingest_event() => {
                        Self::process_cdc_table_event(event, &mut table, &mut table_handler_state)
                            .await;
                    }
                    TableEvent::ReplicationProgress { lsn } => {
                        if table_handler_state
                            .has_no_unpersisted_commit(table.get_persistence_snapshot_lsn())
                        {
                            advance_flush_lsn(&event_sync_sender.flush_lsn_tx, lsn);
                        }
                    }
                    // ==============================
                    // Bulk ingestion events
                    // ==============================
                    TableEvent::LoadFiles {
                        files,
                        storage_config,
                        lsn,
                    } => {
                        if table_handler_state.special_table_state == SpecialTableState::InitialCopy
                        {
                            if !files.is_empty() {
                                table_handler_state.initial_copy_loaded_files = true;
                            }
                        } else {
                            // A bulk load is a commit at `lsn` that has to be persisted too.
                            table_handler_state.latest_commit_lsn =
                                table_handler_state.latest_commit_lsn.max(Some(lsn));
                        }
                        table.batch_ingest(files, storage_config, lsn).await;
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
                            table_handler_state
                                .force_snapshot_completion_tx
                                .send(Some(Ok(/*lsn=*/ 0)))
                                .unwrap();
                            continue;
                        }

                        // Fast-path: if iceberg snapshot requirement is already satisfied, notify directly.
                        let requested_lsn = requested_lsn.unwrap();
                        let last_persistence_snapshot_lsn = table.get_persistence_snapshot_lsn();
                        let replication_lsn = *replication_lsn_rx.borrow();
                        let persisted_table_lsn = table_handler_state.get_persisted_table_lsn(
                            last_persistence_snapshot_lsn,
                            replication_lsn,
                        );

                        if persisted_table_lsn >= requested_lsn {
                            table_handler_state.notify_persisted_table_lsn(persisted_table_lsn);
                            continue;
                        }
                        // Iceberg snapshot LSN requirement is not met, record the required LSN, so later commit will pick up.
                        else {
                            table_handler_state.largest_force_snapshot_lsn =
                                Some(match table_handler_state.largest_force_snapshot_lsn {
                                    None => requested_lsn,
                                    Some(old_largest) => std::cmp::max(old_largest, requested_lsn),
                                });
                        }
                    }
                    // Branch to trigger a force regular index merge request.
                    TableEvent::ForceRegularIndexMerge => {
                        // TODO(hjiang): If there's already table maintenance ongoing, skip.
                        if !table_handler_state.can_start_new_maintenance() {
                            let _ = table_handler_state
                                .table_maintenance_completion_tx
                                .send(Ok(()));
                            continue;
                        }
                        // Otherwise queue a request.
                        assert_eq!(
                            table_handler_state.index_merge_request_status,
                            MaintenanceRequestStatus::Unrequested
                        );
                        table_handler_state.index_merge_request_status =
                            MaintenanceRequestStatus::ForceRegular;
                    }
                    // Branch to trigger a force regular data compaction request.
                    TableEvent::ForceRegularDataCompaction => {
                        if !table_handler_state.can_start_new_maintenance() {
                            let _ = table_handler_state
                                .table_maintenance_completion_tx
                                .send(Ok(()));
                            continue;
                        }
                        // Otherwise queue a request.
                        assert_eq!(
                            table_handler_state.data_compaction_request_status,
                            MaintenanceRequestStatus::Unrequested
                        );
                        table_handler_state.data_compaction_request_status =
                            MaintenanceRequestStatus::ForceRegular;
                    }
                    // Branch to trigger a force full index merge request.
                    TableEvent::ForceFullMaintenance => {
                        if !table_handler_state.can_start_new_maintenance() {
                            let _ = table_handler_state
                                .table_maintenance_completion_tx
                                .send(Ok(()));
                            continue;
                        }
                        // Otherwise queue a request.
                        assert_eq!(
                            table_handler_state.index_merge_request_status,
                            MaintenanceRequestStatus::Unrequested
                        );
                        assert_eq!(
                            table_handler_state.data_compaction_request_status,
                            MaintenanceRequestStatus::Unrequested
                        );
                        table_handler_state.data_compaction_request_status =
                            MaintenanceRequestStatus::ForceFull;
                    }
                    // Branch to drop the iceberg table and clear pinned data files from the global object storage cache, only used when the whole table requested to drop.
                    // So we block wait for asynchronous request completion.
                    TableEvent::DropTable => {
                        // A flush waiting for disk space would hold up the drop for its whole wait.
                        table.cancel_flushes_waiting_for_disk_space();
                        // Fast-path: no other concurrent events, directly clean up states and ack back.
                        if table_handler_state.can_drop_table_now(table.has_ongoing_flush()) {
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
                        // Force create the snapshot with LSN `start_lsn`
                        if !table_handler_state.mooncake_snapshot_ongoing {
                            let created = table.try_create_mooncake_snapshot(SnapshotOption {
                                uuid: uuid::Uuid::new_v4(),
                                force_create: true,
                                dump_snapshot: false,
                                iceberg_snapshot_option: IcebergSnapshotOption::BestEffort(
                                    uuid::Uuid::new_v4(),
                                ),
                                index_merge_option: MaintenanceOption::Skip,
                                data_compaction_option: MaintenanceOption::Skip,
                            });
                            if created {
                                table_handler_state.mooncake_snapshot_ongoing = true;
                            }
                        }
                        table_handler_state.finish_initial_copy(start_lsn);
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

                        if table_handler_state.has_pending_force_snapshot_request()
                            && !table_handler_state.persistence_snapshot_ongoing
                        {
                            // flush if needed
                            if let Some(commit_lsn) = table_handler_state.table_consistent_view_lsn
                            {
                                if table_handler_state
                                    .should_force_flush(commit_lsn, table.get_last_flush_lsn())
                                {
                                    let event_id = uuid::Uuid::new_v4();
                                    table.flush(commit_lsn, event_id).unwrap();
                                    table_handler_state.last_unflushed_commit_lsn = None;
                                }
                            }

                            // force snapshot if lsn is satisfied
                            if table_handler_state
                                .largest_force_snapshot_lsn
                                .expect("has_pending_force_snapshot_request")
                                < table.get_min_ongoing_flush_lsn()
                            {
                                table_handler_state.reset_iceberg_state_at_mooncake_snapshot();
                                if let SpecialTableState::AlterTable { .. } =
                                    table_handler_state.special_table_state
                                {
                                    table.force_empty_persistence_payload();
                                }
                                assert!(table.try_create_mooncake_snapshot(
                                    table_handler_state.get_mooncake_snapshot_option(
                                        /*request_force=*/ true, uuid
                                    )
                                ));
                                table_handler_state.mooncake_snapshot_ongoing = true;
                                continue;
                            }
                        }

                        // Fallback to normal periodic snapshot.
                        table_handler_state.reset_iceberg_state_at_mooncake_snapshot();
                        table_handler_state.mooncake_snapshot_ongoing = table
                            .try_create_mooncake_snapshot(
                                table_handler_state.get_mooncake_snapshot_option(
                                    /*request_force=*/ false, uuid,
                                ),
                            );
                    }
                    TableEvent::RegularIcebergSnapshot {
                        mut persistence_snapshot_payload,
                    } => {
                        // Update table maintenance status.
                        if persistence_snapshot_payload.contains_table_maintenance_payload()
                            && table_handler_state.table_maintenance_process_status
                                == MaintenanceProcessStatus::ReadyToPersist
                        {
                            table_handler_state.table_maintenance_process_status =
                                MaintenanceProcessStatus::InPersist;
                        }
                        table_handler_state.persistence_snapshot_ongoing = true;
                        if table_handler_state
                            .should_complete_alter_table(persistence_snapshot_payload.flush_lsn)
                        {
                            if let SpecialTableState::AlterTable {
                                ref mut alter_table_request,
                                ..
                            } = table_handler_state.special_table_state
                            {
                                let new_table_metadata =
                                    table.alter_table(alter_table_request.take().unwrap());
                                persistence_snapshot_payload.new_table_schema =
                                    Some(new_table_metadata);
                            } else {
                                unreachable!("alter table request is not set");
                            }
                            table_handler_state.finish_alter_table();
                            Self::process_blocked_events(&mut table, &mut table_handler_state)
                                .await;
                        }
                        table.persist_iceberg_snapshot(persistence_snapshot_payload);
                    }
                    TableEvent::MooncakeTableSnapshotResult {
                        mooncake_snapshot_result,
                    } => {
                        // Record mooncake snapshot completion.
                        // Notice: operation completion record should be the first thing to do on event notification, and contains all information.
                        table.record_mooncake_snapshot_completion(&mooncake_snapshot_result);

                        // Spawn a detached best-effort task to delete evicted object storage cache.
                        start_task_to_delete_evicted(
                            mooncake_snapshot_result.evicted_data_files_to_delete,
                        );

                        // Mark mooncake snapshot as completed.
                        table.mark_mooncake_snapshot_completed();
                        table_handler_state.mooncake_snapshot_ongoing = false;

                        // Drop table if requested, and table at a clean state.
                        if table_handler_state.special_table_state == SpecialTableState::DropTable
                            && table_handler_state.can_drop_table_now(table.has_ongoing_flush())
                        {
                            // Decrement reference count for data compaction payload if applicable.
                            if let Some(payload) = mooncake_snapshot_result
                                .data_compaction_payload
                                .take_payload()
                            {
                                payload.unpin_referenced_compaction_payload().await;
                            }

                            drop_table(&mut table, event_sync_sender).await;
                            return;
                        }

                        // Notify read the mooncake table commit of LSN.
                        table.notify_snapshot_reader(mooncake_snapshot_result.commit_lsn);

                        // Process iceberg snapshot and trigger iceberg snapshot if necessary.
                        let min_pending_flush_lsn = table.get_min_ongoing_flush_lsn();
                        if TableHandlerState::can_initiate_iceberg_snapshot(
                            mooncake_snapshot_result.commit_lsn,
                            min_pending_flush_lsn,
                            table_handler_state.persistence_snapshot_result_consumed,
                            table_handler_state.persistence_snapshot_ongoing,
                        ) && table_handler_state.persistence_backoff_elapsed()
                        {
                            if let Some(persistence_snapshot_payload) =
                                mooncake_snapshot_result.persistence_snapshot_payload
                            {
                                table_handler_event_sender
                                    .send(TableEvent::RegularIcebergSnapshot {
                                        persistence_snapshot_payload,
                                    })
                                    .await
                                    .unwrap();
                            }
                        }

                        // Record whether data compaction actually takes place.
                        let mut data_compaction_take_place = false;

                        // Only attempt new maintenance when there's no ongoing one.
                        if table_handler_state.table_maintenance_process_status
                            == MaintenanceProcessStatus::Unrequested
                        {
                            // ==========================
                            // Data compaction
                            // ==========================
                            //
                            // Unlike snapshot, we can actually have multiple data compaction operations ongoing concurrently,
                            // to simplify workflow we limit at most one ongoing.
                            //
                            // If there's force compact request, and there's nothing to compact, directly ack back.
                            if table_handler_state
                                .data_compaction_request_status
                                .is_force_request()
                                && mooncake_snapshot_result
                                    .data_compaction_payload
                                    .is_nothing()
                            {
                                let _ = table_handler_state
                                    .table_maintenance_completion_tx
                                    .send(Ok(()));
                                table_handler_state.data_compaction_request_status =
                                    MaintenanceRequestStatus::Unrequested;
                            }

                            // Get payload and try perform maintenance operations.
                            if let Some(data_compaction_payload) = mooncake_snapshot_result
                                .data_compaction_payload
                                .get_payload_reference()
                            {
                                data_compaction_take_place = true;
                                table_handler_state.table_maintenance_process_status =
                                    MaintenanceProcessStatus::InProcess;
                                table.perform_data_compaction(data_compaction_payload.clone());
                            }

                            // ==========================
                            // Index merge
                            // ==========================
                            //
                            // Unlike snapshot, we can actually have multiple file index merge operations ongoing concurrently,
                            // to simplify workflow we limit at most one ongoing.
                            //
                            // If there's force merge request, and there's nothing to merge, directly ack back.
                            if table_handler_state
                                .index_merge_request_status
                                .is_force_request()
                                && mooncake_snapshot_result
                                    .file_indices_merge_payload
                                    .is_nothing()
                            {
                                let _ = table_handler_state
                                    .table_maintenance_completion_tx
                                    .send(Ok(()));
                                table_handler_state.index_merge_request_status =
                                    MaintenanceRequestStatus::Unrequested;
                            }

                            if let Some(file_indices_merge_payload) = mooncake_snapshot_result
                                .file_indices_merge_payload
                                .take_payload()
                            {
                                assert_eq!(
                                    table_handler_state.table_maintenance_process_status,
                                    MaintenanceProcessStatus::Unrequested
                                );
                                table_handler_state.table_maintenance_process_status =
                                    MaintenanceProcessStatus::InProcess;
                                table.perform_index_merge(file_indices_merge_payload);
                            }
                        }

                        // Decrement reference count for data compaction payload if applicable.
                        if let Some(payload) = mooncake_snapshot_result
                            .data_compaction_payload
                            .take_payload()
                        {
                            if !data_compaction_take_place {
                                payload.unpin_referenced_compaction_payload().await;
                            }
                        }
                    }
                    TableEvent::PersistenceSnapshotResult {
                        persistence_snapshot_result,
                    } => {
                        table_handler_state.persistence_snapshot_ongoing = false;
                        match persistence_snapshot_result {
                            Ok(snapshot_res) => {
                                // Record iceberg snapshot completion.
                                // Notice: operation completion record should be the first thing to do on event notification, and contains all information.
                                table.record_iceberg_snapshot_completion(&snapshot_res);

                                // Start a background task to delete evicted files at best-effort.
                                start_task_to_delete_evicted(
                                    snapshot_res.evicted_files_to_delete.clone(),
                                );

                                // Update table maintenance operation status.
                                if table_handler_state.table_maintenance_process_status
                                    == MaintenanceProcessStatus::InPersist
                                    && snapshot_res.contains_maintenance_result()
                                {
                                    table_handler_state.table_maintenance_process_status =
                                        MaintenanceProcessStatus::Unrequested;
                                    // Table maintenance could come from table internal events, which doesn't have notification receiver.
                                    let _ = table_handler_state
                                        .table_maintenance_completion_tx
                                        .send(Ok(()));
                                }

                                table_handler_state.record_persistence_success();

                                // Buffer iceberg persistence result, which later will be reflected to mooncake snapshot.
                                let iceberg_flush_lsn = snapshot_res.flush_lsn;
                                advance_flush_lsn(
                                    &event_sync_sender.flush_lsn_tx,
                                    iceberg_flush_lsn,
                                );
                                table.set_persistence_snapshot_res(snapshot_res);
                                table_handler_state.persistence_snapshot_result_consumed = false;

                                // Notify all waiters with LSN satisfied.
                                let replication_lsn = *replication_lsn_rx.borrow();
                                table_handler_state.update_iceberg_persisted_lsn(
                                    iceberg_flush_lsn,
                                    replication_lsn,
                                );
                            }
                            Err(e) => {
                                // Take the table manager back; an unknown commit outcome or a
                                // lost manager cannot be retried safely.
                                let manager_recovered = table.on_persistence_failed();
                                let outcome_unknown = !manager_recovered
                                    || matches!(e, crate::Error::IcebergCommitOutcomeUnknown(_));
                                let failures = if e.is_storage_full() {
                                    table_handler_state.record_persistence_failure_on_full_disk()
                                } else {
                                    table_handler_state.record_persistence_failure()
                                };
                                error!(
                                    error = ?e,
                                    table = table.mooncake_table_id(),
                                    failures,
                                    "iceberg persistence failed"
                                );
                                let errored_reason = if outcome_unknown {
                                    Some(format!("iceberg persistence failed and its outcome is unknown: {e}"))
                                } else if failures >= MAX_CONSECUTIVE_PERSISTENCE_FAILURES {
                                    Some(format!(
                                        "iceberg persistence failed {failures} times in a row, last error: {e}"
                                    ))
                                } else {
                                    None
                                };

                                if table_handler_state.has_pending_force_snapshot_request() {
                                    if let Err(send_err) = table_handler_state
                                        .force_snapshot_completion_tx
                                        .send(Some(Err(e.clone())))
                                    {
                                        error!(error = ?send_err, "failed to notify force snapshot, because receive end has closed channel");
                                    }
                                }

                                // Update table maintenance operation status.
                                if table_handler_state.table_maintenance_process_status
                                    == MaintenanceProcessStatus::InPersist
                                {
                                    table_handler_state.table_maintenance_process_status =
                                        MaintenanceProcessStatus::Unrequested;
                                    // Table maintenance could come from table internal events, which doesn't have notification receiver.
                                    let _ = table_handler_state
                                        .table_maintenance_completion_tx
                                        .send(Err(e));
                                }

                                // If iceberg snapshot fails, send error back to all broadcast subscribers and unset force snapshot requests.
                                table_handler_state.largest_force_snapshot_lsn = None;

                                if let Some(reason) = errored_reason {
                                    if Self::process_errored_table_event(
                                        TableEvent::EnterErrored { reason },
                                        &mut table,
                                        &mut table_handler_state,
                                        &mut errored_drop,
                                        &table_handler_event_sender,
                                    )
                                    .await
                                    {
                                        Self::drop_errored_table(&mut table, event_sync_sender)
                                            .await;
                                        return;
                                    }
                                    continue;
                                }
                            }
                        }

                        // Drop table if requested, and table at a clean state.
                        if table_handler_state.special_table_state == SpecialTableState::DropTable
                            && table_handler_state.can_drop_table_now(table.has_ongoing_flush())
                        {
                            drop_table(&mut table, event_sync_sender).await;
                            return;
                        }
                    }
                    TableEvent::IndexMergeResult { index_merge_result } => {
                        table_handler_state
                            .mark_index_merge_completed(&index_merge_result)
                            .await;
                        match index_merge_result {
                            Ok(index_merge_result) => {
                                table.record_index_merge_completion(&index_merge_result);
                                table.set_file_indices_merge_res(index_merge_result);
                                // Check whether need to drop table.
                                if table_handler_state.special_table_state
                                    == SpecialTableState::DropTable
                                    && table_handler_state
                                        .can_drop_table_now(table.has_ongoing_flush())
                                {
                                    drop_table(&mut table, event_sync_sender).await;
                                    return;
                                }
                            }
                            Err(err) => {
                                error!(error = ?err, "failed to perform index merge");
                                Self::log_if_background_maintenance_stopped(
                                    &table_handler_state,
                                    &table,
                                );
                                table.on_index_merge_failed();
                            }
                        }
                    }
                    TableEvent::DataCompactionResult {
                        data_compaction_result,
                    } => {
                        table_handler_state
                            .mark_data_compaction_completed(&data_compaction_result)
                            .await;
                        match data_compaction_result {
                            Ok(data_compaction_res) => {
                                table.record_data_compaction_completion(&data_compaction_res);
                                table.set_data_compaction_res(data_compaction_res)
                            }
                            Err(err) => {
                                // TODO(hjiang): Need to record failed compaction result back to snapshot, so committed deletion logs could be pruned.
                                error!(error = ?err, "failed to perform compaction");
                                Self::log_if_background_maintenance_stopped(
                                    &table_handler_state,
                                    &table,
                                );
                                table.on_data_compaction_failed();
                            }
                        }
                        // Check whether need to drop table.
                        if table_handler_state.special_table_state == SpecialTableState::DropTable
                            && table_handler_state.can_drop_table_now(table.has_ongoing_flush())
                        {
                            drop_table(&mut table, event_sync_sender).await;
                            return;
                        }
                    }
                    TableEvent::EvictedFilesToDelete { evicted_files } => {
                        start_task_to_delete_evicted(evicted_files.files);
                    }
                    TableEvent::FlushResult {
                        event_id,
                        xact_id,
                        flush_result,
                    } => match flush_result {
                        Some(Ok(disk_slice)) => {
                            let rows_persisted = disk_slice.output_files().len();

                            if let Some(xact_id) = xact_id {
                                table.apply_stream_flush_result(xact_id, disk_slice, event_id);
                            } else {
                                table.apply_flush_result(disk_slice, event_id);
                            }

                            // Handle a special case: if there're no rows persisted in the flush operation (i.e., in a streaming transaction, all appended rows get deleted), mooncake and iceberg snapshot won't get created.
                            // In case of pending force snapshot requests, we should force iceberg snapshot even if the payload to persist is empty, so snapshot requests never get blocked.
                            if rows_persisted == 0
                                && table_handler_state.has_pending_force_snapshot_request()
                            {
                                table.force_empty_persistence_payload();
                            }
                        }
                        Some(Err(e)) => {
                            // The flush task already retried; the rows cannot be made durable.
                            error!(error = ?e, table = table.mooncake_table_id(), "failed to flush disk slice");
                            if Self::process_errored_table_event(
                                TableEvent::EnterErrored {
                                    reason: format!("writing table data to disk failed: {e}"),
                                },
                                &mut table,
                                &mut table_handler_state,
                                &mut errored_drop,
                                &table_handler_event_sender,
                            )
                            .await
                            {
                                Self::drop_errored_table(&mut table, event_sync_sender).await;
                                return;
                            }
                        }
                        None => {
                            debug!("flush result is none");
                        }
                    },
                    // ==============================
                    // Replication events
                    // ==============================
                    //
                    #[cfg(test)]
                    TableEvent::PanicForTest => {
                        panic!("event loop panic injected by test");
                    }
                    _ => {
                        unreachable!("unexpected event: {:?}", event);
                    }
                }
            }
        }
        // If all senders have been dropped, exit the loop
        if let Err(e) = table.shutdown().await {
            error!(error = %e, "failed to shutdown table");
        }
        debug!("all event senders dropped, shutting down table handler");
    }

    /// Enter the errored state; returns whether a DROP was already pending (None if the table
    /// was already errored).
    fn enter_errored(
        table_handler_state: &mut TableHandlerState,
        table: &MooncakeTable,
        reason: String,
    ) -> Option<bool> {
        error!(
            table = table.mooncake_table_id(),
            reason = %reason,
            "mirror table entered the errored state; it must be dropped and re-created"
        );
        table.cancel_flushes_waiting_for_disk_space();
        table_handler_state.enter_errored(table.mooncake_table_id(), reason)
    }

    fn errored_message(table_handler_state: &TableHandlerState, table: &MooncakeTable) -> String {
        match &table_handler_state.special_table_state {
            SpecialTableState::Errored { reason } => {
                crate::Error::errored_table_message(table.mooncake_table_id(), reason)
            }
            _ => unreachable!("table is not errored"),
        }
    }

    /// Handle one event of an errored table, or the event that makes it errored. Nothing is
    /// applied to the table any more: waiters get the errored message, and results of background
    /// tasks that were in flight only release what they hold. Returns whether the table must be
    /// dropped now.
    async fn process_errored_table_event(
        event: TableEvent,
        table: &mut MooncakeTable,
        table_handler_state: &mut TableHandlerState,
        errored_drop: &mut ErroredDrop,
        table_handler_event_sender: &Sender<TableEvent>,
    ) -> bool {
        match event {
            TableEvent::EnterErrored { reason } => {
                if Self::enter_errored(table_handler_state, table, reason) == Some(true) {
                    Self::request_errored_drop(errored_drop, table_handler_event_sender);
                }
            }
            // The failed task stopped counting as in flight when it panicked.
            TableEvent::BackgroundTaskFailed { task, message } => {
                let reason = format!("internal error in {task}: {message}");
                if Self::enter_errored(table_handler_state, table, reason) == Some(true) {
                    Self::request_errored_drop(errored_drop, table_handler_event_sender);
                }
            }
            TableEvent::DropTable => {
                Self::request_errored_drop(errored_drop, table_handler_event_sender);
            }
            TableEvent::ErroredDropTimeout => {
                if errored_drop.pending {
                    warn!(
                        table = table.mooncake_table_id(),
                        in_flight = table.in_flight_background_tasks(),
                        "dropping errored table while background tasks are still running"
                    );
                    return true;
                }
            }
            TableEvent::ForceSnapshot { .. } => {
                let message = Self::errored_message(table_handler_state, table);
                let _ = table_handler_state
                    .force_snapshot_completion_tx
                    .send(Some(Err(crate::Error::table_errored(&message))));
            }
            TableEvent::ForceRegularIndexMerge
            | TableEvent::ForceRegularDataCompaction
            | TableEvent::ForceFullMaintenance => {
                let message = Self::errored_message(table_handler_state, table);
                let _ = table_handler_state
                    .table_maintenance_completion_tx
                    .send(Err(crate::Error::table_errored(&message)));
            }
            TableEvent::MooncakeTableSnapshotResult {
                mut mooncake_snapshot_result,
            } => {
                table_handler_state.mooncake_snapshot_ongoing = false;
                let mut evicted =
                    std::mem::take(&mut mooncake_snapshot_result.evicted_data_files_to_delete);
                if let Some(payload) = mooncake_snapshot_result
                    .data_compaction_payload
                    .take_payload()
                {
                    evicted.extend(payload.unpin_referenced_compaction_payload().await);
                }
                Self::delete_evicted_files(evicted);
            }
            TableEvent::IndexMergeResult { .. } => {
                table_handler_state.table_maintenance_process_status =
                    MaintenanceProcessStatus::Unrequested;
            }
            TableEvent::DataCompactionResult {
                data_compaction_result,
            } => {
                table_handler_state.table_maintenance_process_status =
                    MaintenanceProcessStatus::Unrequested;
                if let Ok(res) = data_compaction_result {
                    Self::delete_evicted_files(res.evicted_files_to_delete);
                }
            }
            TableEvent::PersistenceSnapshotResult {
                persistence_snapshot_result,
            } => {
                table_handler_state.persistence_snapshot_ongoing = false;
                match persistence_snapshot_result {
                    Ok(mut res) => {
                        if let Some(manager) = res.table_manager.take() {
                            table.restore_table_manager(manager);
                        }
                        Self::delete_evicted_files(std::mem::take(
                            &mut res.evicted_files_to_delete,
                        ));
                    }
                    Err(_) => {
                        table.on_persistence_failed();
                    }
                }
            }
            TableEvent::EvictedFilesToDelete { evicted_files } => {
                Self::delete_evicted_files(evicted_files.files);
            }
            // CDC events, periodic events and flush results are dropped: nothing is applied to an
            // errored table any more. Unapplied flush output stays in the table directory, which
            // DROP deletes.
            _ => {}
        }
        // A background task still running could write into the warehouse after the drop (which a
        // re-created mirror with the same name would then load) or fail on files deleted under it.
        errored_drop.pending && table.in_flight_background_tasks() == 0
    }

    fn request_errored_drop(
        errored_drop: &mut ErroredDrop,
        table_handler_event_sender: &Sender<TableEvent>,
    ) {
        if errored_drop.pending {
            return;
        }
        errored_drop.pending = true;
        let sender = table_handler_event_sender.clone();
        let wait = errored_drop.background_task_wait;
        tokio::spawn(async move {
            tokio::time::sleep(wait).await;
            let _ = sender.send(TableEvent::ErroredDropTimeout).await;
        });
    }

    fn log_if_background_maintenance_stopped(
        table_handler_state: &TableHandlerState,
        table: &MooncakeTable,
    ) {
        if table_handler_state.consecutive_maintenance_failures
            == table_handler_state::MAX_CONSECUTIVE_BACKGROUND_MAINTENANCE_FAILURES
        {
            error!(
                table = table.mooncake_table_id(),
                failures = table_handler_state.consecutive_maintenance_failures,
                "background compaction and index merge stopped after repeated failures; explicit optimize requests still run them"
            );
        }
    }

    /// Best-effort deletion of evicted cache files in a detached task.
    fn delete_evicted_files(files: Vec<String>) {
        if files.is_empty() {
            return;
        }
        tokio::task::spawn(async move {
            if let Err(err) = io_utils::delete_local_files(&files).await {
                error!(error = ?err, "failed to delete evicted cache files of errored table");
            }
        });
    }

    /// Best-effort cleanup of an errored table; always acknowledges the drop so users can
    /// re-create the mirror. Failures are logged, except an iceberg drop failure, which would let a
    /// re-created mirror load stale data and is therefore reported.
    async fn drop_errored_table(table: &mut MooncakeTable, event_sync_sender: EventSyncSender) {
        if let Err(e) = table.shutdown().await {
            error!(error = ?e, table = table.mooncake_table_id(), "failed to release caches of errored table");
        }
        let mut dropped = table.drop_iceberg_table().await;
        if let Err(e) = &dropped {
            warn!(error = ?e, table = table.mooncake_table_id(), "failed to drop iceberg table of errored table; retrying once");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            dropped = table.drop_iceberg_table().await;
        }
        if let Err(e) = dropped {
            error!(error = ?e, table = table.mooncake_table_id(), "failed to drop iceberg table of errored table");
            let _ = event_sync_sender.drop_table_completion_tx.send(Err(e));
            return;
        }
        if let Err(e) = table.drop_mooncake_table().await {
            error!(error = ?e, table = table.mooncake_table_id(), "failed to delete local files of errored table");
        }
        let _ = event_sync_sender.drop_table_completion_tx.send(Ok(()));
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

        // If the table is in a blocking state, buffer the event.
        if table_handler_state.is_in_blocking_state() {
            table_handler_state.initial_copy_buffered_events.push(event);
            return;
        }
        if table_handler_state.skip_resent_event(&event) {
            // Streamed rows of a replayed transaction were collected again; drop them.
            if let TableEvent::Commit {
                xact_id: Some(xact_id),
                ..
            } = event
            {
                if table.has_transaction_stream(xact_id) {
                    table.abort_in_stream_batch(xact_id);
                }
            }
            return;
        }

        // Discard events that we have already processed (persisted, or in the initial copy). They
        // must not move the table's LSNs either: an empty copy claims no commit LSN, and a dropped
        // commit would otherwise become one that no snapshot ever reaches.
        if table_handler_state.should_discard_event(&event) {
            return;
        }

        // Don't update the lsn if the event is not processed yet.
        table_handler_state.update_table_lsns(&event);

        match event {
            TableEvent::Append { row, xact_id, .. } => {
                let result = match xact_id {
                    Some(xact_id) => {
                        let res = table.append_in_stream_batch(row, xact_id);
                        if table.should_transaction_flush(xact_id) {
                            let event_id = uuid::Uuid::new_v4();
                            if let Err(e) =
                                table.flush_stream(xact_id, /*lsn=*/ None, event_id)
                            {
                                error!(error = %e, "failed to flush stream");
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
            TableEvent::Delete {
                row,
                lsn,
                xact_id,
                delete_if_exists,
                ..
            } => {
                match xact_id {
                    Some(xact_id) => {
                        assert!(!delete_if_exists);
                        table.delete_in_stream_batch(row, xact_id).await;
                    }
                    None => {
                        if delete_if_exists {
                            table.delete_if_exists(row, lsn).await;
                        } else {
                            table.delete(row, lsn).await;
                        }
                    }
                };
            }
            TableEvent::Commit { lsn, xact_id, .. } => {
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
            TableEvent::CommitFlush { lsn, xact_id, .. } => {
                Self::commit_and_attempt_flush(
                    lsn,
                    xact_id,
                    table_handler_state,
                    table,
                    /*force_flush_requested=*/ true,
                )
                .await;
            }
            TableEvent::StreamFlush { xact_id, .. } => {
                let event_id = uuid::Uuid::new_v4();
                if let Err(e) = table.flush_stream(xact_id, /*lsn=*/ None, event_id) {
                    error!(error = %e, "failed to flush stream");
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

    /// A commit below the table's commit LSN belongs to a transaction re-sent after the table
    /// applied it, which the replay floors should have skipped. Applying it could duplicate rows,
    /// so the mirror is errored instead; returns whether it was.
    fn error_if_commit_behind_table(
        lsn: u64,
        table_handler_state: &mut TableHandlerState,
        table: &MooncakeTable,
    ) -> bool {
        let table_commit_lsn = table.get_commit_lsn_baseline();
        if lsn >= table_commit_lsn {
            return false;
        }
        Self::enter_errored(
            table_handler_state,
            table,
            format!(
                "a transaction committed at LSN {lsn} arrived after the mirror had applied changes up to LSN {table_commit_lsn}"
            ),
        );
        true
    }

    async fn commit_and_attempt_flush(
        lsn: u64,
        xact_id: Option<u32>,
        table_handler_state: &mut TableHandlerState,
        table: &mut MooncakeTable,
        force_flush_requested: bool,
    ) {
        // Force create a flush if
        // 1. force snapshot is requested
        // 2. previous flush LSN is not enough to satisfy force snapshot request
        // 3. current commit LSN is enough to satisfy force snapshot request

        let last_flush_lsn = table.get_last_flush_lsn();
        let should_force_flush = table_handler_state.should_force_flush(lsn, last_flush_lsn);

        match xact_id {
            Some(xact_id) => {
                // Attempt to flush all preceding unflushed committed non-streaming writes.
                if let Some(last_unflushed_commit_lsn) =
                    table_handler_state.last_unflushed_commit_lsn
                {
                    let event_id = uuid::Uuid::new_v4();
                    if let Err(e) = table.flush(last_unflushed_commit_lsn, event_id) {
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
                if Self::error_if_commit_behind_table(lsn, table_handler_state, table) {
                    table.abort_in_stream_batch(xact_id);
                    return;
                }
                let event_id = uuid::Uuid::new_v4();
                if let Err(e) = table.commit_transaction_stream(xact_id, lsn, event_id) {
                    error!(error = %e, "stream commit flush failed");
                }
            }
            None => {
                if Self::error_if_commit_behind_table(lsn, table_handler_state, table) {
                    return;
                }
                table.commit(lsn);
                if table.should_flush() || should_force_flush || force_flush_requested {
                    table_handler_state.last_unflushed_commit_lsn = None;
                    let event_id = uuid::Uuid::new_v4();
                    if let Err(e) = table.flush(lsn, event_id) {
                        error!(error = %e, "flush failed in commit");
                    }
                }
            }
        }
    }
}

/// The flush LSN feeds the replication slot acknowledgement, so it only moves forward.
fn advance_flush_lsn(flush_lsn_tx: &watch::Sender<u64>, lsn: u64) {
    flush_lsn_tx.send_if_modified(|current| {
        if lsn > *current {
            *current = lsn;
            true
        } else {
            false
        }
    });
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod failure_tests;

#[cfg(test)]
#[cfg(feature = "chaos-test")]
mod chaos_table_metadata;

#[cfg(test)]
#[cfg(feature = "chaos-test")]
mod chaos_test;

#[cfg(test)]
#[cfg(feature = "chaos-test")]
mod chaos_replay;

#[cfg(test)]
#[cfg(feature = "chaos-test")]
mod regression;

#[cfg(feature = "profile-test")]
pub mod profile_test;

#[cfg(test)]
mod errored_state_tests;

#[cfg(test)]
mod errored_drop_tests;

#[cfg(test)]
mod maintenance_failure_tests;

#[cfg(test)]
mod persistence_failure_tests;

/// Serves a table whose event loop panicked: the table is errored, requests fail, DROP works.
struct EventLoopPanicFallback {
    dropper: TableDropper,
    table_name: String,
    drop_completion_tx: crate::event_sync::DropCompletionSender,
    force_snapshot_completion_tx: watch::Sender<Option<crate::Result<u64>>>,
    table_maintenance_completion_tx: tokio::sync::broadcast::Sender<crate::Result<()>>,
    table_status_tx: watch::Sender<crate::event_sync::TableStatus>,
}

impl EventLoopPanicFallback {
    async fn run(self, event_receiver: &mut Receiver<TableEvent>, panic_message: String) {
        let reason = format!("internal error in table event loop: {panic_message}");
        error!(
            table = self.table_name,
            reason, "mirror table entered the errored state; it must be dropped and re-created"
        );
        let message = crate::Error::errored_table_message(&self.table_name, &reason);
        let _ = self
            .table_status_tx
            .send(crate::event_sync::TableStatus::Errored(message.clone()));
        let _ = self
            .force_snapshot_completion_tx
            .send(Some(Err(crate::Error::table_errored(&message))));
        let _ = self
            .table_maintenance_completion_tx
            .send(Err(crate::Error::table_errored(&message)));
        while let Some(event) = event_receiver.recv().await {
            match event {
                TableEvent::DropTable => {
                    let result = self
                        .dropper
                        .drop_all(ERRORED_DROP_BACKGROUND_TASK_WAIT)
                        .await;
                    if let Err(e) = &result {
                        error!(error = ?e, table = self.table_name, "failed to drop errored table");
                    }
                    let _ = self.drop_completion_tx.send(result);
                    return;
                }
                TableEvent::ForceSnapshot { .. } => {
                    let _ = self
                        .force_snapshot_completion_tx
                        .send(Some(Err(crate::Error::table_errored(&message))));
                }
                TableEvent::ForceRegularIndexMerge
                | TableEvent::ForceRegularDataCompaction
                | TableEvent::ForceFullMaintenance => {
                    let _ = self
                        .table_maintenance_completion_tx
                        .send(Err(crate::Error::table_errored(&message)));
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod supervision_tests;
