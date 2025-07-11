use crate::storage::mooncake_table::SnapshotOption;
use crate::storage::mooncake_table::INITIAL_COPY_XACT_ID;
use crate::storage::{io_utils, MooncakeTable};
use crate::table_notify::TableEvent;
use crate::{Error, Result};
use std::collections::BTreeMap;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};
use tracing::Instrument;
use tracing::{debug, error, info_span};

/// Handler for table operations
pub struct TableHandler {
    /// Handle to periodical events.
    _periodic_event_handle: JoinHandle<()>,

    /// Handle to the event processing task
    _event_handle: Option<JoinHandle<()>>,

    /// Sender for the table event queue
    event_sender: Sender<TableEvent>,
}

/// Contains a few senders, which notifies after certain iceberg events completion.
pub struct EventSyncSender {
    /// Notifies when drop table completes.
    pub drop_table_completion_tx: oneshot::Sender<Result<()>>,
    /// Notifies when iceberg flush LSN advances.
    pub flush_lsn_tx: watch::Sender<u64>,
}

#[derive(PartialEq)]
pub enum SpecialTableState {
    Normal,
    InitialCopy,
    _AlterTable,
    DropTable,
}

pub struct TableHandlerState {
    // cached table states
    //
    // Initial persisted LSN.
    // On moonlink recovery, it's possible that moonlink hasn't sent back latest flush LSN back to source table, so source database (i.e. postgres) will replay unacknowledged parts, which might contain already persisted content.
    // To avoid duplicate records, we compare iceberg initial flush LSN with new coming messages' LSN.
    // - For streaming events, we keep a buffer as usual, and decide whether to keep or discard the buffer at stream commit;
    // - For non-streaming events, they start with a [`Begin`] message containing the final LSN of the current transaction, which we could leverage to decide keep or not.
    initial_persistence_lsn: Option<u64>,
    // Record LSN if the last handled table event is committed, which indicates mooncake table stays at a consistent view, so table could be flushed safely.
    table_consistent_view_lsn: Option<u64>,
    // Latest LSN of the table's latest commit.
    latest_commit_lsn: Option<u64>,

    // table management and event handling states
    //
    // Whether iceberg snapshot result has been consumed by the latest mooncake snapshot, when creating a mooncake snapshot.
    //
    // There're three possible states for an iceberg snapshot:
    // - snapshot ongoing = false, result consumed = true: no active iceberg snapshot
    // - snapshot ongoing = true, result consumed = true: iceberg snapshot is ongoing
    // - snapshot ongoing = false, result consumed = false: iceberg snapshot completes, but wait for mooncake snapshot to consume the result
    //
    iceberg_snapshot_result_consumed: bool,
    iceberg_snapshot_ongoing: bool,
    // Whether there's an ongoing background maintenance operation, for example, index merge, data compaction, etc.
    // To simplify state management, we have at most one ongoing maintainance operation at the same time.
    maintainance_ongoing: bool,
    // Whether there's an ongoing mooncake snapshot operation.
    mooncake_snapshot_ongoing: bool,
    // Pending force snapshot requests.
    pending_force_snapshot_lsns: BTreeMap<u64, Vec<Option<Sender<Result<()>>>>>,
    // Special table state, for example, initial copy, alter table, drop table, etc.
    special_table_state: SpecialTableState,
    // Buffered events during blocking operations: initial copy, alter table, drop table, etc.
    initial_copy_buffered_events: Vec<TableEvent>,
}

impl TableHandlerState {
    pub fn new() -> Self {
        Self {
            iceberg_snapshot_result_consumed: true,
            iceberg_snapshot_ongoing: false,
            maintainance_ongoing: false,
            mooncake_snapshot_ongoing: false,
            initial_persistence_lsn: None,
            table_consistent_view_lsn: None,
            latest_commit_lsn: None,
            pending_force_snapshot_lsns: BTreeMap::new(),
            special_table_state: SpecialTableState::Normal,
            initial_copy_buffered_events: Vec::new(),
        }
    }

    pub fn update_table_lsns(&mut self, event: &TableEvent) {
        if event.is_ingest_event() {
            match event {
                TableEvent::Commit { lsn, .. } => {
                    self.latest_commit_lsn = Some(*lsn);
                    self.table_consistent_view_lsn = Some(*lsn);
                }
                _ => {
                    self.table_consistent_view_lsn = None;
                }
            }
        }
    }

    // Used to decide whether we could create an iceberg snapshot.
    // The completion of an iceberg snapshot is **NOT** marked as the finish of snapshot thread, but the handling of its results.
    // We can only create a new iceberg snapshot when (1) there's no ongoing iceberg snapshot; and (2) previous snapshot results have been acknowledged.
    //
    pub fn can_initiate_iceberg_snapshot(&self) -> bool {
        self.iceberg_snapshot_result_consumed && !self.iceberg_snapshot_ongoing
    }

    pub fn reset_iceberg_state_at_mooncake_snapshot(&mut self) {
        // Validate iceberg snapshot state before mooncake snapshot creation.
        //
        // Assertion on impossible state.
        assert!(!self.iceberg_snapshot_ongoing || self.iceberg_snapshot_result_consumed);

        // If there's pending iceberg snapshot result unconsumed, the following mooncake snapshot will properly handle it.
        if !self.iceberg_snapshot_result_consumed {
            self.iceberg_snapshot_result_consumed = true;
            self.iceberg_snapshot_ongoing = false;
        }
    }

    pub fn force_snapshot_requested(&self, cur_lsn: u64) -> bool {
        !self.pending_force_snapshot_lsns.is_empty()
            && cur_lsn
                >= *self
                    .pending_force_snapshot_lsns
                    .iter()
                    .next()
                    .as_ref()
                    .unwrap()
                    .0
            && !self.mooncake_snapshot_ongoing
    }

    pub fn should_discard_event(&self, event: &TableEvent) -> bool {
        if self.initial_persistence_lsn.is_none() {
            return false;
        }
        let initial_persistence_lsn = self.initial_persistence_lsn.unwrap();
        if let Some(lsn) = event.get_lsn_for_ingest_event() {
            lsn <= initial_persistence_lsn
        } else {
            false
        }
    }

    pub fn is_in_blocking_state(&self) -> bool {
        self.special_table_state != SpecialTableState::Normal
    }

    /// Enter initial copy mode. Subsequent CDC events will be
    /// buffered in a dedicated streaming memslice until
    /// `finish_initial_copy` is called.
    /// In this case of a streaming transaction, we simply use the already provided `xact_id` to identify the transaction. In the case of non-streaming, we use `INITIAL_COPY_XACT_ID` to identify the transaction.
    /// All commits are buffered and deferred until initial copy finishes.
    pub fn start_initial_copy(&mut self) {
        self.special_table_state = SpecialTableState::InitialCopy;
    }

    pub fn finish_initial_copy(&mut self) {
        self.special_table_state = SpecialTableState::Normal;
    }

    pub fn start_drop_table(&mut self) {
        self.special_table_state = SpecialTableState::DropTable;
    }
}

impl TableHandler {
    /// Create a new TableHandler for the given schema and table name
    pub async fn new(
        mut table: MooncakeTable,
        event_sync_sender: EventSyncSender,
        replication_lsn_rx: watch::Receiver<u64>,
    ) -> Self {
        // Create channel for events
        let (event_sender, event_receiver) = mpsc::channel(100);

        // Create channel for internal control events.
        table.register_table_notify(event_sender.clone()).await;

        // Spawn the task to notify periodical events.
        let event_sender_for_periodical_snapshot = event_sender.clone();
        let event_sender_for_periodical_force_snapshot = event_sender.clone();
        let periodic_event_handle = tokio::spawn(async move {
            let mut periodic_snapshot_interval = time::interval(Duration::from_millis(500));
            let mut periodic_force_snapshot_interval = time::interval(Duration::from_secs(300));

            loop {
                tokio::select! {
                    // Sending to channel fails only happens when eventloop exits, directly exit timer events.
                    _ = periodic_snapshot_interval.tick() => {
                        if event_sender_for_periodical_snapshot.send(TableEvent::PeriodicalMooncakeTableSnapshot).await.is_err() {
                           return;
                        }
                    }
                    _ = periodic_force_snapshot_interval.tick() => {
                        if event_sender_for_periodical_force_snapshot.send(TableEvent::ForceSnapshot { lsn: None, tx: None }).await.is_err() {
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
                Self::event_loop(event_sync_sender, event_receiver, replication_lsn_rx, table)
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
        event_sync_sender: EventSyncSender,
        mut event_receiver: Receiver<TableEvent>,
        replication_lsn_rx: watch::Receiver<u64>,
        mut table: MooncakeTable,
    ) {
        let mut table_handler_state = TableHandlerState::new();
        table_handler_state.initial_persistence_lsn = table.get_iceberg_snapshot_lsn();
        // Used to clean up mooncake table status, and send completion notification.
        let drop_table = async |table: &mut MooncakeTable, event_sync_sender: EventSyncSender| {
            // Step-1: shutdown the table, which unreferences and deletes all cache files.
            if let Err(e) = table.shutdown().await {
                event_sync_sender
                    .drop_table_completion_tx
                    .send(Err(e))
                    .unwrap();
                return;
            }

            // Step-2: delete the iceberg table.
            if let Err(e) = table.drop_iceberg_table().await {
                event_sync_sender
                    .drop_table_completion_tx
                    .send(Err(e))
                    .unwrap();
                return;
            }

            // Step-3: delete the mooncake table.
            if let Err(e) = table.drop_mooncake_table().await {
                event_sync_sender
                    .drop_table_completion_tx
                    .send(Err(e))
                    .unwrap();
                return;
            }

            // Step-4: send back completion notification.
            event_sync_sender
                .drop_table_completion_tx
                .send(Ok(()))
                .unwrap();
        };

        // Util function to spawn a detached task to delete evicted data files.
        let start_task_to_delete_evicted = |evicted_file_to_delete: Vec<String>| {
            if evicted_file_to_delete.is_empty() {
                return;
            }
            tokio::task::spawn(async move {
                if let Err(err) = io_utils::delete_local_files(evicted_file_to_delete).await {
                    error!("Failed to delete object storage cache: {:?}", err);
                }
            });
        };

        // Process events until the receiver is closed or a Shutdown event is received
        loop {
            tokio::select! {
                // Process events from the queue
                Some(event) = event_receiver.recv() => {
                    table_handler_state.update_table_lsns(&event);

                    match event {
                        event if event.is_ingest_event() => {
                            Self::process_cdc_table_event(event, &mut table, &mut table_handler_state).await;
                        }
                        TableEvent::Shutdown => {
                            if let Err(e) = table.shutdown().await {
                                error!(error = %e, "failed to shutdown table");
                            }
                            debug!("shutting down table handler");
                            break;
                        }
                        // ==============================
                        // Interactive blocking events
                        // ==============================
                        //
                        TableEvent::ForceSnapshot { lsn, tx } => {
                            let requested_lsn = if lsn.is_some() {
                                lsn
                            } else if table_handler_state.latest_commit_lsn.is_some() {
                                table_handler_state.latest_commit_lsn
                            } else {
                                None
                            };

                            // Fast-path: nothing to snapshot.
                            if requested_lsn.is_none() {
                                if let Some(tx) = tx {
                                    tx.send(Ok(())).await.unwrap();
                                }
                                continue;
                            }

                            // Fast-path: if iceberg snapshot requirement is already satisfied, notify directly.
                            let requested_lsn = requested_lsn.unwrap();
                            let last_iceberg_snapshot_lsn = table.get_iceberg_snapshot_lsn();
                            let replication_lsn = *replication_lsn_rx.borrow();
                            if table_handler_state.is_iceberg_snapshot_satisfy_force_snapshot(
                                requested_lsn,
                                last_iceberg_snapshot_lsn,
                                replication_lsn
                            ) {
                                if let Some(tx) = tx {
                                    tx.send(Ok(())).await.unwrap();
                                }
                                continue;
                            }

                            // Iceberg snapshot LSN requirement is not met, record the required LSN, so later commit will pick up.
                            else {
                                table_handler_state.pending_force_snapshot_lsns.entry(requested_lsn).or_default().push(tx);
                            }
                        }
                        // Branch to drop the iceberg table and clear pinned data files from the global object storage cache, only used when the whole table requested to drop.
                        // So we block wait for asynchronous request completion.
                        TableEvent::DropTable => {
                            // Fast-path: no other concurrent events, directly clean up states and ack back.
                            if !table_handler_state.mooncake_snapshot_ongoing && !table_handler_state.iceberg_snapshot_ongoing {
                                drop_table(&mut table, event_sync_sender).await;
                                return;
                            }

                            // Otherwise, leave a drop marker to clean up states later.
                            table_handler_state.start_drop_table();
                        }
                        TableEvent::AlterTable { column_to_drop } => {
                            debug!("altering table, dropping columns: {:?}", column_to_drop);
                        }
                        TableEvent::StartInitialCopy => {
                            debug!("starting initial copy");
                            table_handler_state.start_initial_copy();
                        }
                        TableEvent::FinishInitialCopy => {
                            debug!("finishing initial copy");
                            if let Err(e) = table.commit_transaction_stream(INITIAL_COPY_XACT_ID, 0).await {
                                error!(error = %e, "failed to finish initial copy");
                            }
                            // Force create the snapshot with LSN 0
                            assert!(table.create_snapshot(SnapshotOption {
                                force_create: true,
                                skip_iceberg_snapshot: true,
                                skip_file_indices_merge: true,
                                skip_data_file_compaction: true,
                            }));
                            table_handler_state.mooncake_snapshot_ongoing = true;
                            table_handler_state.finish_initial_copy();

                            // Apply the buffered events.
                            let buffered_events = table_handler_state.initial_copy_buffered_events.drain(..).collect::<Vec<_>>();
                            for event in buffered_events {
                                Self::process_cdc_table_event(event, &mut table, &mut table_handler_state).await;
                            }
                        }
                        // ==============================
                        // Table internal events
                        // ==============================
                        //
                        TableEvent::PeriodicalMooncakeTableSnapshot => {
                            // Only create a periodic snapshot if there isn't already one in progress
                            if table_handler_state.mooncake_snapshot_ongoing {
                                continue;
                            }

                            // Check whether a flush and force snapshot is needed.
                            if !table_handler_state.pending_force_snapshot_lsns.is_empty() {
                                if let Some(commit_lsn) = table_handler_state.table_consistent_view_lsn {
                                    table.flush(commit_lsn).await.unwrap();
                                    table_handler_state.reset_iceberg_state_at_mooncake_snapshot();
                                    assert!(table.create_snapshot(SnapshotOption {
                                        force_create: true,
                                        skip_iceberg_snapshot: table_handler_state.iceberg_snapshot_ongoing,
                                        skip_file_indices_merge: table_handler_state.maintainance_ongoing,
                                        skip_data_file_compaction: table_handler_state.maintainance_ongoing,
                                    }));
                                    table_handler_state.mooncake_snapshot_ongoing = true;
                                    continue;
                                }
                            }

                            // Fallback to normal periodic snapshot.
                            table_handler_state.reset_iceberg_state_at_mooncake_snapshot();
                            table_handler_state.mooncake_snapshot_ongoing = table.create_snapshot(SnapshotOption {
                                force_create: false,
                                skip_iceberg_snapshot: table_handler_state.iceberg_snapshot_ongoing,
                                skip_file_indices_merge: table_handler_state.maintainance_ongoing,
                                skip_data_file_compaction:  table_handler_state.maintainance_ongoing,
                            });
                        }
                        TableEvent::MooncakeTableSnapshotResult { lsn, iceberg_snapshot_payload, data_compaction_payload, file_indice_merge_payload, evicted_data_files_to_delete } => {
                            // Spawn a detached best-effort task to delete evicted object storage cache.
                            start_task_to_delete_evicted(evicted_data_files_to_delete);

                            // Mark mooncake snapshot as completed.
                            table.mark_mooncake_snapshot_completed();

                            // Drop table if requested, and table at a clean state.
                            if table_handler_state.special_table_state == SpecialTableState::DropTable && !table_handler_state.iceberg_snapshot_ongoing {
                                drop_table(&mut table, event_sync_sender).await;
                                return;
                            }

                            // Notify read the mooncake table commit of LSN.
                            table.notify_snapshot_reader(lsn);

                            // Process iceberg snapshot and trigger iceberg snapshot if necessary.
                            if table_handler_state.can_initiate_iceberg_snapshot() {
                                if let Some(iceberg_snapshot_payload) = iceberg_snapshot_payload {
                                    table_handler_state.iceberg_snapshot_ongoing = true;
                                    table.persist_iceberg_snapshot(iceberg_snapshot_payload);
                                }
                            }

                            // Attempt to process data compaction.
                            // Unlike snapshot, we can actually have multiple file index merge operations ongoing concurrently,
                            // to simplify workflow we limit at most one ongoing.
                            if !table_handler_state.maintainance_ongoing {
                                if let Some(data_compaction_payload) = data_compaction_payload {
                                    table_handler_state.maintainance_ongoing = true;
                                    table.perform_data_compaction(data_compaction_payload);
                                }
                            }

                            // Attempt to process file indices merge.
                            // Unlike snapshot, we can actually have multiple file index merge operations ongoing concurrently,
                            // to simplify workflow we limit at most one ongoing.
                            if !table_handler_state.maintainance_ongoing {
                                if let Some(file_indice_merge_payload) = file_indice_merge_payload {
                                    table_handler_state.maintainance_ongoing = true;
                                    table.perform_index_merge(file_indice_merge_payload);
                                }
                            }

                            table_handler_state.mooncake_snapshot_ongoing = false;
                        }
                        TableEvent::IcebergSnapshot { iceberg_snapshot_result } => {
                            table_handler_state.iceberg_snapshot_ongoing = false;
                            match iceberg_snapshot_result {
                                Ok(snapshot_res) => {
                                    let iceberg_flush_lsn = snapshot_res.flush_lsn;
                                    event_sync_sender.flush_lsn_tx.send(iceberg_flush_lsn).unwrap();
                                    table.set_iceberg_snapshot_res(snapshot_res);
                                    table_handler_state.iceberg_snapshot_result_consumed = false;

                                    // Notify all waiters with LSN satisfied.
                                    let replication_lsn = *replication_lsn_rx.borrow();
                                    table_handler_state.update_force_iceberg_snapshot_requests(iceberg_flush_lsn, replication_lsn).await;

                                    // mark alter table as completed if needed

                                }
                                Err(e) => {
                                    for (_, tx) in table_handler_state.pending_force_snapshot_lsns.iter() {
                                        for cur_tx in tx {
                                            let err = Error::IcebergMessage(format!("Failed to create iceberg snapshot: {e:?}"));
                                            if let Some(cur_tx) = cur_tx {
                                                cur_tx.send(Err(err)).await.unwrap();
                                            }
                                        }
                                    }
                                    table_handler_state.pending_force_snapshot_lsns.clear();
                                }
                            }

                            // Drop table if requested, and table at a clean state.
                            if table_handler_state.special_table_state == SpecialTableState::DropTable && !table_handler_state.mooncake_snapshot_ongoing {
                                drop_table(&mut table, event_sync_sender).await;
                                return;
                            }
                        }
                        TableEvent::IndexMerge { index_merge_result } => {
                            table.set_file_indices_merge_res(index_merge_result);
                            table_handler_state.maintainance_ongoing = false;
                        }
                        TableEvent::DataCompaction { data_compaction_result } => {
                            match data_compaction_result {
                                Ok(data_compaction_res) => {
                                    table.set_data_compaction_res(data_compaction_res)
                                }
                                Err(err) => {
                                    error!(error = ?err, "failed to perform compaction");
                                }
                            }
                            table_handler_state.maintainance_ongoing = false;
                        }
                        TableEvent::ReadRequest { cache_handles } => {
                            table.set_read_request_res(cache_handles);
                        }
                        TableEvent::EvictedDataFilesToDelete { evicted_data_files } => {
                            start_task_to_delete_evicted(evicted_data_files);
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

    #[allow(clippy::too_many_arguments)]
    async fn process_cdc_table_event(
        event: TableEvent,
        table: &mut MooncakeTable,
        table_handler_state: &mut TableHandlerState,
    ) {
        // ==============================
        // Replication events
        // ==============================
        //

        if table_handler_state.should_discard_event(&event) {
            return;
        }
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
        assert_eq!(is_initial_copy_event, table_handler_state.special_table_state == SpecialTableState::InitialCopy);

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
                            if let Err(e) = table.flush_transaction_stream(xact_id).await {
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
                // Force create snapshot if
                // 1. force snapshot is requested
                // and 2. LSN which meets force snapshot requirement has appeared, before that we still allow buffering
                // and 3. there's no snapshot creation operation ongoing

                let force_snapshot = table_handler_state.force_snapshot_requested(lsn);

                match xact_id {
                    Some(xact_id) => {
                        if let Err(e) = table.commit_transaction_stream(xact_id, lsn).await {
                            error!(error = %e, "stream commit flush failed");
                        }
                    }
                    None => {
                        table.commit(lsn);
                        if table.should_flush() || force_snapshot {
                            if let Err(e) = table.flush(lsn).await {
                                error!(error = %e, "flush failed in commit");
                            }
                        }
                    }
                }

                if force_snapshot {
                    table_handler_state.reset_iceberg_state_at_mooncake_snapshot();
                    assert!(table.create_snapshot(SnapshotOption {
                        force_create: true,
                        skip_iceberg_snapshot: table_handler_state.iceberg_snapshot_ongoing,
                        skip_file_indices_merge: table_handler_state.maintainance_ongoing,
                        skip_data_file_compaction: table_handler_state.maintainance_ongoing,
                    }));
                    table_handler_state.mooncake_snapshot_ongoing = true;
                }
            }
            TableEvent::StreamAbort { xact_id } => {
                table.abort_in_stream_batch(xact_id);
            }
            TableEvent::Flush { lsn } => {
                if let Err(e) = table.flush(lsn).await {
                    error!(error = %e, "explicit flush failed");
                }
            }
            TableEvent::StreamFlush { xact_id } => {
                if let Err(e) = table.flush_transaction_stream(xact_id).await {
                    error!(error = %e, "stream flush failed");
                }
            }
            _ => {
                unreachable!("unexpected event: {:?}", event)
            }
        }
    }
}

impl TableHandlerState {
    /// Decide whether current iceberg snapshot could serve the force iceberg snapshot request.
    pub(crate) fn is_iceberg_snapshot_satisfy_force_snapshot(
        &self,
        requested_lsn: u64,
        iceberg_snapshot_lsn: Option<u64>,
        replication_lsn: u64,
    ) -> bool {
        // Case-1: there're no activities in the current table, but replication LSN already covers requested LSN.
        if iceberg_snapshot_lsn.is_none() && self.table_consistent_view_lsn.is_none() {
            return replication_lsn >= requested_lsn;
        }

        // Case-2: iceberg snapshot LSN already satisfies.
        if let Some(iceberg_snapshot_lsn) = iceberg_snapshot_lsn {
            if iceberg_snapshot_lsn >= requested_lsn {
                return true;
            }
        }

        // Case-3: table's at a consistent state, which has been fully persisted into iceberg;
        // meanwhile, replication LSN has covered the requested LSN.
        if iceberg_snapshot_lsn == self.table_consistent_view_lsn
            && replication_lsn >= requested_lsn
        {
            return true;
        }

        false
    }

    /// Update requested iceberg snapshot LSNs.
    async fn update_force_iceberg_snapshot_requests(
        &mut self,
        iceberg_snapshot_lsn: u64,
        replication_lsn: u64,
    ) {
        let mut updated_requests = BTreeMap::new();

        // TODO(hjiang): Could be optimized, since as long as we found the first requested LSN which doesn't satisfy, we could directly place all left requests to updated lsns.
        for (requested_lsn, senders) in std::mem::take(&mut self.pending_force_snapshot_lsns) {
            if self.is_iceberg_snapshot_satisfy_force_snapshot(
                requested_lsn,
                Some(iceberg_snapshot_lsn),
                replication_lsn,
            ) {
                for cur_sender in senders.into_iter().flatten() {
                    cur_sender.send(Ok(())).await.unwrap();
                }
            } else {
                updated_requests.insert(requested_lsn, senders);
            }
        }

        self.pending_force_snapshot_lsns = updated_requests;
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_utils;
