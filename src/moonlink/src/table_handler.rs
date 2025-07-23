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
use crate::storage::mooncake_table::DataCompactionResult;
use crate::storage::mooncake_table::MaintenanceOption;
use crate::storage::mooncake_table::SnapshotOption;
use crate::storage::mooncake_table::INITIAL_COPY_XACT_ID;
use crate::storage::{io_utils, MooncakeTable};
use crate::table_notify::TableEvent;
use crate::{Error, Result};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;
use tokio::time::Duration;
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

#[derive(PartialEq)]
enum SpecialTableState {
    Normal,
    InitialCopy,
    AlterTable {
        alter_table_lsn: u64,
        alter_table_request: Option<AlterTableRequest>,
    },
    DropTable,
}

#[derive(Clone, Debug, PartialEq)]
enum MaintenanceRequestStatus {
    /// Force Maintenance request is not requested.
    Unrequested,
    /// Force regular Maintenance is requested.
    ForceRegular,
    /// Force full Maintenance is requested.
    ForceFull,
}

#[derive(Clone, Debug, PartialEq)]
enum MaintenanceProcessStatus {
    /// Force maintainence request is not being requested.
    Unrequested,
    /// Force maintainence request is being processed.
    InProcess,
    /// Maintainence result has been put into snapshot buffer, which will be persisted into iceberg later.
    ReadyToPersist,
    /// Maintainence task result is being peristed into iceberg.
    InPersist,
}

struct TableHandlerState {
    // cached table states
    //
    // Initial persisted LSN.
    // On moonlink recovery, it's possible that moonlink hasn't sent back latest flush LSN back to source table, so source database (i.e. postgres) will replay unacknowledged parts, which might contain already persisted content.
    // To avoid duplicate records, we compare iceberg initial flush LSN with new coming messages' LSN.
    // This also is used in the initial copy mode to discard cdc events that may be already included in the initial copy.
    // If we have already seen this LSN, we simply discard the event.
    initial_persistence_lsn: Option<u64>,
    // Record LSN if the last handled table event is a commit operation, which indicates mooncake table stays at a consistent view, so table could be flushed safely.
    table_consistent_view_lsn: Option<u64>,
    // Latest LSN of the table's latest commit.
    latest_commit_lsn: Option<u64>,

    // ================================================
    // Table management and event handling states
    // ================================================
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
    // Whether there's an ongoing background Maintenance operation, for example, index merge, data compaction, etc.
    // To simplify state management, we have at most one ongoing Maintenance operation at the same time.
    maintenance_ongoing: bool,
    // Whether there's an ongoing mooncake snapshot operation.
    mooncake_snapshot_ongoing: bool,
    // Largest pending force snapshot LSN.
    largest_force_snapshot_lsn: Option<u64>,
    // Notify when force snapshot completes.
    force_snapshot_completion_tx: watch::Sender<Option<Result<u64>>>,
    // Special table state, for example, initial copy, alter table, drop table, etc.
    special_table_state: SpecialTableState,
    // Buffered events during blocking operations: initial copy, alter table, drop table, etc.
    initial_copy_buffered_events: Vec<TableEvent>,

    // ================================================
    // Table maintainence status
    // ================================================
    //
    // Assume there's at most one table maintainence operation ongoing.
    //
    // Index merge request status.
    index_merge_request_status: MaintenanceRequestStatus,
    /// Data compaction request status.
    data_compaction_request_status: MaintenanceRequestStatus,
    /// Table maintainance process status.
    table_maintenance_process_status: MaintenanceProcessStatus,
    /// Notify when data compaction completes.
    table_maintenance_completion_tx: broadcast::Sender<Result<()>>,
}

impl TableHandlerState {
    fn new(
        table_maintenance_completion_tx: broadcast::Sender<Result<()>>,
        force_snapshot_completion_tx: watch::Sender<Option<Result<u64>>>,
        initial_persistence_lsn: Option<u64>,
    ) -> Self {
        Self {
            iceberg_snapshot_result_consumed: true,
            iceberg_snapshot_ongoing: false,
            mooncake_snapshot_ongoing: false,
            initial_persistence_lsn,
            latest_commit_lsn: None,
            special_table_state: SpecialTableState::Normal,
            // Force snapshot fields.
            table_consistent_view_lsn: initial_persistence_lsn,
            largest_force_snapshot_lsn: None,
            force_snapshot_completion_tx,
            // Table maintenance fields.
            maintenance_ongoing: false,
            index_merge_request_status: MaintenanceRequestStatus::Unrequested,
            data_compaction_request_status: MaintenanceRequestStatus::Unrequested,
            table_maintenance_process_status: MaintenanceProcessStatus::Unrequested,
            table_maintenance_completion_tx,
            // Initial copy fields.
            initial_copy_buffered_events: Vec::new(),
        }
    }

    fn update_table_lsns(&mut self, event: &TableEvent) {
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

    /// Mark index merge completion.
    async fn mark_index_merge_completed(&mut self) {
        assert!(self.maintenance_ongoing);
        self.maintenance_ongoing = false;
        self.index_merge_request_status = MaintenanceRequestStatus::Unrequested;
        self.table_maintenance_process_status = MaintenanceProcessStatus::ReadyToPersist;
    }

    /// Get Maintenance task operation option.
    fn get_maintenance_task_option(
        &self,
        request_status: &MaintenanceRequestStatus,
    ) -> MaintenanceOption {
        if self.maintenance_ongoing {
            return MaintenanceOption::Skip;
        }
        match request_status {
            MaintenanceRequestStatus::Unrequested => MaintenanceOption::BestEffort,
            MaintenanceRequestStatus::ForceRegular => MaintenanceOption::ForceRegular,
            MaintenanceRequestStatus::ForceFull => MaintenanceOption::ForceFull,
        }
    }
    fn get_index_merge_maintenance_option(&self) -> MaintenanceOption {
        self.get_maintenance_task_option(&self.index_merge_request_status)
    }
    fn get_data_compaction_maintenance_option(&self) -> MaintenanceOption {
        self.get_maintenance_task_option(&self.data_compaction_request_status)
    }

    /// Mark data compaction completion.
    async fn mark_data_compaction_completed(
        &mut self,
        data_compaction_result: &Result<DataCompactionResult>,
    ) {
        assert!(self.maintenance_ongoing);
        self.maintenance_ongoing = false;
        self.data_compaction_request_status = MaintenanceRequestStatus::Unrequested;
        match &data_compaction_result {
            Ok(_) => {
                self.table_maintenance_process_status = MaintenanceProcessStatus::ReadyToPersist;
            }
            Err(err) => {
                self.table_maintenance_process_status = MaintenanceProcessStatus::Unrequested;
                self.table_maintenance_completion_tx
                    .send(Err(err.clone()))
                    .unwrap();
            }
        }
    }

    // Used to decide whether we could create an iceberg snapshot.
    // The completion of an iceberg snapshot is **NOT** marked as the finish of snapshot thread, but the handling of its results.
    // We can only create a new iceberg snapshot when (1) there's no ongoing iceberg snapshot, (2) previous snapshot results have been acknowledged.
    //
    fn can_initiate_iceberg_snapshot(&self) -> bool {
        self.iceberg_snapshot_result_consumed && !self.iceberg_snapshot_ongoing
    }

    fn reset_iceberg_state_at_mooncake_snapshot(&mut self) {
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

    /// Return whether there're pending force snapshot requests.
    fn has_pending_force_snapshot_request(&self) -> bool {
        self.largest_force_snapshot_lsn.is_some()
    }

    /// Return mooncake snapshot option.
    ///
    /// # Arguments
    ///
    /// * request_force: request to force create a mooncake / iceberg snapshot.
    fn get_mooncake_snapshot_option(&self, request_force: bool) -> SnapshotOption {
        let mut force_create = request_force;
        if self.table_maintenance_process_status == MaintenanceProcessStatus::ReadyToPersist {
            force_create = true;
        }
        if self.index_merge_request_status != MaintenanceRequestStatus::Unrequested
            && self.table_maintenance_process_status == MaintenanceProcessStatus::Unrequested
        {
            force_create = true;
        }
        if self.data_compaction_request_status != MaintenanceRequestStatus::Unrequested
            && self.table_maintenance_process_status == MaintenanceProcessStatus::Unrequested
        {
            force_create = true;
        }
        SnapshotOption {
            force_create,
            skip_iceberg_snapshot: self.iceberg_snapshot_ongoing,
            index_merge_option: self.get_index_merge_maintenance_option(),
            data_compaction_option: self.get_data_compaction_maintenance_option(),
        }
    }

    /// Return whether should force to create a mooncake and iceberg snapshot, based on the new coming commit LSN.
    fn should_force_snapshot_by_commit_lsn(&self, commit_lsn: u64) -> bool {
        // Case-1: there're completed but not persisted table maintainence changes.
        if self.table_maintenance_process_status == MaintenanceProcessStatus::ReadyToPersist {
            return true;
        }

        // Case-2: there're pending force snapshot requests.
        if let Some(largest_requested_lsn) = self.largest_force_snapshot_lsn {
            return largest_requested_lsn <= commit_lsn && !self.mooncake_snapshot_ongoing;
        }

        false
    }

    fn should_discard_event(&self, event: &TableEvent) -> bool {
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

    fn is_in_blocking_state(&self) -> bool {
        self.special_table_state != SpecialTableState::Normal
    }

    /// Enter initial copy mode. Subsequent CDC events will be
    /// buffered in `initial_copy_buffered_events` until `finish_initial_copy` is called.
    /// We set `initial_persistence_lsn` to the start LSN to avoid duplicate events that may have already been captured by the initial copy.
    fn start_initial_copy(&mut self) {
        self.special_table_state = SpecialTableState::InitialCopy;
    }

    fn finish_initial_copy(&mut self) {
        assert!(self.special_table_state == SpecialTableState::InitialCopy);
        self.special_table_state = SpecialTableState::Normal;
        self.latest_commit_lsn = Some(0);
        self.table_consistent_view_lsn = Some(0);
    }

    fn should_complete_alter_table(&self, iceberg_snapshot_lsn: u64) -> bool {
        if let SpecialTableState::AlterTable {
            alter_table_lsn, ..
        } = self.special_table_state
        {
            assert!(iceberg_snapshot_lsn <= alter_table_lsn);
            iceberg_snapshot_lsn == alter_table_lsn
        } else {
            false
        }
    }

    fn finish_alter_table(&mut self) {
        assert!(matches!(
            self.special_table_state,
            SpecialTableState::AlterTable { .. }
        ));
        self.special_table_state = SpecialTableState::Normal;
    }

    fn mark_drop_table(&mut self) {
        self.special_table_state = SpecialTableState::DropTable;
    }

    /// Get the largest LSN where all updates have been persisted into iceberg.
    /// The difference between "persisted table LSN" and "iceberg snapshot LSN" is, suppose we have two tables, table A has persisted all changes to iceberg with flush LSN-1;
    /// if there're no further updates to the table A, meanwhile there're updates to table B with LSN-2, flush LSN-1 actually represents a consistent view of LSN-2.
    ///
    /// In the above situation, LSN-1 is "iceberg snapshot LSN", while LSN-2 is "persisted table LSN".
    pub(crate) fn get_persisted_table_lsn(
        &self,
        iceberg_snapshot_lsn: Option<u64>,
        replication_lsn: u64,
    ) -> u64 {
        // Case-1: there're no activities in the current table, but replication LSN already covers requested LSN.
        if iceberg_snapshot_lsn.is_none() && self.table_consistent_view_lsn.is_none() {
            return replication_lsn;
        }

        // Case-2: if there're no updates since last iceberg snapshot, replication LSN indicates persisted table LSN.
        if iceberg_snapshot_lsn == self.table_consistent_view_lsn {
            // Notice: replication LSN comes from replication events, so if all events have been processed (i.e., a clean recovery case), replication LSN is 0.
            return std::cmp::max(replication_lsn, iceberg_snapshot_lsn.unwrap());
        }

        // Case-3: iceberg snapshot LSN indicates the persisted table LSN.
        // No guarantee an iceberg snapshot has been persisted here.
        iceberg_snapshot_lsn.unwrap_or(0)
    }

    /// Update requested iceberg snapshot LSNs, if applicable.
    fn update_force_iceberg_snapshot_requests(
        &mut self,
        iceberg_snapshot_lsn: u64,
        replication_lsn: u64,
    ) {
        if !self.has_pending_force_snapshot_request() {
            return;
        }

        let persisted_table_lsn =
            self.get_persisted_table_lsn(Some(iceberg_snapshot_lsn), replication_lsn);
        self.notify_persisted_table_lsn(persisted_table_lsn);
        let largest_force_snapshot_lsn = self.largest_force_snapshot_lsn.unwrap();
        if persisted_table_lsn >= largest_force_snapshot_lsn {
            self.largest_force_snapshot_lsn = None;
        }
    }

    /// Notify the persisted table LSN.
    fn notify_persisted_table_lsn(&mut self, persisted_table_lsn: u64) {
        if let Err(e) = self
            .force_snapshot_completion_tx
            .send(Some(Ok(persisted_table_lsn)))
        {
            error!(error = ?e, "failed to notify force snapshot, because receiver end has closed channel");
        }
    }

    fn start_alter_table(&mut self, alter_table_request: AlterTableRequest) {
        // Alter table will block any events, so table must be at a consistent view.
        assert!(self.table_consistent_view_lsn.is_some());
        assert!(self.special_table_state == SpecialTableState::Normal);
        // Trigger a force snapshot.
        // Note: if there's pending force snapshot that's larger than current table consistent view LSN,
        // we will keep the larger one.
        // And we need to make sure 'PeriodicalMooncakeTableSnapshot' will still trigger a force snapshot immediately.
        self.largest_force_snapshot_lsn = match self.largest_force_snapshot_lsn {
            Some(lsn) => Some(std::cmp::max(lsn, self.table_consistent_view_lsn.unwrap())),
            None => Some(self.table_consistent_view_lsn.unwrap()),
        };
        self.special_table_state = SpecialTableState::AlterTable {
            alter_table_lsn: self.table_consistent_view_lsn.unwrap(),
            alter_table_request: Some(alter_table_request),
        };
    }
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
        let event_sender_for_periodical_snapshot = event_sender.clone();
        let event_sender_for_periodical_force_snapshot = event_sender.clone();
        let periodic_event_handle = tokio::spawn(async move {
            let mut periodic_snapshot_interval = tokio::time::interval(Duration::from_millis(500));
            let mut periodic_force_snapshot_interval =
                tokio::time::interval(Duration::from_secs(300));

            loop {
                tokio::select! {
                    // Sending to channel fails only happens when eventloop exits, directly exit timer events.
                    _ = periodic_snapshot_interval.tick() => {
                        if event_sender_for_periodical_snapshot.send(TableEvent::PeriodicalMooncakeTableSnapshot).await.is_err() {
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
                if let Err(err) = io_utils::delete_local_files(&evicted_file_to_delete).await {
                    error!("Failed to delete object storage cache: {:?}", err);
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
                            // TODO(hjiang): Handle cases where there're not enough file indices to merge.
                            assert_eq!(table_handler_state.index_merge_request_status, MaintenanceRequestStatus::Unrequested);
                            table_handler_state.index_merge_request_status = MaintenanceRequestStatus::ForceRegular;
                        }
                        // Branch to trigger a force regular data compaction request.
                        TableEvent::ForceRegularDataCompaction => {
                            // TODO(hjiang): Handle cases where there're not enough files to compact.
                            assert_eq!(table_handler_state.data_compaction_request_status, MaintenanceRequestStatus::Unrequested);
                            table_handler_state.data_compaction_request_status = MaintenanceRequestStatus::ForceRegular;
                        }
                        // Branch to trigger a force full index merge request.
                        TableEvent::ForceFullMaintenance => {
                            // TODO(hjiang): Handle cases where there're not enough file indices to merge.
                            assert_eq!(table_handler_state.index_merge_request_status, MaintenanceRequestStatus::Unrequested);
                            assert_eq!(table_handler_state.data_compaction_request_status, MaintenanceRequestStatus::Unrequested);
                            table_handler_state.data_compaction_request_status = MaintenanceRequestStatus::ForceFull;
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
                        TableEvent::PeriodicalMooncakeTableSnapshot => {
                            // Only create a periodic snapshot if there isn't already one in progress
                            if table_handler_state.mooncake_snapshot_ongoing {
                                continue;
                            }

                            // Check whether a flush and force snapshot is needed.
                            if table_handler_state.has_pending_force_snapshot_request() && !table_handler_state.iceberg_snapshot_ongoing {
                                if let Some(commit_lsn) = table_handler_state.table_consistent_view_lsn {
                                    table.flush(commit_lsn).await.unwrap();
                                    table_handler_state.reset_iceberg_state_at_mooncake_snapshot();
                                    if let SpecialTableState::AlterTable { .. } = table_handler_state.special_table_state {
                                        table.force_empty_iceberg_payload();
                                    }
                                    assert!(table.create_snapshot(table_handler_state.get_mooncake_snapshot_option(/*request_force=*/true)));
                                    table_handler_state.mooncake_snapshot_ongoing = true;
                                    continue;
                                }
                            }

                            // Fallback to normal periodic snapshot.
                            table_handler_state.reset_iceberg_state_at_mooncake_snapshot();
                            table_handler_state.mooncake_snapshot_ongoing = table.create_snapshot(table_handler_state.get_mooncake_snapshot_option(/*request_force=*/false));
                        }
                        TableEvent::MooncakeTableSnapshotResult { lsn, iceberg_snapshot_payload, data_compaction_payload, file_indice_merge_payload, evicted_data_files_to_delete } => {
                            // Spawn a detached best-effort task to delete evicted object storage cache.
                            start_task_to_delete_evicted(evicted_data_files_to_delete);

                            // Mark mooncake snapshot as completed.
                            table.mark_mooncake_snapshot_completed();
                            table_handler_state.mooncake_snapshot_ongoing = false;

                            // Drop table if requested, and table at a clean state.
                            if table_handler_state.special_table_state == SpecialTableState::DropTable && !table_handler_state.iceberg_snapshot_ongoing {
                                drop_table(&mut table, event_sync_sender).await;
                                return;
                            }

                            // Notify read the mooncake table commit of LSN.
                            table.notify_snapshot_reader(lsn);

                            // Process iceberg snapshot and trigger iceberg snapshot if necessary.
                            if table_handler_state.can_initiate_iceberg_snapshot() {
                                if let Some(mut iceberg_snapshot_payload) = iceberg_snapshot_payload {
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
                            }

                            // Attempt to process data compaction.
                            // Unlike snapshot, we can actually have multiple file index merge operations ongoing concurrently,
                            // to simplify workflow we limit at most one ongoing.
                            if !table_handler_state.maintenance_ongoing {
                                if let Some(data_compaction_payload) = data_compaction_payload {
                                    table_handler_state.maintenance_ongoing = true;
                                    assert_eq!(table_handler_state.table_maintenance_process_status, MaintenanceProcessStatus::Unrequested);
                                    table_handler_state.table_maintenance_process_status = MaintenanceProcessStatus::InProcess;
                                    table.perform_data_compaction(data_compaction_payload);
                                }
                            }

                            // Attempt to process file indices merge.
                            // Unlike snapshot, we can actually have multiple file index merge operations ongoing concurrently,
                            // to simplify workflow we limit at most one ongoing.
                            if !table_handler_state.maintenance_ongoing {
                                if let Some(file_indice_merge_payload) = file_indice_merge_payload {
                                    table_handler_state.maintenance_ongoing = true;
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
                                        table_handler_state.table_maintenance_completion_tx.send(Ok(())).unwrap();
                                    }

                                    // Buffer iceberg persistence result, which later will be reflected to mooncake snapshot.
                                    let iceberg_flush_lsn = snapshot_res.flush_lsn;
                                    event_sync_sender.flush_lsn_tx.send(iceberg_flush_lsn).unwrap();
                                    table.set_iceberg_snapshot_res(snapshot_res);
                                    table_handler_state.iceberg_snapshot_result_consumed = false;

                                    // Notify all waiters with LSN satisfied.
                                    let replication_lsn = *replication_lsn_rx.borrow();
                                    table_handler_state.update_force_iceberg_snapshot_requests(iceberg_flush_lsn, replication_lsn);
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
                                        table_handler_state.table_maintenance_completion_tx.send(Err(e)).unwrap();
                                    }

                                    // If iceberg snapshot fails, send error back to all broadcast subscribers and unset force snapshot requests.
                                    table_handler_state.largest_force_snapshot_lsn = None;
                                }
                            }

                            // Drop table if requested, and table at a clean state.
                            if table_handler_state.special_table_state == SpecialTableState::DropTable && !table_handler_state.mooncake_snapshot_ongoing {
                                drop_table(&mut table, event_sync_sender).await;
                                return;
                            }
                        }
                        TableEvent::IndexMergeResult { index_merge_result } => {
                            table.set_file_indices_merge_res(index_merge_result);
                            table_handler_state.mark_index_merge_completed().await;
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
                        }
                        TableEvent::ReadRequestCompletion { cache_handles } => {
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

                let should_force_snapshot =
                    table_handler_state.should_force_snapshot_by_commit_lsn(lsn);

                match xact_id {
                    Some(xact_id) => {
                        if let Err(e) = table.commit_transaction_stream(xact_id, lsn).await {
                            error!(error = %e, "stream commit flush failed");
                        }
                    }
                    None => {
                        table.commit(lsn);
                        if table.should_flush() || should_force_snapshot {
                            if let Err(e) = table.flush(lsn).await {
                                error!(error = %e, "flush failed in commit");
                            }
                        }
                    }
                }

                if should_force_snapshot {
                    table_handler_state.reset_iceberg_state_at_mooncake_snapshot();
                    assert!(table.create_snapshot(
                        table_handler_state.get_mooncake_snapshot_option(/*request_force=*/ true)
                    ));
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
