/// Table handler state manages table event process states.
use crate::storage::mooncake_table::AlterTableRequest;
use crate::storage::mooncake_table::DataCompactionResult;
use crate::storage::snapshot_options::IcebergSnapshotOption;
use crate::storage::snapshot_options::MaintenanceOption;
use crate::storage::snapshot_options::SnapshotOption;
use crate::table_notify::TableEvent;
use crate::Result;
use more_asserts as ma;
use tokio::sync::{broadcast, watch};
use tracing::error;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SpecialTableState {
    Normal,
    InitialCopy,
    AlterTable {
        alter_table_lsn: u64,
        alter_table_request: Option<AlterTableRequest>,
    },
    DropTable,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MaintenanceRequestStatus {
    /// Force Maintenance request is not requested.
    Unrequested,
    /// Force regular Maintenance is requested.
    ForceRegular,
    /// Force full Maintenance is requested.
    ForceFull,
}

impl MaintenanceRequestStatus {
    /// Return whether the current maintenance request is force one.
    pub(crate) fn is_force_request(&self) -> bool {
        matches!(
            self,
            MaintenanceRequestStatus::ForceRegular | MaintenanceRequestStatus::ForceFull
        )
    }

    /// Return whether there's an ongoing request.
    pub(crate) fn is_requested(&self) -> bool {
        matches!(
            self,
            MaintenanceRequestStatus::ForceRegular | MaintenanceRequestStatus::ForceFull
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MaintenanceProcessStatus {
    /// Force maintenance request is not being requested.
    Unrequested,
    /// Force maintenance request is being processed.
    InProcess,
    /// Maintenance result has been put into snapshot buffer, which will be persisted into iceberg later.
    ReadyToPersist,
    /// Maintenance task result is being persisted into iceberg.
    InPersist,
}

impl MaintenanceProcessStatus {
    /// Return whether there's maintenance process ongoing.
    pub(crate) fn is_maintenance_ongoing(&self) -> bool {
        !matches!(self, MaintenanceProcessStatus::Unrequested)
    }
}

pub(crate) struct TableHandlerState {
    // cached table states
    //
    // Initial persisted LSN.
    // On moonlink recovery, it's possible that moonlink hasn't sent back latest flush LSN back to source table, so source database (i.e. postgres) will replay unacknowledged parts, which might contain already persisted content.
    // To avoid duplicate records, we compare iceberg initial flush LSN with new coming messages' LSN.
    // This also is used in the initial copy mode to discard cdc events that may be already included in the initial copy.
    // If we have already seen this LSN, we simply discard the event.
    pub(crate) initial_persistence_lsn: Option<u64>,
    // Record LSN if the last handled table event is a commit operation, which indicates mooncake table stays at a consistent view, so table could be flushed safely.
    pub(crate) table_consistent_view_lsn: Option<u64>,
    // Latest LSN of the table's latest commit.
    pub(crate) latest_commit_lsn: Option<u64>,
    // Last unflushed commit LSN for non-streaming transaction.
    // Used to flush non-streaming writes at streaming transaction commit.
    pub(crate) last_unflushed_commit_lsn: Option<u64>,

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
    pub(crate) persistence_snapshot_result_consumed: bool,
    pub(crate) persistence_snapshot_ongoing: bool,
    // Whether there's an ongoing mooncake snapshot operation.
    pub(crate) mooncake_snapshot_ongoing: bool,
    // Largest pending force snapshot LSN.
    pub(crate) largest_force_snapshot_lsn: Option<u64>,
    // Notify when force snapshot completes.
    pub(crate) force_snapshot_completion_tx: watch::Sender<Option<Result<u64>>>,
    // Special table state, for example, initial copy, alter table, drop table, etc.
    pub(crate) special_table_state: SpecialTableState,
    // Buffered events during blocking operations: initial copy, alter table, drop table, etc.
    pub(crate) initial_copy_buffered_events: Vec<TableEvent>,

    // ================================================
    // Table maintenance status
    // ================================================
    //
    // Assume there's at most one table maintenance operation ongoing.
    //
    // Index merge request status.
    pub(crate) index_merge_request_status: MaintenanceRequestStatus,
    /// Data compaction request status.
    pub(crate) data_compaction_request_status: MaintenanceRequestStatus,
    /// Table maintenance process status.
    pub(crate) table_maintenance_process_status: MaintenanceProcessStatus,
    /// Notify when data compaction completes.
    pub(crate) table_maintenance_completion_tx: broadcast::Sender<Result<()>>,

    // ================================================
    // Write-ahead log (WAL)
    // ================================================
    //
    pub(crate) wal_persist_ongoing: bool,
}

impl TableHandlerState {
    pub(crate) fn new(
        table_maintenance_completion_tx: broadcast::Sender<Result<()>>,
        force_snapshot_completion_tx: watch::Sender<Option<Result<u64>>>,
        initial_persistence_lsn: Option<u64>,
        persistence_snapshot_lsn: Option<u64>,
    ) -> Self {
        Self {
            persistence_snapshot_result_consumed: true,
            persistence_snapshot_ongoing: false,
            mooncake_snapshot_ongoing: false,
            initial_persistence_lsn,
            last_unflushed_commit_lsn: None,
            latest_commit_lsn: None,
            special_table_state: SpecialTableState::Normal,
            // Force snapshot fields.
            table_consistent_view_lsn: persistence_snapshot_lsn,
            largest_force_snapshot_lsn: None,
            force_snapshot_completion_tx,
            // Table maintenance fields.
            index_merge_request_status: MaintenanceRequestStatus::Unrequested,
            data_compaction_request_status: MaintenanceRequestStatus::Unrequested,
            table_maintenance_process_status: MaintenanceProcessStatus::Unrequested,
            table_maintenance_completion_tx,
            // Initial copy fields.
            initial_copy_buffered_events: Vec::new(),
            wal_persist_ongoing: false,
        }
    }

    pub(crate) fn update_table_lsns(&mut self, event: &TableEvent) {
        if event.is_ingest_event() {
            match event {
                // Update LSN for commit operations.
                TableEvent::Commit { lsn, xact_id, .. } => {
                    self.latest_commit_lsn = Some(*lsn);
                    self.table_consistent_view_lsn = Some(*lsn);
                    if xact_id.is_none() {
                        // Unset at flush operation.
                        self.last_unflushed_commit_lsn = Some(*lsn);
                    }
                }
                TableEvent::CommitFlush { lsn, .. } => {
                    self.latest_commit_lsn = Some(*lsn);
                    self.table_consistent_view_lsn = Some(*lsn);
                }
                // Unset for table write operations.
                TableEvent::Append { .. }
                | TableEvent::Delete { .. }
                | TableEvent::StreamAbort { .. } => {
                    self.table_consistent_view_lsn = None;
                }
                // Doesn't update for [`StreamAbort`] and [`StreamFlush`].
                _ => {}
            }
        }
    }

    /// Return mooncake snapshot option.
    ///
    /// # Arguments
    ///
    /// * request_force: request to force create a mooncake / iceberg snapshot.
    pub(crate) fn get_mooncake_snapshot_option(
        &self,
        request_force: bool,
        uuid: uuid::Uuid,
    ) -> SnapshotOption {
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
            uuid,
            force_create,
            dump_snapshot: false,
            iceberg_snapshot_option: self.get_iceberg_snapshot_option(),
            index_merge_option: self.get_index_merge_maintenance_option(),
            data_compaction_option: self.get_data_compaction_maintenance_option(),
        }
    }

    /// Used at recovery, to decide whether the incoming table event should be considered.
    pub(crate) fn should_discard_event(&self, event: &TableEvent) -> bool {
        if self.initial_persistence_lsn.is_none() {
            return false;
        }
        // Streaming events cannot be discarded, whose LSN is sent at commit phase.
        if event.is_streaming_update() {
            return false;
        }
        // For non-streaming events, discard if LSN is less than flush LSN.
        let initial_persistence_lsn = self.initial_persistence_lsn.unwrap();
        if let Some(lsn) = event.get_lsn_for_ingest_event() {
            lsn <= initial_persistence_lsn
        } else {
            false
        }
    }

    pub(crate) fn is_in_blocking_state(&self) -> bool {
        self.special_table_state != SpecialTableState::Normal
    }

    /// Get the largest LSN where all updates have been persisted into iceberg.
    /// The difference between "persisted table LSN" and "iceberg snapshot LSN" is, suppose we have two tables, table A has persisted all changes to iceberg with flush LSN-1;
    /// if there're no further updates to the table A, meanwhile there're updates to table B with LSN-2, flush LSN-1 actually represents a consistent view of LSN-2.
    ///
    /// In the above situation, LSN-1 is "iceberg snapshot LSN", while LSN-2 is "persisted table LSN".
    pub(crate) fn get_persisted_table_lsn(
        &self,
        persistence_snapshot_lsn: Option<u64>,
        replication_lsn: u64,
    ) -> u64 {
        // Case-1: there're no activities in the current table, replication LSN indicates current status.
        if persistence_snapshot_lsn.is_none() && self.table_consistent_view_lsn.is_none() {
            return replication_lsn;
        }

        // Case-2: if there're no updates since last iceberg snapshot, replication LSN indicates persisted table LSN.
        if persistence_snapshot_lsn == self.table_consistent_view_lsn {
            // Notice: replication LSN comes from replication events, so if all events have been processed (i.e., a clean recovery case), replication LSN is 0.
            return std::cmp::max(replication_lsn, persistence_snapshot_lsn.unwrap());
        }

        // Case-3: iceberg snapshot LSN indicates the persisted table LSN.
        // No guarantee an iceberg snapshot has been persisted here.
        persistence_snapshot_lsn.unwrap_or(0)
    }

    /// Notify the persisted table LSN.
    pub(crate) fn notify_persisted_table_lsn(&mut self, persisted_table_lsn: u64) {
        if let Err(e) = self
            .force_snapshot_completion_tx
            .send(Some(Ok(persisted_table_lsn)))
        {
            error!(error = ?e, "failed to notify force snapshot, because receiver end has closed channel");
        }
    }

    /// ============================
    /// Force snapshot
    /// ============================
    ///
    /// Update requested iceberg snapshot LSNs, if applicable.
    pub(crate) fn update_iceberg_persisted_lsn(
        &mut self,
        persistence_snapshot_lsn: u64,
        replication_lsn: u64,
    ) {
        let persisted_table_lsn =
            self.get_persisted_table_lsn(Some(persistence_snapshot_lsn), replication_lsn);
        self.notify_persisted_table_lsn(persisted_table_lsn);

        if let Some(largest_force_snapshot_lsn) = self.largest_force_snapshot_lsn {
            if persisted_table_lsn >= largest_force_snapshot_lsn {
                self.largest_force_snapshot_lsn = None;
            }
        }
    }

    /// Return whether there're pending force snapshot requests.
    pub(crate) fn has_pending_force_snapshot_request(&self) -> bool {
        self.largest_force_snapshot_lsn.is_some()
    }

    pub(crate) fn should_force_flush(&self, commit_lsn: u64, existing_flush_lsn: u64) -> bool {
        if let Some(largest_force_snapshot_lsn) = self.largest_force_snapshot_lsn {
            commit_lsn >= largest_force_snapshot_lsn
                && existing_flush_lsn < largest_force_snapshot_lsn
        } else {
            false
        }
    }

    /// Return whether there's background tasks ongoing.
    fn has_background_task_ongoing(&mut self, has_ongoing_flush: bool) -> bool {
        if self.mooncake_snapshot_ongoing {
            return false;
        }
        if self.persistence_snapshot_ongoing {
            return false;
        }
        if self.wal_persist_ongoing {
            return false;
        }
        if self.table_maintenance_process_status != MaintenanceProcessStatus::Unrequested {
            return false;
        }
        if has_ongoing_flush {
            return false;
        }
        true
    }

    /// ============================
    /// Drop table
    /// ============================
    ///
    pub(crate) fn mark_drop_table(&mut self) {
        assert_eq!(self.special_table_state, SpecialTableState::Normal);
        self.special_table_state = SpecialTableState::DropTable;
    }

    /// Return whether table handler could be dropped now.
    /// If there're any background activities ongoing, we cannot drop table immediately.
    pub(crate) fn can_drop_table_now(&mut self, has_ongoing_flush: bool) -> bool {
        self.has_background_task_ongoing(has_ongoing_flush)
    }

    /// ============================
    /// Alter table
    /// ============================
    ///
    pub(crate) fn start_alter_table(&mut self, alter_table_request: AlterTableRequest) {
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

    pub(crate) fn should_complete_alter_table(&self, persistence_snapshot_lsn: u64) -> bool {
        if let SpecialTableState::AlterTable {
            alter_table_lsn, ..
        } = self.special_table_state
        {
            ma::assert_le!(persistence_snapshot_lsn, alter_table_lsn);
            persistence_snapshot_lsn == alter_table_lsn
        } else {
            false
        }
    }

    pub(crate) fn finish_alter_table(&mut self) {
        assert!(matches!(
            self.special_table_state,
            SpecialTableState::AlterTable { .. }
        ));
        self.special_table_state = SpecialTableState::Normal;
    }

    /// ============================
    /// Initial copy
    /// ============================
    ///
    /// Enter initial copy mode. Subsequent CDC events will be
    /// buffered in `initial_copy_buffered_events` until `finish_initial_copy` is called.
    /// We set `initial_persistence_lsn` to the start LSN to avoid duplicate events that may have already been captured by the initial copy.
    pub(crate) fn start_initial_copy(&mut self) {
        assert_eq!(self.special_table_state, SpecialTableState::Normal);
        self.special_table_state = SpecialTableState::InitialCopy;
    }

    pub(crate) fn finish_initial_copy(&mut self, lsn: u64) {
        assert_eq!(self.special_table_state, SpecialTableState::InitialCopy);
        self.special_table_state = SpecialTableState::Normal;
        self.latest_commit_lsn = Some(lsn);
        self.table_consistent_view_lsn = Some(lsn);
    }

    /// ============================
    /// Iceberg snapshot
    /// ============================
    ///
    /// Used to decide whether we could create an iceberg snapshot.
    /// The completion of an iceberg snapshot is **NOT** marked as the finish of snapshot thread, but the handling of its results.
    /// We can only create a new iceberg snapshot when (1) there's no ongoing iceberg snapshot, (2) previous snapshot results have been acknowledged, (3) there's no pending flush LSNs < flush_lsn
    ///
    pub(crate) fn can_initiate_iceberg_snapshot(
        flush_lsn: u64,
        min_ongoing_flush_lsn: u64,
        persistence_snapshot_result_consumed: bool,
        persistence_snapshot_ongoing: bool,
    ) -> bool {
        persistence_snapshot_result_consumed
            && !persistence_snapshot_ongoing
            && flush_lsn < min_ongoing_flush_lsn
    }

    pub(crate) fn reset_iceberg_state_at_mooncake_snapshot(&mut self) {
        // Validate iceberg snapshot state before mooncake snapshot creation.
        //
        // Assertion on impossible state.
        assert!(!self.persistence_snapshot_ongoing || self.persistence_snapshot_result_consumed);

        // If there's pending iceberg snapshot result unconsumed, the following mooncake snapshot will properly handle it.
        if !self.persistence_snapshot_result_consumed {
            self.persistence_snapshot_result_consumed = true;
            self.persistence_snapshot_ongoing = false;
        }
    }

    /// ============================
    /// Table maintenance
    /// ============================
    ///
    /// Get Maintenance task operation option.
    pub(crate) fn get_maintenance_task_option(
        &self,
        request_status: &MaintenanceRequestStatus,
    ) -> MaintenanceOption {
        if self.table_maintenance_process_status != MaintenanceProcessStatus::Unrequested {
            return MaintenanceOption::Skip;
        }
        match request_status {
            MaintenanceRequestStatus::Unrequested => {
                MaintenanceOption::BestEffort(uuid::Uuid::new_v4())
            }
            MaintenanceRequestStatus::ForceRegular => {
                MaintenanceOption::ForceRegular(uuid::Uuid::new_v4())
            }
            MaintenanceRequestStatus::ForceFull => {
                MaintenanceOption::ForceFull(uuid::Uuid::new_v4())
            }
        }
    }
    fn get_iceberg_snapshot_option(&self) -> IcebergSnapshotOption {
        if self.persistence_snapshot_ongoing {
            IcebergSnapshotOption::Skip
        } else {
            IcebergSnapshotOption::BestEffort(uuid::Uuid::new_v4())
        }
    }
    fn get_index_merge_maintenance_option(&self) -> MaintenanceOption {
        self.get_maintenance_task_option(&self.index_merge_request_status)
    }
    fn get_data_compaction_maintenance_option(&self) -> MaintenanceOption {
        self.get_maintenance_task_option(&self.data_compaction_request_status)
    }

    /// Mark index merge completion.
    pub(crate) async fn mark_index_merge_completed(&mut self) {
        assert_eq!(
            self.table_maintenance_process_status,
            MaintenanceProcessStatus::InProcess
        );
        self.index_merge_request_status = MaintenanceRequestStatus::Unrequested;
        self.table_maintenance_process_status = MaintenanceProcessStatus::ReadyToPersist;
    }

    /// Mark data compaction completion.
    pub(crate) async fn mark_data_compaction_completed(
        &mut self,
        data_compaction_result: &Result<DataCompactionResult>,
    ) {
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

    /// We can have at most one table maintenance ongoing, only allow to start a new one when there's no ongoing operations, nor another requested ones.
    pub(crate) fn can_start_new_maintenance(&self) -> bool {
        if self
            .table_maintenance_process_status
            .is_maintenance_ongoing()
        {
            return false;
        }
        if self.index_merge_request_status.is_requested() {
            return false;
        }
        if self.data_compaction_request_status.is_requested() {
            return false;
        }
        true
    }
}
