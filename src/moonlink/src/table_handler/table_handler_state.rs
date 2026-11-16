use crate::event_sync::TableStatus;
/// Table handler state manages table event process states.
use crate::storage::mooncake_table::AlterTableRequest;
use crate::storage::mooncake_table::DataCompactionResult;
use crate::storage::snapshot_options::IcebergSnapshotOption;
use crate::storage::snapshot_options::MaintenanceOption;
use crate::storage::snapshot_options::SnapshotOption;
use crate::table_notify::TableEvent;
use crate::Result;
use more_asserts as ma;
use std::collections::HashSet;
use tokio::sync::{broadcast, watch};
use tracing::{debug, error, info};

/// Consecutive failed iceberg persistence attempts after which the table is errored.
pub(crate) const MAX_CONSECUTIVE_PERSISTENCE_FAILURES: u32 = 10;
pub(crate) const MAX_CONSECUTIVE_BACKGROUND_MAINTENANCE_FAILURES: u32 = 5;
#[cfg(not(test))]
const PERSISTENCE_RETRY_BASE_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);
#[cfg(test)]
const PERSISTENCE_RETRY_BASE_BACKOFF: std::time::Duration = std::time::Duration::from_millis(10);
const PERSISTENCE_RETRY_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SpecialTableState {
    Normal,
    InitialCopy,
    AlterTable {
        alter_table_lsn: u64,
        alter_table_request: Option<AlterTableRequest>,
    },
    DropTable,
    /// The table can no longer be kept consistent with its source. It only serves errors and
    /// DROP from now on; never left within a process.
    Errored {
        reason: String,
    },
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
    // Transactions with writes since the latest commit (`None` is the non-streamed one), which
    // keep the table from a consistent view. When a streamed one ends without being applied, the
    // view comes back once no writer is left.
    writers_since_latest_commit: HashSet<Option<u32>>,
    /// Re-sent events skipped because the table had already applied them (see [`Self::skip_resent_event`]).
    pub(crate) skipped_resent_events: u64,
    /// Re-sent commits skipped in total, and the commit LSNs of the current run of skips.
    pub(crate) skipped_resent_commits: u64,
    skipped_run: Option<SkippedRun>,
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
    // Whether the ongoing initial copy loaded any files (an empty source table loads none).
    pub(crate) initial_copy_loaded_files: bool,

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
    // Errored state
    // ================================================
    //
    pub(crate) table_status_tx: Option<watch::Sender<TableStatus>>,

    // ================================================
    // Persistence retry
    // ================================================
    //
    /// Consecutive failed iceberg persistence attempts.
    pub(crate) consecutive_persistence_failures: u32,
    /// Consecutive failed compactions / index merges; background maintenance stops at
    /// [`MAX_CONSECUTIVE_BACKGROUND_MAINTENANCE_FAILURES`] (explicit optimize requests still run).
    pub(crate) consecutive_maintenance_failures: u32,
    /// Consecutive persistence failures caused by a full disk (not counted above).
    pub(crate) consecutive_full_disk_persistence_failures: u32,
    /// Do not start another iceberg persistence before this instant.
    pub(crate) persistence_retry_after: Option<std::time::Instant>,
    /// A persistence failed and must be retried even without new writes.
    pub(crate) persistence_retry_pending: bool,
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
            // Everything up to the persisted snapshot counts as applied, so transactions postgres
            // re-sends after a restart are skipped like those re-sent after a reconnect, streamed
            // ones included. The other floor is `initial_persistence_lsn` (the persisted snapshot,
            // or the copy boundary after an initial copy): an event at or below either is skipped.
            // A recovered table has both at its snapshot LSN, and a copy that loaded files sets
            // both to its boundary; neither floor ever moves down.
            latest_commit_lsn: persistence_snapshot_lsn,
            writers_since_latest_commit: HashSet::new(),
            skipped_resent_events: 0,
            skipped_resent_commits: 0,
            skipped_run: None,
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
            initial_copy_loaded_files: false,
            table_status_tx: None,
            consecutive_persistence_failures: 0,
            consecutive_maintenance_failures: 0,
            consecutive_full_disk_persistence_failures: 0,
            persistence_retry_after: None,
            persistence_retry_pending: false,
        }
    }

    pub(crate) fn with_table_status_tx(mut self, tx: watch::Sender<TableStatus>) -> Self {
        self.table_status_tx = Some(tx);
        self
    }

    /// Record a failed iceberg persistence and back off exponentially; returns the number of
    /// consecutive failures.
    pub(crate) fn record_persistence_failure(&mut self) -> u32 {
        self.consecutive_persistence_failures += 1;
        let exponent = std::cmp::min(self.consecutive_persistence_failures - 1, 16);
        let backoff = std::cmp::min(
            PERSISTENCE_RETRY_BASE_BACKOFF.saturating_mul(1 << exponent),
            PERSISTENCE_RETRY_MAX_BACKOFF,
        );
        self.persistence_retry_after = Some(std::time::Instant::now() + backoff);
        self.persistence_retry_pending = true;
        self.consecutive_persistence_failures
    }

    /// Record a persistence that failed because the disk is full: retried with a capped backoff
    /// but not counted towards erroring the table, since freeing space fixes it. Returns the
    /// number of consecutive (counted) failures.
    pub(crate) fn record_persistence_failure_on_full_disk(&mut self) -> u32 {
        self.consecutive_full_disk_persistence_failures += 1;
        let exponent = std::cmp::min(self.consecutive_full_disk_persistence_failures - 1, 6);
        let backoff = std::cmp::min(
            PERSISTENCE_RETRY_BASE_BACKOFF.saturating_mul(1 << exponent),
            PERSISTENCE_RETRY_MAX_BACKOFF,
        );
        self.persistence_retry_after = Some(std::time::Instant::now() + backoff);
        self.persistence_retry_pending = true;
        self.consecutive_persistence_failures
    }

    pub(crate) fn record_persistence_success(&mut self) {
        self.consecutive_full_disk_persistence_failures = 0;
        self.consecutive_persistence_failures = 0;
        self.persistence_retry_after = None;
        self.persistence_retry_pending = false;
    }

    pub(crate) fn persistence_backoff_elapsed(&self) -> bool {
        self.persistence_retry_after
            .map(|t| std::time::Instant::now() >= t)
            .unwrap_or(true)
    }

    pub(crate) fn is_errored(&self) -> bool {
        matches!(self.special_table_state, SpecialTableState::Errored { .. })
    }

    /// Move the table into the errored state and fail every pending waiter.
    /// Returns whether a DROP was pending (it must now be completed by the caller), or None if
    /// the table was already errored.
    pub(crate) fn enter_errored(&mut self, table_name: &str, reason: String) -> Option<bool> {
        if self.is_errored() {
            return None;
        }
        let drop_pending = self.special_table_state == SpecialTableState::DropTable;
        let message = crate::Error::errored_table_message(table_name, &reason);
        let err = crate::Error::table_errored(&message);
        let _ = self
            .force_snapshot_completion_tx
            .send(Some(Err(err.clone())));
        let _ = self.table_maintenance_completion_tx.send(Err(err));
        self.largest_force_snapshot_lsn = None;
        self.initial_copy_buffered_events = Vec::new();
        if let Some(tx) = &self.table_status_tx {
            let _ = tx.send(TableStatus::Errored(message));
        }
        self.special_table_state = SpecialTableState::Errored { reason };
        Some(drop_pending)
    }

    pub(crate) fn update_table_lsns(&mut self, event: &TableEvent) {
        // A streamed transaction is never discarded by `should_discard_event` (its rows arrive
        // before its LSN); one committed at or below the persisted/copy boundary is aborted when
        // its commit is processed, so it must not move the LSNs either.
        if let TableEvent::Commit {
            lsn,
            xact_id: Some(xact_id),
        }
        | TableEvent::CommitFlush {
            lsn,
            xact_id: Some(xact_id),
        } = event
        {
            if self
                .initial_persistence_lsn
                .is_some_and(|boundary| *lsn <= boundary)
            {
                self.end_stream_without_applying(*xact_id);
                return;
            }
        }
        if event.is_ingest_event() {
            match event {
                // Update LSN for commit operations.
                TableEvent::Commit { lsn, xact_id, .. } => {
                    self.latest_commit_lsn = Some(*lsn);
                    self.table_consistent_view_lsn = Some(*lsn);
                    self.writers_since_latest_commit.clear();
                    if xact_id.is_none() {
                        // Unset at flush operation.
                        self.last_unflushed_commit_lsn = Some(*lsn);
                    }
                }
                TableEvent::CommitFlush { lsn, .. } => {
                    self.latest_commit_lsn = Some(*lsn);
                    self.table_consistent_view_lsn = Some(*lsn);
                    self.writers_since_latest_commit.clear();
                }
                // Unset for table write operations.
                TableEvent::Append { xact_id, .. } | TableEvent::Delete { xact_id, .. } => {
                    self.table_consistent_view_lsn = None;
                    self.writers_since_latest_commit.insert(*xact_id);
                }
                TableEvent::StreamAbort { xact_id } => {
                    self.end_stream_without_applying(*xact_id);
                }
                // [`StreamFlush`] writes a streamed transaction's rows to its own files, which
                // leaves the view as it is.
                _ => {}
            }
        }
    }

    /// A streamed transaction ended without changing the table: aborted, or its commit dropped
    /// as already applied. Without other writes since the latest commit, the table is back at
    /// that commit's consistent view, so a pending forced snapshot can flush it instead of
    /// waiting for the next commit, which never comes on an idle table.
    fn end_stream_without_applying(&mut self, xact_id: u32) {
        self.writers_since_latest_commit.remove(&Some(xact_id));
        if self.writers_since_latest_commit.is_empty() {
            self.table_consistent_view_lsn = self.latest_commit_lsn;
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
        // Retry a failed persistence once its backoff has elapsed, even if nothing new arrived.
        if self.persistence_retry_pending
            && self.persistence_backoff_elapsed()
            && !self.persistence_snapshot_ongoing
        {
            force_create = true;
        }
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
        let initial_persistence_lsn = self.initial_persistence_lsn.unwrap();
        match event {
            // Commits carry the commit's end LSN, which is what persistence LSNs are made of.
            TableEvent::Commit { lsn, .. } | TableEvent::CommitFlush { lsn, .. } => {
                *lsn <= initial_persistence_lsn
            }
            // Row events carry the start LSN of their commit record, and the next transaction's
            // commit record can start exactly at the persisted commit's end LSN.
            _ => event
                .get_lsn_for_ingest_event()
                .is_some_and(|lsn| lsn < initial_persistence_lsn),
        }
    }

    /// Whether the event belongs to a transaction this table has already applied. When replication
    /// reconnects, postgres re-sends every transaction after the slot's confirmed LSN, which trails
    /// what is applied in memory. LSN boundaries follow [`Self::should_discard_event`].
    pub(crate) fn is_replayed_applied_event(&self, event: &TableEvent) -> bool {
        let Some(latest_commit_lsn) = self.latest_commit_lsn else {
            return false;
        };
        match event {
            TableEvent::Commit { lsn, .. } => *lsn <= latest_commit_lsn,
            TableEvent::Append {
                lsn, xact_id: None, ..
            }
            | TableEvent::Delete {
                lsn, xact_id: None, ..
            } => *lsn < latest_commit_lsn,
            _ => false,
        }
    }

    /// Whether to skip `event` because the table already applied it (postgres re-sends everything
    /// after the slot's confirmed LSN on a restart or reconnect). Skips are counted, and a run of
    /// them is logged once it ends, so a skip that should not have happened leaves a trace.
    pub(crate) fn skip_resent_event(&mut self, event: &TableEvent) -> bool {
        if !self.is_replayed_applied_event(event) {
            if event.is_ingest_event() {
                self.finish_skipped_run();
            }
            return false;
        }
        self.skipped_resent_events += 1;
        if let TableEvent::Commit {
            xact_id: Some(xact_id),
            ..
        } = event
        {
            self.end_stream_without_applying(*xact_id);
        }
        let run = self.skipped_run.get_or_insert(SkippedRun::default());
        run.events += 1;
        if let TableEvent::Commit { lsn, .. } = event {
            debug!(
                lsn,
                "skipping a re-sent transaction the table already applied"
            );
            self.skipped_resent_commits += 1;
            run.commits += 1;
            run.first_commit_lsn.get_or_insert(*lsn);
            run.last_commit_lsn = Some(*lsn);
        }
        true
    }

    fn finish_skipped_run(&mut self) {
        if let Some(run) = self.skipped_run.take() {
            info!(
                transactions = run.commits,
                events = run.events,
                first_commit_lsn = run.first_commit_lsn,
                last_commit_lsn = run.last_commit_lsn,
                "skipped re-sent transactions the table had already applied"
            );
        }
    }

    /// Whether every committed change this table has applied is persisted, so the replication slot
    /// may move past the current replication position for it.
    pub(crate) fn has_no_unpersisted_commit(&self, persistence_snapshot_lsn: Option<u64>) -> bool {
        if self.special_table_state != SpecialTableState::Normal {
            return false;
        }
        match (self.latest_commit_lsn, persistence_snapshot_lsn) {
            (None, _) => true,
            (Some(commit_lsn), Some(persisted_lsn)) => commit_lsn <= persisted_lsn,
            (Some(_), None) => false,
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
        // Case-1: the table has never committed anything, so there is nothing to persist and the
        // replication LSN indicates current status. A missing consistent view alone doesn't mean
        // that: any write after the last commit clears it.
        if persistence_snapshot_lsn.is_none() && self.latest_commit_lsn.is_none() {
            return replication_lsn;
        }

        // Case-2: if there're no updates since last iceberg snapshot, replication LSN indicates persisted table LSN.
        if let Some(persistence_snapshot_lsn) = persistence_snapshot_lsn {
            if self.table_consistent_view_lsn == Some(persistence_snapshot_lsn) {
                // Notice: replication LSN comes from replication events, so if all events have been processed (i.e., a clean recovery case), replication LSN is 0.
                return std::cmp::max(replication_lsn, persistence_snapshot_lsn);
            }
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
        self.initial_copy_loaded_files = false;
    }

    pub(crate) fn finish_initial_copy(&mut self, lsn: u64) {
        assert_eq!(self.special_table_state, SpecialTableState::InitialCopy);
        self.special_table_state = SpecialTableState::Normal;
        // The boundary is the table's commit LSN only if the copy produced data at it. After an
        // empty copy the table has no activity yet; claiming a consistent view at the boundary
        // would report a persisted table LSN of 0 until the first write (stalling forced snapshots
        // and the replication slot), since no iceberg snapshot ever exists at that LSN.
        if self.initial_copy_loaded_files {
            self.latest_commit_lsn = Some(raised_floor(self.latest_commit_lsn, lsn));
            self.table_consistent_view_lsn = Some(lsn);
        }
        // Buffered events the copy already contains are dropped while they are applied.
        self.initial_persistence_lsn = Some(raised_floor(self.initial_persistence_lsn, lsn));
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
            // A maintenance that keeps failing (e.g. on an unreadable file) would otherwise be
            // retried, rewriting and deleting its partial output, on every snapshot.
            MaintenanceRequestStatus::Unrequested
                if self.consecutive_maintenance_failures
                    >= MAX_CONSECUTIVE_BACKGROUND_MAINTENANCE_FAILURES =>
            {
                MaintenanceOption::Skip
            }
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

    /// Mark index merge completion. A failed merge leaves nothing to persist, so maintenance
    /// goes back to idle and any optimize waiter gets the error.
    pub(crate) async fn mark_index_merge_completed(
        &mut self,
        index_merge_result: &Result<crate::storage::mooncake_table::FileIndiceMergeResult>,
    ) {
        assert_eq!(
            self.table_maintenance_process_status,
            MaintenanceProcessStatus::InProcess
        );
        self.index_merge_request_status = MaintenanceRequestStatus::Unrequested;
        match index_merge_result {
            Ok(_) => {
                self.table_maintenance_process_status = MaintenanceProcessStatus::ReadyToPersist;
                self.consecutive_maintenance_failures = 0;
            }
            Err(err) => {
                self.table_maintenance_process_status = MaintenanceProcessStatus::Unrequested;
                self.consecutive_maintenance_failures += 1;
                // Background maintenance has no waiter; ignore the missing receiver.
                let _ = self.table_maintenance_completion_tx.send(Err(err.clone()));
            }
        }
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
                self.consecutive_maintenance_failures = 0;
            }
            Err(err) => {
                self.table_maintenance_process_status = MaintenanceProcessStatus::Unrequested;
                self.consecutive_maintenance_failures += 1;
                // Background maintenance has no waiter; ignore the missing receiver.
                let _ = self.table_maintenance_completion_tx.send(Err(err.clone()));
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

/// Consecutive skipped re-sent events, logged as one line when the run ends.
#[derive(Debug, Default)]
struct SkippedRun {
    events: u64,
    commits: u64,
    first_commit_lsn: Option<u64>,
    last_commit_lsn: Option<u64>,
}

/// A replay floor moved to `lsn`, never down: losing a higher floor would apply events twice.
fn raised_floor(floor: Option<u64>, lsn: u64) -> u64 {
    floor.map_or(lsn, |floor| floor.max(lsn))
}
