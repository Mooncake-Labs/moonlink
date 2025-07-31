/// This file implements chaos test for table handler.
///
/// System invariants:
/// - Begin events happen only after end events
/// - End events happen only after begin events
/// - Rows to delete comes from committed appended ones
/// - LSN always increases
use crate::event_sync::create_table_event_syncer;
use crate::row::{MoonlinkRow, RowValue};
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::gcs_test_utils::*;
#[cfg(feature = "storage-gcs")]
use crate::storage::filesystem::gcs::test_guard::TestGuard as GcsTestGuard;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::s3_test_utils::*;
#[cfg(feature = "storage-s3")]
use crate::storage::filesystem::s3::test_guard::TestGuard as S3TestGuard;
use crate::storage::mooncake_table::{table_creation_test_utils::*, TableMetadata};
use crate::table_handler::test_utils::*;
use crate::table_handler::{TableEvent, TableHandler};
use crate::union_read::ReadStateManager;
use crate::{IcebergTableConfig, ObjectStorageCache};
use crate::{StorageConfig, TableEventManager};

use more_asserts as ma;
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::{tempdir, TempDir};
use tokio::sync::mpsc;
use tokio::sync::watch;

/// To avoid excessive and continuous table maintenance operations, set an interval between each invocation for each non table update operation.
const NON_UPDATE_COMMAND_INTERVAL_LSN: u64 = 5;

/// Create a test moonlink row.
fn create_row(id: i32, name: &str, age: i32) -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(id),
        RowValue::ByteArray(name.as_bytes().to_vec()),
        RowValue::Int32(age),
    ])
}

/// Events randomly selected for chaos test.
#[derive(Debug)]
struct ChaosEvent {
    table_events: Vec<TableEvent>,
    table_maintenance_event: Option<TableEvent>,
    snapshot_read_lsn: Option<u64>,
    force_snapshot_lsn: Option<u64>,
}

impl ChaosEvent {
    fn create_table_events(table_events: Vec<TableEvent>) -> Self {
        Self {
            table_events,
            table_maintenance_event: None,
            snapshot_read_lsn: None,
            force_snapshot_lsn: None,
        }
    }
    fn create_table_maintenance_event(table_event: TableEvent) -> Self {
        Self {
            table_events: vec![],
            table_maintenance_event: Some(table_event),
            snapshot_read_lsn: None,
            force_snapshot_lsn: None,
        }
    }
    fn create_snapshot_read(lsn: u64) -> Self {
        Self {
            table_events: vec![],
            table_maintenance_event: None,
            snapshot_read_lsn: Some(lsn),
            force_snapshot_lsn: None,
        }
    }
    fn create_force_snapshot(lsn: u64) -> Self {
        Self {
            table_events: vec![],
            table_maintenance_event: None,
            snapshot_read_lsn: None,
            force_snapshot_lsn: Some(lsn),
        }
    }
}

#[derive(Default)]
struct NonTableUpdateCmdCall {
    /// LSN (value of [`cur_lsn`]) for the last read snapshot invocation.
    read_snapshot_lsn: u64,
    /// LSN (value of [`cur_lsn`]) for the last force snapshot invocation.
    force_snapshot_lsn: u64,
    /// LSN (value of [`cur_lsn`]) for the last force index merge invocation.
    force_index_merge_lsn: u64,
    /// LSN (value of [`cur_lsn`]) for the last force data compaction invocation.
    force_data_compaction_lsn: u64,
}

#[derive(Debug, Clone)]
enum EventKind {
    BeginStreamingTxn,
    BeginNonStreamingTxn,
    Append,
    Delete,
    StreamAbort,
    StreamFlush,
    EndWithFlush,
    EndNoFlush,
    ReadSnapshot,
    /// Foreground force snapshot only happens after commit operation, otherwise it gets blocked.
    ForegroundForceSnapshot,
    /// Foreground force table maintenance only happens after commit operation, otherwise it gets blocked.
    ForegroundForceIndexMerge,
    ForegroundForceDataCompaction,
}

#[derive(Clone, Debug, PartialEq)]
enum TxnState {
    /// No active transaction ongoing.
    Empty,
    /// Within a non-streaming transaction.
    InNonStreaming,
    /// Within a streaming transaction.
    InStreaming,
}

struct ChaosState {
    /// Used to generate random events, with current timestamp as random seed.
    rng: StdRng,
    /// Non table update operation invocation status.
    non_table_update_cmd_call: NonTableUpdateCmdCall,
    /// Used to generate rows to insert.
    next_id: i32,
    /// Inserted rows in committed transactions.
    committed_inserted_rows: VecDeque<(i32 /*id*/, MoonlinkRow)>,
    /// Inserted rows in the current uncommitted transaction.
    uncommitted_inserted_rows: VecDeque<(i32 /*id*/, MoonlinkRow)>,
    /// Deleted committed row ids in the current uncommitted transaction.
    deleted_committed_row_ids: HashSet<i32>,
    /// Deleted uncommitted row ids in the current uncommitted transaction.
    /// Notice: only stream transactions are able to delete uncommitted rows.
    deleted_uncommitted_row_ids: HashSet<i32>,
    /// Used to indicate whether there's an ongoing transaction.
    txn_state: TxnState,
    /// LSN to use for the next operation, including update operations and commits.
    cur_lsn: u64,
    /// Txn id used for streaming transaction.
    cur_xact_id: u32,
    /// Used to read snapshot.
    read_state_manager: ReadStateManager,
    /// Last commit LSN.
    last_commit_lsn: Option<u64>,
    /// Whether the last finished transaction committed successfully, or not.
    last_txn_is_committed: bool,
}

impl ChaosState {
    fn new(read_state_manager: ReadStateManager) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let rng = StdRng::seed_from_u64(nanos as u64);
        Self {
            rng,
            non_table_update_cmd_call: NonTableUpdateCmdCall::default(),
            txn_state: TxnState::Empty,
            next_id: 0,
            committed_inserted_rows: VecDeque::new(),
            uncommitted_inserted_rows: VecDeque::new(),
            deleted_committed_row_ids: HashSet::new(),
            deleted_uncommitted_row_ids: HashSet::new(),
            read_state_manager,
            cur_lsn: 0,
            cur_xact_id: 0,
            last_commit_lsn: None,
            last_txn_is_committed: false,
        }
    }

    /// Get the current LSN to use for the current operation, and increment.
    fn get_and_update_cur_lsn(&mut self) -> u64 {
        let cur_lsn = self.cur_lsn;
        self.cur_lsn += 1;
        cur_lsn
    }

    /// Assert on preconditions to start a new transaction, whether it's streaming one or non-streaming one.
    fn assert_txn_begin_precondition(&self) {
        assert_eq!(self.txn_state, TxnState::Empty);
        assert!(self.uncommitted_inserted_rows.is_empty());
        assert!(self.deleted_committed_row_ids.is_empty());
        assert!(self.deleted_uncommitted_row_ids.is_empty());
    }

    /// Start a streaming transaction, and return xact id to use for current transaction.
    fn begin_streaming_txn(&mut self) {
        self.assert_txn_begin_precondition();
        self.txn_state = TxnState::InStreaming;
    }

    fn begin_non_streaming_txn(&mut self) {
        self.assert_txn_begin_precondition();
        self.txn_state = TxnState::InNonStreaming;
    }

    /// Abort the current stream transaction.
    fn stream_abort_transaction(&mut self) {
        assert_eq!(self.txn_state, TxnState::InStreaming);
        self.txn_state = TxnState::Empty;
        self.cur_xact_id += 1;
        self.uncommitted_inserted_rows.clear();
        self.deleted_committed_row_ids.clear();
        self.deleted_uncommitted_row_ids.clear();
        self.last_txn_is_committed = false;
    }

    fn commit_transaction(&mut self, lsn: u64) {
        // Update transaction id if streaming one.
        if self.txn_state == TxnState::InStreaming {
            self.cur_xact_id += 1;
        }

        // Set chaos test states.
        assert_ne!(self.txn_state, TxnState::Empty);
        self.txn_state = TxnState::Empty;
        self.last_commit_lsn = Some(lsn);
        self.last_txn_is_committed = true;

        // Set table states.
        self.committed_inserted_rows
            .extend(self.uncommitted_inserted_rows.drain(..));
        self.committed_inserted_rows.retain(|(id, _)| {
            !self.deleted_committed_row_ids.contains(id)
                && !self.deleted_uncommitted_row_ids.contains(id)
        });
        self.deleted_committed_row_ids.clear();
        self.deleted_uncommitted_row_ids.clear();
    }

    /// Get transaction id to set for both streaming and non-streaming transactions.
    fn get_cur_xact_id(&self) -> Option<u32> {
        if self.txn_state == TxnState::InStreaming {
            Some(self.cur_xact_id)
        } else {
            None
        }
    }

    fn get_next_row_to_append(&mut self) -> MoonlinkRow {
        let row = create_row(self.next_id, /*name=*/ "user", self.next_id % 5);
        self.uncommitted_inserted_rows
            .push_back((self.next_id, row.clone()));
        self.next_id += 1;
        row
    }

    /// Return all [`id`] fields for the moonlink rows which haven't been deleted in the alphabetical order.
    fn get_valid_ids(&self) -> Vec<i32> {
        self.committed_inserted_rows
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>()
    }

    /// Return whether we could delete a row in the next event.
    fn can_delete(&self) -> bool {
        // There're undeleted committed records.
        if !self.committed_inserted_rows.is_empty()
            && self.deleted_committed_row_ids.len() != self.committed_inserted_rows.len()
        {
            ma::assert_lt!(
                self.deleted_committed_row_ids.len(),
                self.committed_inserted_rows.len()
            );
            return true;
        }

        // Streaming transactions are allowed to delete rows inserted in the current transaction.
        if self.txn_state == TxnState::InStreaming
            && self.deleted_uncommitted_row_ids.len() != self.uncommitted_inserted_rows.len()
        {
            ma::assert_lt!(
                self.deleted_uncommitted_row_ids.len(),
                self.uncommitted_inserted_rows.len()
            );
            return true;
        }

        false
    }

    /// Get a random row to delete.
    fn get_random_row_to_delete(&mut self) -> MoonlinkRow {
        let mut candidates: Vec<(i32, MoonlinkRow, bool /*committed*/)> = self
            .committed_inserted_rows
            .iter()
            .filter(|(id, _)| !self.deleted_committed_row_ids.contains(id))
            .map(|(id, row)| (*id, row.clone(), /*committed=*/ true))
            .collect();

        // If within a streaming transaction, could also delete from uncommitted inserted rows.
        if self.txn_state == TxnState::InStreaming {
            candidates.extend(
                self.uncommitted_inserted_rows
                    .iter()
                    .filter(|(id, _)| !self.deleted_uncommitted_row_ids.contains(id))
                    .map(|(id, row)| (*id, row.clone(), /*committed=*/ false)),
            );
        }
        assert!(!candidates.is_empty());

        // Randomly pick one row from the candidates.
        let random_idx = self.rng.random_range(0..candidates.len());
        let (id, row, is_committed) = candidates[random_idx].clone();

        // Update deleted rows set.
        if is_committed {
            assert!(self.deleted_committed_row_ids.insert(id));
        } else {
            assert!(self.deleted_uncommitted_row_ids.insert(id));
        }

        row
    }

    /// Attempt to push non table update operations to choices.
    fn try_push_read_snapshot_cmd(&mut self, choices: &mut Vec<EventKind>) {
        if self.last_commit_lsn.is_some()
            && self.cur_lsn - self.non_table_update_cmd_call.read_snapshot_lsn
                >= NON_UPDATE_COMMAND_INTERVAL_LSN
        {
            choices.push(EventKind::ReadSnapshot);
            self.non_table_update_cmd_call.read_snapshot_lsn = self.cur_lsn;
        }
    }
    fn try_push_table_maintenance_cmd(&mut self, choices: &mut Vec<EventKind>) {
        if self.last_commit_lsn.is_none() {
            return;
        }

        // Foreground table maintenance operations happen after a sucessfully committed transaction.
        if self.uncommitted_inserted_rows.is_empty()
            && self.uncommitted_inserted_rows.is_empty()
            && self.last_txn_is_committed
        {
            if self.cur_lsn - self.non_table_update_cmd_call.force_snapshot_lsn
                >= NON_UPDATE_COMMAND_INTERVAL_LSN
            {
                choices.push(EventKind::ForegroundForceSnapshot);
                self.non_table_update_cmd_call.force_snapshot_lsn = self.cur_lsn;
            }
            if self.cur_lsn - self.non_table_update_cmd_call.force_index_merge_lsn
                >= NON_UPDATE_COMMAND_INTERVAL_LSN
            {
                choices.push(EventKind::ForegroundForceIndexMerge);
                self.non_table_update_cmd_call.force_index_merge_lsn = self.cur_lsn;
            }
            if self.cur_lsn - self.non_table_update_cmd_call.force_data_compaction_lsn
                >= NON_UPDATE_COMMAND_INTERVAL_LSN
            {
                choices.push(EventKind::ForegroundForceDataCompaction);
                self.non_table_update_cmd_call.force_data_compaction_lsn = self.cur_lsn;
            }
        }
    }

    fn generate_random_events(&mut self) -> ChaosEvent {
        let mut choices = vec![];

        self.try_push_read_snapshot_cmd(&mut choices);
        self.try_push_table_maintenance_cmd(&mut choices);
        if self.txn_state == TxnState::Empty {
            choices.push(EventKind::BeginStreamingTxn);
            choices.push(EventKind::BeginNonStreamingTxn);
        } else {
            choices.push(EventKind::Append);
            if self.can_delete() {
                choices.push(EventKind::Delete);
            }
            if self.txn_state == TxnState::InStreaming {
                choices.push(EventKind::StreamFlush);
                choices.push(EventKind::StreamAbort);
            }
            choices.push(EventKind::EndWithFlush);
            choices.push(EventKind::EndNoFlush);
        }

        match *choices.choose(&mut self.rng).unwrap() {
            EventKind::ReadSnapshot => {
                ChaosEvent::create_snapshot_read(self.last_commit_lsn.unwrap())
            }
            EventKind::ForegroundForceSnapshot => {
                ChaosEvent::create_force_snapshot(self.last_commit_lsn.unwrap())
            }
            EventKind::ForegroundForceIndexMerge => {
                ChaosEvent::create_table_maintenance_event(TableEvent::ForceRegularIndexMerge)
            }
            EventKind::ForegroundForceDataCompaction => {
                ChaosEvent::create_table_maintenance_event(TableEvent::ForceRegularDataCompaction)
            }
            EventKind::BeginStreamingTxn => {
                self.begin_streaming_txn();
                let row = self.get_next_row_to_append();
                ChaosEvent::create_table_events(vec![TableEvent::Append {
                    row,
                    xact_id: self.get_cur_xact_id(),
                    lsn: self.get_and_update_cur_lsn(),
                    is_copied: false,
                }])
            }
            EventKind::BeginNonStreamingTxn => {
                self.begin_non_streaming_txn();
                let row = self.get_next_row_to_append();
                ChaosEvent::create_table_events(vec![TableEvent::Append {
                    row,
                    xact_id: self.get_cur_xact_id(),
                    lsn: self.get_and_update_cur_lsn(),
                    is_copied: false,
                }])
            }
            EventKind::Append => ChaosEvent::create_table_events(vec![TableEvent::Append {
                row: self.get_next_row_to_append(),
                xact_id: self.get_cur_xact_id(),
                lsn: self.get_and_update_cur_lsn(),
                is_copied: false,
            }]),
            EventKind::Delete => ChaosEvent::create_table_events(vec![TableEvent::Delete {
                row: self.get_random_row_to_delete(),
                xact_id: self.get_cur_xact_id(),
                lsn: self.get_and_update_cur_lsn(),
            }]),
            EventKind::StreamFlush => {
                ChaosEvent::create_table_events(vec![TableEvent::StreamFlush {
                    xact_id: self.get_cur_xact_id().unwrap(),
                }])
            }
            EventKind::StreamAbort => {
                let xact_id = self.get_cur_xact_id().unwrap();
                self.stream_abort_transaction();
                ChaosEvent::create_table_events(vec![TableEvent::StreamAbort { xact_id }])
            }
            EventKind::EndWithFlush => {
                let lsn = self.get_and_update_cur_lsn();
                let xact_id = self.get_cur_xact_id();
                self.commit_transaction(lsn);
                ChaosEvent::create_table_events(vec![TableEvent::CommitFlush { lsn, xact_id }])
            }
            EventKind::EndNoFlush => {
                let lsn = self.get_and_update_cur_lsn();
                let xact_id = self.get_cur_xact_id();
                self.commit_transaction(lsn);
                ChaosEvent::create_table_events(vec![TableEvent::Commit { lsn, xact_id }])
            }
        }
    }
}

#[derive(Clone, Debug)]
enum TableMainenanceOption {
    /// No table maintenance in background.
    NoTableMaintenance,
    /// Index merge is enabled by default: merge take place as long as there're at least two index files.
    IndexMerge,
    /// Data compaction is enabled by default: compaction take place as long as there're at least two data files.
    DataCompaction,
}

#[derive(Clone, Debug)]
struct TestEnvConfig {
    /// Table background maintenance option.
    maintenance_option: TableMainenanceOption,
    /// Event count.
    event_count: usize,
    /// Whether error injection is enabled.
    error_injection_enabled: bool,
    /// Filesystem storage config for persistence.
    storage_config: StorageConfig,
}

#[allow(dead_code)]
struct TestEnvironment {
    test_env_config: TestEnvConfig,
    cache_temp_dir: TempDir,
    table_temp_dir: TempDir,
    object_storage_cache: ObjectStorageCache,
    read_state_manager: ReadStateManager,
    table_event_manager: TableEventManager,
    table_handler: TableHandler,
    event_sender: mpsc::Sender<TableEvent>,
    event_replay_rx: mpsc::UnboundedReceiver<TableEvent>,
    wal_flush_lsn_rx: watch::Receiver<u64>,
    last_commit_lsn_tx: watch::Sender<u64>,
    replication_lsn_tx: watch::Sender<u64>,
    mooncake_table_metadata: Arc<TableMetadata>,
    iceberg_table_config: IcebergTableConfig,
}

impl TestEnvironment {
    async fn new(config: TestEnvConfig) -> Self {
        let table_temp_dir = tempdir().unwrap();
        let mooncake_table_metadata = match &config.maintenance_option {
            TableMainenanceOption::NoTableMaintenance => create_test_table_metadata_disable_flush(
                table_temp_dir.path().to_str().unwrap().to_string(),
            ),
            TableMainenanceOption::IndexMerge => {
                create_test_table_metadata_with_index_merge_disable_flush(
                    table_temp_dir.path().to_str().unwrap().to_string(),
                )
            }
            TableMainenanceOption::DataCompaction => {
                create_test_table_metadata_with_data_compaction_disable_flush(
                    table_temp_dir.path().to_str().unwrap().to_string(),
                )
            }
        };

        // Local filesystem to store read-through cache.
        let cache_temp_dir = tempdir().unwrap();
        let object_storage_cache = ObjectStorageCache::default_for_test(&cache_temp_dir);

        // Create mooncake table and table event notification receiver.
        let iceberg_table_config = if config.error_injection_enabled {
            get_iceberg_table_config_with_chaos_injection(config.storage_config.clone())
        } else {
            get_iceberg_table_config_with_storage_config(config.storage_config.clone())
        };
        let table = create_mooncake_table(
            mooncake_table_metadata.clone(),
            iceberg_table_config.clone(),
            object_storage_cache.clone(),
        )
        .await;
        let (replication_lsn_tx, replication_lsn_rx) = watch::channel(0u64);
        let (last_commit_lsn_tx, last_commit_lsn_rx) = watch::channel(0u64);
        let read_state_manager =
            ReadStateManager::new(&table, replication_lsn_rx.clone(), last_commit_lsn_rx);
        let (table_event_sync_sender, table_event_sync_receiver) = create_table_event_syncer();
        let (event_replay_tx, event_replay_rx) = mpsc::unbounded_channel();
        let table_handler = TableHandler::new(
            table,
            table_event_sync_sender,
            replication_lsn_rx.clone(),
            Some(event_replay_tx),
        )
        .await;
        let wal_flush_lsn_rx = table_event_sync_receiver.wal_flush_lsn_rx.clone();
        let table_event_manager =
            TableEventManager::new(table_handler.get_event_sender(), table_event_sync_receiver);
        let event_sender = table_handler.get_event_sender();

        Self {
            test_env_config: config,
            cache_temp_dir,
            table_temp_dir,
            object_storage_cache,
            table_event_manager,
            read_state_manager,
            table_handler,
            event_sender,
            event_replay_rx,
            wal_flush_lsn_rx,
            replication_lsn_tx,
            last_commit_lsn_tx,
            mooncake_table_metadata,
            iceberg_table_config,
        }
    }
}

/// Test util function to check whether iceberg snapshot contains expected content.
async fn validate_persisted_iceberg_table(
    mooncake_table_metadata: Arc<TableMetadata>,
    iceberg_table_config: IcebergTableConfig,
    snapshot_lsn: u64,
    expected_ids: Vec<i32>,
) {
    let (event_sender, _event_receiver) = mpsc::channel(100);
    let (replication_lsn_tx, replication_lsn_rx) = watch::channel(0u64);
    let (last_commit_lsn_tx, last_commit_lsn_rx) = watch::channel(0u64);
    replication_lsn_tx.send(snapshot_lsn).unwrap();
    last_commit_lsn_tx.send(snapshot_lsn).unwrap();

    // Use a fresh new cache for new iceberg table manager.
    let cache_temp_dir = tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&cache_temp_dir);

    let mut table = create_mooncake_table(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
        object_storage_cache,
    )
    .await;
    table.register_table_notify(event_sender).await;

    let read_state_manager =
        ReadStateManager::new(&table, replication_lsn_rx.clone(), last_commit_lsn_rx);
    check_read_snapshot(
        &read_state_manager,
        Some(snapshot_lsn),
        /*expected_ids=*/ &expected_ids,
    )
    .await;
}

async fn chaos_test_impl(mut env: TestEnvironment) {
    let test_env_config = env.test_env_config.clone();
    let event_sender = env.event_sender.clone();
    let read_state_manager = env.read_state_manager;
    let mut table_event_manager = env.table_event_manager;
    let last_commit_lsn_tx = env.last_commit_lsn_tx.clone();
    let replication_lsn_tx = env.replication_lsn_tx.clone();

    // Fields used to recreate a new mooncake table.
    let mooncake_table_metadata = env.mooncake_table_metadata.clone();
    let iceberg_table_config = env.iceberg_table_config.clone();

    let task = tokio::spawn(async move {
        let mut state = ChaosState::new(read_state_manager);

        for _ in 0..test_env_config.event_count {
            let chaos_events = state.generate_random_events();

            // Perform table maintenance operations.
            if let Some(TableEvent::ForceRegularIndexMerge) = &chaos_events.table_maintenance_event
            {
                let mut rx = table_event_manager.initiate_index_merge().await;
                rx.recv().await.unwrap().unwrap();
            }
            if let Some(TableEvent::ForceRegularDataCompaction) =
                &chaos_events.table_maintenance_event
            {
                let mut rx = table_event_manager.initiate_data_compaction().await;
                rx.recv().await.unwrap().unwrap();
            }

            // Perform table update operations.
            for cur_event in chaos_events.table_events.into_iter() {
                // For commit events, need to set up corresponding replication and commit LSN.
                if let TableEvent::Commit { lsn, .. } = cur_event {
                    replication_lsn_tx.send(lsn).unwrap();
                    last_commit_lsn_tx.send(lsn).unwrap();
                } else if let TableEvent::CommitFlush { lsn, .. } = cur_event {
                    replication_lsn_tx.send(lsn).unwrap();
                    last_commit_lsn_tx.send(lsn).unwrap();
                }
                event_sender.send(cur_event).await.unwrap();
            }

            // Perform snapshot read operation and check.
            if let Some(read_lsn) = chaos_events.snapshot_read_lsn {
                let requested_read_lsn = if read_lsn == 0 { None } else { Some(read_lsn) };
                let expected_ids = state.get_valid_ids();
                check_read_snapshot(
                    &state.read_state_manager,
                    requested_read_lsn,
                    /*expected_ids=*/ &expected_ids,
                )
                .await;
            }

            // Perform force snapshot and check.
            if let Some(snapshot_lsn) = chaos_events.force_snapshot_lsn {
                let rx = table_event_manager.initiate_snapshot(snapshot_lsn).await;
                TableEventManager::synchronize_force_snapshot_request(rx, snapshot_lsn)
                    .await
                    .unwrap();

                // Now iceberg snapshot content should be exactly the same as moooncake table, recover states from persistence layer and perform another read.
                validate_persisted_iceberg_table(
                    mooncake_table_metadata.clone(),
                    iceberg_table_config.clone(),
                    snapshot_lsn,
                    state.get_valid_ids(),
                )
                .await;
            }
        }

        // TODO(hjiang): Temporarily hard code a sleep time to trigger background tasks.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // If anything bad happens in the eventloop, drop table would fail.
        table_event_manager.drop_table().await.unwrap();
    });

    // Await the task directly and handle its result.
    let task_result = task.await;

    // Print out events in order if chaos test fails.
    if let Err(e) = task_result {
        // Display all enqueued events for debugging and replay.
        while let Some(cur_event) = env.event_replay_rx.recv().await {
            println!("{cur_event:?}");
        }
        // Propagate the panic to fail the test.
        if let Ok(panic) = e.try_into_panic() {
            std::panic::resume_unwind(panic);
        }
    }
}

/// ============================
/// Local filesystem persistence
/// ============================
///
/// Chaos test with no background table maintenance enabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos_with_no_background_maintenance() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        maintenance_option: TableMainenanceOption::NoTableMaintenance,
        error_injection_enabled: false,
        event_count: 3000,
        storage_config: StorageConfig::FileSystem { root_directory },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with index merge enabled by default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos_with_index_merge() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        maintenance_option: TableMainenanceOption::IndexMerge,
        error_injection_enabled: false,
        event_count: 3000,
        storage_config: StorageConfig::FileSystem { root_directory },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with data compaction enabled by default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos_with_data_compaction() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        maintenance_option: TableMainenanceOption::DataCompaction,
        error_injection_enabled: false,
        event_count: 3000,
        storage_config: StorageConfig::FileSystem { root_directory },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// ============================
/// S3 persistence
/// ============================
///
/// Chaos test with no background table maintenance enabled.
#[cfg(feature = "storage-s3")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_chaos_with_no_background_maintenance() {
    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let accessor_config = create_s3_storage_config(&warehouse_uri);
    let test_env_config = TestEnvConfig {
        maintenance_option: TableMainenanceOption::NoTableMaintenance,
        error_injection_enabled: false,
        event_count: 3000,
        storage_config: accessor_config.storage_config,
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with index merge enabled by default.
#[cfg(feature = "storage-s3")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_chaos_with_index_merge() {
    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let accessor_config = create_s3_storage_config(&warehouse_uri);
    let test_env_config = TestEnvConfig {
        maintenance_option: TableMainenanceOption::IndexMerge,
        error_injection_enabled: false,
        event_count: 3000,
        storage_config: accessor_config.storage_config,
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with data compaction enabled by default.
#[cfg(feature = "storage-s3")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_chaos_with_data_compaction() {
    let (bucket, warehouse_uri) = get_test_s3_bucket_and_warehouse();
    let _test_guard = S3TestGuard::new(bucket.clone()).await;
    let accessor_config = create_s3_storage_config(&warehouse_uri);
    let test_env_config = TestEnvConfig {
        maintenance_option: TableMainenanceOption::DataCompaction,
        error_injection_enabled: false,
        event_count: 3000,
        storage_config: accessor_config.storage_config,
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// ============================
/// GCS persistence
/// ============================
///
/// Chaos test with no background table maintenance enabled.
#[cfg(feature = "storage-gcs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gcs_chaos_with_no_background_maintenance() {
    let (bucket, warehouse_uri) = get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let accessor_config = create_gcs_storage_config(&warehouse_uri);
    let test_env_config = TestEnvConfig {
        maintenance_option: TableMainenanceOption::NoTableMaintenance,
        error_injection_enabled: false,
        event_count: 3000,
        storage_config: accessor_config.storage_config,
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with index merge enabled by default.
#[cfg(feature = "storage-gcs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gcs_chaos_with_index_merge() {
    let (bucket, warehouse_uri) = get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let accessor_config = create_gcs_storage_config(&warehouse_uri);
    let test_env_config = TestEnvConfig {
        maintenance_option: TableMainenanceOption::IndexMerge,
        error_injection_enabled: false,
        event_count: 3000,
        storage_config: accessor_config.storage_config,
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with data compaction enabled by default.
#[cfg(feature = "storage-gcs")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_gcs_chaos_with_data_compaction() {
    let (bucket, warehouse_uri) = get_test_gcs_bucket_and_warehouse();
    let _test_guard = GcsTestGuard::new(bucket.clone()).await;
    let accessor_config = create_gcs_storage_config(&warehouse_uri);
    let test_env_config = TestEnvConfig {
        maintenance_option: TableMainenanceOption::DataCompaction,
        error_injection_enabled: false,
        event_count: 3000,
        storage_config: accessor_config.storage_config,
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// ============================
/// Delay and error injection
/// ============================
///
/// Chaos test with no background table maintenance enabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos_with_no_background_maintenance_with_chaos_injection() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        maintenance_option: TableMainenanceOption::NoTableMaintenance,
        error_injection_enabled: true,
        event_count: 100,
        storage_config: StorageConfig::FileSystem { root_directory },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with index merge enabled by default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos_with_index_merge_with_chaos_injection() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        maintenance_option: TableMainenanceOption::IndexMerge,
        error_injection_enabled: true,
        event_count: 100,
        storage_config: StorageConfig::FileSystem { root_directory },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}

/// Chaos test with data compaction enabled by default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos_with_data_compaction_with_chaos_injection() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        maintenance_option: TableMainenanceOption::DataCompaction,
        error_injection_enabled: true,
        event_count: 100,
        storage_config: StorageConfig::FileSystem { root_directory },
    };
    let env = TestEnvironment::new(test_env_config).await;
    chaos_test_impl(env).await;
}
