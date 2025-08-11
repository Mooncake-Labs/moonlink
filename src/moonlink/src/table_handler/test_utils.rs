use crate::event_sync::create_table_event_syncer;
use crate::row::{IdentityProp, MoonlinkRow, RowValue};
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::mooncake_table::table_creation_test_utils::create_test_arrow_schema;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::get_read_state_filepath_remap;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::wal::test_utils::{
    assert_wal_events_contains, assert_wal_events_does_not_contain,
};
use crate::storage::wal::test_utils::{wal_file_exists, WAL_TEST_TABLE_ID};
use crate::storage::wal::{PersistentWalMetadata, WalManager};
use crate::storage::IcebergTableConfig;
use crate::storage::{verify_files_and_deletions, MooncakeTable};
use crate::table_handler::{TableEvent, TableHandler};
use crate::table_handler_timer::create_table_handler_timers;
use crate::union_read::{decode_read_state_for_testing, ReadStateManager};
use crate::{
    FileSystemAccessor, IcebergTableManager, MooncakeTableConfig, StorageConfig, TableEventManager,
    WalConfig, WalTransactionState,
};
use crate::{ObjectStorageCache, Result};

use arrow_array::RecordBatch;
use futures::StreamExt;
use iceberg::io::FileIOBuilder;
use iceberg::io::FileRead;
use more_asserts as ma;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use tokio::sync::{mpsc, watch};
use tracing::debug;

/// Creates a `MoonlinkRow` for testing purposes.
pub fn create_row(id: i32, name: &str, age: i32) -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(id),
        RowValue::ByteArray(name.as_bytes().to_vec()),
        RowValue::Int32(age),
    ])
}

/// Get iceberg table manager config.
pub fn get_iceberg_manager_config(table_name: String, warehouse_uri: String) -> IcebergTableConfig {
    let storage_config = StorageConfig::FileSystem {
        root_directory: warehouse_uri,
        atomic_write_dir: None,
    };
    IcebergTableConfig {
        namespace: vec!["default".to_string()],
        table_name,
        accessor_config: AccessorConfig::new_with_storage_config(storage_config),
    }
}

/// Holds the common environment components for table handler tests.
pub struct TestEnvironment {
    pub handler: TableHandler,
    event_sender: mpsc::Sender<TableEvent>,
    read_state_manager: Option<Arc<ReadStateManager>>,
    replication_tx: watch::Sender<u64>,
    last_commit_tx: watch::Sender<u64>,
    snapshot_lsn_tx: watch::Sender<u64>,
    pub(crate) wal_filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
    wal_filesystem_path: String,
    pub(crate) force_snapshot_completion_rx: watch::Receiver<Option<Result<u64>>>,
    pub(crate) wal_flush_lsn_rx: watch::Receiver<u64>,
    pub(crate) table_event_manager: TableEventManager,
    pub(crate) temp_dir: TempDir,
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        // Dropping read state manager involves asynchronous operation, which depends on table handler.
        // explicitly destruct read state manager first, and sleep for a while, to "make sure" async destruction finishes.
        self.read_state_manager = None;
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

impl TestEnvironment {
    /// Creates a default test environment with default settings.
    pub async fn default() -> Self {
        let temp_dir = tempdir().unwrap();
        let mooncake_table_config =
            MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
        Self::new(temp_dir, mooncake_table_config).await
    }

    /// Create a new test environment with the given mooncake table.
    pub(crate) async fn new_with_mooncake_table(
        temp_dir: TempDir,
        mooncake_table: MooncakeTable,
    ) -> Self {
        let (replication_tx, replication_rx) = watch::channel(0u64);
        let (last_commit_tx, last_commit_rx) = watch::channel(0u64);
        let snapshot_lsn_tx = mooncake_table.get_snapshot_watch_sender().clone();
        let read_state_manager = Some(Arc::new(ReadStateManager::new(
            &mooncake_table,
            replication_rx.clone(),
            last_commit_rx,
            get_read_state_filepath_remap(),
        )));
        let (table_event_sync_sender, table_event_sync_receiver) = create_table_event_syncer();
        let force_snapshot_completion_rx = table_event_sync_receiver
            .force_snapshot_completion_rx
            .clone();
        let wal_flush_lsn_rx = table_event_sync_receiver.wal_flush_lsn_rx.clone();

        // TODO(Paul): Change this default when we support object storage for WAL
        let default_wal_config =
            WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, temp_dir.path());
        let wal_filesystem_path = default_wal_config.get_accessor_config().get_root_path();
        let wal_filesystem_accessor = Arc::new(FileSystemAccessor::new(
            default_wal_config.get_accessor_config().clone(),
        ));
        let table_handler_timer = create_table_handler_timers();

        let handler = TableHandler::new(
            mooncake_table,
            table_event_sync_sender,
            table_handler_timer,
            replication_rx.clone(),
            /*event_replay_tx=*/ None,
        )
        .await;
        let table_event_manager =
            TableEventManager::new(handler.get_event_sender(), table_event_sync_receiver);
        let event_sender = handler.get_event_sender();

        Self {
            handler,
            event_sender,
            read_state_manager,
            replication_tx,
            last_commit_tx,
            snapshot_lsn_tx,
            wal_filesystem_accessor,
            wal_filesystem_path,
            force_snapshot_completion_rx,
            wal_flush_lsn_rx,
            table_event_manager,
            temp_dir,
        }
    }

    /// Creates a new test environment with default settings.
    pub async fn new(temp_dir: TempDir, mooncake_table_config: MooncakeTableConfig) -> Self {
        let path = temp_dir.path().to_path_buf();
        let table_name = "table_name";
        let iceberg_table_config =
            get_iceberg_manager_config(table_name.to_string(), path.to_str().unwrap().to_string());
        let wal_config = WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, temp_dir.path());
        let mooncake_table = MooncakeTable::new(
            (*create_test_arrow_schema()).clone(),
            table_name.to_string(),
            1,
            path,
            IdentityProp::Keys(vec![0]),
            iceberg_table_config.clone(),
            mooncake_table_config,
            wal_config,
            ObjectStorageCache::default_for_test(&temp_dir),
            create_test_filesystem_accessor(&iceberg_table_config),
        )
        .await
        .unwrap();

        Self::new_with_mooncake_table(temp_dir, mooncake_table).await
    }

    /// Create iceberg table manager.
    pub fn create_iceberg_table_manager(
        &self,
        mooncake_table_config: MooncakeTableConfig,
    ) -> IcebergTableManager {
        let table_name = "table_name";
        let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
            name: table_name.to_string(),
            table_id: 0,
            schema: create_test_arrow_schema(),
            config: mooncake_table_config.clone(),
            path: self.temp_dir.path().to_path_buf(),
            identity: IdentityProp::Keys(vec![0]),
        });
        let iceberg_table_config = get_iceberg_manager_config(
            table_name.to_string(),
            self.temp_dir.path().to_str().unwrap().to_string(),
        );
        IcebergTableManager::new(
            mooncake_table_metadata,
            // Create new and separate object storage cache for new iceberg table manager.
            ObjectStorageCache::default_for_test(&self.temp_dir),
            create_test_filesystem_accessor(&iceberg_table_config),
            iceberg_table_config.clone(),
        )
        .unwrap()
    }

    async fn send_event(&self, event: TableEvent) {
        self.event_sender
            .send(event)
            .await
            .expect("Failed to send event");
    }

    // --- Util functions for iceberg drop table ---

    /// Request to drop iceberg table and block wait its completion.
    pub async fn drop_table(&mut self) -> Result<()> {
        self.table_event_manager.drop_table().await
    }

    // --- Operation Helpers ---

    pub async fn append_row(
        &self,
        id: i32,
        name: &str,
        age: i32,
        lsn: u64,
        xact_id: Option<u32>,
    ) -> TableEvent {
        debug!(
            "append_row: id: {}, name: {}, age: {}, lsn: {}, xact_id: {:?}",
            id, name, age, lsn, xact_id
        );
        let row = create_row(id, name, age);
        let event = TableEvent::Append {
            is_copied: false,
            row,
            lsn,
            xact_id,
            is_recovery: false,
        };
        self.send_event(event.clone()).await;
        event
    }

    pub async fn delete_row(
        &self,
        id: i32,
        name: &str,
        age: i32,
        lsn: u64,
        xact_id: Option<u32>,
    ) -> TableEvent {
        let row = create_row(id, name, age);
        let event = TableEvent::Delete {
            row,
            lsn,
            xact_id,
            is_recovery: false,
        };
        self.send_event(event.clone()).await;
        event
    }

    pub async fn commit(&self, lsn: u64) -> TableEvent {
        let event = TableEvent::Commit {
            lsn,
            xact_id: None,
            is_recovery: false,
        };
        self.send_event(event.clone()).await;
        event
    }

    pub async fn stream_commit(&self, lsn: u64, xact_id: u32) -> TableEvent {
        let event = TableEvent::Commit {
            lsn,
            xact_id: Some(xact_id),
            is_recovery: false,
        };
        self.send_event(event.clone()).await;
        event
    }

    pub async fn stream_abort(&self, xact_id: u32) -> TableEvent {
        let event = TableEvent::StreamAbort {
            xact_id,
            is_recovery: false,
        };
        self.send_event(event.clone()).await;
        event
    }

    /// Force an index merge operation, and block wait its completion.
    pub async fn force_index_merge_and_sync(&mut self) -> Result<()> {
        let mut rx = self.table_event_manager.initiate_index_merge().await;
        rx.recv().await.unwrap()
    }

    /// Force a data compaction operation, and block wait its completion.
    pub async fn force_data_compaction_and_sync(&mut self) -> Result<()> {
        let mut rx = self.table_event_manager.initiate_data_compaction().await;
        rx.recv().await.unwrap()
    }

    /// Force a full table maintenance task operation, and block wait its completion.
    pub async fn force_full_maintenance_and_sync(&mut self) -> Result<()> {
        let mut rx = self.table_event_manager.initiate_full_compaction().await;
        rx.recv().await.unwrap()
    }

    pub async fn flush_table_and_sync(&mut self, lsn: u64, xact_id: Option<u32>) {
        self.send_event(TableEvent::CommitFlush {
            lsn,
            xact_id,
            is_recovery: false,
        })
        .await;
        self.send_event(TableEvent::ForceSnapshot { lsn: Some(lsn) })
            .await;
        TableEventManager::synchronize_force_snapshot_request(
            self.force_snapshot_completion_rx.clone(),
            lsn,
        )
        .await
        .unwrap();
    }

    pub async fn flush_table(&self, lsn: u64) {
        self.send_event(TableEvent::CommitFlush {
            lsn,
            xact_id: None,
            is_recovery: false,
        })
        .await;
    }

    pub async fn stream_flush(&self, xact_id: u32) {
        self.send_event(TableEvent::StreamFlush {
            xact_id,
            is_recovery: false,
        })
        .await;
    }

    // --- LSN and Verification Helpers ---

    /// Sets both table commit and replication LSN to the same value.
    /// This makes data up to `lsn` potentially readable.
    pub fn set_readable_lsn(&self, lsn: u64) {
        self.set_table_commit_lsn(lsn);
        self.replication_tx
            .send(lsn)
            .expect("Failed to send replication LSN");
    }

    /// Sets table commit LSN and a potentially higher replication LSN.
    pub fn set_readable_lsn_with_cap(&self, table_commit_lsn: u64, replication_cap_lsn: u64) {
        self.set_table_commit_lsn(table_commit_lsn);
        self.replication_tx
            .send(replication_cap_lsn.max(table_commit_lsn))
            .expect("Failed to send replication LSN");
    }

    /// Directly set the table commit LSN watch channel.
    pub fn set_table_commit_lsn(&self, lsn: u64) {
        self.last_commit_tx
            .send(lsn)
            .expect("Failed to send last commit LSN");
    }

    /// Directly set the replication LSN watch channel.
    pub fn set_replication_lsn(&self, lsn: u64) {
        self.replication_tx
            .send(lsn)
            .expect("Failed to send replication LSN");
    }

    pub fn set_snapshot_lsn(&self, lsn: u64) {
        self.snapshot_lsn_tx
            .send(lsn)
            .expect("Failed to send snapshot LSN");
    }

    pub async fn verify_snapshot(&self, target_lsn: u64, expected_ids: &[i32]) {
        check_read_snapshot(
            self.read_state_manager.as_ref().unwrap(),
            Some(target_lsn),
            expected_ids,
        )
        .await;
    }

    // --- Lifecycle Helper ---
    pub async fn shutdown(&mut self) {
        self.send_event(TableEvent::DropTable).await;
        if let Some(handle) = self.handler._event_handle.take() {
            handle.await.expect("TableHandler task panicked");
        }
    }

    /// Force WAL persistence by sending a periodic WAL event and waiting for completion
    /// Note that this assumes that this is being called after an event with an updated LSN
    /// has just been sent, if not it will wait indefinitely.
    pub async fn force_wal_persistence(&mut self, expected_lsn: u64) {
        self.send_event(TableEvent::PeriodicalPersistenceUpdateWal(
            uuid::Uuid::new_v4(),
        ))
        .await;

        loop {
            if *self.wal_flush_lsn_rx.borrow() >= expected_lsn {
                break;
            }
            self.wal_flush_lsn_rx.changed().await.unwrap();
        }
    }

    // TODO(Paul): Rework these when implementing object storage WAL
    /// Infers the lowest file number by looking at all remaining files in the WAL directory.
    pub async fn infer_lowest_wal_file_number(&self) -> Option<u64> {
        let mut files = tokio::fs::read_dir(PathBuf::from(&self.wal_filesystem_path))
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "Failed to read wal filesystem path: {}",
                    self.wal_filesystem_path
                )
            });
        let mut lowest = u64::MAX;

        while let Some(file) = files.next_entry().await.unwrap() {
            debug!("file: {:?}", file);
            let path = file.path();
            let file_name = path.file_name()?.to_str()?;

            // skip metadata file
            if file_name == WalManager::get_metadata_file_name() {
                continue;
            }

            if let Some(num_str) = file_name.strip_prefix("wal_")?.strip_suffix(".json") {
                if let Ok(num) = num_str.parse::<u64>() {
                    lowest = lowest.min(num);
                }
            }
        }
        (lowest != u64::MAX).then_some(lowest)
    }

    // Recover wal events locally by reading from the wal filesystem and finding the lowest file number
    // TODO(Paul): Rework these when implementing object storage WAL
    pub async fn get_wal_events_with_start_file_number(
        &self,
        start_file_num: u64,
    ) -> Vec<TableEvent> {
        let wal_events_stream = WalManager::recover_flushed_wals_flat(
            self.wal_filesystem_accessor.clone(),
            start_file_num,
        );
        let wal_events_vec = wal_events_stream
            .collect::<Vec<Result<TableEvent>>>()
            .await
            .into_iter()
            .map(|event| match event {
                // panic if there are any errors
                Ok(event) => event,
                Err(e) => {
                    panic!("Error recovering wal events: {e:?}");
                }
            })
            .collect::<Vec<TableEvent>>();
        wal_events_vec
    }

    pub async fn get_wal_events_inferring_lowest_file_number(&self) -> Vec<TableEvent> {
        let lowest_file_number = self.infer_lowest_wal_file_number().await.unwrap();
        self.get_wal_events_with_start_file_number(lowest_file_number)
            .await
    }

    /// Infers the lowest file number by looking at all remaining files in the WAL directory.
    pub async fn check_wal_events_inferring_lowest_file_number(
        &self,
        should_contain_table_events: &[TableEvent],
        should_not_contain_table_events: &[TableEvent],
    ) {
        let wal_events = self.get_wal_events_inferring_lowest_file_number().await;

        assert_wal_events_contains(&wal_events, should_contain_table_events);
        assert_wal_events_does_not_contain(&wal_events, should_not_contain_table_events);
    }

    pub async fn get_latest_wal_metadata(&self) -> PersistentWalMetadata {
        WalManager::recover_persistent_wal_metadata(self.wal_filesystem_accessor.clone()).await
    }

    pub async fn check_wal_events_from_metadata(
        &self,
        wal_metadata: &PersistentWalMetadata,
        should_contain_table_events: &[TableEvent],
        should_not_contain_table_events: &[TableEvent],
    ) {
        let live_wal_files_tracker = wal_metadata.get_live_wal_files_tracker();
        if wal_metadata.get_live_wal_files_tracker().is_empty() {
            assert!(
                should_contain_table_events.is_empty(),
                "No live wal files found in wal metadata but should contain events was not empty: {should_contain_table_events:?}"
            );
            return;
        }

        let start_file_number = live_wal_files_tracker.first().unwrap().file_number;

        let wal_events = self
            .get_wal_events_with_start_file_number(start_file_number)
            .await;

        assert_wal_events_contains(&wal_events, should_contain_table_events);
        assert_wal_events_does_not_contain(&wal_events, should_not_contain_table_events);
    }

    pub async fn check_wal_metadata_invariants(
        &self,
        metadata: &PersistentWalMetadata,
        iceberg_snapshot_lsn: Option<u64>,
        should_contain_xact_ids: Vec<u32>,
        should_not_contain_xact_ids: Vec<u32>,
    ) {
        assert_eq!(
            metadata.get_iceberg_snapshot_lsn(),
            iceberg_snapshot_lsn,
            "iceberg snapshot lsn should be the same"
        );

        // Assertions for active transaction tracking
        for (xact_id, xact_state) in metadata.get_active_transactions() {
            assert!(
                should_contain_xact_ids.contains(xact_id),
                "xact {xact_id} should be in the metadata"
            );
            assert!(
                !should_not_contain_xact_ids.contains(xact_id),
                "xact {xact_id} should not be in the metadata"
            );

            if let WalTransactionState::Commit { completion_lsn, .. }
            | WalTransactionState::Abort { completion_lsn, .. } = xact_state
            {
                if let Some(lsn) = iceberg_snapshot_lsn {
                    ma::assert_gt!(
                        *completion_lsn,
                        lsn,
                        "xact {xact_id} should have completion LSN > iceberg snapshot LSN"
                    );
                }
            }
        }

        // Assertions for main transaction tracking
        for i in 0..metadata.get_main_transaction_tracker().len() {
            let xact = &metadata.get_main_transaction_tracker()[i];
            if i != metadata.get_main_transaction_tracker().len() - 1 {
                if let WalTransactionState::Open { .. } = xact {
                    panic!("xact {xact:?} should not be open unless it is the last xact");
                }
            }
            if let WalTransactionState::Commit { completion_lsn, .. }
            | WalTransactionState::Abort { completion_lsn, .. } = xact
            {
                if let Some(lsn) = iceberg_snapshot_lsn {
                    ma::assert_gt!(
                        *completion_lsn,
                        lsn,
                        "xact {xact:?} should have completion LSN > iceberg snapshot LSN"
                    );
                }
            }
        }

        // Check consistency between files
        let active_wal_files = metadata.get_live_wal_files_tracker();

        let lowest_file_number_from_fs = self.infer_lowest_wal_file_number().await;
        let lowest_file_number_from_metadata =
            active_wal_files.first().map(|file| file.file_number);
        assert_eq!(
            lowest_file_number_from_fs, lowest_file_number_from_metadata,
            "lowest file number from fs and metadata should be the same"
        );

        for file in active_wal_files {
            assert!(
                wal_file_exists(self.wal_filesystem_accessor.clone(), file.file_number).await,
                "file {file_number} should exist",
                file_number = file.file_number
            );
        }
    }
}

/// Verifies the state of a read snapshot against expected row IDs.
pub async fn check_read_snapshot(
    read_manager: &ReadStateManager,
    target_lsn: Option<u64>,
    expected_ids: &[i32],
) {
    let read_state = read_manager.try_read(target_lsn).await.unwrap();
    let (data_files, puffin_files, deletion_vectors, position_deletes) =
        decode_read_state_for_testing(&read_state);

    if data_files.is_empty() && !expected_ids.is_empty() {
        unreachable!(
            "No snapshot files returned for LSN {:?} when rows (IDs: {:?}) were expected. Expected files because expected_ids is not empty.",
            target_lsn, expected_ids
        );
    }
    verify_files_and_deletions(
        &data_files,
        &puffin_files,
        position_deletes,
        deletion_vectors,
        expected_ids,
    )
    .await;
}

/// Test util function to load one arrow batch from the given local parquet file.
pub(crate) async fn load_one_arrow_batch(filepath: &str) -> RecordBatch {
    let file_io = FileIOBuilder::new_fs_io().build().unwrap();
    let input_file = file_io.new_input(filepath).unwrap();
    let input_file_metadata = input_file.metadata().await.unwrap();
    let reader = input_file.reader().await.unwrap();
    let bytes = reader.read(0..input_file_metadata.size).await.unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let mut reader = builder.build().unwrap();

    reader
        .next()
        .transpose()
        .unwrap()
        .expect("Should have one batch")
}
