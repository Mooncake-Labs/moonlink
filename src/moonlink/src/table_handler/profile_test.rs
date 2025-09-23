use crate::event_sync::create_table_event_syncer;
use crate::row::{IdentityProp, MoonlinkRow, RowValue};
use crate::storage::mooncake_table::table_event_manager::TableEventManager;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::storage_utils::TableId;
use crate::storage::MooncakeTable;
use crate::table_handler::{TableEvent, TableHandler};
use crate::table_handler_timer::create_table_handler_timers;
use crate::union_read::ReadStateManager;
use crate::{
    BaseFileSystemAccess, CacheTrait, DataCompactionConfig, DiskSliceWriterConfig,
    FileIndexMergeConfig, FileSystemAccessor, IcebergPersistenceConfig, WalConfig, WalManager,
};
use crate::{IcebergTableConfig, ObjectStorageCache, ObjectStorageCacheConfig, StorageConfig};

use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use function_name::named;
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::{tempdir, TempDir};
use tokio::sync::mpsc;
use tokio::sync::watch;

/// WAL ID used for testing.
const WAL_TEST_TABLE_ID: &str = "1";
/// Iceberg test namespace and table name.
const ICEBERG_TEST_NAMESPACE: &str = "namespace";
const ICEBERG_TEST_TABLE: &str = "test_table";
/// Test constant for table id.
const TEST_TABLE_ID: TableId = TableId(0);
/// Pprof profiling frequency.
const PPROF_PROFILE_FREQ: i32 = 99;

/// Create a test moonlink row.
fn create_row(id: i32, name: &str, age: i32) -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(id),
        RowValue::ByteArray(name.as_bytes().to_vec()),
        RowValue::Int32(age),
    ])
}

/// Events randomly selected for profile test.
#[derive(Debug)]
struct ProfileEvent {
    table_events: Vec<TableEvent>,
}

impl ProfileEvent {
    fn create_table_events(table_events: Vec<TableEvent>) -> Self {
        Self { table_events }
    }
}

#[derive(Debug, Clone)]
enum EventKind {
    BeginNonStreamingTxn,
    Append,
    Delete,
    EndNoFlush,
}

#[derive(Clone, Debug, PartialEq)]
enum TxnState {
    /// No active transaction ongoing.
    Empty,
    /// Within a non-streaming transaction.
    InNonStreaming,
}

struct ProfileState {
    /// Used to generate random events, with current timestamp as random seed.
    rng: StdRng,
    /// Whether to enable delete operations.
    append_only: bool,
    /// Whether to test upsert / delete if exists.
    is_upsert_table: bool,
    /// Used to generate rows to insert.
    next_id: i32,
    /// Inserted rows in committed transactions.
    committed_inserted_rows: VecDeque<(i32 /*id*/, MoonlinkRow)>,
    /// Inserted rows in the current uncommitted transaction.
    uncommitted_inserted_rows: VecDeque<(i32 /*id*/, MoonlinkRow)>,
    /// Deleted committed row ids in the current uncommitted transaction.
    deleted_committed_row_ids: HashSet<i32 /*id*/>,
    /// Used to indicate whether there's an ongoing transaction.
    txn_state: TxnState,
    /// LSN to use for the next operation, including update operations and commits.
    cur_lsn: u64,
    /// Last commit LSN.
    last_commit_lsn: Option<u64>,
    /// Whether the last finished transaction committed successfully, or not.
    last_txn_is_committed: bool,
}

impl ProfileState {
    fn new(random_seed: u64, append_only: bool, upsert_delete_if_exists: bool) -> Self {
        let rng = StdRng::seed_from_u64(random_seed);
        Self {
            rng,
            append_only,
            is_upsert_table: upsert_delete_if_exists,
            txn_state: TxnState::Empty,
            next_id: 0,
            committed_inserted_rows: VecDeque::new(),
            uncommitted_inserted_rows: VecDeque::new(),
            deleted_committed_row_ids: HashSet::new(),
            cur_lsn: 0,
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

    /// Clear all buffered rows for the current transaction.
    fn clear_cur_transaction_buffered_rows(&mut self) {
        self.uncommitted_inserted_rows.clear();
        self.deleted_committed_row_ids.clear();
    }

    /// Assert on preconditions to start a new transaction, whether it's streaming one or non-streaming one.
    fn assert_txn_begin_precondition(&self) {
        assert_eq!(self.txn_state, TxnState::Empty);
        assert!(self.uncommitted_inserted_rows.is_empty());
        assert!(self.deleted_committed_row_ids.is_empty());
    }

    fn begin_non_streaming_txn(&mut self) {
        self.assert_txn_begin_precondition();
        self.txn_state = TxnState::InNonStreaming;
    }

    fn commit_transaction(&mut self, lsn: u64) {
        // Set profile test states.
        assert_ne!(self.txn_state, TxnState::Empty);
        self.txn_state = TxnState::Empty;
        self.last_commit_lsn = Some(lsn);
        self.last_txn_is_committed = true;

        // Set table states.
        self.committed_inserted_rows
            .extend(self.uncommitted_inserted_rows.drain(..));
        self.committed_inserted_rows
            .retain(|(id, _)| !self.deleted_committed_row_ids.contains(id));

        self.clear_cur_transaction_buffered_rows();
    }

    fn get_next_row_to_append(&mut self) -> MoonlinkRow {
        let row = create_row(self.next_id, /*name=*/ "user", self.next_id % 5);
        self.uncommitted_inserted_rows
            .push_back((self.next_id, row.clone()));
        self.next_id += 1;
        row
    }

    fn can_append(&self) -> bool {
        if self.is_upsert_table {
            return false;
        }
        true
    }

    /// Return whether we could delete a row in the next event.
    ///
    /// The logic corresponds to [`get_random_row_to_delete`].
    fn can_delete(&self) -> bool {
        if self.append_only {
            return false;
        }

        let committed_inserted_rows = self.committed_inserted_rows.len();
        let deleted_committed_rows = self.deleted_committed_row_ids.len();

        // There're undeleted committed records, which are not deleted in the current transaction.
        if committed_inserted_rows > deleted_committed_rows {
            return true;
        }

        false
    }

    /// Get a random row to delete.
    fn get_random_row_to_delete(&mut self) -> MoonlinkRow {
        // Delete if exists is only supported for non-streaming transaction.
        if self.is_upsert_table && self.rng.random_range(0..100) < 50 {
            // Delete a none existing row.
            let row = create_row(self.next_id, /*name=*/ "user", self.next_id % 5);
            self.next_id += 1;
            return row;
        }

        let candidates: Vec<(i32, MoonlinkRow)> = self
            .committed_inserted_rows
            .iter()
            .filter(|(id, _)| !self.deleted_committed_row_ids.contains(id))
            .map(|(id, row)| (*id, row.clone()))
            .collect();
        assert!(!candidates.is_empty());

        // Randomly pick one row from the candidates.
        let random_idx = self.rng.random_range(0..candidates.len());
        let (id, row) = candidates[random_idx].clone();

        // Update deleted rows set.
        assert!(self.deleted_committed_row_ids.insert(id));

        row
    }

    fn generate_random_events(&mut self) -> ProfileEvent {
        let mut choices = vec![];

        if self.txn_state == TxnState::Empty {
            choices.push(EventKind::BeginNonStreamingTxn);
        } else {
            if self.can_append() {
                choices.push(EventKind::Append);
            }
            if self.can_delete() {
                choices.push(EventKind::Delete);
            }
            choices.push(EventKind::EndNoFlush);
        }
        assert!(!choices.is_empty());

        match *choices.choose(&mut self.rng).unwrap() {
            EventKind::BeginNonStreamingTxn => {
                self.begin_non_streaming_txn();
                let row = self.get_next_row_to_append();
                ProfileEvent::create_table_events(vec![TableEvent::Append {
                    row,
                    xact_id: None,
                    lsn: self.get_and_update_cur_lsn(),
                    is_recovery: false,
                }])
            }
            EventKind::Append => ProfileEvent::create_table_events(vec![TableEvent::Append {
                row: self.get_next_row_to_append(),
                xact_id: None,
                lsn: self.get_and_update_cur_lsn(),
                is_recovery: false,
            }]),
            EventKind::Delete => ProfileEvent::create_table_events(vec![TableEvent::Delete {
                row: self.get_random_row_to_delete(),
                xact_id: None,
                lsn: self.get_and_update_cur_lsn(),
                delete_if_exists: self.is_upsert_table,
                is_recovery: false,
            }]),
            EventKind::EndNoFlush => {
                let lsn = self.get_and_update_cur_lsn();
                self.commit_transaction(lsn);
                ProfileEvent::create_table_events(vec![TableEvent::Commit {
                    lsn,
                    xact_id: None,
                    is_recovery: false,
                }])
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum SpecialTableOption {
    /// No special table option.
    None,
    /// Upsert/ delete if exists.
    UpsertDeleteIfExists,
    /// Append only.
    AppendOnly,
    /// Disable iceberg snapshot.
    NoIcebergSnapshot,
}

#[derive(Clone, Debug)]
struct TestEnvConfig {
    /// Test name, used to generate profile files.
    test_name: String,
    /// Special table option.
    special_table_option: SpecialTableOption,
    /// Event count.
    event_count: usize,
    /// Filesystem storage config for persistence.
    storage_config: StorageConfig,
}

#[allow(dead_code)]
struct TestEnvironment {
    seed: u64,
    test_env_config: TestEnvConfig,
    cache_temp_dir: TempDir,
    table_temp_dir: TempDir,
    object_storage_cache: ObjectStorageCache,
    read_state_manager: ReadStateManager,
    table_event_manager: TableEventManager,
    table_handler: TableHandler,
    event_sender: mpsc::Sender<TableEvent>,
    wal_flush_lsn_rx: watch::Receiver<u64>,
    last_commit_lsn_tx: watch::Sender<u64>,
    replication_lsn_tx: watch::Sender<u64>,
    mooncake_table_metadata: Arc<MooncakeTableMetadata>,
    iceberg_table_config: IcebergTableConfig,
}

impl TestEnvironment {
    async fn new(config: TestEnvConfig) -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let table_temp_dir = tempdir().unwrap();
        let disk_slice_write_config = DiskSliceWriterConfig {
            parquet_file_size: DiskSliceWriterConfig::DEFAULT_DISK_SLICE_PARQUET_FILE_SIZE,
            chaos_config: None,
        };
        let identity = IdentityProp::Keys(vec![0]);
        let iceberg_persistence_config =
            create_iceberg_persistence_config(config.special_table_option.clone());
        let mooncake_table_metadata = create_test_table_metadata_for_profile(
            table_temp_dir.path().to_str().unwrap().to_string(),
            iceberg_persistence_config,
            disk_slice_write_config,
            identity.clone(),
        );

        // Local filesystem to store read-through cache.
        let cache_temp_dir = tempdir().unwrap();
        let object_storage_cache = ObjectStorageCache::new(ObjectStorageCacheConfig::new(
            /*max_bytes=*/ 1 << 30, // 1GiB
            cache_temp_dir.path().to_str().unwrap().to_string(),
            /*optimize_local_filesystem=*/ true,
        ));

        // Create mooncake table and table event notification receiver.
        let iceberg_table_config =
            get_iceberg_table_config_with_storage_config(config.storage_config.clone());
        let table = create_mooncake_table(
            mooncake_table_metadata.clone(),
            iceberg_table_config.clone(),
            Arc::new(object_storage_cache.clone()),
        )
        .await;
        let (replication_lsn_tx, replication_lsn_rx) = watch::channel(0u64);
        let (last_commit_lsn_tx, last_commit_lsn_rx) = watch::channel(0u64);
        let read_state_filepath_remap =
            std::sync::Arc::new(|local_filepath: String| local_filepath);
        let read_state_manager = ReadStateManager::new(
            &table,
            replication_lsn_rx.clone(),
            last_commit_lsn_rx,
            read_state_filepath_remap,
        );
        let (table_event_sync_sender, table_event_sync_receiver) = create_table_event_syncer();
        let table_handler = TableHandler::new(
            table,
            table_event_sync_sender,
            create_table_handler_timers(),
            replication_lsn_rx.clone(),
            /*handler_event_replay_tx=*/ None,
            /*table_event_replay_tx=*/ None,
        )
        .await;
        let wal_flush_lsn_rx = table_event_sync_receiver.wal_flush_lsn_rx.clone();
        let table_event_manager =
            TableEventManager::new(table_handler.get_event_sender(), table_event_sync_receiver);
        let event_sender = table_handler.get_event_sender();

        Self {
            seed,
            test_env_config: config,
            cache_temp_dir,
            table_temp_dir,
            object_storage_cache,
            table_event_manager,
            read_state_manager,
            table_handler,
            event_sender,
            wal_flush_lsn_rx,
            replication_lsn_tx,
            last_commit_lsn_tx,
            mooncake_table_metadata,
            iceberg_table_config,
        }
    }
}

fn create_test_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "0".to_string(),
        )])),
        Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "1".to_string(),
        )])),
        Field::new("age", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "2".to_string(),
        )])),
    ]))
}

fn get_iceberg_table_config_with_storage_config(
    storage_config: StorageConfig,
) -> IcebergTableConfig {
    let accessor_config = crate::AccessorConfig::new_with_storage_config(storage_config);
    IcebergTableConfig {
        namespace: vec![ICEBERG_TEST_NAMESPACE.to_string()],
        table_name: ICEBERG_TEST_TABLE.to_string(),
        data_accessor_config: accessor_config.clone(),
        metadata_accessor_config: crate::IcebergCatalogConfig::File { accessor_config },
    }
}

async fn create_mooncake_table(
    mooncake_table_metadata: Arc<MooncakeTableMetadata>,
    iceberg_table_config: IcebergTableConfig,
    object_storage_cache: Arc<dyn CacheTrait>,
) -> MooncakeTable {
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &mooncake_table_metadata.path);
    let wal_manager = WalManager::new(&wal_config);
    let table = MooncakeTable::new(
        create_test_arrow_schema().as_ref().clone(),
        ICEBERG_TEST_TABLE.to_string(),
        /*version=*/ TEST_TABLE_ID.0,
        mooncake_table_metadata.path.clone(),
        iceberg_table_config.clone(),
        mooncake_table_metadata.config.clone(),
        wal_manager,
        object_storage_cache,
        create_test_filesystem_accessor(&iceberg_table_config),
    )
    .await
    .unwrap();

    table
}

fn create_test_filesystem_accessor(
    iceberg_table_config: &IcebergTableConfig,
) -> Arc<dyn BaseFileSystemAccess> {
    Arc::new(FileSystemAccessor::new(
        iceberg_table_config.data_accessor_config.clone(),
    ))
}

fn create_iceberg_persistence_config(
    special_table_option: SpecialTableOption,
) -> IcebergPersistenceConfig {
    if special_table_option == SpecialTableOption::NoIcebergSnapshot {
        IcebergPersistenceConfig {
            new_data_file_count: usize::MAX,
            new_committed_deletion_log: usize::MAX,
            new_compacted_data_file_count: usize::MAX,
            old_compacted_data_file_count: usize::MAX,
            old_merged_file_indices_count: usize::MAX,
        }
    } else {
        IcebergPersistenceConfig::default()
    }
}

fn create_test_table_metadata_for_profile(
    local_table_directory: String,
    iceberg_persistence_config: IcebergPersistenceConfig,
    disk_slice_write_config: DiskSliceWriterConfig,
    identity: IdentityProp,
) -> Arc<MooncakeTableMetadata> {
    let mut config = crate::MooncakeTableConfig::new(local_table_directory.clone());
    config.batch_size = 4096;
    config.mem_slice_size = config.batch_size * 8;
    config.disk_slice_writer_config = disk_slice_write_config;
    config.persistence_config = iceberg_persistence_config;
    config.append_only = identity == IdentityProp::None;
    config.row_identity = identity;
    config.snapshot_deletion_record_count = 1000;
    config.data_compaction_config = DataCompactionConfig {
        min_data_file_to_compact: 16,
        max_data_file_to_compact: 32,
        data_file_final_size: 1 << 29, // 512MiB
        data_file_deletion_percentage: 50,
    };
    config.file_index_config = FileIndexMergeConfig {
        min_file_indices_to_merge: 16,
        max_file_indices_to_merge: 32,
        index_block_final_size: 1 << 29, // 512MiB
    };
    Arc::new(MooncakeTableMetadata {
        mooncake_table_id: ICEBERG_TEST_TABLE.to_string(),
        table_id: 0,
        schema: create_test_arrow_schema(),
        config,
        path: std::path::PathBuf::from(local_table_directory),
    })
}

async fn profile_test_impl(env: TestEnvironment) {
    let test_env_config = env.test_env_config.clone();
    let event_sender = env.event_sender.clone();
    let mut table_event_manager = env.table_event_manager;
    let last_commit_lsn_tx = env.last_commit_lsn_tx;
    let replication_lsn_tx = env.replication_lsn_tx.clone();

    let mut state = ProfileState::new(
        env.seed,
        test_env_config.special_table_option == SpecialTableOption::AppendOnly,
        test_env_config.special_table_option == SpecialTableOption::UpsertDeleteIfExists,
    );
    let mut table_events = VecDeque::new();
    for cur_event_count in 0..test_env_config.event_count {
        let cur_events = state.generate_random_events();
        table_events.extend(cur_events.table_events);

        // Print out event generation progress periodically.
        if (cur_event_count + 1) % 500 == 0 {
            println!("{cur_event_count} events generated");
        }
    }
    println!("Random event generation over, start ingestion.");

    // Start collecting profile with pprof.
    let profile_target_file = format!(
        "/tmp/{}-{}.svg",
        env.test_env_config.test_name,
        uuid::Uuid::new_v4()
    );
    println!("Profile target file is {profile_target_file}");
    let guard = pprof::ProfilerGuard::new(PPROF_PROFILE_FREQ).unwrap();

    let task = tokio::spawn(async move {
        let mut ingested_event_count = 0;
        while let Some(cur_event) = table_events.pop_front() {
            // For commit events, need to set up corresponding replication and commit LSN.
            if let TableEvent::Commit { lsn, .. } = cur_event {
                replication_lsn_tx.send(lsn).unwrap();
                last_commit_lsn_tx.send(lsn).unwrap();
                event_sender.send(cur_event).await.unwrap();

                ingested_event_count += 1;
                if ingested_event_count > 500
                    && test_env_config.special_table_option != SpecialTableOption::NoIcebergSnapshot
                {
                    let rx = table_event_manager.initiate_snapshot(lsn).await;
                    TableEventManager::synchronize_force_snapshot_request(rx, lsn)
                        .await
                        .unwrap();
                    ingested_event_count = 0;
                }
            }
            // Handle non-commit table events.
            else {
                event_sender.send(cur_event).await.unwrap();
                ingested_event_count += 1;
            }
        }

        // TODO(hjiang): Temporarily hard code a sleep time to trigger background tasks.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // If anything bad happens in the eventloop, drop table would fail.
        table_event_manager.drop_table().await.unwrap();
    });

    // Await the task directly and handle its result.
    let task_result = task.await;

    // Print out events in order if profile test fails.
    if let Err(e) = task_result {
        // Propagate the panic to fail the test.
        if let Ok(panic) = e.try_into_panic() {
            std::panic::resume_unwind(panic);
        }
    }

    if let Ok(report) = guard.report().build() {
        let file = std::fs::File::create(profile_target_file).unwrap();
        report.flamegraph(file).unwrap();
    }
}

/// Profile test with data compaction enabled by default.
#[named]
pub async fn test_normal_profile_on_local_fs() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!().to_string(),
        special_table_option: SpecialTableOption::None,
        event_count: 50000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    profile_test_impl(env).await;
}

/// Profile test for append-only table.
#[named]
pub async fn test_append_only_table_profile_on_local_fs() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!().to_string(),
        special_table_option: SpecialTableOption::AppendOnly,
        event_count: 50000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    profile_test_impl(env).await;
}

/// Profile test for no iceberg persistence situation, which is meant to check ingestion and mooncake snapshot only profiling.
/// Also useful to tune iceberg persistence configs.
#[named]
pub async fn test_no_iceberg_persistence_on_local_fs() {
    let iceberg_temp_dir = tempdir().unwrap();
    let root_directory = iceberg_temp_dir.path().to_str().unwrap().to_string();
    let test_env_config = TestEnvConfig {
        test_name: function_name!().to_string(),
        special_table_option: SpecialTableOption::NoIcebergSnapshot,
        event_count: 100000,
        storage_config: StorageConfig::FileSystem {
            root_directory,
            atomic_write_dir: None,
        },
    };
    let env = TestEnvironment::new(test_env_config).await;
    profile_test_impl(env).await;
}
