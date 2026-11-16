use crate::event_sync::create_table_event_syncer;
use crate::row::IdentityProp;
use crate::row::{MoonlinkRow, RowValue};
use crate::storage::filesystem::accessor_config::AccessorConfig;
use crate::storage::mooncake_table::table_creation_test_utils::create_test_arrow_schema;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::get_read_state_filepath_remap;
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
use crate::storage::IcebergTableConfig;
use crate::storage::{verify_files_and_deletions, MooncakeTable};
use crate::table_handler::{TableEvent, TableHandler};
use crate::table_handler_timer::create_table_handler_timers;
use crate::union_read::{decode_read_state_for_testing, ReadStateManager};
use crate::IcebergCatalogConfig;
use crate::Result;
use crate::{IcebergTableManager, MooncakeTableConfig, StorageConfig, TableEventManager};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use iceberg::io::FileIOBuilder;
use iceberg::io::FileRead;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::AsyncArrowWriter;
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
        data_accessor_config: AccessorConfig::new_with_storage_config(storage_config.clone()),
        metadata_accessor_config: IcebergCatalogConfig::File {
            accessor_config: AccessorConfig::new_with_storage_config(storage_config),
        },
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
    pub(crate) force_snapshot_completion_rx: watch::Receiver<Option<Result<u64>>>,
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
        let mut mooncake_table_config =
            MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
        mooncake_table_config.row_identity = IdentityProp::Keys(vec![0]);
        Self::new(temp_dir, mooncake_table_config).await
    }

    /// Create a new test environment with the given mooncake table.
    pub(crate) async fn new_with_mooncake_table(temp_dir: TempDir, table: MooncakeTable) -> Self {
        let (replication_tx, replication_rx) = watch::channel(0u64);
        let (last_commit_tx, last_commit_rx) = watch::channel(0u64);
        let snapshot_lsn_tx = table.get_snapshot_watch_sender().clone();
        let (table_event_sync_sender, table_event_sync_receiver) = create_table_event_syncer();
        let read_state_manager = Some(Arc::new(ReadStateManager::new(
            &table,
            replication_rx.clone(),
            last_commit_rx,
            table_event_sync_receiver.table_status_rx.clone(),
            get_read_state_filepath_remap(),
        )));
        let force_snapshot_completion_rx = table_event_sync_receiver
            .force_snapshot_completion_rx
            .clone();
        let table_handler_timer = create_table_handler_timers();

        let handler = TableHandler::new(
            table,
            table_event_sync_sender,
            table_handler_timer,
            replication_rx.clone(),
            /*event_replay_tx=*/ None,
            /*table_event_replay_tx=*/ None,
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
            force_snapshot_completion_rx,
            table_event_manager,
            temp_dir,
        }
    }

    /// Creates a new test environment with default settings.
    pub async fn new(temp_dir: TempDir, mooncake_table_config: MooncakeTableConfig) -> Self {
        Self::new_with_table_setup(temp_dir, mooncake_table_config, |_| {}).await
    }

    /// Like [`Self::new`], with `setup` applied to the table before the handler takes it.
    pub(crate) async fn new_with_table_setup(
        temp_dir: TempDir,
        mooncake_table_config: MooncakeTableConfig,
        setup: impl FnOnce(&mut MooncakeTable),
    ) -> Self {
        let path = temp_dir.path().to_path_buf();
        let table_name = "table_name";
        let iceberg_table_config =
            get_iceberg_manager_config(table_name.to_string(), path.to_str().unwrap().to_string());

        let mut mooncake_table = MooncakeTable::new(
            (*create_test_arrow_schema()).clone(),
            table_name.to_string(),
            1,
            path,
            iceberg_table_config.clone(),
            mooncake_table_config,
            create_test_object_storage_cache(&temp_dir),
            create_test_filesystem_accessor(&iceberg_table_config),
        )
        .await
        .unwrap();
        setup(&mut mooncake_table);

        Self::new_with_mooncake_table(temp_dir, mooncake_table).await
    }

    /// Create iceberg table manager.
    pub async fn create_iceberg_table_manager(
        &self,
        mooncake_table_config: MooncakeTableConfig,
    ) -> IcebergTableManager {
        let table_name = "table_name";

        let mooncake_table_metadata = Arc::new(MooncakeTableMetadata {
            mooncake_table_id: table_name.to_string(),
            table_id: 0,
            schema: create_test_arrow_schema(),
            config: mooncake_table_config.clone(),
            path: self.temp_dir.path().to_path_buf(),
        });
        let iceberg_table_config = get_iceberg_manager_config(
            table_name.to_string(),
            self.temp_dir.path().to_str().unwrap().to_string(),
        );
        IcebergTableManager::new(
            mooncake_table_metadata,
            // Create new and separate object storage cache for new iceberg table manager.
            create_test_object_storage_cache(&self.temp_dir),
            create_test_filesystem_accessor(&iceberg_table_config),
            iceberg_table_config.clone(),
        )
        .await
        .unwrap()
    }

    pub async fn send_event(&self, event: TableEvent) {
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
        let event = TableEvent::Append { row, lsn, xact_id };
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
            delete_if_exists: false,
        };
        self.send_event(event.clone()).await;
        event
    }

    pub async fn commit(&self, lsn: u64) -> TableEvent {
        let event = TableEvent::Commit { lsn, xact_id: None };
        self.send_event(event.clone()).await;
        event
    }

    pub async fn stream_commit(&self, lsn: u64, xact_id: u32) -> TableEvent {
        let event = TableEvent::Commit {
            lsn,
            xact_id: Some(xact_id),
        };
        self.send_event(event.clone()).await;
        event
    }

    pub async fn stream_abort(&self, xact_id: u32) -> TableEvent {
        let event = TableEvent::StreamAbort { xact_id };
        self.send_event(event.clone()).await;
        event
    }

    /// Force an index merge operation, and block wait its completion.
    pub async fn force_index_merge_and_sync(&mut self) -> Result<()> {
        let rx = self.table_event_manager.initiate_index_merge().await?;
        TableEventManager::synchronize_maintenance_request(rx).await
    }

    /// Force a data compaction operation, and block wait its completion.
    pub async fn force_data_compaction_and_sync(&mut self) -> Result<()> {
        let rx = self.table_event_manager.initiate_data_compaction().await?;
        TableEventManager::synchronize_maintenance_request(rx).await
    }

    /// Force a full table maintenance task operation, and block wait its completion.
    pub async fn force_full_maintenance_and_sync(&mut self) -> Result<()> {
        let rx = self.table_event_manager.initiate_full_compaction().await?;
        TableEventManager::synchronize_maintenance_request(rx).await
    }

    /// Commit the transaction, flush and create mooncake/iceberg snapshot.
    pub async fn flush_table_and_sync(&mut self, lsn: u64, xact_id: Option<u32>) {
        self.send_event(TableEvent::CommitFlush { lsn, xact_id })
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
        self.send_event(TableEvent::CommitFlush { lsn, xact_id: None })
            .await;
    }

    pub async fn stream_flush(&self, xact_id: u32) {
        self.send_event(TableEvent::StreamFlush { xact_id }).await;
    }

    pub async fn bulk_upload_files(
        &self,
        files: Vec<String>,
        storage_config: StorageConfig,
        lsn: u64,
    ) {
        self.send_event(TableEvent::LoadFiles {
            files,
            storage_config,
            lsn,
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

/// Test util function to generate a parquet under the given [`tempdir`].
pub(crate) async fn generate_parquet_file(tempdir: &TempDir, filename: &str) -> String {
    let schema = create_test_arrow_schema();
    let ids = Int32Array::from(vec![1, 2, 3]);
    let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    let ages = Int32Array::from(vec![10, 20, 30]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(names), Arc::new(ages)],
    )
    .unwrap();
    let file_path = tempdir.path().join(filename);
    let file_path_str = file_path.to_str().unwrap().to_string();
    let file = tokio::fs::File::create(file_path).await.unwrap();
    let mut writer: AsyncArrowWriter<tokio::fs::File> =
        AsyncArrowWriter::try_new(file, schema, /*props=*/ None).unwrap();
    writer.write(&batch).await.unwrap();
    writer.close().await.unwrap();
    file_path_str
}

/// Test util function to generate a parquet file to S3.
#[cfg(feature = "storage-s3")]
pub(crate) async fn generate_parquet_file_in_s3(storage_config: StorageConfig, s3_filepath: &str) {
    let filename = std::path::Path::new(&s3_filepath)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let temp_dir = tempdir().unwrap();
    let local_parquet_file = generate_parquet_file(&temp_dir, &filename).await;
    let accessor_config = AccessorConfig::new_with_storage_config(storage_config);
    use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
    let filesystem_accessor = crate::FileSystemAccessor::new(accessor_config);
    filesystem_accessor
        .copy_from_local_to_remote(&local_parquet_file, s3_filepath)
        .await
        .unwrap();
}
