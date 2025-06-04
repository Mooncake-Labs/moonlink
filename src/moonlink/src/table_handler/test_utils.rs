use crate::row::{IdentityProp, MoonlinkRow, RowValue};
use crate::storage::mooncake_table::{
    DiskFileDeletionVector, TableMetadata as MooncakeTableMetadata,
};
use crate::storage::IcebergTableConfig;
use crate::storage::{load_blob_from_puffin_file, DeletionVector};
use crate::storage::{verify_files_and_deletions, MooncakeTable};
use crate::table_handler::{IcebergEventSyncSender, TableEvent, TableHandler}; // Ensure this path is correct
use crate::union_read::{decode_read_state_for_testing, ReadStateManager};
use crate::Result;
use crate::{
    IcebergEventSyncReceiver, IcebergTableEventManager, IcebergTableManager,
    TableConfig as MooncakeTableConfig,
};

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::RecordBatch;
use iceberg::io::FileIOBuilder;
use iceberg::io::FileRead;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use tokio::sync::{mpsc, watch};

/// Creates a default schema for testing.
pub fn default_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "1".to_string(),
        )])),
        Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "2".to_string(),
        )])),
        Field::new("age", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "3".to_string(),
        )])),
    ])
}

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
    IcebergTableConfig {
        warehouse_uri,
        namespace: vec!["default".to_string()],
        table_name,
    }
}

/// Holds the common environment components for table handler tests.
pub struct TestEnvironment {
    pub handler: TableHandler,
    event_sender: mpsc::Sender<TableEvent>,
    read_state_manager: Arc<ReadStateManager>,
    replication_tx: watch::Sender<u64>,
    last_commit_tx: watch::Sender<u64>,
    pub(crate) iceberg_table_event_manager: IcebergTableEventManager,
    pub(crate) temp_dir: TempDir,
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
        let read_state_manager = Arc::new(ReadStateManager::new(
            &mooncake_table,
            replication_rx,
            last_commit_rx,
        ));

        let (iceberg_drop_table_completion_tx, iceberg_drop_table_completion_rx) = mpsc::channel(1);
        let iceberg_event_sync_sender = IcebergEventSyncSender {
            iceberg_drop_table_completion_tx,
        };
        let iceberg_event_sync_receiver = IcebergEventSyncReceiver {
            iceberg_drop_table_completion_rx,
        };
        let handler = TableHandler::new(mooncake_table, iceberg_event_sync_sender);
        let iceberg_table_event_manager =
            IcebergTableEventManager::new(handler.get_event_sender(), iceberg_event_sync_receiver);
        let event_sender = handler.get_event_sender();

        Self {
            handler,
            event_sender,
            read_state_manager,
            replication_tx,
            last_commit_tx,
            iceberg_table_event_manager,
            temp_dir,
        }
    }

    /// Creates a new test environment with default settings.
    pub async fn new(temp_dir: TempDir, mooncake_table_config: MooncakeTableConfig) -> Self {
        let path = temp_dir.path().to_path_buf();
        let table_name = "table_name";
        let iceberg_table_config =
            get_iceberg_manager_config(table_name.to_string(), path.to_str().unwrap().to_string());
        let mooncake_table = MooncakeTable::new(
            default_schema(),
            table_name.to_string(),
            1,
            path,
            IdentityProp::Keys(vec![0]),
            iceberg_table_config,
            mooncake_table_config,
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
            id: 0,
            schema: Arc::new(default_schema()),
            config: mooncake_table_config.clone(),
            path: self.temp_dir.path().to_path_buf(),
            identity: IdentityProp::Keys(vec![0]),
        });
        let iceberg_table_config = get_iceberg_manager_config(
            table_name.to_string(),
            self.temp_dir.path().to_str().unwrap().to_string(),
        );
        IcebergTableManager::new(mooncake_table_metadata, iceberg_table_config).unwrap()
    }

    async fn send_event(&self, event: TableEvent) {
        self.event_sender
            .send(event)
            .await
            .expect("Failed to send event");
    }

    // --- Util functions for iceberg drop table ---

    /// Request to drop iceberg table and block wait its completion.
    pub async fn drop_iceberg_table(&mut self) -> Result<()> {
        self.iceberg_table_event_manager.drop_table().await
    }

    // --- Operation Helpers ---

    pub async fn append_row(&self, id: i32, name: &str, age: i32, xact_id: Option<u32>) {
        let row = create_row(id, name, age);
        self.send_event(TableEvent::Append { row, xact_id }).await;
    }

    pub async fn delete_row(&self, id: i32, name: &str, age: i32, lsn: u64, xact_id: Option<u32>) {
        let row = create_row(id, name, age);
        self.send_event(TableEvent::Delete { row, lsn, xact_id })
            .await;
    }

    pub async fn commit(&self, lsn: u64) {
        self.send_event(TableEvent::Commit { lsn, xact_id: None })
            .await;
    }

    pub async fn stream_commit(&self, lsn: u64, xact_id: u32) {
        self.send_event(TableEvent::Commit {
            lsn,
            xact_id: Some(xact_id),
        })
        .await;
    }

    pub async fn stream_abort(&self, xact_id: u32) {
        self.send_event(TableEvent::StreamAbort { xact_id }).await;
    }

    pub async fn flush_table(&self, lsn: u64) {
        self.send_event(TableEvent::Flush { lsn }).await;
    }

    pub async fn stream_flush(&self, xact_id: u32) {
        self.send_event(TableEvent::StreamFlush { xact_id }).await;
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

    pub async fn verify_snapshot(&self, target_lsn: u64, expected_ids: &[i32]) {
        check_read_snapshot(&self.read_state_manager, target_lsn, expected_ids).await;
    }

    // --- Lifecycle Helper ---
    pub async fn shutdown(&mut self) {
        self.send_event(TableEvent::_Shutdown).await;
        if let Some(handle) = self.handler._event_handle.take() {
            handle.await.expect("TableHandler task panicked");
        }
    }
}

/// Verifies the state of a read snapshot against expected row IDs.
pub async fn check_read_snapshot(
    read_manager: &ReadStateManager,
    target_lsn: u64,
    expected_ids: &[i32],
) {
    let read_state = read_manager.try_read(Some(target_lsn)).await.unwrap();
    let (data_files, puffin_files, deletion_vectors, position_deletes) =
        decode_read_state_for_testing(&read_state);

    if data_files.is_empty() && !expected_ids.is_empty() {
        unreachable!(
            "No snapshot files returned for LSN {} when rows (IDs: {:?}) were expected. Expected files because expected_ids is not empty.",
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

/// Test util function to load all arrow batch from the given local parquet file.
pub(crate) async fn load_arrow_batch(filepath: &str) -> RecordBatch {
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

/// Test util function to check consistency for snapshot batch deletion vector and deletion puffin blob.
pub(crate) async fn check_deletion_vector_consistency(disk_dv_entry: &DiskFileDeletionVector) {
    if disk_dv_entry.puffin_deletion_blob.is_none() {
        assert!(disk_dv_entry
            .batch_deletion_vector
            .collect_deleted_rows()
            .is_empty());
        return;
    }

    let local_fileio = FileIOBuilder::new_fs_io().build().unwrap();
    let blob = load_blob_from_puffin_file(
        local_fileio,
        &disk_dv_entry
            .puffin_deletion_blob
            .as_ref()
            .unwrap()
            .puffin_filepath,
    )
    .await
    .unwrap();
    let iceberg_deletion_vector = DeletionVector::deserialize(blob).unwrap();
    let batch_deletion_vector = iceberg_deletion_vector
        .take_as_batch_delete_vector(MooncakeTableConfig::default().batch_size());
    assert_eq!(batch_deletion_vector, disk_dv_entry.batch_deletion_vector);
}
