/// This module provides a few test util functions.
use crate::row::IdentityProp as RowIdentity;
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableConfig;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::storage::iceberg::puffin_utils;
use crate::storage::mooncake_table::DataCompactionPayload;
use crate::storage::mooncake_table::FileIndiceMergePayload;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::IcebergSnapshotResult;
use crate::storage::mooncake_table::Snapshot;
use crate::storage::mooncake_table::SnapshotOption;
use crate::storage::mooncake_table::{
    DiskFileEntry, TableConfig as MooncakeTableConfig, TableMetadata as MooncakeTableMetadata,
};
use crate::storage::MooncakeTable;
use crate::table_notify::TableNotify;
use crate::ObjectStorageCache;
use crate::Result;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use arrow_array::RecordBatch;
use iceberg::io::FileIO;
use iceberg::io::FileIOBuilder;
use iceberg::io::FileRead;
use iceberg::Result as IcebergResult;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

/// Test util function to check consistency for snapshot batch deletion vector and deletion puffin blob.
async fn check_deletion_vector_consistency(disk_file_entry: &DiskFileEntry) {
    if disk_file_entry.puffin_deletion_blob.is_none() {
        assert!(disk_file_entry
            .batch_deletion_vector
            .collect_deleted_rows()
            .is_empty());
        return;
    }

    let local_fileio = FileIOBuilder::new_fs_io().build().unwrap();
    let blob = puffin_utils::load_blob_from_puffin_file(
        local_fileio,
        disk_file_entry
            .puffin_deletion_blob
            .as_ref()
            .unwrap()
            .puffin_file_cache_handle
            .get_cache_filepath(),
    )
    .await
    .unwrap();
    let iceberg_deletion_vector = DeletionVector::deserialize(blob).unwrap();
    let batch_deletion_vector = iceberg_deletion_vector.take_as_batch_delete_vector();
    assert_eq!(batch_deletion_vector, disk_file_entry.batch_deletion_vector);
}

/// Test util function to check deletion vector consistency for the given snapshot.
pub(crate) async fn check_deletion_vector_consistency_for_snapshot(snapshot: &Snapshot) {
    for disk_deletion_vector in snapshot.disk_files.values() {
        check_deletion_vector_consistency(disk_deletion_vector).await;
    }
}

/// Test util functions to check recovered snapshot only contains remote filepaths and they do exist.
pub(crate) async fn validate_recovered_snapshot(snapshot: &Snapshot, warehouse_uri: &str) {
    let warehouse_directory = std::path::PathBuf::from(warehouse_uri);
    let mut data_filepaths: HashSet<String> = HashSet::new();

    // Check data files and their puffin blobs.
    for (cur_disk_file, cur_deletion_vector) in snapshot.disk_files.iter() {
        let cur_disk_pathbuf = std::path::PathBuf::from(cur_disk_file.file_path());
        assert!(cur_disk_pathbuf.starts_with(&warehouse_directory));
        assert!(tokio::fs::try_exists(cur_disk_pathbuf).await.unwrap());
        assert!(data_filepaths.insert(cur_disk_file.file_path().clone()));

        if cur_deletion_vector.puffin_deletion_blob.is_none() {
            continue;
        }
        let puffin_filepath = cur_deletion_vector
            .puffin_deletion_blob
            .as_ref()
            .unwrap()
            .puffin_file_cache_handle
            .get_cache_filepath();
        assert!(tokio::fs::try_exists(puffin_filepath).await.unwrap());
    }

    // Check file indices.
    let mut index_referenced_data_filepaths: HashSet<String> = HashSet::new();
    for cur_file_index in snapshot.indices.file_indices.iter() {
        // Check index blocks are imported into the iceberg table.
        // But index blocks are always cached on-disk, so not under warehouse uri.
        for cur_index_block in cur_file_index.index_blocks.iter() {
            let index_pathbuf = std::path::PathBuf::from(&cur_index_block.index_file.file_path());
            assert!(tokio::fs::try_exists(&index_pathbuf).await.unwrap());
        }

        // Check data files referenced by index blocks are imported into iceberg table.
        for cur_data_filepath in cur_file_index.files.iter() {
            let data_file_pathbuf = std::path::PathBuf::from(cur_data_filepath.file_path());
            assert!(data_file_pathbuf.starts_with(&warehouse_directory));
            assert!(tokio::fs::try_exists(&data_file_pathbuf).await.unwrap());
            index_referenced_data_filepaths.insert(cur_data_filepath.file_path().clone());
        }
    }

    assert_eq!(index_referenced_data_filepaths, data_filepaths);
}

/// Test util function to create arrow schema.
pub(crate) fn create_test_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
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
    ]))
}

/// Test util function to create mooncake table metadata.
pub(crate) fn create_test_table_metadata(
    local_table_directory: String,
) -> Arc<MooncakeTableMetadata> {
    Arc::new(MooncakeTableMetadata {
        name: "test_table".to_string(),
        id: 0,
        schema: create_test_arrow_schema(),
        config: MooncakeTableConfig::new(local_table_directory.clone()),
        path: std::path::PathBuf::from(local_table_directory),
        identity: RowIdentity::FullRow,
    })
}

/// Test util function to load the first arrow batch from the given parquet file.
/// Precondition: caller unit tests persist rows in one arrow record batch and one parquet file.
pub(crate) async fn load_arrow_batch(
    file_io: &FileIO,
    filepath: &str,
) -> IcebergResult<RecordBatch> {
    let input_file = file_io.new_input(filepath)?;
    let input_file_metadata = input_file.metadata().await?;
    let reader = input_file.reader().await?;
    let bytes = reader.read(0..input_file_metadata.size).await?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let mut reader = builder.build()?;
    let batch = reader.next().transpose()?.expect("Should have one batch");
    Ok(batch)
}

/// Util function to create mooncake table and iceberg table manager; object storage cache will be created internally.
///
/// Iceberg snapshot will be created whenever `create_snapshot` is called.
pub(crate) async fn create_table_and_iceberg_manager(
    temp_dir: &TempDir,
) -> (MooncakeTable, IcebergTableManager, Receiver<TableNotify>) {
    let default_data_compaction_config = DataCompactionConfig::default();
    create_table_and_iceberg_manager_with_data_compaction_config(
        temp_dir,
        default_data_compaction_config,
    )
    .await
}

/// Similar to [`create_table_and_iceberg_manager`], but it takes data compaction config.
pub(crate) async fn create_table_and_iceberg_manager_with_data_compaction_config(
    temp_dir: &TempDir,
    data_compaction_config: DataCompactionConfig,
) -> (MooncakeTable, IcebergTableManager, Receiver<TableNotify>) {
    let path = temp_dir.path().to_path_buf();
    let object_storage_cache = ObjectStorageCache::default_for_test(temp_dir);
    let warehouse_uri = path.clone().to_str().unwrap().to_string();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = IcebergTableConfig {
        warehouse_uri,
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
    };
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mooncake_table_config = MooncakeTableConfig {
        iceberg_snapshot_new_data_file_count: 0,
        data_compaction_config,
        ..Default::default()
    };

    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*version=*/ 1,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
        object_storage_cache.clone(),
    )
    .await
    .unwrap();

    let iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        object_storage_cache.clone(),
        iceberg_table_config.clone(),
    )
    .unwrap();

    let (notify_tx, notify_rx) = mpsc::channel(100);
    table.register_table_notify(notify_tx).await;

    (table, iceberg_table_manager, notify_rx)
}

/// Test util function to block wait a mooncake snapshot and get its result.
pub(crate) async fn get_mooncake_snapshot_result(
    notify_rx: &mut Receiver<TableNotify>,
) -> (
    u64,
    Option<IcebergSnapshotPayload>,
    Option<FileIndiceMergePayload>,
    Option<DataCompactionPayload>,
    Vec<String>,
) {
    let notification = notify_rx.recv().await.unwrap();
    match notification {
        TableNotify::MooncakeTableSnapshot {
            lsn,
            iceberg_snapshot_payload,
            file_indice_merge_payload,
            data_compaction_payload,
            evicted_data_files_to_delete,
        } => (
            lsn,
            iceberg_snapshot_payload,
            file_indice_merge_payload,
            data_compaction_payload,
            evicted_data_files_to_delete,
        ),
        _ => {
            panic!("Expects to receive mooncake snapshot completion notification, but receives others.");
        }
    }
}

/// Test util function to perform a mooncake snapshot, block wait its completion and get its result.
pub(crate) async fn create_mooncake_snapshot(
    table: &mut MooncakeTable,
    notify_rx: &mut Receiver<TableNotify>,
) -> (
    u64,
    Option<IcebergSnapshotPayload>,
    Option<FileIndiceMergePayload>,
    Option<DataCompactionPayload>,
    Vec<String>,
) {
    assert!(table.create_snapshot(SnapshotOption::default()));
    get_mooncake_snapshot_result(notify_rx).await
}

/// Test util function to perform an iceberg snapshot, block wait its completion and gets its result.
pub(crate) async fn create_iceberg_snapshot(
    table: &mut MooncakeTable,
    iceberg_snapshot_payload: Option<IcebergSnapshotPayload>,
    notify_rx: &mut Receiver<TableNotify>,
) -> Result<IcebergSnapshotResult> {
    table.persist_iceberg_snapshot(iceberg_snapshot_payload.unwrap());
    let notification = notify_rx.recv().await.unwrap();
    match notification {
        TableNotify::IcebergSnapshot {
            iceberg_snapshot_result,
        } => iceberg_snapshot_result,
        _ => {
            panic!(
                "Expects to receive iceberg snapshot completion notification, but receives others."
            )
        }
    }
}
