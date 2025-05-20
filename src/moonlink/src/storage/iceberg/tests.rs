use crate::row::IdentityProp as RowIdentity;
use crate::row::MoonlinkRow;
use crate::row::RowValue;
use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
use crate::storage::iceberg::iceberg_table_manager::*;
use crate::storage::iceberg::puffin_utils;
#[cfg(feature = "storage-s3")]
use crate::storage::iceberg::s3_test_utils;
use crate::storage::index::Index;
use crate::storage::index::MooncakeIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::Snapshot;
use crate::storage::mooncake_table::{
    DiskFileDeletionVector, TableConfig as MooncakeTableConfig,
    TableMetadata as MooncakeTableMetadata,
};
use crate::storage::storage_utils::create_data_file;
use crate::storage::storage_utils::DataFileRef;
use crate::storage::storage_utils::RawDeletionRecord;
use crate::storage::storage_utils::RecordLocation;
use crate::storage::MooncakeTable;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use arrow_array::{Int32Array, RecordBatch, StringArray};
use iceberg::io::FileIO;
use iceberg::io::FileIOBuilder;
use iceberg::io::FileRead;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::AsyncArrowWriter;
use tempfile::tempdir;
use tempfile::TempDir;

/// Create test batch deletion vector.
fn test_committed_deletion_log_1(
    data_filepath: DataFileRef,
) -> Vec<(DataFileRef, BatchDeletionVector)> {
    let mut deletion_vector = BatchDeletionVector::new(MooncakeTableConfig::DEFAULT_BATCH_SIZE);
    deletion_vector.delete_row(0);

    vec![(data_filepath.clone(), deletion_vector)]
}
/// Test deletion vector 2 includes deletion vector 1, used to mimic new data file rows deletion situation.
fn test_committed_deletion_log_2(
    data_filepath: DataFileRef,
) -> Vec<(DataFileRef, BatchDeletionVector)> {
    let mut deletion_vector = BatchDeletionVector::new(MooncakeTableConfig::DEFAULT_BATCH_SIZE);
    deletion_vector.delete_row(1);
    deletion_vector.delete_row(2);

    vec![(data_filepath.clone(), deletion_vector)]
}

/// Test util function to create arrow schema.
fn create_test_arrow_schema() -> Arc<ArrowSchema> {
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
fn create_test_table_metadata(local_table_directory: String) -> Arc<MooncakeTableMetadata> {
    Arc::new(MooncakeTableMetadata {
        name: "test_table".to_string(),
        id: 0,
        schema: create_test_arrow_schema(),
        config: MooncakeTableConfig::new(),
        path: PathBuf::from(local_table_directory),
        identity: RowIdentity::FullRow,
    })
}

/// Test util function to create arrow record batch.
fn test_batch_1(arrow_schema: Arc<ArrowSchema>) -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])), // id column
            Arc::new(StringArray::from(vec!["a", "b", "c"])), // name column
            Arc::new(Int32Array::from(vec![10, 20, 30])), // age column
        ],
    )
    .unwrap()
}
fn test_batch_2(arrow_schema: Arc<ArrowSchema>) -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5, 6])), // id column
            Arc::new(StringArray::from(vec!["d", "e", "f"])), // name column
            Arc::new(Int32Array::from(vec![40, 50, 60])), // age column
        ],
    )
    .unwrap()
}

/// Test util function to write arrow record batch into local file.
async fn write_arrow_record_batch_to_local<P: AsRef<std::path::Path>>(
    path: P,
    schema: Arc<ArrowSchema>,
    batch: &RecordBatch,
) -> IcebergResult<()> {
    let file = tokio::fs::File::create(&path).await?;
    let mut writer = AsyncArrowWriter::try_new(file, schema, None)?;
    writer.write(batch).await?;
    writer.close().await?;
    Ok(())
}

/// Test util function to load all arrow batch from the given parquet file.
async fn load_arrow_batch(file_io: &FileIO, filepath: &str) -> IcebergResult<RecordBatch> {
    let input_file = file_io.new_input(filepath)?;
    let input_file_metadata = input_file.metadata().await?;
    let reader = input_file.reader().await?;
    let bytes = reader.read(0..input_file_metadata.size).await?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
    let mut reader = builder.build()?;
    let batch = reader.next().transpose()?.expect("Should have one batch");
    Ok(batch)
}

/// Test util to get file indices filepaths and their corresponding data filepaths.
fn get_file_indices_filepath_and_data_filepaths(
    mooncake_index: &MooncakeIndex,
) -> (
    Vec<String>, /*file indices filepath*/
    Vec<String>, /*data filepaths*/
) {
    let file_indices = &mooncake_index.file_indices;

    let mut data_files: Vec<String> = vec![];
    let mut index_files: Vec<String> = vec![];
    for cur_file_index in file_indices.iter() {
        data_files.extend(
            cur_file_index
                .files
                .iter()
                .map(|cur_file| cur_file.file_name().clone())
                .collect::<Vec<_>>(),
        );
        index_files.extend(
            cur_file_index
                .index_blocks
                .iter()
                .map(|cur_index_block| cur_index_block.file_path.clone())
                .collect::<Vec<_>>(),
        );
    }

    (data_files, index_files)
}

/// Test util function to check consistency for snapshot batch deletion vector and deletion puffin blob.
async fn check_deletion_vector_consistency(disk_dv_entry: &DiskFileDeletionVector) {
    if disk_dv_entry.puffin_deletion_blob.is_none() {
        assert!(disk_dv_entry
            .batch_deletion_vector
            .collect_deleted_rows()
            .is_empty());
        return;
    }

    let local_fileio = FileIOBuilder::new_fs_io().build().unwrap();
    let blob = puffin_utils::load_blob_from_puffin_file(
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

/// Test util function to check deletion vector consistency for the given snapshot.
async fn check_deletion_vector_consistency_for_snapshot(snapshot: &Snapshot) {
    for disk_deletion_vector in snapshot.disk_files.values() {
        check_deletion_vector_consistency(disk_deletion_vector).await;
    }
}

/// Test snapshot store and load for different types of catalogs based on the given warehouse.
async fn test_store_and_load_snapshot_impl(
    iceberg_table_manager: &mut IcebergTableManager,
) -> IcebergResult<()> {
    // At the beginning of the test, there's nothing in table.
    assert!(iceberg_table_manager.persisted_data_files.is_empty());

    // Create arrow schema and table.
    let arrow_schema = create_test_arrow_schema();
    let tmp_dir = tempdir()?;

    // Write first snapshot to iceberg table (with deletion vector).
    let data_filename_1 = "data-1.parquet";
    let batch = test_batch_1(arrow_schema.clone());
    let parquet_path = tmp_dir.path().join(data_filename_1);
    let data_file = create_data_file(0, parquet_path.to_str().unwrap().to_string());
    write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch).await?;

    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        flush_lsn: 0,
        data_files: vec![data_file.clone()],
        new_deletion_vector: test_committed_deletion_log_1(data_file.clone()),
        file_indices: vec![],
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload)
        .await?;

    // Write second snapshot to iceberg table, with updated deletion vector and new data file.
    let data_filename_2 = "data-2.parquet";
    let batch = test_batch_2(arrow_schema.clone());
    let parquet_path = tmp_dir.path().join(data_filename_2);
    let data_file = create_data_file(1, parquet_path.to_str().unwrap().to_string());
    write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch).await?;

    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        flush_lsn: 1,
        data_files: vec![data_file.clone()],
        new_deletion_vector: test_committed_deletion_log_2(data_file.clone()),
        file_indices: vec![],
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload)
        .await?;

    // Check persisted items in the iceberg table.
    assert_eq!(
        iceberg_table_manager.persisted_data_files.len(),
        2,
        "Persisted items for table manager is {:?}",
        iceberg_table_manager.persisted_data_files
    );

    // Check the loaded data file is of the expected format and content.
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    for (loaded_path, data_entry) in iceberg_table_manager.persisted_data_files.iter() {
        let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.as_str()).await?;
        let deleted_rows = data_entry.deletion_vector.collect_deleted_rows();
        assert_eq!(
            *loaded_arrow_batch.schema_ref(),
            arrow_schema,
            "Expect arrow schema {:?}, actual arrow schema {:?}",
            arrow_schema,
            loaded_arrow_batch.schema_ref()
        );

        // Check second data file and its deletion vector.
        if loaded_path.ends_with(data_filename_2) {
            assert_eq!(
                loaded_arrow_batch,
                test_batch_2(arrow_schema.clone()),
                "Expect arrow record batch {:?}, actual arrow record batch {:?}",
                loaded_arrow_batch,
                test_batch_2(arrow_schema.clone())
            );
            assert_eq!(
                deleted_rows,
                vec![1, 2],
                "Loaded deletion vector is not the same as the one gets stored."
            );
            continue;
        }

        // Check first data file and its deletion vector.
        assert!(loaded_path.ends_with(data_filename_1));
        assert_eq!(
            loaded_arrow_batch,
            test_batch_1(arrow_schema.clone()),
            "Expect arrow record batch {:?}, actual arrow record batch {:?}",
            loaded_arrow_batch,
            test_batch_1(arrow_schema.clone())
        );
        assert_eq!(
            deleted_rows,
            vec![0],
            "Loaded deletion vector is not the same as the one gets stored."
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_sync_snapshots() -> IcebergResult<()> {
    // Create arrow schema and table.
    let tmp_dir = tempdir()?;
    let mooncake_table_metadata =
        create_test_table_metadata(tmp_dir.path().to_str().unwrap().to_string());
    let config = IcebergTableConfig {
        warehouse_uri: tmp_dir.path().to_str().unwrap().to_string(),
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
        drop_table_if_exists: false,
    };
    let mut iceberg_table_manager = IcebergTableManager::new(mooncake_table_metadata, config);
    test_store_and_load_snapshot_impl(&mut iceberg_table_manager).await?;
    Ok(())
}

/// Testing scenario: attempt an iceberg snapshot when no data file, deletion vector or index files generated.
#[tokio::test]
async fn test_empty_content_snapshot_creation() -> IcebergResult<()> {
    let tmp_dir = tempdir()?;
    let mooncake_table_metadata =
        create_test_table_metadata(tmp_dir.path().to_str().unwrap().to_string());
    let config = IcebergTableConfig {
        warehouse_uri: tmp_dir.path().to_str().unwrap().to_string(),
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
        drop_table_if_exists: false,
    };
    let mut iceberg_table_manager =
        IcebergTableManager::new(mooncake_table_metadata.clone(), config.clone());
    let iceberg_snapshot_payload = IcebergSnapshotPayload {
        flush_lsn: 0,
        data_files: vec![],
        new_deletion_vector: vec![],
        file_indices: vec![],
    };
    iceberg_table_manager
        .sync_snapshot(iceberg_snapshot_payload)
        .await?;

    // Recover from iceberg snapshot, and check mooncake table snapshot version.
    let mut iceberg_table_manager =
        IcebergTableManager::new(mooncake_table_metadata.clone(), config.clone());
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.in_memory_index.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    Ok(())
}

/// Test util function to check the given row doesn't exist in the snapshot indices.
async fn check_row_index_nonexistent(snapshot: &Snapshot, row: &MoonlinkRow) {
    let key = snapshot.metadata.identity.get_lookup_key(row);
    let locs = snapshot
        .indices
        .find_record(&RawDeletionRecord {
            lookup_key: key,
            row_identity: snapshot.metadata.identity.extract_identity_for_key(row),
            pos: None,
            lsn: 0, // LSN has nothing to do with deletion record search
        })
        .await;
    assert!(
        locs.is_empty(),
        "Deletion record {:?} exists for row {:?}",
        locs,
        row
    );
}

/// Test util function to check the given row exists in snapshot, and it's on-disk.
async fn check_row_index_on_disk(snapshot: &Snapshot, row: &MoonlinkRow) {
    let key = snapshot.metadata.identity.get_lookup_key(row);
    let locs = snapshot
        .indices
        .find_record(&RawDeletionRecord {
            lookup_key: key,
            row_identity: snapshot.metadata.identity.extract_identity_for_key(row),
            pos: None,
            lsn: 0, // LSN has nothing to do with deletion record search
        })
        .await;
    assert_eq!(
        locs.len(),
        1,
        "Actual location for row {:?} is {:?}",
        row,
        locs
    );
    assert!(
        matches!(locs[0], RecordLocation::DiskFile(_, _)),
        "Actual location for row {:?} is {:?}",
        row,
        locs
    );
}

async fn mooncake_table_snapshot_persist_impl(warehouse_uri: String) -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = IcebergTableConfig {
        warehouse_uri,
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
        drop_table_if_exists: false,
    };
    let schema = create_test_arrow_schema();
    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mut mooncake_table_config = MooncakeTableConfig::new();
    mooncake_table_config.iceberg_snapshot_new_data_file_count = 0;
    let mut table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*version=*/ 1,
        path,
        identity_property.clone(),
        iceberg_table_config.clone(),
        mooncake_table_config,
    )
    .await;

    // Perform a few table write operations.
    //
    // Operation series 1: append three rows, delete one of them, flush, commit and create snapshot.
    // Expects to see one data file with no deletion vector, because mooncake table handle deletion inline before persistence, and all record batches are dumped into one single data file.
    // The three rows are deleted in three operations series respectively.
    let row1 = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row1.clone()).map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!(
                "Failed to append row1 {:?} to mooncake table because {:?}",
                row1, e
            ),
        )
    })?;
    let row2 = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Alice".as_bytes().to_vec()),
        RowValue::Int32(10),
    ]);
    table.append(row2.clone()).map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!(
                "Failed to append row2 {:?} to mooncake table because {:?}",
                row2, e
            ),
        )
    })?;
    let row3 = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row3.clone()).map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!(
                "Failed to append row3 {:?} to mooncake table because {:?}",
                row3, e
            ),
        )
    })?;
    // First deletion of row1, which happens in MemSlice.
    table.delete(row1.clone(), /*flush_lsn=*/ 100).await;
    table.flush(/*flush_lsn=*/ 200).await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Failed to flush records to mooncake table because {:?}", e),
        )
    })?;
    table.commit(/*flush_lsn=*/ 200);
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
    );
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert_eq!(
        snapshot.disk_files.len(),
        1,
        "Persisted items for table manager is {:?}",
        snapshot.disk_files
    );
    assert_eq!(
        snapshot.indices.file_indices.len(),
        1,
        "Snapshot data files and file indices are {:?}",
        get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
    );
    check_row_index_nonexistent(&snapshot, &row1).await;
    check_row_index_on_disk(&snapshot, &row2).await;
    check_row_index_on_disk(&snapshot, &row3).await;
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    // Check the loaded data file is of the expected format and content.
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.file_name().as_str()).await?;
    let expected_arrow_batch = RecordBatch::try_new(
        schema.clone(),
        // row2 and row3
        vec![
            Arc::new(Int32Array::from(vec![2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(Int32Array::from(vec![10, 50])),
        ],
    )
    .unwrap();
    assert_eq!(
        loaded_arrow_batch, expected_arrow_batch,
        "Expected arrow data is {:?}, actual data is {:?}",
        expected_arrow_batch, loaded_arrow_batch
    );

    let deleted_rows = deletion_vector.batch_deletion_vector.collect_deleted_rows();
    assert!(
        deleted_rows.is_empty(),
        "There should be no deletion vector in iceberg table."
    );

    // --------------------------------------
    // Operation series 2: no more additional rows appended, only to delete the first row in the table.
    // Expects to see a new deletion vector, because its corresponding data file has been persisted.
    table.delete(row2.clone(), /*flush_lsn=*/ 300).await;
    table.flush(/*flush_lsn=*/ 300).await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!(
                "Failed to flush records {:?} to mooncake table because {:?}",
                row2, e
            ),
        )
    })?;
    table.commit(/*flush_lsn=*/ 300);
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
    );
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert_eq!(
        snapshot.disk_files.len(),
        1,
        "Persisted items for table manager is {:?}",
        snapshot.disk_files
    );
    assert_eq!(
        snapshot.indices.file_indices.len(),
        1,
        "Snapshot data files and file indices are {:?}",
        get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
    );
    // row1 is deleted in-memory, so file index doesn't track it
    check_row_index_nonexistent(&snapshot, &row1).await;
    // row2 is deleted, but still exist in data file
    check_row_index_on_disk(&snapshot, &row2).await;
    check_row_index_on_disk(&snapshot, &row3).await;
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    // Check the loaded data file is of the expected format and content.
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.file_name().as_str()).await?;
    assert_eq!(
        loaded_arrow_batch, expected_arrow_batch,
        "Expected arrow data is {:?}, actual data is {:?}",
        expected_arrow_batch, loaded_arrow_batch
    );

    let deleted_rows = deletion_vector.batch_deletion_vector.collect_deleted_rows();
    let expected_deleted_rows = vec![0_u64];
    assert_eq!(
        deleted_rows, expected_deleted_rows,
        "Expected deletion vector {:?}, actual deletion vector {:?}",
        expected_deleted_rows, deleted_rows
    );

    // --------------------------------------
    // Operation series 3: no more additional rows appended, only to delete the last row in the table.
    // Expects to see the existing deletion vector updated, because its corresponding data file has been persisted.
    table.delete(row3.clone(), /*flush_lsn=*/ 400).await;
    table.flush(/*flush_lsn=*/ 400).await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Failed to flush records to mooncake table because {:?}", e),
        )
    })?;
    table.commit(/*flush_lsn=*/ 400);
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
    );
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert_eq!(
        snapshot.disk_files.len(),
        1,
        "Persisted items for table manager is {:?}",
        snapshot.disk_files
    );
    assert_eq!(
        snapshot.indices.file_indices.len(),
        1,
        "Snapshot data files and file indices are {:?}",
        get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
    );
    check_row_index_nonexistent(&snapshot, &row1).await;
    // row2 and row3 are deleted, but still exist in data file
    check_row_index_on_disk(&snapshot, &row2).await;
    check_row_index_on_disk(&snapshot, &row3).await;
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 400);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    // Check the loaded data file is of the expected format and content.
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.file_name().as_str()).await?;
    assert_eq!(
        loaded_arrow_batch, expected_arrow_batch,
        "Expected arrow data is {:?}, actual data is {:?}",
        expected_arrow_batch, loaded_arrow_batch
    );

    let deleted_rows = deletion_vector.batch_deletion_vector.collect_deleted_rows();
    let expected_deleted_rows = vec![0_u64, 1_u64];
    assert_eq!(
        deleted_rows, expected_deleted_rows,
        "Expected deletion vector {:?}, actual deletion vector {:?}",
        expected_deleted_rows, deleted_rows
    );
    let (_, data_entry) = iceberg_table_manager
        .persisted_data_files
        .iter()
        .next()
        .unwrap();

    // --------------------------------------
    // Operation series 4: append a new row, and don't delete any rows.
    // Expects to see the existing deletion vector unchanged and new data file created.
    let row4 = MoonlinkRow::new(vec![
        RowValue::Int32(4),
        RowValue::ByteArray("Tom".as_bytes().to_vec()),
        RowValue::Int32(40),
    ]);
    table.append(row4.clone()).map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!(
                "Failed to append row4 {:?} to mooncake table because {:?}",
                row4, e
            ),
        )
    })?;
    table.flush(/*flush_lsn=*/ 500).await.map_err(|e| {
        IcebergError::new(
            iceberg::ErrorKind::Unexpected,
            format!("Failed to flush records to mooncake table because {:?}", e),
        )
    })?;
    table.commit(/*flush_lsn=*/ 500);
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
    );
    let mut snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert_eq!(
        snapshot.disk_files.len(),
        2,
        "Persisted items for table manager is {:?}",
        snapshot.disk_files
    );
    assert_eq!(
        snapshot.indices.file_indices.len(),
        2,
        "Snapshot data files and file indices are {:?}",
        get_file_indices_filepath_and_data_filepaths(&snapshot.indices)
    );
    check_row_index_nonexistent(&snapshot, &row1).await;
    check_row_index_on_disk(&snapshot, &row2).await;
    check_row_index_on_disk(&snapshot, &row3).await;
    check_row_index_on_disk(&snapshot, &row4).await;
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);

    // The old data file and deletion vector is unchanged.
    let old_data_entry = iceberg_table_manager
        .persisted_data_files
        .remove(loaded_path.file_name());
    assert!(
        old_data_entry.is_some(),
        "Add new data file shouldn't change existing persisted items"
    );
    assert_eq!(
        &old_data_entry.unwrap(),
        data_entry,
        "Add new data file shouldn't change existing persisted items"
    );
    let (file_in_new_snapshot, _) = snapshot
        .disk_files
        .iter()
        .find(|(path, _)| path.file_name() == loaded_path.file_name())
        .unwrap();
    snapshot.disk_files.remove(&file_in_new_snapshot.file_id());

    // Check new data file is correctly managed by iceberg table with no deletion vector.
    let (loaded_path, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.file_name().as_str()).await?;

    let expected_arrow_batch = RecordBatch::try_new(
        schema.clone(),
        // row4
        vec![
            Arc::new(Int32Array::from(vec![4])),
            Arc::new(StringArray::from(vec!["Tom"])),
            Arc::new(Int32Array::from(vec![40])),
        ],
    )
    .unwrap();
    assert_eq!(
        loaded_arrow_batch, expected_arrow_batch,
        "Expected arrow data is {:?}, actual data is {:?}",
        expected_arrow_batch, loaded_arrow_batch
    );

    let deleted_rows = deletion_vector.batch_deletion_vector.collect_deleted_rows();
    assert!(
        deleted_rows.is_empty(),
        "The new appended data file should have no deletion vector aside, but actually it contains deletion vector {:?}",
        deleted_rows
    );

    Ok(())
}

#[tokio::test]
async fn test_filesystem_sync_snapshots() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap().to_string();
    mooncake_table_snapshot_persist_impl(path).await
}

#[tokio::test]
#[cfg(feature = "storage-s3")]
async fn test_object_storage_sync_snapshots() -> IcebergResult<()> {
    let (bucket_name, warehouse_uri) = s3_test_utils::get_test_minio_bucket_and_warehouse();
    s3_test_utils::object_store_test_utils::create_test_s3_bucket(bucket_name.clone()).await?;
    mooncake_table_snapshot_persist_impl(warehouse_uri).await
}

#[tokio::test]
async fn test_drop_table_at_creation() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap().to_string();

    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let iceberg_table_config = IcebergTableConfig {
        warehouse_uri: path.clone(),
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
        drop_table_if_exists: false,
    };

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mut mooncake_table_config = MooncakeTableConfig::new();
    mooncake_table_config.iceberg_snapshot_new_data_file_count = 0;
    let mut table = MooncakeTable::new(
        create_test_arrow_schema().as_ref().clone(),
        "test_table".to_string(),
        /*version=*/ 1,
        PathBuf::from(&path),
        mooncake_table_metadata.identity.clone(),
        iceberg_table_config.clone(),
        mooncake_table_config,
    )
    .await;
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();

    // Create a new iceberg table manager, recovery gets a fresh state.
    let mut iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
    );
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());

    Ok(())
}

// --------- state-based unit tests ---------
// Possible states for data records:
// (1) Uncommitted in-memory record batches
// (2) Committed in-memory record batches
// (3) Committed data files
// (4) Uncommitted and committed in-memory record batches
// (5) Committed in-memory record batches and committed data files
// (6) Uncommitted/committed record batches, and committed data files
//
// Possible states for deletion vectors:
// (1) No deletion record
// (2) Only uncommitted deletion record
// (3) Only committed deletion record
// (4) Uncommitted and committed deletion record
//
// For states, refer to https://docs.google.com/document/d/1hZ0H66_eefjFezFQf1Pdr-A63eJ94OH9TsInNS-6xao/edit?usp=sharing
//
// A few testing assumptions / preparations:
// - There will be at most two types of data files, one written before test case perform any append/delete operations, another after new rows appended.
// - To differentiate these two types of data files, the first type of record batch contains two rows, the second type of record batch contains only one row.

// Util function to create mooncake table and iceberg table manager.
async fn create_table_and_iceberg_manager(
    temp_dir: &TempDir,
) -> (MooncakeTable, IcebergTableManager) {
    let path = temp_dir.path().to_path_buf();
    let warehouse_uri = path.clone().to_str().unwrap().to_string();
    let mooncake_table_metadata =
        create_test_table_metadata(temp_dir.path().to_str().unwrap().to_string());
    let identity_property = mooncake_table_metadata.identity.clone();

    let iceberg_table_config = IcebergTableConfig {
        warehouse_uri,
        namespace: vec!["namespace".to_string()],
        table_name: "test_table".to_string(),
        drop_table_if_exists: false,
    };
    let schema = create_test_arrow_schema();

    // Create iceberg snapshot whenever `create_snapshot` is called.
    let mut mooncake_table_config = MooncakeTableConfig::new();
    mooncake_table_config.iceberg_snapshot_new_data_file_count = 0;
    let table = MooncakeTable::new(
        schema.as_ref().clone(),
        "test_table".to_string(),
        /*version=*/ 1,
        path,
        identity_property,
        iceberg_table_config.clone(),
        mooncake_table_config,
    )
    .await;

    let iceberg_table_manager = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        iceberg_table_config.clone(),
    );

    (table, iceberg_table_manager)
}

// Test util function to prepare for committed and persisted data file,
// here we write two rows and assume they'll be included in one arrow record batch and one data file.
// Return the second row for later deletion if necessary.
async fn prepare_committed_and_flushed_data_files(
    table: &mut MooncakeTable,
    lsn: u64,
) -> MoonlinkRow {
    // Append first row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();

    // Append second row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(4),
        RowValue::ByteArray("David".as_bytes().to_vec()),
        RowValue::Int32(40),
    ]);
    table.append(row.clone()).unwrap();

    table.commit(lsn);
    table.flush(lsn).await.unwrap();

    row
}

// Test util function to check whether the data file in iceberg snapshot is the same as initially-prepared one from `prepare_committed_and_flushed_data_files`.
//
// # Arguments
//
// * deleted: whether the record in the data file has been deleted.
async fn check_prev_data_files(
    snapshot: &Snapshot,
    iceberg_table_manager: &IcebergTableManager,
    deleted: bool,
) {
    assert_eq!(snapshot.disk_files.len(), 1);
    let (data_file, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    let loaded_arrow_batch = load_arrow_batch(file_io, data_file.file_name().as_str())
        .await
        .unwrap();
    let expected_arrow_batch = RecordBatch::try_new(
        iceberg_table_manager.mooncake_table_metadata.schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 4])),
            Arc::new(StringArray::from(vec!["John", "David"])),
            Arc::new(Int32Array::from(vec![30, 40])),
        ],
    )
    .unwrap();
    assert_eq!(loaded_arrow_batch, expected_arrow_batch);

    // In the test suite, we only delete the second prepared row.
    assert!(!deletion_vector
        .batch_deletion_vector
        .is_deleted(/*row_idx=*/ 0));
    if deleted {
        assert!(deletion_vector
            .batch_deletion_vector
            .is_deleted(/*row_idx=*/ 1));
    } else {
        assert!(!deletion_vector
            .batch_deletion_vector
            .is_deleted(/*row_idx=*/ 1));
    }
}

// Test util function to check whether the data file in iceberg snapshot is the same as the newly appended row.
//
// # Arguments
//
// * deleted: whether the record in the data file has been deleted.
async fn check_new_data_files(
    snapshot: &Snapshot,
    iceberg_table_manager: &IcebergTableManager,
    deleted: bool,
) {
    assert_eq!(snapshot.disk_files.len(), 1);
    let (data_file, deletion_vector) = snapshot.disk_files.iter().next().unwrap();
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();
    let loaded_arrow_batch = load_arrow_batch(file_io, data_file.file_name().as_str())
        .await
        .unwrap();
    let expected_arrow_batch = RecordBatch::try_new(
        iceberg_table_manager.mooncake_table_metadata.schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["John"])),
            Arc::new(Int32Array::from(vec![30])),
        ],
    )
    .unwrap();
    assert_eq!(loaded_arrow_batch, expected_arrow_batch);

    // In the test suite, we only delete the second prepared row.
    assert!(!deletion_vector
        .batch_deletion_vector
        .is_deleted(/*row_idx=*/ 0));
    if deleted {
        assert!(deletion_vector
            .batch_deletion_vector
            .is_deleted(/*row_idx=*/ 1));
    } else {
        assert!(!deletion_vector
            .batch_deletion_vector
            .is_deleted(/*row_idx=*/ 1));
    }
}

// Test util function to check whether the data files in iceberg snapshot is the same as initially-prepared one from `prepare_committed_and_flushed_data_files`, and the newly added row.
//
// # Arguments
//
// * deleted: whether the record in the data file has been deleted.
async fn check_prev_and_new_data_files(
    snapshot: &Snapshot,
    iceberg_table_manager: &IcebergTableManager,
    deleted: Vec<bool>,
) {
    assert_eq!(snapshot.disk_files.len(), 2);
    let file_io = iceberg_table_manager
        .iceberg_table
        .as_ref()
        .unwrap()
        .file_io();

    let mut loaded_record_batches: Vec<RecordBatch> = Vec::with_capacity(2);
    let mut batch_deletion_vectors: Vec<&BatchDeletionVector> = Vec::with_capacity(2);
    for (cur_data_file, cur_deletion_vector) in snapshot.disk_files.iter() {
        let cur_arrow_batch = load_arrow_batch(file_io, cur_data_file.file_name().as_str())
            .await
            .unwrap();
        loaded_record_batches.push(cur_arrow_batch);
        batch_deletion_vectors.push(&cur_deletion_vector.batch_deletion_vector);
    }
    // In the test suite, the first record has two rows, and the second record has one row.
    if loaded_record_batches[0].num_rows() == 1 {
        loaded_record_batches.swap(0, 1);
        batch_deletion_vectors.swap(0, 1);
    }

    // Check data files.
    let expected_arrow_batch_1 = RecordBatch::try_new(
        iceberg_table_manager.mooncake_table_metadata.schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 4])),
            Arc::new(StringArray::from(vec!["John", "David"])),
            Arc::new(Int32Array::from(vec![30, 40])),
        ],
    )
    .unwrap();
    let expected_arrow_batch_2 = RecordBatch::try_new(
        iceberg_table_manager.mooncake_table_metadata.schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["Tim"])),
            Arc::new(Int32Array::from(vec![20])),
        ],
    )
    .unwrap();
    assert_eq!(loaded_record_batches[0], expected_arrow_batch_1);
    assert_eq!(loaded_record_batches[1], expected_arrow_batch_2);

    // Check deletion vector.
    assert!(!batch_deletion_vectors[0].is_deleted(/*row_idx=*/ 0));
    if deleted[0] {
        assert!(batch_deletion_vectors[0].is_deleted(/*row_idx=*/ 1));
    } else {
        assert!(!batch_deletion_vectors[0].is_deleted(/*row_idx=*/ 1));
    }

    if deleted[1] {
        assert!(batch_deletion_vectors[1].is_deleted(/*row_idx=*/ 0));
    } else {
        assert!(!batch_deletion_vectors[1].is_deleted(/*row_idx=*/ 0));
    }
}

// Testing combination: (1) + (1) => no snapshot
#[tokio::test]
async fn test_state_1_1() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    assert!(table.create_snapshot().is_none());

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (1) + (2) => no snapshot
#[tokio::test]
async fn test_state_1_2() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    // Prepate deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 1).await;

    // Request to create snapshot.
    assert!(table.create_snapshot().is_none());

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (1) + (3) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_1_3() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (1) + (4) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_1_4() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 400).await;

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (2) + (1) => no snapshot
#[tokio::test]
async fn test_state_2_1() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (2) + (2) => no snapshot
#[tokio::test]
async fn test_state_2_2() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    // Prepate deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 200).await;

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (2) + (3) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_2_3() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite.
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (2) + (4) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_2_4() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 500).await;

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (1) => snapshot with data file created
#[tokio::test]
async fn test_state_3_1() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_new_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (2) => snapshot with data files created
#[tokio::test]
async fn test_state_3_2() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();
    // Prepate deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 300).await;

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_new_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (3) + committed deletion before flush => snapshot with data files and deletion vector
#[tokio::test]
async fn test_state_3_3_deletion_before_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 500).await.unwrap();

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (3) + committed deletion after flush => snapshot with data files
#[tokio::test]
async fn test_state_3_3_deletion_after_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![false, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (4) + committed deletion record before flush => snapshot with data files with deletion vector
#[tokio::test]
async fn test_state_3_4_committed_deletion_before_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 500).await.unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (3) + (4) + committed deletion record after flush => snapshot with data files
#[tokio::test]
async fn test_state_3_4_committed_deletion_after_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![false, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (4) + (1) => no snapshot
#[tokio::test]
async fn test_state_4_1() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite (committed record batch).
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (4) + (2) => no snapshot
#[tokio::test]
async fn test_state_4_2() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite (committed record batch).
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    // Prepate deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 200).await;
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    assert!(snapshot.data_file_flush_lsn.is_none());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (4) + (3) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_4_3() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed record batch).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (4) + (4) => no snapshot, depends on snapshot threshold
#[tokio::test]
async fn test_state_4_4() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 500).await;
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 100);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (1) => snapshot with data file created
#[tokio::test]
async fn test_state_5_1() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 300);

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_new_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (2) => snapshot with data files created
#[tokio::test]
async fn test_state_5_2() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();
    // Prepate deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 300).await;
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 400);

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_new_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (3) + committed deletion before flush => snapshot with data files and deletion vector
#[tokio::test]
async fn test_state_5_3_deletion_before_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 500).await.unwrap();
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 600);

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (3) + committed deletion after flush => snapshot with data files
#[tokio::test]
async fn test_state_5_3_deletion_after_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 600);

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![false, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (4) + committed deletion record before flush => snapshot with data files with deletion vector
#[tokio::test]
async fn test_state_5_4_committed_deletion_before_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 500).await.unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 700);

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (5) + (4) + committed deletion record after flush => snapshot with data files
#[tokio::test]
async fn test_state_5_4_committed_deletion_after_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 700);

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![false, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (1) => snapshot with data file created
#[tokio::test]
async fn test_state_6_1() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 300);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_new_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (2) => snapshot with data files created
#[tokio::test]
async fn test_state_6_2() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare data file pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 100);
    table.flush(/*lsn=*/ 200).await.unwrap();
    // Prepate deletion log pre-requisite.
    table.delete(row.clone(), /*lsn=*/ 300).await;
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 400);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_new_data_files(&snapshot, &iceberg_table_manager, /*deleted=*/ false).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 200);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (3) + committed deletion before flush => snapshot with data files and deletion vector
#[tokio::test]
async fn test_state_6_3_deletion_before_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 500).await.unwrap();
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 600);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (3) + committed deletion after flush => snapshot with data files
#[tokio::test]
async fn test_state_6_3_deletion_after_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 600);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![false, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (4) + committed deletion record before flush => snapshot with data files with deletion vector
#[tokio::test]
async fn test_state_6_4_committed_deletion_before_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 200).await;
    table.commit(/*lsn=*/ 300);
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 400);
    table.flush(/*lsn=*/ 500).await.unwrap();
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 700);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![true, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 500);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}

// Testing combination: (6) + (4) + committed deletion record after flush => snapshot with data files
#[tokio::test]
async fn test_state_6_4_committed_deletion_after_flush() -> IcebergResult<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut table, mut iceberg_table_manager) = create_table_and_iceberg_manager(&temp_dir).await;

    // Prepare environment setup.
    let old_row = prepare_committed_and_flushed_data_files(&mut table, /*lsn=*/ 100).await;
    // Prepate data files pre-requisite.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Tim".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 200);
    table.flush(/*lsn=*/ 300).await.unwrap();
    // Prepare deletion pre-requisite (committed deletion record).
    table.delete(old_row.clone(), /*lsn=*/ 400).await;
    table.commit(/*lsn=*/ 500);
    // Prepare deletion pre-requisite (uncommitted deletion record).
    table.delete(row.clone(), /*lsn=*/ 600).await;
    // Prepare committed but unflushed record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(3),
        RowValue::ByteArray("Cat".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row).unwrap();
    table.commit(/*lsn=*/ 700);
    // Prepare uncommitted record batch.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(5),
        RowValue::ByteArray("Ethan".as_bytes().to_vec()),
        RowValue::Int32(50),
    ]);
    table.append(row).unwrap();

    // Request to create snapshot.
    table.create_mooncake_and_iceberg_snapshot_for_test().await;

    // Check iceberg snapshot status.
    let snapshot = iceberg_table_manager.load_snapshot_from_table().await?;
    check_prev_and_new_data_files(
        &snapshot,
        &iceberg_table_manager,
        /*deleted=*/ vec![false, false],
    )
    .await;
    assert_eq!(snapshot.indices.file_indices.len(), 2);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 300);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;

    Ok(())
}
