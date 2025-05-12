use crate::storage::iceberg::catalog_utils;
use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
};
use crate::storage::iceberg::moonlink_catalog::MoonlinkCatalog;
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::validation as IcebergValidation;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use iceberg::io::FileIO;
use iceberg::puffin::CompressionCodec;
use iceberg::spec::DataFileFormat;
use iceberg::spec::{DataFile, ManifestEntry};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::Transaction;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::writer::file_writer::location_generator::LocationGenerator;
use iceberg::Error as IcebergError;
use iceberg::Result as IcebergResult;
use typed_builder::TypedBuilder;
use uuid::Uuid;

#[cfg(test)]
use mockall::*;

#[allow(dead_code)]
#[derive(Clone, Debug, TypedBuilder)]
pub struct IcebergTableManagerConfig {
    /// Table warehouse location.
    #[builder(default = "/tmp/moonlink_iceberg".to_string())]
    pub warehouse_uri: String,
    /// Mooncake table metadata.
    pub mooncake_table_schema: Arc<ArrowSchema>,
    /// Namespace for the iceberg table.
    #[builder(default = vec!["default".to_string()])]
    pub namespace: Vec<String>,
    /// Iceberg table name.
    #[builder(default = "table".to_string())]
    pub table_name: String,
}

#[allow(dead_code)]
#[cfg_attr(test, automock)]
pub(crate) trait IcebergOperation {
    /// Write a new snapshot to iceberg table.
    /// It could be called for multiple times to write and commit multiple snapshots.
    ///
    /// Please notice, it takes full content to commit snapshot.
    ///
    /// TODO(hjiang): We're storing the iceberg table status in two places, one for iceberg table manager, another at snapshot.
    /// Provide delta change interface, so snapshot doesn't need to store everything.
    async fn sync_snapshot(
        &mut self,
        snapshot_disk_files: HashMap<PathBuf, BatchDeletionVector>,
    ) -> IcebergResult<()>;

    /// Load latest snapshot from iceberg table. Used for recovery and initialization.
    async fn load_snapshot_from_table(&mut self) -> IcebergResult<()>
    where
        Self: Sized;
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct DataFileEntry {
    /// Iceberg data file, used to decide what to persist at new commit requests.
    data_file: DataFile,
    /// In-memory deletion vector.
    deletion_vector: BatchDeletionVector,
}

/// TODO(hjiang):
/// 1. Support a data file handle, which is a remote file path, plus an optional local cache filepath.
/// 2. Support a deletion vector handle, which is a remote file path, with an optional in-memory buffer and a local cache filepath.
#[allow(dead_code)]
#[derive(Debug)]
pub struct IcebergTableManager {
    /// Iceberg table configuration.
    config: IcebergTableManagerConfig,

    /// Iceberg catalog, which interacts with the iceberg table.
    catalog: Box<dyn MoonlinkCatalog>,

    /// The iceberg table it's managing.
    iceberg_table: Option<IcebergTable>,

    /// Maps from already persisted data file filepath to its deletion vector, and iceberg `DataFile`.
    persisted_items: HashMap<PathBuf, DataFileEntry>,
}

impl IcebergTableManager {
    #[allow(dead_code)]
    pub fn new(config: IcebergTableManagerConfig) -> IcebergTableManager {
        let catalog = catalog_utils::create_catalog(&config.warehouse_uri).unwrap();
        Self {
            config,
            catalog,
            iceberg_table: None,
            persisted_items: HashMap::new(),
        }
    }

    /// Get or create an iceberg table, and load full table status into table manager.
    async fn get_or_create_table(&mut self) -> IcebergResult<()> {
        if self.iceberg_table.is_none() {
            let table = catalog_utils::get_or_create_iceberg_table(
                &*self.catalog,
                &self.config.warehouse_uri,
                &self.config.namespace,
                &self.config.table_name.clone(),
                self.config.mooncake_table_schema.as_ref(),
            )
            .await?;
            self.iceberg_table = Some(table);
        }
        Ok(())
    }

    /// Write deletion vector to puffin file.
    async fn write_deletion_vector(
        &mut self,
        data_file: String,
        deletion_vector: BatchDeletionVector,
    ) -> IcebergResult<()> {
        let deleted_rows = deletion_vector.collect_deleted_rows();
        if deleted_rows.is_empty() {
            return Ok(());
        }

        if !deleted_rows.is_empty() {
            // TODO(hjiang): Currently one deletion vector is stored in one puffin file, need to revisit later.
            let deleted_row_count = deleted_rows.len();
            let mut iceberg_deletion_vector = DeletionVector::new();
            iceberg_deletion_vector.mark_rows_deleted(deleted_rows);
            let blob_properties = HashMap::from([
                (DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(), data_file),
                (
                    DELETION_VECTOR_CADINALITY.to_string(),
                    deleted_row_count.to_string(),
                ),
            ]);
            let table_metadata = self.iceberg_table.as_ref().unwrap().metadata();
            let blob = iceberg_deletion_vector.serialize(
                table_metadata.current_snapshot_id().unwrap_or(-1),
                table_metadata.next_sequence_number(),
                blob_properties,
            );

            // TODO(hjiang): Current iceberg-rust doesn't support deletion vector officially, so we do our own hack to rewrite manifest file by our own catalog implementation.
            let location_generator = DefaultLocationGenerator::new(
                self.iceberg_table.as_ref().unwrap().metadata().clone(),
            )?;
            let puffin_filepath =
                location_generator.generate_location(&format!("{}-puffin.bin", Uuid::new_v4()));
            let mut puffin_writer = puffin_utils::create_puffin_writer(
                self.iceberg_table.as_ref().unwrap().file_io(),
                puffin_filepath.clone(),
            )
            .await?;
            // TODO(hjiang): Provide option to enable compression for puffin blob.
            puffin_writer.add(blob, CompressionCodec::None).await?;

            self.catalog
                .record_puffin_metadata_and_close(puffin_filepath, puffin_writer)
                .await?;
        }

        Ok(())
    }

    /// Load data file into table manager from the current manifest entry.
    async fn load_data_file_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
    ) -> IcebergResult<()> {
        let data_file = entry.data_file();
        if data_file.file_format() == DataFileFormat::Puffin {
            return Ok(());
        }

        let file_path = PathBuf::from(data_file.file_path().to_string());
        assert_eq!(
            data_file.file_format(),
            DataFileFormat::Parquet,
            "Data file is of file format parquet for entry {:?}.",
            entry,
        );
        let new_data_file_entry = DataFileEntry {
            data_file: data_file.clone(),
            deletion_vector: BatchDeletionVector::new(/*max_rows=*/ 0),
        };
        let old_entry = self.persisted_items.insert(file_path, new_data_file_entry);
        assert!(old_entry.is_none());
        Ok(())
    }

    /// Load deletion vector into table manager from the current manifest entry.
    async fn load_deletion_vector_from_manifest_entry(
        &mut self,
        entry: &ManifestEntry,
        file_io: &FileIO,
    ) -> IcebergResult<()> {
        let data_file = entry.data_file();
        if data_file.file_format() == DataFileFormat::Parquet {
            return Ok(());
        }

        let referenced_path_buf: PathBuf = data_file.referenced_data_file().unwrap().into();
        let data_file_entry = self.persisted_items.get_mut(&referenced_path_buf);
        assert!(
            data_file_entry.is_some(),
            "At recovery, the data file path for {:?} doesn't exist",
            referenced_path_buf
        );

        IcebergValidation::validate_puffin_manifest_entry(entry)?;
        let deletion_vector = DeletionVector::load_from_dv_blob(file_io.clone(), data_file).await?;
        let batch_deletion_vector = deletion_vector.take_as_batch_delete_vector();
        data_file_entry.unwrap().deletion_vector = batch_deletion_vector;

        Ok(())
    }
}

/// TODO(hjiang): Parallelize all IO operations.
impl IcebergOperation for IcebergTableManager {
    async fn sync_snapshot(
        &mut self,
        mut disk_files: HashMap<PathBuf, BatchDeletionVector>,
    ) -> IcebergResult<()> {
        // Initialize iceberg table on access.
        self.get_or_create_table().await?;

        let mut new_data_files = vec![];
        let mut new_persisted_items: HashMap<PathBuf, DataFileEntry> = HashMap::new();
        let old_persisted_items = self.persisted_items.clone();
        for (local_path, deletion_vector) in disk_files.drain() {
            match old_persisted_items.get(&local_path) {
                Some(entry) => {
                    if entry.deletion_vector == deletion_vector {
                        let mut new_data_file_entry: DataFileEntry = entry.clone();
                        new_data_file_entry.deletion_vector = deletion_vector;
                        new_persisted_items.insert(local_path, new_data_file_entry);
                        continue;
                    }
                    let path_str = entry.data_file.file_path().to_string();
                    self.write_deletion_vector(path_str, deletion_vector.clone())
                        .await?;

                    let mut new_data_file_entry: DataFileEntry = entry.clone();
                    new_data_file_entry.deletion_vector = deletion_vector;
                    new_persisted_items.insert(local_path, new_data_file_entry);
                }
                None => {
                    let data_file = catalog_utils::write_record_batch_to_iceberg(
                        self.iceberg_table.as_ref().unwrap(),
                        &local_path,
                    )
                    .await?;
                    let data_filepath = data_file.file_path().to_string();
                    new_data_files.push(data_file.clone());
                    self.write_deletion_vector(data_filepath, deletion_vector.clone())
                        .await?;
                    new_persisted_items.insert(
                        local_path,
                        DataFileEntry {
                            data_file,
                            deletion_vector,
                        },
                    );
                }
            }
        }

        // If no change for both data files and deletion vectors, no need to initiate a new transaction.
        if self.persisted_items == new_persisted_items {
            return Ok(());
        }
        self.persisted_items = new_persisted_items;

        // Only start append action when there're new data files.
        let mut txn = Transaction::new(self.iceberg_table.as_ref().unwrap());
        if !new_data_files.is_empty() {
            let mut action =
                txn.fast_append(/*commit_uuid=*/ None, /*key_metadata=*/ vec![])?;
            action.add_data_files(new_data_files)?;
            txn = action.apply().await?;
        }

        self.iceberg_table = Some(txn.commit(&*self.catalog).await?);
        self.catalog.clear_puffin_metadata();

        Ok(())
    }

    // TODO(hjiang): Write a macro to avoid verbose error mapping.
    async fn load_snapshot_from_table(&mut self) -> IcebergResult<()> {
        if self.iceberg_table.is_some() {
            return Ok(());
        }
        self.get_or_create_table().await?;
        let table_metadata = self.iceberg_table.as_ref().unwrap().metadata();

        // There's nothing stored in iceberg table (aka, first time initialization).
        if table_metadata.current_snapshot().is_none() {
            return Ok(());
        }

        // Load table state into iceberg table manager.
        let snapshot_meta = table_metadata.current_snapshot().unwrap();
        let manifest_list = snapshot_meta
            .load_manifest_list(
                self.iceberg_table.as_ref().unwrap().file_io(),
                table_metadata,
            )
            .await?;

        let file_io = self.iceberg_table.as_ref().unwrap().file_io().clone();
        for manifest_file in manifest_list.entries().iter() {
            // All files (i.e. data files, deletion vector, manifest files) under the same snapshot are assigned with the same sequence number.
            // Reference: https://iceberg.apache.org/spec/?h=content#sequence-numbers
            let manifest = manifest_file.load_manifest(&file_io).await?;
            let (manifest_entries, _) = manifest.into_parts();
            let snapshot_seq_no = manifest_entries.first().unwrap().sequence_number();
            for entry in manifest_entries.into_iter() {
                // Sanity check all manifest entries are of the sequence number.
                let cur_entry_seq_no = entry.sequence_number();
                if snapshot_seq_no != cur_entry_seq_no {
                    return Err(IcebergError::new(
                        iceberg::ErrorKind::DataInvalid,
                        format!("When reading from iceberg table, snapshot sequence id inconsistency found {:?} vs {:?}", snapshot_seq_no, cur_entry_seq_no),
                    ));
                }
                // On load, we do two pass on all entries, to check whether all deletion vector has a corresponding data file.
                self.load_data_file_from_manifest_entry(entry.as_ref())
                    .await?;
                self.load_deletion_vector_from_manifest_entry(entry.as_ref(), &file_io)
                    .await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::row::MoonlinkRow;
    use crate::row::{IdentityProp, RowValue};
    use crate::storage::iceberg::iceberg_table_manager::IcebergTableManager;
    #[cfg(feature = "storage-s3")]
    use crate::storage::iceberg::s3_test_utils;
    use crate::storage::MooncakeTable;

    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::tempdir;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use iceberg::io::FileRead;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;

    /// Create test batch deletion vector.
    fn test_deletion_vector_1() -> BatchDeletionVector {
        let mut deletion_vector = BatchDeletionVector::new(/*max_rows=*/ 3);
        deletion_vector.delete_row(2);
        deletion_vector
    }
    /// Test deletion vector 2 includes deletion vector 1, used to mimic new data file rows deletion situation.
    fn test_deletion_vector_2() -> BatchDeletionVector {
        let mut deletion_vector = BatchDeletionVector::new(/*max_rows=*/ 3);
        deletion_vector.delete_row(1);
        deletion_vector.delete_row(2);
        deletion_vector
    }

    /// Test util function to create arrow schema.
    fn create_test_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, /*nullable=*/ false).with_metadata(
                HashMap::from([("PARQUET:field_id".to_string(), "1".to_string())]),
            ),
            ArrowField::new("name", ArrowDataType::Utf8, /*nullable=*/ false).with_metadata(
                HashMap::from([("PARQUET:field_id".to_string(), "2".to_string())]),
            ),
        ]))
    }

    /// Test util function to create arrow record batch.
    fn test_batch_1(arrow_schema: Arc<ArrowSchema>) -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])), // id column
                Arc::new(StringArray::from(vec!["a", "b", "c"])), // name column
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
        let file = File::create(&path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(batch)?;
        writer.close()?;
        Ok(())
    }

    /// Test util function to load all arrow batch from the given parquert file.
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

    /// Test snapshot store and load for different types of catalogs based on the given warehouse.
    async fn test_store_and_load_snapshot_impl(
        iceberg_table_manager: &mut IcebergTableManager,
    ) -> IcebergResult<()> {
        // At the beginning of the test, there's nothing in table.
        assert!(iceberg_table_manager.persisted_items.is_empty());

        // Create arrow schema and table.
        let arrow_schema = create_test_arrow_schema();
        let tmp_dir = tempdir()?;

        // Write first snapshot to iceberg table (with deletion vector).
        let data_filename_1 = "data-1.parquet";
        let batch = test_batch_1(arrow_schema.clone());
        let parquet_path = tmp_dir.path().join(data_filename_1);
        write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch)
            .await?;
        let mut snapshot_disk_files: HashMap<PathBuf, BatchDeletionVector> = HashMap::new();
        snapshot_disk_files.insert(parquet_path.clone(), test_deletion_vector_1());
        iceberg_table_manager
            .sync_snapshot(snapshot_disk_files.clone())
            .await?;

        // Write second snapshot to iceberg table, with updated deletion vector and new data file.
        snapshot_disk_files.insert(parquet_path.clone(), test_deletion_vector_2());

        let data_filename_2 = "data-2.parquet";
        let batch = test_batch_2(arrow_schema.clone());
        let parquet_path = tmp_dir.path().join(data_filename_2);
        write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch)
            .await?;
        snapshot_disk_files.insert(parquet_path.clone(), test_deletion_vector_1());
        iceberg_table_manager
            .sync_snapshot(snapshot_disk_files)
            .await?;

        // Check persisted items in the iceberg table.
        assert_eq!(
            iceberg_table_manager.persisted_items.len(),
            2,
            "Persisted items for table manager is {:?}",
            iceberg_table_manager.persisted_items
        );

        // Check the loaded data file is of the expected format and content.
        let file_io = iceberg_table_manager
            .iceberg_table
            .as_ref()
            .unwrap()
            .file_io();
        for (loaded_path, data_entry) in iceberg_table_manager.persisted_items.iter() {
            let loaded_arrow_batch =
                load_arrow_batch(file_io, loaded_path.to_str().unwrap()).await?;
            let deleted_rows = data_entry.deletion_vector.collect_deleted_rows();
            assert_eq!(
                *loaded_arrow_batch.schema_ref(),
                arrow_schema,
                "Expect arrow schema {:?}, actual arrow schema {:?}",
                arrow_schema,
                loaded_arrow_batch.schema_ref()
            );

            // Check second data file and its deletion vector.
            if loaded_path.to_str().unwrap().ends_with(data_filename_2) {
                assert_eq!(
                    loaded_arrow_batch,
                    test_batch_2(arrow_schema.clone()),
                    "Expect arrow record batch {:?}, actual arrow record batch {:?}",
                    loaded_arrow_batch,
                    test_batch_2(arrow_schema.clone())
                );
                assert_eq!(
                    deleted_rows,
                    test_deletion_vector_1().collect_deleted_rows(),
                    "Loaded deletion vector is not the same as the one gets stored."
                );
                continue;
            }

            // Check first data file and its deletion vector.
            assert!(loaded_path.to_str().unwrap().ends_with(data_filename_1));
            assert_eq!(
                loaded_arrow_batch,
                test_batch_1(arrow_schema.clone()),
                "Expect arrow record batch {:?}, actual arrow record batch {:?}",
                loaded_arrow_batch,
                test_batch_1(arrow_schema.clone())
            );
            assert_eq!(
                deleted_rows,
                test_deletion_vector_2().collect_deleted_rows(),
                "Loaded deletion vector is not the same as the one gets stored."
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_snapshots() -> IcebergResult<()> {
        // Create arrow schema and table.
        let arrow_schema = create_test_arrow_schema();
        let tmp_dir = tempdir()?;
        let config = IcebergTableManagerConfig {
            warehouse_uri: tmp_dir.path().to_str().unwrap().to_string(),
            mooncake_table_schema: arrow_schema,
            namespace: vec!["namespace".to_string()],
            table_name: "test_table".to_string(),
        };
        let mut iceberg_table_manager = IcebergTableManager::new(config);
        test_store_and_load_snapshot_impl(&mut iceberg_table_manager).await?;
        Ok(())
    }

    async fn mooncake_table_snapshot_persist_impl(warehouse_uri: String) -> IcebergResult<()> {
        // Create a schema for testing.
        let schema = Schema::new(vec![
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
        ]);
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_path_buf();
        let iceberg_table_config = IcebergTableManagerConfig {
            warehouse_uri,
            mooncake_table_schema: Arc::new(schema.clone()),
            namespace: vec!["namespace".to_string()],
            table_name: "test_table".to_string(),
        };
        let mut table = MooncakeTable::new(
            schema.clone(),
            "test_table".to_string(),
            /*version=*/ 1,
            path,
            IdentityProp::Keys(vec![0]),
            Some(iceberg_table_config.clone()),
        );

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
        // First deletion of row2, which happens in MemSlice.
        table.delete(row1.clone(), /*lsn=*/ 100);
        table.flush(/*lsn=*/ 200).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to flush records to mooncake table because {:?}", e),
            )
        })?;
        table.commit(/*lsn=*/ 200);
        table.create_snapshot().unwrap().await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to create snapshot to iceberg table because {:?}", e),
            )
        })?;

        // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
        let mut iceberg_table_manager = IcebergTableManager::new(iceberg_table_config.clone());
        iceberg_table_manager.load_snapshot_from_table().await?;
        assert_eq!(
            iceberg_table_manager.persisted_items.len(),
            1,
            "Persisted items for table manager is {:?}",
            iceberg_table_manager.persisted_items
        );

        // Check the loaded data file is of the expected format and content.
        let file_io = iceberg_table_manager
            .iceberg_table
            .as_ref()
            .unwrap()
            .file_io();
        let (loaded_path, data_entry) =
            iceberg_table_manager.persisted_items.iter().next().unwrap();
        let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.to_str().unwrap()).await?;
        let expected_arrow_batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
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

        let deleted_rows = data_entry.deletion_vector.collect_deleted_rows();
        assert!(
            deleted_rows.is_empty(),
            "There should be no deletion vector in iceberg table."
        );

        // --------------------------------------
        // Operation series 2: no more additional rows appended, only to delete the first row in the table.
        // Expects to see a new deletion vector, because its corresponding data file has been persisted.
        table.delete(row2.clone(), /*lsn=*/ 300);
        table.flush(/*lsn=*/ 300).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!(
                    "Failed to flush records {:?} to mooncake table because {:?}",
                    row2, e
                ),
            )
        })?;
        table.commit(/*lsn=*/ 300);
        table.create_snapshot().unwrap().await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to create snapshot to iceberg because {:?}", e),
            )
        })?;

        // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
        let mut iceberg_table_manager = IcebergTableManager::new(iceberg_table_config.clone());
        iceberg_table_manager.load_snapshot_from_table().await?;
        assert_eq!(
            iceberg_table_manager.persisted_items.len(),
            1,
            "Persisted items for table manager is {:?}",
            iceberg_table_manager.persisted_items
        );

        // Check the loaded data file is of the expected format and content.
        let file_io = iceberg_table_manager
            .iceberg_table
            .as_ref()
            .unwrap()
            .file_io();
        let (loaded_path, data_entry) =
            iceberg_table_manager.persisted_items.iter().next().unwrap();
        let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.to_str().unwrap()).await?;
        assert_eq!(
            loaded_arrow_batch, expected_arrow_batch,
            "Expected arrow data is {:?}, actual data is {:?}",
            expected_arrow_batch, loaded_arrow_batch
        );

        let deleted_rows = data_entry.deletion_vector.collect_deleted_rows();
        let expected_deleted_rows = vec![0_u64];
        assert_eq!(
            deleted_rows, expected_deleted_rows,
            "Expected deletion vector {:?}, actual deletion vector {:?}",
            expected_deleted_rows, deleted_rows
        );

        // --------------------------------------
        // Operation series 3: no more additional rows appended, only to delete the last row in the table.
        // Expects to see the existing deletion vector updated, because its corresponding data file has been persisted.
        table.delete(row3.clone(), /*lsn=*/ 400);
        table.flush(/*lsn=*/ 400).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to flush records to mooncake table because {:?}", e),
            )
        })?;
        table.commit(/*lsn=*/ 400);
        table.create_snapshot().unwrap().await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to create snapshot to iceberg because {:?}", e),
            )
        })?;

        // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
        let mut iceberg_table_manager = IcebergTableManager::new(iceberg_table_config.clone());
        iceberg_table_manager.load_snapshot_from_table().await?;
        assert_eq!(
            iceberg_table_manager.persisted_items.len(),
            1,
            "Persisted items for table manager is {:?}",
            iceberg_table_manager.persisted_items
        );

        // Check the loaded data file is of the expected format and content.
        let file_io = iceberg_table_manager
            .iceberg_table
            .as_ref()
            .unwrap()
            .file_io();
        let (loaded_path, data_entry) =
            iceberg_table_manager.persisted_items.iter().next().unwrap();
        let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.to_str().unwrap()).await?;
        assert_eq!(
            loaded_arrow_batch, expected_arrow_batch,
            "Expected arrow data is {:?}, actual data is {:?}",
            expected_arrow_batch, loaded_arrow_batch
        );

        let deleted_rows = data_entry.deletion_vector.collect_deleted_rows();
        let expected_deleted_rows = vec![0_u64, 1_u64];
        assert_eq!(
            deleted_rows, expected_deleted_rows,
            "Expected deletion vector {:?}, actual deletion vector {:?}",
            expected_deleted_rows, deleted_rows
        );

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
        table.flush(/*lsn=*/ 500).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to flush records to mooncake table because {:?}", e),
            )
        })?;
        table.commit(/*lsn=*/ 500);
        table.create_snapshot().unwrap().await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to create snapshot to iceberg table because {:?}", e),
            )
        })?;

        // Check iceberg snapshot store and load, here we explicitly load snapshot from iceberg table, whose construction is lazy and asynchronous by design.
        let mut iceberg_table_manager = IcebergTableManager::new(iceberg_table_config.clone());
        iceberg_table_manager.load_snapshot_from_table().await?;
        assert_eq!(
            iceberg_table_manager.persisted_items.len(),
            2,
            "Persisted items for table manager is {:?}",
            iceberg_table_manager.persisted_items
        );

        // The old data file and deletion vector is unchanged.
        let old_data_entry = iceberg_table_manager.persisted_items.remove(loaded_path);
        assert!(
            old_data_entry.is_some(),
            "Add new data file shouldn't change existing persisted items"
        );
        assert_eq!(
            &old_data_entry.unwrap(),
            data_entry,
            "Add new data file shouldn't change existing persisted items"
        );

        // Check new data file is correctly managed by iceberg table with no deletion vector.
        let (loaded_path, data_entry) =
            iceberg_table_manager.persisted_items.iter().next().unwrap();
        let loaded_arrow_batch = load_arrow_batch(file_io, loaded_path.to_str().unwrap()).await?;

        let expected_arrow_batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
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

        let deleted_rows = data_entry.deletion_vector.collect_deleted_rows();
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
}
