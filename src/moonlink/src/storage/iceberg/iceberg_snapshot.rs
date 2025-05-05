use super::catalog_utils::create_catalog;
use super::deletion_vector::DeletionVector;
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
};
use crate::storage::iceberg::file_catalog::FileSystemCatalog;
use crate::storage::iceberg::object_storage_catalog::S3Catalog;
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::test_utils;
use crate::storage::iceberg::validation::validate_puffin_manifest_entry;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::Snapshot;

use futures::executor::block_on;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;

use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow as IcebergArrow;
use iceberg::puffin::CompressionCodec;
use iceberg::spec::ManifestContentType;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::LocationGenerator;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::IcebergWriter;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::Error as IcebergError;
use iceberg::NamespaceIdent;
use iceberg::TableCreation;
use iceberg::{Catalog, Result as IcebergResult, TableIdent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use url::Url;
use uuid::Uuid;

// UNDONE(Iceberg):
// 1. Implement deletion file related load and store operations.
// (unrelated to functionality) 2. Update rest catalog service ip/port, currently it's hard-coded to devcontainer's config, which should be parsed from env variable or config files.
// (unrelated to functionality) 3. Add timeout to rest catalog access.
// (unrelated to functionality) 4. Use real namespace and table name, which we should be able to get it from moonlink, it's hard-coded to "default" and "test_table" for now.

// Get or create an iceberg table in the given catalog from the given namespace and table name.
async fn get_or_create_iceberg_table<C: Catalog + ?Sized>(
    catalog: &C,
    warehouse_uri: &str,
    namespace: &[&str],
    table_name: &str,
    arrow_schema: &ArrowSchema,
) -> IcebergResult<IcebergTable> {
    let namespace_ident = NamespaceIdent::from_strs(namespace).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.to_string());
    match catalog.load_table(&table_ident).await {
        Ok(table) => Ok(table),
        Err(_) => {
            let namespace_already_exists = catalog.namespace_exists(&namespace_ident).await?;
            if !namespace_already_exists {
                catalog
                    .create_namespace(&namespace_ident, /*properties=*/ HashMap::new())
                    .await?;
            }

            let iceberg_schema = IcebergArrow::arrow_schema_to_schema(arrow_schema)?;
            let tbl_creation = TableCreation::builder()
                .name(table_name.to_string())
                .location(format!(
                    "{}/{}/{}",
                    warehouse_uri,
                    namespace_ident.to_url_string(),
                    table_name
                ))
                .schema(iceberg_schema)
                .properties(HashMap::new())
                .build();
            let table = catalog
                .create_table(&table_ident.namespace, tbl_creation)
                .await?;
            Ok(table)
        }
    }
}

// Write the given record batch to the iceberg table.
async fn write_record_batch_to_iceberg(
    table: &IcebergTable,
    parquet_filepath: &PathBuf,
) -> IcebergResult<DataFile> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_generator = DefaultFileNameGenerator::new(
        /*prefix=*/ Uuid::new_v4().to_string(),
        /*suffix=*/ None,
        /*format=*/ DataFileFormat::Parquet,
    );

    let file = std::fs::File::open(parquet_filepath)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut arrow_reader = builder.build()?;

    let parquet_writer_builder = ParquetWriterBuilder::new(
        /*props=*/ WriterProperties::default(),
        /*schame=*/ table.metadata().current_schema().clone(),
        /*file_io=*/ table.file_io().clone(),
        /*location_generator=*/ location_generator,
        /*file_name_generator=*/ file_name_generator,
    );

    // TOOD(hjiang): Add support for partition values.
    let data_file_writer_builder = DataFileWriterBuilder::new(
        parquet_writer_builder,
        /*partition_value=*/ None,
        /*partition_spec_id=*/ 0,
    );
    let mut data_file_writer = data_file_writer_builder.build().await?;
    while let Some(record_batch) = arrow_reader.next().transpose()? {
        data_file_writer.write(record_batch).await?;
    }

    let data_files = data_file_writer.close().await?;
    assert_eq!(
        data_files.len(),
        1,
        "Should only have one parquet file written"
    );

    Ok(data_files[0].clone())
}

// TODO(hjiang): A few data file properties need to respect and consider.
// Reference:
// - https://iceberg.apache.org/docs/latest/configuration/#table-properties
// - https://iceberg.apache.org/docs/latest/configuration/#table-behavior-properties
//
// - write.parquet.row-group-size-bytes
// - write.parquet.page-size-bytes
// - write.parquet.page-row-limit
// - write.parquet.dict-size-bytes
// - write.parquet.compression-codec
// - write.parquet.compression-level
// - write.parquet.bloom-filter-max-bytes
// - write.metadata.compression-codec
pub trait IcebergSnapshot {
    // Write the current version to iceberg
    async fn _export_to_iceberg(&mut self) -> IcebergResult<()>;

    // Create a snapshot by reading from iceberg in async style.
    //
    // TODO(hjiang): Expose an async function only for tokio-based test, otherwise will suffer deadlock with executor.
    // Reference: https://greptime.com/blogs/2023-03-09-bridging-async-and-sync-rust
    async fn _async_load_from_iceberg(&self) -> IcebergResult<Self>
    where
        Self: Sized;

    // Create a snapshot by reading from iceberg
    fn _load_from_iceberg(&self) -> IcebergResult<Self>
    where
        Self: Sized;
}

impl IcebergSnapshot for Snapshot {
    // TODO(hjiang):
    // 1. Extract into multiple functions, for example, data file persistence, deletion vector persistence, iceberg table transaction.
    // 2. Parallalize IO operations, now they're implemented in sequential style.
    #[allow(clippy::await_holding_refcell_ref)]
    async fn _export_to_iceberg(&mut self) -> IcebergResult<()> {
        let url = Url::parse(&self.warehouse_uri)
            .or_else(|_| Url::from_file_path(self.warehouse_uri.clone()))
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Invalid warehouse URI {}: {:?}",
                        self.warehouse_uri.clone(),
                        e
                    ),
                )
            })?;

        // Create catalog based on warehouse uri.
        //
        // TODO(hjiang):
        // 1. This is a hacky way to check whether catalog is moonlink self-implemented ones, which support deletion vector.
        // We should revisit to check more rust-idiomatic way.
        // 2. Logic to decide which catalog to use should be rewritten here; when we release the pg_mooncake and moonlink,
        // likely only filesystem catalog and object storage catalog will be used due to deletion vector support.
        #[allow(unused_assignments)]
        let mut opt_catalog: Option<Rc<RefCell<dyn Catalog>>> = None;
        let mut filesystem_catalog: Option<Rc<RefCell<FileSystemCatalog>>> = None;
        let mut object_storage_catalog: Option<Rc<RefCell<S3Catalog>>> = None;

        // Special handle testing situation.
        if self
            .warehouse_uri
            .starts_with(test_utils::MINIO_TEST_WAREHOUSE_URI_PREFIX)
        {
            let test_bucket = test_utils::get_test_minio_bucket(&self.warehouse_uri);
            let internal_s3_config = Rc::new(RefCell::new(test_utils::create_minio_s3_catalog(
                &test_bucket,
                &self.warehouse_uri,
            )));
            object_storage_catalog = Some(internal_s3_config.clone());
            let catalog_rc: Rc<RefCell<dyn Catalog>> = internal_s3_config.clone();
            opt_catalog = Some(catalog_rc);
        } else if url.scheme() == "file" {
            let absolute_path = url.path();
            let internal_fs_catalog = Rc::new(RefCell::new(FileSystemCatalog::new(
                absolute_path.to_string(),
            )));
            filesystem_catalog = Some(internal_fs_catalog.clone());
            let catalog_rc: Rc<RefCell<dyn Catalog>> = internal_fs_catalog.clone();
            opt_catalog = Some(catalog_rc);
        } else {
            // TODO(hjiang): Fallback to object storage for all warehouse uris.
            todo!("Need to take secrets from client side and create object storage catalog.")
        }
        // `catalog` is guaranteed to be valid.
        let catalog: Rc<RefCell<dyn Catalog>> = opt_catalog.unwrap();

        let table_name = self.metadata.name.clone();
        let namespace = vec!["default"];
        let arrow_schema = self.metadata.schema.as_ref();
        let iceberg_table = get_or_create_iceberg_table(
            &*catalog.borrow(),
            &self.warehouse_uri,
            &namespace,
            &table_name,
            arrow_schema,
        )
        .await?;

        let txn = Transaction::new(&iceberg_table);
        let mut action =
            txn.fast_append(/*commit_uuid=*/ None, /*key_metadata=*/ vec![])?;

        let mut new_disk_files = HashMap::with_capacity(self.disk_files.len());
        let mut new_data_files = Vec::with_capacity(self.disk_files.len());
        for (file_path, deletion_vector) in self.disk_files.drain() {
            // Write data files to iceberg table.
            let data_file =
                write_record_batch_to_iceberg(&iceberg_table.clone(), &file_path).await?;
            let new_path = PathBuf::from(data_file.file_path());

            // Record deleted rows in the deletion vector.
            let deleted_rows = deletion_vector.collect_deleted_rows();
            if !deleted_rows.is_empty() {
                // TODO(hjiang): Currently one deletion vector is stored in one puffin file, need to revisit later.
                let deleted_row_count = deleted_rows.len();
                let mut iceberg_deletion_vector = DeletionVector::new();
                iceberg_deletion_vector.mark_rows_deleted(deleted_rows);
                let blob_properties = HashMap::from([
                    (
                        DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(),
                        new_path.to_str().unwrap().to_string(),
                    ),
                    (
                        DELETION_VECTOR_CADINALITY.to_string(),
                        deleted_row_count.to_string(),
                    ),
                ]);
                let table_metadata = iceberg_table.metadata();
                let blob = iceberg_deletion_vector.serialize(
                    table_metadata.current_snapshot_id().unwrap_or(-1),
                    table_metadata.next_sequence_number(),
                    blob_properties,
                );

                // TODO(hjiang): Current iceberg-rust doesn't support deletion vector officially, so we do our own hack to rewrite manifest file by our own catalog implementation.
                let location_generator =
                    DefaultLocationGenerator::new(iceberg_table.metadata().clone())?;
                let puffin_filepath =
                    location_generator.generate_location(&format!("{}-puffin.bin", Uuid::new_v4()));
                let mut puffin_writer = puffin_utils::create_puffin_writer(
                    iceberg_table.file_io(),
                    puffin_filepath.clone(),
                )
                .await?;
                // TODO(hjiang): Provide option to enable compression for puffin blob.
                puffin_writer.add(blob, CompressionCodec::None).await?;

                if let Some(filesystem_catalog_val) = &mut filesystem_catalog {
                    filesystem_catalog_val
                        .borrow_mut()
                        .record_puffin_metadata_and_close(puffin_filepath, puffin_writer)
                        .await?;
                } else if let Some(object_storage_catalog_val) = &mut object_storage_catalog {
                    object_storage_catalog_val
                        .borrow_mut()
                        .record_puffin_metadata_and_close(puffin_filepath, puffin_writer)
                        .await?;
                }
            }

            new_disk_files.insert(new_path, deletion_vector);
            new_data_files.push(data_file);
        }

        // TODO(hjiang): Likely we should bookkeep the old files so they could be deleted later.
        self.disk_files = new_disk_files;
        action.add_data_files(new_data_files)?;

        // Commit write transaction, which internally creates manifest files and manifest list files.
        let txn = action.apply().await?;
        txn.commit(&*catalog.borrow()).await?;

        Ok(())
    }

    async fn _async_load_from_iceberg(&self) -> IcebergResult<Self> {
        let namespace = vec!["default"];
        let table_name = self.metadata.name.clone();
        let arrow_schema = self.metadata.schema.as_ref();
        let catalog = create_catalog(&self.warehouse_uri)?;
        let iceberg_table = get_or_create_iceberg_table(
            &*catalog,
            &self.warehouse_uri,
            &namespace,
            &table_name,
            arrow_schema,
        )
        .await?;

        let table_metadata = iceberg_table.metadata();
        let snapshot_meta = table_metadata.current_snapshot().unwrap();
        let file_io = iceberg_table.file_io();
        let manifest_list = snapshot_meta
            .load_manifest_list(file_io, table_metadata)
            .await?;
        let mut snapshot = Snapshot::new(self.metadata.clone());

        // Maps from data filepath to batch deletion vector loaded from puffin blob.
        let mut disk_file_to_deletion_vector: HashMap<PathBuf, BatchDeletionVector> =
            HashMap::new();
        for manifest_file in manifest_list.entries().iter() {
            // We're only interested in DATA files when recover snapshot status.
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let manifest = manifest_file.load_manifest(file_io).await?;

            // All files (i.e. data files, deletion vector, manifest files) under the same snapshot are assigned with the same sequence number.
            // Reference: https://iceberg.apache.org/spec/?h=content#sequence-numbers
            let snapshot_seq_no = manifest.entries().first().unwrap().sequence_number();

            for entry in manifest.entries() {
                // Sanity check all manifest entries are of the sequence number.
                let cur_entry_seq_no = entry.sequence_number();
                if snapshot_seq_no != cur_entry_seq_no {
                    return Err(IcebergError::new(
                        iceberg::ErrorKind::DataInvalid,
                        format!("When reading from iceberg table, snapshot sequence id inconsistency found {:?} vs {:?}", snapshot_seq_no, cur_entry_seq_no),
                    ));
                }

                let data_file = entry.data_file();
                let file_path = PathBuf::from(data_file.file_path().to_string());

                // Record batch deletion.
                if data_file.file_format() == DataFileFormat::Puffin {
                    validate_puffin_manifest_entry(entry)?;
                    let deletion_vector =
                        DeletionVector::load_from_dv_blob(file_io.clone(), data_file).await?;
                    let batch_deletion_vector = deletion_vector.take_as_batch_delete_vector();
                    let referenced_path_buf: PathBuf =
                        data_file.referenced_data_file().unwrap().into();
                    disk_file_to_deletion_vector.insert(referenced_path_buf, batch_deletion_vector);
                    continue;
                }

                // Handle data files.
                assert_eq!(
                    data_file.file_format(),
                    DataFileFormat::Parquet,
                    "Data file is of file format parquet."
                );
                disk_file_to_deletion_vector
                    .entry(file_path)
                    .or_insert_with(|| BatchDeletionVector::new(/*max_rows=*/ 0));
            }
        }

        snapshot.disk_files = disk_file_to_deletion_vector;

        Ok(snapshot)
    }

    fn _load_from_iceberg(&self) -> IcebergResult<Self> {
        block_on(self._async_load_from_iceberg())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::Identity;

    use crate::storage::iceberg::test_utils;
    use crate::storage::{
        iceberg::catalog_utils::create_catalog,
        mooncake_table::{Snapshot, TableConfig, TableMetadata},
    };

    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::tempdir;

    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use iceberg::io::FileRead;
    use parquet::arrow::ArrowWriter;

    /// Create test batch deletion vector.
    fn create_test_batch_deletion_vector() -> BatchDeletionVector {
        let mut deletion_vector = BatchDeletionVector::new(/*max_rows=*/ 3);
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

    /// Test snapshot store and load for different types of catalogs based on the given warehouse.
    ///
    /// * `deletion_vector_supported` - whether the given catalog supports deletion vector; if false, skip checking deletion vector load.
    async fn test_store_and_load_snapshot_impl(
        catalog: Box<dyn Catalog>,
        warehouse_uri: &str,
    ) -> IcebergResult<()> {
        // Create arrow schema and table.
        let arrow_schema = create_test_arrow_schema();
        let tmp_dir = tempdir()?;
        let metadata = Arc::new(TableMetadata {
            name: "test_table".to_string(),
            schema: arrow_schema.clone(),
            id: 0, // unused.
            config: TableConfig::new(),
            path: tmp_dir.path().to_path_buf(),
            identity: Identity::Keys(vec![0]),
        });

        // Write first snapshot to iceberg table (with deletion vector).
        {
            let batch = test_batch_1(arrow_schema.clone());
            let mut disk_files = HashMap::new();
            let parquet_path = tmp_dir.path().join("data-1.parquet");
            write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch)
                .await?;
            disk_files.insert(parquet_path.clone(), create_test_batch_deletion_vector());

            let mut snapshot = Snapshot::new(metadata.clone());
            snapshot._set_warehouse_info(warehouse_uri.to_string());
            snapshot.disk_files = disk_files;
            snapshot._export_to_iceberg().await?;
        }

        // Write second snapshot to iceberg table (without deletion vector)
        {
            let batch = test_batch_2(arrow_schema.clone());
            let mut disk_files = HashMap::new();
            let parquet_path = tmp_dir.path().join("data-2.parquet");
            write_arrow_record_batch_to_local(parquet_path.as_path(), arrow_schema.clone(), &batch)
                .await?;
            disk_files.insert(
                parquet_path.clone(),
                BatchDeletionVector::new(/*max_rows=*/ 0),
            );

            let mut snapshot = Snapshot::new(metadata.clone());
            snapshot._set_warehouse_info(warehouse_uri.to_string());
            snapshot.disk_files = disk_files;
            snapshot._export_to_iceberg().await?;
        }

        // Load snapshot from iceberg table and check loaded content.
        let mut snapshot = Snapshot::new(metadata.clone());
        snapshot._set_warehouse_info(warehouse_uri.to_string());
        let loaded_snapshot = snapshot._async_load_from_iceberg().await?;
        assert_eq!(
            loaded_snapshot.disk_files.len(),
            2,
            "Should have exactly two file, but disk files have {:?}.",
            loaded_snapshot.disk_files.keys()
        );

        let namespace = vec!["default"];
        let table_name = "test_table";
        let iceberg_table = get_or_create_iceberg_table(
            &*catalog,
            warehouse_uri,
            &namespace,
            table_name,
            &arrow_schema,
        )
        .await?;
        let file_io = iceberg_table.file_io();

        // Check the loaded data file is of the expected format and content.
        for (loaded_path, loaded_deletion_vector) in loaded_snapshot.disk_files.iter() {
            let input_file = file_io.new_input(loaded_path.to_str().unwrap())?;
            let input_file_metadata = input_file.metadata().await?;
            let reader = input_file.reader().await?;
            let bytes = reader.read(0..input_file_metadata.size).await?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
            let mut reader = builder.build()?;
            let batch = reader.next().transpose()?.expect("Should have one batch");

            // Two data files are written here, one w/ deletion vector, another w/o.
            let deleted_rows = loaded_deletion_vector.collect_deleted_rows();

            // Check the one w/o deletion vector.
            if deleted_rows.is_empty() {
                let actual_ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(
                    *actual_ids,
                    Int32Array::from(vec![4, 5, 6]),
                    "ID data should match, actual batch is {:?}",
                    *actual_ids,
                );

                let actual_strings = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert_eq!(
                    *actual_strings,
                    StringArray::from(vec!["d", "e", "f"]),
                    "String data should match, actual batch is {:?}",
                    *actual_strings,
                );
                continue;
            }

            // Check the one w/ deletion vector.
            let actual_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(
                *actual_ids,
                Int32Array::from(vec![1, 2, 3]),
                "ID data should match"
            );
            let actual_strings = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                *actual_strings,
                StringArray::from(vec!["a", "b", "c"]),
                "String data should match, actual batch is {:?}",
                *actual_strings,
            );

            // Check loaded deletion vector.
            let collect_deleted_rows = loaded_deletion_vector.collect_deleted_rows();
            assert_eq!(
                collect_deleted_rows,
                create_test_batch_deletion_vector().collect_deleted_rows(),
                "Loaded deletion vector is not the same as the one gets stored."
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_warehouse_uri() -> IcebergResult<()> {
        // Create arrow schema and table.
        let arrow_schema = create_test_arrow_schema();
        let tmp_dir = tempdir()?;
        let metadata = Arc::new(TableMetadata {
            name: "test_table".to_string(),
            schema: arrow_schema.clone(),
            id: 0, // unused.
            config: TableConfig::new(),
            path: tmp_dir.path().to_path_buf(),
            identity: Identity::Keys(vec![0]),
        });
        let mut snapshot = Snapshot::new(metadata.clone());
        snapshot._set_warehouse_info("invalid_warehouse_uri".to_string());
        let res = snapshot._export_to_iceberg().await;
        assert!(
            res.is_err(),
            "Snapshot with invalid warehouse should fail when store."
        );
        let err = res.unwrap_err();
        assert_eq!(err.kind(), iceberg::ErrorKind::Unexpected);
        Ok(())
    }

    #[tokio::test]
    async fn test_store_and_load_snapshot_with_filesystem_catalog() -> IcebergResult<()> {
        let tmp_dir = tempdir()?;
        let warehouse_path = tmp_dir.path().to_str().unwrap();
        let catalog = create_catalog(warehouse_path)?;
        test_store_and_load_snapshot_impl(catalog, warehouse_path).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_store_and_load_snapshot_with_minio_catalog() -> IcebergResult<()> {
        let (bucket_name, warehouse_uri) =
            crate::storage::iceberg::test_utils::get_test_minio_bucket_and_warehouse();
        test_utils::object_store_test_utils::create_test_s3_bucket(bucket_name.clone()).await?;

        let catalog = create_catalog(&warehouse_uri)?;
        test_store_and_load_snapshot_impl(catalog, &warehouse_uri).await?;
        Ok(())
    }
}
