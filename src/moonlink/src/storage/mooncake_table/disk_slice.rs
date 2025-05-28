use super::data_batches::BatchEntry;
use crate::error::{Error, Result};
use crate::storage::index::persisted_bucket_hash_map::GlobalIndexBuilder;
use crate::storage::index::{FileIndex, MemIndex};
use crate::storage::storage_utils::{
    create_data_file, get_random_file_name_in_dir, get_unique_file_id_for_flush,
    MooncakeDataFileRef, ProcessedDeletionRecord, RecordLocation,
};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::AsyncArrowWriter;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) struct DiskSliceWriter {
    /// The schema of the DiskSlice.
    ///
    schema: Arc<Schema>,

    dir_path: PathBuf,

    // input
    batches: Vec<BatchEntry>,

    writer_lsn: Option<u64>,

    old_index: Arc<MemIndex>,

    table_auto_incr_id: u32,

    // a mapping of old record locations to new record locations
    // this is used to remap deletions on the disk slice
    batch_id_to_idx: HashMap<u64, usize>,
    row_offset_mapping: Vec<Vec<Option<(usize, usize)>>>,

    new_index: Option<FileIndex>,

    /// Records already flushed data files.
    files: Vec<(MooncakeDataFileRef, usize /* row count */)>,
}

impl DiskSliceWriter {
    #[cfg(debug_assertions)]
    const PARQUET_FILE_SIZE: usize = 1024 * 1024 * 2; // 2MB

    #[cfg(not(debug_assertions))]
    const PARQUET_FILE_SIZE: usize = 1024 * 1024 * 128; // 128MB

    pub(super) fn new(
        schema: Arc<Schema>,
        dir_path: PathBuf,
        batches: Vec<BatchEntry>,
        writer_lsn: Option<u64>,
        table_auto_incr_id: u32,
        old_index: Arc<MemIndex>,
    ) -> Self {
        Self {
            schema,
            dir_path,
            batches,
            files: vec![],
            batch_id_to_idx: HashMap::new(),
            writer_lsn,
            table_auto_incr_id,
            row_offset_mapping: vec![],
            old_index,
            new_index: None,
        }
    }

    /// Apply deletion vector to in-memory batches, write to parquet files and remap index.
    pub(super) async fn write(&mut self) -> Result<()> {
        let mut filtered_batches = Vec::new();
        let mut id = 0;
        for entry in self.batches.iter() {
            let filtered_batch = entry.batch.get_filtered_batch()?;
            if let Some(batch) = filtered_batch {
                let total_rows = entry.batch.data.as_ref().unwrap().num_rows();
                filtered_batches.push((
                    id,
                    batch,
                    entry.batch.deletions.collect_active_rows(total_rows),
                ));
                let mut mapping = Vec::with_capacity(total_rows);
                mapping.resize(total_rows, None);
                self.row_offset_mapping.push(mapping);
                self.batch_id_to_idx.insert(entry.id, id);
                id += 1;
            }
        }
        self.write_batch_to_parquet(&filtered_batches).await?;
        self.remap_index().await?;
        Ok(())
    }

    pub(super) fn lsn(&self) -> Option<u64> {
        self.writer_lsn
    }

    pub(super) fn input_batches(&self) -> &Vec<BatchEntry> {
        &self.batches
    }
    /// Get the list of files in the DiskSlice
    pub(super) fn output_files(&self) -> &[(MooncakeDataFileRef, usize)] {
        self.files.as_slice()
    }

    pub(super) fn old_index(&self) -> &Arc<MemIndex> {
        &self.old_index
    }

    /// Write record batches to parquet files in synchronous mode.
    /// TODO(hjiang): Parallelize the parquet file write operations.
    async fn write_batch_to_parquet(
        &mut self,
        record_batches: &Vec<(usize, RecordBatch, Vec<usize>)>,
    ) -> Result<()> {
        let mut files = Vec::new();
        let mut writer = None;
        let mut out_file_idx = 0;
        let mut out_row_idx = 0;
        let dir_path = &self.dir_path;
        let mut data_file = None;
        for (batch_id, batch, row_indices) in record_batches {
            if writer.is_none() {
                // Generate a unique file name
                // Create the file
                out_file_idx = files.len();
                let file_id = get_unique_file_id_for_flush(
                    self.table_auto_incr_id as u64,
                    out_file_idx as u64,
                );
                let file_path = get_random_file_name_in_dir(dir_path);
                data_file = Some(create_data_file(file_id, file_path));
                let file =
                    tokio::fs::File::create(dir_path.join(data_file.as_ref().unwrap().file_path()))
                        .await
                        .map_err(Error::Io)?;
                writer = Some(AsyncArrowWriter::try_new(file, self.schema.clone(), None)?);
                out_row_idx = 0;
            }
            for row_idx in row_indices {
                self.row_offset_mapping[*batch_id][*row_idx] = Some((out_file_idx, out_row_idx));
                out_row_idx += 1;
            }
            // Write the batch
            writer.as_mut().unwrap().write(batch).await?;
            if writer.as_ref().unwrap().memory_size() > Self::PARQUET_FILE_SIZE {
                // Finalize the writer
                writer.unwrap().close().await?;
                writer = None;
                files.push((data_file.unwrap(), out_row_idx));
                data_file = None;
            }
        }
        if let Some(writer) = writer {
            writer.close().await?;
            files.push((data_file.unwrap(), out_row_idx));
        }
        self.files = files;
        Ok(())
    }

    async fn remap_index(&mut self) -> Result<()> {
        if self.old_index().is_empty() {
            return Ok(());
        }
        let list = self
            .old_index
            .remap_into_vec(&self.batch_id_to_idx, &self.row_offset_mapping);

        let mut index_builder = GlobalIndexBuilder::new();
        index_builder.set_files(self.files.iter().map(|(file, _)| file.clone()).collect());
        index_builder.set_directory(self.dir_path.clone());
        self.new_index = Some(index_builder.build_from_flush(list).await);
        Ok(())
    }

    pub fn take_index(&mut self) -> Option<FileIndex> {
        self.new_index.take()
    }

    pub fn remap_deletion_if_needed(&self, deletion: &mut ProcessedDeletionRecord) {
        if let RecordLocation::MemoryBatch(batch_id, row_idx) = &deletion.pos {
            let batch_was_flushed = self.batch_id_to_idx.contains_key(batch_id);
            if batch_was_flushed {
                let old_location = (*self.batch_id_to_idx.get(batch_id).unwrap(), *row_idx);
                let new_location = self.row_offset_mapping[old_location.0][old_location.1];
                if let Some(new_location) = new_location {
                    deletion.pos = RecordLocation::DiskFile(
                        self.files[new_location.0].0.file_id(),
                        new_location.1,
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::{IdentityProp, MoonlinkRow, RowValue};
    use crate::storage::mooncake_table::mem_slice::MemSlice;
    use crate::storage::storage_utils::RawDeletionRecord;
    use arrow::datatypes::{DataType, Field};
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::Schema;
    use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
    use tempfile::tempdir;

    /// Util function to create test schema.
    fn get_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "2".to_string(),
            )])),
        ]))
    }

    #[tokio::test]
    async fn test_disk_slice_builder() -> Result<()> {
        // Create a temporary directory for the test
        let temp_dir = tempdir().map_err(Error::Io)?;
        // Create a schema for testing
        let schema = get_test_schema();

        let identity = IdentityProp::SinglePrimitiveKey(0);
        // Create a MemSlice with test data
        let mut mem_slice = MemSlice::new(schema.clone(), 100, identity);

        // Add some test rows
        let row1 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("Alice".as_bytes().to_vec()),
        ]);
        let row2 = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::ByteArray("Bob".as_bytes().to_vec()),
        ]);

        mem_slice.append(1, row1, None)?;
        mem_slice.append(2, row2, None)?;
        let (_new_batch, entries, old_index) = mem_slice.drain().unwrap();

        let mut disk_slice = DiskSliceWriter::new(
            schema.clone(),
            temp_dir.path().to_path_buf(),
            entries,
            Some(1),
            /*table_auto_incr_id=*/ 0,
            Arc::new(old_index),
        );
        disk_slice.write().await?;

        // Verify files were created
        assert!(!disk_slice.output_files().is_empty());

        // Read the files and verify the data
        for (file, _rows) in disk_slice.output_files() {
            let file_path = temp_dir.path().join(file.file_path());
            let file = tokio::fs::File::open(file_path).await?;
            let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
            let actual_schema = builder.schema();
            assert_eq!(*actual_schema, schema);

            let mut reader = builder.build().unwrap();
            let mut record_batch_reader = reader.next_row_group().await.unwrap().unwrap();
            let record_batch = record_batch_reader.next().unwrap().unwrap();
            let expected_record_batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                ],
            )
            .unwrap();
            assert_eq!(record_batch, expected_record_batch);
        }
        // Clean up temporary directory
        temp_dir.close().map_err(Error::Io)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_index_remapping() -> Result<()> {
        // Create a temporary directory for the test
        let temp_dir = tempdir().map_err(Error::Io)?;

        // Create a schema for testing
        let schema = get_test_schema();

        let identity = IdentityProp::SinglePrimitiveKey(0);

        // Create a MemSlice with test data - more rows this time
        let mut mem_slice = MemSlice::new(schema.clone(), 3, identity);

        // Add several test rows
        let rows = [
            MoonlinkRow::new(vec![
                RowValue::Int32(1),
                RowValue::ByteArray("Alice".as_bytes().to_vec()),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(2),
                RowValue::ByteArray("Bob".as_bytes().to_vec()),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(3),
                RowValue::ByteArray("Charlie".as_bytes().to_vec()),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(4),
                RowValue::ByteArray("David".as_bytes().to_vec()),
            ]),
            MoonlinkRow::new(vec![
                RowValue::Int32(5),
                RowValue::ByteArray("Eve".as_bytes().to_vec()),
            ]),
        ];

        // Insert original keys into the index
        for row in rows.into_iter() {
            let key = match row.values[0] {
                RowValue::Int32(v) => v as u64,
                _ => panic!("Expected i32"),
            };
            mem_slice.append(key, row, None)?;
        }

        // Delete a couple of rows to test that only active rows are mapped
        mem_slice
            .delete(
                &RawDeletionRecord {
                    lookup_key: 2,
                    row_identity: None,
                    pos: Some((0, 1)),
                    lsn: 1,
                },
                &IdentityProp::SinglePrimitiveKey(0),
            )
            .await; // Delete Bob (ID 2)
        mem_slice
            .delete(
                &RawDeletionRecord {
                    lookup_key: 4,
                    row_identity: None,
                    pos: Some((0, 3)),
                    lsn: 1,
                },
                &IdentityProp::SinglePrimitiveKey(0),
            )
            .await; // Delete David (ID 4)

        let (_new_batch, entries, index) = mem_slice.drain().unwrap();

        let mut disk_slice = DiskSliceWriter::new(
            schema,
            temp_dir.path().to_path_buf(),
            entries,
            Some(1),
            0,
            Arc::new(index),
        );

        // Write the disk slice
        disk_slice.write().await?;

        // Verify files were created
        assert!(!disk_slice.output_files().is_empty());

        // Get the remapped index and verify it
        let new_index = disk_slice.take_index().unwrap();

        // Verify each key has been remapped to a disk location
        for key in [1, 3, 5] {
            // These should exist (undeleted rows)
            let locations = new_index.search(&key).await;
            assert!(
                !locations.is_empty(),
                "Key {key} should exist in the remapped index"
            );

            for location in locations {
                match location {
                    RecordLocation::DiskFile(file_id, _) => {
                        // Verify the file exists in our output files
                        assert!(
                            disk_slice
                                .output_files()
                                .iter()
                                .any(|(file, _)| file.file_id() == file_id),
                            "Referenced file path should exist in output files"
                        );
                    }
                    _ => panic!("Expected DiskFile location, found: {:?}", location),
                }
            }
        }

        // Check that deleted rows are not in the index
        for key in [2, 4] {
            // These should not exist (deleted rows)
            let locations = new_index.search(&key).await;
            assert!(
                locations.is_empty(),
                "Deleted key {key} should not exist in the remapped index"
            );
        }

        // Clean up temporary directory
        temp_dir.close().map_err(Error::Io)?;

        Ok(())
    }
}
