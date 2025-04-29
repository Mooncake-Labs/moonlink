use super::data_batches::BatchEntry;
use crate::error::{Error, Result};
use crate::storage::index::persisted_bucket_hash_map::GlobalIndexBuilder;
use crate::storage::index::{FileIndex, MemIndex};
use crate::storage::storage_utils::{FileId, ProcessedDeletionRecord, RecordLocation};
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;
pub(crate) struct DiskSliceWriter {
    /// The schema of the DiskSlice.
    ///
    schema: Arc<Schema>,

    dir_path: PathBuf,

    // input
    batches: Vec<BatchEntry>,

    writer_lsn: u64,

    old_index: Arc<MemIndex>,

    // a mapping of old record locations to new record locations
    // this is used to remap deletions on the disk slice
    batch_id_to_idx: HashMap<u64, usize>,
    row_offset_mapping: HashMap<(usize, usize), (usize, usize)>,

    new_index: Option<FileIndex>,

    files: Vec<(PathBuf /* file path */, usize /* row count */)>,
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
        writer_lsn: u64,
        old_index: Arc<MemIndex>,
    ) -> Self {
        Self {
            schema,
            dir_path,
            batches,
            files: vec![],
            batch_id_to_idx: HashMap::new(),
            writer_lsn,
            row_offset_mapping: HashMap::new(),
            old_index,
            new_index: None,
        }
    }

    pub(super) fn write(&mut self) -> Result<()> {
        let mut filtered_batches = Vec::new();
        for (id, entry) in self.batches.iter().enumerate() {
            let filtered_batch = entry.batch.get_filtered_batch()?;
            if let Some(batch) = filtered_batch {
                filtered_batches.push((id, batch, entry.batch.deletions.collect_active_rows()));
                self.batch_id_to_idx.insert(entry.id, id);
            }
        }
        self.write_batch_to_parquet(&filtered_batches)?;
        self.remap_index()?;
        Ok(())
    }

    pub(super) fn lsn(&self) -> u64 {
        self.writer_lsn
    }

    pub(super) fn input_batches(&self) -> &Vec<BatchEntry> {
        &self.batches
    }
    /// Get the list of files in the DiskSlice
    pub(super) fn output_files(&self) -> &[(PathBuf, usize)] {
        self.files.as_slice()
    }

    pub(super) fn old_index(&self) -> &Arc<MemIndex> {
        &self.old_index
    }
    /// Write record batches to parquet files
    fn write_batch_to_parquet(
        &mut self,
        record_batches: &Vec<(usize, RecordBatch, Vec<usize>)>,
    ) -> Result<()> {
        let mut files = Vec::new();
        let mut writer = None;
        let mut out_file_idx = 0;
        let mut out_row_idx = 0;
        let dir_path = &self.dir_path;
        let mut file_path = None;
        for (batch_id, batch, row_indices) in record_batches {
            if writer.is_none() {
                // Generate a unique file name
                // Create the file
                let file_name = format!("{}.parquet", Uuid::new_v4());
                file_path = Some(dir_path.join(file_name));
                let file = File::create(file_path.as_ref().unwrap()).map_err(Error::Io)?;
                out_file_idx = files.len();
                writer = Some(ArrowWriter::try_new(file, self.schema.clone(), None)?);
                out_row_idx = 0;
            }
            for row_idx in row_indices {
                self.row_offset_mapping
                    .insert((*batch_id, *row_idx), (out_file_idx, out_row_idx));
                out_row_idx += 1;
            }
            // Write the batch
            writer.as_mut().unwrap().write(batch)?;
            if writer.as_ref().unwrap().memory_size() > Self::PARQUET_FILE_SIZE {
                // Finalize the writer
                writer.unwrap().close()?;
                writer = None;
                files.push((file_path.unwrap(), out_row_idx));
                file_path = None;
            }
        }
        if let Some(writer) = writer {
            writer.close()?;
            files.push((file_path.unwrap(), out_row_idx));
        }
        self.files = files;
        Ok(())
    }

    fn remap_index(&mut self) -> Result<()> {
        let list = self
            .old_index
            .iter()
            .filter_map(|(key, value)| {
                let RecordLocation::MemoryBatch(batch_id, row_idx) = value else {
                    panic!("Invalid record location");
                };
                let old_location = (*self.batch_id_to_idx.get(batch_id).unwrap(), *row_idx);
                let new_location = self.row_offset_mapping.get(&old_location);
                if let Some(new_location) = new_location {
                    Some((*key, new_location.0, new_location.1))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let mut index_builder = GlobalIndexBuilder::new();
        index_builder.set_files(
            self.files
                .iter()
                .map(|(path, _)| Arc::new(path.clone()))
                .collect(),
        );
        index_builder.set_directory(self.dir_path.clone());
        self.new_index = Some(index_builder.build_from_flush(list));
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
                // Guard the case where the record was deleted before making it to disk
                if let Some(new_location) = self.row_offset_mapping.get(&old_location) {
                    deletion.pos = RecordLocation::DiskFile(
                        FileId(Arc::new(self.files[new_location.0].0.clone())),
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
    use crate::row::{MoonlinkRow, RowValue};
    use crate::storage::mooncake_table::mem_slice::MemSlice;
    use crate::storage::storage_utils::RawDeletionRecord;
    use arrow::datatypes::{DataType, Field};
    use arrow_schema::Schema;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    #[test]
    fn test_disk_slice_builder() -> Result<()> {
        // Create a temporary directory for the test
        let temp_dir = tempdir().map_err(Error::Io)?;
        // Create a schema for testing
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create a MemSlice with test data
        let mut mem_slice = MemSlice::new(schema.clone(), 100);

        // Add some test rows
        let row1 = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::ByteArray("Alice".as_bytes().to_vec()),
        ]);
        let row2 = MoonlinkRow::new(vec![
            RowValue::Int32(2),
            RowValue::ByteArray("Bob".as_bytes().to_vec()),
        ]);

        mem_slice.append(1, &row1)?;
        mem_slice.append(2, &row2)?;
        let (_new_batch, entries, _index) = mem_slice.drain().unwrap();
        let mut old_index = MemIndex::new();
        old_index.insert(1, RecordLocation::MemoryBatch(0, 0));
        old_index.insert(2, RecordLocation::MemoryBatch(0, 1));

        let mut disk_slice = DiskSliceWriter::new(
            schema,
            temp_dir.path().to_path_buf(),
            entries,
            1,
            Arc::new(old_index),
        );
        disk_slice.write()?;
        // Verify files were created
        assert!(!disk_slice.output_files().is_empty());
        println!("Files: {:?}", disk_slice.output_files());

        // Read the files and verify the data
        for (file, _rows) in disk_slice.output_files() {
            let file = File::open(file).map_err(Error::Io)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            println!("Converted arrow schema is: {}", builder.schema());

            let mut reader = builder.build().unwrap();
            let record_batch = reader.next().unwrap().unwrap();
            println!("{:?}", record_batch);
        }
        // Clean up temporary directory
        temp_dir.close().map_err(Error::Io)?;

        Ok(())
    }

    #[test]
    fn test_index_remapping() -> Result<()> {
        // Create a temporary directory for the test
        let temp_dir = tempdir().map_err(Error::Io)?;

        // Create a schema for testing
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create a MemSlice with test data - more rows this time
        let mut mem_slice = MemSlice::new(schema.clone(), 3);

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
        for row in rows.iter() {
            let key = match row.values[0] {
                RowValue::Int32(v) => v as u64,
                _ => panic!("Expected i32"),
            };
            mem_slice.append(key, row)?;
        }

        // Delete a couple of rows to test that only active rows are mapped
        mem_slice.delete(&RawDeletionRecord {
            lookup_key: 2,
            _row_identity: None,
            pos: Some((0, 1)),
            lsn: 1,
            xact_id: None,
        }); // Delete Bob (ID 2)
        mem_slice.delete(&RawDeletionRecord {
            lookup_key: 4,
            _row_identity: None,
            pos: Some((0, 3)),
            lsn: 1,
            xact_id: None,
        }); // Delete David (ID 4)

        let (_new_batch, entries, index) = mem_slice.drain().unwrap();

        let mut disk_slice = DiskSliceWriter::new(
            schema,
            temp_dir.path().to_path_buf(),
            entries,
            1,
            Arc::new(index),
        );

        // Write the disk slice
        disk_slice.write()?;

        // Verify files were created
        assert!(!disk_slice.output_files().is_empty());
        println!("Files created: {:?}", disk_slice.output_files());

        // Get the remapped index and verify it
        let new_index = disk_slice.take_index().unwrap();

        // Verify each key has been remapped to a disk location
        for key in [1, 3, 5] {
            // These should exist (undeleted rows)
            let locations = new_index.search(&key);
            assert!(
                !locations.is_empty(),
                "Key {key} should exist in the remapped index"
            );

            for location in locations {
                match location {
                    RecordLocation::DiskFile(file_id, _) => {
                        // Verify the file exists in our output files
                        let file_path = &file_id.0;
                        assert!(
                            disk_slice
                                .output_files()
                                .iter()
                                .any(|(path, _)| path == file_path.as_ref()),
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
            let locations = new_index.search(&key);
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
