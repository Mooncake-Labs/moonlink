// Compaction struct for data files, which takes a number of data files, compact them into one or more final data files, and one single file indices.
// Deletion vectors, which correspond to data files to compact, will be applied inline.

use std::collections::{HashMap, HashSet};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use more_asserts as ma;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::AsyncArrowWriter;

use crate::storage::compaction::table_compaction::{
    CompactedDataEntry, DataCompactionPayload, DataCompactionResult, RemappedRecordLocation,
    SingleFileToCompact,
};
use crate::storage::iceberg::puffin_utils;
use crate::storage::index::persisted_bucket_hash_map::GlobalIndexBuilder;
use crate::storage::index::FileIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::storage_utils::{
    get_random_file_name_in_dir, get_unique_file_id_for_flush, MooncakeDataFileRef,
};
use crate::storage::storage_utils::{FileId, RecordLocation};
use crate::storage::{parquet_utils, storage_utils};
use crate::{create_data_file, Result};

type DataFileRemap = HashMap<RecordLocation, RemappedRecordLocation>;

pub(crate) struct CompactionFileParams {
    /// Local directory to place compacted data files.
    pub(crate) dir_path: std::path::PathBuf,
    /// Used to generate unique file id.
    pub(crate) table_auto_incr_ids: std::ops::Range<u32>,
    /// Final size for compacted data files.
    pub(crate) data_file_final_size: u64,
}

pub(crate) struct CompactionBuilder {
    /// Compaction payload.
    compaction_payload: DataCompactionPayload,
    /// Table schema.
    schema: SchemaRef,
    /// File related parameters for compaction usage.
    file_params: CompactionFileParams,
    /// New data files after compaction.
    new_data_files: Vec<(MooncakeDataFileRef, CompactedDataEntry)>,
    /// ===== Current ongoing compaction operation =====
    ///
    /// Current active async arrow writer, which is initialized in a lazy style.
    cur_arrow_writer: Option<AsyncArrowWriter<tokio::fs::File>>,
    /// Current new data file.
    cur_new_data_file: Option<MooncakeDataFileRef>,
    /// Current row number for the new compaction file.
    cur_row_num: usize,
    /// Current compacted file count, including new compacted data files and index block files.
    compacted_file_count: u64,
}

/// Result for data file compaction.
#[derive(Clone, Debug)]
struct DataFileCompactionResult {
    /// Mapping from record locations before compaction to record locations after compaction.
    data_file_remap: DataFileRemap,
    /// Cache evicted files to delete.
    evicted_files_to_delete: Vec<String>,
}

impl DataFileCompactionResult {
    fn into_parts(self) -> (DataFileRemap, Vec<String>) {
        (self.data_file_remap, self.evicted_files_to_delete)
    }
}

impl CompactionBuilder {
    pub(crate) fn new(
        compaction_payload: DataCompactionPayload,
        schema: SchemaRef,
        file_params: CompactionFileParams,
    ) -> Self {
        Self {
            compaction_payload,
            schema,
            file_params,
            new_data_files: Vec::new(),
            // Current ongoing compaction operation
            cur_arrow_writer: None,
            cur_new_data_file: None,
            cur_row_num: 0,
            compacted_file_count: 0,
        }
    }

    /// Util function to get the next file id.
    fn get_next_file_id(&self) -> u64 {
        let unique_table_auto_incre_id_offset =
            self.compacted_file_count / storage_utils::NUM_FILES_PER_FLUSH;
        let cur_table_auto_incr_id =
            self.file_params.table_auto_incr_ids.start as u64 + unique_table_auto_incre_id_offset;
        assert!(self
            .file_params
            .table_auto_incr_ids
            .contains(&(cur_table_auto_incr_id as u32)));
        let cur_file_idx = self.compacted_file_count
            - storage_utils::NUM_FILES_PER_FLUSH * unique_table_auto_incre_id_offset;
        get_unique_file_id_for_flush(cur_table_auto_incr_id, cur_file_idx)
    }

    /// Util function to get the index of file after compaction, by the given [`file_id`].
    fn get_file_index_after_compaction(start_table_auto_incr_id: u64, file_id: FileId) -> usize {
        let compacted_file_idx = (file_id.0
            - storage_utils::NUM_FILES_PER_FLUSH * start_table_auto_incr_id)
            % storage_utils::NUM_FILES_PER_FLUSH;
        compacted_file_idx as usize
    }

    /// Util function to create a new data file.
    fn create_new_data_file(&self) -> MooncakeDataFileRef {
        assert!(self.cur_new_data_file.is_none());
        let next_file_id = self.get_next_file_id();
        let file_path = get_random_file_name_in_dir(self.file_params.dir_path.as_path());
        create_data_file(next_file_id, file_path)
    }

    /// Initialize arrow writer for once.
    async fn initialize_arrow_writer_if_not(&mut self) -> Result<()> {
        // If we create multiple data files during compaction, simply increment file id and recreate a new one.
        if self.cur_arrow_writer.is_some() {
            assert!(self.cur_new_data_file.is_some());
            return Ok(());
        }

        self.cur_new_data_file = Some(self.create_new_data_file());
        let write_file =
            tokio::fs::File::create(self.cur_new_data_file.as_ref().unwrap().file_path()).await?;
        let properties = parquet_utils::get_parquet_properties_for_compaction();
        let writer: AsyncArrowWriter<tokio::fs::File> =
            AsyncArrowWriter::try_new(write_file, self.schema.clone(), Some(properties))?;
        self.cur_arrow_writer = Some(writer);

        Ok(())
    }

    /// Util function to flush current arrow write and re-initialize related states.
    async fn flush_arrow_writer(&mut self) -> Result<()> {
        self.cur_arrow_writer.as_mut().unwrap().finish().await?;
        let file_size = self.cur_arrow_writer.as_ref().unwrap().bytes_written();
        ma::assert_gt!(file_size, 0);
        ma::assert_gt!(self.cur_row_num, 0);
        let compacted_data_entry = CompactedDataEntry {
            num_rows: self.cur_row_num,
            file_size,
        };
        let new_data_file = std::mem::take(&mut self.cur_new_data_file).unwrap();
        self.new_data_files
            .push((new_data_file, compacted_data_entry));

        // Reinitialize states related to current new compacted data file.
        self.cur_arrow_writer = None;
        self.cur_new_data_file = None;
        self.cur_row_num = 0;
        self.compacted_file_count += 1;

        Ok(())
    }

    /// Util function to read the given parquet file, apply the corresponding deletion vector, and write it to the given arrow writer.
    /// Return the data file mapping, and cache evicted data files to delete.
    #[tracing::instrument(name = "apply_deletion_vec", skip_all)]
    async fn apply_deletion_vector_and_write(
        &mut self,
        data_file_to_compact: SingleFileToCompact,
    ) -> Result<DataFileCompactionResult> {
        // Aggregate evicted files to delete.
        let mut evicted_files_to_delete = vec![];

        let (cache_handle, evicted_files) = self
            .compaction_payload
            .object_storage_cache
            .get_cache_entry(
                data_file_to_compact.file_id,
                &data_file_to_compact.filepath,
                self.compaction_payload.filesystem_accessor.as_ref(),
            )
            .await?;
        evicted_files_to_delete.extend(evicted_files);

        let filepath = if let Some(cache_handle) = &cache_handle {
            cache_handle.get_cache_filepath()
        } else {
            &data_file_to_compact.filepath
        };

        let file = tokio::fs::File::open(filepath).await?;
        let builder = ParquetRecordBatchStreamBuilder::new(file).await?;
        let total_num_rows: usize = builder
            .metadata()
            .row_groups()
            .iter()
            .map(|cur_row_group| cur_row_group.num_rows() as usize)
            .sum();
        let mut reader = builder.build().unwrap();

        let batch_deletion_vector =
            if let Some(puffin_blob_ref) = data_file_to_compact.deletion_vector {
                puffin_utils::load_deletion_vector_from_blob(&puffin_blob_ref).await?
            } else {
                BatchDeletionVector::new(/*max_rows=*/ 0)
            };
        let deleted_rows_num = batch_deletion_vector.get_num_rows_deleted();

        let get_filtered_record_batch = |record_batch: RecordBatch, start_row_idx: usize| {
            if batch_deletion_vector.is_empty() {
                return record_batch;
            }
            batch_deletion_vector
                .apply_to_batch_with_slice(&record_batch, start_row_idx)
                .unwrap()
        };

        let mut old_start_row_idx = 0;
        let mut old_to_new_remap = HashMap::new();
        while let Some(cur_record_batch) = reader.try_next().await? {
            // If all rows have been deleted for the old data file, do nothing.
            let cur_num_rows = cur_record_batch.num_rows();
            let filtered_record_batch =
                get_filtered_record_batch(cur_record_batch, old_start_row_idx);
            if filtered_record_batch.num_rows() == 0 {
                old_start_row_idx += cur_num_rows;
                continue;
            }

            self.initialize_arrow_writer_if_not().await?;
            self.cur_arrow_writer
                .as_mut()
                .unwrap()
                .write(&filtered_record_batch)
                .await?;

            // Construct old data file to new one mapping on-the-fly.
            old_to_new_remap.reserve(old_to_new_remap.len() + cur_num_rows);

            for old_row_idx in old_start_row_idx..(old_start_row_idx + cur_num_rows) {
                if batch_deletion_vector.is_deleted(old_row_idx) {
                    continue;
                }
                let old_record_location =
                    RecordLocation::DiskFile(data_file_to_compact.file_id.file_id, old_row_idx);
                let new_record_location = RecordLocation::DiskFile(
                    self.cur_new_data_file.as_ref().unwrap().file_id(),
                    self.cur_row_num,
                );
                // Precondition: data files are compacted before file indices, so [`self.compacted_file_count`] indicates the index of already compacted data files.
                let remapped_record_location = RemappedRecordLocation {
                    record_location: new_record_location,
                    new_data_file: self.cur_new_data_file.as_ref().unwrap().clone(),
                };
                let old_entry =
                    old_to_new_remap.insert(old_record_location, remapped_record_location);
                assert!(old_entry.is_none());
                self.cur_row_num += 1;
            }

            old_start_row_idx += cur_num_rows;
        }

        // Bytes to write already reached target compacted data file size, flush and close.
        if self.cur_arrow_writer.is_some()
            && self.cur_arrow_writer.as_ref().unwrap().memory_size()
                >= self.file_params.data_file_final_size as usize
        {
            self.flush_arrow_writer().await?;
        }

        // Unpin cache handle after usage, if necessary.
        // TODO(hjiang): Better error propagation, cache handle should be always unpinned whether success or failure.
        if let Some(mut cache_handle) = cache_handle {
            let evicted_files = cache_handle.unreference().await;
            evicted_files_to_delete.extend(evicted_files);
        }

        // Sanity check on compaction result.
        let expected_compacted_num_rows = total_num_rows - deleted_rows_num;
        let actual_compacted_num_rows = old_to_new_remap.len();
        assert_eq!(expected_compacted_num_rows, actual_compacted_num_rows);

        let data_file_compaction_result = DataFileCompactionResult {
            data_file_remap: old_to_new_remap,
            evicted_files_to_delete,
        };

        Ok(data_file_compaction_result)
    }

    /// Util function to compact the given data files, with their corresponding deletion vector applied.
    #[tracing::instrument(name = "compact_data_files", skip_all)]
    async fn compact_data_files(&mut self) -> Result<DataFileCompactionResult> {
        let mut old_to_new_remap = HashMap::new();

        let disk_files = std::mem::take(&mut self.compaction_payload.disk_files);
        let mut evicted_files_to_delete = vec![];
        for single_file_to_compact in disk_files.into_iter() {
            let data_file_compaction_result = self
                .apply_deletion_vector_and_write(single_file_to_compact)
                .await?;
            evicted_files_to_delete.extend(data_file_compaction_result.evicted_files_to_delete);
            old_to_new_remap.extend(data_file_compaction_result.data_file_remap);
        }

        let data_file_compaction_result = DataFileCompactionResult {
            data_file_remap: old_to_new_remap,
            evicted_files_to_delete,
        };
        Ok(data_file_compaction_result)
    }

    /// Util function to get new compacted data files **IN ORDER**.
    fn get_new_compacted_data_files(&self) -> Vec<MooncakeDataFileRef> {
        let mut prev_file_id: u64 = 0;
        let mut new_data_files = Vec::with_capacity(self.new_data_files.len());
        for (cur_new_data_file, _) in self.new_data_files.iter() {
            ma::assert_lt!(prev_file_id, cur_new_data_file.file_id().0);
            prev_file_id = cur_new_data_file.file_id().0;

            new_data_files.push(cur_new_data_file.clone());
        }
        new_data_files
    }

    /// Util function to merge all given file indices into one.
    async fn compact_file_indices(
        &mut self,
        old_file_indices: Vec<FileIndex>,
        old_to_new_remap: &HashMap<RecordLocation, RemappedRecordLocation>,
    ) -> FileIndex {
        let get_remapped_record_location =
            |old_record_location: RecordLocation| -> Option<RecordLocation> {
                if let Some(remapped_record_location) = old_to_new_remap.get(&old_record_location) {
                    return Some(remapped_record_location.record_location.clone());
                }
                None
            };

        let start_table_auto_incr_id = self.file_params.table_auto_incr_ids.start as u64;
        let get_seg_idx = |new_record_location: RecordLocation| -> usize /*seg_idx*/ {
            let file_id = new_record_location.get_file_id().unwrap();
            Self::get_file_index_after_compaction(start_table_auto_incr_id, file_id)
        };

        let file_id_for_index_file = self.get_next_file_id();
        self.compacted_file_count += 1;

        let mut global_index_builder = GlobalIndexBuilder::new();
        global_index_builder.set_directory(self.file_params.dir_path.clone());
        global_index_builder
            .build_from_merge_for_compaction(
                /*num_rows=*/ old_to_new_remap.len() as u32,
                /*file_id=*/ file_id_for_index_file,
                old_file_indices,
                /*new_data_files=*/ self.get_new_compacted_data_files(),
                get_remapped_record_location,
                get_seg_idx,
            )
            .await
    }

    /// Perform a compaction operation, and get the result back.
    #[tracing::instrument(name = "compaction_build", skip_all)]
    #[allow(clippy::mutable_key_type)]
    pub(crate) async fn build(mut self) -> Result<DataCompactionResult> {
        let old_data_files = self
            .compaction_payload
            .disk_files
            .iter()
            .map(|cur_file_to_compact| {
                create_data_file(
                    cur_file_to_compact.file_id.file_id.0,
                    cur_file_to_compact.filepath.clone(),
                )
            })
            .collect::<HashSet<_>>();
        let old_file_indices = self
            .compaction_payload
            .file_indices
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let data_file_compaction_result = self.compact_data_files().await?;
        let (old_record_loc_to_new_mapping, evicted_files_to_delete) =
            data_file_compaction_result.into_parts();

        // All rows have been deleted.
        if old_record_loc_to_new_mapping.is_empty() {
            return Ok(DataCompactionResult {
                id: self.compaction_payload.id,
                uuid: self.compaction_payload.uuid,
                remapped_data_files: old_record_loc_to_new_mapping,
                old_data_files,
                old_file_indices,
                new_data_files: Vec::new(),
                new_file_indices: Vec::new(),
                evicted_files_to_delete,
            });
        }

        // Flush and close the compacted data file.
        if self.cur_arrow_writer.is_some() {
            self.flush_arrow_writer().await?;
        }

        // Perform compaction on file indices.
        let new_file_indices = self
            .compact_file_indices(
                self.compaction_payload.file_indices.clone(),
                &old_record_loc_to_new_mapping,
            )
            .await;

        Ok(DataCompactionResult {
            id: self.compaction_payload.id,
            uuid: self.compaction_payload.uuid,
            remapped_data_files: old_record_loc_to_new_mapping,
            old_data_files,
            old_file_indices,
            new_data_files: self.new_data_files,
            new_file_indices: vec![new_file_indices],
            evicted_files_to_delete,
        })
    }
}
