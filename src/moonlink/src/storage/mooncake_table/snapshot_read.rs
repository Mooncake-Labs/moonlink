use super::data_batches::create_batch_from_rows;
use crate::error::Result;
use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::mooncake_table::snapshot::SnapshotTableState;
use crate::storage::mooncake_table::snapshot_read_output::{
    DataFileForRead, ReadOutput as SnapshotReadOutput,
};
use crate::storage::mooncake_table::table_status::TableSnapshotStatus;
use crate::storage::storage_utils::RecordLocation;
use crate::storage::PuffinDeletionBlobAtRead;
use crate::NonEvictableHandle;
use arrow_schema::Schema;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

impl SnapshotTableState {
    /// =======================
    /// Read snapshot schema
    /// =======================
    ///
    pub(crate) fn get_table_schema(&self) -> Result<Arc<Schema>> {
        Ok(self.mooncake_table_metadata.schema.clone())
    }

    /// =======================
    /// Read snapshot states
    /// =======================
    ///
    pub(crate) fn get_table_snapshot_states(&self) -> Result<TableSnapshotStatus> {
        Ok(TableSnapshotStatus {
            commit_lsn: self.current_snapshot.snapshot_version,
            flush_lsn: self.current_snapshot.flush_lsn,
        })
    }

    /// =======================
    /// Read snapshot
    /// =======================
    ///
    /// Util function to get read state, which returns all current data files information.
    /// If a data file already has a pinned reference, increment the reference count directly to avoid unnecessary IO.
    async fn get_read_files_for_read(&mut self) -> Vec<DataFileForRead> {
        let mut data_files_for_read = Vec::with_capacity(self.current_snapshot.disk_files.len());
        for (file, _) in self.current_snapshot.disk_files.iter() {
            let unique_table_file_id = self.get_table_unique_file_id(file.file_id());
            data_files_for_read.push(DataFileForRead::RemoteFilePath((
                unique_table_file_id,
                file.file_path().to_string(),
            )));
        }

        data_files_for_read
    }

    /// Get committed deletion record for current snapshot.
    async fn get_deletion_records(
        &mut self,
    ) -> (
        Vec<NonEvictableHandle>,       /*puffin file cache handles*/
        Vec<PuffinDeletionBlobAtRead>, /*deletion vector puffin*/
        Vec<(
            u32, /*index of disk file in snapshot*/
            u32, /*row id*/
        )>,
    ) {
        // Get puffin blobs for deletion vector.
        let mut puffin_cache_handles = vec![];
        let mut deletion_vector_blob_at_read = vec![];
        for (idx, (_, disk_deletion_vector)) in self.current_snapshot.disk_files.iter().enumerate()
        {
            if disk_deletion_vector.puffin_deletion_blob.is_none() {
                continue;
            }
            let puffin_deletion_blob = disk_deletion_vector.puffin_deletion_blob.as_ref().unwrap();

            // Add one more reference for puffin cache handle.
            // There'll be no IO operations during cache access, thus no failure or evicted files expected.
            let (new_puffin_cache_handle, cur_evicted) = self
                .object_storage_cache
                .get_cache_entry(
                    puffin_deletion_blob.puffin_file_cache_handle.file_id,
                    /*remote_filepath=*/ "",
                    /*filesystem_accessor*/ self.filesystem_accessor.as_ref(),
                )
                .await
                .unwrap();
            assert!(cur_evicted.is_empty());
            puffin_cache_handles.push(new_puffin_cache_handle.unwrap());

            let puffin_file_index = puffin_cache_handles.len() - 1;
            deletion_vector_blob_at_read.push(PuffinDeletionBlobAtRead {
                data_file_index: idx as u32,
                puffin_file_index: puffin_file_index as u32,
                start_offset: puffin_deletion_blob.start_offset,
                blob_size: puffin_deletion_blob.blob_size,
            });
        }

        // Get committed but un-persisted deletion vector.
        let mut ret = Vec::new();
        for deletion in self.committed_deletion_log.iter() {
            if let RecordLocation::DiskFile(file_id, row_id) = &deletion.pos {
                for (id, (file, _)) in self.current_snapshot.disk_files.iter().enumerate() {
                    if file.file_id() == *file_id {
                        ret.push((id as u32, *row_id as u32));
                        break;
                    }
                }
            }
        }
        (puffin_cache_handles, deletion_vector_blob_at_read, ret)
    }

    pub(crate) async fn request_read(&mut self) -> Result<SnapshotReadOutput> {
        let mut data_file_paths = self.get_read_files_for_read().await;
        let mut associated_files = Vec::new();
        let (puffin_cache_handles, deletion_vectors_at_read, position_deletes) =
            self.get_deletion_records().await;

        // For committed but not persisted records, we create a temporary file for them, which gets deleted after query completion.
        let file_path = self.current_snapshot.get_name_for_inmemory_file();
        let filepath_exists = tokio::fs::try_exists(&file_path).await?;
        if filepath_exists {
            data_file_paths.push(DataFileForRead::TemporaryDataFile(
                file_path.to_string_lossy().to_string(),
            ));
            associated_files.push(file_path.to_string_lossy().to_string());
            return Ok(SnapshotReadOutput {
                data_file_paths,
                puffin_cache_handles,
                deletion_vectors: deletion_vectors_at_read,
                position_deletes,
                associated_files,
                object_storage_cache: Some(self.object_storage_cache.clone()),
                filesystem_accessor: Some(self.filesystem_accessor.clone()),
                table_notifier: Some(self.table_notify.as_ref().unwrap().clone()),
            });
        }

        assert!(matches!(
            self.last_commit,
            RecordLocation::MemoryBatch(_, _)
        ));
        let (batch_id, row_id) = self.last_commit.clone().into();
        if batch_id > 0 || row_id > 0 {
            // add all batches
            let mut filtered_batches = Vec::new();
            let schema = self.current_snapshot.metadata.schema.clone();
            for (id, batch) in self.batches.iter() {
                if *id < batch_id {
                    if let Some(filtered_batch) = batch.get_filtered_batch()? {
                        filtered_batches.push(filtered_batch);
                    }
                } else if *id == batch_id && row_id > 0 {
                    if batch.data.is_some() {
                        if let Some(filtered_batch) = batch.get_filtered_batch_with_limit(row_id)? {
                            filtered_batches.push(filtered_batch);
                        }
                    } else {
                        let rows = self.rows.as_ref().unwrap().get_buffer(row_id);
                        let deletions = &self
                            .batches
                            .values()
                            .last()
                            .expect("batch not found")
                            .deletions;
                        let batch = create_batch_from_rows(rows, schema.clone(), deletions);
                        filtered_batches.push(batch);
                    }
                }
            }

            // TODO(hjiang): Check whether we could avoid IO operation inside of critical section.
            if !filtered_batches.is_empty() {
                // Build a parquet file from current record batches
                let temp_file = tokio::fs::File::create(&file_path).await?;
                let props = WriterProperties::builder()
                    .set_compression(Compression::UNCOMPRESSED)
                    .set_dictionary_enabled(false)
                    .set_encoding(Encoding::PLAIN)
                    .build();
                let mut parquet_writer = AsyncArrowWriter::try_new(temp_file, schema, Some(props))?;
                for batch in filtered_batches.iter() {
                    parquet_writer.write(batch).await?;
                }
                parquet_writer.close().await?;
                data_file_paths.push(DataFileForRead::TemporaryDataFile(
                    file_path.to_string_lossy().to_string(),
                ));
                associated_files.push(file_path.to_string_lossy().to_string());
            }
        }
        Ok(SnapshotReadOutput {
            data_file_paths,
            puffin_cache_handles,
            deletion_vectors: deletion_vectors_at_read,
            position_deletes,
            associated_files,
            object_storage_cache: Some(self.object_storage_cache.clone()),
            filesystem_accessor: Some(self.filesystem_accessor.clone()),
            table_notifier: Some(self.table_notify.as_ref().unwrap().clone()),
        })
    }
}
