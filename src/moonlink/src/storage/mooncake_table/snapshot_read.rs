use super::data_batches::create_batch_from_rows;
use crate::error::Result;
use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::mooncake_table::snapshot::SnapshotTableState;
use crate::storage::mooncake_table::snapshot_read_output::{
    DataFileForRead, ReadOutput as SnapshotReadOutput,
};
use crate::storage::mooncake_table::table_status::TableSnapshotStatus;
use crate::storage::storage_utils::RecordLocation;
use crate::table_notify::{EvictedFiles, TableEvent};
use crate::NonEvictableHandle;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use moonlink_table_metadata::{DeletionVector, PositionDelete};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

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
    /// Get the number of rows in record batches before filtering.
    fn get_in_memory_row_num(&self) -> u64 {
        // Minic union read functionality to get all committed in-memory row.
        let mut num_rows = 0;
        let (batch_id, row_id) = self.last_commit.clone().into();
        if batch_id > 0 || row_id > 0 {
            for (id, batch) in self.batches.iter() {
                if *id < batch_id {
                    num_rows += batch.get_raw_record_number();
                } else if *id == batch_id && row_id > 0 {
                    if batch.data.is_some() {
                        num_rows += batch.get_raw_record_number();
                    } else {
                        let rows = self.rows.as_ref().unwrap().get_buffer(row_id);
                        num_rows += rows.len() as u64;
                    }
                }
            }
        }
        num_rows
    }

    pub(crate) fn get_table_snapshot_states(&self) -> Result<TableSnapshotStatus> {
        let persisted_row_num = self.current_snapshot.get_cardinality();
        let in_memory_row_num = self.get_in_memory_row_num();
        Ok(TableSnapshotStatus {
            commit_lsn: self.current_snapshot.snapshot_version,
            flush_lsn: self.current_snapshot.flush_lsn,
            cardinality: persisted_row_num + in_memory_row_num,
            iceberg_warehouse_location: self.iceberg_warehouse_location.clone(),
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
        Vec<NonEvictableHandle>, /*puffin file cache handles*/
        Vec<DeletionVector>,     /*deletion vector puffin*/
        Vec<PositionDelete>,
    ) {
        // Deletion records consist of two parts:
        // - persisted ones represented in puffin blob format
        // - committed but unpersisted deletion records, which could be deduced from committed batch deletion records and persisted ones corresponding to each data file
        let mut puffin_cache_handles = vec![];
        let mut deletion_vector_blob_at_read = vec![];
        let mut committed_unpersisted_committed_records = vec![];

        for (file_idx, (_, disk_deletion_vector)) in
            self.current_snapshot.disk_files.iter().enumerate()
        {
            if disk_deletion_vector.puffin_deletion_blob.is_none() {
                continue;
            }

            // Get persisted deletion vector, in iceberg puffin blob format.
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
            deletion_vector_blob_at_read.push(DeletionVector {
                data_file_number: file_idx as u32,
                puffin_file_number: puffin_file_index as u32,
                offset: puffin_deletion_blob.start_offset,
                size: puffin_deletion_blob.blob_size,
            });
        }

        // Get committed but un-persisted deletion vector.
        for deletion in self.committed_deletion_log.iter() {
            if let RecordLocation::DiskFile(file_id, row_id) = &deletion.pos {
                for (id, (file, _)) in self.current_snapshot.disk_files.iter().enumerate() {
                    if file.file_id() == *file_id {
                        committed_unpersisted_committed_records.push(PositionDelete {
                            data_file_number: id as u32,
                            data_file_row_number: *row_id as u32,
                        });
                        break;
                    }
                }
            }
        }

        (
            puffin_cache_handles,
            deletion_vector_blob_at_read,
            committed_unpersisted_committed_records,
        )
    }

    /// Prepare and materialize in one step, for tests that hold the snapshot lock anyway.
    #[cfg(test)]
    pub(crate) async fn request_read(&mut self) -> Result<SnapshotReadOutput> {
        self.prepare_read().await?.materialize().await
    }

    /// Collect everything a read needs while holding the snapshot lock: data files, pinned
    /// deletion vectors, and the committed in-memory rows as record batches. No file IO; the
    /// temporary parquet is written by [`PendingRead::materialize`] after the lock is released,
    /// so readers don't stall the snapshot task (and ingestion) on disk writes.
    pub(crate) async fn prepare_read(&mut self) -> Result<PendingRead> {
        let data_file_paths = self.get_read_files_for_read().await;
        let (puffin_cache_handles, deletion_vectors_at_read, position_deletes) =
            self.get_deletion_records().await;
        let mut pending = PendingRead {
            data_file_paths,
            puffin_cache_handles,
            deletion_vectors: deletion_vectors_at_read,
            position_deletes,
            in_memory_batches: Vec::new(),
            schema: self.current_snapshot.metadata.schema.clone(),
            temp_file_path: self.current_snapshot.get_name_for_inmemory_file(),
            object_storage_cache: self.object_storage_cache.clone(),
            filesystem_accessor: self.filesystem_accessor.clone(),
            table_notifier: self.table_notify.as_ref().unwrap().clone(),
            snapshot_seq: self.snapshot_seq.load(std::sync::atomic::Ordering::SeqCst),
        };

        assert!(matches!(
            self.last_commit,
            RecordLocation::MemoryBatch(_, _)
        ));
        let (batch_id, row_id) = self.last_commit.clone().into();
        if batch_id > 0 || row_id > 0 {
            let schema = pending.schema.clone();
            for (id, batch) in self.batches.iter() {
                if *id < batch_id {
                    if let Some(filtered_batch) = batch.get_filtered_batch()? {
                        pending.in_memory_batches.push(filtered_batch);
                    }
                } else if *id == batch_id && row_id > 0 {
                    if batch.data.is_some() {
                        if let Some(filtered_batch) = batch.get_filtered_batch_with_limit(row_id)? {
                            pending.in_memory_batches.push(filtered_batch);
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
                        pending.in_memory_batches.push(batch);
                    }
                }
            }
        }
        Ok(pending)
    }
}

/// A read prepared under the snapshot lock; see [`SnapshotTableState::prepare_read`].
pub(crate) struct PendingRead {
    data_file_paths: Vec<DataFileForRead>,
    puffin_cache_handles: Vec<NonEvictableHandle>,
    deletion_vectors: Vec<DeletionVector>,
    position_deletes: Vec<PositionDelete>,
    in_memory_batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
    temp_file_path: PathBuf,
    object_storage_cache: Arc<dyn CacheTrait>,
    filesystem_accessor: Arc<dyn BaseFileSystemAccess>,
    table_notifier: Sender<TableEvent>,
    /// Snapshot sequence the read was prepared from (read under the snapshot lock).
    snapshot_seq: u64,
}

impl PendingRead {
    pub(crate) fn snapshot_seq(&self) -> u64 {
        self.snapshot_seq
    }

    /// Write committed in-memory rows to a temporary parquet file (deleted with the ReadState).
    pub(crate) async fn materialize(mut self) -> Result<SnapshotReadOutput> {
        let mut associated_files = Vec::new();
        if !self.in_memory_batches.is_empty() {
            let path = self.temp_file_path.to_string_lossy().to_string();
            if let Err(e) = self.write_in_memory_file().await {
                // Unpin deletion vectors and drop the partial file, then surface the error.
                let mut files_to_delete = vec![path];
                for handle in self.puffin_cache_handles.drain(..) {
                    files_to_delete.extend(handle.unreference().await);
                }
                let _ = self
                    .table_notifier
                    .send(TableEvent::EvictedFilesToDelete {
                        evicted_files: EvictedFiles {
                            files: files_to_delete,
                        },
                    })
                    .await;
                return Err(e);
            }
            self.data_file_paths
                .push(DataFileForRead::TemporaryDataFile(path.clone()));
            associated_files.push(path);
        }
        Ok(SnapshotReadOutput {
            data_file_paths: self.data_file_paths,
            puffin_cache_handles: self.puffin_cache_handles,
            deletion_vectors: self.deletion_vectors,
            position_deletes: self.position_deletes,
            associated_files,
            object_storage_cache: Some(self.object_storage_cache),
            filesystem_accessor: Some(self.filesystem_accessor),
            table_notifier: Some(self.table_notifier),
        })
    }

    async fn write_in_memory_file(&self) -> Result<()> {
        let temp_file = tokio::fs::File::create(&self.temp_file_path).await?;
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::PLAIN)
            .build();
        let mut parquet_writer =
            AsyncArrowWriter::try_new(temp_file, self.schema.clone(), Some(props))?;
        for batch in self.in_memory_batches.iter() {
            parquet_writer.write(batch).await?;
        }
        parquet_writer.close().await?;
        Ok(())
    }
}
