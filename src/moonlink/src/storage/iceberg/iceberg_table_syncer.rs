use crate::observability::latency_exporter::BaseLatencyExporter;
use crate::storage::cache::object_storage::base_cache::InlineEvictedFiles;
use crate::storage::compaction::table_compaction::RemappedRecordLocation;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
    MOONCAKE_DELETION_VECTOR_NUM_ROWS,
};
use crate::storage::iceberg::iceberg_table_manager::*;
use crate::storage::iceberg::index::FileIndexBlob;
use crate::storage::iceberg::io_utils as iceberg_io_utils;
use crate::storage::iceberg::moonlink_catalog::PuffinBlobType;
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::iceberg::puffin_writer_proxy::{
    get_puffin_metadata_and_close, PuffinBlobMetadataProxy,
};
use crate::storage::iceberg::schema_utils;
use crate::storage::iceberg::table_manager::{PersistenceFileParams, PersistenceResult};
use crate::storage::iceberg::utils::get_unique_hash_index_v1_filepath;
use crate::storage::index::FileIndex as MooncakeFileIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::take_data_files_to_remove;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::{
    take_data_files_to_import, take_file_indices_to_import, take_file_indices_to_remove,
};
use crate::storage::storage_utils::{self, MooncakeDataFile};
use crate::storage::storage_utils::{
    create_data_file, get_unique_file_id_for_flush, FileId, MooncakeDataFileRef, RecordLocation,
    TableId, TableUniqueFileId,
};
use crate::Result;

use futures::{stream, StreamExt, TryStreamExt};
use iceberg::table::Table;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::vec;

use iceberg::puffin::CompressionCodec;
use iceberg::spec::DataFile;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Error as IcebergError, Result as IcebergResult};

/// Default concurrency of iceberg data file upload.
const DEFAULT_DATA_FILE_UPLOAD_CONCURRENCY: usize = 128;
/// Default concurrency for iceberg file indices import.
const DEFAULT_FILE_INDEX_IMPORT_CONCURRENCY: usize = 128;
/// Default concurrency for iceberg deletion vectors synchronization.
const DEFAULT_SYNC_DELETION_VECTOR_CONCURRENCY: usize = 128;

/// Results for importing data files into iceberg table.
pub struct DataFileImportResult {
    /// New data files to import into iceberg table.
    new_iceberg_data_files: Vec<DataFile>,
    /// Local data file to remote one mapping.
    local_data_files_to_remote: HashMap<String, String>,
    /// New mooncake data files, represented in remote file paths.
    new_remote_data_files: Vec<MooncakeDataFileRef>,
    /// Deletion records after compaction.
    ///
    /// A committed deletion record could appear in two places: committed deletion log in mooncake snapshot, or iceberg puffin blob.
    /// For later, we need to do remapping for already persisted disk files.
    remapped_deletion_records: HashMap<MooncakeDataFileRef, BatchDeletionVector>,
}

/// Result for writing deletion vectors into iceberg table.
struct DeletionVectorsSyncResult {
    puffin_deletion_blobs: HashMap<FileId, PuffinBlobRef>,
    evicted_files_to_delete: Vec<String>,
}

/// Import result for single file index.
struct SingleFileIndexImportResult {
    /// Maps from local index block file to remote filepath.
    local_index_file_to_remote: HashMap<String, String>,
    /// File index.
    mooncake_file_index: MooncakeFileIndex,
    /// Puffin metadata.
    puffin_metadata: Vec<PuffinBlobMetadataProxy>,
    /// Puffin filepath.
    puffin_filepath: String,
}

/// A prepared deletion-vector blob ready for Puffin metadata recording.
struct PreparedDeletionVectorBlob {
    /// Source data file in Iceberg.
    data_file: Arc<MooncakeDataFile>,
    /// Puffin index.
    puffin_index: u64,
    /// Blob size.
    blob_size: usize,
    /// Data file entry
    entry: DataFileEntry,
    /// Puffin file path the blob is stored.
    puffin_filepath: String,
    /// Puffin metadata for the blob.
    puffin_metadata: Option<Vec<PuffinBlobMetadataProxy>>,
    /// Deleted row count.
    deleted_row_count: usize,
}

/// Result of finalizing a deletion-vector blob into the Iceberg table.
struct FinalizeDeletionVectorResult {
    /// File id.
    file_id: FileId,
    /// Puffin blob reference.
    puffin_blob_ref: PuffinBlobRef,
    /// Evicted files.
    evicted_files_to_delete: InlineEvictedFiles,
    /// Data file entry
    entry: DataFileEntry,
}

/// Import one single mooncake file index.
async fn import_one_file_index(
    puffin_filepath: String,
    mooncake_file_index: &MooncakeFileIndex,
    local_data_file_to_remote: &HashMap<String, String>,
    iceberg_table: &Table,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) -> IcebergResult<SingleFileIndexImportResult> {
    let mut local_index_file_to_remote = HashMap::new();

    // Create one puffin file (with one puffin blob inside of it) for each mooncake file index.
    let mut puffin_writer =
        puffin_utils::create_puffin_writer(iceberg_table.file_io(), &puffin_filepath).await?;

    // Upload new index file to iceberg table.
    for cur_index_block in mooncake_file_index.index_blocks.iter() {
        let remote_index_block = iceberg_io_utils::upload_index_file(
            iceberg_table,
            cur_index_block.index_file.file_path(),
            filesystem_accessor,
        )
        .await?;
        local_index_file_to_remote.insert(
            cur_index_block.index_file.file_path().to_string(),
            remote_index_block,
        );
    }

    // Persist the puffin file and record in file catalog.
    let file_index_blob = FileIndexBlob::new(
        mooncake_file_index,
        &local_index_file_to_remote,
        local_data_file_to_remote,
    );
    let puffin_blob = file_index_blob.as_blob()?;
    puffin_writer
        .add(puffin_blob, iceberg::puffin::CompressionCodec::None)
        .await?;
    let puffin_metadata = get_puffin_metadata_and_close(puffin_writer).await?;

    Ok(SingleFileIndexImportResult {
        local_index_file_to_remote,
        mooncake_file_index: mooncake_file_index.clone(),
        puffin_metadata,
        puffin_filepath,
    })
}

impl IcebergTableManager {
    /// Validate schema consistency at store operation.
    async fn validate_schema_consistency_at_store(&self) {
        schema_utils::assert_table_schema_id(self.iceberg_table.as_ref().unwrap());
    }

    // Validate data files to add don't belong to iceberg snapshot.
    fn validate_new_data_files(&self, new_data_files: &[MooncakeDataFileRef]) -> IcebergResult<()> {
        for cur_data_file in new_data_files.iter() {
            if self
                .persisted_data_files
                .contains_key(&cur_data_file.file_id())
            {
                return Err(IcebergError::new(
                    iceberg::ErrorKind::PreconditionFailed,
                    format!("Data file to add {cur_data_file:?} already persisted in iceberg."),
                ));
            }
        }
        Ok(())
    }

    // Validate data files to remove don't belong to iceberg snapshot.
    fn validate_old_data_files(&self, old_data_files: &[MooncakeDataFileRef]) -> IcebergResult<()> {
        for cur_data_file in old_data_files.iter() {
            if !self
                .persisted_data_files
                .contains_key(&cur_data_file.file_id())
            {
                return Err(IcebergError::new(
                    iceberg::ErrorKind::PreconditionFailed,
                    format!("Data file to remove {cur_data_file:?} is not persisted in iceberg."),
                ));
            }
        }
        Ok(())
    }

    /// Util function to get unique table file id for the deletion vector puffin file.
    ///
    /// Notice: only deletion vector puffin generates new file ids.
    fn get_unique_table_id_for_deletion_vector_puffin(
        &self,
        file_params: &PersistenceFileParams,
        puffin_index: u64,
    ) -> TableUniqueFileId {
        let unique_table_auto_incre_id_offset = puffin_index / storage_utils::NUM_FILES_PER_FLUSH;
        let cur_table_auto_incr_id =
            file_params.table_auto_incr_ids.start as u64 + unique_table_auto_incre_id_offset;
        assert!(file_params
            .table_auto_incr_ids
            .contains(&(cur_table_auto_incr_id as u32)));
        let cur_file_idx =
            puffin_index - storage_utils::NUM_FILES_PER_FLUSH * unique_table_auto_incre_id_offset;
        TableUniqueFileId {
            table_id: TableId(self.mooncake_table_metadata.table_id),
            file_id: FileId(get_unique_file_id_for_flush(
                cur_table_auto_incr_id,
                cur_file_idx,
            )),
        }
    }

    /// Dump local data files into iceberg table.
    /// Return new iceberg data files for append transaction, and local data filepath to remote data filepath for index block remapping.
    async fn sync_data_files(
        &mut self,
        new_data_files: Vec<MooncakeDataFileRef>,
        old_data_files: Vec<MooncakeDataFileRef>,
        data_file_records_remap: &HashMap<RecordLocation, RemappedRecordLocation>,
    ) -> IcebergResult<DataFileImportResult> {
        if new_data_files.is_empty() && old_data_files.is_empty() {
            return Ok(DataFileImportResult {
                new_iceberg_data_files: Vec::new(),
                local_data_files_to_remote: HashMap::new(),
                new_remote_data_files: Vec::new(),
                remapped_deletion_records: HashMap::new(),
            });
        }
        // Record data files synchronization latency.
        let _guard = self.persistence_stats_sync_data_files.start();
        let mut local_data_files_to_remote = HashMap::with_capacity(new_data_files.len());
        let mut new_remote_data_files = Vec::with_capacity(new_data_files.len());
        let mut new_iceberg_data_files = Vec::with_capacity(new_data_files.len());

        let iceberg_table = self.iceberg_table.clone();
        let filesystem_accessor = self.filesystem_accessor.clone();

        // Import disk slice writer to iceberg table.
        let new_data_files_clone = new_data_files.clone();
        let iceberg_data_files: Vec<DataFile> = stream::iter(new_data_files_clone.into_iter())
            .map(move |local_data_file| {
                let iceberg_table = iceberg_table.clone();
                let filesystem_accessor = filesystem_accessor.clone();
                async move {
                    iceberg_io_utils::write_record_batch_to_iceberg(
                        iceberg_table.as_ref().as_ref().unwrap(),
                        local_data_file.file_path(),
                        iceberg_table.as_ref().unwrap().metadata(),
                        filesystem_accessor.as_ref(),
                    )
                    .await
                }
            })
            .buffered(DEFAULT_DATA_FILE_UPLOAD_CONCURRENCY)
            .try_collect()
            .await?;

        // Handle imported new data files.
        for (idx, local_data_file) in new_data_files.into_iter().enumerate() {
            let cur_iceberg_data_file = iceberg_data_files[idx].clone();
            let num_rows = cur_iceberg_data_file.record_count();

            // Insert new entry into iceberg table manager persisted data files.
            let old_entry = self.persisted_data_files.insert(
                local_data_file.file_id(),
                DataFileEntry {
                    data_file: cur_iceberg_data_file.clone(),
                    // Max number of rows will be initialized when deletion take place.
                    deletion_vector: BatchDeletionVector::new(num_rows as usize),
                },
            );
            assert!(old_entry.is_none());

            // Insert into local to remote mapping.
            assert!(local_data_files_to_remote
                .insert(
                    local_data_file.file_path().clone(),
                    cur_iceberg_data_file.file_path().to_string(),
                )
                .is_none());

            // Record all imported iceberg data files, with file id unchanged.
            new_remote_data_files.push(create_data_file(
                local_data_file.file_id().0,
                cur_iceberg_data_file.file_path().to_string(),
            ));

            // Record file path to file id mapping.
            assert!(self
                .remote_data_file_to_file_id
                .insert(
                    cur_iceberg_data_file.file_path().to_string(),
                    local_data_file.file_id(),
                )
                .is_none());

            new_iceberg_data_files.push(cur_iceberg_data_file);
        }

        // Remap already persisted data files; till now all data file's attributes are accessible.
        let mut remapped_deletion_records =
            HashMap::<MooncakeDataFileRef, BatchDeletionVector>::new();
        for (old_file_id, old_data_file_entry) in self.persisted_data_files.iter() {
            let old_batch_deletion_vector = &old_data_file_entry.deletion_vector;
            let old_deleted_rows = old_batch_deletion_vector.collect_deleted_rows();

            for old_row_idx in old_deleted_rows.into_iter() {
                let old_record_location =
                    RecordLocation::DiskFile(*old_file_id, old_row_idx as usize);
                if let Some(new_remapped_record_location) =
                    data_file_records_remap.get(&old_record_location)
                {
                    let new_data_file = new_remapped_record_location.new_data_file.clone();
                    let new_row_idx = new_remapped_record_location.record_location.get_row_idx();
                    if let Some(new_batch_deletion_vector) =
                        remapped_deletion_records.get_mut(&new_data_file)
                    {
                        assert!(new_batch_deletion_vector.delete_row(new_row_idx));
                    } else {
                        let new_data_file_entry = self
                            .persisted_data_files
                            .get(&new_data_file.file_id())
                            // Invariant sanity check: all data files have been imported into in-memory state.
                            .unwrap();
                        let mut new_batch_deletion_vector = BatchDeletionVector::new(
                            new_data_file_entry.data_file.record_count() as usize,
                        );
                        assert!(new_batch_deletion_vector.delete_row(new_row_idx));
                        assert!(remapped_deletion_records
                            .insert(new_data_file, new_batch_deletion_vector)
                            .is_none());
                    }
                }
            }
        }

        // Handle removed data files.
        let mut data_files_to_remove_set = HashSet::with_capacity(old_data_files.len());
        for cur_data_file in old_data_files.into_iter() {
            let old_entry = self
                .persisted_data_files
                .remove(&cur_data_file.file_id())
                .unwrap();
            data_files_to_remove_set.insert(old_entry.data_file.file_path().to_string());
            assert!(self
                .remote_data_file_to_file_id
                .remove(old_entry.data_file.file_path())
                .is_some());
        }
        self.catalog
            .set_data_files_to_remove(data_files_to_remove_set);

        Ok(DataFileImportResult {
            new_iceberg_data_files,
            local_data_files_to_remote,
            new_remote_data_files,
            remapped_deletion_records,
        })
    }

    // prepare the deletion vector blob that would be written into puffin metadata file.
    async fn prepare_deletion_vector_blob(
        &self,
        puffin_index: u64,
        data_file: Arc<MooncakeDataFile>,
        new_deletion_vector: BatchDeletionVector,
    ) -> IcebergResult<PreparedDeletionVectorBlob> {
        let mut entry = self
            .persisted_data_files
            .get(&data_file.file_id())
            .unwrap()
            .clone();
        assert_eq!(
            entry.deletion_vector.get_max_rows(),
            new_deletion_vector.get_max_rows()
        );
        entry.deletion_vector.merge_with(&new_deletion_vector);
        let iceberg_data_file = entry.data_file.file_path().to_string();

        let deleted_rows = entry.deletion_vector.clone().collect_deleted_rows();
        assert!(!deleted_rows.is_empty());

        let deleted_row_count = deleted_rows.len();
        let mut iceberg_deletion_vector = DeletionVector::new();
        iceberg_deletion_vector.mark_rows_deleted(deleted_rows);

        let blob_properties = HashMap::from([
            (
                DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(),
                iceberg_data_file.clone(),
            ),
            (
                DELETION_VECTOR_CADINALITY.to_string(),
                deleted_row_count.to_string(),
            ),
            (
                MOONCAKE_DELETION_VECTOR_NUM_ROWS.to_string(),
                entry.deletion_vector.get_max_rows().to_string(),
            ),
        ]);
        let blob = iceberg_deletion_vector.serialize(blob_properties);
        let blob_size = blob.data().len();
        let puffin_filepath = self.get_unique_deletion_vector_filepath();
        let mut puffin_writer = puffin_utils::create_puffin_writer(
            self.iceberg_table.as_ref().unwrap().file_io(),
            &puffin_filepath,
        )
        .await?;
        puffin_writer.add(blob, CompressionCodec::None).await?;
        let puffin_metadata = get_puffin_metadata_and_close(puffin_writer).await?;
        Ok(PreparedDeletionVectorBlob {
            data_file,
            puffin_index,
            blob_size,
            puffin_filepath,
            puffin_metadata: Some(puffin_metadata),
            deleted_row_count,
            entry,
        })
    }

    // Finalize deletion vector by caching it's Puffin file
    async fn finalize_deletion_vector(
        &self,
        file_params: &PersistenceFileParams,
        deletion_vector_blob: PreparedDeletionVectorBlob,
    ) -> IcebergResult<FinalizeDeletionVectorResult> {
        let unique_file_id = self.get_unique_table_id_for_deletion_vector_puffin(
            file_params,
            deletion_vector_blob.puffin_index,
        );
        let (cache_handle, evicted_files_to_delete) = self
            .object_storage_cache
            .get_cache_entry(
                unique_file_id,
                &deletion_vector_blob.puffin_filepath,
                self.filesystem_accessor.as_ref(),
            )
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!(
                        "Failed to get cache entry for {}",
                        deletion_vector_blob.puffin_filepath
                    ),
                )
                .with_retryable(true)
                .with_source(e)
            })?;

        let puffin_blob_ref = PuffinBlobRef {
            puffin_file_cache_handle: cache_handle.unwrap(),
            start_offset: 4_u32, // Puffin file starts with 4 magic bytes.
            blob_size: deletion_vector_blob.blob_size as u32,
            num_rows: deletion_vector_blob.deleted_row_count,
        };

        Ok(FinalizeDeletionVectorResult {
            file_id: deletion_vector_blob.data_file.file_id(),
            puffin_blob_ref,
            evicted_files_to_delete,
            entry: deletion_vector_blob.entry,
        })
    }

    /// Dump committed deletion logs into iceberg table, only the changed part will be persisted.
    /// Precondition: batch deletion vector in [`new_deletion_logs`] is not empty.
    ///
    /// Puffin blob write condition:
    /// 1. No compression is performed, otherwise it's hard to get blob size without another read operation.
    /// 2. We put one deletion vector within one puffin file.
    async fn sync_deletion_vector(
        &mut self,
        new_deletion_logs: HashMap<MooncakeDataFileRef, BatchDeletionVector>,
        file_params: &PersistenceFileParams,
    ) -> IcebergResult<DeletionVectorsSyncResult> {
        if new_deletion_logs.is_empty() {
            return Ok(DeletionVectorsSyncResult {
                puffin_deletion_blobs: HashMap::new(),
                evicted_files_to_delete: Vec::new(),
            });
        }
        let _guard = self.persistence_stats_sync_deletion_vectors.start();
        let mut puffin_deletion_blobs = HashMap::with_capacity(new_deletion_logs.len());
        let mut evicted_files_to_delete = vec![];
        let prepared: Vec<IcebergResult<PreparedDeletionVectorBlob>>;
        let finalized: Vec<IcebergResult<FinalizeDeletionVectorResult>>;
        {
            let mgr: &Self = self;
            prepared = stream::iter(new_deletion_logs.into_iter().enumerate().map(
                |(puffin_index, (data_file, new_deletion_vector))| async move {
                    let mgr = mgr;
                    mgr.prepare_deletion_vector_blob(
                        puffin_index as u64,
                        data_file,
                        new_deletion_vector,
                    )
                    .await
                },
            ))
            .buffer_unordered(DEFAULT_SYNC_DELETION_VECTOR_CONCURRENCY)
            .collect()
            .await;
        }
        let mut prepared: Vec<PreparedDeletionVectorBlob> =
            prepared.into_iter().collect::<IcebergResult<Vec<_>>>()?;

        for task in &mut prepared {
            let puffin_metadata = task.puffin_metadata.take().unwrap();
            self.catalog.record_puffin_metadata(
                task.puffin_filepath.clone(),
                puffin_metadata,
                PuffinBlobType::DeletionVector,
            );
        }
        {
            let mgr: &Self = self;
            finalized = stream::iter(prepared.into_iter().map(|task| async move {
                let mgr = mgr;
                mgr.finalize_deletion_vector(file_params, task).await
            }))
            .buffer_unordered(DEFAULT_SYNC_DELETION_VECTOR_CONCURRENCY)
            .collect()
            .await;
        }
        for result in finalized {
            let result = result?;
            let old_entry = self
                .persisted_data_files
                .insert(result.file_id, result.entry);
            assert!(old_entry.is_some());
            assert!(puffin_deletion_blobs
                .insert(result.file_id, result.puffin_blob_ref)
                .is_none());
            evicted_files_to_delete.extend(result.evicted_files_to_delete);
        }
        Ok(DeletionVectorsSyncResult {
            puffin_deletion_blobs,
            evicted_files_to_delete,
        })
    }

    /// Update data file path pointed by file indices, from local filepath to remote, with file id unchanged.
    ///
    /// # Arguments:
    ///
    /// * local_data_file_to_remote: contains mappings from newly imported data files to remote paths.
    /// * local_index_file_to_remote: contains mappings from newly imported data files to remote paths.
    fn get_updated_file_index_at_import(
        old_file_index: &MooncakeFileIndex,
        local_data_file_to_remote: &HashMap<String, String>,
        local_index_file_to_remote: &HashMap<String, String>,
    ) -> MooncakeFileIndex {
        let mut new_file_index = old_file_index.clone();

        // Update data file from local path to remote one.
        for cur_data_file in new_file_index.files.iter_mut() {
            let remote_data_file = local_data_file_to_remote
                .get(cur_data_file.file_path())
                // [`local_data_file_to_remote`] only contains new data files introduced in the previous persistence,
                // but it's possible that the data file was already persisted in the previous iterations.
                .unwrap_or(cur_data_file.file_path())
                .clone();
            *cur_data_file = create_data_file(cur_data_file.file_id().0, remote_data_file);
        }

        // Update index block from local path to remote one.
        for cur_index_block in new_file_index.index_blocks.iter_mut() {
            let remote_index_block_filepath = local_index_file_to_remote
                .get(cur_index_block.index_file.file_path())
                .unwrap()
                .clone();
            cur_index_block.index_file = create_data_file(
                cur_index_block.index_file.file_id().0,
                remote_index_block_filepath,
            );
            // At this point, all index block files are at an inconsistent state, which have their
            // - file path pointing to remote path
            // - cache handle pinned and refers to local cache file path
            // The inconsistency will be fixed when they're imported into mooncake snapshot.
        }

        new_file_index
    }

    /// Process file indices to import.
    /// One mooncake file index correspond to one puffin file, with one blob inside of it.
    ///
    /// [`local_data_file_to_remote`] should contain all local data filepath to remote data filepath mapping.
    /// Return the mapping from local index files to remote index files.
    async fn import_file_indices(
        &mut self,
        file_indices_to_import: &[MooncakeFileIndex],
        local_data_file_to_remote: &HashMap<String, String>,
    ) -> IcebergResult<HashMap<String, String>> {
        // TODO(hjiang): Maps from local filepath to remote filepath.
        // After sync, file index still stores local index file location.
        // After cache design, we should be able to provide a "handle" abstraction, which could be either local or remote.
        // The hash map here is merely a workaround to pass remote path to iceberg file index structure.

        if file_indices_to_import.is_empty() && local_data_file_to_remote.is_empty() {
            return Ok(HashMap::new());
        }
        // Record file indices synchronization latency.
        let _guard = self.persistence_stats_sync_file_indices.start();
        let mut local_index_file_to_remote = HashMap::new();

        let iceberg_table = self.iceberg_table.as_ref().unwrap();
        let filesystem_accessor = &*self.filesystem_accessor;
        let local_data_file_to_remote_clone = Arc::new(local_data_file_to_remote.clone());
        let file_indices_to_import_clone = file_indices_to_import.to_vec();

        let file_index_import_results: Vec<SingleFileIndexImportResult> =
            stream::iter(file_indices_to_import_clone.into_iter())
                .map(move |mooncake_file_index: MooncakeFileIndex| {
                    let iceberg_table = iceberg_table;
                    let puffin_filepath = get_unique_hash_index_v1_filepath(iceberg_table);
                    let filesystem_accessor = filesystem_accessor;
                    let local_data_file_to_remote_clone = local_data_file_to_remote_clone.clone();

                    async move {
                        import_one_file_index(
                            puffin_filepath,
                            &mooncake_file_index,
                            &local_data_file_to_remote_clone,
                            iceberg_table,
                            filesystem_accessor,
                        )
                        .await
                    }
                })
                .buffer_unordered(DEFAULT_FILE_INDEX_IMPORT_CONCURRENCY)
                .try_collect()
                .await?;

        for cur_res in file_index_import_results.into_iter() {
            let expected_new_count =
                local_index_file_to_remote.len() + cur_res.local_index_file_to_remote.len();
            local_index_file_to_remote.extend(cur_res.local_index_file_to_remote);
            let actual_new_count = local_index_file_to_remote.len();
            // Assert there's duplicate local index file.
            assert_eq!(expected_new_count, actual_new_count);

            assert!(self
                .persisted_file_indices
                .insert(cur_res.mooncake_file_index, cur_res.puffin_filepath.clone())
                .is_none());
            self.catalog.record_puffin_metadata(
                cur_res.puffin_filepath,
                cur_res.puffin_metadata,
                PuffinBlobType::FileIndex,
            );
        }

        Ok(local_index_file_to_remote)
    }

    /// Dump file indices into the iceberg table, only new file indices will be persisted into the table.
    /// Return file index ids which should be added into iceberg table.
    ///
    /// # Arguments:
    ///
    /// * local_data_file_to_remote: contains mappings from newly imported data files to remote paths.
    ///
    /// TODO(hjiang): Need to configure (1) the number of blobs in a puffin file; and (2) the number of file index in a puffin blob.
    /// For implementation simplicity, put everything in a single file and a single blob.
    async fn sync_file_indices(
        &mut self,
        file_indices_to_import: &[MooncakeFileIndex],
        file_indices_to_remove: &[MooncakeFileIndex],
        local_data_file_to_remote: HashMap<String, String>,
    ) -> IcebergResult<Vec<MooncakeFileIndex>> {
        if file_indices_to_import.is_empty() && file_indices_to_remove.is_empty() {
            return Ok(vec![]);
        }

        // Import new file indices.
        let local_index_block_to_remote = self
            .import_file_indices(file_indices_to_import, &local_data_file_to_remote)
            .await?;

        // Update local file indices:
        // - Redirect local data file to remote one
        // - Redirect local index block file to remote one
        let remote_file_indices = file_indices_to_import
            .iter()
            .map(|old_file_index| {
                Self::get_updated_file_index_at_import(
                    old_file_index,
                    &local_data_file_to_remote,
                    &local_index_block_to_remote,
                )
            })
            .collect::<Vec<_>>();

        // Process file indices to remove.
        self.catalog.set_index_puffin_files_to_remove(
            file_indices_to_remove
                .iter()
                .map(|cur_index| self.persisted_file_indices.remove(cur_index).unwrap())
                .collect::<HashSet<String>>(),
        );

        Ok(remote_file_indices)
    }

    pub(crate) async fn sync_snapshot_impl(
        &mut self,
        mut snapshot_payload: IcebergSnapshotPayload,
        file_params: PersistenceFileParams,
    ) -> Result<PersistenceResult> {
        // Start recording overall snapshot synchronization latency.
        let persistence_stats_overall = self.persistence_stats_overall.clone();
        let _guard = persistence_stats_overall.start();

        // Initialize iceberg table on access.
        self.initialize_iceberg_table_for_once().await?;

        // Validate schema consistency before persistence operation.
        self.validate_schema_consistency_at_store().await;

        let new_data_files = take_data_files_to_import(&mut snapshot_payload);
        let old_data_files = take_data_files_to_remove(&mut snapshot_payload);
        let new_file_indices = take_file_indices_to_import(&mut snapshot_payload);
        let old_file_indices = take_file_indices_to_remove(&mut snapshot_payload);

        // Validate data files to add and remove are valid.
        self.validate_new_data_files(&new_data_files)?;
        self.validate_old_data_files(&old_data_files)?;

        // Persist data files.
        let data_file_import_result = self
            .sync_data_files(
                new_data_files,
                old_data_files,
                &snapshot_payload
                    .data_compaction_payload
                    .data_file_records_remap,
            )
            .await?;

        // Persist committed deletion logs.
        let mut new_deletion_vector =
            std::mem::take(&mut snapshot_payload.import_payload.new_deletion_vector);
        let remapped_deletion_records = data_file_import_result.remapped_deletion_records;
        for (remapped_data_file, remapped_batch_deletion_vector) in
            remapped_deletion_records.into_iter()
        {
            if let Some(new_batch_deletion_vector) =
                new_deletion_vector.get_mut(&remapped_data_file)
            {
                // TODO(hjiang): Should validate conflict.
                new_batch_deletion_vector.merge_with(&remapped_batch_deletion_vector);
            } else {
                assert!(new_deletion_vector
                    .insert(remapped_data_file, remapped_batch_deletion_vector)
                    .is_none());
            }
        }

        let deletion_vectors_sync_result = self
            .sync_deletion_vector(new_deletion_vector, &file_params)
            .await?;

        let remote_file_indices = self
            .sync_file_indices(
                &new_file_indices,
                &old_file_indices,
                data_file_import_result.local_data_files_to_remote,
            )
            .await?;

        // Update snapshot summary properties.
        let snapshot_properties = HashMap::<String, String>::from([(
            MOONCAKE_TABLE_FLUSH_LSN.to_string(),
            snapshot_payload.flush_lsn.to_string(),
        )]);

        let mut txn = Transaction::new(self.iceberg_table.as_ref().unwrap());
        let mut action = txn.fast_append();

        // Duplicate files check is very expensive, disable for production usage.
        #[cfg(not(any(test, debug_assertions)))]
        {
            action = action.with_check_duplicate(false);
        }
        #[cfg(any(test, debug_assertions))]
        {
            action = action.with_check_duplicate(true);
        }

        // Only start append action when there're new data files.
        if !data_file_import_result.new_iceberg_data_files.is_empty() {
            let action = action.add_data_files(data_file_import_result.new_iceberg_data_files);
            let action = action.set_snapshot_properties(snapshot_properties);
            txn = action.apply(txn)?;
        }
        // Start an append transaction only to add snapshot properties.
        else {
            let action = action.set_snapshot_properties(snapshot_properties);
            txn = action.apply(txn)?;
        }

        let updated_iceberg_table = {
            // Start recording transaction commit latency.
            let _guard = self.persistence_stats_transaction_commit.start();
            // Commit the transaction.
            txn.commit(&*self.catalog).await?
        };
        self.iceberg_table = Some(updated_iceberg_table);

        self.catalog.clear_puffin_metadata();

        // NOTICE: persisted data files and file indices are returned in the order of (1) newly imported ones; (2) index merge ones; (3) data compacted ones.
        Ok(PersistenceResult {
            remote_data_files: data_file_import_result.new_remote_data_files,
            puffin_blob_ref: deletion_vectors_sync_result.puffin_deletion_blobs,
            remote_file_indices,
            evicted_files_to_delete: deletion_vectors_sync_result.evicted_files_to_delete,
        })
    }
}
