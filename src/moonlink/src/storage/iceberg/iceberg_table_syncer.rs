use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
    MOONCAKE_DELETION_VECTOR_NUM_ROWS,
};
use crate::storage::iceberg::iceberg_table_manager::*;
use crate::storage::iceberg::index::FileIndexBlob;
use crate::storage::iceberg::io_utils as iceberg_io_utils;
#[cfg(all(not(feature = "chaos-test"), any(test, debug_assertions)))]
use crate::storage::iceberg::manifest_utils;
use crate::storage::iceberg::manifest_utils::ManifestEntryCount;
use crate::storage::iceberg::moonlink_catalog::PuffinBlobType;
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::puffin_utils::PuffinBlobRef;
use crate::storage::iceberg::schema_utils;
use crate::storage::iceberg::table_manager::{PersistenceFileParams, PersistenceResult};
use crate::storage::index::FileIndex as MooncakeFileIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::take_data_files_to_remove;
use crate::storage::mooncake_table::IcebergSnapshotPayload;
use crate::storage::mooncake_table::{
    take_data_files_to_import, take_file_indices_to_import, take_file_indices_to_remove,
};
use crate::storage::storage_utils::{
    create_data_file, get_unique_file_id_for_flush, FileId, MooncakeDataFileRef, TableId,
    TableUniqueFileId,
};
use crate::storage::{io_utils, storage_utils};

use std::collections::{HashMap, HashSet};
use std::vec;

use iceberg::puffin::CompressionCodec;
use iceberg::spec::DataFile;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Error as IcebergError, Result as IcebergResult};

/// Results for importing data files into iceberg table.
pub struct DataFileImportResult {
    /// New data files to import into iceberg table.
    new_iceberg_data_files: Vec<DataFile>,
    /// Local data file to remote one mapping.
    local_data_files_to_remote: HashMap<String, String>,
    /// New mooncake data files, represented in remote file paths.
    new_remote_data_files: Vec<MooncakeDataFileRef>,
}

impl IcebergTableManager {
    /// Validate schema consistency at store operation.
    async fn validate_schema_consistency_at_store(&self) {
        schema_utils::assert_table_schema_id(self.iceberg_table.as_ref().unwrap());
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

    /// Write deletion vector to puffin file.
    /// Precondition: batch deletion vector is not empty.
    ///
    /// Puffin blob write condition:
    /// 1. No compression is performed, otherwise it's hard to get blob size without another read operation.
    /// 2. We put one deletion vector within one puffin file.
    async fn write_deletion_vector(
        &mut self,
        data_file: String,
        deletion_vector: BatchDeletionVector,
        file_params: &PersistenceFileParams,
        puffin_index: u64,
    ) -> IcebergResult<PuffinBlobRef> {
        let deleted_rows = deletion_vector.collect_deleted_rows();
        assert!(!deleted_rows.is_empty());

        let deleted_row_count = deleted_rows.len();
        let mut iceberg_deletion_vector = DeletionVector::new();
        iceberg_deletion_vector.mark_rows_deleted(deleted_rows);
        let blob_properties = HashMap::from([
            (DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(), data_file),
            (
                DELETION_VECTOR_CADINALITY.to_string(),
                deleted_row_count.to_string(),
            ),
            (
                MOONCAKE_DELETION_VECTOR_NUM_ROWS.to_string(),
                deletion_vector.get_max_rows().to_string(),
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

        self.catalog
            .record_puffin_metadata_and_close(
                puffin_filepath.clone(),
                puffin_writer,
                PuffinBlobType::DeletionVector,
            )
            .await?;

        // Import the puffin file in object storage cache.
        let unique_file_id =
            self.get_unique_table_id_for_deletion_vector_puffin(file_params, puffin_index);
        let (cache_handle, evicted_files_to_delete) = self
            .object_storage_cache
            .get_cache_entry(
                unique_file_id,
                &puffin_filepath,
                self.filesystem_accessor.as_ref(),
            )
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to get cache entry for {puffin_filepath}: {e:?}"),
                )
                .with_retryable(true)
            })?;
        io_utils::delete_local_files(&evicted_files_to_delete)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to delete files for {evicted_files_to_delete:?}: {e:?}"),
                )
                .with_retryable(true)
            })?;

        let puffin_blob_ref = PuffinBlobRef {
            puffin_file_cache_handle: cache_handle.unwrap(),
            start_offset: 4_u32, // Puffin file starts with 4 magic bytes.
            blob_size: blob_size as u32,
        };
        Ok(puffin_blob_ref)
    }

    /// Dump local data files into iceberg table.
    /// Return new iceberg data files for append transaction, and local data filepath to remote data filepath for index block remapping.
    async fn sync_data_files(
        &mut self,
        new_data_files: Vec<MooncakeDataFileRef>,
        old_data_files: Vec<MooncakeDataFileRef>,
        new_deletion_vector: &HashMap<MooncakeDataFileRef, BatchDeletionVector>,
    ) -> IcebergResult<DataFileImportResult> {
        let mut local_data_files_to_remote = HashMap::with_capacity(new_data_files.len());
        let mut new_remote_data_files = Vec::with_capacity(new_data_files.len());
        let mut new_iceberg_data_files = Vec::with_capacity(new_data_files.len());

        // Handle imported new data files.
        for local_data_file in new_data_files.into_iter() {
            let iceberg_data_file = iceberg_io_utils::write_record_batch_to_iceberg(
                self.iceberg_table.as_ref().unwrap(),
                local_data_file.file_path(),
                self.iceberg_table.as_ref().unwrap().metadata(),
                self.filesystem_accessor.as_ref(),
            )
            .await?;

            // Try get deletion vector batch size.
            let max_rows = new_deletion_vector
                .get(&local_data_file)
                .map(|dv| dv.get_max_rows());

            // Insert new entry into iceberg table manager persisted data files.
            let old_entry = self.persisted_data_files.insert(
                local_data_file.file_id(),
                DataFileEntry {
                    data_file: iceberg_data_file.clone(),
                    // Max number of rows will be initialized when deletion take place.
                    deletion_vector: BatchDeletionVector::new(
                        max_rows.unwrap_or(UNINITIALIZED_BATCH_DELETION_VECTOR_MAX_ROW),
                    ),
                },
            );
            assert!(old_entry.is_none());

            // Insert into local to remote mapping.
            assert!(local_data_files_to_remote
                .insert(
                    local_data_file.file_path().clone(),
                    iceberg_data_file.file_path().to_string(),
                )
                .is_none());

            // Record all imported iceberg data files, with file id unchanged.
            new_remote_data_files.push(create_data_file(
                local_data_file.file_id().0,
                iceberg_data_file.file_path().to_string(),
            ));

            // Record file path to file id mapping.
            assert!(self
                .remote_data_file_to_file_id
                .insert(
                    iceberg_data_file.file_path().to_string(),
                    local_data_file.file_id(),
                )
                .is_none());

            new_iceberg_data_files.push(iceberg_data_file);
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
        })
    }

    /// Dump committed deletion logs into iceberg table, only the changed part will be persisted.
    async fn sync_deletion_vector(
        &mut self,
        new_deletion_logs: HashMap<MooncakeDataFileRef, BatchDeletionVector>,
        file_params: &PersistenceFileParams,
    ) -> IcebergResult<HashMap<FileId, PuffinBlobRef>> {
        let mut puffin_deletion_blobs = HashMap::with_capacity(new_deletion_logs.len());
        for (puffin_index, (data_file, new_deletion_vector)) in
            new_deletion_logs.into_iter().enumerate()
        {
            let mut entry = self
                .persisted_data_files
                .get(&data_file.file_id())
                .unwrap()
                .clone();

            if entry.deletion_vector.get_max_rows() == UNINITIALIZED_BATCH_DELETION_VECTOR_MAX_ROW {
                entry.deletion_vector =
                    BatchDeletionVector::new(new_deletion_vector.get_max_rows());
            }
            entry.deletion_vector.merge_with(&new_deletion_vector);

            // Data filepath in iceberg table.
            let iceberg_data_file = entry.data_file.file_path();
            let puffin_blob = self
                .write_deletion_vector(
                    iceberg_data_file.to_string(),
                    entry.deletion_vector.clone(),
                    file_params,
                    puffin_index as u64,
                )
                .await?;
            let old_entry = self.persisted_data_files.insert(data_file.file_id(), entry);
            assert!(old_entry.is_some());
            puffin_deletion_blobs.insert(data_file.file_id(), puffin_blob);
        }
        Ok(puffin_deletion_blobs)
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
        let mut local_index_file_to_remote = HashMap::new();

        for cur_file_index in file_indices_to_import.iter() {
            // Create one puffin file (with one puffin blob inside of it) for each mooncake file index.
            let puffin_filepath = self.get_unique_hash_index_v1_filepath();
            let mut puffin_writer = puffin_utils::create_puffin_writer(
                self.iceberg_table.as_ref().unwrap().file_io(),
                &puffin_filepath,
            )
            .await?;

            // Upload new index file to iceberg table.
            for cur_index_block in cur_file_index.index_blocks.iter() {
                let remote_index_block = iceberg_io_utils::upload_index_file(
                    self.iceberg_table.as_ref().unwrap(),
                    cur_index_block.index_file.file_path(),
                    self.filesystem_accessor.as_ref(),
                )
                .await?;
                local_index_file_to_remote.insert(
                    cur_index_block.index_file.file_path().to_string(),
                    remote_index_block,
                );
            }
            self.persisted_file_indices
                .insert(cur_file_index.clone(), puffin_filepath.clone());

            // Persist the puffin file and record in file catalog.
            let file_index_blob = FileIndexBlob::new(
                cur_file_index,
                &local_index_file_to_remote,
                local_data_file_to_remote,
            );
            let puffin_blob = file_index_blob.as_blob()?;
            puffin_writer
                .add(puffin_blob, iceberg::puffin::CompressionCodec::None)
                .await?;
            self.catalog
                .record_puffin_metadata_and_close(
                    puffin_filepath,
                    puffin_writer,
                    PuffinBlobType::FileIndex,
                )
                .await?;
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
    /// For implementation simpicity, put everything in a single file and a single blob.
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

    /// Get current manifest entries, used for sanity check purpose.
    async fn get_expected_entry_count_after_sync(
        &self,
        snapshot_payload: &IcebergSnapshotPayload,
    ) -> ManifestEntryCount {
        #[cfg(any(feature = "chaos-test", not(any(test, debug_assertions))))]
        {
            let _ = snapshot_payload; // Suppress unused warning.
            ManifestEntryCount::default()
        }

        #[cfg(all(not(feature = "chaos-test"), any(test, debug_assertions)))]
        {
            let table_metadata = self.iceberg_table.as_ref().unwrap().metadata();
            let file_io = self.iceberg_table.as_ref().unwrap().file_io().clone();
            let mut entries_count =
                manifest_utils::get_manifest_entries_number(table_metadata, file_io).await;

            let new_data_files = snapshot_payload.get_new_data_files();
            let old_data_files = snapshot_payload.get_old_data_files();
            let new_file_indices = snapshot_payload.get_new_file_indices();
            let old_file_indices = snapshot_payload.get_old_file_indices();

            // Update data files count.
            entries_count.data_file_entries += new_data_files.len();
            entries_count.data_file_entries -= old_data_files.len();

            // Update file indices count.
            entries_count.file_indices_entries += new_file_indices.len();
            entries_count.file_indices_entries -= old_file_indices.len();

            // Update deletion vectors count.
            let committed_deletion_logs = &snapshot_payload.committed_deletion_logs;
            let data_file_ids_for_deletion_log = committed_deletion_logs
                .iter()
                .map(|(file_id, _)| *file_id)
                .collect::<HashSet<_>>();
            for cur_file_id in data_file_ids_for_deletion_log.into_iter() {
                if let Some(data_file_entry) = self.persisted_data_files.get(&cur_file_id) {
                    if !data_file_entry.deletion_vector.is_empty() {
                        continue;
                    }
                }
                entries_count.deletion_vector_entries += 1;
            }
            for cur_old_data_file in old_data_files.into_iter() {
                let file_id = cur_old_data_file.file_id();
                if let Some(data_file_entry) = self.persisted_data_files.get(&file_id) {
                    if !data_file_entry.deletion_vector.is_empty() {
                        entries_count.deletion_vector_entries -= 1;
                    }
                }
            }

            entries_count
        }
    }

    /// Get actual entry count after sync.
    async fn get_actual_entry_count_after_sync(&self) -> ManifestEntryCount {
        #[cfg(any(feature = "chaos-test", not(any(test, debug_assertions))))]
        {
            ManifestEntryCount::default()
        }

        #[cfg(all(not(feature = "chaos-test"), any(test, debug_assertions)))]
        {
            let table_metadata = self.iceberg_table.as_ref().unwrap().metadata();
            let file_io = self.iceberg_table.as_ref().unwrap().file_io().clone();
            manifest_utils::get_manifest_entries_number(table_metadata, file_io).await
        }
    }

    pub(crate) async fn sync_snapshot_impl(
        &mut self,
        mut snapshot_payload: IcebergSnapshotPayload,
        file_params: PersistenceFileParams,
    ) -> IcebergResult<PersistenceResult> {
        // Initialize iceberg table on access.
        self.initialize_iceberg_table_for_once().await?;

        // Validate schema consistency before persistence operation.
        self.validate_schema_consistency_at_store().await;

        // Used to validate iceberg persistence status.
        let expected_manifest_entries_after_sync = self
            .get_expected_entry_count_after_sync(&snapshot_payload)
            .await;

        let new_data_files = take_data_files_to_import(&mut snapshot_payload);
        let old_data_files = take_data_files_to_remove(&mut snapshot_payload);
        let new_file_indices = take_file_indices_to_import(&mut snapshot_payload);
        let old_file_indices = take_file_indices_to_remove(&mut snapshot_payload);

        // Persist data files.
        let data_file_import_result = self
            .sync_data_files(
                new_data_files,
                old_data_files,
                &snapshot_payload.import_payload.new_deletion_vector,
            )
            .await?;

        // Persist committed deletion logs.
        let new_deletion_vector =
            std::mem::take(&mut snapshot_payload.import_payload.new_deletion_vector);
        let deletion_puffin_blobs = self
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
        let mut snapshot_properties = HashMap::<String, String>::from([(
            MOONCAKE_TABLE_FLUSH_LSN.to_string(),
            snapshot_payload.flush_lsn.to_string(),
        )]);
        snapshot_properties.insert(
            MOONCAKE_WAL_METADATA.to_string(),
            serde_json::to_string(&snapshot_payload.iceberg_corresponding_wal_metadata).unwrap(),
        );

        let mut txn = Transaction::new(self.iceberg_table.as_ref().unwrap());
        let action = txn.fast_append();
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

        // Commit the transaction.
        let updated_iceberg_table = txn.commit(&*self.catalog).await?;
        self.iceberg_table = Some(updated_iceberg_table);

        self.catalog.clear_puffin_metadata();

        // Get manifest entries status after sync, used to validate iceberg persistence.
        let actual_manifest_entries_after_sync = self.get_actual_entry_count_after_sync().await;
        assert_eq!(
            expected_manifest_entries_after_sync,
            actual_manifest_entries_after_sync
        );

        // NOTICE: persisted data files and file indices are returned in the order of (1) newly imported ones; (2) index merge ones; (3) data compacted ones.
        Ok(PersistenceResult {
            remote_data_files: data_file_import_result.new_remote_data_files,
            puffin_blob_ref: deletion_puffin_blobs,
            remote_file_indices,
        })
    }
}
