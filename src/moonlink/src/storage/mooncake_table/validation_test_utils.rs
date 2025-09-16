use crate::row::MoonlinkRow;
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
/// This module contains testing utils for validation.
use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::puffin_utils;
use crate::storage::mooncake_table::table_creation_test_utils::create_test_filesystem_accessor;
use crate::storage::mooncake_table::table_creation_test_utils::create_test_object_storage_cache;
use crate::storage::mooncake_table::test_utils::*;
use crate::storage::mooncake_table::test_utils_commons::*;
use crate::storage::mooncake_table::DiskFileEntry;
use crate::storage::mooncake_table::Snapshot;
use crate::storage::mooncake_table::TableMetadata;
use crate::storage::storage_utils::FileId;
use crate::storage::storage_utils::RawDeletionRecord;
use crate::storage::storage_utils::RecordLocation;
use crate::IcebergTableConfig;
use crate::IcebergTableManager;
use crate::ObjectStorageCache;
use crate::TableManager;
use iceberg::io::FileIOBuilder;
use moonlink_table_metadata::PositionDelete;
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::TempDir;

/// Test util function to check consistency for snapshot batch deletion vector and deletion puffin blob.
pub(crate) async fn check_deletion_vector_consistency(disk_file_entry: &DiskFileEntry) {
    if disk_file_entry.puffin_deletion_blob.is_none() {
        assert!(disk_file_entry
            .committed_deletion_vector
            .collect_deleted_rows()
            .is_empty());
        return;
    }

    let local_fileio = FileIOBuilder::new_fs_io().build().unwrap();
    let blob = puffin_utils::load_blob_from_puffin_file(
        local_fileio,
        disk_file_entry
            .puffin_deletion_blob
            .as_ref()
            .unwrap()
            .puffin_file_cache_handle
            .get_cache_filepath(),
    )
    .await
    .unwrap();
    let iceberg_deletion_vector = DeletionVector::deserialize(blob).unwrap();
    let batch_deletion_vector = iceberg_deletion_vector.take_as_batch_delete_vector();
    assert_eq!(
        batch_deletion_vector,
        disk_file_entry.committed_deletion_vector
    );
}

/// Test util function to check deletion vector consistency for the given snapshot.
pub(crate) async fn check_deletion_vector_consistency_for_snapshot(snapshot: &Snapshot) {
    for disk_deletion_vector in snapshot.disk_files.values() {
        check_deletion_vector_consistency(disk_deletion_vector).await;
    }
}

/// Test util functions to check recovered snapshot only contains remote filepaths and they do exist.
pub(crate) async fn validate_recovered_snapshot(
    snapshot: &Snapshot,
    warehouse_uri: &str,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) {
    let warehouse_directory = std::path::PathBuf::from(warehouse_uri);
    let mut data_filepaths: HashSet<String> = HashSet::new();

    // Check data files and their puffin blobs.
    for (cur_disk_file, cur_deletion_vector) in snapshot.disk_files.iter() {
        let cur_disk_pathbuf = std::path::PathBuf::from(cur_disk_file.file_path());
        assert!(cur_disk_pathbuf.starts_with(&warehouse_directory));
        assert!(filesystem_accessor
            .object_exists(cur_disk_file.file_path())
            .await
            .unwrap());
        assert!(data_filepaths.insert(cur_disk_file.file_path().clone()));

        if cur_deletion_vector.puffin_deletion_blob.is_none() {
            continue;
        }
        let puffin_filepath = cur_deletion_vector
            .puffin_deletion_blob
            .as_ref()
            .unwrap()
            .puffin_file_cache_handle
            .get_cache_filepath();
        assert!(tokio::fs::try_exists(puffin_filepath).await.unwrap());
    }

    // For append-only table, there's no file indices.
    if snapshot.indices.file_indices.is_empty() {
        return;
    }

    // Check file indices.
    let mut index_referenced_data_filepaths: HashSet<String> = HashSet::new();
    for cur_file_index in snapshot.indices.file_indices.iter() {
        // Check index blocks are imported into the iceberg table.
        // But index blocks are always cached on-disk, so not under warehouse uri.
        for cur_index_block in cur_file_index.index_blocks.iter() {
            // Index blocks are always placed in object storage cache, so mooncake snapshot references to local files.
            let index_pathbuf = std::path::PathBuf::from(&cur_index_block.index_file.file_path());
            assert!(tokio::fs::try_exists(&index_pathbuf).await.unwrap());
        }

        // Check data files referenced by index blocks are imported into iceberg table.
        for cur_data_filepath in cur_file_index.files.iter() {
            let data_file_pathbuf = std::path::PathBuf::from(cur_data_filepath.file_path());
            assert!(data_file_pathbuf.starts_with(&warehouse_directory));
            assert!(filesystem_accessor
                .object_exists(cur_data_filepath.file_path())
                .await
                .unwrap());
            index_referenced_data_filepaths.insert(cur_data_filepath.file_path().clone());
        }
    }

    assert_eq!(index_referenced_data_filepaths, data_filepaths);
}

/// Test util function to check certain data file doesn't exist in non evictable cache.
pub(crate) async fn check_file_not_pinned(
    object_storage_cache: &ObjectStorageCache,
    file_id: FileId,
) {
    let non_evicted_file_ids = object_storage_cache.get_non_evictable_filenames().await;
    let table_unique_file_id = get_unique_table_file_id(file_id);
    assert!(!non_evicted_file_ids.contains(&table_unique_file_id));
}

/// Test util function to check certain data file exists in non evictable cache.
pub(crate) async fn check_file_pinned(object_storage_cache: &ObjectStorageCache, file_id: FileId) {
    let non_evicted_file_ids = object_storage_cache.get_non_evictable_filenames().await;
    let table_unique_file_id = get_unique_table_file_id(file_id);
    assert!(non_evicted_file_ids.contains(&table_unique_file_id));
}

/// Test util function to check the given row doesn't exist in the snapshot indices.
pub(crate) async fn check_row_index_nonexistent(snapshot: &Snapshot, row: &MoonlinkRow) {
    let key = snapshot.metadata.config.row_identity.get_lookup_key(row);
    let locs = snapshot
        .indices
        .find_record(&RawDeletionRecord {
            lookup_key: key,
            row_identity: snapshot
                .metadata
                .config
                .row_identity
                .extract_identity_for_key(row),
            pos: None,
            lsn: 0, // LSN has nothing to do with deletion record search
            delete_if_exists: false,
        })
        .await;
    assert!(
        locs.is_empty(),
        "Deletion record {locs:?} exists for row {row:?}"
    );
}

/// Test util function to check the given row exists in snapshot, and it's on-disk.
pub(crate) async fn check_row_index_on_disk(
    snapshot: &Snapshot,
    row: &MoonlinkRow,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) {
    let key = snapshot.metadata.config.row_identity.get_lookup_key(row);
    let locs = snapshot
        .indices
        .find_record(&RawDeletionRecord {
            lookup_key: key,
            row_identity: snapshot
                .metadata
                .config
                .row_identity
                .extract_identity_for_key(row),
            pos: None,
            lsn: 0, // LSN has nothing to do with deletion record search
            delete_if_exists: false,
        })
        .await;
    assert_eq!(locs.len(), 1, "Actual location for row {row:?} is {locs:?}");
    match &locs[0] {
        RecordLocation::DiskFile(file_id, _) => {
            let filepath = snapshot
                .disk_files
                .get_key_value(&FileId(file_id.0))
                .as_ref()
                .unwrap()
                .0
                .file_path();
            let exists = filesystem_accessor.object_exists(filepath).await.unwrap();
            assert!(exists, "Data file {filepath:?} doesn't exist");
        }
        _ => {
            panic!("Unexpected location {:?}", locs[0]);
        }
    }
}

/// Test util function to validate mooncake snapshot result.
pub(crate) async fn verify_recovered_mooncake_snapshot(snapshot: &Snapshot, expected_ids: &[i32]) {
    let mut position_deletes = vec![];
    let mut data_files = vec![];
    for (file_idx, (cur_data_file, cur_disk_entry)) in snapshot.disk_files.iter().enumerate() {
        data_files.push(cur_data_file.file_path().clone());
        let rows_deleted = cur_disk_entry
            .committed_deletion_vector
            .collect_deleted_rows();
        for row_idx in rows_deleted.into_iter() {
            position_deletes.push(PositionDelete {
                data_file_number: file_idx as u32,
                data_file_row_number: row_idx as u32,
            });
        }
    }
    verify_files_and_deletions(
        &data_files,
        /*puffin_file_paths=*/ &[],
        position_deletes,
        /*deletion_vectors=*/ vec![],
        expected_ids,
    )
    .await;
}

/// Test util function to validate iceberg content.
pub(crate) async fn verify_iceberg_content(
    iceberg_table_config: IcebergTableConfig,
    mooncake_table_metadata: Arc<TableMetadata>,
    cache_temp_dir: &TempDir,
    expected_ids: &[i32],
) {
    let filesystem_accessor = create_test_filesystem_accessor(&iceberg_table_config);
    let mut iceberg_table_manager_for_recovery = IcebergTableManager::new(
        mooncake_table_metadata.clone(),
        create_test_object_storage_cache(cache_temp_dir), // Use separate cache for each table.
        filesystem_accessor.clone(),
        iceberg_table_config.clone(),
    )
    .await
    .unwrap();
    let (_, snapshot) = iceberg_table_manager_for_recovery
        .load_snapshot_from_table()
        .await
        .unwrap();
    verify_recovered_mooncake_snapshot(&snapshot, expected_ids).await;
}
