use crate::storage::compaction::compactor::{CompactionBuilder, CompactionFileParams};
use crate::storage::compaction::table_compaction::{DataCompactionPayload, SingleFileToCompact};
use crate::storage::compaction::test_utils;
use crate::storage::compaction::test_utils::get_record_location_mapping;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::storage_utils::{
    self, get_unique_file_id_for_flush, MooncakeDataFileRef, TableId, TableUniqueFileId,
};
use crate::storage::storage_utils::{FileId, RecordLocation};
use crate::storage::PuffinBlobRef;
use crate::{create_data_file, FileSystemAccessor, ObjectStorageCache};

use std::collections::HashMap;

/// Single compacted file size.
const SINGLE_COMPACTED_DATA_FILE_SIZE: u64 = u64::MAX;
/// File size for multiple compacted files.
/// Since current we don't split ont old uncompacted file into multiple files, setting cut-off flush threshold 1 means each old file leads to one compacted file.
const MULTI_COMPACTED_DATA_FILE_SIZE: u64 = 1;

/// Test constant for test table id.
const TEST_TABLE_ID: TableId = TableId(0);
/// Test util function to get single file to compact.
fn get_single_file_to_compact(
    file: &MooncakeDataFileRef,
    deletion_vector: Option<PuffinBlobRef>,
) -> SingleFileToCompact {
    SingleFileToCompact {
        file_id: TableUniqueFileId {
            table_id: TEST_TABLE_ID,
            file_id: file.file_id(),
        },
        filepath: file.file_path().clone(),
        deletion_vector,
    }
}

/// Test util function to get unique table file id for the given file id.
fn get_table_unique_table_id(file_id: u64) -> TableUniqueFileId {
    TableUniqueFileId {
        table_id: TEST_TABLE_ID,
        file_id: FileId(file_id),
    }
}

/// ============================
/// Compact to single file
/// ============================
///
/// Case-1: single file, no deletion vector.
#[tokio::test]
async fn test_data_file_compaction_1() {
    // Create data file and corresponding file indices.
    let temp_dir = tempfile::tempdir().unwrap();
    let data_file = temp_dir.path().join("test-1.parquet");
    let data_file = create_data_file(/*file_id=*/ 0, data_file.to_str().unwrap().to_string());
    let record_batch = test_utils::create_test_batch_1();
    test_utils::dump_arrow_record_batches(vec![record_batch], data_file.clone()).await;
    let file_index = test_utils::create_file_index_1(
        temp_dir.path().to_path_buf(),
        data_file.clone(),
        /*start_file_id=*/ 1,
    )
    .await;

    // Prepare compaction payload.
    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor: FileSystemAccessor::default_for_test(&temp_dir),
        disk_files: vec![get_single_file_to_compact(
            &data_file, /*deletion_vector=*/ None,
        )],
        file_indices: vec![file_index],
    };
    let table_auto_incr_id: u64 = 2;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_ids: (table_auto_incr_id as u32)..(table_auto_incr_id as u32 + 1),
        data_file_final_size: SINGLE_COMPACTED_DATA_FILE_SIZE,
    };

    // Perform compaction.
    let builder = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params);
    let compaction_result = builder.build().await.unwrap();

    // Check compaction results.
    //
    // Check remap results.
    let compacted_file_id = FileId(get_unique_file_id_for_flush(
        table_auto_incr_id,
        /*file_idx=*/ 0,
    ));
    let expected_remap = test_utils::get_expected_remap_for_one_file(
        compacted_file_id,
        /*deletion_vector=*/ vec![],
    );
    let actual_remap = get_record_location_mapping(&compaction_result.remapped_data_files);
    assert_eq!(actual_remap, expected_remap);

    // Check file indice compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.new_file_indices.as_slice(),
        /*expected_file_id=*/ Some(compacted_file_id),
        /*old_row_indices=*/ vec![0, 1, 2],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.new_data_files,
        /*old_row_indices=*/ vec![0, 1, 2],
    )
    .await;
}

/// Case-2: single file, with deletion vector, and there're row left after deletion.
#[tokio::test]
async fn test_data_file_compaction_2() {
    // Create data file and file indices.
    let temp_dir = tempfile::tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let data_file = temp_dir.path().join("test-1.parquet");

    let data_file = create_data_file(/*file_id=*/ 0, data_file.to_str().unwrap().to_string());
    let record_batch = test_utils::create_test_batch_1();
    test_utils::dump_arrow_record_batches(vec![record_batch], data_file.clone()).await;
    let file_index = test_utils::create_file_index_1(
        temp_dir.path().to_path_buf(),
        data_file.clone(),
        /*start_file_id=*/ 1,
    )
    .await;

    // Create deletion vector puffin file.
    let puffin_filepath = temp_dir.path().join("deletion-vector-1.bin");
    let mut batch_deletion_vector = BatchDeletionVector::new(/*max_rows=*/ 3);
    assert!(batch_deletion_vector.delete_row(1));
    let puffin_blob_ref = test_utils::dump_deletion_vector_puffin(
        data_file.file_path().clone(),
        puffin_filepath.to_str().unwrap().to_string(),
        batch_deletion_vector,
        object_storage_cache.clone(),
        filesystem_accessor.as_ref(),
        get_table_unique_table_id(/*file_id=*/ 1),
    )
    .await;

    // Prepare compaction payload.
    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor: filesystem_accessor.clone(),
        disk_files: vec![get_single_file_to_compact(
            &data_file,
            Some(puffin_blob_ref),
        )],
        file_indices: vec![file_index.clone()],
    };
    let table_auto_incr_id: u64 = 2;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_ids: (table_auto_incr_id as u32)..(table_auto_incr_id as u32 + 1),
        data_file_final_size: SINGLE_COMPACTED_DATA_FILE_SIZE,
    };

    // Perform compaction.
    let builder = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params);
    let compaction_result = builder.build().await.unwrap();

    // Check compaction results.
    //
    // Check remap results.
    let compacted_file_id = FileId(get_unique_file_id_for_flush(
        table_auto_incr_id,
        /*file_idx=*/ 0,
    ));
    let expected_remap = test_utils::get_expected_remap_for_one_file(
        compacted_file_id,
        /*deletion_vector=*/ vec![1],
    );
    let actual_remap = get_record_location_mapping(&compaction_result.remapped_data_files);
    assert_eq!(actual_remap, expected_remap);

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.new_file_indices.as_slice(),
        /*expected_file_id=*/ Some(compacted_file_id),
        /*old_row_indices=*/ vec![0, 2],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.new_data_files,
        /*old_row_indices=*/ vec![0, 2],
    )
    .await;
}

/// Case-3: single file, with deletion vector, and no rows left.
#[tokio::test]
async fn test_data_file_compaction_3() {
    // Create data file.
    let temp_dir = tempfile::tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let data_file = temp_dir.path().join("test-1.parquet");

    // Create data file and file indices.
    let data_file = create_data_file(/*file_id=*/ 0, data_file.to_str().unwrap().to_string());
    let record_batch = test_utils::create_test_batch_1();
    test_utils::dump_arrow_record_batches(vec![record_batch], data_file.clone()).await;
    let file_index = test_utils::create_file_index_1(
        temp_dir.path().to_path_buf(),
        data_file.clone(),
        /*start_file_id=*/ 1,
    )
    .await;

    // Create deletion vector puffin file.
    let puffin_filepath = temp_dir.path().join("deletion-vector-1.bin");
    let mut batch_deletion_vector = BatchDeletionVector::new(/*max_rows=*/ 3);
    assert!(batch_deletion_vector.delete_row(0));
    assert!(batch_deletion_vector.delete_row(1));
    assert!(batch_deletion_vector.delete_row(2));
    let puffin_blob_ref = test_utils::dump_deletion_vector_puffin(
        data_file.file_path().clone(),
        puffin_filepath.to_str().unwrap().to_string(),
        batch_deletion_vector,
        object_storage_cache.clone(),
        filesystem_accessor.as_ref(),
        get_table_unique_table_id(/*file_id=*/ 1),
    )
    .await;

    // Prepare compaction payload.
    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor: filesystem_accessor.clone(),
        disk_files: vec![get_single_file_to_compact(
            &data_file,
            Some(puffin_blob_ref),
        )],
        file_indices: vec![file_index.clone()],
    };
    let table_auto_incr_id: u64 = 2;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_ids: (table_auto_incr_id as u32)..(table_auto_incr_id as u32 + 1),
        data_file_final_size: SINGLE_COMPACTED_DATA_FILE_SIZE,
    };

    // Check compaction results.
    //
    // Check remap results.
    let builder = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params);
    let compaction_result = builder.build().await.unwrap();

    // Check remap results.
    assert!(compaction_result.remapped_data_files.is_empty());

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.new_file_indices.as_slice(),
        /*expected_file_id=*/ None,
        /*old_row_indices=*/ vec![],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.new_data_files,
        /*old_row_indices=*/ vec![],
    )
    .await;
}

/// ============================
/// Compact with two files
/// ============================
///
/// Case-4: two files, no deletion vector.
#[tokio::test]
async fn test_data_file_compaction_4() {
    // Create data files and file indices.
    let temp_dir = tempfile::tempdir().unwrap();
    let data_file_1 = temp_dir.path().join("test-1.parquet");
    let data_file_2 = temp_dir.path().join("test-2.parquet");

    let data_file_1 = create_data_file(
        /*file_id=*/ 0,
        data_file_1.to_str().unwrap().to_string(),
    );
    let data_file_2 = create_data_file(
        /*file_id=*/ 1,
        data_file_2.to_str().unwrap().to_string(),
    );
    let record_batch_1 = test_utils::create_test_batch_1();
    let record_batch_2 = test_utils::create_test_batch_2();
    test_utils::dump_arrow_record_batches(vec![record_batch_1], data_file_1.clone()).await;
    test_utils::dump_arrow_record_batches(vec![record_batch_2], data_file_2.clone()).await;

    let file_index_1 = test_utils::create_file_index_1(
        temp_dir.path().to_path_buf(),
        data_file_1.clone(),
        /*start_file_id=*/ 2,
    )
    .await;
    let file_index_2 = test_utils::create_file_index_2(
        temp_dir.path().to_path_buf(),
        data_file_2.clone(),
        /*start_file_id=*/ 3,
    )
    .await;

    // Prepare compaction payload.
    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor: FileSystemAccessor::default_for_test(&temp_dir),
        disk_files: vec![
            get_single_file_to_compact(&data_file_1, /*deletion_vector=*/ None),
            get_single_file_to_compact(&data_file_2, /*deletion_vector=*/ None),
        ],
        file_indices: vec![file_index_1.clone(), file_index_2.clone()],
    };
    let table_auto_incr_id: u64 = 4;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_ids: (table_auto_incr_id as u32)..(table_auto_incr_id as u32 + 1),
        data_file_final_size: SINGLE_COMPACTED_DATA_FILE_SIZE,
    };

    // Perform compaction.
    let builder = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params);
    let compaction_result = builder.build().await.unwrap();

    // Check compaction results.
    //
    // Check remap results.
    let compacted_file_id = FileId(get_unique_file_id_for_flush(
        table_auto_incr_id,
        /*file_idx=*/ 0,
    ));
    let expected_remap = test_utils::get_expected_remap_for_two_files(
        compacted_file_id,
        /*deletion_vectors=*/ vec![vec![], vec![]],
    );
    let actual_remap = get_record_location_mapping(&compaction_result.remapped_data_files);
    assert_eq!(expected_remap, actual_remap);

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.new_file_indices.as_slice(),
        /*expected_file_id=*/ Some(compacted_file_id),
        /*old_row_indices=*/ (0..6).collect(),
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.new_data_files,
        /*old_row_indices=*/ (0..6).collect(),
    )
    .await;
}

/// Case-5: two files, each with deletion vector and partially deleted.
#[tokio::test]
async fn test_data_file_compaction_5() {
    // Create data file.
    let temp_dir = tempfile::tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let data_file_1 = temp_dir.path().join("test-1.parquet");
    let data_file_2 = temp_dir.path().join("test-2.parquet");

    let data_file_1 = create_data_file(
        /*file_id=*/ 0,
        data_file_1.to_str().unwrap().to_string(),
    );
    let data_file_2 = create_data_file(
        /*file_id=*/ 1,
        data_file_2.to_str().unwrap().to_string(),
    );
    let record_batch_1 = test_utils::create_test_batch_1();
    let record_batch_2 = test_utils::create_test_batch_2();
    test_utils::dump_arrow_record_batches(vec![record_batch_1], data_file_1.clone()).await;
    test_utils::dump_arrow_record_batches(vec![record_batch_2], data_file_2.clone()).await;

    let file_index_1 = test_utils::create_file_index_1(
        temp_dir.path().to_path_buf(),
        data_file_1.clone(),
        /*start_file_id=*/ 2,
    )
    .await;
    let file_index_2 = test_utils::create_file_index_2(
        temp_dir.path().to_path_buf(),
        data_file_2.clone(),
        /*start_file_id=*/ 3,
    )
    .await;

    // Create deletion vector puffin file.
    let puffin_filepath_1 = temp_dir.path().join("deletion-vector-1.bin");
    let mut batch_deletion_vector_1 = BatchDeletionVector::new(/*max_rows=*/ 3);
    assert!(batch_deletion_vector_1.delete_row(1));
    let puffin_blob_ref_1 = test_utils::dump_deletion_vector_puffin(
        data_file_1.file_path().clone(),
        puffin_filepath_1.to_str().unwrap().to_string(),
        batch_deletion_vector_1,
        object_storage_cache.clone(),
        filesystem_accessor.as_ref(),
        get_table_unique_table_id(/*file_id=*/ 2),
    )
    .await;

    let puffin_filepath_2 = temp_dir.path().join("deletion-vector-2.bin");
    let mut batch_deletion_vector_2 = BatchDeletionVector::new(/*max_rows=*/ 3);
    assert!(batch_deletion_vector_2.delete_row(0));
    assert!(batch_deletion_vector_2.delete_row(2));
    let puffin_blob_ref_2 = test_utils::dump_deletion_vector_puffin(
        data_file_2.file_path().clone(),
        puffin_filepath_2.to_str().unwrap().to_string(),
        batch_deletion_vector_2,
        object_storage_cache.clone(),
        filesystem_accessor.as_ref(),
        get_table_unique_table_id(/*file_id=*/ 3),
    )
    .await;

    // Prepare compaction payload.
    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor: filesystem_accessor.clone(),
        disk_files: vec![
            get_single_file_to_compact(&data_file_1, Some(puffin_blob_ref_1)),
            get_single_file_to_compact(&data_file_2, Some(puffin_blob_ref_2)),
        ],
        file_indices: vec![file_index_1.clone(), file_index_2.clone()],
    };
    let table_auto_incr_id: u64 = 4;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_ids: (table_auto_incr_id as u32)..(table_auto_incr_id as u32 + 1),
        data_file_final_size: SINGLE_COMPACTED_DATA_FILE_SIZE,
    };

    // Perform compaction.
    let builder = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params);
    let compaction_result = builder.build().await.unwrap();

    // Check compaction results.
    //
    // Check remap results.
    let compacted_file_id = FileId(get_unique_file_id_for_flush(
        table_auto_incr_id,
        /*file_idx=*/ 0,
    ));
    let expected_remap = test_utils::get_expected_remap_for_two_files(
        compacted_file_id,
        /*deletion_vectors=*/
        vec![
            vec![1],    // deletion vector for the first data file
            vec![0, 2], // deletion vector the second data file
        ],
    );
    let actual_remap = get_record_location_mapping(&compaction_result.remapped_data_files);
    assert_eq!(expected_remap, actual_remap);

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.new_file_indices.as_slice(),
        /*expected_file_id=*/ Some(compacted_file_id),
        /*old_row_indices=*/ vec![0, 2, 4],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.new_data_files,
        /*old_row_indices=*/ vec![0, 2, 4],
    )
    .await;
}

/// Case-6: two files, and all rows deleted.
#[tokio::test]
async fn test_data_file_compaction_6() {
    // Create data file.
    let temp_dir = tempfile::tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let data_file_1 = temp_dir.path().join("test-1.parquet");
    let data_file_2 = temp_dir.path().join("test-2.parquet");

    let data_file_1 = create_data_file(
        /*file_id=*/ 0,
        data_file_1.to_str().unwrap().to_string(),
    );
    let data_file_2 = create_data_file(
        /*file_id=*/ 1,
        data_file_2.to_str().unwrap().to_string(),
    );
    let record_batch_1 = test_utils::create_test_batch_1();
    let record_batch_2 = test_utils::create_test_batch_2();
    test_utils::dump_arrow_record_batches(vec![record_batch_1], data_file_1.clone()).await;
    test_utils::dump_arrow_record_batches(vec![record_batch_2], data_file_2.clone()).await;

    let file_index_1 = test_utils::create_file_index_1(
        temp_dir.path().to_path_buf(),
        data_file_1.clone(),
        /*start_file_id=*/ 2,
    )
    .await;
    let file_index_2 = test_utils::create_file_index_2(
        temp_dir.path().to_path_buf(),
        data_file_2.clone(),
        /*start_file_id=*/ 3,
    )
    .await;

    // Create deletion vector puffin file.
    let puffin_filepath_1 = temp_dir.path().join("deletion-vector-1.bin");
    let mut batch_deletion_vector_1 = BatchDeletionVector::new(/*max_rows=*/ 3);
    assert!(batch_deletion_vector_1.delete_row(0));
    assert!(batch_deletion_vector_1.delete_row(1));
    assert!(batch_deletion_vector_1.delete_row(2));
    let puffin_blob_ref_1 = test_utils::dump_deletion_vector_puffin(
        data_file_1.file_path().clone(),
        puffin_filepath_1.to_str().unwrap().to_string(),
        batch_deletion_vector_1,
        object_storage_cache.clone(),
        filesystem_accessor.as_ref(),
        get_table_unique_table_id(/*file_id=*/ 2),
    )
    .await;

    let puffin_filepath_2 = temp_dir.path().join("deletion-vector-2.bin");
    let mut batch_deletion_vector_2 = BatchDeletionVector::new(/*max_rows=*/ 3);
    assert!(batch_deletion_vector_2.delete_row(0));
    assert!(batch_deletion_vector_2.delete_row(1));
    assert!(batch_deletion_vector_2.delete_row(2));
    let puffin_blob_ref_2 = test_utils::dump_deletion_vector_puffin(
        data_file_2.file_path().clone(),
        puffin_filepath_2.to_str().unwrap().to_string(),
        batch_deletion_vector_2,
        object_storage_cache.clone(),
        filesystem_accessor.as_ref(),
        get_table_unique_table_id(/*file_id=*/ 3),
    )
    .await;

    // Prepare compaction payload.
    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor: filesystem_accessor.clone(),
        disk_files: vec![
            get_single_file_to_compact(&data_file_1, Some(puffin_blob_ref_1)),
            get_single_file_to_compact(&data_file_2, Some(puffin_blob_ref_2)),
        ],
        file_indices: vec![file_index_1.clone(), file_index_2.clone()],
    };
    let table_auto_incr_id: u64 = 4;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_ids: (table_auto_incr_id as u32)..(table_auto_incr_id as u32 + 1),
        data_file_final_size: SINGLE_COMPACTED_DATA_FILE_SIZE,
    };

    // Check compaction results.
    //
    // Check remap results.
    let builder = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params);
    let compaction_result = builder.build().await.unwrap();

    // Check remap results.
    assert!(compaction_result.remapped_data_files.is_empty());

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.new_file_indices.as_slice(),
        /*expected_file_id=*/ None,
        /*old_row_indices=*/ vec![],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.new_data_files,
        /*old_row_indices=*/ vec![],
    )
    .await;
}

/// ============================
/// Compact one file with multiple record batches
/// ============================
///
/// Case-7: one file with two record batches, first record batch completely deleted, and second record batch partially deleted.
#[tokio::test]
async fn test_data_file_compaction_7() {
    // Create data file.
    let temp_dir = tempfile::tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let data_file = temp_dir.path().join("test-1.parquet");

    let data_file = create_data_file(/*file_id=*/ 0, data_file.to_str().unwrap().to_string());
    let record_batch_1 = test_utils::create_test_batch_1();
    let record_batch_2 = test_utils::create_test_batch_2();
    test_utils::dump_arrow_record_batches(vec![record_batch_1, record_batch_2], data_file.clone())
        .await;

    let file_index = test_utils::create_file_index_for_both_batches(
        temp_dir.path().to_path_buf(),
        data_file.clone(),
        /*start_file_id=*/ 2,
    )
    .await;

    // Create deletion vector puffin file.
    let puffin_filepath = temp_dir.path().join("deletion-vector-1.bin");
    let mut batch_deletion_vector = BatchDeletionVector::new(/*max_rows=*/ 6);
    // Deletion record for the first record batch.
    assert!(batch_deletion_vector.delete_row(0));
    assert!(batch_deletion_vector.delete_row(1));
    assert!(batch_deletion_vector.delete_row(2));
    // Deletion record for the second record batch.
    assert!(batch_deletion_vector.delete_row(3));
    assert!(batch_deletion_vector.delete_row(5));
    // Dump deletion records to puffin blob.
    let puffin_blob_ref = test_utils::dump_deletion_vector_puffin(
        data_file.file_path().clone(),
        puffin_filepath.to_str().unwrap().to_string(),
        batch_deletion_vector,
        object_storage_cache.clone(),
        filesystem_accessor.as_ref(),
        get_table_unique_table_id(/*file_id=*/ 2),
    )
    .await;

    // Prepare compaction payload.
    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor: filesystem_accessor.clone(),
        disk_files: vec![get_single_file_to_compact(
            &data_file,
            Some(puffin_blob_ref),
        )],
        file_indices: vec![file_index.clone()],
    };
    let table_auto_incr_id: u64 = 4;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_ids: (table_auto_incr_id as u32)..(table_auto_incr_id as u32 + 1),
        data_file_final_size: SINGLE_COMPACTED_DATA_FILE_SIZE,
    };

    // Perform compaction.
    let builder = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params);
    let compaction_result = builder.build().await.unwrap();

    // Check compaction results.
    //
    // Check remap results.
    let compacted_file_id = FileId(get_unique_file_id_for_flush(
        table_auto_incr_id,
        /*file_idx=*/ 0,
    ));
    let expected_remap = test_utils::get_expected_remap_for_two_batches_in_one_file(
        compacted_file_id,
        /*deletion_vectors=*/ vec![0, 1, 2, 3, 5],
    );
    let actual_remap = get_record_location_mapping(&compaction_result.remapped_data_files);
    assert_eq!(expected_remap, actual_remap);

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.new_file_indices.as_slice(),
        /*expected_file_id=*/ Some(compacted_file_id),
        /*old_row_indices=*/ vec![4],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.new_data_files,
        /*old_row_indices=*/ vec![4],
    )
    .await;
}

/// Case-8: one file with two record batches, either doesn't have deleted rows.
#[tokio::test]
async fn test_data_file_compaction_8() {
    // Create data file.
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let data_file = temp_dir.path().join("test-1.parquet");

    let data_file = create_data_file(/*file_id=*/ 0, data_file.to_str().unwrap().to_string());
    let record_batch_1 = test_utils::create_test_batch_1();
    let record_batch_2 = test_utils::create_test_batch_2();
    test_utils::dump_arrow_record_batches(vec![record_batch_1, record_batch_2], data_file.clone())
        .await;

    let file_index = test_utils::create_file_index_for_both_batches(
        temp_dir.path().to_path_buf(),
        data_file.clone(),
        /*start_file_id=*/ 2,
    )
    .await;

    // Prepare compaction payload.
    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor: filesystem_accessor.clone(),
        disk_files: vec![get_single_file_to_compact(
            &data_file, /*deletion_vector=*/ None,
        )],
        file_indices: vec![file_index.clone()],
    };
    let table_auto_incr_id: u64 = 4;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_ids: (table_auto_incr_id as u32)..(table_auto_incr_id as u32 + 1),
        data_file_final_size: SINGLE_COMPACTED_DATA_FILE_SIZE,
    };

    // Perform compaction.
    let builder = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params);
    let compaction_result = builder.build().await.unwrap();

    // Check compaction results.
    //
    // Check remap results.
    let compacted_file_id = FileId(get_unique_file_id_for_flush(
        table_auto_incr_id,
        /*file_idx=*/ 0,
    ));
    let expected_remap = test_utils::get_expected_remap_for_two_batches_in_one_file(
        compacted_file_id,
        /*deletion_vectors=*/ vec![],
    );
    let actual_remap = get_record_location_mapping(&compaction_result.remapped_data_files);
    assert_eq!(expected_remap, actual_remap);

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.new_file_indices.as_slice(),
        /*expected_file_id=*/ Some(compacted_file_id),
        /*old_row_indices=*/ vec![0, 1, 2, 3, 4, 5],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.new_data_files,
        /*old_row_indices=*/ vec![0, 1, 2, 3, 4, 5],
    )
    .await;
}

/// Case-9: one file with two record batches, first one partially deleted, second one completed deleted.
#[tokio::test]
async fn test_data_file_compaction_9() {
    // Create data file.
    let temp_dir = tempfile::tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let data_file = temp_dir.path().join("test-1.parquet");

    let data_file = create_data_file(/*file_id=*/ 0, data_file.to_str().unwrap().to_string());
    let record_batch_1 = test_utils::create_test_batch_1();
    let record_batch_2 = test_utils::create_test_batch_2();
    test_utils::dump_arrow_record_batches(vec![record_batch_1, record_batch_2], data_file.clone())
        .await;

    let file_index = test_utils::create_file_index_for_both_batches(
        temp_dir.path().to_path_buf(),
        data_file.clone(),
        /*start_file_id=*/ 2,
    )
    .await;

    // Create deletion vector puffin file.
    let puffin_filepath = temp_dir.path().join("deletion-vector-1.bin");
    let mut batch_deletion_vector = BatchDeletionVector::new(/*max_rows=*/ 6);
    // Deletion record for the first record batch.
    assert!(batch_deletion_vector.delete_row(0));
    assert!(batch_deletion_vector.delete_row(2));
    // Deletion record for the second record batch.
    assert!(batch_deletion_vector.delete_row(3));
    assert!(batch_deletion_vector.delete_row(4));
    assert!(batch_deletion_vector.delete_row(5));
    // Dump deletion records to puffin blob.
    let puffin_blob_ref = test_utils::dump_deletion_vector_puffin(
        data_file.file_path().clone(),
        puffin_filepath.to_str().unwrap().to_string(),
        batch_deletion_vector,
        object_storage_cache.clone(),
        filesystem_accessor.as_ref(),
        get_table_unique_table_id(/*file_id=*/ 2),
    )
    .await;

    // Prepare compaction payload.
    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor: filesystem_accessor.clone(),
        disk_files: vec![get_single_file_to_compact(
            &data_file,
            /*deletion_vector=*/ Some(puffin_blob_ref),
        )],
        file_indices: vec![file_index.clone()],
    };
    let table_auto_incr_id: u64 = 4;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_ids: (table_auto_incr_id as u32)..(table_auto_incr_id as u32 + 1),
        data_file_final_size: SINGLE_COMPACTED_DATA_FILE_SIZE,
    };

    // Perform compaction.
    let builder = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params);
    let compaction_result = builder.build().await.unwrap();

    // Check compaction results.
    //
    // Check remap results.
    let compacted_file_id = FileId(get_unique_file_id_for_flush(
        table_auto_incr_id,
        /*file_idx=*/ 0,
    ));
    let expected_remap = test_utils::get_expected_remap_for_two_batches_in_one_file(
        compacted_file_id,
        /*deletion_vectors=*/ vec![0, 2, 3, 4, 5],
    );
    let actual_remap = get_record_location_mapping(&compaction_result.remapped_data_files);
    assert_eq!(expected_remap, actual_remap);

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.new_file_indices.as_slice(),
        /*expected_file_id=*/ Some(compacted_file_id),
        /*old_row_indices=*/ vec![1],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.new_data_files,
        /*old_row_indices=*/ vec![1],
    )
    .await;
}

/// Case-10: one file with two record batches, all rows deleted in both record batches.
#[tokio::test]
async fn test_data_file_compaction_10() {
    // Create data file.
    let temp_dir = tempfile::tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let data_file = temp_dir.path().join("test-1.parquet");

    let data_file = create_data_file(/*file_id=*/ 0, data_file.to_str().unwrap().to_string());
    let record_batch_1 = test_utils::create_test_batch_1();
    let record_batch_2 = test_utils::create_test_batch_2();
    test_utils::dump_arrow_record_batches(vec![record_batch_1, record_batch_2], data_file.clone())
        .await;

    let file_index = test_utils::create_file_index_for_both_batches(
        temp_dir.path().to_path_buf(),
        data_file.clone(),
        /*start_file_id=*/ 2,
    )
    .await;

    // Create deletion vector puffin file.
    let puffin_filepath = temp_dir.path().join("deletion-vector-1.bin");
    let mut batch_deletion_vector = BatchDeletionVector::new(/*max_rows=*/ 6);
    // Deletion record for the first record batch.
    assert!(batch_deletion_vector.delete_row(0));
    assert!(batch_deletion_vector.delete_row(1));
    assert!(batch_deletion_vector.delete_row(2));
    // Deletion record for the second record batch.
    assert!(batch_deletion_vector.delete_row(3));
    assert!(batch_deletion_vector.delete_row(4));
    assert!(batch_deletion_vector.delete_row(5));
    // Dump deletion records to puffin blob.
    let puffin_blob_ref = test_utils::dump_deletion_vector_puffin(
        data_file.file_path().clone(),
        puffin_filepath.to_str().unwrap().to_string(),
        batch_deletion_vector,
        object_storage_cache.clone(),
        filesystem_accessor.as_ref(),
        get_table_unique_table_id(/*file_id=*/ 2),
    )
    .await;

    // Prepare compaction payload.
    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor: filesystem_accessor.clone(),
        disk_files: vec![get_single_file_to_compact(
            &data_file,
            /*deletion_vector=*/ Some(puffin_blob_ref),
        )],
        file_indices: vec![file_index.clone()],
    };
    let table_auto_incr_id: u64 = 4;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_ids: (table_auto_incr_id as u32)..(table_auto_incr_id as u32 + 1),
        data_file_final_size: SINGLE_COMPACTED_DATA_FILE_SIZE,
    };

    // Perform compaction.
    let builder = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params);
    let compaction_result = builder.build().await.unwrap();

    // Check compaction results.
    //
    // Check remap results.
    let compacted_file_id = FileId(get_unique_file_id_for_flush(
        table_auto_incr_id,
        /*file_idx=*/ 0,
    ));
    let expected_remap = test_utils::get_expected_remap_for_two_batches_in_one_file(
        compacted_file_id,
        /*deletion_vectors=*/ vec![0, 1, 2, 3, 4, 5],
    );
    let actual_remap = get_record_location_mapping(&compaction_result.remapped_data_files);
    assert_eq!(expected_remap, actual_remap);

    // Check file indices compaction.
    test_utils::check_file_indices_compaction(
        compaction_result.new_file_indices.as_slice(),
        /*expected_file_id=*/ Some(compacted_file_id),
        /*old_row_indices=*/ vec![],
    )
    .await;

    // Check data file compaction.
    test_utils::check_data_file_compaction(
        compaction_result.new_data_files,
        /*old_row_indices=*/ vec![],
    )
    .await;
}

/// ============================
/// Compact to multiple files
/// ============================
///
/// Case-1: there're deleted records in each data file.
#[tokio::test]
async fn test_multiple_compacted_data_files_1() {
    // Create data file.
    let temp_dir = tempfile::tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let data_file_1 = temp_dir.path().join("test-1.parquet");
    let data_file_2 = temp_dir.path().join("test-2.parquet");

    let data_file_1 = create_data_file(
        /*file_id=*/ 0,
        data_file_1.to_str().unwrap().to_string(),
    );
    let data_file_2 = create_data_file(
        /*file_id=*/ 1,
        data_file_2.to_str().unwrap().to_string(),
    );
    let record_batch_1 = test_utils::create_test_batch_1();
    let record_batch_2 = test_utils::create_test_batch_2();
    test_utils::dump_arrow_record_batches(vec![record_batch_1], data_file_1.clone()).await;
    test_utils::dump_arrow_record_batches(vec![record_batch_2], data_file_2.clone()).await;

    let file_index_1 = test_utils::create_file_index_1(
        temp_dir.path().to_path_buf(),
        data_file_1.clone(),
        /*start_file_id=*/ 2,
    )
    .await;
    let file_index_2 = test_utils::create_file_index_2(
        temp_dir.path().to_path_buf(),
        data_file_2.clone(),
        /*start_file_id=*/ 3,
    )
    .await;

    // Create deletion vector puffin file.
    let puffin_filepath_1 = temp_dir.path().join("deletion-vector-1.bin");
    let mut batch_deletion_vector_1 = BatchDeletionVector::new(/*max_rows=*/ 3);
    assert!(batch_deletion_vector_1.delete_row(1));
    let puffin_blob_ref_1 = test_utils::dump_deletion_vector_puffin(
        data_file_1.file_path().clone(),
        puffin_filepath_1.to_str().unwrap().to_string(),
        batch_deletion_vector_1,
        object_storage_cache.clone(),
        filesystem_accessor.as_ref(),
        get_table_unique_table_id(/*file_id=*/ 2),
    )
    .await;

    let puffin_filepath_2 = temp_dir.path().join("deletion-vector-2.bin");
    let mut batch_deletion_vector_2 = BatchDeletionVector::new(/*max_rows=*/ 3);
    assert!(batch_deletion_vector_2.delete_row(0));
    assert!(batch_deletion_vector_2.delete_row(2));
    let puffin_blob_ref_2 = test_utils::dump_deletion_vector_puffin(
        data_file_2.file_path().clone(),
        puffin_filepath_2.to_str().unwrap().to_string(),
        batch_deletion_vector_2,
        object_storage_cache.clone(),
        filesystem_accessor.as_ref(),
        get_table_unique_table_id(/*file_id=*/ 3),
    )
    .await;

    // Prepare compaction payload.
    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor: filesystem_accessor.clone(),
        disk_files: vec![
            get_single_file_to_compact(&data_file_1, Some(puffin_blob_ref_1)),
            get_single_file_to_compact(&data_file_2, Some(puffin_blob_ref_2)),
        ],
        file_indices: vec![file_index_1.clone(), file_index_2.clone()],
    };
    let table_auto_incr_id: u64 = 4;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_ids: (table_auto_incr_id as u32)..(table_auto_incr_id as u32 + 4),
        data_file_final_size: MULTI_COMPACTED_DATA_FILE_SIZE,
    };

    // Perform compaction.
    let builder = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params);
    let compaction_result = builder.build().await.unwrap();

    let old_file_id_1 = FileId(0);
    let old_file_id_2 = FileId(1);
    let new_file_id_1 = FileId(get_unique_file_id_for_flush(table_auto_incr_id, 0));
    let new_file_id_2 = FileId(get_unique_file_id_for_flush(table_auto_incr_id, 1));

    // Check compaction results.
    //
    // Check remap results.
    let expected_remap = HashMap::<RecordLocation, RecordLocation>::from([
        (
            RecordLocation::DiskFile(old_file_id_1, 0),
            RecordLocation::DiskFile(new_file_id_1, 0),
        ),
        (
            RecordLocation::DiskFile(old_file_id_1, 2),
            RecordLocation::DiskFile(new_file_id_1, 1),
        ),
        (
            RecordLocation::DiskFile(old_file_id_2, 1),
            (RecordLocation::DiskFile(new_file_id_2, 0)),
        ),
    ]);
    let actual_remap = get_record_location_mapping(&compaction_result.remapped_data_files);
    assert_eq!(expected_remap, actual_remap);

    // Check file indices compaction.
    let expected_record_locations = vec![
        (new_file_id_1, /*row_idx=*/ 0),
        (new_file_id_1, /*row_idx=*/ 1),
        (new_file_id_2, /*row_idx=*/ 0),
    ];

    test_utils::check_file_indices_compaction_for_multiple_compacted_files(
        compaction_result.new_file_indices.as_slice(),
        expected_record_locations,
        /*old_row_indices=*/ vec![0, 2, 4],
    )
    .await;

    // Check data file compaction.
    test_utils::check_compacted_single_data_files(
        compaction_result.new_data_files,
        /*old_row_indices=*/ vec![vec![0, 2], vec![4]],
    )
    .await;
}

/// Case-2: all rows have been deleted for both files.
#[tokio::test]
async fn test_multiple_compacted_data_files_2() {
    // Create data file.
    let temp_dir = tempfile::tempdir().unwrap();
    let object_storage_cache = ObjectStorageCache::default_for_test(&temp_dir);
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let data_file_1 = temp_dir.path().join("test-1.parquet");
    let data_file_2 = temp_dir.path().join("test-2.parquet");

    let data_file_1 = create_data_file(
        /*file_id=*/ 0,
        data_file_1.to_str().unwrap().to_string(),
    );
    let data_file_2 = create_data_file(
        /*file_id=*/ 1,
        data_file_2.to_str().unwrap().to_string(),
    );
    let record_batch_1 = test_utils::create_test_batch_1();
    let record_batch_2 = test_utils::create_test_batch_2();
    test_utils::dump_arrow_record_batches(vec![record_batch_1], data_file_1.clone()).await;
    test_utils::dump_arrow_record_batches(vec![record_batch_2], data_file_2.clone()).await;

    let file_index_1 = test_utils::create_file_index_1(
        temp_dir.path().to_path_buf(),
        data_file_1.clone(),
        /*start_file_id=*/ 2,
    )
    .await;
    let file_index_2 = test_utils::create_file_index_2(
        temp_dir.path().to_path_buf(),
        data_file_2.clone(),
        /*start_file_id=*/ 3,
    )
    .await;

    // Create deletion vector puffin file.
    let puffin_filepath_1 = temp_dir.path().join("deletion-vector-1.bin");
    let mut batch_deletion_vector_1 = BatchDeletionVector::new(/*max_rows=*/ 3);
    assert!(batch_deletion_vector_1.delete_row(0));
    assert!(batch_deletion_vector_1.delete_row(1));
    assert!(batch_deletion_vector_1.delete_row(2));
    let puffin_blob_ref_1 = test_utils::dump_deletion_vector_puffin(
        data_file_1.file_path().clone(),
        puffin_filepath_1.to_str().unwrap().to_string(),
        batch_deletion_vector_1,
        object_storage_cache.clone(),
        filesystem_accessor.as_ref(),
        get_table_unique_table_id(/*file_id=*/ 2),
    )
    .await;

    let puffin_filepath_2 = temp_dir.path().join("deletion-vector-2.bin");
    let mut batch_deletion_vector_2 = BatchDeletionVector::new(/*max_rows=*/ 3);
    assert!(batch_deletion_vector_2.delete_row(0));
    assert!(batch_deletion_vector_2.delete_row(1));
    assert!(batch_deletion_vector_2.delete_row(2));
    let puffin_blob_ref_2 = test_utils::dump_deletion_vector_puffin(
        data_file_2.file_path().clone(),
        puffin_filepath_2.to_str().unwrap().to_string(),
        batch_deletion_vector_2,
        object_storage_cache.clone(),
        filesystem_accessor.as_ref(),
        get_table_unique_table_id(/*file_id=*/ 3),
    )
    .await;

    // Prepare compaction payload.
    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor: filesystem_accessor.clone(),
        disk_files: vec![
            get_single_file_to_compact(&data_file_1, Some(puffin_blob_ref_1)),
            get_single_file_to_compact(&data_file_2, Some(puffin_blob_ref_2)),
        ],
        file_indices: vec![file_index_1.clone(), file_index_2.clone()],
    };
    let table_auto_incr_id: u64 = 4;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_ids: (table_auto_incr_id as u32)..(table_auto_incr_id as u32 + 4),
        data_file_final_size: MULTI_COMPACTED_DATA_FILE_SIZE,
    };

    // Perform compaction.
    let builder = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params);
    let compaction_result = builder.build().await.unwrap();
    assert!(compaction_result.new_data_files.is_empty());
    assert!(compaction_result.remapped_data_files.is_empty());
}

/// Testing scenario: new compacted data files are larger than max flush count.
/// For more details, please refer to https://github.com/Mooncake-Labs/moonlink/issues/641
#[tokio::test]
async fn test_large_number_of_data_files() {
    // Create data file.
    let temp_dir = tempfile::tempdir().unwrap();

    let target_data_files_to_compact = storage_utils::NUM_FILES_PER_FLUSH + 1;
    let record_batch = test_utils::create_test_batch_1();
    let row_count_per_file = record_batch.num_rows();

    let mut old_data_files_to_compact = vec![];
    let mut old_file_indices_to_compact = vec![];
    for idx in 0..target_data_files_to_compact {
        let data_file_file_id = idx;
        let index_block_file_id = target_data_files_to_compact + idx;

        // Prepare data files to compact.
        let data_file_path = temp_dir.path().join(format!("test-{idx}.parquet"));
        let data_file = create_data_file(
            data_file_file_id,
            data_file_path.to_str().unwrap().to_string(),
        );
        test_utils::dump_arrow_record_batches(vec![record_batch.clone()], data_file.clone()).await;
        old_data_files_to_compact.push(get_single_file_to_compact(
            &data_file, /*deletion_vector=*/ None,
        ));

        // Prepare file indices to compact.
        let file_index = test_utils::create_file_index_1(
            temp_dir.path().to_path_buf(),
            data_file.clone(),
            index_block_file_id,
        )
        .await;
        old_file_indices_to_compact.push(file_index);
    }

    // Prepare compaction payload.
    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: ObjectStorageCache::default_for_test(&temp_dir),
        filesystem_accessor: FileSystemAccessor::default_for_test(&temp_dir),
        disk_files: old_data_files_to_compact,
        file_indices: old_file_indices_to_compact,
    };
    let start_table_auto_incr_id = target_data_files_to_compact as u32 * 3;
    let end_table_auto_incr_id = target_data_files_to_compact as u32 * 4;
    let file_params = CompactionFileParams {
        dir_path: std::path::PathBuf::from(temp_dir.path()),
        table_auto_incr_ids: start_table_auto_incr_id..end_table_auto_incr_id,
        data_file_final_size: 1, // Dump each data file into its own file.
    };

    // Perform compaction.
    let builder = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params);
    let compaction_result = builder.build().await.unwrap();
    assert_eq!(
        compaction_result.remapped_data_files.len(),
        target_data_files_to_compact as usize * row_count_per_file
    );
    assert_eq!(
        compaction_result.old_data_files.len(),
        target_data_files_to_compact as usize
    );
    assert_eq!(
        compaction_result.new_data_files.len(),
        target_data_files_to_compact as usize
    );
    assert_eq!(
        compaction_result.old_file_indices.len(),
        target_data_files_to_compact as usize
    );
    assert_eq!(compaction_result.new_file_indices.len(), 1);
}
