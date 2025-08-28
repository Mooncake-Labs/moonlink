use arrow_array::{Int32Array, RecordBatch, StringArray};
use iceberg::io::FileIOBuilder;
use iceberg::puffin::CompressionCodec;
use parquet::arrow::AsyncArrowWriter;

use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::compaction::table_compaction::{CompactedDataEntry, RemappedRecordLocation};
use crate::storage::filesystem::accessor::base_filesystem_accessor::BaseFileSystemAccess;
use crate::storage::iceberg::deletion_vector::DeletionVector;
use crate::storage::iceberg::deletion_vector::{
    DELETION_VECTOR_CADINALITY, DELETION_VECTOR_REFERENCED_DATA_FILE,
    MOONCAKE_DELETION_VECTOR_NUM_ROWS,
};
use crate::storage::iceberg::puffin_utils;
use crate::storage::iceberg::puffin_writer_proxy;
use crate::storage::iceberg::test_utils::load_arrow_batch;
use crate::storage::index::persisted_bucket_hash_map::{
    test_get_hashes_for_index, GlobalIndexBuilder,
};
use crate::storage::index::FileIndex;
use crate::storage::mooncake_table::delete_vector::BatchDeletionVector;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::storage_utils::{FileId, TableUniqueFileId};
use crate::storage::storage_utils::{MooncakeDataFileRef, RecordLocation};
use crate::storage::PuffinBlobRef;
use crate::ObjectStorageCache;

use crate::error::Result;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Test data.
const NUM_ROWS: usize = 6;
const ID_VALUES: [i32; 6] = [1, 2, 3, 4, 5, 6];
const NAME_VALUES: [&str; 6] = ["a", "b", "c", "d", "e", "f"];
const AGE_VALUES: [i32; 6] = [10, 20, 30, 40, 50, 60];
/// The first test record batch correspond to test data [0, 3), the second one correspond to test data [3, 6).
const FILE_INDICES: [[usize; 3]; 2] = [[0, 1, 2], [3, 4, 5]];

/// Test util function to get hash value for the given row.
fn get_hash_for_row(val1: i32, val2: &str, val3: i32) -> u64 {
    let mut hasher = DefaultHasher::new();
    val1.hash(&mut hasher);
    val2.hash(&mut hasher);
    val3.hash(&mut hasher);
    hasher.finish()
}

/// Test util function to dump parquet files to local filesystem.
pub(crate) fn create_test_batch_1() -> RecordBatch {
    let arrow_schema = create_test_arrow_schema();
    RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(ID_VALUES[..3].to_vec())), // id column
            Arc::new(StringArray::from(NAME_VALUES[..3].to_vec())), // name column
            Arc::new(Int32Array::from(AGE_VALUES[..3].to_vec())), // age column
        ],
    )
    .unwrap()
}
pub(crate) fn create_test_batch_2() -> RecordBatch {
    let arrow_schema = create_test_arrow_schema();
    RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(ID_VALUES[3..].to_vec())), // id column
            Arc::new(StringArray::from(NAME_VALUES[3..].to_vec())), // name column
            Arc::new(Int32Array::from(AGE_VALUES[3..].to_vec())), // age column
        ],
    )
    .unwrap()
}

/// Test util function to dump arrow record batches to local filesystem.
pub(crate) async fn dump_arrow_record_batches(
    record_batches: Vec<RecordBatch>,
    data_file: MooncakeDataFileRef,
) {
    let write_file = tokio::fs::File::create(data_file.file_path())
        .await
        .unwrap();
    let mut writer =
        AsyncArrowWriter::try_new(write_file, create_test_arrow_schema(), /*props=*/ None).unwrap();
    for cur_record_batch in record_batches.iter() {
        writer.flush().await.unwrap();
        writer.write(cur_record_batch).await.unwrap();
    }
    writer.close().await.unwrap();
}

/// Test util function to create and dump file index, which correspond to test record batch.
pub(crate) async fn create_file_index_1(
    directory: std::path::PathBuf,
    data_file: MooncakeDataFileRef,
    start_file_id: u64,
) -> Result<FileIndex> {
    let entries = vec![
        (
            get_hash_for_row(ID_VALUES[0], NAME_VALUES[0], AGE_VALUES[0]),
            /*seg_idx=*/ 0,
            /*row_idx=*/ 0,
        ),
        (
            get_hash_for_row(ID_VALUES[1], NAME_VALUES[1], AGE_VALUES[1]),
            /*seg_idx=*/ 0,
            /*row_idx=*/ 1,
        ),
        (
            get_hash_for_row(ID_VALUES[2], NAME_VALUES[2], AGE_VALUES[2]),
            /*seg_idx=*/ 0,
            /*row_idx=*/ 2,
        ),
    ];

    let mut builder = GlobalIndexBuilder::new();
    builder.set_files(vec![data_file]);
    builder.set_directory(directory);
    builder.build_from_flush(entries, start_file_id).await
}
pub(crate) async fn create_file_index_2(
    directory: std::path::PathBuf,
    data_file: MooncakeDataFileRef,
    start_file_id: u64,
) -> Result<FileIndex> {
    let entries = vec![
        (
            get_hash_for_row(ID_VALUES[3], NAME_VALUES[3], AGE_VALUES[3]),
            /*seg_idx=*/ 0,
            /*row_idx=*/ 0,
        ),
        (
            get_hash_for_row(ID_VALUES[4], NAME_VALUES[4], AGE_VALUES[4]),
            /*seg_idx=*/ 0,
            /*row_idx=*/ 1,
        ),
        (
            get_hash_for_row(ID_VALUES[5], NAME_VALUES[5], AGE_VALUES[5]),
            /*seg_idx=*/ 0,
            /*row_idx=*/ 2,
        ),
    ];

    let mut builder = GlobalIndexBuilder::new();
    builder.set_files(vec![data_file]);
    builder.set_directory(directory);
    builder.build_from_flush(entries, start_file_id).await
}
pub(crate) async fn create_file_index_for_both_batches(
    directory: std::path::PathBuf,
    data_file: MooncakeDataFileRef,
    start_file_id: u64,
) -> Result<FileIndex> {
    let mut entries = vec![];
    for idx in 0..NUM_ROWS {
        entries.push((
            get_hash_for_row(ID_VALUES[idx], NAME_VALUES[idx], AGE_VALUES[idx]),
            /*seg_idx=*/ 0,
            /*row_idx=*/ idx,
        ));
    }

    let mut builder = GlobalIndexBuilder::new();
    builder.set_files(vec![data_file]);
    builder.set_directory(directory);
    builder.build_from_flush(entries, start_file_id).await
}

/// Test util functions to dump deletion vector puffin file to local filesystem.
/// Precondition: rows to delete are sorted in ascending order.
pub(crate) async fn dump_deletion_vector_puffin(
    data_file: String,
    puffin_filepath: String,
    batch_deletion_vector: BatchDeletionVector,
    object_storage_cache: ObjectStorageCache,
    filesystem_accessor: &dyn BaseFileSystemAccess,
    table_unique_file_id: TableUniqueFileId,
) -> PuffinBlobRef {
    let deleted_rows = batch_deletion_vector.collect_deleted_rows();
    let deleted_rows_num = deleted_rows.len();

    let mut iceberg_deletion_vector = DeletionVector::new();
    iceberg_deletion_vector.mark_rows_deleted(deleted_rows);
    let blob_properties = HashMap::from([
        (DELETION_VECTOR_REFERENCED_DATA_FILE.to_string(), data_file),
        (
            DELETION_VECTOR_CADINALITY.to_string(),
            deleted_rows_num.to_string(),
        ),
        (
            MOONCAKE_DELETION_VECTOR_NUM_ROWS.to_string(),
            batch_deletion_vector.get_max_rows().to_string(),
        ),
    ]);
    let blob = iceberg_deletion_vector.serialize(blob_properties);
    let blob_size = blob.data().len();
    let mut puffin_writer = puffin_utils::create_puffin_writer(
        &FileIOBuilder::new_fs_io().build().unwrap(),
        &puffin_filepath,
    )
    .await
    .unwrap();
    puffin_writer
        .add(blob, CompressionCodec::None)
        .await
        .unwrap();
    puffin_writer_proxy::get_puffin_metadata_and_close(puffin_writer)
        .await
        .unwrap();

    // Download and pin the puffin blob in the object storage cache.
    let (cache_handle, _) = object_storage_cache
        .get_cache_entry(table_unique_file_id, &puffin_filepath, filesystem_accessor)
        .await
        .unwrap();

    PuffinBlobRef {
        puffin_file_cache_handle: cache_handle.unwrap(),
        start_offset: 4_u32, // Puffin file starts with 4 magic bytes.
        blob_size: blob_size as u32,
        num_rows: deleted_rows_num,
    }
}

/// Test util function to get the expected remap hashmap.
///
/// Precondition: data files are generated by `create_test_batch_1` and `create_test_batch_2`.
pub(crate) fn get_expected_remap_for_one_file(
    compacted_file_id: FileId,
    deletion_vector: Vec<usize>,
) -> HashMap<RecordLocation, RecordLocation> {
    let mut expected_remap = HashMap::new();
    let mut new_row_idx = 0;
    for old_row_idx in 0..3 {
        if !deletion_vector.contains(&old_row_idx) {
            expected_remap.insert(
                RecordLocation::DiskFile(FileId(0), old_row_idx),
                RecordLocation::DiskFile(compacted_file_id, new_row_idx),
            );
            new_row_idx += 1;
        }
    }
    expected_remap
}

/// Test util function to get old record location to new one mapping.
pub(crate) fn get_record_location_mapping(
    remapped_data_files: &HashMap<RecordLocation, RemappedRecordLocation>,
) -> HashMap<RecordLocation, RecordLocation> {
    let mut remapped_record_location = HashMap::with_capacity(remapped_data_files.len());
    for (old_record_location, cur_remapped) in remapped_data_files.iter() {
        let new_record_location = cur_remapped.record_location.clone();
        remapped_record_location.insert(old_record_location.clone(), new_record_location);
    }
    remapped_record_location
}

/// Test util function to get the possible expected remap.
///
/// Precondition: data files are generated by `create_test_batch_1` and `create_test_batch_2`.
pub(crate) fn get_expected_remap_for_two_files(
    compacted_file_id: FileId,
    deletion_vectors: Vec<Vec<usize>>,
) -> HashMap<RecordLocation, RecordLocation> {
    assert_eq!(deletion_vectors.len(), 2);

    let mut expected_remap = HashMap::new();
    let mut new_row_idx = 0;
    for (old_file_id, cur_deletion_vector) in deletion_vectors.iter().enumerate().take(2) {
        for old_row_idx in 0..3 {
            if !cur_deletion_vector.contains(&old_row_idx) {
                expected_remap.insert(
                    RecordLocation::DiskFile(FileId(old_file_id as u64), old_row_idx),
                    RecordLocation::DiskFile(compacted_file_id, new_row_idx),
                );
                new_row_idx += 1;
            }
        }
    }

    expected_remap
}

/// Test util function to get the possible expected remap.
///
/// Precondition: data files are generated by `create_test_batch_1` and `create_test_batch_2`.
pub(crate) fn get_expected_remap_for_two_batches_in_one_file(
    compacted_file_id: FileId,
    deletion_vector: Vec<usize>,
) -> HashMap<RecordLocation, RecordLocation> {
    let mut expected_remap = HashMap::new();
    let mut new_row_idx = 0;
    for old_row_idx in 0..6 {
        if !deletion_vector.contains(&old_row_idx) {
            expected_remap.insert(
                RecordLocation::DiskFile(FileId(0), old_row_idx),
                RecordLocation::DiskFile(compacted_file_id, new_row_idx),
            );
            new_row_idx += 1;
        }
    }

    expected_remap
}

/// Test util function to get arrow record batches for the given uncompacted data file.
///
/// Precondition: data files are generated by `create_test_batch_1` and `create_test_batch_2`.
fn get_uncompacted_arrow_batches(file_idx: usize, old_row_indices: &[usize]) -> RecordBatch {
    let mut id_col = vec![];
    let mut name_col = vec![];
    let mut age_col = vec![];

    for row_idx in FILE_INDICES[file_idx] {
        if old_row_indices.contains(&row_idx) {
            id_col.push(ID_VALUES[row_idx]);
            name_col.push(NAME_VALUES[row_idx]);
            age_col.push(AGE_VALUES[row_idx]);
        }
    }
    RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(id_col)),    // id column
            Arc::new(StringArray::from(name_col)), // name column
            Arc::new(Int32Array::from(age_col)),   // age column
        ],
    )
    .unwrap()
}

/// Test util function to get the expected compacted arrow record batch.
///
/// Precondition: data files are generated by `create_test_batch_1` and `create_test_batch_2`.
fn get_compacted_arrow_batch(old_row_indices: Vec<usize>) -> RecordBatch {
    let mut id_col = vec![];
    let mut name_col = vec![];
    let mut age_col = vec![];

    for row_idx in 0..6 {
        if old_row_indices.contains(&row_idx) {
            id_col.push(ID_VALUES[row_idx]);
            name_col.push(NAME_VALUES[row_idx]);
            age_col.push(AGE_VALUES[row_idx]);
        }
    }

    RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(id_col)),    // id column
            Arc::new(StringArray::from(name_col)), // name column
            Arc::new(Int32Array::from(age_col)),   // age column
        ],
    )
    .unwrap()
}

/// Test util function to validate deletion vector applied to single compacted file.
pub(crate) async fn check_compacted_single_data_files(
    new_data_files: Vec<(MooncakeDataFileRef, CompactedDataEntry)>,
    old_row_indices: Vec<Vec<usize>>,
) {
    assert_eq!(new_data_files.len(), old_row_indices.len());

    // Due to non-deterministic iteration for hashmap, we have to get all expected and actual record batchs, sort and compare.
    let mut expected_record_batches = vec![];
    let mut actual_record_batches = vec![];

    for (file_idx, (cur_new_data_file, cur_old_row_indices)) in new_data_files
        .iter()
        .zip(old_row_indices.iter())
        .enumerate()
    {
        let expected_record_batch = get_uncompacted_arrow_batches(file_idx, cur_old_row_indices);
        let loaded_record_batch = load_arrow_batch(
            &FileIOBuilder::new_fs_io().build().unwrap(),
            cur_new_data_file.0.file_path(),
        )
        .await
        .unwrap();

        expected_record_batches.push(expected_record_batch);
        actual_record_batches.push(loaded_record_batch);
    }

    expected_record_batches.sort_by_key(|cur_record_batch| {
        cur_record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0)
    });
    actual_record_batches.sort_by_key(|cur_record_batch| {
        cur_record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0)
    });
    assert_eq!(expected_record_batches, actual_record_batches);
}

/// Test util function to validate data file compaction.
///
/// # Arguments
///
/// * old_row_indices: row indices of the data files before compaction, which should exist in the compacted data files.
///
/// Precondition: data files are generated by `create_test_batch_1` and `create_test_batch_2`.
pub(crate) async fn check_data_file_compaction(
    new_data_files: Vec<(MooncakeDataFileRef, CompactedDataEntry)>,
    old_row_indices: Vec<usize>,
) {
    if old_row_indices.is_empty() {
        assert!(new_data_files.is_empty());
        return;
    }

    // Data files are compacted into one.
    assert_eq!(new_data_files.len(), 1);

    let expected_arrow_batch = get_compacted_arrow_batch(old_row_indices);
    let loaded_arrow_batch = load_arrow_batch(
        &FileIOBuilder::new_fs_io().build().unwrap(),
        new_data_files[0].0.file_path(),
    )
    .await
    .unwrap();
    assert_eq!(expected_arrow_batch, loaded_arrow_batch);
}

/// Test util function to check file indices compaction, for multiple compacted data files.
///
/// # Arguments
///
/// * old_row_indices: row indices of the data files before compaction, which should exist in the compacted data files.
///
/// Precondition: data files are generated by `create_test_batch_1` and `create_test_batch_2`.
pub(crate) async fn check_file_indices_compaction_for_multiple_compacted_files(
    file_indices: &[FileIndex],
    expected_record_locations: Vec<(FileId, usize /*row-idx*/)>,
    old_row_indices: Vec<usize>,
) {
    if old_row_indices.is_empty() {
        assert!(file_indices.is_empty());
        assert!(old_row_indices.is_empty());
        return;
    }

    // File indices are compacted into one.
    assert_eq!(file_indices.len(), 1);
    let compacted_file_index = file_indices[0].clone();
    assert_eq!(
        compacted_file_index.num_rows as usize,
        old_row_indices.len()
    );

    let mut actual_record_locations = vec![];
    for old_row_idx in old_row_indices.iter() {
        let hash_value = get_hash_for_row(
            ID_VALUES[*old_row_idx],
            NAME_VALUES[*old_row_idx],
            AGE_VALUES[*old_row_idx],
        );
        let locs = compacted_file_index
            .search_values(&test_get_hashes_for_index(&[hash_value]))
            .await;
        assert_eq!(locs.len(), 1);
        let (_, RecordLocation::DiskFile(actual_file_id, actual_new_row_idx)) = locs[0] else {
            panic!("Failed to get record location for {hash_value}");
        };
        actual_record_locations.push((actual_file_id, actual_new_row_idx));
    }
    assert_eq!(expected_record_locations, actual_record_locations);
}

/// Test util function to check file indices compaction.
///
/// # Arguments
///
/// * old_row_indices: row indices of the data files before compaction, which should exist in the compacted data files.
///
/// Precondition: data files are generated by `create_test_batch_1` and `create_test_batch_2`.
pub(crate) async fn check_file_indices_compaction(
    file_indices: &[FileIndex],
    expected_file_id: Option<FileId>,
    old_row_indices: Vec<usize>,
) {
    if old_row_indices.is_empty() {
        assert!(file_indices.is_empty());
        return;
    }

    // File indices are compacted into one.
    assert_eq!(file_indices.len(), 1);
    let compacted_file_index = file_indices[0].clone();
    assert_eq!(
        compacted_file_index.num_rows as usize,
        old_row_indices.len()
    );

    // In the unit test, we check whether we've iterated through all row indices from [0, num_rows).
    let mut new_row_indices: HashSet<u32> =
        (0..compacted_file_index.num_rows).collect::<HashSet<_>>();

    for old_row_idx in old_row_indices.iter() {
        let hash_value = get_hash_for_row(
            ID_VALUES[*old_row_idx],
            NAME_VALUES[*old_row_idx],
            AGE_VALUES[*old_row_idx],
        );
        let locs = compacted_file_index
            .search_values(&test_get_hashes_for_index(&[hash_value]))
            .await;
        assert_eq!(
            locs.len(),
            1,
            "Failed to search for {old_row_idx}-th row in the compacted file indice"
        );
        let (_, RecordLocation::DiskFile(actual_file_id, actual_new_row_idx)) = locs[0] else {
            panic!("Failed to get record location for {hash_value}");
        };
        assert_eq!(expected_file_id.unwrap(), actual_file_id);
        // Check whether the actual new row index falls in the valid range, and hasn't been visited before.
        assert!(new_row_indices.remove(&(actual_new_row_idx as u32)));
    }
    // Check all expected row indices have been visited.
    assert!(new_row_indices.is_empty());
}
