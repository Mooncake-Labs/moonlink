//! Compacted files close once they reach the final size, counting the row groups already
//! written, not only the one in progress.
use crate::storage::compaction::compactor::{CompactionBuilder, CompactionFileParams};
use crate::storage::compaction::table_compaction::{DataCompactionPayload, SingleFileToCompact};
use crate::storage::compaction::test_utils;
use crate::storage::index::persisted_bucket_hash_map::GlobalIndexBuilder;
use crate::storage::index::FileIndex;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::storage_utils::{MooncakeDataFileRef, TableId, TableUniqueFileId};
use crate::{create_data_file, FileSystemAccessor};
use arrow_array::{Int32Array, RecordBatch, StringArray};
use parquet::file::properties::DEFAULT_MAX_ROW_GROUP_SIZE;
use std::sync::Arc;

/// Write `num_rows` narrow rows to a new data file and build its file index.
async fn data_file_with_index(
    dir: &std::path::Path,
    file_id: u64,
    num_rows: usize,
) -> (MooncakeDataFileRef, FileIndex) {
    let path = dir.join(format!("input-{file_id}.parquet"));
    let data_file = create_data_file(file_id, path.to_str().unwrap().to_string());
    let ids: Vec<i32> = (0..num_rows as i32).collect();
    let batch = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(ids.clone())),
            Arc::new(StringArray::from(vec!["n"; num_rows])),
            Arc::new(Int32Array::from(ids)),
        ],
    )
    .unwrap();
    test_utils::dump_arrow_record_batches(vec![batch], data_file.clone()).await;

    let entries = (0..num_rows)
        .map(|row_idx| {
            (
                (file_id << 32) | row_idx as u64,
                /*seg_idx=*/ 0,
                row_idx,
            )
        })
        .collect();
    let mut builder = GlobalIndexBuilder::new();
    builder.set_files(vec![data_file.clone()]);
    builder.set_directory(dir.to_path_buf());
    let file_index = builder
        .build_from_flush(entries, /*file_id=*/ 100 + file_id)
        .await
        .unwrap();
    (data_file, file_index)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_compacted_file_closes_after_a_full_row_group() {
    let temp_dir = tempfile::tempdir().unwrap();
    // The first input fills exactly one row group, so after it the writer holds no rows in
    // progress, only the row group already written.
    let (large, large_index) =
        data_file_with_index(temp_dir.path(), 0, DEFAULT_MAX_ROW_GROUP_SIZE).await;
    let (small, small_index) = data_file_with_index(temp_dir.path(), 1, 10).await;

    let payload = DataCompactionPayload {
        uuid: uuid::Uuid::new_v4(),
        object_storage_cache: create_test_object_storage_cache(&temp_dir),
        filesystem_accessor: FileSystemAccessor::default_for_test(&temp_dir),
        disk_files: [&large, &small]
            .into_iter()
            .map(|file| SingleFileToCompact {
                file_id: TableUniqueFileId {
                    table_id: TableId(0),
                    file_id: file.file_id(),
                },
                data_file_cache_handle: None,
                filepath: file.file_path().clone(),
                deletion_vector: None,
            })
            .collect(),
        file_indices: vec![large_index, small_index],
    };
    let file_params = CompactionFileParams {
        dir_path: temp_dir.path().to_path_buf(),
        table_auto_incr_ids: 10..14,
        // Any written bytes reach the final size.
        data_file_final_size: 1,
    };
    let result = CompactionBuilder::new(payload, create_test_arrow_schema(), file_params)
        .build()
        .await
        .unwrap();

    let mut rows_per_file: Vec<usize> = result
        .new_data_files
        .iter()
        .map(|(_, entry)| entry.num_rows)
        .collect();
    rows_per_file.sort();
    assert_eq!(rows_per_file, vec![10, DEFAULT_MAX_ROW_GROUP_SIZE]);
}
