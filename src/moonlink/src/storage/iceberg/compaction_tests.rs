/// This test suite tests data compaction.
///
/// Possible states for compaction:
/// (1) No corresponding deletion vector
/// (2) There're rows left after applying deletion vector
/// (3) No rows left after deletion vector
///
/// Possibles states for deletion records:
/// (1) Deletion record is uncommitted
/// (2) Deletion record is committed, but not persisted into iceberg
/// (3) Deletion record committed and persisted into iceberg
///
/// Possible states for concurrent deletion:
/// (1) No deletion happens for compacted files between compaction initiation and compaction reflected to mooncake snapshot
/// (2) There's deletion happens in between
///
/// Impossible states:
/// 1 - 3 - *
///
/// Possible states for file indices:
/// (1) File indices correponds 1-1 to their data files, which means no index merge
/// (2) File indices have been merged
///
/// For more details, please refer to https://docs.google.com/document/d/1aiQqhl5F8QODJm3HPl47BZX0rfNyUbUPSHGArUcCIw4/edit?usp=sharing
use crate::row::{IdentityProp, MoonlinkRow, RowValue};
use crate::storage::compaction::compaction_config::DataCompactionConfig;
use crate::storage::iceberg::table_manager::TableManager;
use crate::storage::iceberg::test_utils::*;
use crate::storage::index::{FileIndex, MooncakeIndex};
use crate::storage::mooncake_table::table_accessor_test_utils::*;
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::table_operation_test_utils::*;
use crate::storage::mooncake_table::validation_test_utils::*;
use crate::storage::mooncake_table::Snapshot;
use crate::storage::storage_utils::{
    FileId, MooncakeDataFileRef, ProcessedDeletionRecord, RawDeletionRecord, RecordLocation,
};
use crate::storage::MooncakeTable;
use crate::{FileSystemAccessor, TableEvent};

use arrow_array::{Int32Array, RecordBatch, StringArray};
use iceberg::io::FileIOBuilder;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

/// Test data.
const ID_VALUES: [i32; 4] = [1, 2, 3, 4];
const NAME_VALUES: [&str; 4] = ["a", "b", "c", "d"];
const AGE_VALUES: [i32; 4] = [10, 20, 30, 40];

/// Test util function to get the moonlink row of the request index.
fn get_moonlink_row(idx: usize) -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(ID_VALUES[idx]),
        RowValue::ByteArray(NAME_VALUES[idx].as_bytes().to_vec()),
        RowValue::Int32(AGE_VALUES[idx]),
    ])
}

/// Test util functio to convert moonlink row to arrow batch.
fn extract_value_from_row(row: MoonlinkRow) -> RecordBatch {
    let mut col_1 = vec![];
    let mut col_2 = vec![];
    let mut col_3 = vec![];

    match row.values[0] {
        RowValue::Int32(v) => col_1.push(v),
        _ => panic!("Moonlink row first elements expect to be int32"),
    }
    match &row.values[1] {
        RowValue::ByteArray(v) => col_2.push(String::from_utf8(v.clone()).unwrap()),
        _ => panic!("Moonlink row second elements expect to be string"),
    }
    match row.values[2] {
        RowValue::Int32(v) => col_3.push(v),
        _ => panic!("Moonlink row third elements expect to be int32"),
    }

    RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(col_1)),  // id column
            Arc::new(StringArray::from(col_2)), // name column
            Arc::new(Int32Array::from(col_3)),  // age column
        ],
    )
    .unwrap()
}

/// Test util function to get data compaction config for all unit tests under the test suite.
fn get_data_compaction_config() -> DataCompactionConfig {
    // Perform compaction as long as there're two data files.
    DataCompactionConfig {
        min_data_file_to_compact: 2,
        max_data_file_to_compact: u32::MAX,
        data_file_final_size: 1000000,
    }
}

/// Test util function to get file id and row idx from process deletion log.
fn parse_processed_deletion_log(
    process_deletion_log: &ProcessedDeletionRecord,
) -> (FileId, usize /*row-idx*/) {
    match process_deletion_log.pos {
        RecordLocation::DiskFile(file_id, row_idx) => (file_id, row_idx),
        _ => panic!(
            "Process deletion record is expected to be disk file, but receives {process_deletion_log:?}"
        ),
    }
}

/// Test util function to get arrow batches in the given rows.
async fn get_arrow_batches_with_row_idx(
    data_file: &str,
    row_indices: Vec<usize>,
) -> Vec<RecordBatch> {
    let file_io = FileIOBuilder::new_fs_io().build().unwrap();
    let loaded_record_batch = load_arrow_batch(&file_io, data_file).await.unwrap();
    row_indices
        .iter()
        .map(|cur_row_idx| {
            loaded_record_batch.slice(/*offset=*/ *cur_row_idx, /*length=*/ 1)
        })
        .collect::<Vec<_>>()
}

/// Test util function to check the given referenced arrow batch are equal to rows.
/// - [`rows`] are ordered in the order of first element
/// - [`arrow_batches`] order is non-deterministic, due to the natural non-deteministism for data compaction
fn check_deleted_rows(mut arrow_batches: Vec<RecordBatch>, rows: Vec<MoonlinkRow>) {
    assert_eq!(arrow_batches.len(), rows.len());
    arrow_batches.sort_by_key(|batch| {
        let column = batch.column(/*index=*/ 0);
        let array = column.as_any().downcast_ref::<Int32Array>().unwrap();
        array.value(0)
    });

    for (actual_record_batch, cur_row) in arrow_batches.iter().zip(rows.iter()) {
        let expected_record_batch = extract_value_from_row(cur_row.clone());
        assert_eq!(expected_record_batch, *actual_record_batch);
    }
}

/// Test util function to get possible row arrow batches.
/// Due to the non-deterministic nature of data compaction (aka, it's unsure for all data files to compact, which rows appear first in the final one), we need to get all possible arrow batches.
fn get_possible_compacted_arrow_batches(row_indices: Vec<usize>) -> Vec<RecordBatch> {
    let mut res = Vec::with_capacity(2);

    for all_row_indices in [vec![0, 1, 2, 3], vec![2, 3, 0, 1]] {
        let mut col_1 = vec![];
        let mut col_2 = vec![];
        let mut col_3 = vec![];

        for row_idx in &all_row_indices {
            if row_indices.contains(row_idx) {
                col_1.push(ID_VALUES[*row_idx]);
                col_2.push(NAME_VALUES[*row_idx]);
                col_3.push(AGE_VALUES[*row_idx]);
            }
        }
        let expected_arrow_record = RecordBatch::try_new(
            create_test_arrow_schema(),
            vec![
                Arc::new(Int32Array::from(col_1)),  // id column
                Arc::new(StringArray::from(col_2)), // name column
                Arc::new(Int32Array::from(col_3)),  // age column
            ],
        )
        .unwrap();
        res.push(expected_arrow_record);
    }

    res
}

/// Test util function to check loaded arrow batch records are expected.
async fn check_loaded_arrow_batches(data_file: &str, row_indices: Vec<usize>) {
    let possible_arrow_batches = get_possible_compacted_arrow_batches(row_indices);
    let file_io = FileIOBuilder::new_fs_io().build().unwrap();
    let actual_record_batch = load_arrow_batch(&file_io, data_file).await.unwrap();
    assert!(
        possible_arrow_batches.contains(&actual_record_batch),
        "Actual record batch is {actual_record_batch:?}"
    );
}

/// Test util function to check loaded file indices are expected.
async fn check_loaded_file_index(file_index: FileIndex, row_indices: Vec<usize>) {
    assert_eq!(row_indices.len(), file_index.num_rows as usize);
    let row_num = file_index.num_rows;

    let row_identity = IdentityProp::FullRow;
    let mut mooncake_index = MooncakeIndex::new();
    mooncake_index.insert_file_index(file_index);

    let mut result_row_indices = vec![];
    for cur_row_index in row_indices {
        let cur_row = get_moonlink_row(cur_row_index);
        let raw_deletion_record = RawDeletionRecord {
            lookup_key: row_identity.get_lookup_key(&cur_row),
            lsn: 0, // Doesn't affect.
            pos: None,
            row_identity: row_identity.extract_identity_columns(cur_row),
        };
        let record_locations = mooncake_index.find_record(&raw_deletion_record).await;
        assert_eq!(
            record_locations.len(),
            1,
            "Actual record locations are {record_locations:?}"
        );
        match record_locations[0] {
            RecordLocation::DiskFile(_, row_idx) => result_row_indices.push(row_idx),
            _ => panic!("Record location shouldn't be in-memory position."),
        }
    }

    // Check all row indices are iterated through.
    result_row_indices.sort();
    assert_eq!(
        result_row_indices,
        (0..row_num as usize).collect::<Vec<_>>()
    );
}

/// Test util function to check compacted data file is as expected.
/// Return file id for the compacted data file.
async fn check_loaded_snapshot(
    snapshot: &Snapshot,
    row_indices: Vec<usize>,
) -> MooncakeDataFileRef {
    // After compaction, there should be only one data file with no deletion vector.
    let (data_file, disk_file_entry) = snapshot.disk_files.iter().next().unwrap();
    assert!(disk_file_entry.puffin_deletion_blob.is_none());
    assert!(disk_file_entry.batch_deletion_vector.is_empty());
    check_loaded_arrow_batches(data_file.file_path(), row_indices.clone()).await;

    let file_indice = snapshot.indices.file_indices.clone();
    assert_eq!(file_indice.len(), 1);
    assert_eq!(file_indice[0].files, vec![data_file.clone()]);
    check_loaded_file_index(file_indice[0].clone(), row_indices.clone()).await;

    data_file.clone()
}

/// Test util function which imports two data files and file indices to mooncake table and iceberg table.
/// These two data files are committed and flushed at two transaction, separately with LSN 0 and 1.
async fn prepare_committed_and_flushed_data_files(
    table: &mut MooncakeTable,
    notify_rx: &mut Receiver<TableEvent>,
) -> Vec<MoonlinkRow> {
    // Append first row.
    let row_1 = get_moonlink_row(/*idx=*/ 0);
    let row_2 = get_moonlink_row(/*idx=*/ 1);
    table.append(row_1.clone()).unwrap();
    table.append(row_2.clone()).unwrap();
    table.commit(/*lsn=*/ 0);
    flush_table_and_sync(table, notify_rx, /*lsn=*/ 0)
        .await
        .unwrap();

    // Append second row.
    let row_3 = get_moonlink_row(/*idx=*/ 2);
    let row_4 = get_moonlink_row(/*idx=*/ 3);
    table.append(row_3.clone()).unwrap();
    table.append(row_4.clone()).unwrap();
    table.commit(/*lsn=*/ 2);
    flush_table_and_sync(table, notify_rx, /*lsn=*/ 1)
        .await
        .unwrap();

    vec![row_1, row_2, row_3, row_4]
}

/// Test util function to check whether mooncake snapshot does match persisted snapshot for data files and file indices after compaction.
///
/// # Arguments
///
/// * table_data_file: data file in the mooncake snapshot, assume there's only one final data file.
/// * table_file_indices: file indices in the mooncake snapshot, assume there's only one final file index.
async fn check_snapshot_reflects_persistence_for_compaction(
    persisted_snapshot: &Snapshot,
    table_data_file: &MooncakeDataFileRef,
    table_file_indices: Vec<FileIndex>,
) {
    // Validate data files in the mooncake snapshot matches those in the persisted snapshot.
    let persisted_disk_files =
        get_disk_files_for_snapshot_and_assert(persisted_snapshot, /*expected_file_num=*/ 1).await;
    assert_eq!(&persisted_disk_files[0], table_data_file.file_path());

    // Index block is harder to validate, considering the fact that index blocks are always cached locally,
    // - For recovered snapshot from iceberg snapshot, index block filepath are from read-through cache;
    // - For mooncake snapshot one, index block filepath are from write-through cache.
    //
    // Validate persisted index block matches with cache handle.
    let persisted_file_indices = get_file_indices_for_snapshot(persisted_snapshot);
    let persisted_index_block =
        get_only_index_block_file_from_file_indices(&persisted_file_indices);
    let cache_handle = persisted_file_indices[0].index_blocks[0]
        .cache_handle
        .as_ref()
        .unwrap();
    assert_eq!(cache_handle.get_cache_filepath(), persisted_index_block);
    assert!(tokio::fs::try_exists(persisted_index_block).await.unwrap());

    // Validate index block within mooncake snapshot matches with cache handle.
    let table_index_block = get_only_index_block_file_from_file_indices(&table_file_indices);
    let cache_handle = table_file_indices[0].index_blocks[0]
        .cache_handle
        .as_ref()
        .unwrap();
    assert_eq!(cache_handle.get_cache_filepath(), table_index_block);
    assert!(tokio::fs::try_exists(table_index_block).await.unwrap());
}

#[tokio::test]
async fn test_compaction_1_1_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager_to_load, mut receiver) =
        create_table_and_iceberg_manager_with_data_compaction_config(
            &temp_dir,
            get_data_compaction_config(),
        )
        .await;
    let _ = prepare_committed_and_flushed_data_files(&mut table, &mut receiver).await;

    // Perform mooncake and iceberg snapshot, and data compaction.
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut receiver,
        /*injected_committed_deletion_rows=*/ vec![],
        /*injected_uncommitted_deletion_rows=*/ vec![],
    )
    .await;

    // Check iceberg snapshot status.
    let (next_file_id, snapshot) = iceberg_table_manager_to_load
        .load_snapshot_from_table()
        .await
        .unwrap();

    assert_eq!(next_file_id, 2); // one compacted data file, one compacted index block file
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 1);
    check_loaded_snapshot(&snapshot, /*row_indices=*/ vec![0, 1, 2, 3]).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager_to_load
            .config
            .accessor_config
            .get_root_path(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Check disk files for the current mooncake snapshot.
    let disk_files = get_disk_files_for_table(&table).await;
    assert_eq!(disk_files.len(), 1);
    let (data_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.puffin_deletion_blob.is_none());
    assert!(disk_file_entry.batch_deletion_vector.is_empty());
    // Check data files and file indices in mooncake table snapshot is the same as iceberg persisted ones.
    let actual_file_indices = get_file_indices_for_table(&table).await;
    check_snapshot_reflects_persistence_for_compaction(&snapshot, data_file, actual_file_indices)
        .await;

    // Check deletion log for the current mooncake snapshot.
    let (committed_deletion_log, uncommitted_deletion_log) =
        get_deletion_logs_for_snapshot(&table).await;
    assert!(committed_deletion_log.is_empty());
    assert!(uncommitted_deletion_log.is_empty());
}

#[tokio::test]
async fn test_compaction_1_1_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager_to_load, mut receiver) =
        create_table_and_iceberg_manager_with_data_compaction_config(
            &temp_dir,
            get_data_compaction_config(),
        )
        .await;
    let rows = prepare_committed_and_flushed_data_files(&mut table, &mut receiver).await;

    // Perform mooncake and iceberg snapshot, and data compaction.
    let injected_committed_deletion_rows = vec![
        (rows[1].clone(), /*lsn=*/ 6), // Belong to the first data file.
    ];
    let injected_uncommitted_deletion_rows = vec![
        (rows[3].clone(), /*lsn=*/ 7), // Belong to the second data file.
    ];
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut receiver,
        injected_committed_deletion_rows,
        injected_uncommitted_deletion_rows,
    )
    .await;

    // Check iceberg snapshot status.
    let (next_file_id, snapshot) = iceberg_table_manager_to_load
        .load_snapshot_from_table()
        .await
        .unwrap();

    assert_eq!(next_file_id, 2); // one compacted data file, one compacted index block file
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 1);
    check_loaded_snapshot(&snapshot, /*row_indices=*/ vec![0, 1, 2, 3]).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager_to_load
            .config
            .accessor_config
            .get_root_path(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Check disk files for the current mooncake snapshot.
    let disk_files = get_disk_files_for_table(&table).await;
    assert_eq!(disk_files.len(), 1);
    let (compacted_data_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.puffin_deletion_blob.is_none());
    let deleted_rows = disk_file_entry.batch_deletion_vector.collect_deleted_rows();
    assert!(
        deleted_rows == vec![1] || deleted_rows == vec![3],
        "Deleted rows are {deleted_rows:?}"
    );
    // Check data files and file indices in mooncake table snapshot is the same as iceberg persisted ones.
    let actual_file_indices = get_file_indices_for_table(&table).await;
    check_snapshot_reflects_persistence_for_compaction(
        &snapshot,
        compacted_data_file,
        actual_file_indices,
    )
    .await;

    // Check deletion log for the current mooncake snapshot.
    let (committed_deletion_log, uncommitted_deletion_log) =
        get_deletion_logs_for_snapshot(&table).await;

    // Check committed deletion logs.
    assert_eq!(committed_deletion_log.len(), 1);
    let (file_id_1, row_idx_1) = parse_processed_deletion_log(&committed_deletion_log[0]);
    assert_eq!(file_id_1, compacted_data_file.file_id());

    // Get referenced arrow batches.
    let referenced_arrow_batches =
        get_arrow_batches_with_row_idx(compacted_data_file.file_path(), vec![row_idx_1]).await;
    check_deleted_rows(referenced_arrow_batches, vec![rows[1].clone()]);

    // Check uncommitted deletion logs.
    assert_eq!(uncommitted_deletion_log.len(), 1);
    let (file_id_1, row_idx_1) =
        parse_processed_deletion_log(uncommitted_deletion_log[0].as_ref().unwrap());
    assert_eq!(file_id_1, compacted_data_file.file_id());

    // Get referenced arrow batches.
    let referenced_arrow_batches =
        get_arrow_batches_with_row_idx(compacted_data_file.file_path(), vec![row_idx_1]).await;
    check_deleted_rows(referenced_arrow_batches, vec![rows[3].clone()]);
}

#[tokio::test]
async fn test_compaction_1_2_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager_to_load, mut receiver) =
        create_table_and_iceberg_manager_with_data_compaction_config(
            &temp_dir,
            get_data_compaction_config(),
        )
        .await;
    let rows = prepare_committed_and_flushed_data_files(&mut table, &mut receiver).await;

    // Delete one row and commit.
    table.delete(rows[0].clone(), /*lsn=*/ 2).await;
    table.commit(/*lsn=*/ 3);
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut receiver,
        /*injected_committed_deletion_rows=*/ vec![],
        /*injected_uncommitted_deletion_rows=*/ vec![],
    )
    .await;

    // Check iceberg snapshot status.
    let (next_file_id, snapshot) = iceberg_table_manager_to_load
        .load_snapshot_from_table()
        .await
        .unwrap();

    assert_eq!(next_file_id, 2); // one compacted data file, one compacted index block file
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 1);
    check_loaded_snapshot(&snapshot, /*row_indices=*/ vec![0, 1, 2, 3]).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager_to_load
            .config
            .accessor_config
            .get_root_path(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Check disk files for the current mooncake snapshot.
    let disk_files = get_disk_files_for_table(&table).await;
    assert_eq!(disk_files.len(), 1);
    let (compacted_data_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.puffin_deletion_blob.is_none());
    let deleted_rows = disk_file_entry.batch_deletion_vector.collect_deleted_rows();
    assert!(
        deleted_rows == vec![0] || deleted_rows == vec![2],
        "Deleted rows are {deleted_rows:?}"
    );
    // Check data files and file indices in mooncake table snapshot is the same as iceberg persisted ones.
    let actual_file_indices = get_file_indices_for_table(&table).await;
    check_snapshot_reflects_persistence_for_compaction(
        &snapshot,
        compacted_data_file,
        actual_file_indices,
    )
    .await;

    // Check deletion log for the current mooncake snapshot.
    let (committed_deletion_log, uncommitted_deletion_log) =
        get_deletion_logs_for_snapshot(&table).await;

    // Check committed deletion logs.
    assert_eq!(committed_deletion_log.len(), 1);
    let (file_id_1, row_idx_1) = parse_processed_deletion_log(&committed_deletion_log[0]);
    assert_eq!(file_id_1, compacted_data_file.file_id());

    // Get referenced arrow batches.
    let referenced_arrow_batches =
        get_arrow_batches_with_row_idx(compacted_data_file.file_path(), vec![row_idx_1]).await;
    check_deleted_rows(referenced_arrow_batches, vec![rows[0].clone()]);

    // Check uncommitted deletion logs.
    assert!(uncommitted_deletion_log.is_empty());
}

#[tokio::test]
async fn test_compaction_1_2_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager_to_load, mut receiver) =
        create_table_and_iceberg_manager_with_data_compaction_config(
            &temp_dir,
            get_data_compaction_config(),
        )
        .await;
    let rows = prepare_committed_and_flushed_data_files(&mut table, &mut receiver).await;

    // Delete one row and commit.
    table.delete(rows[0].clone(), /*lsn=*/ 2).await;
    table.commit(/*lsn=*/ 3);

    // Perform mooncake and iceberg snapshot, and data compaction.
    let injected_committed_deletion_rows = vec![
        (rows[1].clone(), /*lsn=*/ 6), // Belong to the first data file.
    ];
    let injected_uncommitted_deletion_rows = vec![
        (rows[3].clone(), /*lsn=*/ 7), // Belong to the second data file.
    ];
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut receiver,
        injected_committed_deletion_rows,
        injected_uncommitted_deletion_rows,
    )
    .await;

    // Check iceberg snapshot status.
    let (next_file_id, snapshot) = iceberg_table_manager_to_load
        .load_snapshot_from_table()
        .await
        .unwrap();

    assert_eq!(next_file_id, 2); // one compacted data file, one compacted index block file
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 1);
    check_loaded_snapshot(&snapshot, /*row_indices=*/ vec![0, 1, 2, 3]).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager_to_load
            .config
            .accessor_config
            .get_root_path(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Check disk files for the current mooncake snapshot.
    let disk_files = get_disk_files_for_table(&table).await;
    assert_eq!(disk_files.len(), 1);
    let (compacted_data_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.puffin_deletion_blob.is_none());
    let deleted_rows = disk_file_entry.batch_deletion_vector.collect_deleted_rows();
    assert!(
        deleted_rows == vec![0, 1] || deleted_rows == vec![2, 3],
        "Deleted rows are {deleted_rows:?}"
    );
    // Check data files and file indices in mooncake table snapshot is the same as iceberg persisted ones.
    let actual_file_indices = get_file_indices_for_table(&table).await;
    check_snapshot_reflects_persistence_for_compaction(
        &snapshot,
        compacted_data_file,
        actual_file_indices,
    )
    .await;

    // Check deletion log for the current mooncake snapshot.
    let (committed_deletion_log, uncommitted_deletion_log) =
        get_deletion_logs_for_snapshot(&table).await;

    // Check committed deletion logs.
    assert_eq!(committed_deletion_log.len(), 2);
    let (file_id_1, row_idx_1) = parse_processed_deletion_log(&committed_deletion_log[0]);
    let (file_id_2, row_idx_2) = parse_processed_deletion_log(&committed_deletion_log[1]);
    assert_eq!(file_id_1, compacted_data_file.file_id());
    assert_eq!(file_id_2, compacted_data_file.file_id());

    // Get referenced arrow batches.
    let referenced_arrow_batches =
        get_arrow_batches_with_row_idx(compacted_data_file.file_path(), vec![row_idx_1, row_idx_2])
            .await;
    check_deleted_rows(
        referenced_arrow_batches,
        vec![rows[0].clone(), rows[1].clone()],
    );

    // Check uncommitted deletion logs.
    assert_eq!(uncommitted_deletion_log.len(), 1);
    let (file_id_1, row_idx_1) =
        parse_processed_deletion_log(uncommitted_deletion_log[0].as_ref().unwrap());
    assert_eq!(file_id_1, compacted_data_file.file_id());

    // Get referenced arrow batches.
    let referenced_arrow_batches =
        get_arrow_batches_with_row_idx(compacted_data_file.file_path(), vec![row_idx_1]).await;
    check_deleted_rows(referenced_arrow_batches, vec![rows[3].clone()]);
}

#[tokio::test]
async fn test_compaction_2_2_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager_to_load, mut receiver) =
        create_table_and_iceberg_manager_with_data_compaction_config(
            &temp_dir,
            get_data_compaction_config(),
        )
        .await;
    let rows = prepare_committed_and_flushed_data_files(&mut table, &mut receiver).await;

    // Delete two rows and commit/flush/persist into iceberg.
    table.delete(rows[0].clone(), /*lsn=*/ 2).await; // Belong to the first data file.
    table.commit(/*lsn=*/ 3);
    flush_table_and_sync(&mut table, &mut receiver, /*lsn=*/ 3)
        .await
        .unwrap();

    table.delete(rows[2].clone(), /*lsn=*/ 4).await; // Belong to the second data file.
    table.commit(/*lsn=*/ 5);

    // Perform mooncake and iceberg snapshot, and data compaction.
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut receiver,
        /*injected_committed_deletion_rows=*/ vec![],
        /*injected_uncommitted_deletion_rows=*/ vec![],
    )
    .await;

    // Check iceberg snapshot status.
    let (next_file_id, snapshot) = iceberg_table_manager_to_load
        .load_snapshot_from_table()
        .await
        .unwrap();

    assert_eq!(next_file_id, 2); // one compacted data file, one compacted index block file
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 3);
    check_loaded_snapshot(&snapshot, /*row_indices=*/ vec![1, 2, 3]).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager_to_load
            .config
            .accessor_config
            .get_root_path(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Check disk files for the current mooncake snapshot.
    let disk_files = get_disk_files_for_table(&table).await;
    assert_eq!(disk_files.len(), 1);
    let (compacted_data_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.puffin_deletion_blob.is_none());
    let deleted_rows = disk_file_entry.batch_deletion_vector.collect_deleted_rows();
    assert!(
        deleted_rows == vec![0] || deleted_rows == vec![1],
        "Deleted rows are {deleted_rows:?}"
    );
    // Check data files and file indices in mooncake table snapshot is the same as iceberg persisted ones.
    let actual_file_indices = get_file_indices_for_table(&table).await;
    check_snapshot_reflects_persistence_for_compaction(
        &snapshot,
        compacted_data_file,
        actual_file_indices,
    )
    .await;

    // Check deletion log for the current mooncake snapshot.
    let (committed_deletion_log, uncommitted_deletion_log) =
        get_deletion_logs_for_snapshot(&table).await;
    assert!(uncommitted_deletion_log.is_empty());

    assert_eq!(committed_deletion_log.len(), 1);
    let (file_id_1, row_idx_1) = parse_processed_deletion_log(&committed_deletion_log[0]);
    assert_eq!(file_id_1, compacted_data_file.file_id());

    // Get referenced arrow batches.
    let referenced_arrow_batches =
        get_arrow_batches_with_row_idx(compacted_data_file.file_path(), vec![row_idx_1]).await;
    check_deleted_rows(referenced_arrow_batches, vec![rows[2].clone()]);
}

#[tokio::test]
async fn test_compaction_2_2_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager_to_load, mut receiver) =
        create_table_and_iceberg_manager_with_data_compaction_config(
            &temp_dir,
            get_data_compaction_config(),
        )
        .await;
    let rows = prepare_committed_and_flushed_data_files(&mut table, &mut receiver).await;

    // Delete two rows and commit/flush/persist into iceberg.
    table.delete(rows[0].clone(), /*lsn=*/ 2).await; // Belong to the first data file.
    table.commit(/*lsn=*/ 3);
    flush_table_and_sync(&mut table, &mut receiver, /*lsn=*/ 3)
        .await
        .unwrap();

    table.delete(rows[2].clone(), /*lsn=*/ 4).await; // Belong to the second data file.
    table.commit(/*lsn=*/ 5);

    // Perform mooncake and iceberg snapshot, and data compaction.
    let injected_committed_deletion_rows = vec![
        (rows[1].clone(), /*lsn=*/ 6), // Belong to the first data file.
    ];
    let injected_uncommitted_deletion_rows = vec![
        (rows[3].clone(), /*lsn=*/ 7), // Belong to the second data file.
    ];
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut receiver,
        injected_committed_deletion_rows,
        injected_uncommitted_deletion_rows,
    )
    .await;

    // Check iceberg snapshot status.
    let (next_file_id, snapshot) = iceberg_table_manager_to_load
        .load_snapshot_from_table()
        .await
        .unwrap();

    assert_eq!(next_file_id, 2); // one compacted data file, one compacted index block file
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 3);
    check_loaded_snapshot(&snapshot, /*row_indices=*/ vec![1, 2, 3]).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager_to_load
            .config
            .accessor_config
            .get_root_path(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Check disk files for the current mooncake snapshot.
    let disk_files = get_disk_files_for_table(&table).await;
    assert_eq!(disk_files.len(), 1);
    let (compacted_data_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.puffin_deletion_blob.is_none());
    let deleted_rows = disk_file_entry.batch_deletion_vector.collect_deleted_rows();
    // Due to the non-deterministic nature of hashmap, the row indices in the compacted data file is also non-deterministic.
    assert!(
        deleted_rows == vec![0, 1] || deleted_rows == vec![0, 2],
        "Deleted rows are {deleted_rows:?}"
    );
    // Check data files and file indices in mooncake table snapshot is the same as iceberg persisted ones.
    let actual_file_indices = get_file_indices_for_table(&table).await;
    check_snapshot_reflects_persistence_for_compaction(
        &snapshot,
        compacted_data_file,
        actual_file_indices,
    )
    .await;

    // Check deletion log for the current mooncake snapshot.
    let (committed_deletion_log, uncommitted_deletion_log) =
        get_deletion_logs_for_snapshot(&table).await;

    // Check committed deletion logs.
    assert_eq!(committed_deletion_log.len(), 2);
    let (file_id_1, row_idx_1) = parse_processed_deletion_log(&committed_deletion_log[0]);
    let (file_id_2, row_idx_2) = parse_processed_deletion_log(&committed_deletion_log[1]);
    assert_eq!(file_id_1, compacted_data_file.file_id());
    assert_eq!(file_id_2, compacted_data_file.file_id());

    // Get referenced arrow batches.
    let referenced_arrow_batches =
        get_arrow_batches_with_row_idx(compacted_data_file.file_path(), vec![row_idx_1, row_idx_2])
            .await;
    check_deleted_rows(
        referenced_arrow_batches,
        vec![rows[1].clone(), rows[2].clone()],
    );

    // Check uncommitted deletion logs.
    assert_eq!(uncommitted_deletion_log.len(), 1);
    let (file_id_1, row_idx_1) =
        parse_processed_deletion_log(uncommitted_deletion_log[0].as_ref().unwrap());
    assert_eq!(file_id_1, compacted_data_file.file_id());

    // Get referenced arrow batches.
    let referenced_arrow_batches =
        get_arrow_batches_with_row_idx(compacted_data_file.file_path(), vec![row_idx_1]).await;
    check_deleted_rows(referenced_arrow_batches, vec![rows[3].clone()]);
}

#[tokio::test]
async fn test_compaction_2_3_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager_to_load, mut receiver) =
        create_table_and_iceberg_manager_with_data_compaction_config(
            &temp_dir,
            get_data_compaction_config(),
        )
        .await;
    let rows = prepare_committed_and_flushed_data_files(&mut table, &mut receiver).await;

    // Delete two rows and commit/flush/persist into iceberg.
    table.delete(rows[0].clone(), /*lsn=*/ 2).await; // Belong to the first data file.
    table.commit(/*lsn=*/ 3);
    flush_table_and_sync(&mut table, &mut receiver, /*lsn=*/ 3)
        .await
        .unwrap();

    table.delete(rows[2].clone(), /*lsn=*/ 4).await; // Belong to the second data file.
    table.commit(/*lsn=*/ 5);
    flush_table_and_sync(&mut table, &mut receiver, /*lsn=*/ 5)
        .await
        .unwrap();

    // Perform mooncake and iceberg snapshot, and data compaction.
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut receiver,
        /*injected_committed_deletion_rows=*/ vec![],
        /*injected_uncommitted_deletion_rows=*/ vec![],
    )
    .await;

    // Check iceberg snapshot status.
    let (next_file_id, snapshot) = iceberg_table_manager_to_load
        .load_snapshot_from_table()
        .await
        .unwrap();

    assert_eq!(next_file_id, 2); // one compacted data file, one compacted index block file
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 5);
    check_loaded_snapshot(&snapshot, /*row_indices=*/ vec![1, 3]).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager_to_load
            .config
            .accessor_config
            .get_root_path(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Check disk files for the current mooncake snapshot.
    let disk_files = get_disk_files_for_table(&table).await;
    assert_eq!(disk_files.len(), 1);
    let (data_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.puffin_deletion_blob.is_none());
    assert!(disk_file_entry.batch_deletion_vector.is_empty());
    // Check data files and file indices in mooncake table snapshot is the same as iceberg persisted ones.
    let actual_file_indices = get_file_indices_for_table(&table).await;
    check_snapshot_reflects_persistence_for_compaction(&snapshot, data_file, actual_file_indices)
        .await;

    // Check deletion log for current snapshot.
    let (committed_deletion_log, uncommitted_deletion_log) =
        get_deletion_logs_for_snapshot(&table).await;
    assert!(committed_deletion_log.is_empty());
    assert!(uncommitted_deletion_log.is_empty());
}

#[tokio::test]
async fn test_compaction_2_3_2() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager_to_load, mut receiver) =
        create_table_and_iceberg_manager_with_data_compaction_config(
            &temp_dir,
            get_data_compaction_config(),
        )
        .await;
    let rows = prepare_committed_and_flushed_data_files(&mut table, &mut receiver).await;

    // Delete two rows and commit/flush/persist into iceberg.
    table.delete(rows[0].clone(), /*lsn=*/ 2).await; // Belong to the first data file.
    table.commit(/*lsn=*/ 3);
    flush_table_and_sync(&mut table, &mut receiver, /*lsn=*/ 3)
        .await
        .unwrap();

    table.delete(rows[2].clone(), /*lsn=*/ 4).await; // Belong to the second data file.
    table.commit(/*lsn=*/ 5);
    flush_table_and_sync(&mut table, &mut receiver, /*lsn=*/ 5)
        .await
        .unwrap();

    // Perform mooncake and iceberg snapshot, and data compaction.
    let injected_committed_deletion_rows = vec![
        (rows[1].clone(), /*lsn=*/ 6), // Belong to the first data file.
    ];
    let injected_uncommitted_deletion_rows = vec![
        (rows[3].clone(), /*lsn=*/ 7), // Belong to the second data file.
    ];
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut receiver,
        injected_committed_deletion_rows,
        injected_uncommitted_deletion_rows,
    )
    .await;

    // Check iceberg snapshot status.
    let (next_file_id, snapshot) = iceberg_table_manager_to_load
        .load_snapshot_from_table()
        .await
        .unwrap();

    assert_eq!(next_file_id, 2); // one compacted data file, one compacted index block file
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 5);
    check_loaded_snapshot(&snapshot, /*row_indices=*/ vec![1, 3]).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager_to_load
            .config
            .accessor_config
            .get_root_path(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Check disk files for the current mooncake snapshot.
    let disk_files = get_disk_files_for_table(&table).await;
    assert_eq!(disk_files.len(), 1);
    let (compacted_data_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.puffin_deletion_blob.is_none());
    // Deleted row index in the compacted data file.
    let committed_compacted_row_indice: Vec<usize> = disk_file_entry
        .batch_deletion_vector
        .collect_deleted_rows()
        .iter()
        .map(|idx| *idx as usize)
        .collect();
    assert_eq!(committed_compacted_row_indice.len(), 1);
    // Check data files and file indices in mooncake table snapshot is the same as iceberg persisted ones.
    let actual_file_indices = get_file_indices_for_table(&table).await;
    check_snapshot_reflects_persistence_for_compaction(
        &snapshot,
        compacted_data_file,
        actual_file_indices,
    )
    .await;
    // Check referenced deleted arrow batches.
    let committed_deleted_arrow_batches = get_arrow_batches_with_row_idx(
        compacted_data_file.file_path(),
        committed_compacted_row_indice,
    )
    .await;
    check_deleted_rows(committed_deleted_arrow_batches, vec![rows[1].clone()]);

    // Check comitted deletion logs.
    let (committed_deletion_log, uncommitted_deletion_log) =
        get_deletion_logs_for_snapshot(&table).await;

    assert_eq!(committed_deletion_log.len(), 1);
    let (file_id_1, row_idx_1) = parse_processed_deletion_log(&committed_deletion_log[0]);
    assert_eq!(file_id_1, compacted_data_file.file_id());

    // Get referenced arrow batches.
    let referenced_arrow_batches =
        get_arrow_batches_with_row_idx(compacted_data_file.file_path(), vec![row_idx_1]).await;
    check_deleted_rows(referenced_arrow_batches, vec![rows[1].clone()]);

    // Check uncommitted deletion logs.
    assert_eq!(uncommitted_deletion_log.len(), 1);
    let (file_id_1, row_idx_1) =
        parse_processed_deletion_log(uncommitted_deletion_log[0].as_ref().unwrap());
    assert_eq!(file_id_1, compacted_data_file.file_id());

    // Get referenced arrow batches.
    let referenced_arrow_batches =
        get_arrow_batches_with_row_idx(compacted_data_file.file_path(), vec![row_idx_1]).await;
    check_deleted_rows(referenced_arrow_batches, vec![rows[3].clone()]);
}

#[tokio::test]
async fn test_compaction_3_2_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager_to_load, mut receiver) =
        create_table_and_iceberg_manager_with_data_compaction_config(
            &temp_dir,
            get_data_compaction_config(),
        )
        .await;
    let rows = prepare_committed_and_flushed_data_files(&mut table, &mut receiver).await;

    // Delete two rows and commit/flush/persist into iceberg.
    table.delete(rows[0].clone(), /*lsn=*/ 2).await; // Belong to the first data file.
    table.commit(/*lsn=*/ 3);
    table.delete(rows[1].clone(), /*lsn=*/ 4).await; // Belong to the first data file.
    table.commit(/*lsn=*/ 5);

    table.delete(rows[2].clone(), /*lsn=*/ 6).await; // Belong to the second data file.
    table.commit(/*lsn=*/ 7);
    table.delete(rows[3].clone(), /*lsn=*/ 8).await; // Belong to the second data file.
    table.commit(/*lsn=*/ 9);

    // Perform mooncake and iceberg snapshot, and data compaction.
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut receiver,
        /*injected_committed_deletion_rows=*/ vec![],
        /*injected_uncommitted_deletion_rows=*/ vec![],
    )
    .await;

    // Check iceberg snapshot status.
    let (next_file_id, snapshot) = iceberg_table_manager_to_load
        .load_snapshot_from_table()
        .await
        .unwrap();

    assert_eq!(next_file_id, 2); // one compacted data file, one compacted index block file
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 1);
    check_loaded_snapshot(&snapshot, /*row_indices=*/ vec![0, 1, 2, 3]).await;
    assert_eq!(snapshot.indices.file_indices.len(), 1);
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager_to_load
            .config
            .accessor_config
            .get_root_path(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Check disk files for the current mooncake snapshot.
    let disk_files = get_disk_files_for_table(&table).await;
    assert_eq!(disk_files.len(), 1);
    let (compacted_data_file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.puffin_deletion_blob.is_none());
    let deleted_rows = disk_file_entry.batch_deletion_vector.collect_deleted_rows();
    assert_eq!(deleted_rows, vec![0, 1, 2, 3]);

    // Check data files and file indices in mooncake table snapshot is the same as iceberg persisted ones.
    let actual_file_indices = get_file_indices_for_table(&table).await;
    check_snapshot_reflects_persistence_for_compaction(
        &snapshot,
        compacted_data_file,
        actual_file_indices,
    )
    .await;

    // Check deletion log for the current mooncake snapshot.
    let (committed_deletion_log, uncommitted_deletion_log) =
        get_deletion_logs_for_snapshot(&table).await;
    assert!(uncommitted_deletion_log.is_empty());

    assert_eq!(committed_deletion_log.len(), 4);
    let (file_id_1, row_idx_1) = parse_processed_deletion_log(&committed_deletion_log[0]);
    let (file_id_2, row_idx_2) = parse_processed_deletion_log(&committed_deletion_log[1]);
    let (file_id_3, row_idx_3) = parse_processed_deletion_log(&committed_deletion_log[2]);
    let (file_id_4, row_idx_4) = parse_processed_deletion_log(&committed_deletion_log[3]);
    assert_eq!(file_id_1, compacted_data_file.file_id());
    assert_eq!(file_id_2, compacted_data_file.file_id());
    assert_eq!(file_id_3, compacted_data_file.file_id());
    assert_eq!(file_id_4, compacted_data_file.file_id());

    // Get referenced arrow batches.
    let referenced_arrow_batches = get_arrow_batches_with_row_idx(
        compacted_data_file.file_path(),
        vec![row_idx_1, row_idx_2, row_idx_3, row_idx_4],
    )
    .await;
    check_deleted_rows(referenced_arrow_batches, rows.clone());
}

#[tokio::test]
async fn test_compaction_3_3_1() {
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem_accessor = FileSystemAccessor::default_for_test(&temp_dir);
    let (mut table, mut iceberg_table_manager_to_load, mut receiver) =
        create_table_and_iceberg_manager_with_data_compaction_config(
            &temp_dir,
            get_data_compaction_config(),
        )
        .await;
    let rows = prepare_committed_and_flushed_data_files(&mut table, &mut receiver).await;

    // Delete two rows and commit/flush/persist into iceberg.
    table.delete(rows[0].clone(), /*lsn=*/ 2).await; // Belong to the first data file.
    table.delete(rows[1].clone(), /*lsn=*/ 3).await; // Belong to the first data file.
    table.commit(/*lsn=*/ 4);
    flush_table_and_sync(&mut table, &mut receiver, /*lsn=*/ 4)
        .await
        .unwrap();

    table.delete(rows[2].clone(), /*lsn=*/ 5).await; // Belong to the second data file.
    table.delete(rows[3].clone(), /*lsn=*/ 6).await; // Belong to the second data file.
    table.commit(/*lsn=*/ 7);
    flush_table_and_sync(&mut table, &mut receiver, /*lsn=*/ 7)
        .await
        .unwrap();

    // Perform mooncake and iceberg snapshot, and data compaction.
    create_mooncake_and_persist_for_data_compaction_for_test(
        &mut table,
        &mut receiver,
        /*injected_committed_deletion_rows=*/ vec![],
        /*injected_uncommitted_deletion_rows=*/ vec![],
    )
    .await;

    // Check iceberg snapshot status.
    let (next_file_id, snapshot) = iceberg_table_manager_to_load
        .load_snapshot_from_table()
        .await
        .unwrap();

    assert_eq!(next_file_id, 0);
    assert_eq!(snapshot.data_file_flush_lsn.unwrap(), 7);
    assert!(snapshot.disk_files.is_empty());
    assert!(snapshot.indices.file_indices.is_empty());
    check_deletion_vector_consistency_for_snapshot(&snapshot).await;
    validate_recovered_snapshot(
        &snapshot,
        &iceberg_table_manager_to_load
            .config
            .accessor_config
            .get_root_path(),
        filesystem_accessor.as_ref(),
    )
    .await;

    // Check disk files for the current mooncake snapshot.
    let disk_files = get_disk_files_for_table(&table).await;
    assert!(disk_files.is_empty());

    // Check deletion log for the current mooncake snapshot.
    let (committed_deletion_log, uncommitted_deletion_log) =
        get_deletion_logs_for_snapshot(&table).await;
    assert!(committed_deletion_log.is_empty());
    assert!(uncommitted_deletion_log.is_empty());
}
