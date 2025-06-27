use itertools::Itertools;
use rstest::rstest;
use tempfile::TempDir;
use tokio::sync::mpsc::Receiver;

/// This file contains state-machine based unit test for data files and file index, which checks their state transfer for a few operations, for example, compaction and request read.
///
/// remote storage state:
/// - Has remote storage
/// - no remote storage
///
/// Local cache state:
/// - Local cache pinned
/// - No local cache (including unreferenced)
///
/// Use request state (use includes request to read or compact, which adds reference count):
/// - Requested to use
/// - Not requested to use
///
/// Constraint:
/// - Impossible to have “no remote storage” and “no local cache”
/// - Local cache pinned indicates either “no remote storage” or “request to use”
///
/// Possible states for disk file entries:
/// (0) No such entry
/// (1) Has remote storage, no local cache, not used
/// (2) Has remote storage, no local cache, in use
/// (3) Has remote storage, local cache pinned, in use
/// (4) no remote storage, local cache pinned, in use
/// (5) no remote storage, local cache pinned, not used
///
/// State inputs:
/// - Iceberg snapshot completion
/// - Request to use, and pinned in cache
/// - Request to use, but not pinned in cache
/// - Use finished, still pinned in cache
/// - Use finished, but not pinned in cache
///
/// State transition for disk file entries:
/// Initial state: no entry
/// - No entry + disk write => no remote, local, not used
///
/// Initial state: no remote, local, not used
/// - no remote, local, not used + use => no remote, local, in use
/// - no remote, local, not used + persist => remote, no local, not used
///
/// Initial state: no remote, local, in use
/// - no remote, local, in use + persist => remote, local, in use
/// - no remote, local, in use + use => no remote, local, in use
/// - no remote, local, in use + use over => no remote, local, in use
///
/// Initial state: remote, local, in use
/// - remote, local, in use + use => remote, local, in use
/// - remote, local, in use + use over & pinned => remote, local, in use
/// - remote, local, in use + use over & unpinned => remote, no local, not used
///
/// Initial state: remote, no local, not used
/// - remote, no local, not used + use & pinned => remote, local, in use
/// - remote, no local, not used + use & unpinned => remote, no local, in use
///
/// Initial state: remote, no local, in use
/// - remote, no local, in use + use & pinned => remote, local, in use
/// - remote, no local, in use + use & unpinned => remote, no local, in use
/// - remote, no local, in use + use over => remote, no local, not used
///
/// For more details, please refer to https://docs.google.com/document/d/1f2d0E_Zi8FbR4QmW_YEhcZwMpua0_pgkaNdrqM1qh2E/edit?usp=sharing
///
/// File indices share most of the operations with data files;
/// for example, they're created at disk slice / stream transaction, persist to iceberg, perform data compaction, etc;
/// so we combine the test state-machine for file indices and data files.
use crate::row::{MoonlinkRow, RowValue};
use crate::storage::cache::object_storage::test_utils::*;
use crate::storage::mooncake_table::state_test_utils::*;
use crate::table_notify::TableNotify;
use crate::{MooncakeTable, ObjectStorageCache};

/// ========================
/// Test util function for read
/// ========================
///
/// Prepare persisted data files in mooncake table.
/// Rows are committed and flushed with LSN 1.
async fn prepare_test_disk_file_for_read(
    temp_dir: &TempDir,
    cache: ObjectStorageCache,
    use_batch_write: bool,
) -> (MooncakeTable, Receiver<TableNotify>) {
    let (mut table, table_notify) =
        create_mooncake_table_and_notify_for_read(temp_dir, cache).await;

    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);

    if use_batch_write {
        table.append(row.clone()).unwrap();
        table.commit(/*lsn=*/ 1);
        table.flush(/*lsn=*/ 1).await.unwrap();
    } else {
        table
            .append_in_stream_batch(row.clone(), /*xact_id=*/ 0)
            .unwrap();
        let commit = table
            .prepare_transaction_stream_commit(/*xact_id=*/ 0, /*lsn=*/ 1)
            .await
            .unwrap();
        table.commit_transaction_stream(commit).await.unwrap();
    }

    (table, table_notify)
}

/// Test scenario: when shutdown table, all cache entries should be unpinned.
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_shutdown_table(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_infinite_object_storage_cache(&temp_dir, /*optimize_local_filesystem=*/ false);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Shutdown the table, which unreferences all cache handles in the snapshot.
    table.shutdown().await.unwrap();

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
}

/// ========================
/// Use by read
/// ========================
///
/// Test scenario: no remote, local, not used + use => no remote, local, in use
#[tokio::test]
#[rstest]
#[case(true, true)]
#[case(false, true)]
#[case(true, false)]
#[case(false, false)]
async fn test_5_read_4_by_batch_write(
    #[case] optimize_local_filesystem: bool,
    #[case] use_batch_write: bool,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_infinite_object_storage_cache(&temp_dir, optimize_local_filesystem);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_local_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ true).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        2
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        1
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// Test scenario: no remote, local, not used + persist => remote, no local, not used
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_5_1_without_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_infinite_object_storage_cache(&temp_dir, /*optimize_local_filesystem=*/ false);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    // Till now, iceberg snapshot has been persisted, need an extra mooncake snapshot to reflect persistence result.
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Check data file has been recorded in mooncake table.
    let _ = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ true).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // data file
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// State transfer is the same as [`test_5_1_without_local_optimization`].
/// Test scenario: no remote, local, not used + persist => remote, no local, not used
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_5_1_with_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_infinite_object_storage_cache(&temp_dir, /*optimize_local_filesystem=*/ true);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let local_data_files_and_index_blocks = get_data_files_and_index_block_files(&table).await;

    // Till now, iceberg snapshot has been persisted, need an extra mooncake snapshot to reflect persistence result.
    let (_, _, _, mut files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, local_data_files_and_index_blocks);

    // Check data file has been recorded in mooncake table.
    let _ = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ false).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // data file
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// Test scenario: no remote, local, in use + persist => remote, local, in use
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_4_3_with_local_filesystem_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ false,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ true).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        1
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // data file
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// State transfer is the same as [`test_4_3_without_local_filesystem_optimization`].
/// Test scenario: no remote, local, in use + persist => remote, local, in use
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_4_3_without_local_filesystem_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ true,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Get index block files.
    let local_index_block = get_only_index_block_filepath(&table).await;

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert_eq!(files_to_delete, vec![local_index_block]);

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ false).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        1
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // data file
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// Test scenario: no remote, local, in use + use => no remote, local, in use
#[tokio::test]
#[rstest]
#[case(true, true)]
#[case(false, true)]
#[case(true, false)]
#[case(false, false)]
async fn test_4_read_4(#[case] optimize_local_filesystem: bool, #[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_object_storage_cache_with_one_file_size(&temp_dir, optimize_local_filesystem);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count for the first time.
    let snapshot_read_output_1 = perform_read_request_for_test(&mut table).await;
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;

    // Read and increment reference count for the second time.
    let snapshot_read_output_2 = perform_read_request_for_test(&mut table).await;
    let snapshot_read_output_2_clone = snapshot_read_output_2.clone();
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;

    // Read and increment reference count for the third time.
    let read_state_3 = snapshot_read_output_2_clone.take_as_read_state().await;

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_local_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ true).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        4,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1, read_state_2, read_state_3],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        1,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// Test scenario: no remote, local, in use + use over => no remote, local, in use
#[tokio::test]
#[rstest]
#[case(true, true)]
#[case(false, true)]
#[case(true, false)]
#[case(false, false)]
async fn test_4_read_and_read_over_4(
    #[case] optimize_local_filesystem: bool,
    #[case] use_batch_write: bool,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_object_storage_cache_with_one_file_size(&temp_dir, optimize_local_filesystem);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Read, increment reference count.
    let snapshot_read_output_1 = perform_read_request_for_test(&mut table).await;
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;

    // Read, increment reference count for the second time.
    let snapshot_read_output_2 = perform_read_request_for_test(&mut table).await;
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;

    // Drop read state and still referenced.
    drop(read_state_1);
    sync_read_request_for_test(&mut table, &mut table_notify).await;

    // Create a mooncake snapshot to reflect read request completion result.
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_local_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ true).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        2
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_2],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        1
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// Test scenario: remote, local, in use + use => remote, local, in use
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_3_read_3_without_filesystem_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ false,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output_1 = perform_read_request_for_test(&mut table).await;
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output_2 = perform_read_request_for_test(&mut table).await;
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ true).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        2,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1, read_state_2],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// State transfer is the same as [`test_3_read_3_with_filesystem_optimization`].
/// Test scenario: remote, local, in use + use => remote, local, in use
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_3_read_3_with_filesystem_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ true,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output_1 = perform_read_request_for_test(&mut table).await;
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;

    // Get local index block.
    let local_index_block = get_only_index_block_filepath(&table).await;

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert_eq!(files_to_delete, vec![local_index_block]);

    // Read and increment reference count.
    let snapshot_read_output_2 = perform_read_request_for_test(&mut table).await;
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ false).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        2,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1, read_state_2],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// Test scenario: remote, local, in use + use over & pinned => remote, local, in use
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_3_read_and_read_over_and_pinned_3_without_local_filesystem_optimization(
    #[case] use_batch_write: bool,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ false,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output_1 = perform_read_request_for_test(&mut table).await;
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output_2 = perform_read_request_for_test(&mut table).await;
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;
    drop(read_state_2);
    sync_read_request_for_test(&mut table, &mut table_notify).await;

    // Create a mooncake snapshot to reflect read request completion result.
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ true).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        1,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// State transfer is the same as [`test_3_read_and_read_over_and_pinned_3_with_local_filesystem_optimization`].
/// Test scenario: remote, local, in use + use over & pinned => remote, local, in use
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_3_read_and_read_over_and_pinned_3_with_local_filesystem_optimization(
    #[case] use_batch_write: bool,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ true,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output_1 = perform_read_request_for_test(&mut table).await;
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;

    // Get local index file.
    let local_index_block = get_only_index_block_filepath(&table).await;

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert_eq!(files_to_delete, vec![local_index_block]);

    // Read and increment reference count.
    let snapshot_read_output_2 = perform_read_request_for_test(&mut table).await;
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;
    drop(read_state_2);
    sync_read_request_for_test(&mut table, &mut table_notify).await;

    // Create a mooncake snapshot to reflect read request completion result.
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ false).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        1,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// Test scenario: remote, local, in use + use over & unpinned => remote, no local, not used
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_3_read_and_read_over_and_unpinned_1_without_local_optimization(
    #[case] use_batch_write: bool,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_infinite_object_storage_cache(&temp_dir, /*optimize_local_filesystem=*/ false);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;
    drop(read_state);
    sync_read_request_for_test(&mut table, &mut table_notify).await;

    // Create a mooncake snapshot to reflect read request completion result.
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Check data file has been recorded in mooncake table.
    let _ = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ true).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// State transfer is the same as [`test_3_read_and_read_over_and_unpinned_1_without_local_optimization`].
/// Test scenario: remote, local, in use + use over & unpinned => remote, no local, not used
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_3_read_and_read_over_and_unpinned_1_with_local_optimization(
    #[case] use_batch_write: bool,
) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_infinite_object_storage_cache(&temp_dir, /*optimize_local_filesystem=*/ true);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let local_data_files_and_index_blocks = get_data_files_and_index_block_files(&table).await;

    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert_eq!(files_to_delete, local_data_files_and_index_blocks);

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;
    drop(read_state);
    sync_read_request_for_test(&mut table, &mut table_notify).await;

    // Create a mooncake snapshot to reflect read request completion result.
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Check data file has been recorded in mooncake table.
    let _ = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ false).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// Test scenario: remote, no local, not used + use & pinned => remote, local, in use
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_1_read_and_pinned_3_without_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_infinite_object_storage_cache(&temp_dir, /*optimize_local_filesystem=*/ false);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ true).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        1,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// State transfer is the same as [`test_1_read_and_pinned_3_without_local_optimization`].
/// Test scenario: remote, no local, not used + use & pinned => remote, local, in use
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_1_read_and_pinned_3_with_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache =
        create_infinite_object_storage_cache(&temp_dir, /*optimize_local_filesystem=*/ true);

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let data_files_and_index_blocks = get_data_files_and_index_block_files(&table).await;

    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert_eq!(files_to_delete, data_files_and_index_blocks);

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ false).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        1,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// Test scenario: remote, no local, not used + use & unpinned => remote, no local, not used
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_1_read_and_unpinned_3_without_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ false,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Import second data file into cache, so the cached entry will be evicted.
    import_fake_cache_entry(&temp_dir, &mut cache).await;

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let _ = get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ true).await;

    // Check cache state.
    check_file_not_pinned(&cache, data_file_id).await;
    check_file_pinned(&cache, FAKE_FILE_ID.file_id).await;

    // Drop all read states and check reference count.
    drop_read_states(vec![read_state], &mut table, &mut table_notify).await;
    check_file_not_pinned(&cache, data_file_id).await;
    check_file_pinned(&cache, FAKE_FILE_ID.file_id).await;
}

/// State transfer is the same as [`test_1_read_and_unpinned_3_without_local_optimization`].
/// Test scenario: remote, no local, not used + use & unpinned => remote, no local, not used
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_1_read_and_unpinned_3_with_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ true,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let local_data_files_and_index_blocks = get_data_files_and_index_block_files(&table).await;

    let (_, _, _, mut files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, local_data_files_and_index_blocks);

    // Import second data file into cache, so the cached entry will be evicted.
    import_fake_cache_entry(&temp_dir, &mut cache).await;

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let _ = get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ false).await;

    // Check cache state.
    check_file_not_pinned(&cache, data_file_id).await;
    check_file_pinned(&cache, FAKE_FILE_ID.file_id).await;

    // Drop all read states and check reference count.
    drop_read_states(vec![read_state], &mut table, &mut table_notify).await;
    check_file_not_pinned(&cache, data_file_id).await;
    check_file_pinned(&cache, FAKE_FILE_ID.file_id).await;
}

/// Test scenario: remote, no local, in use + use & pinned => remote, local, in use
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_2_read_and_pinned_3_without_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ false,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Import second data file into cache, so the cached entry will be evicted.
    let mut fake_cache_handle = import_fake_cache_entry(&temp_dir, &mut cache).await;

    // Read, but no reference count hold within read state.
    let snapshot_read_output_1 = perform_read_request_for_test(&mut table).await;
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;
    // Till now, the state is (remote, no local, in use).

    // Unreference the second cache handle, so we could pin requires files again in cache.
    let evicted_files_to_delete = fake_cache_handle.unreference().await;
    assert!(evicted_files_to_delete.is_empty());

    let snapshot_read_output_2 = perform_read_request_for_test(&mut table).await;
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;

    // Check fake file has been evicted.
    let fake_filepath = temp_dir.path().join(FAKE_FILE_NAME);
    sync_delete_evicted_files(
        &mut table_notify,
        vec![fake_filepath.to_str().unwrap().to_string()],
    )
    .await;

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ true).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        1,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1, read_state_2],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // data file
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// State transfer is the same as [`test_2_read_and_pinned_3_without_local_optimization`].
/// Test scenario: remote, no local, in use + use & pinned => remote, local, in use
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_2_read_and_pinned_3_with_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ true,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let local_data_files_and_index_blocks = get_data_files_and_index_block_files(&table).await;

    let (_, _, _, mut files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, local_data_files_and_index_blocks);

    // Import second data file into cache, so the cached entry will be evicted.
    let mut fake_cache_handle = import_fake_cache_entry(&temp_dir, &mut cache).await;

    // Read, but no reference count hold within read state.
    let snapshot_read_output_1 = perform_read_request_for_test(&mut table).await;
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;
    // Till now, the state is (remote, no local, in use).

    // Unreference the second cache handle, so we could pin requires files again in cache.
    let evicted_files_to_delete = fake_cache_handle.unreference().await;
    assert!(evicted_files_to_delete.is_empty());

    let snapshot_read_output_2 = perform_read_request_for_test(&mut table).await;
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;

    // Check fake file has been evicted.
    let fake_filepath = temp_dir.path().join(FAKE_FILE_NAME);
    sync_delete_evicted_files(
        &mut table_notify,
        vec![fake_filepath.to_str().unwrap().to_string()],
    )
    .await;

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let index_block_file_id =
        get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ false).await;

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(data_file_id))
            .await,
        1,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );

    // Drop all read states and check reference count.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1, read_state_2],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // data file
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 1).await; // index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(index_block_file_id))
            .await,
        1,
    );
}

/// Test scenario: remote, no local, in use + use & unpinned => remote, no local, in use
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_2_read_and_unpinned_2_without_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ false,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Import second data file into cache, so the cached entry will be evicted.
    import_fake_cache_entry(&temp_dir, &mut cache).await;

    // Read, but no reference count hold within read state.
    let snapshot_read_output_1 = perform_read_request_for_test(&mut table).await;
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;
    // Till now, the state is (remote, no local, in use).

    // Read, but no reference count hold within read state.
    let snapshot_read_output_2 = perform_read_request_for_test(&mut table).await;
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let _ = get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ true).await;

    // Check cache state.
    check_file_not_pinned(&cache, data_file_id).await;
    check_file_pinned(&cache, FAKE_FILE_ID.file_id).await;

    // Drop all read states and check reference count; cache only manages fake file here.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1, read_state_2],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    check_file_not_pinned(&cache, data_file_id).await;
    check_file_pinned(&cache, FAKE_FILE_ID.file_id).await;
}

/// State transfer is the same as [`test_2_read_and_unpinned_2_with_local_optimization`].
/// Test scenario: remote, no local, in use + use & unpinned => remote, no local, in use
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_2_read_and_unpinned_2_with_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ true,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let local_data_files_and_index_blocks = get_data_files_and_index_block_files(&table).await;

    let (_, _, _, mut files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, local_data_files_and_index_blocks);

    // Import second data file into cache, so the cached entry will be evicted.
    import_fake_cache_entry(&temp_dir, &mut cache).await;

    // Read, but no reference count hold within read state.
    let snapshot_read_output_1 = perform_read_request_for_test(&mut table).await;
    let read_state_1 = snapshot_read_output_1.take_as_read_state().await;
    // Till now, the state is (remote, no local, in use).

    // Read, but no reference count hold within read state.
    let snapshot_read_output_2 = perform_read_request_for_test(&mut table).await;
    let read_state_2 = snapshot_read_output_2.take_as_read_state().await;

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;
    let _ = get_only_index_block_file_id(&table, &temp_dir, /*is_local=*/ false).await;

    // Check cache state.
    check_file_not_pinned(&cache, data_file_id).await;
    check_file_pinned(&cache, FAKE_FILE_ID.file_id).await;

    // Drop all read states and check reference count; cache only manages fake file here.
    let files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state_1, read_state_2],
        &mut table,
        &mut table_notify,
    )
    .await;
    assert!(files_to_delete.is_empty());
    check_file_not_pinned(&cache, data_file_id).await;
    check_file_pinned(&cache, FAKE_FILE_ID.file_id).await;
}

/// Test scenario: remote, no local, in use + use over => remote, no local, not used
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_2_read_over_1_without_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ false,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Import second data file into cache, so the cached entry will be evicted.
    import_fake_cache_entry(&temp_dir, &mut cache).await;

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;
    // Till now, the state is (remote, no local, in use).

    drop(read_state);

    // Check data file has been recorded in mooncake table.
    let data_file_id = get_only_remote_data_file_id(&table, &temp_dir).await;

    // Check cache state.
    check_file_not_pinned(&cache, data_file_id).await;
    check_file_pinned(&cache, FAKE_FILE_ID.file_id).await;
}

/// State transfer is the same as [`test_2_read_over_1_without_local_optimization`].
/// Test scenario: remote, no local, in use + use over => remote, no local, not used
#[tokio::test]
#[rstest]
#[case(true)]
#[case(false)]
async fn test_2_read_over_1_with_local_optimization(#[case] use_batch_write: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ true,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_file_for_read(&temp_dir, cache.clone(), use_batch_write).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let local_data_files_and_index_blocks = get_data_files_and_index_block_files(&table).await;

    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert_eq!(files_to_delete, local_data_files_and_index_blocks);

    // Import second data file into cache, so the cached entry will be evicted.
    import_fake_cache_entry(&temp_dir, &mut cache).await;

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;
    // Till now, the state is (remote, no local, in use).

    drop(read_state);

    // Check data file has been recorded in mooncake table.
    let disk_files = get_disk_files_for_snapshot(&table).await;
    assert_eq!(disk_files.len(), 1);
    let (file, disk_file_entry) = disk_files.iter().next().unwrap();
    assert!(disk_file_entry.cache_handle.is_none());
    assert!(is_remote_file(file, &temp_dir));

    // Check cache state.
    check_file_not_pinned(&cache, file.file_id()).await;
    check_file_pinned(&cache, FAKE_FILE_ID.file_id).await;
}

/// There're two things different from use for read:
/// - we can only check data compaction after its completion
/// - data compaction only happens when there's remote path
///
/// Possible state transfer:
/// - remote, local, in use + use + use over & pinned => (old) remote, local, in use, (new) no remote, local, no use
/// - remote, local, in use + use + use over & unpinned => (old) remote, local, not used, (new) no remote, local, no use
/// - remote, no local, not used + use & pinned + use over & pinned => (old) remote, local, in use, (new) no remote, local, no use
/// - remote, no local, not used + use & pinned + use over & unpinned => (old) remote, no local, not used, (new) no remote, local, no use
/// - remote, no local, in use + use + use over => (old) remote, no local, in use, (new) no remote, local, no use
///
/// ========================
/// Test util function for compaction
/// ========================
///
/// Test util function to create two data files for compaction.
/// Rows are committed and flushed with LSN 1 and 2 respectively.
async fn prepare_test_disk_files_for_compaction(
    temp_dir: &TempDir,
    cache: ObjectStorageCache,
) -> (MooncakeTable, Receiver<TableNotify>) {
    let (mut table, table_notify) =
        create_mooncake_table_and_notify_for_compaction(temp_dir, cache).await;

    // Append, commit and flush the first row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("John".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 1);
    table.flush(/*lsn=*/ 1).await.unwrap();

    // Append, commit and flush the second row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(2),
        RowValue::ByteArray("Bob".as_bytes().to_vec()),
        RowValue::Int32(20),
    ]);
    table.append(row.clone()).unwrap();
    table.commit(/*lsn=*/ 2);
    table.flush(/*lsn=*/ 2).await.unwrap();

    (table, table_notify)
}

/// ========================
/// Use by compaction
/// ========================
///
/// Test scenario: remote, local, in use + use + use over & pinned => (old) remote, local, in use, (new) no remote, local, no use
#[tokio::test]
async fn test_3_compact_3_5_without_local_filesystem_optimization() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ false,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_files_for_compaction(&temp_dir, cache.clone()).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Get old compacted files before compaction.
    let disk_files = get_disk_files_for_snapshot(&table).await;
    assert_eq!(disk_files.len(), 2);
    let old_compacted_data_files = disk_files.keys().cloned().collect::<Vec<_>>();
    let old_compacted_index_block_files = get_index_block_filepaths(&table).await;
    let old_compacted_index_block_file_ids = get_index_block_file_ids(&table).await;
    assert_eq!(old_compacted_index_block_file_ids.len(), 2);

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, data_compaction_payload, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());
    assert!(data_compaction_payload.is_some());

    // Perform data compaction: use pinned local cache file and unreference.
    let mut evicted_files_to_delete = perform_data_compaction_for_test(
        &mut table,
        &mut table_notify,
        data_compaction_payload.unwrap(),
    )
    .await;
    evicted_files_to_delete.sort();
    assert_eq!(evicted_files_to_delete, old_compacted_index_block_files);

    // Check data file has been recorded in mooncake table.
    let (new_compacted_data_file_size, new_compacted_data_file_id) =
        get_new_compacted_local_file_size_and_id(&table, &temp_dir).await;
    let new_compacted_index_block_size = get_index_block_files_size(&table).await;
    let new_compacted_index_block_file_ids = get_index_block_file_ids(&table).await;
    assert_eq!(new_compacted_index_block_file_ids.len(), 1);

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 2).await; // data files
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    // Two old compacted data files, one new compacted data file, and one new compacted index block
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 4).await;
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_data_file_id
            ))
            .await,
        1,
    );
    for cur_old_compacted_data_file in old_compacted_data_files.iter() {
        assert_eq!(
            cache
                .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                    cur_old_compacted_data_file.file_id()
                ))
                .await,
            1,
        );
    }

    // Drop all read states and check reference count and evicted files to delete.
    let mut actual_files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    actual_files_to_delete.sort();
    let expected_files_to_delete = old_compacted_data_files
        .iter()
        .map(|f| f.file_path().clone())
        .sorted()
        .collect::<Vec<_>>();
    assert_eq!(actual_files_to_delete, expected_files_to_delete);

    // Check cache status.
    assert_eq!(
        cache.cache.read().await.cur_bytes,
        (new_compacted_data_file_size as u64) + new_compacted_index_block_size,
    );
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // compacted data file and compacted index block
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_data_file_id
            ))
            .await,
        1
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_index_block_file_ids[0]
            ))
            .await,
        1,
    );
}

/// State transfer is the same as [`test_3_compact_3_5_without_local_filesystem_optimization`].
/// Test scenario: remote, local, in use + use + use over & pinned => (old) remote, local, in use, (new) no remote, local, no use
#[tokio::test]
async fn test_3_compact_3_5_with_local_filesystem_optimization() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ true,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_files_for_compaction(&temp_dir, cache.clone()).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Get local data files and index blocks.
    let local_index_blocks = get_index_block_filepaths(&table).await;

    // Get old compacted files before compaction.
    let disk_files = get_disk_files_for_snapshot(&table).await;
    assert_eq!(disk_files.len(), 2);
    let old_compacted_data_files = disk_files.keys().cloned().collect::<Vec<_>>();
    let old_compacted_index_block_file_ids = get_index_block_file_ids(&table).await;
    assert_eq!(old_compacted_index_block_file_ids.len(), 2);

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, data_compaction_payload, mut files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, local_index_blocks);
    assert!(data_compaction_payload.is_some());

    // Perform data compaction: use pinned local cache file and unreference.
    let evicted_files_to_delete = perform_data_compaction_for_test(
        &mut table,
        &mut table_notify,
        data_compaction_payload.unwrap(),
    )
    .await;
    assert!(evicted_files_to_delete.is_empty());

    // Check data file has been recorded in mooncake table.
    let (new_compacted_data_file_size, new_compacted_data_file_id) =
        get_new_compacted_local_file_size_and_id(&table, &temp_dir).await;
    let new_compacted_index_block_size = get_index_block_files_size(&table).await;
    let new_compacted_index_block_file_ids = get_index_block_file_ids(&table).await;
    assert_eq!(new_compacted_index_block_file_ids.len(), 1);

    // Check cache state.
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 2).await; // data files
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    // Two old compacted data files, one new compacted data file, and one new compacted index block
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 4).await;
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_data_file_id
            ))
            .await,
        1,
    );
    for cur_old_compacted_data_file in old_compacted_data_files.iter() {
        assert_eq!(
            cache
                .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                    cur_old_compacted_data_file.file_id()
                ))
                .await,
            1,
        );
    }

    // Drop all read states and check reference count and evicted files to delete.
    let mut actual_files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    actual_files_to_delete.sort();
    let expected_files_to_delete = old_compacted_data_files
        .iter()
        .map(|f| f.file_path().clone())
        .sorted()
        .collect::<Vec<_>>();
    assert_eq!(actual_files_to_delete, expected_files_to_delete);

    // Check cache status.
    assert_eq!(
        cache.cache.read().await.cur_bytes,
        (new_compacted_data_file_size as u64) + new_compacted_index_block_size,
    );
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // compacted data file and compacted index block
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_data_file_id
            ))
            .await,
        1
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_index_block_file_ids[0]
            ))
            .await,
        1,
    );
}

/// Test scenario: remote, local, in use + use + use over & unpinned => (old) remote, no local, not used, (new) no remote, local, no use
#[tokio::test]
async fn test_3_compact_1_5_without_local_filesystem_optimization() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ false,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_files_for_compaction(&temp_dir, cache.clone()).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Get old compacted files before compaction.
    let disk_files = get_disk_files_for_snapshot(&table).await;
    assert_eq!(disk_files.len(), 2);
    let mut old_compacted_data_files = disk_files
        .keys()
        .map(|f| f.file_path().clone())
        .collect::<Vec<_>>();
    old_compacted_data_files.sort();

    let mut old_compacted_index_block_files = get_index_block_filepaths(&table).await;
    old_compacted_index_block_files.sort();
    let old_compacted_index_block_file_ids = get_index_block_file_ids(&table).await;
    assert_eq!(old_compacted_index_block_file_ids.len(), 2);

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, data_compaction_payload, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());
    assert!(data_compaction_payload.is_some());

    // Perform data compaction: use pinned local cache file and unreference.
    let mut evicted_files_to_delete = perform_data_compaction_for_test(
        &mut table,
        &mut table_notify,
        data_compaction_payload.unwrap(),
    )
    .await;
    evicted_files_to_delete.sort();
    assert_eq!(evicted_files_to_delete, old_compacted_index_block_files);

    // Drop read state, so old data files are unreferenced any more.
    let mut files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, old_compacted_data_files);

    // Check data file has been recorded in mooncake table.
    let (new_compacted_data_file_size, new_compacted_data_file_id) =
        get_new_compacted_local_file_size_and_id(&table, &temp_dir).await;
    let new_compacted_file_index_size = get_index_block_files_size(&table).await;
    let new_compacted_index_block_file_ids = get_index_block_file_ids(&table).await;

    // Check cache state.
    assert_eq!(
        cache.cache.read().await.cur_bytes,
        (new_compacted_data_file_size as u64) + new_compacted_file_index_size,
    );
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_data_file_id
            ))
            .await,
        1,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_index_block_file_ids[0]
            ))
            .await,
        1,
    );
}

/// State transfer is the same as [`test_3_compact_1_5_with_local_filesystem_optimization`].
/// Test scenario: remote, local, in use + use + use over & unpinned => (old) remote, no local, not used, (new) no remote, local, no use
#[tokio::test]
async fn test_3_compact_1_5_with_local_filesystem_optimization() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ true,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_files_for_compaction(&temp_dir, cache.clone()).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Get old compacted files before compaction.
    let disk_files = get_disk_files_for_snapshot(&table).await;
    assert_eq!(disk_files.len(), 2);
    let old_compacted_data_files = disk_files
        .keys()
        .map(|f| f.file_path().clone())
        .sorted()
        .collect::<Vec<_>>();

    let old_compacted_index_block_files = get_index_block_filepaths(&table).await;
    let old_compacted_index_block_file_ids = get_index_block_file_ids(&table).await;
    assert_eq!(old_compacted_index_block_file_ids.len(), 2);

    // Read and increment reference count.
    let snapshot_read_output = perform_read_request_for_test(&mut table).await;
    let read_state = snapshot_read_output.take_as_read_state().await;

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, data_compaction_payload, mut files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, old_compacted_index_block_files);
    assert!(data_compaction_payload.is_some());

    // Perform data compaction: use pinned local cache file and unreference.
    let evicted_files_to_delete = perform_data_compaction_for_test(
        &mut table,
        &mut table_notify,
        data_compaction_payload.unwrap(),
    )
    .await;
    assert!(evicted_files_to_delete.is_empty());

    // Drop read state, so old data files are unreferenced any more.
    let mut files_to_delete = drop_read_states_and_create_mooncake_snapshot(
        vec![read_state],
        &mut table,
        &mut table_notify,
    )
    .await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, old_compacted_data_files);

    // Check data file has been recorded in mooncake table.
    let (new_compacted_data_file_size, new_compacted_data_file_id) =
        get_new_compacted_local_file_size_and_id(&table, &temp_dir).await;
    let new_compacted_file_index_size = get_index_block_files_size(&table).await;
    let new_compacted_index_block_file_ids = get_index_block_file_ids(&table).await;
    assert_eq!(new_compacted_index_block_file_ids.len(), 1);

    // Check cache state.
    assert_eq!(
        cache.cache.read().await.cur_bytes,
        (new_compacted_data_file_size as u64) + new_compacted_file_index_size,
    );
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_data_file_id
            ))
            .await,
        1,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_index_block_file_ids[0]
            ))
            .await,
        1,
    );
}

/// Test scenario: remote, no local, not used + use & pinned + use over & unpinned => (old) remote, no local, not used, (new) no remote, local, no use
#[tokio::test]
async fn test_1_compact_1_5_without_local_optimization() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ false,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_files_for_compaction(&temp_dir, cache.clone()).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Get old compacted files before compaction.
    let _ = get_disk_files_for_snapshot_and_assert(&table, /*expected_file_num=*/ 2).await;

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, data_compaction_payload, mut files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    files_to_delete.sort();
    assert!(files_to_delete.is_empty());
    assert!(data_compaction_payload.is_some());

    // Import second data file into cache, so the cached entry will be evicted.
    let mut fake_cache_handle = import_fake_cache_entry(&temp_dir, &mut cache).await;
    let evicted_files_to_delete = fake_cache_handle.unreference().await;
    assert!(evicted_files_to_delete.is_empty());

    // Perform data compaction: use remote file to perform compaction.
    let evicted_files_to_delete = perform_data_compaction_for_test(
        &mut table,
        &mut table_notify,
        data_compaction_payload.unwrap(),
    )
    .await;
    // It contains one fake file, and two downloaded local file and their file indices.
    assert_eq!(evicted_files_to_delete.len(), 5);

    // Check data file has been recorded in mooncake table.
    let (new_compacted_data_file_size, new_compacted_data_file_id) =
        get_new_compacted_local_file_size_and_id(&table, &temp_dir).await;
    let file_indices = get_index_block_filepaths(&table).await;
    assert_eq!(file_indices.len(), 1);
    let new_compacted_index_block_size = get_index_block_files_size(&table).await;
    let new_compacted_index_block_file_ids = get_index_block_file_ids(&table).await;
    assert_eq!(new_compacted_index_block_file_ids.len(), 1);

    // Check cache state.
    assert_eq!(
        cache.cache.read().await.cur_bytes,
        (new_compacted_data_file_size as u64) + new_compacted_index_block_size,
    );
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_data_file_id
            ))
            .await,
        1,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_index_block_file_ids[0]
            ))
            .await,
        1,
    );
}

/// State transfer is the same as [`test_1_compact_1_5_without_local_optimization`].
/// Test scenario: remote, no local, not used + use & pinned + use over & unpinned => (old) remote, no local, not used, (new) no remote, local, no use
#[tokio::test]
async fn test_1_compact_1_5_with_local_optimization() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut cache = create_object_storage_cache_with_one_file_size(
        &temp_dir, /*optimize_local_filesystem=*/ true,
    );

    let (mut table, mut table_notify) =
        prepare_test_disk_files_for_compaction(&temp_dir, cache.clone()).await;
    let (_, _, _, files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    assert!(files_to_delete.is_empty());

    // Get old compacted data files and index blocks before compaction.
    let local_data_files_and_index_blocks = get_data_files_and_index_block_files(&table).await;

    // Persist and reflect result to mooncake snapshot.
    create_mooncake_and_persist_for_test(&mut table, &mut table_notify).await;
    let (_, _, data_compaction_payload, mut files_to_delete) =
        create_mooncake_snapshot_for_test(&mut table, &mut table_notify).await;
    files_to_delete.sort();
    assert_eq!(files_to_delete, local_data_files_and_index_blocks);
    assert!(data_compaction_payload.is_some());

    // Import second data file into cache, so the cached entry will be evicted.
    let mut fake_cache_handle = import_fake_cache_entry(&temp_dir, &mut cache).await;
    let evicted_files_to_delete = fake_cache_handle.unreference().await;
    assert!(evicted_files_to_delete.is_empty());

    // Perform data compaction: use remote file to perform compaction.
    let evicted_files_to_delete = perform_data_compaction_for_test(
        &mut table,
        &mut table_notify,
        data_compaction_payload.unwrap(),
    )
    .await;
    // It contains one fake file.
    assert_eq!(evicted_files_to_delete.len(), 1);

    // Check data file has been recorded in mooncake table.
    let (new_compacted_data_file_size, new_compacted_data_file_id) =
        get_new_compacted_local_file_size_and_id(&table, &temp_dir).await;
    let file_indices = get_index_block_filepaths(&table).await;
    assert_eq!(file_indices.len(), 1);
    let new_compacted_index_block_size = get_index_block_files_size(&table).await;
    let new_compacted_index_block_file_ids = get_index_block_file_ids(&table).await;
    assert_eq!(new_compacted_index_block_file_ids.len(), 1);

    // Check cache state.
    assert_eq!(
        cache.cache.read().await.cur_bytes,
        (new_compacted_data_file_size as u64) + new_compacted_index_block_size,
    );
    assert_pending_eviction_entries_size(&mut cache, /*expected_count=*/ 0).await;
    assert_evictable_cache_size(&mut cache, /*expected_count=*/ 0).await;
    assert_non_evictable_cache_size(&mut cache, /*expected_count=*/ 2).await; // data file and index block file
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_data_file_id
            ))
            .await,
        1,
    );
    assert_eq!(
        cache
            .get_non_evictable_entry_ref_count(&get_unique_table_file_id(
                new_compacted_index_block_file_ids[0]
            ))
            .await,
        1,
    );
}
