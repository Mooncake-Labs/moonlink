use crate::storage::mooncake_table::test_utils::TestContext;
use crate::storage::wal::test_utils::WAL_TEST_TABLE_ID;
use crate::storage::wal::test_utils::*;
use crate::storage::wal::WalManager;
use crate::storage::wal::{PersistentWalMetadata, WalTransactionState};
use crate::WalConfig;
use crate::{assert_wal_file_does_not_exist, assert_wal_file_exists, assert_wal_logs_equal};

#[tokio::test]
async fn test_wal_insert_persist_files() {
    let context = TestContext::new("wal_persist");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let (mut wal, expected_events) = create_test_wal(wal_config).await;

    // Persist and verify file number
    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    // Check file exists and has content
    assert_wal_file_exists!(wal.get_file_system_accessor(), 0);

    let expected_wal_events = convert_to_wal_events_vector(&expected_events);
    assert_wal_logs_equal!(&[0], wal.get_file_system_accessor(), expected_wal_events);
}

#[tokio::test]
async fn test_wal_empty_persist() {
    let context = TestContext::new("wal_empty_persist");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let mut wal = WalManager::new(&wal_config);

    // Persist without any events
    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    // No file should be created for empty WAL
    assert!(!wal_file_exists(wal.get_file_system_accessor(), 0).await);
}

#[tokio::test]
async fn test_wal_file_numbering_sequence() {
    let context = TestContext::new("wal_file_numbering");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let mut wal = WalManager::new(&wal_config);

    let mut events = Vec::new();

    // First loop: push and persist events
    for i in 0..3 {
        add_new_example_append_event(100 + i, None, &mut wal, &mut events);
        wal.do_wal_persistence_update_for_test(None).await.unwrap();
    }

    // Second loop: check file existence and contents
    for i in 0..3 {
        let expected_wal_events = convert_to_wal_events_vector(&events[i as usize..=(i as usize)]);
        assert_wal_file_exists!(wal.get_file_system_accessor(), i);
        assert_wal_logs_equal!(&[i], wal.get_file_system_accessor(), expected_wal_events);
    }
}

#[tokio::test]
async fn test_wal_truncation_deletes_files() {
    let context = TestContext::new("wal_truncation");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let mut wal = WalManager::new(&wal_config);

    // first commit in files 0, 1, 2, complete_lsn is 101
    let mut events = Vec::new();
    for _ in 0..2 {
        add_new_example_append_event(100, None, &mut wal, &mut events);
        wal.do_wal_persistence_update_for_test(None).await.unwrap();
    }
    add_new_example_commit_event(101, None, &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    // second commit in files 3, 4, complete_lsn is 102
    add_new_example_append_event(101, None, &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    add_new_example_commit_event(102, None, &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    // Truncate from LSN 102 (should delete files 0, 1, 2 - files with LSN < 102)
    wal.do_wal_persistence_update_for_test(Some(101))
        .await
        .unwrap();

    // Verify files 0, 1, 2 are deleted
    for i in 0..3 {
        assert_wal_file_does_not_exist!(wal.get_file_system_accessor(), i);
    }

    // Verify files 3, 4 still exist and contain correct content
    for i in 3..5 {
        assert_wal_file_exists!(wal.get_file_system_accessor(), i);

        let expected_events = convert_to_wal_events_vector(&events[i as usize..=(i as usize)]);
        assert_wal_logs_equal!(&[i], wal.get_file_system_accessor(), expected_events);
    }
}

#[tokio::test]
async fn test_wal_truncation_with_no_files() {
    let context = TestContext::new("wal_truncation_no_files");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let mut wal = WalManager::new(&wal_config);

    // Test truncation with no files - should not panic or error
    wal.do_wal_persistence_update_for_test(Some(100))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_wal_truncation_deletes_all_files() {
    let context = TestContext::new("wal_truncation_delete_all");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let mut wal = WalManager::new(&wal_config);
    let mut events = Vec::new();

    // Test truncation that should delete all files
    add_new_example_append_event(100, None, &mut wal, &mut events);
    add_new_example_commit_event(101, None, &mut wal, &mut events);
    // first persist the wal
    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    // now truncate should delete all files
    wal.do_wal_persistence_update_for_test(Some(200))
        .await
        .unwrap(); // Higher than any LSN

    // check that the files are deleted
    assert!(!wal_file_exists(wal.get_file_system_accessor(), 0).await);
    assert!(!wal_file_exists(wal.get_file_system_accessor(), 1).await);
}

// ------------------------------------------------------------
// Truncation tests where the iceberg LSN is across xact boundaries
// ------------------------------------------------------------

#[tokio::test]
async fn test_wal_truncate_incomplete_main_xact() {
    let context = TestContext::new("wal_persist_truncate");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let mut wal = WalManager::new(&wal_config);

    let mut events = Vec::new();
    add_new_example_append_event(100, None, &mut wal, &mut events);
    add_new_example_append_event(100, None, &mut wal, &mut events);

    // first persist the wal
    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    // Use LSN 101 to truncate, but should not delete the file since main txn is not finished
    wal.do_wal_persistence_update_for_test(Some(100))
        .await
        .unwrap();

    let expected_events = convert_to_wal_events_vector(&events);
    assert_wal_logs_equal!(&[0], wal.get_file_system_accessor(), expected_events);
}

#[tokio::test]
async fn test_wal_truncate_unfinished_main_xact_multiple_commits() {
    let context = TestContext::new("wal_persist_truncate");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let mut wal = WalManager::new(&wal_config);

    let mut events = Vec::new();
    add_new_example_append_event(100, None, &mut wal, &mut events);
    add_new_example_append_event(100, None, &mut wal, &mut events);
    add_new_example_commit_event(101, None, &mut wal, &mut events);

    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    add_new_example_delete_event(101, None, &mut wal, &mut events);
    add_new_example_append_event(101, None, &mut wal, &mut events);
    add_new_example_commit_event(103, None, &mut wal, &mut events);

    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    add_new_example_append_event(103, None, &mut wal, &mut events);
    add_new_example_commit_event(110, None, &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    // Use LSN 106 to truncate, should delete the first and second file but not the third
    wal.do_wal_persistence_update_for_test(Some(106))
        .await
        .unwrap();

    // verify the first and second file are deleted
    assert_wal_file_does_not_exist!(wal.get_file_system_accessor(), 0);
    assert_wal_file_does_not_exist!(wal.get_file_system_accessor(), 1);
    assert_wal_file_exists!(wal.get_file_system_accessor(), 2);

    let expected_events = convert_to_wal_events_vector(&events[6..]);
    assert_wal_logs_equal!(&[2], wal.get_file_system_accessor(), expected_events);
}

#[tokio::test]
async fn test_wal_truncate_main_and_streaming_xact_interleave() {
    // Testing case: main xact and streaming xact are interleaving and streaming xact prevents file cleanup
    let context = TestContext::new("wal_truncate_main_and_streaming_xact_interleave");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let mut wal = WalManager::new(&wal_config);

    let mut events = Vec::new();

    // persist file 0: main xact
    add_new_example_append_event(100, None, &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    // persist file 1: streaming event
    add_new_example_append_event(100, Some(1), &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    // persist file 2: main xact
    add_new_example_commit_event(101, None, &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(Some(100))
        .await
        .unwrap();

    // persist file 3: streaming event
    add_new_example_append_event(101, Some(1), &mut wal, &mut events);
    add_new_example_commit_event(102, Some(1), &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    // truncate up to the main xact, which should delete file 0
    wal.do_wal_persistence_update_for_test(Some(101))
        .await
        .unwrap();

    // we should only have file 1, 2 and 3
    assert_wal_file_does_not_exist!(wal.get_file_system_accessor(), 0);
    assert_wal_file_exists!(wal.get_file_system_accessor(), 1);

    let expected_events = convert_to_wal_events_vector(&events[1..]);
    assert_wal_logs_equal!(&[1, 2, 3], wal.get_file_system_accessor(), expected_events);
}

#[tokio::test]
async fn test_wal_multiple_interleaved_truncations() {
    // multiple truncations should behave
    let context = TestContext::new("wal_multiple_interleaved_truncations");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let mut wal = WalManager::new(&wal_config);

    let mut events = Vec::new();

    // persist file 0:
    add_new_example_append_event(100, None, &mut wal, &mut events);
    add_new_example_append_event(100, Some(1), &mut wal, &mut events);
    add_new_example_commit_event(101, None, &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(None).await.unwrap();
    // active now: xact 1

    // persist file 1:
    add_new_example_append_event(101, Some(1), &mut wal, &mut events);
    add_new_example_commit_event(102, Some(1), &mut wal, &mut events);
    add_new_example_append_event(102, None, &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(Some(102))
        .await
        .unwrap();
    // active now: main

    // persist file 2:
    add_new_example_commit_event(103, None, &mut wal, &mut events);
    add_new_example_append_event(103, Some(2), &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(Some(102))
        .await
        .unwrap();
    // active now: xact 2

    // persist file 3:
    add_new_example_commit_event(103, Some(2), &mut wal, &mut events);
    add_new_example_append_event(104, Some(2), &mut wal, &mut events);
    add_new_example_commit_event(105, Some(2), &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(Some(103))
        .await
        .unwrap();
    // active now: none

    // lifetimes:
    // xact 1: 100 -> 102
    // xact 2: 103 -> 105
    // main: 100 -> 101, 102 -> 103

    // we should only  have file 2 and 3
    assert_wal_file_does_not_exist!(wal.get_file_system_accessor(), 0);
    assert_wal_file_does_not_exist!(wal.get_file_system_accessor(), 1);

    assert_wal_file_exists!(wal.get_file_system_accessor(), 2);
    assert_wal_file_exists!(wal.get_file_system_accessor(), 3);

    // truncate up to the main xact
    let expected_events = convert_to_wal_events_vector(&events[6..]);
    assert_wal_logs_equal!(&[2, 3], wal.get_file_system_accessor(), expected_events);
}

#[tokio::test]
async fn test_wal_stream_abort() {
    // Testing case: streaming xact is not finished and prevents file cleanup
    let context = TestContext::new("wal_stream_abort");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let mut wal = WalManager::new(&wal_config);

    let mut events = Vec::new();

    // persist file 0:
    add_new_example_append_event(100, None, &mut wal, &mut events);
    add_new_example_append_event(100, Some(1), &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    // persist file 1:
    add_new_example_append_event(100, Some(1), &mut wal, &mut events);
    add_new_example_stream_abort_event(1, &mut wal, &mut events);
    add_new_example_commit_event(101, None, &mut wal, &mut events);
    wal.do_wal_persistence_update_for_test(Some(101))
        .await
        .unwrap();

    // persist file 2:
    add_new_example_append_event(101, None, &mut wal, &mut events);
    add_new_example_append_event(101, Some(2), &mut wal, &mut events);

    wal.do_wal_persistence_update_for_test(Some(101))
        .await
        .unwrap();

    // we should only  have file 2 (abort should 'complete' transaction 1)
    assert_wal_file_does_not_exist!(wal.get_file_system_accessor(), 0);
    assert_wal_file_does_not_exist!(wal.get_file_system_accessor(), 1);

    assert_wal_file_exists!(wal.get_file_system_accessor(), 2);

    // truncate up to the main xact
    let expected_events = convert_to_wal_events_vector(&events[5..]);
    assert_wal_logs_equal!(&[2], wal.get_file_system_accessor(), expected_events);
}

#[tokio::test]
async fn test_wal_recovery_basic() {
    let context = TestContext::new("wal_recovery_basic");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let (mut wal, expected_events) = create_test_wal(wal_config).await;

    // Persist the events first
    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    // Recover events using flat stream
    let recovered_events =
        get_table_events_vector_recovery(wal.get_file_system_accessor(), 0).await;

    assert_ingestion_events_vectors_equal(&recovered_events, &expected_events);
}

#[tokio::test]
async fn test_main_tracker_merges_multiple_subset_commits_in_same_file() {
    let context = TestContext::new("wal_main_tracker_merge_subset");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let mut wal = WalManager::new(&wal_config);

    // Create two main commits in the same file (file 0). Only the highest LSN subset commit should remain.
    add_new_example_append_event(100, None, &mut wal, &mut Vec::new());
    add_new_example_commit_event(101, None, &mut wal, &mut Vec::new());
    // Another main txn entirely within the same file 0
    add_new_example_append_event(102, None, &mut wal, &mut Vec::new());
    add_new_example_commit_event(103, None, &mut wal, &mut Vec::new());

    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    // Read persisted metadata and verify the invariant
    let metadata: PersistentWalMetadata =
        WalManager::recover_from_persistent_wal_metadata(wal.get_file_system_accessor())
            .await
            .expect("metadata should exist");

    let main = metadata.get_main_transaction_tracker();
    // Only one subset commit for file 0 should remain
    assert_eq!(
        main.len(),
        1,
        "expected exactly one main commit tracked for file 0"
    );
    match &main[0] {
        WalTransactionState::Commit {
            start_file,
            completion_lsn,
            file_end,
        } => {
            assert_eq!((*start_file, *file_end), (0, 0));
            assert_eq!(*completion_lsn, 103);
        }
        other => panic!("unexpected main tracker state: {other:?}"),
    }
}

#[tokio::test]
async fn test_main_tracker_allows_one_spanning_and_one_subset_per_file() {
    let context = TestContext::new("wal_main_tracker_spanning_and_subset");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let mut wal = WalManager::new(&wal_config);

    // File 0: start a main txn but do not commit yet
    add_new_example_append_event(100, None, &mut wal, &mut Vec::new());
    wal.do_wal_persistence_update_for_test(None).await.unwrap(); // advances to file 1

    // File 1: first, commit the spanning txn (start 0 -> end 1)
    add_new_example_commit_event(101, None, &mut wal, &mut Vec::new());
    // Then, create a new main txn entirely within file 1 and commit it
    add_new_example_append_event(102, None, &mut wal, &mut Vec::new());
    add_new_example_commit_event(103, None, &mut wal, &mut Vec::new());

    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    let metadata: PersistentWalMetadata =
        WalManager::recover_from_persistent_wal_metadata(wal.get_file_system_accessor())
            .await
            .expect("metadata should exist");

    let main = metadata.get_main_transaction_tracker();
    assert_eq!(main.len(), 2, "expected two commits tracked in total");

    match &main[0] {
        WalTransactionState::Commit {
            start_file,
            completion_lsn,
            file_end,
        } => {
            assert_eq!((*start_file, *file_end), (0, 1));
            assert_eq!(*completion_lsn, 101);
        }
        other => panic!("unexpected main tracker state[0]: {other:?}"),
    }

    match &main[1] {
        WalTransactionState::Commit {
            start_file,
            completion_lsn,
            file_end,
        } => {
            assert_eq!((*start_file, *file_end), (1, 1));
            assert_eq!(*completion_lsn, 103);
        }
        other => panic!("unexpected main tracker state[1]: {other:?}"),
    }
}

#[tokio::test]
async fn test_main_tracker_keeps_highest_subset_commit_per_file() {
    let context = TestContext::new("wal_main_tracker_highest_subset");
    let wal_config =
        WalConfig::default_wal_config_local(WAL_TEST_TABLE_ID, &context.path().to_path_buf());
    let mut wal = WalManager::new(&wal_config);

    // File 0: start main txn but don't commit yet, then persist
    add_new_example_append_event(100, None, &mut wal, &mut Vec::new());
    wal.do_wal_persistence_update_for_test(None).await.unwrap(); // now file 1

    // In file 1: first, commit the spanning txn 0->1
    add_new_example_commit_event(101, None, &mut wal, &mut Vec::new());
    // Then, multiple subset commits in file 1 – only the highest should be kept
    add_new_example_append_event(102, None, &mut wal, &mut Vec::new());
    add_new_example_commit_event(103, None, &mut wal, &mut Vec::new());
    add_new_example_append_event(104, None, &mut wal, &mut Vec::new());
    add_new_example_commit_event(105, None, &mut wal, &mut Vec::new());

    wal.do_wal_persistence_update_for_test(None).await.unwrap();

    let metadata: PersistentWalMetadata =
        WalManager::recover_from_persistent_wal_metadata(wal.get_file_system_accessor())
            .await
            .expect("metadata should exist");

    let main = metadata.get_main_transaction_tracker();
    assert_eq!(main.len(), 2, "expected two commits tracked in total");

    match &main[0] {
        WalTransactionState::Commit {
            start_file,
            completion_lsn,
            file_end,
        } => {
            assert_eq!((*start_file, *file_end), (0, 1));
            assert_eq!(*completion_lsn, 101);
        }
        other => panic!("unexpected main tracker state[0]: {other:?}"),
    }

    match &main[1] {
        WalTransactionState::Commit {
            start_file,
            completion_lsn,
            file_end,
        } => {
            assert_eq!((*start_file, *file_end), (1, 1));
            assert_eq!(*completion_lsn, 105);
        }
        other => panic!("unexpected main tracker state[1]: {other:?}"),
    }
}
