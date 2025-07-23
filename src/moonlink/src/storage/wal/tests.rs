use crate::storage::mooncake_table::test_utils::{test_row, TestContext};
use crate::storage::wal::test_utils::*;
use crate::storage::wal::WalManager;
use crate::table_notify::TableEvent;
use crate::FileSystemConfig;
use futures::StreamExt;
use tokio::fs;

#[tokio::test]
async fn test_wal_insert_persist_files() {
    let context = TestContext::new("wal_persist");
    let (mut wal, expected_events) = create_test_wal(&context).await;

    // Persist and verify file number
    wal.persist_and_truncate(None).await.unwrap();

    // Check file exists and has content
    let wal_file_path = context.path().join("wal_0.json");
    assert!(fs::try_exists(&wal_file_path).await.unwrap());

    let expected_wal_events = convert_to_wal_events_vector(&expected_events);
    check_wal_logs_equal(
        &["wal_0.json"],
        wal.get_file_system_accessor(),
        expected_wal_events,
    )
    .await;
}

#[tokio::test]
async fn test_wal_empty_persist() {
    let context = TestContext::new("wal_empty_persist");
    let mut wal = WalManager::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    // Persist without any events
    wal.persist_and_truncate(None).await.unwrap();

    // No file should be created for empty WAL
    assert!(local_dir_is_empty(&context.path()).await);
}

#[tokio::test]
async fn test_wal_file_numbering_sequence() {
    let context = TestContext::new("wal_file_numbering");
    let mut wal = WalManager::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    let row = test_row(1, "Alice", 30);
    let mut events = Vec::new();

    // First loop: push and persist events
    for i in 0..3 {
        let event = TableEvent::Append {
            row: row.clone(),
            xact_id: None,
            lsn: 100 + i,
            is_copied: false,
        };
        wal.push(&event);
        wal.persist_and_truncate(None).await.unwrap();
        events.push(event);
    }

    // Second loop: check file existence and contents
    for i in 0..3 {
        let file_name = format!("wal_{i}.json");
        assert!(fs::try_exists(&context.path().join(&file_name))
            .await
            .unwrap());
        let expected_wal_events = convert_to_wal_events_vector(&events[i..=i]);
        check_wal_logs_equal(
            &[&file_name],
            wal.get_file_system_accessor(),
            expected_wal_events,
        )
        .await;
    }
}

#[tokio::test]
async fn test_wal_truncation_deletes_files() {
    let context = TestContext::new("wal_truncation");
    let mut wal = WalManager::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    let row = test_row(1, "Alice", 30);

    // Create multiple WAL files with known events
    let mut events = Vec::new();
    for i in 0..5 {
        let event = TableEvent::Append {
            row: row.clone(),
            xact_id: None,
            lsn: 100 + i,
            is_copied: false,
        };
        wal.push(&event);
        wal.persist_and_truncate(None).await.unwrap();
        events.push(event);
    }

    // Truncate from LSN 102 (should delete files 0, 1 - files with LSN < 102)
    wal.persist_and_truncate(Some(102)).await.unwrap();

    // Verify files 0, 1 are deleted
    assert!(!fs::try_exists(&context.path().join("wal_0.json"))
        .await
        .unwrap());
    assert!(!fs::try_exists(&context.path().join("wal_1.json"))
        .await
        .unwrap());

    // Verify files 2, 3, 4 still exist and contain correct content
    for i in 2..5 {
        assert!(
            fs::try_exists(&context.path().join(format!("wal_{i}.json")))
                .await
                .unwrap()
        );

        let expected_events = convert_to_wal_events_vector(&events[i..=i]);
        check_wal_logs_equal(
            &[&format!("wal_{i}.json")],
            wal.get_file_system_accessor(),
            expected_events,
        )
        .await;
    }
}

#[tokio::test]
async fn test_wal_truncation_with_no_files() {
    let context = TestContext::new("wal_truncation_no_files");
    let mut wal = WalManager::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    // Test truncation with no files - should not panic or error
    wal.persist_and_truncate(Some(100)).await.unwrap();
}

#[tokio::test]
async fn test_wal_truncation_deletes_all_files() {
    let context = TestContext::new("wal_truncation_delete_all");
    let mut wal = WalManager::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    // Test truncation that should delete all files
    let row = test_row(1, "Alice", 30);
    wal.push(&TableEvent::Append {
        row: row.clone(),
        xact_id: None,
        lsn: 100,
        is_copied: false,
    });
    // first persist the wal
    wal.persist_and_truncate(None).await.unwrap();

    // now truncate should delete all files
    wal.persist_and_truncate(Some(200)).await.unwrap(); // Higher than any LSN
    assert!(local_dir_is_empty(&context.path()).await);
}

#[tokio::test]
async fn test_wal_persist_and_truncate() {
    let context = TestContext::new("wal_persist_truncate");
    let mut wal = WalManager::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    let row = test_row(1, "Alice", 30);

    // Add events
    let events = vec![
        TableEvent::Append {
            row: row.clone(),
            xact_id: None,
            lsn: 100,
            is_copied: false,
        },
        TableEvent::Append {
            row: row.clone(),
            xact_id: None,
            lsn: 101,
            is_copied: false,
        },
    ];

    for event in &events {
        wal.push(event);
    }
    // first persist the wal
    wal.persist_and_truncate(None).await.unwrap();

    // Use LSN 102 to truncate, which will delete the file since its highest_lsn is 101 < 102
    wal.persist_and_truncate(Some(102)).await.unwrap();

    // File should be created but then deleted due to truncation
    assert!(local_dir_is_empty(&context.path()).await);
}

#[tokio::test]
async fn test_wal_recovery_basic() {
    let context = TestContext::new("wal_recovery_basic");
    let (mut wal, expected_events) = create_test_wal(&context).await;

    // Persist the events first
    wal.persist_and_truncate(None).await.unwrap();

    // Verify file contents
    let expected_wal_events = convert_to_wal_events_vector(&expected_events);
    check_wal_logs_equal(
        &["wal_0.json"],
        wal.get_file_system_accessor(),
        expected_wal_events,
    )
    .await;

    // Recover events using flat stream
    let recovered_events =
        get_table_events_vector_recovery(wal.get_file_system_accessor(), 0, 100).await;

    // Create expected events again for comparison (since we consumed them above)
    let row = test_row(1, "Alice", 30);
    let expected_events_for_comparison: Vec<TableEvent> = (0..5)
        .map(|i| TableEvent::Append {
            row: row.clone(),
            xact_id: None,
            lsn: 100 + i,
            is_copied: false,
        })
        .collect();

    assert_ingestion_events_vectors_equal(&recovered_events, &expected_events_for_comparison);
}

#[tokio::test]
async fn test_wal_recovery_with_lsn_filtering() {
    let context = TestContext::new("wal_recovery_lsn_filter");
    let (mut wal, expected_events) = create_test_wal(&context).await;

    // Persist the events first
    wal.persist_and_truncate(None).await.unwrap();

    // Recover from >= LSN 102 (should get 3 events: LSN 102, 103, 104) using flat stream
    let recovered_events =
        get_table_events_vector_recovery(wal.get_file_system_accessor(), 0, 102).await;

    assert_eq!(recovered_events.len(), 3);
    // Verify all events have LSN >= 102
    for event in &recovered_events {
        if let Some(lsn) = event.get_lsn_for_ingest_event() {
            assert!(lsn >= 102);
        }
    }

    // Slice the expected events to get only those with LSN >= 102 (indices 2, 3, 4)
    let expected_filtered_events = &expected_events[2..5];
    assert_ingestion_events_vectors_equal(&recovered_events, expected_filtered_events);
}

#[tokio::test]
async fn test_wal_recovery_mixed_event_types() {
    let context = TestContext::new("wal_mixed_events");
    let mut wal = WalManager::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    let row1 = test_row(1, "Alice", 30);
    let row2 = test_row(2, "Bob", 25);

    // Test all event types
    let events = vec![
        TableEvent::Append {
            row: row1.clone(),
            xact_id: Some(1),
            lsn: 100,
            is_copied: false,
        },
        TableEvent::Append {
            row: row2.clone(),
            xact_id: Some(1),
            lsn: 101,
            is_copied: true, // Test copied flag
        },
        TableEvent::Delete {
            row: row1.clone(),
            lsn: 102,
            xact_id: Some(1),
        },
        TableEvent::Commit {
            lsn: 103,
            xact_id: Some(1),
        },
        TableEvent::StreamAbort { xact_id: 1 },
        TableEvent::StreamFlush { xact_id: 2 },
    ];

    for event in &events {
        wal.push(event);
    }

    wal.persist_and_truncate(None).await.unwrap();

    // Recover and verify all event types
    let mut recovered_events = Vec::new();
    let mut stream = WalManager::recover_flushed_wals(wal.get_file_system_accessor(), 0, 0);
    while let Some(result) = stream.next().await {
        match result {
            Ok(event_batch) => recovered_events.extend(event_batch),
            Err(e) => panic!("Recovery failed: {e:?}"),
        }
    }
    assert_ingestion_events_vectors_equal(&recovered_events, &events);
}

#[tokio::test]
async fn test_wal_multiple_persist_truncate_recovery() {
    let context = TestContext::new("wal_cycles");
    let mut wal = WalManager::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    let row = test_row(1, "Alice", 30);
    let inner_iterations = 3;
    let outer_iterations = 3;

    // Multiple cycles of persist and truncate
    for cycle in 0..outer_iterations {
        // Add events for this cycle
        let mut expected_events = Vec::new();
        for i in 0..inner_iterations {
            let event = TableEvent::Append {
                row: row.clone(),
                xact_id: None,
                lsn: (cycle * 10) + i,
                is_copied: false,
            };
            wal.push(&event);
            expected_events.push(event);
        }

        // the last snapshot file contains (cycle * 10) + 0, (cycle * 10) + 1, (cycle * 10) + 2 with a file number of (cycle)
        // we want to delete all files before this cycle
        if cycle > 0 {
            wal.persist_and_truncate(Some((cycle * 10) - 1))
                .await
                .unwrap();
            for i in 0..(cycle - 1) {
                assert!(
                    !fs::try_exists(&context.path().join(format!("wal_{i}.json")))
                        .await
                        .unwrap()
                );
            }
        } else {
            wal.persist_and_truncate(None).await.unwrap();
        }
        let last_snapshot_file_number = cycle;
        let recovered_events = get_table_events_vector_recovery(
            wal.get_file_system_accessor(),
            last_snapshot_file_number,
            cycle * 10,
        )
        .await;
        assert_ingestion_events_vectors_equal(&recovered_events, &expected_events);
    }
}
