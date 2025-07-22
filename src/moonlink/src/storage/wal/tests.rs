use crate::storage::mooncake_table::test_utils::{test_row, TestContext};
use crate::storage::wal::test_utils::*;
use crate::storage::wal::Wal;
use crate::table_notify::TableEvent;
use crate::FileSystemConfig;
use futures::StreamExt;

#[tokio::test]
async fn test_wal_insert_persist_files() {
    let context = TestContext::new("wal_persist");
    let (wal, expected_events) = create_test_wal(&context).await;

    // Persist and verify file number
    let file_number = wal.persist().await.unwrap();
    assert_eq!(file_number, Some(0));

    // Check file exists and has content
    let wal_file_path = context.path().join("wal_0.json");
    assert!(wal_file_path.exists());

    let expected_wal_events = convert_to_wal_events_vector(expected_events);
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
    let wal = Wal::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    // Persist without any events
    let file_number = wal.persist().await.unwrap();
    assert_eq!(file_number, None); // Should return None when no file is created

    // No file should be created for empty WAL
    let wal_file_path = context.path().join("wal_0.json");
    assert!(!wal_file_path.exists());
}

#[tokio::test]
async fn test_wal_file_numbering_sequence() {
    let context = TestContext::new("wal_file_numbering");
    let wal = Wal::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    let row = test_row(1, "Alice", 30);

    // First persist
    let event1 = TableEvent::Append {
        row: row.clone(),
        xact_id: None,
        lsn: 100,
        is_copied: false,
    };
    wal.insert(&event1).await;
    let file_number1 = wal.persist().await.unwrap();
    assert_eq!(file_number1, Some(0));

    // Second persist
    let event2 = TableEvent::Append {
        row: row.clone(),
        xact_id: None,
        lsn: 101,
        is_copied: false,
    };
    wal.insert(&event2).await;
    let file_number2 = wal.persist().await.unwrap();
    assert_eq!(file_number2, Some(1));

    // Third persist
    let event3 = TableEvent::Append {
        row: row.clone(),
        xact_id: None,
        lsn: 102,
        is_copied: false,
    };
    wal.insert(&event3).await;
    let file_number3 = wal.persist().await.unwrap();
    assert_eq!(file_number3, Some(2));

    // Verify files exist
    assert!(context.path().join("wal_0.json").exists());
    assert!(context.path().join("wal_1.json").exists());
    assert!(context.path().join("wal_2.json").exists());

    // Use the new helper infrastructure to verify file contents
    let expected_wal_events_0 = convert_to_wal_events_vector(vec![event1]);
    let expected_wal_events_1 = convert_to_wal_events_vector(vec![event2]);
    let expected_wal_events_2 = convert_to_wal_events_vector(vec![event3]);

    check_wal_logs_equal(
        &["wal_0.json"],
        wal.get_file_system_accessor(),
        expected_wal_events_0,
    )
    .await;
    check_wal_logs_equal(
        &["wal_1.json"],
        wal.get_file_system_accessor(),
        expected_wal_events_1,
    )
    .await;
    check_wal_logs_equal(
        &["wal_2.json"],
        wal.get_file_system_accessor(),
        expected_wal_events_2,
    )
    .await;
}

#[tokio::test]
async fn test_wal_truncation_deletes_files() {
    let context = TestContext::new("wal_truncation");
    let wal = Wal::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    let row = test_row(1, "Alice", 30);

    // Create multiple WAL files with known events
    for i in 0..5 {
        let event = TableEvent::Append {
            row: row.clone(),
            xact_id: None,
            lsn: 100 + i,
            is_copied: false,
        };
        wal.insert(&event).await;
        wal.persist().await.unwrap();
    }

    // Truncate from LSN 102 (should delete files 0, 1 - files with LSN < 102)
    wal.truncate_flushed_wals(102).await.unwrap();

    // Verify files 0, 1 are deleted
    assert!(!context.path().join("wal_0.json").exists());
    assert!(!context.path().join("wal_1.json").exists());

    // Verify files 2, 3, 4 still exist and contain correct content
    for i in 2..5 {
        assert!(context.path().join(format!("wal_{i}.json")).exists());

        let expected_event = TableEvent::Append {
            row: row.clone(),
            xact_id: None,
            lsn: 100 + i,
            is_copied: false,
        };
        let expected_events = convert_to_wal_events_vector(vec![expected_event]);
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
    let wal = Wal::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    // Test truncation with no files - should not panic or error
    wal.truncate_flushed_wals(100).await.unwrap();
}

#[tokio::test]
async fn test_wal_truncation_deletes_all_files() {
    let context = TestContext::new("wal_truncation_delete_all");
    let wal = Wal::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    // Test truncation that should delete all files
    let row = test_row(1, "Alice", 30);
    wal.insert(&TableEvent::Append {
        row: row.clone(),
        xact_id: None,
        lsn: 100,
        is_copied: false,
    })
    .await;
    wal.persist().await.unwrap();

    wal.truncate_flushed_wals(200).await.unwrap(); // Higher than any LSN
    assert!(!context.path().join("wal_0.json").exists());
}

#[tokio::test]
async fn test_wal_persist_and_truncate() {
    let context = TestContext::new("wal_persist_truncate");
    let wal = Wal::new(FileSystemConfig::FileSystem {
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
        wal.insert(event).await;
    }

    // Persist and truncate in one operation
    // Use LSN 102 to truncate, which will delete the file since its highest_lsn is 101 < 102
    let file_number = wal.persist_and_truncate(Some(102)).await.unwrap();
    assert_eq!(file_number, Some(0));

    // File should be created but then deleted due to truncation
    assert!(!context.path().join("wal_0.json").exists());
}

#[tokio::test]
async fn test_wal_recovery_basic() {
    let context = TestContext::new("wal_recovery_basic");
    let (wal, expected_events) = create_test_wal(&context).await;

    // Persist the events first
    wal.persist().await.unwrap();

    // Verify file contents using helper infrastructure
    let expected_wal_events = convert_to_wal_events_vector(expected_events);
    check_wal_logs_equal(
        &["wal_0.json"],
        wal.get_file_system_accessor(),
        expected_wal_events,
    )
    .await;

    // Recover events using flat stream
    let recovered_events = get_table_events_vector_recovery(&wal, 0, 100).await;

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
    let (wal, expected_events) = create_test_wal(&context).await;

    // Persist the events first
    wal.persist().await.unwrap();

    // Recover from >= LSN 102 (should get 3 events: LSN 102, 103, 104) using flat stream
    let recovered_events = get_table_events_vector_recovery(&wal, 0, 102).await;

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
    let wal = Wal::new(FileSystemConfig::FileSystem {
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
        wal.insert(event).await;
    }

    wal.persist().await.unwrap();

    // Recover and verify all event types
    let mut recovered_events = Vec::new();
    let mut stream = wal.recover_flushed_wals(0, 0);
    while let Some(result) = stream.next().await {
        match result {
            Ok(event_batch) => recovered_events.extend(event_batch),
            Err(e) => panic!("Recovery failed: {e:?}"),
        }
    }

    // Verify recovered events match original events by creating them again
    let expected_recovered_events = vec![
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
            is_copied: true,
        },
        TableEvent::Delete {
            row: row1,
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
    assert_ingestion_events_vectors_equal(&recovered_events, &expected_recovered_events);
}

#[tokio::test]
async fn test_wal_concurrent_access() {
    let context = TestContext::new("wal_concurrent");
    let wal = std::sync::Arc::new(Wal::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    }));

    let row = test_row(1, "Alice", 30);

    // Task 1: Insert all events serially (one after another due to internal Mutex)
    let insert_handle = {
        let wal_clone = wal.clone();
        let row_clone = row.clone();
        tokio::spawn(async move {
            for i in 0..50 {
                wal_clone
                    .insert(&TableEvent::Append {
                        row: row_clone.clone(),
                        xact_id: None,
                        lsn: i,
                        is_copied: false,
                    })
                    .await;
                tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
            }
        })
    };

    // Task 2: Concurrent persists that can happen while inserts are still ongoing
    let persist_handle = {
        let wal_clone = wal.clone();
        tokio::spawn(async move {
            // Try to persist multiple times while inserts are happening
            for _ in 0..10 {
                let _ = wal_clone.persist().await;
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        })
    };

    // Wait for both tasks to complete
    insert_handle.await.unwrap();
    persist_handle.await.unwrap();

    // Final persist to ensure all remaining events are flushed
    let _ = wal.persist().await.unwrap();

    // Recover all events and verify we got all 50 events
    let mut events = Vec::new();
    let mut stream = wal.recover_flushed_wals(0, 0);
    while let Some(result) = stream.next().await {
        match result {
            Ok(event_batch) => events.extend(event_batch),
            Err(e) => panic!("Recovery failed: {e:?}"),
        }
    }

    assert_eq!(events.len(), 50); // 50 events total

    // Verify all LSNs are present (0-49)
    let mut lsns: Vec<u64> = events
        .iter()
        .filter_map(|event| event.get_lsn_for_ingest_event())
        .collect();
    lsns.sort();
    assert_eq!(lsns, (0..50).collect::<Vec<u64>>());
}

#[tokio::test]
async fn test_wal_multiple_persist_truncate_recovery() {
    let context = TestContext::new("wal_cycles");
    let wal = Wal::new(FileSystemConfig::FileSystem {
        root_directory: context.path().to_str().unwrap().to_string(),
    });

    let row = test_row(1, "Alice", 30);
    let inner_iterations = 3;
    let outer_iterations = 3;

    // Multiple cycles of persist and truncate
    for cycle in 0..outer_iterations {
        // Add events for this cycle
        for i in 0..inner_iterations {
            wal.insert(&TableEvent::Append {
                row: row.clone(),
                xact_id: None,
                lsn: cycle * 10 + i,
                is_copied: false,
            })
            .await;
        }
        // the last snapshot file contains (cycle * 10) + 1, (cycle * 10) + 2, (cycle * 10) + 3 with a file number of (cycle)
        let last_snapshot_file_number = wal
            .persist_and_truncate(Some(cycle * 10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(Some(last_snapshot_file_number), Some(cycle));

        if cycle > 0 {
            for i in 0..(cycle - 1) {
                assert!(!context.path().join(format!("wal_{i}.json")).exists());
            }
        }

        let mut expected_events = Vec::new();
        for i in 0..inner_iterations {
            expected_events.push(TableEvent::Append {
                row: row.clone(),
                xact_id: None,
                lsn: (cycle * 10) + i,
                is_copied: false,
            });
        }

        let recovered_events =
            get_table_events_vector_recovery(&wal, last_snapshot_file_number, cycle * 10).await;
        assert_ingestion_events_vectors_equal(&recovered_events, &expected_events);
    }
}
