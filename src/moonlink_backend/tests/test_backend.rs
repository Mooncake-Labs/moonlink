mod common;

#[cfg(test)]
mod tests {
    use crate::common::{ids_from_state, SRC_URI};

    use super::common::{
        assert_scan_ids_eq, crash_and_recover_backend_with_guard, create_backend_from_base_path,
        current_wal_lsn, get_serialized_table_config, smoke_create_and_insert, TestGuard,
        TestGuardMode, DATABASE, TABLE,
    };
    use moonlink_backend::RowEventOperation;
    use moonlink_backend::{table_status::TableStatus, REST_API_URI};
    use moonlink_backend::{EventRequest, MoonlinkBackend, RowEventRequest};
    use moonlink_metadata_store::{base_metadata_store::MetadataStoreTrait, SqliteMetadataStore};

    use arrow::datatypes::Schema as ArrowSchema;
    use arrow_schema::{DataType, Field};
    use serde_json::json;
    use serial_test::serial;
    use std::collections::{HashMap, HashSet};
    use std::time::SystemTime;
    use tempfile::TempDir;

    // ───────────────────────────── Tests ─────────────────────────────

    /// Validate `create_table` and `drop_table` across successive uses.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_moonlink_service() {
        let (guard, client) = TestGuard::new(Some("test"), true).await;
        let backend = guard.backend();
        // Till now, table already created at backend.

        // First round of table operations.
        backend
            .drop_table(DATABASE.to_string(), TABLE.to_string())
            .await
            .unwrap();
        smoke_create_and_insert(guard.tmp().unwrap(), backend, &client, SRC_URI).await;

        // Second round of table operations.
        backend
            .drop_table(DATABASE.to_string(), TABLE.to_string())
            .await
            .unwrap();
        smoke_create_and_insert(guard.tmp().unwrap(), backend, &client, SRC_URI).await;
    }

    /// Testing scenario: drop a non-existent table shouldn't crash.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_drop_non_existent_table() {
        let (guard, _client) = TestGuard::new(Some("test"), true).await;
        let backend = guard.backend();

        // We're good as long as backend doesn't crash.
        backend
            .drop_table(
                "non_existent_database".to_string(),
                "non_existent_table".to_string(),
            )
            .await
            .unwrap();
    }

    /// End-to-end: inserts should appear in `scan_table`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_scan_returns_inserted_rows() {
        let (guard, client) = TestGuard::new(Some("scan_test"), true).await;
        let backend = guard.backend();

        client
            .simple_query("INSERT INTO scan_test VALUES (1,'a'),(2,'b');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1, 2]));

        // Add one more row.
        client
            .simple_query("INSERT INTO scan_test VALUES (3,'c');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1, 2, 3]));
    }

    /// `scan_table(..., Some(lsn))` should return rows up to that LSN.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_scan_table_with_lsn() {
        let (guard, client) = TestGuard::new(Some("lsn_test"), true).await;
        let backend = guard.backend();

        client
            .simple_query("INSERT INTO lsn_test VALUES (1,'a');")
            .await
            .unwrap();
        let lsn1 = current_wal_lsn(&client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn1))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1]));

        client
            .simple_query("INSERT INTO lsn_test VALUES (2,'b');")
            .await
            .unwrap();
        let lsn2 = current_wal_lsn(&client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn2))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1, 2]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_scan_empty_table() {
        let (guard, _client) = TestGuard::new(Some("empty_table"), true).await;
        let backend = guard.backend();
        let ids = ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), None)
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::new());
    }

    /// Validates that `create_iceberg_snapshot` writes Iceberg metadata.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_create_iceberg_snapshot() {
        let (guard, client) = TestGuard::new(Some("snapshot_test"), true).await;
        let backend = guard.backend();

        client
            .simple_query("INSERT INTO snapshot_test VALUES (1,'a');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        // Read snapshot of the latest LSN to make sure all changes are synchronized to mooncake snapshot.
        backend
            .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
            .await
            .unwrap();

        // After all changes reflected at mooncake snapshot, trigger an iceberg snapshot.
        backend
            .create_snapshot(DATABASE.to_string(), TABLE.to_string(), lsn)
            .await
            .unwrap();

        // Look for any file in the Iceberg metadata dir.
        let meta_dir = guard
            .tmp()
            .unwrap()
            .path()
            .join(DATABASE)
            .join(TABLE)
            .join("metadata");
        assert!(meta_dir.exists());
        assert!(meta_dir.read_dir().unwrap().next().is_some());

        // Check table status.
        let table_statuses = backend.list_tables().await.unwrap();
        let expected_table_status = TableStatus {
            database: DATABASE.to_string(),
            table: TABLE.to_string(),
            commit_lsn: lsn,
            flush_lsn: Some(lsn),
            cardinality: 1,
            iceberg_warehouse_location: guard.tmp().unwrap().path().to_str().unwrap().to_string(),
        };
        assert_eq!(table_statuses, vec![expected_table_status]);
    }

    /// Test that replication connections are properly cleaned up and can be recreated.
    /// This validates that dropping the last table from a connection properly cleans up
    /// the replication slot, allowing new connections to be established.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_replication_connection_cleanup() {
        let (guard, client) = TestGuard::new(Some("repl_test"), true).await;
        let backend = guard.backend();

        client
            .simple_query("INSERT INTO repl_test VALUES (1,'first');")
            .await
            .unwrap();

        let lsn = current_wal_lsn(&client).await;
        let ids = ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1]));

        // Drop the table (this should clean up the replication connection)
        client
            .simple_query("DROP TABLE IF EXISTS repl_test;")
            .await
            .unwrap();
        backend
            .drop_table(DATABASE.to_string(), TABLE.to_string())
            .await
            .unwrap();

        // Second cycle: add table again, insert different data, verify it works
        client
            .simple_query("CREATE TABLE repl_test (id BIGINT PRIMARY KEY, name TEXT);")
            .await
            .unwrap();
        backend
            .create_table(
                DATABASE.to_string(),
                TABLE.to_string(),
                /*table_name=*/ "public.repl_test".to_string(),
                SRC_URI.to_string(),
                guard.get_serialized_table_config(),
                None, /* input_schema */
            )
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO repl_test VALUES (2,'second');")
            .await
            .unwrap();

        let lsn = current_wal_lsn(&client).await;
        let ids = ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
                .await
                .unwrap(),
        );
        // Should only see the new row (2), not the old one (1)
        assert_eq!(ids, HashSet::from([2]));
    }

    /// End-to-end: bulk insert (1M rows)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_bulk_insert_one_million_rows() {
        let (guard, client) = TestGuard::new(Some("bulk_test"), true).await;
        let backend = guard.backend();

        client
            .simple_query(
                "INSERT INTO bulk_test (id, name)
             SELECT gs, 'val_' || gs
             FROM generate_series(1, 1000000) AS gs;",
            )
            .await
            .unwrap();

        let lsn_after_insert = current_wal_lsn(&client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(
                    DATABASE.to_string(),
                    TABLE.to_string(),
                    Some(lsn_after_insert),
                )
                .await
                .unwrap(),
        );

        assert_eq!(ids.len(), 1_000_000);
        assert!(ids.contains(&1), "row id 1 missing");
        assert!(ids.contains(&1_000_000), "row id 1_000_000 missing");
        assert_eq!(ids.len(), 1_000_000);
    }

    /// Testing scenario: perform table creation and drop operations, and check metadata store table states.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_metadata_store() {
        let (guard, _) = TestGuard::new(Some("metadata_store"), true).await;
        // Till now, table [`metadata_store`] has been created at both row storage and column storage database.
        let backend = guard.backend();
        let database_directory = guard.tmp().as_ref().unwrap().path().to_str().unwrap();
        let metadata_store = SqliteMetadataStore::new_with_directory(database_directory)
            .await
            .unwrap();

        // Check metadata storage after table creation.
        let metadata_entries = metadata_store
            .get_all_table_metadata_entries()
            .await
            .unwrap();
        assert_eq!(metadata_entries.len(), 1);
        let table = &metadata_entries[0].table;
        assert_eq!(table, TABLE);
        assert_eq!(
            metadata_entries[0]
                .moonlink_table_config
                .iceberg_table_config
                .namespace,
            vec![format!("{DATABASE}")],
        );
        assert_eq!(
            metadata_entries[0]
                .moonlink_table_config
                .iceberg_table_config
                .table_name,
            format!("{TABLE}")
        );

        // Drop table and check metadata storage.
        backend
            .drop_table(DATABASE.to_string(), TABLE.to_string())
            .await
            .unwrap();
        let metadata_entries = metadata_store
            .get_all_table_metadata_entries()
            .await
            .unwrap();
        assert!(metadata_entries.is_empty());
    }

    /// Test recovery.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery() {
        let (mut guard, client) = TestGuard::new(Some("recovery"), true).await;
        guard.set_test_mode(TestGuardMode::Crash);
        let backend = guard.backend();

        // Drop the table that setup_backend created so we can test the full cycle
        backend
            .drop_table(DATABASE.to_string(), TABLE.to_string())
            .await
            .unwrap();

        // First cycle: add table, insert data, verify it works
        backend
            .create_table(
                DATABASE.to_string(),
                TABLE.to_string(),
                "public.recovery".to_string(),
                SRC_URI.to_string(),
                guard.get_serialized_table_config(),
                None, /* input_schema */
            )
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO recovery VALUES (1,'first');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        // Wait until changes reflected to mooncake snapshot, and force create iceberg snapshot to test mooncake/iceberg table recovery.
        backend
            .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
            .await
            .unwrap();
        backend
            .create_snapshot(DATABASE.to_string(), TABLE.to_string(), lsn)
            .await
            .unwrap();

        let (backend, _testing_directory_before_recovery) =
            crash_and_recover_backend_with_guard(guard).await;
        assert_scan_ids_eq(&backend, DATABASE.to_string(), TABLE.to_string(), lsn, [1]).await;

        // Insert new rows to make sure recovered mooncake table works as usual.
        client
            .simple_query("INSERT INTO recovery VALUES (2,'second');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        // Wait until changes reflected to mooncake snapshot, and force create iceberg snapshot to test mooncake/iceberg table recovery.
        assert_scan_ids_eq(
            &backend,
            DATABASE.to_string(),
            TABLE.to_string(),
            lsn,
            [1, 2],
        )
        .await;
    }

    /// Test recovery for rest ingested table.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery_for_rest_table() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_store_accessor =
            SqliteMetadataStore::new_with_directory(temp_dir.path().to_str().unwrap())
                .await
                .unwrap();
        let mut backend = MoonlinkBackend::new(
            temp_dir.path().to_str().unwrap().into(),
            /*data_server_uri=*/ None,
            Box::new(metadata_store_accessor),
        )
        .await
        .unwrap();
        backend.initialize_event_api().await.unwrap();

        // Create a rest table.
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "0".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "1".to_string(),
            )])),
            Field::new("age", DataType::Int32, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "2".to_string(),
            )])),
        ]);
        backend
            .create_table(
                DATABASE.to_string(),
                TABLE.to_string(),
                "public.recovery_for_rest_table".to_string(),
                REST_API_URI.to_string(),
                get_serialized_table_config(&temp_dir),
                Some(arrow_schema),
            )
            .await
            .unwrap();

        // Ingest data into table.
        let row_event_request = RowEventRequest {
            src_table_name: "public.recovery_for_rest_table".to_string(),
            operation: RowEventOperation::Insert,
            payload: json!({
                "id": 1,
                "name": "Alice Johnson",
                "age": 30
            }),
            timestamp: SystemTime::now(),
            tx: None,
        };
        let rest_event_request = EventRequest::RowRequest(row_event_request);
        backend
            .send_event_request(rest_event_request)
            .await
            .unwrap();

        // Force snapshot with LSN 2, LSN 1 = row insertion, LSN 2 = commit.
        backend
            .create_snapshot(DATABASE.to_string(), TABLE.to_string(), /*lsn=*/ 2)
            .await
            .unwrap();

        // Crash backend recovery and recreate backend.
        backend.shutdown_connection(REST_API_URI, false).await;
        backend =
            create_backend_from_base_path(temp_dir.path().to_str().unwrap().to_string()).await;
        assert_scan_ids_eq(
            &backend,
            DATABASE.to_string(),
            TABLE.to_string(),
            /*lsn=*/ 2,
            [1],
        )
        .await;
    }

    #[cfg(feature = "test-utils")]
    use super::common::{assert_scan_nonunique_ids_eq, crash_and_recover_backend};
    #[cfg(feature = "test-utils")]
    use crate::common::nonunique_ids_from_state;
    #[cfg(feature = "test-utils")]
    use rstest::*;

    /// Multiple failures and recovery from just the WAL
    #[cfg(feature = "test-utils")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery_with_wal_only() {
        let (mut guard, client) = TestGuard::new(Some("recovery"), false).await;
        guard.set_test_mode(TestGuardMode::Crash);
        let backend = guard.backend();

        // Drop the table that setup_backend created so we can test the full cycle
        backend
            .drop_table(DATABASE.to_string(), TABLE.to_string())
            .await
            .unwrap();
        backend
            .create_table(
                DATABASE.to_string(),
                TABLE.to_string(),
                "public.recovery".to_string(),
                SRC_URI.to_string(),
                guard.get_serialized_table_config(),
                None, /* input_schema */
            )
            .await
            .unwrap();

        // Insert rows, flush to WAL and then recover
        for i in 0..10 {
            client
                .simple_query(&format!("INSERT INTO recovery VALUES ({i},'{i}');"))
                .await
                .unwrap();
        }
        let lsn = current_wal_lsn(&client).await;
        backend
            .wait_for_wal_flush(DATABASE.to_string(), TABLE.to_string(), lsn)
            .await
            .unwrap();
        let (backend, testing_directory) = crash_and_recover_backend_with_guard(guard).await;
        assert_scan_nonunique_ids_eq(
            &backend,
            DATABASE.to_string(),
            TABLE.to_string(),
            lsn,
            &(0..10).map(|i| (i, 1)).collect::<HashMap<_, _>>(),
        )
        .await;

        // After recovery, ensure that insertion and reading works as expected
        client
            .simple_query("INSERT INTO recovery VALUES (10,'10');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;
        assert_scan_nonunique_ids_eq(
            &backend,
            DATABASE.to_string(),
            TABLE.to_string(),
            lsn,
            &(0..11).map(|i| (i, 1)).collect::<HashMap<_, _>>(),
        )
        .await;

        // Insert more rows, flush to WAL and recover again
        for i in 11..20 {
            client
                .simple_query(&format!("INSERT INTO recovery VALUES ({i},'{i}');"))
                .await
                .unwrap();
        }
        let lsn = current_wal_lsn(&client).await;
        backend
            .wait_for_wal_flush(DATABASE.to_string(), TABLE.to_string(), lsn)
            .await
            .unwrap();
        let backend = crash_and_recover_backend(backend, &testing_directory).await;
        assert_scan_nonunique_ids_eq(
            &backend,
            DATABASE.to_string(),
            TABLE.to_string(),
            lsn,
            &(0..20).map(|i| (i, 1)).collect::<HashMap<_, _>>(),
        )
        .await;
    }

    /// Tests recovery when postgres replay LSN is running behind WAL and we have to de-duplicate events.
    #[cfg(feature = "test-utils")]
    #[rstest]
    #[case::no_iceberg_snapshot(false)]
    #[case::with_iceberg_snapshot(true)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery_with_wal_and_incomplete_pg_replay(#[case] use_iceberg: bool) {
        use crate::common::{connect_to_postgres, create_backend_from_tempdir};

        let (mut guard, client1) = TestGuard::new(Some("recovery"), false).await;
        let (mut client2, _) = connect_to_postgres().await;

        guard.set_test_mode(TestGuardMode::Crash);

        // Set the logical decoding work mem to a small value to force a streaming xact
        client1
            .simple_query("ALTER SYSTEM SET logical_decoding_work_mem = '64kB';")
            .await
            .unwrap();
        // Reload configuration in a separate statement.
        client1
            .simple_query("SELECT pg_reload_conf();")
            .await
            .unwrap();
        let backend = guard.backend();

        // Drop the table that setup_backend created so we can test the full cycle
        backend
            .drop_table(DATABASE.to_string(), TABLE.to_string())
            .await
            .unwrap();
        backend
            .create_table(
                DATABASE.to_string(),
                TABLE.to_string(),
                "public.recovery".to_string(),
                SRC_URI.to_string(),
                guard.get_serialized_table_config(),
                None, /* input_schema */
            )
            .await
            .unwrap();

        // here we start a long transaction WITHOUT committing - this will go into the WAL but
        // should not be reapplied on recovery because it is not yet committed when the recovery happens
        // A streaming transaction should be triggered here because of the lower work mem setting
        // Simultaneously, we insert a first batch of rows that should go into WAL, and that should flush both
        // the streaming xact events and the main transaction events into the WAL.
        let long_transaction_query = "
        INSERT INTO recovery (id, name)
            SELECT gs, 'val_' || gs
            FROM generate_series(0, 9999) AS gs;";
        let transaction = client2.transaction().await.unwrap();
        transaction
            .execute(long_transaction_query, &[])
            .await
            .unwrap();
        for i in 0..10 {
            client1
                .simple_query(&format!("INSERT INTO recovery VALUES ({i},'{i}');"))
                .await
                .unwrap();
            if use_iceberg && i == 5 {
                // Take an iceberg snapshot and flush to WAL
                let lsn = current_wal_lsn(&client1).await;
                backend
                    .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
                    .await
                    .unwrap();
                backend
                    .create_snapshot(DATABASE.to_string(), TABLE.to_string(), lsn)
                    .await
                    .unwrap();
            }
        }
        let completed_lsn = current_wal_lsn(&client1).await;
        backend
            .wait_for_wal_flush(DATABASE.to_string(), TABLE.to_string(), completed_lsn)
            .await
            .unwrap();

        // Shutdown connection, THEN commit transaction while the backend is not running
        // On recovery, both the WAL and postgres should be replaying the same events, but
        // we test here for deduplication of events.
        guard.backend().shutdown_connection(SRC_URI, false).await;
        let testing_directory = guard.take_test_directory();
        drop(guard);
        transaction.commit().await.unwrap();
        let lsn_after_commit = current_wal_lsn(&client1).await;
        let backend = create_backend_from_tempdir(&testing_directory).await;

        // we should only expect 1 of each row if we deduplicated correctly
        let ids = nonunique_ids_from_state(
            &backend
                .scan_table(
                    DATABASE.to_string(),
                    TABLE.to_string(),
                    Some(lsn_after_commit),
                )
                .await
                .unwrap(),
        );
        for i in 0..10 {
            assert_eq!(ids.get(&i), Some(&2), "i: {i}");
        }
        for i in 10..10000 {
            assert_eq!(ids.get(&i), Some(&1), "i: {i}");
        }

        // reset the postgres logical decoding
        client1
            .simple_query("RESET logical_decoding_work_mem;")
            .await
            .unwrap();
        client1
            .simple_query("SELECT pg_reload_conf();")
            .await
            .unwrap();
    }

    /// Tests recovery when postgres has events that were created
    /// when the backend was not running.
    #[cfg(feature = "test-utils")]
    #[rstest]
    #[case::no_iceberg_snapshot(false)]
    #[case::with_iceberg_snapshot(true)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery_with_wal_pg_runs_ahead(#[case] use_iceberg: bool) {
        use crate::common::create_backend_from_tempdir;

        let (mut guard, client) = TestGuard::new(Some("recovery"), false).await;
        guard.set_test_mode(TestGuardMode::Crash);
        let backend = guard.backend();

        // Drop the table that setup_backend created so we can test the full cycle
        backend
            .drop_table(DATABASE.to_string(), TABLE.to_string())
            .await
            .unwrap();

        backend
            .create_table(
                DATABASE.to_string(),
                TABLE.to_string(),
                "public.recovery".to_string(),
                SRC_URI.to_string(),
                guard.get_serialized_table_config(),
                None, /* input_schema */
            )
            .await
            .unwrap();

        // We let postgres run ahead of the WAL. Here, we only ensure that the WAL captures up to
        // the first 10 rows.
        for i in 0..10 {
            client
                .simple_query(&format!("INSERT INTO recovery VALUES ({i},'{i}');"))
                .await
                .unwrap();
        }
        let wal_flush_lsn = current_wal_lsn(&client).await;
        backend
            .wait_for_wal_flush(DATABASE.to_string(), TABLE.to_string(), wal_flush_lsn)
            .await
            .unwrap();

        if use_iceberg {
            // Take an iceberg snapshot
            let lsn = current_wal_lsn(&client).await;
            backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
                .await
                .unwrap();
            backend
                .create_snapshot(DATABASE.to_string(), TABLE.to_string(), lsn)
                .await
                .unwrap();
        }

        for i in 10..20 {
            client
                .simple_query(&format!("INSERT INTO recovery VALUES ({i},'{i}');"))
                .await
                .unwrap();
        }

        // Insert more rows while the backend is not running
        guard.backend().shutdown_connection(SRC_URI, false).await;
        for i in 20..30 {
            client
                .simple_query(&format!("INSERT INTO recovery VALUES ({i},'{i}');"))
                .await
                .unwrap();
        }
        let lsn_run_ahead = current_wal_lsn(&client).await;
        let testing_directory = guard.take_test_directory();
        let backend = create_backend_from_tempdir(&testing_directory).await;

        let expected = (0..30).map(|i| (i, 1)).collect::<HashMap<_, _>>();
        assert_scan_nonunique_ids_eq(
            &backend,
            DATABASE.to_string(),
            TABLE.to_string(),
            lsn_run_ahead,
            &expected,
        )
        .await;
    }

    /// Multiple failures and recovery interleaving WAL and iceberg snapshot
    /// Tests case where WAL and iceberg snapshot have captured the same events
    /// and case when WAL has captured more events than the iceberg snapshot
    #[cfg(feature = "test-utils")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery_with_wal_and_iceberg_snapshot() {
        let (mut guard, client) = TestGuard::new(Some("recovery"), false).await;
        guard.set_test_mode(TestGuardMode::Crash);
        let backend = guard.backend();

        // Drop the table that setup_backend created so we can test the full cycle
        backend
            .drop_table(DATABASE.to_string(), TABLE.to_string())
            .await
            .unwrap();
        backend
            .create_table(
                DATABASE.to_string(),
                TABLE.to_string(),
                "public.recovery".to_string(),
                SRC_URI.to_string(),
                guard.get_serialized_table_config(),
                None, /* input_schema */
            )
            .await
            .unwrap();

        // Take an iceberg snapshot and a WAL flush that are caught up to the same LSN, then test recovery
        for i in 0..10 {
            client
                .simple_query(&format!("INSERT INTO recovery VALUES ({i},'{i}');"))
                .await
                .unwrap();
        }
        let lsn = current_wal_lsn(&client).await;
        backend
            .wait_for_wal_flush(DATABASE.to_string(), TABLE.to_string(), lsn)
            .await
            .unwrap();
        backend
            .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
            .await
            .unwrap();
        backend
            .create_snapshot(DATABASE.to_string(), TABLE.to_string(), lsn)
            .await
            .unwrap();
        let (backend, testing_directory) = crash_and_recover_backend_with_guard(guard).await;
        assert_scan_nonunique_ids_eq(
            &backend,
            DATABASE.to_string(),
            TABLE.to_string(),
            lsn,
            &(0..10).map(|i| (i, 1)).collect::<HashMap<_, _>>(),
        )
        .await;

        // After recovery, ensure that insertion and reading works as expected
        client
            .simple_query("INSERT INTO recovery VALUES (10,'10');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;
        assert_scan_nonunique_ids_eq(
            &backend,
            DATABASE.to_string(),
            TABLE.to_string(),
            lsn,
            &(0..11).map(|i| (i, 1)).collect::<HashMap<_, _>>(),
        )
        .await;

        // Take an iceberg snapshot, but let the WAL run ahead of it, then test recovery
        let lsn = current_wal_lsn(&client).await;
        backend
            .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
            .await
            .unwrap();
        backend
            .create_snapshot(DATABASE.to_string(), TABLE.to_string(), lsn)
            .await
            .unwrap();
        for i in 11..20 {
            client
                .simple_query(&format!("INSERT INTO recovery VALUES ({i},'{i}');"))
                .await
                .unwrap();
        }
        let lsn = current_wal_lsn(&client).await;
        backend
            .wait_for_wal_flush(DATABASE.to_string(), TABLE.to_string(), lsn)
            .await
            .unwrap();
        let backend = crash_and_recover_backend(backend, &testing_directory).await;
        assert_scan_nonunique_ids_eq(
            &backend,
            DATABASE.to_string(),
            TABLE.to_string(),
            lsn,
            &(0..20).map(|i| (i, 1)).collect::<HashMap<_, _>>(),
        )
        .await;
    }

    /// Test scenario: perform a few requests on non-existent databases and tables, make sure error is correctly propagated.
    #[cfg(feature = "test-utils")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_on_non_existent_table() {
        const NON_EXISTENT_TABLE: &str = "non-existent-table";

        let (mut guard, client) = TestGuard::new(Some("non_existent_table"), true).await;
        guard.set_test_mode(TestGuardMode::Crash);

        let lsn = current_wal_lsn(&client).await;
        let non_existent_schema: &str = "non-existent-schema";

        // Scan table on non-existent database.
        let backend = guard.backend();
        let res = backend
            .scan_table(
                non_existent_schema.to_string(),
                NON_EXISTENT_TABLE.to_string(),
                Some(lsn),
            )
            .await;
        assert!(res.is_err());

        // Scan table on non-existent table.
        let res = backend
            .scan_table(
                DATABASE.to_string(),
                NON_EXISTENT_TABLE.to_string(),
                Some(lsn),
            )
            .await;
        assert!(res.is_err());

        // Read schema on non-existent database.
        let res = backend
            .get_table_schema(
                non_existent_schema.to_string(),
                NON_EXISTENT_TABLE.to_string(),
            )
            .await;
        assert!(res.is_err());

        // Read schema on non-existent table.
        let res = backend
            .get_table_schema(DATABASE.to_string(), NON_EXISTENT_TABLE.to_string())
            .await;
        assert!(res.is_err());
    }
}
