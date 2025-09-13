mod common;

#[cfg(feature = "test-utils")]
#[cfg(test)]
mod tests {
    use super::common::{
        assert_scan_nonunique_ids_eq, crash_and_recover_backend,
        crash_and_recover_backend_with_guard, current_wal_lsn, get_database_uri, TestGuard,
        TestGuardMode, DATABASE, TABLE,
    };
    use rstest::rstest;
    use serial_test::serial;
    use std::collections::HashMap;

    use crate::common::nonunique_ids_from_state;

    /// Multiple failures and recovery from just the WAL

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery_with_wal_only() {
        let uri = get_database_uri();
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
                uri,
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

    #[rstest]
    #[case::no_iceberg_snapshot(false)]
    #[case::with_iceberg_snapshot(true)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery_with_wal_and_incomplete_pg_replay(#[case] use_iceberg: bool) {
        use crate::common::{connect_to_postgres, create_backend_from_tempdir};

        let uri = get_database_uri();
        let (mut guard, client1) = TestGuard::new(Some("recovery"), false).await;
        let (mut client2, _) = connect_to_postgres(&uri).await;

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
                uri.clone(),
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
        guard.backend().shutdown_connection(&uri, false).await;
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

    #[rstest]
    #[case::no_iceberg_snapshot(false)]
    #[case::with_iceberg_snapshot(true)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery_with_wal_pg_runs_ahead(#[case] use_iceberg: bool) {
        use crate::common::create_backend_from_tempdir;

        let uri = get_database_uri();
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
                uri.clone(),
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
        guard.backend().shutdown_connection(&uri, false).await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery_with_wal_and_iceberg_snapshot() {
        let uri = get_database_uri();
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
                uri,
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
}
