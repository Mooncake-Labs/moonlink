mod common;

#[cfg(test)]
mod tests {
    use super::common::{
        assert_scan_ids_eq, connect_to_postgres, crash_and_recover_backend_with_guard,
        create_backend_from_base_path, current_wal_lsn, get_database_uri,
        get_serialized_table_config, TestGuard, TestGuardMode, DATABASE, TABLE,
    };
    use moonlink_metadata_store::{base_metadata_store::MetadataStoreTrait, SqliteMetadataStore};
    use std::time::Duration;
    use tempfile::TempDir;

    use serial_test::serial;

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
        let uri = get_database_uri();
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
                uri,
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

    /// Set in the child process of [`restart_right_after_create_table`]: the base path and the table
    /// config of the backend that creates the mirror, and how many persistences fail first.
    const CRASH_CHILD_BASE_PATH: &str = "MOONLINK_TEST_CRASH_CHILD_BASE_PATH";
    const CRASH_CHILD_TABLE_CONFIG: &str = "MOONLINK_TEST_CRASH_CHILD_TABLE_CONFIG";
    const CRASH_CHILD_PERSISTENCE_FAILURES: &str = "MOONLINK_TEST_CRASH_CHILD_PERSISTENCE_FAILURES";
    /// The child creates a mirror of a small table; it is killed (and the test fails) after this.
    const CRASH_CHILD_TIMEOUT: Duration = Duration::from_secs(120);

    /// Testing scenario: moonlink dies right after create_table returns. Recovery resumes the
    /// mirror from its iceberg table and the replication slot, which holds no change from before
    /// the copy, so create_table must not return before the copied rows are persisted. The mirror is
    /// created in a child process (this test binary, running `test_name`) that exits the moment
    /// create_table returns: unlike shutting down a backend in this process, that also stops its
    /// table handlers, as a crash does. The first `persistence_failures` persistences in the child
    /// fail with a transient error, which create_table has to wait out.
    async fn restart_right_after_create_table(test_name: &str, persistence_failures: usize) {
        let uri = get_database_uri();
        if let Ok(base_path) = std::env::var(CRASH_CHILD_BASE_PATH) {
            let failures: usize = std::env::var(CRASH_CHILD_PERSISTENCE_FAILURES)
                .unwrap()
                .parse()
                .unwrap();
            moonlink::test_hooks::inject_persistence_failures(failures);
            let backend = create_backend_from_base_path(base_path).await;
            backend
                .create_table(
                    DATABASE.to_string(),
                    TABLE.to_string(),
                    "public.copy_restart".to_string(),
                    uri,
                    std::env::var(CRASH_CHILD_TABLE_CONFIG).unwrap(),
                    None, /* input_schema */
                )
                .await
                .unwrap();
            assert_eq!(
                moonlink::test_hooks::injected_persistence_failures_left(),
                0
            );
            std::process::exit(0);
        }

        let testing_directory = TempDir::new().unwrap();
        let base_path = testing_directory.path().to_str().unwrap().to_string();
        let (client, _) = connect_to_postgres(&uri).await;
        let _ = client
            .simple_query(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
                 WHERE slot_name = 'moonlink_slot_postgres';",
            )
            .await;
        let _ = client
            .simple_query("SELECT pg_drop_replication_slot('moonlink_slot_postgres')")
            .await;
        client
            .simple_query(
                "DROP TABLE IF EXISTS copy_restart;
                 CREATE TABLE copy_restart (id BIGINT PRIMARY KEY, name TEXT);
                 INSERT INTO copy_restart SELECT i, 'row-' || i FROM generate_series(1, 100) AS i;",
            )
            .await
            .unwrap();

        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &format!("tests::{test_name}"),
                "--test-threads=1",
            ])
            .env(CRASH_CHILD_BASE_PATH, &base_path)
            .env(
                CRASH_CHILD_TABLE_CONFIG,
                get_serialized_table_config(&testing_directory),
            )
            .env(
                CRASH_CHILD_PERSISTENCE_FAILURES,
                persistence_failures.to_string(),
            )
            .spawn()
            .unwrap();
        let deadline = tokio::time::Instant::now() + CRASH_CHILD_TIMEOUT;
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "the child creating the mirror did not finish within {CRASH_CHILD_TIMEOUT:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert!(
            status.success(),
            "the child creating the mirror failed: {status}"
        );

        let backend = create_backend_from_base_path(base_path).await;
        let lsn = current_wal_lsn(&client).await;
        assert_scan_ids_eq(
            &backend,
            DATABASE.to_string(),
            TABLE.to_string(),
            lsn,
            1..=100,
        )
        .await;

        // The recovered mirror keeps replicating on top of the copy.
        client
            .simple_query("INSERT INTO copy_restart VALUES (101, 'row-101');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;
        assert_scan_ids_eq(
            &backend,
            DATABASE.to_string(),
            TABLE.to_string(),
            lsn,
            1..=101,
        )
        .await;

        backend
            .drop_table(DATABASE.to_string(), TABLE.to_string())
            .await
            .unwrap();
        backend.shutdown_connection(&uri, true).await;
        client
            .simple_query("DROP TABLE IF EXISTS copy_restart;")
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_restart_right_after_create_table_keeps_copied_rows() {
        restart_right_after_create_table(
            "test_restart_right_after_create_table_keeps_copied_rows",
            /*persistence_failures=*/ 0,
        )
        .await;
    }

    /// Testing scenario: the first persistence of the copy fails with a transient error. The table
    /// handler retries it, so create_table must keep waiting rather than drop the mirror.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_create_table_waits_out_a_transient_persistence_failure() {
        restart_right_after_create_table(
            "test_create_table_waits_out_a_transient_persistence_failure",
            /*persistence_failures=*/ 1,
        )
        .await;
    }

    /// Test scenario: perform a few requests on non-existent databases and tables, make sure error is correctly propagated.

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
