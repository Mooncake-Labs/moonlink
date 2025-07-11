mod common;

#[cfg(test)]
mod tests {
    use moonlink_metadata_store::{base_metadata_store::MetadataStoreTrait, PgMetadataStore};
    use tempfile::TempDir;
    use tokio_postgres::{connect, NoTls};

    use super::common::{
        current_wal_lsn, ids_from_state, smoke_create_and_insert, DatabaseId, TableId, TestGuard,
        TestGuardMode, METADATA_STORE_URI, MOONLINK_SCHEMA, TABLE_ID,
    };

    use serial_test::serial;
    use std::collections::HashSet;

    use moonlink_backend::MoonlinkBackend;

    const SRC_URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";
    const DST_URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";

    // ───────────────────────────── Tests ─────────────────────────────

    /// Validate `create_table` and `drop_table` across successive uses.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_moonlink_service() {
        let (guard, client) = TestGuard::new(Some("test")).await;
        let backend = guard.backend();

        smoke_create_and_insert(backend, &client, guard.database_id, SRC_URI).await;
        backend.drop_table(guard.database_id, TABLE_ID).await;
        smoke_create_and_insert(backend, &client, guard.database_id, SRC_URI).await;
    }

    /// End-to-end: inserts should appear in `scan_table`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_scan_returns_inserted_rows() {
        let (guard, client) = TestGuard::new(Some("scan_test")).await;
        let backend = guard.backend();

        client
            .simple_query("INSERT INTO scan_test VALUES (1,'a'),(2,'b');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn))
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
                .scan_table(guard.database_id, TABLE_ID, Some(lsn))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1, 2, 3]));
    }

    /// `scan_table(..., Some(lsn))` should return rows up to that LSN.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_scan_table_with_lsn() {
        let (guard, client) = TestGuard::new(Some("lsn_test")).await;
        let backend = guard.backend();

        client
            .simple_query("INSERT INTO lsn_test VALUES (1,'a');")
            .await
            .unwrap();
        let lsn1 = current_wal_lsn(&client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn1))
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
                .scan_table(guard.database_id, TABLE_ID, Some(lsn2))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1, 2]));
    }

    /// Validates that `create_iceberg_snapshot` writes Iceberg metadata.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_create_iceberg_snapshot() {
        let (guard, client) = TestGuard::new(Some("snapshot_test")).await;
        let backend = guard.backend();

        client
            .simple_query("INSERT INTO snapshot_test VALUES (1,'a');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        // Read snapshot of the latest LSN to make sure all changes are synchronized to mooncake snapshot.
        backend
            .scan_table(guard.database_id, TABLE_ID, Some(lsn))
            .await
            .unwrap();

        // After all changes reflected at mooncake snapshot, trigger an iceberg snapshot.
        backend
            .create_snapshot(guard.database_id, TABLE_ID, lsn)
            .await
            .unwrap();

        // Look for any file in the Iceberg metadata dir.
        let meta_dir = guard
            .tmp()
            .unwrap()
            .path()
            .join("public")
            .join(format!("{}.{}", guard.database_id, TABLE_ID))
            .join("metadata");
        assert!(meta_dir.exists());
        assert!(meta_dir.read_dir().unwrap().next().is_some());
    }

    /// Test that replication connections are properly cleaned up and can be recreated.
    /// This validates that dropping the last table from a connection properly cleans up
    /// the replication slot, allowing new connections to be established.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_replication_connection_cleanup() {
        let (guard, client) = TestGuard::new(Some("repl_test")).await;
        let backend = guard.backend();

        client
            .simple_query("INSERT INTO repl_test VALUES (1,'first');")
            .await
            .unwrap();

        let lsn = current_wal_lsn(&client).await;
        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1]));

        // Drop the table (this should clean up the replication connection)
        client
            .simple_query("DROP TABLE IF EXISTS repl_test;")
            .await
            .unwrap();
        backend.drop_table(guard.database_id, TABLE_ID).await;

        // Second cycle: add table again, insert different data, verify it works
        client
            .simple_query("CREATE TABLE repl_test (id BIGINT PRIMARY KEY, name TEXT);")
            .await
            .unwrap();
        backend
            .create_table(
                guard.database_id,
                TABLE_ID,
                DST_URI.to_string(),
                /*table_name=*/ "public.repl_test".to_string(),
                SRC_URI.to_string(),
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
                .scan_table(guard.database_id, TABLE_ID, Some(lsn))
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
        let (guard, client) = TestGuard::new(Some("bulk_test")).await;
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
                .scan_table(guard.database_id, TABLE_ID, Some(lsn_after_insert))
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
        let (guard, _) = TestGuard::new(Some("metadata_store")).await;
        // Till now, table [`metadata_store`] has been created at both row storage and column storage database.
        let backend = guard.backend();
        let metadata_store = PgMetadataStore::new(METADATA_STORE_URI.to_string()).unwrap();

        // Check metadata storage after table creation.
        let metadata_entries = metadata_store
            .get_all_table_metadata_entries()
            .await
            .unwrap();
        assert_eq!(metadata_entries.len(), 1);
        assert_eq!(metadata_entries[0].table_id, TABLE_ID as u32);
        assert_eq!(
            metadata_entries[0]
                .moonlink_table_config
                .iceberg_table_config
                .table_name,
            format!("{}.{}", guard.database_id, TABLE_ID)
        );

        // Drop table and check metadata storage.
        backend.drop_table(guard.database_id, TABLE_ID).await;
        let metadata_entries = metadata_store
            .get_all_table_metadata_entries()
            .await
            .unwrap();
        assert!(metadata_entries.is_empty());
    }

    /// Test recovery, where database to recovery is not managed by moonlink.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery_not_managed_by_moonlink() {
        // Connect to Postgres.
        let (client, connection) = connect(SRC_URI, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Intentionally drop schema, which means current database is not managed by moonlink.
        client
            .simple_query(&format!("DROP SCHEMA IF EXISTS {MOONLINK_SCHEMA} CASCADE;"))
            .await
            .unwrap();

        // Attempt recovery logic, no tables should be recovered.
        let temp_dir = TempDir::new().unwrap();
        let _backend = MoonlinkBackend::<DatabaseId, TableId>::new(
            temp_dir.path().to_str().unwrap().to_string(),
            /*metadata_store_uris=*/ vec![METADATA_STORE_URI.to_string()],
        )
        .await
        .unwrap();
    }

    /// Test recovery.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery() {
        let (mut guard, client) = TestGuard::new(Some("recovery")).await;
        guard.set_test_mode(TestGuardMode::Crash);

        let database_id = guard.database_id;
        let backend = guard.backend();

        // Drop the table that setup_backend created so we can test the full cycle
        backend.drop_table(guard.database_id, TABLE_ID).await;

        // First cycle: add table, insert data, verify it works
        backend
            .create_table(
                guard.database_id,
                TABLE_ID,
                DST_URI.to_string(),
                "public.recovery".to_string(),
                SRC_URI.to_string(),
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
            .scan_table(guard.database_id, TABLE_ID, Some(lsn))
            .await
            .unwrap();
        backend
            .create_snapshot(guard.database_id, TABLE_ID, lsn)
            .await
            .unwrap();

        // Shutdown pg connection and table handler.
        backend.shutdown_connection(SRC_URI).await;
        // Take the testing directory, for recovery from iceberg table.
        let _testing_directory_before_recovery = guard.take_test_directory();
        // Drop everything for the old backend.
        drop(guard);

        // Attempt recovery logic.
        let temp_dir = TempDir::new().unwrap();
        let backend = MoonlinkBackend::<DatabaseId, TableId>::new(
            temp_dir.path().to_str().unwrap().to_string(),
            /*metadata_store_uris=*/ vec![METADATA_STORE_URI.to_string()],
        )
        .await
        .unwrap();
        let ids = ids_from_state(
            &backend
                .scan_table(database_id, TABLE_ID, Some(lsn))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1]));

        // Insert new rows to make sure recovered mooncake table works as usual.
        client
            .simple_query("INSERT INTO recovery VALUES (2,'second');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        // Wait until changes reflected to mooncake snapshot, and force create iceberg snapshot to test mooncake/iceberg table recovery.
        let ids = ids_from_state(
            &backend
                .scan_table(database_id, TABLE_ID, Some(lsn))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1, 2]));
    }
}
