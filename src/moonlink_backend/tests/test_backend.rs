mod common;

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::common::{current_wal_lsn, ids_from_state, smoke_create_and_insert, TestGuard};

    use serial_test::serial;
    use std::collections::HashSet;

    use moonlink_backend::recreate_directory;

    const URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";

    // ───────────────────────────── Tests ─────────────────────────────

    /// Low-level filesystem helper: directory (re)creation.
    #[test]
    #[serial]
    fn test_recreate_directory() {
        let tmp = TempDir::new().unwrap();
        let file = tmp.path().join("tmp.txt");
        std::fs::write(&file, b"x").unwrap();
        assert!(file.exists());

        // idempotent "wipe" of an existing dir
        recreate_directory(tmp.path().to_str().unwrap()).unwrap();
        assert!(!file.exists());

        // creation of a brand-new path
        let inner = tmp.path().join("sub");
        recreate_directory(inner.to_str().unwrap()).unwrap();
        assert!(inner.exists());
    }

    /// Validate `create_table` and `drop_table` across successive uses.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_moonlink_service() {
        let (guard, client) = TestGuard::new(Some("test")).await;
        let backend = guard.backend();

        smoke_create_and_insert(backend, &client, URI).await;
        backend.drop_table("test").await.unwrap();
        smoke_create_and_insert(backend, &client, URI).await;
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

        let ids = ids_from_state(&backend.scan_table(&"scan_test", Some(lsn)).await.unwrap());
        assert_eq!(ids, HashSet::from([1, 2]));

        // Add one more row.
        client
            .simple_query("INSERT INTO scan_test VALUES (3,'c');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        let ids = ids_from_state(&backend.scan_table(&"scan_test", Some(lsn)).await.unwrap());
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

        let ids = ids_from_state(&backend.scan_table(&"lsn_test", Some(lsn1)).await.unwrap());
        assert_eq!(ids, HashSet::from([1]));

        client
            .simple_query("INSERT INTO lsn_test VALUES (2,'b');")
            .await
            .unwrap();
        let lsn2 = current_wal_lsn(&client).await;

        let ids = ids_from_state(&backend.scan_table(&"lsn_test", Some(lsn2)).await.unwrap());
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

        // It's not guaranteed whether "table insertion" or "create iceberg snapshot" reaches table handler eventloop first, add a sleep to reduce flakiness.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        backend
            .create_iceberg_snapshot(&"snapshot_test", lsn)
            .await
            .unwrap();

        // Look for any file in the Iceberg metadata dir.
        let meta_dir = guard
            .tmp()
            .unwrap()
            .path()
            .join("public")
            .join("snapshot_test")
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
        let ids = ids_from_state(&backend.scan_table(&"repl_test", Some(lsn)).await.unwrap());
        assert_eq!(ids, HashSet::from([1]));

        // Drop the table (this should clean up the replication connection)
        client
            .simple_query("DROP TABLE IF EXISTS repl_test;")
            .await
            .unwrap();
        backend.drop_table("repl_test").await.unwrap();

        // Second cycle: add table again, insert different data, verify it works
        client
            .simple_query("CREATE TABLE repl_test (id BIGINT PRIMARY KEY, name TEXT);")
            .await
            .unwrap();
        backend
            .create_table("repl_test", "public.repl_test", URI)
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO repl_test VALUES (2,'second');")
            .await
            .unwrap();

        let lsn = current_wal_lsn(&client).await;
        let ids = ids_from_state(&backend.scan_table(&"repl_test", Some(lsn)).await.unwrap());
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
                .scan_table(&"bulk_test", Some(lsn_after_insert))
                .await
                .unwrap(),
        );

        assert_eq!(
            ids.len(),
            1_000_000,
            "expected exactly 1 000 000 rows visible in snapshot"
        );
        assert!(ids.contains(&1), "row id 1 missing");
        assert!(ids.contains(&1_000_000), "row id 1_000_000 missing");
        assert!(
            ids.len() == 1_000_000,
            "expected exactly 1_000_000 rows visible in snapshot"
        );
    }
}
