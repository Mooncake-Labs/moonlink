//! Replication across restarts and reconnects: the slot, re-sent transactions and reads.
mod common;

#[cfg(feature = "test-utils")]
#[cfg(test)]
mod tests {
    use super::common::{
        confirmed_flush_lsn, connect_to_postgres, create_backend_from_base_path, current_wal_lsn,
        get_database_uri, get_serialized_table_config, scan_id_counts, setup_backend, DATABASE,
    };
    use moonlink_backend::MoonlinkBackend;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio_postgres::Client;

    const TABLE_A: &str = "mooncake-schema.rr-a";
    const TABLE_B: &str = "mooncake-schema.rr-b";

    async fn recreate_src_table(client: &Client, name: &str) {
        client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {name}; CREATE TABLE {name} (id BIGINT, name TEXT);"
            ))
            .await
            .unwrap();
    }

    async fn create_mirror(
        backend: &MoonlinkBackend,
        tmp: &tempfile::TempDir,
        mooncake: &str,
        src: &str,
    ) {
        backend
            .create_table(
                DATABASE.to_string(),
                mooncake.to_string(),
                format!("public.{src}"),
                get_database_uri(),
                get_serialized_table_config(tmp),
                None,
            )
            .await
            .unwrap();
    }

    async fn insert_range(client: &Client, src: &str, lo: i64, hi: i64) -> u64 {
        client
            .simple_query(&format!(
                "INSERT INTO {src} SELECT g, g::text FROM generate_series({lo},{hi}) g;"
            ))
            .await
            .unwrap();
        current_wal_lsn(client).await
    }

    async fn persist(backend: &MoonlinkBackend, mooncake: &str, lsn: u64) {
        backend
            .scan_table(DATABASE.to_string(), mooncake.to_string(), Some(lsn))
            .await
            .unwrap();
        backend
            .create_snapshot(DATABASE.to_string(), mooncake.to_string(), lsn)
            .await
            .unwrap();
    }

    fn counts(lo: i64, hi: i64) -> HashMap<i64, u64> {
        (lo..=hi).map(|i| (i, 1)).collect()
    }

    async fn cleanup(backend: &MoonlinkBackend, tables: &[&str]) {
        for t in tables {
            let _ = backend
                .drop_table(DATABASE.to_string(), t.to_string())
                .await;
        }
        backend.shutdown_connection(&get_database_uri(), true).await;
    }

    /// A recovered table that is not registered yet must hold the slot back: while it is still
    /// being recovered, an already registered table that persists new changes must not move the
    /// slot past the changes the slower table has to replay.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn slot_waits_for_every_recovered_table() {
        let (tmp, backend, client) = setup_backend(None, false).await;
        recreate_src_table(&client, "rr_a").await;
        recreate_src_table(&client, "rr_b").await;
        create_mirror(&backend, &tmp, TABLE_A, "rr_a").await;
        create_mirror(&backend, &tmp, TABLE_B, "rr_b").await;

        let l1 = insert_range(&client, "rr_b", 1, 10).await;
        persist(&backend, TABLE_B, l1).await;
        let l2 = insert_range(&client, "rr_b", 11, 20).await;
        backend
            .scan_table(DATABASE.to_string(), TABLE_B.to_string(), Some(l2))
            .await
            .unwrap();
        let l3 = insert_range(&client, "rr_a", 1, 10).await;
        persist(&backend, TABLE_A, l3).await;
        assert!(confirmed_flush_lsn(&client).await < l2);

        backend
            .shutdown_connection(&get_database_uri(), false)
            .await;
        drop(backend);
        moonlink_backend::set_recovery_delay_between_tables_for_test(Duration::from_secs(25));
        // Writes to A while B's recovery is still pending.
        let writer = tokio::spawn(async move {
            let (c, _h) = connect_to_postgres(&get_database_uri()).await;
            tokio::time::sleep(Duration::from_secs(3)).await;
            for k in 0..5i64 {
                insert_range(&c, "rr_a", 1000 + k * 10_000, 1000 + k * 10_000 + 9_999).await;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        let base_path = tmp.path().to_str().unwrap().to_string();
        let backend = create_backend_from_base_path(base_path).await;
        moonlink_backend::set_recovery_delay_between_tables_for_test(Duration::ZERO);
        writer.await.unwrap();
        let confirmed_at_b_registration = confirmed_flush_lsn(&client).await;
        tokio::time::sleep(Duration::from_secs(8)).await;
        let got = scan_id_counts(&backend, DATABASE.to_string(), TABLE_B.to_string(), l2).await;
        cleanup(&backend, &[TABLE_A, TABLE_B]).await;
        assert_eq!(
            got,
            counts(1, 20),
            "B lost rows: B iceberg={l1:X}, B in-memory up to {l2:X}, slot confirmed when B registered={confirmed_at_b_registration:X}"
        );
    }

    /// Right after a restart, a recovered table whose post-snapshot changes are still being replayed
    /// from the slot must not serve a read at an LSN covering those changes without them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn recovered_table_read_waits_for_slot_replay() {
        let (tmp, backend, client) = setup_backend(None, false).await;
        recreate_src_table(&client, "rr_a").await;
        recreate_src_table(&client, "rr_b").await;
        create_mirror(&backend, &tmp, TABLE_A, "rr_a").await;
        create_mirror(&backend, &tmp, TABLE_B, "rr_b").await;

        // B: rows 1..=10 persisted, 11..=20 only in memory.
        let l1 = insert_range(&client, "rr_b", 1, 10).await;
        persist(&backend, TABLE_B, l1).await;
        let l2 = insert_range(&client, "rr_b", 11, 20).await;
        backend
            .scan_table(DATABASE.to_string(), TABLE_B.to_string(), Some(l2))
            .await
            .unwrap();
        // A: persisted at a later LSN.
        let l3 = insert_range(&client, "rr_a", 1, 5).await;
        persist(&backend, TABLE_A, l3).await;
        assert!(confirmed_flush_lsn(&client).await < l2);

        // Crash and recover.
        backend
            .shutdown_connection(&get_database_uri(), false)
            .await;
        drop(backend);
        let base_path = tmp.path().to_str().unwrap().to_string();
        let backend = create_backend_from_base_path(base_path).await;

        let early = scan_id_counts(&backend, DATABASE.to_string(), TABLE_B.to_string(), l2).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        let late = scan_id_counts(&backend, DATABASE.to_string(), TABLE_B.to_string(), l2).await;
        cleanup(&backend, &[TABLE_A, TABLE_B]).await;
        assert_eq!(late, counts(1, 20), "rows lost after replay");
        assert_eq!(
            early,
            counts(1, 20),
            "read at LSN {l2:X} right after restart returned {} rows (stale), {} rows 5s later",
            early.len(),
            late.len()
        );
    }
}
