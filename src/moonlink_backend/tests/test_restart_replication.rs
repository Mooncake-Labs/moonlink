//! Replication across restarts and reconnects: the slot, re-sent transactions and reads.
mod common;

#[cfg(feature = "test-utils")]
#[cfg(test)]
mod tests {
    use super::common::{
        confirmed_flush_lsn, connect_to_postgres, create_backend_from_base_path, current_wal_lsn,
        get_database_uri, get_serialized_table_config, id_counts_from_state_with_deletes,
        scan_id_counts, setup_backend, DATABASE,
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

    /// A mirror that stops receiving writes must not hold the replication slot back: once another
    /// mirror's changes are persisted, the slot moves past the idle mirror's last change.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn idle_table_does_not_pin_slot() {
        let (tmp, backend, client) = setup_backend(None, false).await;
        recreate_src_table(&client, "rr_a").await;
        recreate_src_table(&client, "rr_b").await;
        create_mirror(&backend, &tmp, TABLE_A, "rr_a").await;
        create_mirror(&backend, &tmp, TABLE_B, "rr_b").await;

        // Idle table gets one write, persisted, then never written again.
        let idle_lsn = insert_range(&client, "rr_b", 1, 10).await;
        persist(&backend, TABLE_B, idle_lsn).await;

        // Active table keeps writing and persisting.
        let mut active_lsn = 0;
        for round in 0..5 {
            active_lsn = insert_range(&client, "rr_a", round * 100, round * 100 + 99).await;
            persist(&backend, TABLE_A, active_lsn).await;
        }
        // The user-facing "force a snapshot" on the idle table, too.
        let now = current_wal_lsn(&client).await;
        backend
            .create_snapshot(DATABASE.to_string(), TABLE_B.to_string(), now)
            .await
            .unwrap();

        let mut confirmed = 0;
        for _ in 0..35 {
            confirmed = confirmed_flush_lsn(&client).await;
            if confirmed >= active_lsn {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        cleanup(&backend, &[TABLE_A, TABLE_B]).await;
        assert!(
            confirmed >= active_lsn,
            "slot pinned by idle table: confirmed_flush_lsn={confirmed:X} idle_iceberg_lsn={idle_lsn:X} active_iceberg_lsn={active_lsn:X}"
        );
    }

    /// Wait until the slot is confirmed at `lsn` or later; returns the last confirmed LSN.
    async fn wait_for_confirmed(client: &Client, lsn: u64) -> u64 {
        let mut confirmed = 0;
        for _ in 0..40 {
            confirmed = confirmed_flush_lsn(client).await;
            if confirmed >= lsn {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        confirmed
    }

    /// A mirror of a table that never had a row has nothing to persist and must not pin the slot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn empty_table_does_not_pin_slot() {
        let (tmp, backend, client) = setup_backend(None, false).await;
        recreate_src_table(&client, "rr_a").await;
        recreate_src_table(&client, "rr_b").await;
        create_mirror(&backend, &tmp, TABLE_A, "rr_a").await;
        create_mirror(&backend, &tmp, TABLE_B, "rr_b").await;

        let active_lsn = insert_range(&client, "rr_a", 1, 100).await;
        persist(&backend, TABLE_A, active_lsn).await;
        let confirmed = wait_for_confirmed(&client, active_lsn).await;
        cleanup(&backend, &[TABLE_A, TABLE_B]).await;
        assert!(
            confirmed >= active_lsn,
            "slot pinned by an empty mirror: confirmed_flush_lsn={confirmed:X} active_iceberg_lsn={active_lsn:X}"
        );
    }

    /// After a restart, a recovered mirror with nothing new must not pin the slot either.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn recovered_idle_table_does_not_pin_slot() {
        let (tmp, backend, client) = setup_backend(None, false).await;
        recreate_src_table(&client, "rr_a").await;
        recreate_src_table(&client, "rr_b").await;
        create_mirror(&backend, &tmp, TABLE_A, "rr_a").await;
        create_mirror(&backend, &tmp, TABLE_B, "rr_b").await;
        let idle_lsn = insert_range(&client, "rr_b", 1, 10).await;
        persist(&backend, TABLE_B, idle_lsn).await;

        backend
            .shutdown_connection(&get_database_uri(), false)
            .await;
        drop(backend);
        let base_path = tmp.path().to_str().unwrap().to_string();
        let backend = create_backend_from_base_path(base_path).await;

        let active_lsn = insert_range(&client, "rr_a", 1, 100).await;
        persist(&backend, TABLE_A, active_lsn).await;
        let confirmed = wait_for_confirmed(&client, active_lsn).await;
        let b = scan_id_counts(
            &backend,
            DATABASE.to_string(),
            TABLE_B.to_string(),
            active_lsn,
        )
        .await;
        cleanup(&backend, &[TABLE_A, TABLE_B]).await;
        assert_eq!(b, counts(1, 10), "idle mirror after restart");
        assert!(
            confirmed >= active_lsn,
            "slot pinned by a recovered idle mirror: confirmed_flush_lsn={confirmed:X} idle_iceberg_lsn={idle_lsn:X} active_iceberg_lsn={active_lsn:X}"
        );
    }

    async fn terminate_walsender(client: &Client) {
        client
            .simple_query(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
                 WHERE slot_name = 'moonlink_slot_postgres' AND active_pid IS NOT NULL;",
            )
            .await
            .unwrap();
    }

    async fn reconnect_mid_transaction(streamed: bool) {
        let (_ctl, _h) = connect_to_postgres(&get_database_uri()).await;
        let work_mem = if streamed { "64kB" } else { "1GB" };
        _ctl.simple_query(&format!(
            "ALTER SYSTEM SET logical_decoding_work_mem = '{work_mem}'"
        ))
        .await
        .unwrap();
        _ctl.simple_query("SELECT pg_reload_conf()").await.unwrap();

        let (tmp, backend, client) = setup_backend(None, false).await;
        recreate_src_table(&client, "rr_a").await;
        create_mirror(&backend, &tmp, TABLE_A, "rr_a").await;

        const N: i64 = 200_000;
        let mut failures = Vec::new();
        let mut hi_total = 0;
        for (i, delay_ms) in [0u64, 50, 150, 400, 800].into_iter().enumerate() {
            let lo = i as i64 * N + 1;
            let hi = lo + N - 1;
            hi_total = hi;
            let lsn = insert_range(&client, "rr_a", lo, hi).await;
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            terminate_walsender(&client).await;
            let got =
                scan_id_counts(&backend, DATABASE.to_string(), TABLE_A.to_string(), lsn).await;
            let dups = got.values().filter(|c| **c > 1).count();
            let missing = (1..=hi).filter(|id| !got.contains_key(id)).count();
            if dups > 0 || missing > 0 {
                failures.push(format!(
                    "delay {delay_ms}ms: {dups} duplicated ids, {missing} missing ids"
                ));
            }
        }
        cleanup(&backend, &[TABLE_A]).await;
        _ctl.simple_query("ALTER SYSTEM RESET logical_decoding_work_mem")
            .await
            .unwrap();
        _ctl.simple_query("SELECT pg_reload_conf()").await.unwrap();
        assert!(
            failures.is_empty(),
            "streamed={streamed}, ids 1..={hi_total}: {failures:?}"
        );
    }

    /// A replication reconnect while a large non-streamed transaction is being delivered: postgres
    /// re-sends it whole, and none of it may be applied twice.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn reconnect_mid_non_streamed_transaction_applies_it_once() {
        reconnect_mid_transaction(false).await;
    }

    /// The same with protocol v2 streaming forced (logical_decoding_work_mem = 64kB).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn reconnect_mid_streamed_transaction_applies_it_once() {
        reconnect_mid_transaction(true).await;
    }

    /// Large streamed UPDATEs of persisted rows, each followed by a replication reconnect while it
    /// may still be delivered (`reconnect`) or not: postgres re-sends an interrupted one under the
    /// same xid after the first attempt is aborted, and every row must end up updated exactly once
    /// (a lost delete leaves a duplicate, a lost insert a gap).
    async fn streamed_updates(reconnect: bool) {
        let (ctl, _h) = connect_to_postgres(&get_database_uri()).await;
        ctl.simple_query("ALTER SYSTEM SET logical_decoding_work_mem = '64kB'")
            .await
            .unwrap();
        ctl.simple_query("SELECT pg_reload_conf()").await.unwrap();

        let (tmp, backend, client) = setup_backend(None, false).await;
        client
            .simple_query(
                "DROP TABLE IF EXISTS rr_pk; CREATE TABLE rr_pk (id BIGINT PRIMARY KEY, name TEXT);",
            )
            .await
            .unwrap();
        create_mirror(&backend, &tmp, TABLE_A, "rr_pk").await;
        const N: i64 = 100_000;
        let lsn = insert_range(&client, "rr_pk", 1, N).await;
        persist(&backend, TABLE_A, lsn).await;

        let mut failures = Vec::new();
        for (round, delay_ms) in [0u64, 50, 150, 400].into_iter().enumerate() {
            client
                .simple_query(&format!("UPDATE rr_pk SET name = 'round {round}';"))
                .await
                .unwrap();
            let lsn = current_wal_lsn(&client).await;
            if reconnect {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                terminate_walsender(&client).await;
            }
            // The table task dies on a lost stream state, and the scan would wait forever.
            let state = tokio::time::timeout(
                Duration::from_secs(120),
                backend.scan_table(DATABASE.to_string(), TABLE_A.to_string(), Some(lsn)),
            )
            .await
            .unwrap_or_else(|_| panic!("round {round}: the mirror stopped applying changes"))
            .unwrap();
            let got = id_counts_from_state_with_deletes(&state).await;
            let dups = got.values().filter(|c| **c > 1).count();
            let missing = (1..=N).filter(|id| !got.contains_key(id)).count();
            if dups > 0 || missing > 0 {
                failures.push(format!(
                    "delay {delay_ms}ms: {dups} duplicated ids, {missing} missing ids"
                ));
            }
        }
        cleanup(&backend, &[TABLE_A]).await;
        ctl.simple_query("ALTER SYSTEM RESET logical_decoding_work_mem")
            .await
            .unwrap();
        ctl.simple_query("SELECT pg_reload_conf()").await.unwrap();
        assert!(
            failures.is_empty(),
            "reconnect={reconnect}, ids 1..={N}: {failures:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn reconnect_mid_streamed_update_applies_it_once() {
        streamed_updates(true).await;
    }

    /// Control: the same streamed UPDATEs without a reconnect.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn streamed_update_applies_it_once() {
        streamed_updates(false).await;
    }

    /// After a restart, an errored mirror is restored as a placeholder and named by the periodic
    /// warning; a healthy mirror of the same database is not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn errored_placeholders_are_reported_after_restart() {
        let (tmp, backend, client) = setup_backend(None, false).await;
        recreate_src_table(&client, "rr_a").await;
        recreate_src_table(&client, "rr_b").await;
        create_mirror(&backend, &tmp, TABLE_A, "rr_a").await;
        create_mirror(&backend, &tmp, TABLE_B, "rr_b").await;
        insert_range(&client, "rr_a", 1, 10).await;
        client.simple_query("TRUNCATE rr_a;").await.unwrap();
        // Once this is persisted, the TRUNCATE before it has errored the other mirror.
        let lsn = insert_range(&client, "rr_b", 1, 10).await;
        persist(&backend, TABLE_B, lsn).await;

        backend
            .shutdown_connection(&get_database_uri(), false)
            .await;
        drop(backend);
        let base_path = tmp.path().to_str().unwrap().to_string();
        let backend = create_backend_from_base_path(base_path).await;
        let reported = backend.placeholder_mirror_names().await;
        cleanup(&backend, &[TABLE_A, TABLE_B]).await;
        assert_eq!(
            reported,
            (vec![format!("{DATABASE}.{TABLE_A}")], vec![]),
            "errored and pending mirrors named after the restart"
        );
    }

    /// Restart dedup: a multi-table transaction persisted in only one of its tables, and two
    /// interleaved transactions whose second commit record starts at the persisted LSN of the first.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn restart_dedup_multi_table_and_boundary() {
        let (tmp, backend, client) = setup_backend(None, false).await;
        recreate_src_table(&client, "rr_a").await;
        recreate_src_table(&client, "rr_b").await;
        create_mirror(&backend, &tmp, TABLE_A, "rr_a").await;
        create_mirror(&backend, &tmp, TABLE_B, "rr_b").await;
        let (s1, _h1) = connect_to_postgres(&get_database_uri()).await;
        let (s2, _h2) = connect_to_postgres(&get_database_uri()).await;

        // X touches both tables; only A persists it.
        client
            .simple_query("BEGIN; INSERT INTO rr_a SELECT g, g::text FROM generate_series(1,5) g; INSERT INTO rr_b SELECT g, g::text FROM generate_series(1,5) g; COMMIT;")
            .await
            .unwrap();
        let lx = current_wal_lsn(&client).await;
        persist(&backend, TABLE_A, lx).await;

        // Interleaved T1/T2 on A; A persisted exactly at T1's end.
        s1.simple_query("BEGIN; INSERT INTO rr_a SELECT g, g::text FROM generate_series(6,10) g;")
            .await
            .unwrap();
        s2.simple_query("BEGIN; INSERT INTO rr_a SELECT g, g::text FROM generate_series(11,15) g;")
            .await
            .unwrap();
        s1.simple_query("COMMIT").await.unwrap();
        let e1 = current_wal_lsn(&client).await;
        persist(&backend, TABLE_A, e1).await;
        s2.simple_query("COMMIT").await.unwrap();
        let e2 = current_wal_lsn(&client).await;
        for t in [TABLE_A, TABLE_B] {
            backend
                .scan_table(DATABASE.to_string(), t.to_string(), Some(e2))
                .await
                .unwrap();
        }

        backend
            .shutdown_connection(&get_database_uri(), false)
            .await;
        drop(backend);
        let base_path = tmp.path().to_str().unwrap().to_string();
        let backend = create_backend_from_base_path(base_path).await;
        tokio::time::sleep(Duration::from_secs(6)).await;
        let a = scan_id_counts(&backend, DATABASE.to_string(), TABLE_A.to_string(), e2).await;
        let b = scan_id_counts(&backend, DATABASE.to_string(), TABLE_B.to_string(), e2).await;
        cleanup(&backend, &[TABLE_A, TABLE_B]).await;
        assert_eq!(a, counts(1, 15), "table A after restart");
        assert_eq!(b, counts(1, 5), "table B after restart");
    }

    /// A crash before the table has any Iceberg snapshot: every change is still in the slot, and
    /// after recovery each row is there exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn crash_before_first_snapshot_recovers_every_row_once() {
        let (tmp, backend, client) = setup_backend(None, false).await;
        recreate_src_table(&client, "rr_a").await;
        create_mirror(&backend, &tmp, TABLE_A, "rr_a").await;
        insert_range(&client, "rr_a", 1, 10).await;
        let lsn = insert_range(&client, "rr_a", 11, 20).await;
        backend
            .scan_table(DATABASE.to_string(), TABLE_A.to_string(), Some(lsn))
            .await
            .unwrap();

        backend
            .shutdown_connection(&get_database_uri(), false)
            .await;
        drop(backend);
        let base_path = tmp.path().to_str().unwrap().to_string();
        let backend = create_backend_from_base_path(base_path).await;
        let after = scan_id_counts(&backend, DATABASE.to_string(), TABLE_A.to_string(), lsn).await;
        let lsn = insert_range(&client, "rr_a", 21, 25).await;
        let later = scan_id_counts(&backend, DATABASE.to_string(), TABLE_A.to_string(), lsn).await;
        cleanup(&backend, &[TABLE_A]).await;
        assert_eq!(after, counts(1, 20), "after recovery");
        assert_eq!(later, counts(1, 25), "replication after recovery");
    }
}
