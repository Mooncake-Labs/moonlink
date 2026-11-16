//! Replication across restarts and reconnects: the slot, re-sent transactions and reads.
mod common;

#[cfg(feature = "test-utils")]
#[cfg(test)]
mod tests {
    use super::common::{
        confirmed_flush_lsn, connect_to_postgres, create_backend_from_base_path, current_wal_lsn,
        get_database_uri, get_serialized_table_config, id_counts_from_state_with_deletes,
        scan_id_counts, setup_backend, LogicalDecodingWorkMem, DATABASE,
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
        let _work_mem = LogicalDecodingWorkMem::set(if streamed { "64kB" } else { "1GB" }).await;

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

    /// Number of streamed transactions postgres has sent for the moonlink slot.
    async fn streamed_transactions_sent(client: &Client) -> i64 {
        client
            .query_one(
                "SELECT coalesce(sum(stream_txns), 0)::bigint FROM pg_stat_replication_slots \
                 WHERE slot_name LIKE 'moonlink_slot%'",
                &[],
            )
            .await
            .unwrap()
            .get(0)
    }

    /// Wait up to 30 s for postgres to have streamed `at_least` transactions for the moonlink slot
    /// (slot statistics are reported about once a second, and decoding can lag the client by
    /// seconds); returns the number streamed.
    async fn wait_until_streamed(client: &Client, at_least: i64) -> i64 {
        let mut streamed = streamed_transactions_sent(client).await;
        for _ in 0..150 {
            if streamed >= at_least {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            streamed = streamed_transactions_sent(client).await;
        }
        streamed
    }

    /// Scan `mooncake` at `lsn` and count its rows per id, failing if the mirror stopped applying.
    async fn live_id_counts(
        backend: &MoonlinkBackend,
        mooncake: &str,
        lsn: u64,
    ) -> HashMap<i64, u64> {
        let state = tokio::time::timeout(
            Duration::from_secs(120),
            backend.scan_table(DATABASE.to_string(), mooncake.to_string(), Some(lsn)),
        )
        .await
        .unwrap_or_else(|_| panic!("{mooncake} stopped applying changes"))
        .unwrap();
        id_counts_from_state_with_deletes(&state).await
    }

    /// Large streamed UPDATEs of persisted rows, each interrupted by a replication reconnect while it
    /// is being streamed (before its COMMIT): postgres re-sends it under the same xid after the
    /// first attempt is aborted, and every row must end up updated exactly once (a lost delete
    /// leaves a duplicate, a lost insert a gap).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn reconnect_mid_streamed_update_applies_it_once() {
        let _work_mem = LogicalDecodingWorkMem::set("64kB").await;
        let (tmp, backend, client) = setup_backend(None, false).await;
        let (writer, _writer_connection) = connect_to_postgres(&get_database_uri()).await;
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
        for (round, delay_ms) in [0u64, 200, 600].into_iter().enumerate() {
            let streamed_before = streamed_transactions_sent(&client).await;
            writer
                .simple_query(&format!("BEGIN; UPDATE rr_pk SET name = 'round {round}';"))
                .await
                .unwrap();
            // Wait until postgres streams the open transaction, then cut the stream.
            wait_until_streamed(&client, streamed_before + 1).await;
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            terminate_walsender(&client).await;
            writer.simple_query("COMMIT").await.unwrap();
            let lsn = current_wal_lsn(&client).await;
            let got = live_id_counts(&backend, TABLE_A, lsn).await;
            let streamed =
                wait_until_streamed(&client, streamed_before + 2).await - streamed_before;
            let dups = got.values().filter(|c| **c > 1).count();
            let missing = (1..=N).filter(|id| !got.contains_key(id)).count();
            if streamed < 2 || dups > 0 || missing > 0 {
                failures.push(format!(
                    "delay {delay_ms}ms: streamed {streamed} times, {dups} duplicated ids, {missing} missing ids"
                ));
            }
        }
        cleanup(&backend, &[TABLE_A]).await;
        assert!(failures.is_empty(), "ids 1..={N}: {failures:?}");
    }

    /// Restart with streamed transactions in the slot backlog that one mirror already persisted:
    /// another mirror with a small unpersisted change holds the slot behind them, so postgres
    /// re-sends them after the restart. The mirror that persisted them must skip them (an INSERT,
    /// an UPDATE of every row and a DELETE of a quarter), keep applying, and answer a read at a
    /// later LSN with that change.
    async fn restart_replays_streamed_transactions(primary_key: bool) {
        let _work_mem = LogicalDecodingWorkMem::set("64kB").await;
        let (tmp, backend, client) = setup_backend(None, false).await;
        let key = if primary_key { " PRIMARY KEY" } else { "" };
        client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS rr_a; CREATE TABLE rr_a (id BIGINT{key}, name TEXT);"
            ))
            .await
            .unwrap();
        recreate_src_table(&client, "rr_b").await;
        create_mirror(&backend, &tmp, TABLE_A, "rr_a").await;
        create_mirror(&backend, &tmp, TABLE_B, "rr_b").await;
        let held = insert_range(&client, "rr_b", 1, 5).await;
        backend
            .scan_table(DATABASE.to_string(), TABLE_B.to_string(), Some(held))
            .await
            .unwrap();

        const N: i64 = 20_000;
        let first_streamed = current_wal_lsn(&client).await;
        let streamed_before = streamed_transactions_sent(&client).await;
        insert_range(&client, "rr_a", 1, N).await;
        client
            .simple_query("UPDATE rr_a SET name = 'updated';")
            .await
            .unwrap();
        client
            .simple_query(&format!("DELETE FROM rr_a WHERE id <= {};", N / 4))
            .await
            .unwrap();
        let applied = current_wal_lsn(&client).await;
        persist(&backend, TABLE_A, applied).await;
        assert_eq!(
            wait_until_streamed(&client, streamed_before + 3).await - streamed_before,
            3,
            "the INSERT, UPDATE and DELETE were streamed"
        );
        let confirmed = confirmed_flush_lsn(&client).await;
        assert!(
            confirmed < first_streamed,
            "the slot ({confirmed:X}) is held behind the streamed transactions ({first_streamed:X})"
        );

        backend
            .shutdown_connection(&get_database_uri(), false)
            .await;
        drop(backend);
        let base_path = tmp.path().to_str().unwrap().to_string();
        let backend = create_backend_from_base_path(base_path).await;
        let later = insert_range(&client, "rr_a", N + 1, N + 1).await;
        let a = live_id_counts(&backend, TABLE_A, later).await;
        let b = live_id_counts(&backend, TABLE_B, later).await;
        cleanup(&backend, &[TABLE_A, TABLE_B]).await;
        assert_eq!(
            a,
            counts(N / 4 + 1, N + 1),
            "mirror that persisted the replay"
        );
        assert_eq!(b, counts(1, 5), "mirror that held the slot");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn restart_replays_streamed_transactions_once_with_primary_key() {
        restart_replays_streamed_transactions(true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn restart_replays_streamed_transactions_once_without_primary_key() {
        restart_replays_streamed_transactions(false).await;
    }

    /// Reset the peak resident memory of the process (VmHWM) to its current size.
    fn reset_peak_rss() {
        std::fs::write("/proc/self/clear_refs", "5").unwrap();
    }

    /// Peak resident memory of the process since the last reset.
    fn peak_rss_bytes() -> u64 {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let line = status.lines().find(|l| l.starts_with("VmHWM:")).unwrap();
        line.split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<u64>()
            .unwrap()
            * 1024
    }

    /// Measurement, not run by default (set MOONLINK_MEASURE_STREAMED_DELETE_ROWS): the peak memory a
    /// streamed DELETE of every row of a table without a key (REPLICA IDENTITY FULL, 200-byte rows)
    /// adds to the process until the mirror has applied it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn streamed_delete_peak_memory() {
        let Some(rows) = std::env::var("MOONLINK_MEASURE_STREAMED_DELETE_ROWS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
        else {
            eprintln!("skipped: set MOONLINK_MEASURE_STREAMED_DELETE_ROWS to measure");
            return;
        };
        let (tmp, backend, client) = setup_backend(None, false).await;
        recreate_src_table(&client, "rr_a").await;
        create_mirror(&backend, &tmp, TABLE_A, "rr_a").await;
        client
            .simple_query(&format!(
                "INSERT INTO rr_a SELECT g, repeat('x', 200) || g FROM generate_series(1, {rows}) g;"
            ))
            .await
            .unwrap();
        let inserted = current_wal_lsn(&client).await;
        persist(&backend, TABLE_A, inserted).await;

        let _work_mem = LogicalDecodingWorkMem::set("64kB").await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        reset_peak_rss();
        let rss_before = peak_rss_bytes();
        let start = std::time::Instant::now();
        client.simple_query("DELETE FROM rr_a;").await.unwrap();
        let deleted = current_wal_lsn(&client).await;
        let state = tokio::time::timeout(
            Duration::from_secs(3600),
            backend.scan_table(DATABASE.to_string(), TABLE_A.to_string(), Some(deleted)),
        )
        .await
        .expect("the mirror stopped applying changes")
        .unwrap();
        let elapsed = start.elapsed();
        let peak = peak_rss_bytes();
        let left = id_counts_from_state_with_deletes(&state).await;
        drop(state);
        cleanup(&backend, &[TABLE_A]).await;
        eprintln!(
            "STREAMED_DELETE_MEMORY rows={rows} peak_rss={} MiB before={} MiB growth={} MiB ({} B/row) applied_in={:.1}s",
            peak >> 20,
            rss_before >> 20,
            peak.saturating_sub(rss_before) >> 20,
            peak.saturating_sub(rss_before) / rows as u64,
            elapsed.as_secs_f64()
        );
        assert!(left.is_empty(), "{} rows left", left.len());
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

    /// Deletes one of three identical rows of a table without a key, for rows of many column types
    /// that are on disk: non-streamed, then inside a streamed transaction. The snapshot must pick a
    /// copy by reading it back and comparing it with the deleted row (or its fingerprint).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn full_identity_delete_of_one_of_identical_rows_across_types() {
        let (tmp, backend, client) = setup_backend(None, false).await;
        client
            .simple_query(
                "DROP TABLE IF EXISTS fp_t; CREATE TABLE fp_t (id BIGINT, s SMALLINT, i INT, \
                 f4 REAL, f8 DOUBLE PRECISION, n NUMERIC(12,3), b BOOL, t TEXT, c CHAR(5), \
                 vc VARCHAR(10), by BYTEA, d DATE, tm TIME, ts TIMESTAMP, \
                 tstz TIMESTAMPTZ, u UUID, j JSON, jb JSONB, ia INT[], ta TEXT[], \
                 pad TEXT);",
            )
            .await
            .unwrap();
        create_mirror(&backend, &tmp, TABLE_A, "fp_t").await;
        let row1 =
            "1, 7, 70000, 1.5, -0.0, 12345.678, true, 'héllo', 'ab', 'v', '\\x00ff'::bytea, \
                    '2024-02-29', '13:14:15.123456', '2024-01-01 00:00:00.5', \
                    '2024-01-01 12:00:00+02', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', \
                    '{\"a\": [1, 2]}', '{\"b\": 1, \"a\": 2}', '{1,NULL,3}', '{\"x\",\"y z\"}', \
                    ''";
        let row2 = "2, NULL, -1, 'NaN', '-Infinity', -0.001, false, '', NULL, NULL, '\\x'::bytea, \
                    '1970-01-01', '00:00:00', '1999-12-31 23:59:59', \
                    '2000-01-01 00:00:00+00', NULL, 'null', '[]', '{}', '{NULL}', ''";
        for row in [row1, row2] {
            for _ in 0..3 {
                client
                    .simple_query(&format!("INSERT INTO fp_t VALUES ({row});"))
                    .await
                    .unwrap();
            }
        }
        client
            .simple_query("INSERT INTO fp_t (id, pad) VALUES (3, 'single');")
            .await
            .unwrap();
        let loaded = current_wal_lsn(&client).await;
        persist(&backend, TABLE_A, loaded).await;

        let delete_one = |id: i64| {
            format!(
                "DELETE FROM fp_t WHERE ctid IN (SELECT ctid FROM fp_t WHERE id = {id} LIMIT 1);"
            )
        };
        client
            .simple_query(&format!("{} {}", delete_one(1), delete_one(2)))
            .await
            .unwrap();
        let after_plain = current_wal_lsn(&client).await;
        let got = live_id_counts(&backend, TABLE_A, after_plain).await;
        let want: HashMap<i64, u64> = [(1, 2), (2, 2), (3, 1)].into_iter().collect();
        assert_eq!(got, want, "non-streamed delete of one identical row");
        persist(&backend, TABLE_A, after_plain).await;

        let _work_mem = LogicalDecodingWorkMem::set("64kB").await;
        let streamed_before = streamed_transactions_sent(&client).await;
        // Committed only once postgres has streamed it: decoding can lag the client by seconds.
        let (writer, _writer_connection) = connect_to_postgres(&get_database_uri()).await;
        writer
            .simple_query(&format!(
                "BEGIN; INSERT INTO fp_t (id, pad) SELECT 100 + g, repeat('p', 200) \
                 FROM generate_series(1, 3000) g; {} {} DELETE FROM fp_t WHERE id > 100;",
                delete_one(1),
                delete_one(2)
            ))
            .await
            .unwrap();
        assert!(
            wait_until_streamed(&client, streamed_before + 1).await > streamed_before,
            "the transaction was not streamed before its commit"
        );
        writer.simple_query("COMMIT;").await.unwrap();
        let after_streamed = current_wal_lsn(&client).await;
        let got = live_id_counts(&backend, TABLE_A, after_streamed).await;
        cleanup(&backend, &[TABLE_A]).await;
        let want: HashMap<i64, u64> = [(1, 1), (2, 1), (3, 1)].into_iter().collect();
        assert_eq!(got, want, "streamed delete of one identical row");
    }
}
