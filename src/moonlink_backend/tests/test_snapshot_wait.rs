mod common;

#[cfg(test)]
mod tests {
    use super::common::{current_wal_lsn, scan_ids, TestGuard, DATABASE, TABLE};
    use serial_test::serial;
    use std::collections::HashSet;
    use std::time::{Duration, Instant};

    /// pg_mooncake passes the end of the session's last transaction. That transaction may have
    /// written WAL without committing anything to the mirrored table (an xid-less transaction that
    /// pruned pages, a logical message); create_snapshot must still return once everything the
    /// table committed up to that LSN is persisted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_create_snapshot_at_lsn_past_last_table_commit() {
        let (guard, client) = TestGuard::new(Some("snapshot_xidless"), true).await;
        let backend = guard.backend();
        client
            .simple_query("INSERT INTO snapshot_xidless VALUES (1, 'a'), (2, 'b');")
            .await
            .unwrap();
        let commit_lsn = current_wal_lsn(&client).await;
        // Non-transactional WAL after the last commit, like HOT pruning in a later SELECT.
        client
            .simple_query("SELECT pg_logical_emit_message(false, 'review', 'x');")
            .await
            .unwrap();
        // XactLastCommitEnd points at the end of that WAL, which need not be written out yet.
        let row = client
            .query_one("SELECT pg_current_wal_insert_lsn()", &[])
            .await
            .unwrap();
        let lsn: u64 = row.get::<_, tokio_postgres::types::PgLsn>(0).into();
        assert!(lsn > commit_lsn);

        let res = tokio::time::timeout(
            Duration::from_secs(20),
            backend.create_snapshot(DATABASE.to_string(), TABLE.to_string(), lsn),
        )
        .await
        .expect("create_snapshot waited for a commit that will never come");
        res.unwrap();

        let tables = backend.list_tables().await.unwrap();
        let status = tables.iter().find(|t| t.table == TABLE).unwrap();
        assert!(
            status.flush_lsn.unwrap_or(0) >= commit_lsn,
            "rows committed before the LSN were not persisted: {status:?}"
        );
        assert_eq!(
            scan_ids(backend, DATABASE.to_string(), TABLE.to_string(), lsn).await,
            HashSet::from([1, 2])
        );
    }

    /// The wait for replication is bounded and returns an actionable error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_create_snapshot_wait_is_bounded() {
        let (guard, client) = TestGuard::new(Some("snapshot_bounded"), true).await;
        let backend = guard.backend();
        backend.set_lsn_wait_timeout(Duration::from_secs(1));
        let lsn = current_wal_lsn(&client).await + (1u64 << 40);

        let started = Instant::now();
        let err = backend
            .create_snapshot(DATABASE.to_string(), TABLE.to_string(), lsn)
            .await
            .expect_err("snapshot at an unreachable LSN must fail");
        assert!(started.elapsed() < Duration::from_secs(10));
        assert!(
            err.to_string()
                .contains(&format!("replication did not reach LSN {lsn}")),
            "{err}"
        );
    }

    /// Dropping the mirror while a snapshot request waits ends the request with an error; it
    /// neither blocks the drop nor panics.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_drop_while_create_snapshot_waits() {
        let (guard, client) = TestGuard::new(Some("snapshot_drop"), true).await;
        let backend = guard.backend().clone();
        backend.set_lsn_wait_timeout(Duration::from_secs(60));
        let lsn = current_wal_lsn(&client).await + (1u64 << 40);

        let waiting = tokio::spawn({
            let backend = backend.clone();
            async move {
                backend
                    .create_snapshot(DATABASE.to_string(), TABLE.to_string(), lsn)
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        let dropped = tokio::time::timeout(
            Duration::from_secs(10),
            backend.drop_table(DATABASE.to_string(), TABLE.to_string()),
        )
        .await;
        assert!(matches!(dropped, Ok(Ok(()))), "{dropped:?}");

        let res = tokio::time::timeout(Duration::from_secs(10), waiting)
            .await
            .expect("snapshot request kept waiting after the table was dropped")
            .expect("snapshot request panicked");
        assert!(res.is_err(), "{res:?}");
    }
}
