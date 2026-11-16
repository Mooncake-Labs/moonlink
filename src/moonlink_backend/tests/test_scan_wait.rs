mod common;

#[cfg(test)]
mod tests {
    use super::common::{current_wal_lsn, TestGuard, DATABASE, TABLE};
    use serial_test::serial;
    use std::time::{Duration, Instant};

    /// LSN that replication will not reach during the test.
    async fn unreachable_lsn(client: &tokio_postgres::Client) -> u64 {
        current_wal_lsn(client).await + (1u64 << 40)
    }

    /// A scan waiting for an LSN gives up after the configured bound with an actionable error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_scan_wait_is_bounded() {
        let (guard, client) = TestGuard::new(Some("scan_wait_bounded"), true).await;
        let backend = guard.backend();
        backend.set_lsn_wait_timeout(Duration::from_secs(1));
        let lsn = unreachable_lsn(&client).await;

        let started = Instant::now();
        let err = backend
            .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
            .await
            .expect_err("scan of an unreachable LSN must fail");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{:?}",
            started.elapsed()
        );
        assert!(
            err.to_string()
                .contains(&format!("did not reach LSN {lsn}")),
            "{err}"
        );

        // A reachable LSN still works afterwards.
        let lsn = current_wal_lsn(&client).await;
        backend
            .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
            .await
            .unwrap();
    }

    /// A scan waiting for an LSN must not hold the backend-wide table lock: drop/create and
    /// scans or listings of other tables keep working.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_waiting_scan_does_not_block_other_requests() {
        let (guard, client) = TestGuard::new(Some("scan_wait_lock"), true).await;
        let backend = guard.backend().clone();
        backend.set_lsn_wait_timeout(Duration::from_secs(60));
        let lsn = unreachable_lsn(&client).await;

        let waiting = tokio::spawn({
            let backend = backend.clone();
            async move {
                backend
                    .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
                    .await
                    .map(|_| ())
            }
        });
        tokio::time::sleep(Duration::from_millis(500)).await;

        let drop_table = tokio::time::timeout(
            Duration::from_secs(10),
            backend.drop_table(DATABASE.to_string(), TABLE.to_string()),
        )
        .await;
        assert!(
            matches!(drop_table, Ok(Ok(()))),
            "drop_table blocked behind a waiting scan: {drop_table:?}"
        );
        let list = tokio::time::timeout(Duration::from_secs(10), backend.list_tables()).await;
        assert!(matches!(list, Ok(Ok(_))), "list_tables blocked: {list:?}");

        // The dropped table's reader closes its watches, so the waiting scan ends with an error.
        let res = tokio::time::timeout(Duration::from_secs(10), waiting)
            .await
            .expect("waiting scan did not end after its table was dropped")
            .unwrap();
        assert!(res.is_err(), "{res:?}");
    }
}
