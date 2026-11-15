mod common;

#[cfg(feature = "test-utils")]
#[cfg(test)]
mod tests {
    use super::common::{
        assert_scan_nonunique_ids_eq, confirmed_flush_lsn, crash_and_recover_backend,
        crash_and_recover_backend_with_guard, current_wal_lsn, get_database_uri, TestGuard,
        TestGuardMode, DATABASE, TABLE,
    };
    use serial_test::serial;
    use std::collections::HashMap;

    /// After a crash, postgres re-sends everything after the slot's confirmed LSN, which can trail
    /// the iceberg snapshot (status updates are periodic, and after a postgres crash the slot is
    /// only as recent as its last checkpoint). Re-sent transactions at or below the snapshot must be
    /// skipped, and later ones applied exactly once, without panicking.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery_skips_resent_persisted_transactions() {
        let uri = get_database_uri();
        let (mut guard, client) = TestGuard::new(Some("recovery"), false).await;
        guard.set_test_mode(TestGuardMode::Crash);
        let backend = guard.backend();
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

        let mut guard = Some(guard);
        let mut recovered: Option<(moonlink_backend::MoonlinkBackend, tempfile::TempDir)> = None;
        let mut resent_persisted_transaction = false;
        for round in 0..3i64 {
            let backend = match (&guard, &recovered) {
                (Some(guard), _) => guard.backend().as_ref(),
                (None, Some((backend, _))) => backend,
                _ => unreachable!(),
            };
            // Rows persisted into iceberg.
            let mut lsn_before_last_persisted_insert = 0;
            for i in round * 20..round * 20 + 10 {
                lsn_before_last_persisted_insert = current_wal_lsn(&client).await;
                client
                    .simple_query(&format!("INSERT INTO recovery VALUES ({i},'{i}');"))
                    .await
                    .unwrap();
            }
            let snapshot_lsn = current_wal_lsn(&client).await;
            backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(snapshot_lsn))
                .await
                .unwrap();
            backend
                .create_snapshot(DATABASE.to_string(), TABLE.to_string(), snapshot_lsn)
                .await
                .unwrap();
            // Rows applied in memory only.
            for i in round * 20 + 10..round * 20 + 20 {
                client
                    .simple_query(&format!("INSERT INTO recovery VALUES ({i},'{i}');"))
                    .await
                    .unwrap();
            }
            let lsn = current_wal_lsn(&client).await;
            backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
                .await
                .unwrap();

            let (backend, testing_directory) = match (guard.take(), recovered.take()) {
                (Some(guard), _) => crash_and_recover_backend_with_guard(guard).await,
                (None, Some((backend, testing_directory))) => {
                    let backend = crash_and_recover_backend(backend, &testing_directory).await;
                    (backend, testing_directory)
                }
                _ => unreachable!(),
            };
            // Recovery re-registers the table, so the slot no longer moves until the next snapshot.
            if confirmed_flush_lsn(&client).await <= lsn_before_last_persisted_insert {
                resent_persisted_transaction = true;
            }
            assert_scan_nonunique_ids_eq(
                &backend,
                DATABASE.to_string(),
                TABLE.to_string(),
                lsn,
                &(0..round * 20 + 20)
                    .map(|i| (i, 1))
                    .collect::<HashMap<_, _>>(),
            )
            .await;
            recovered = Some((backend, testing_directory));
        }
        assert!(
            resent_persisted_transaction,
            "the slot never trailed the iceberg snapshot, so no persisted transaction was re-sent"
        );
    }
}
