mod common;

#[cfg(test)]
mod tests {
    use super::common::{
        connect_to_postgres, current_wal_lsn, get_database_uri, TestGuard, DATABASE, TABLE,
    };
    use serial_test::serial;

    // Helper: terminate replication using a separate connection to avoid borrowing conflicts

    use crate::common::nonunique_ids_from_state;

    async fn terminate_replication_new_conn() {
        let uri = get_database_uri();
        let (client, connection) = connect_to_postgres(&uri).await;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let _ = client
            .simple_query(
                "SELECT pg_terminate_backend(active_pid)\n                 FROM pg_replication_slots\n                 WHERE slot_name LIKE 'moonlink_slot%' AND active_pid IS NOT NULL;",
            )
            .await;
    }

    /// Reconnect resumes replication (single table)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_reconnect_resumes_replication_single_table() {
        let (guard, client) = TestGuard::new(Some("reconnect_single"), true).await;
        let backend = guard.backend();

        // Insert a baseline row and verify it's visible
        client
            .simple_query("INSERT INTO reconnect_single VALUES (1,'a');")
            .await
            .unwrap();
        let lsn1 = current_wal_lsn(&client).await;
        let ids = nonunique_ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn1))
                .await
                .unwrap(),
        );
        let mut keys: Vec<i64> = ids.keys().cloned().collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![1]);

        // Terminate replication to force reconnect
        terminate_replication_new_conn().await;

        // Insert rows after termination; these should be replicated after reconnect
        client
            .simple_query("INSERT INTO reconnect_single VALUES (2,'b'),(3,'c');")
            .await
            .unwrap();
        let lsn2 = current_wal_lsn(&client).await;

        // Wait until WAL flush reaches lsn2, then verify rows once
        let ids = nonunique_ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn2))
                .await
                .unwrap(),
        );
        let mut keys: Vec<i64> = ids.keys().cloned().collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![1, 2, 3]);
    }

    /// Reconnect mid-traffic with a large batch (100k rows) to exercise streaming; no duplicates or drops.

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_reconnect_resumes_replication_large_streaming_batch() {
        let (guard, client) = TestGuard::new(Some("reconnect_streaming"), true).await;
        let backend = guard.backend();

        // Baseline row
        client
            .simple_query("INSERT INTO reconnect_streaming VALUES (1,'a');")
            .await
            .unwrap();
        let lsn1 = current_wal_lsn(&client).await;
        let ids = nonunique_ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn1))
                .await
                .unwrap(),
        );
        let expected_len = 1;
        assert_eq!(ids.len(), expected_len);
        let mut keys: Vec<i64> = ids.keys().cloned().collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![1]);

        // Force disconnect to trigger reconnect
        terminate_replication_new_conn().await;

        // Insert a large batch while disconnected (should trigger streamed xact)
        client
            .simple_query(
                "INSERT INTO reconnect_streaming (id, name)
                 SELECT gs, 'v_' || gs::text FROM generate_series(2, 100001) AS gs;",
            )
            .await
            .unwrap();
        let lsn2 = current_wal_lsn(&client).await;

        let expected_len = 100001usize;
        let ids = nonunique_ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn2))
                .await
                .unwrap(),
        );
        assert_eq!(ids.len(), expected_len);
        let mut keys: Vec<i64> = ids.keys().cloned().collect();
        keys.sort_unstable();
        let expected_keys: Vec<i64> = (1..=expected_len as i64).collect();
        assert_eq!(keys, expected_keys);
    }

    /// Reconnect preserves multiple tables

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_reconnect_preserves_multiple_tables() {
        let (guard, client) = TestGuard::new(Some("reconnect_multi_a"), true).await;
        let backend = guard.backend();

        // Create second source table and register a second moonlink table
        client
            .simple_query("DROP TABLE IF EXISTS reconnect_multi_b; CREATE TABLE reconnect_multi_b (id BIGINT PRIMARY KEY, name TEXT);")
            .await
            .unwrap();
        let table_b = format!("{TABLE}_b");
        let uri = get_database_uri();
        backend
            .create_table(
                DATABASE.to_string(),
                table_b.clone(),
                /*table_name=*/ "public.reconnect_multi_b".to_string(),
                uri.to_string(),
                guard.get_serialized_table_config(),
                None,
            )
            .await
            .unwrap();

        // Baseline inserts into both tables
        client
            .simple_query("INSERT INTO reconnect_multi_a VALUES (1,'a1'),(2,'a2');")
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO reconnect_multi_b VALUES (10,'b1'),(20,'b2');")
            .await
            .unwrap();
        let lsn1 = current_wal_lsn(&client).await;

        // Verify baseline visible on both
        let ids_a = nonunique_ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn1))
                .await
                .unwrap(),
        );
        let expected_len = 2;
        assert_eq!(ids_a.len(), expected_len);
        let mut keys_a: Vec<i64> = ids_a.keys().cloned().collect();
        keys_a.sort_unstable();
        assert_eq!(keys_a, vec![1, 2]);
        let ids_b = nonunique_ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), table_b.clone(), Some(lsn1))
                .await
                .unwrap(),
        );
        let expected_len = 2;
        assert_eq!(ids_b.len(), expected_len);
        let mut keys_b: Vec<i64> = ids_b.keys().cloned().collect();
        keys_b.sort_unstable();
        assert_eq!(keys_b, vec![10, 20]);

        // Terminate replication to force reconnect
        terminate_replication_new_conn().await;

        // New inserts after termination
        client
            .simple_query("INSERT INTO reconnect_multi_a VALUES (3,'a3'),(4,'a4');")
            .await
            .unwrap();
        client
            .simple_query("INSERT INTO reconnect_multi_b VALUES (30,'b3'),(40,'b4');")
            .await
            .unwrap();
        let lsn2 = current_wal_lsn(&client).await;

        // sleep for 1 second
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        // Verify both tables include all rows up to lsn2
        let ids_a = nonunique_ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn2))
                .await
                .unwrap(),
        );
        let expected_len = 4;
        assert_eq!(ids_a.len(), expected_len);
        let mut keys_a: Vec<i64> = ids_a.keys().cloned().collect();
        keys_a.sort_unstable();
        assert_eq!(keys_a, vec![1, 2, 3, 4]);
        // vec of i64
        let ids_b = nonunique_ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), table_b, Some(lsn2))
                .await
                .unwrap(),
        );
        let expected_len = 4;
        assert_eq!(ids_b.len(), expected_len);
        let mut keys_b: Vec<i64> = ids_b.keys().cloned().collect();
        keys_b.sort_unstable();
        assert_eq!(keys_b, vec![10, 20, 30, 40]);
    }

    /// Large transaction across client termination: abort mid-streaming, re-issue, no duplicates.

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_abort_client_mid_streaming_no_duplicates() {
        let uri = get_database_uri();
        let (guard, mut client1) = TestGuard::new(Some("abort_mid_streaming"), true).await;
        let backend = guard.backend();

        // Capture backend pid for client1 so we can terminate it mid-transaction.
        let pid_row = client1
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap();
        let client1_pid: i32 = pid_row.get(0);

        // Begin a large transaction that should produce streamed events before commit.
        let tx = client1.transaction().await.unwrap();
        let total: i64 = 500_000;
        tx.execute(
            &format!(
                "INSERT INTO abort_mid_streaming (id, name)
                 SELECT gs, 'v_' || gs::text FROM generate_series(1, {total}) AS gs;"
            ),
            &[],
        )
        .await
        .unwrap();

        // Terminate the actual client session running the transaction to force an abort.
        let (admin, _ha) = crate::common::connect_to_postgres(&uri).await;
        let _ = admin
            .simple_query(&format!("SELECT pg_terminate_backend({client1_pid});"))
            .await
            .unwrap();

        // Re-issue the same logical operation on a fresh session and commit.
        let (client2, _h2) = crate::common::connect_to_postgres(&uri).await;
        client2
            .simple_query(&format!(
                "INSERT INTO abort_mid_streaming (id, name)
                 SELECT gs, 'v_' || gs::text FROM generate_series(1, {total}) AS gs;"
            ))
            .await
            .unwrap();

        // Read up to current LSN and verify no duplicates for 1..=total.
        let lsn = current_wal_lsn(&client2).await;
        let ids = nonunique_ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
                .await
                .unwrap(),
        );
        let expected_len = total as usize;
        assert_eq!(ids.len(), expected_len);
        let mut keys: Vec<i64> = ids.keys().cloned().collect();
        keys.sort_unstable();
        let expected_keys: Vec<i64> = (1..=total).collect();
        assert_eq!(keys, expected_keys);
    }

    /// Large transaction across reconnect

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_large_transaction_across_reconnect() {
        let (guard, mut client) = TestGuard::new(Some("txn_large"), true).await;
        let backend = guard.backend();

        // Begin a large transaction inserting many rows in batches
        let tx = client.transaction().await.unwrap();
        let total: i64 = 1_000_000;
        let batch: i64 = 500;
        let mut inserted: i64 = 0;
        while inserted < total {
            let start = inserted + 1;
            let end = (inserted + batch).min(total);
            let stmt = format!(
                "INSERT INTO txn_large (id, name) SELECT gs, 'v_' || gs::text FROM generate_series({start}, {end}) AS gs;"
            );
            tx.execute(stmt.as_str(), &[]).await.unwrap();
            inserted = end;
            if inserted == total / 2 {
                // Disconnect replication mid-way
                terminate_replication_new_conn().await;
            }
        }

        // Commit after disconnect; reconnect should resume and apply once
        tx.commit().await.unwrap();
        let lsn = current_wal_lsn(&client).await;

        // Verify all rows 1..=total appear exactly once
        let ids = nonunique_ids_from_state(
            &backend
                .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
                .await
                .unwrap(),
        );
        let expected_len = total as usize;
        assert_eq!(ids.len(), expected_len);
        let mut keys: Vec<i64> = ids.keys().cloned().collect();
        keys.sort_unstable();
        let expected_keys: Vec<i64> = (1..=total).collect();
        assert_eq!(keys, expected_keys);
    }
}
