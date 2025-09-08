mod common;

#[cfg(test)]
mod tests {

    use super::common::{connect_to_postgres, get_database_uri};
    use moonlink_connectors::pg_replicate::PostgresConnection;
    use serial_test::serial;

    use tokio::task::yield_now;
    use tokio::time::{sleep, Duration};
    use tokio_postgres::SimpleQueryMessage;

    // Helper: fetch backend pid of the control-plane client via SELECT pg_backend_pid()
    async fn get_control_pid(conn: &mut PostgresConnection) -> i32 {
        let res = conn
            .run_control_query("SELECT pg_backend_pid();")
            .await
            .unwrap();
        for msg in res {
            if let SimpleQueryMessage::Row(row) = msg {
                let pid: i32 = row.get(0).unwrap().parse().unwrap();
                return pid;
            }
        }
        panic!("no pid row returned");
    }

    // Helper: show lock_timeout to verify settings reapplied on reconnect
    async fn get_lock_timeout(conn: &mut PostgresConnection) -> String {
        let res = conn.run_control_query("SHOW lock_timeout;").await.unwrap();
        for msg in res {
            if let SimpleQueryMessage::Row(row) = msg {
                return row.get(0).unwrap().to_string();
            }
        }
        panic!("no lock_timeout row returned");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_run_control_query_reconnects_after_terminate() {
        let uri = get_database_uri();
        // Build a dedicated PostgresConnection (control-plane)
        let mut conn = PostgresConnection::new(uri.clone()).await.unwrap();

        // Baseline: SELECT 1 works
        let _ = conn.run_control_query("SELECT 1;").await.unwrap();

        // Terminate the backend PID for this connection
        let pid = get_control_pid(&mut conn).await;
        let (admin, _handle) = connect_to_postgres(&uri).await;
        let _ = admin
            .simple_query(&format!("SELECT pg_terminate_backend({pid});"))
            .await
            .unwrap();

        // Next query should trigger reconnect and succeed
        let _ = conn.run_control_query("SELECT 1;").await.unwrap();
        // Verify session settings reapplied
        let lt = get_lock_timeout(&mut conn).await;
        assert_eq!(lt, "100ms");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_alter_table_replica_identity_after_terminate() {
        let uri = get_database_uri();
        // Setup: ensure a small temp table exists
        let (client, _handle) = connect_to_postgres(&uri).await;
        let _ = client
            .simple_query(
                "DROP TABLE IF EXISTS retry_test;
                 CREATE TABLE retry_test (id BIGINT PRIMARY KEY, name TEXT);",
            )
            .await
            .unwrap();

        // Build a dedicated PostgresConnection
        let mut conn = PostgresConnection::new(uri.clone()).await.unwrap();

        // Kill control-plane backend
        let pid = get_control_pid(&mut conn).await;
        let (admin, _h) = connect_to_postgres(&uri).await;
        let _ = admin
            .simple_query(&format!("SELECT pg_terminate_backend({pid});"))
            .await
            .unwrap();

        // This should reconnect and apply
        conn.alter_table_replica_identity("retry_test")
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_drop_replication_slot_after_terminate() {
        let uri = get_database_uri();
        let mut conn = PostgresConnection::new(uri.clone()).await.unwrap();

        // Kill control-plane backend to force reconnect on next call
        let pid = get_control_pid(&mut conn).await;
        let (admin, _h) = connect_to_postgres(&uri).await;
        let _ = admin
            .simple_query(&format!("SELECT pg_terminate_backend({pid});"))
            .await
            .unwrap();

        // Even if slot doesn't exist, this should succeed (or no-op)
        let _ = conn.drop_replication_slot().await;
    }

    /// Test: non-transport SQL error does not loop infinitely and returns error
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_run_control_query_non_transport_error() {
        let uri = get_database_uri();
        let mut conn = PostgresConnection::new(uri).await.unwrap();
        let err = conn.run_control_query("THIS IS NOT SQL").await.err();
        assert!(err.is_some());
    }

    /// Test: attempt_drop_else_retry transport error path schedules background retry and completes
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_attempt_drop_transport_error_background_retry() {
        let uri = get_database_uri();
        // Prepare table and publication membership
        let (admin, _h) = connect_to_postgres(&uri).await;
        let _ = admin
            .simple_query(
                "DROP TABLE IF EXISTS retry_drop;
                 CREATE TABLE retry_drop (id BIGINT PRIMARY KEY, name TEXT);
                 DROP PUBLICATION IF EXISTS moonlink_pub;
                 CREATE PUBLICATION moonlink_pub WITH (publish_via_partition_root = true);
                 ALTER PUBLICATION moonlink_pub ADD TABLE public.retry_drop;",
            )
            .await
            .unwrap();

        // Create a PostgresConnection (will set lock_timeout too)
        let mut conn = PostgresConnection::new(uri.clone()).await.unwrap();

        // Kill control-plane backend to force a transport error on the first simple_query
        let pid = get_control_pid(&mut conn).await;
        let (killer, _hk) = connect_to_postgres(&uri).await;
        let _ = killer
            .simple_query(&format!("SELECT pg_terminate_backend({pid});"))
            .await
            .unwrap();

        // Call remove_table_from_publication: should return quickly and schedule background retry
        conn.remove_table_from_publication("public.retry_drop")
            .await
            .unwrap();

        // Wait for background retries to complete, then assert membership removed
        conn.wait_for_pending_retries().await;

        let rows = admin
            .simple_query(
                "SELECT 1 FROM pg_publication_tables WHERE pubname = 'moonlink_pub' AND schemaname = 'public' AND tablename = 'retry_drop';",
            )
            .await
            .unwrap();
        // Expect no rows (table removed from publication)
        assert!(rows
            .iter()
            .all(|m| !matches!(m, SimpleQueryMessage::Row(_))));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[serial]
    async fn test_run_control_query_backoff_virtual_time() {
        let uri = get_database_uri();
        // Build a control-plane connection
        let mut conn = PostgresConnection::new(uri.clone()).await.unwrap();
        let _ = conn.run_control_query("SELECT 1;").await.unwrap();

        // Induce transport error for the first attempt
        let pid = get_control_pid(&mut conn).await;
        let (admin, _h) = connect_to_postgres(&uri).await;
        let _ = admin
            .simple_query(&format!("SELECT pg_terminate_backend({pid});"))
            .await
            .unwrap();

        // Spawn the retried query; it should block on the first backoff sleep (300ms)
        let handle = tokio::spawn(async move { conn.run_control_query("SELECT 1;").await });

        // Give the task a chance to start and hit the sleep
        yield_now().await;
        assert!(!handle.is_finished(), "should be waiting on backoff sleep");

        // Advance less than backoff duration; should still be pending
        sleep(Duration::from_millis(299)).await; // with start_paused, this advances virtual time
        yield_now().await;
        assert!(
            !handle.is_finished(),
            "should still be waiting before 300ms"
        );

        // Cross the backoff boundary; retry should proceed and complete
        sleep(Duration::from_millis(1)).await;
        let res = handle.await.unwrap();
        assert!(res.is_ok(), "expected query to succeed after backoff");
    }
}
