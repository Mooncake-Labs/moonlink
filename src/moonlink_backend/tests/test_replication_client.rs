mod common;

#[cfg(test)]
mod tests {
    use super::common::{connect_to_postgres, get_database_uri};
    use serial_test::serial;
    use std::time::{SystemTime, UNIX_EPOCH};

    use moonlink_connectors::pg_replicate::clients::postgres::ReplicationClient;
    use moonlink_connectors::pg_replicate::table::TableName;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_estimate_relation_block_count_monotonic() {
        let uri = get_database_uri();
        let (sql_client, _conn) = connect_to_postgres(&uri).await;

        // Unique table name per run to avoid collisions
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("block_count_{}", suffix);
        let fqtn = format!("public.{}", table);

        // Create a small table
        sql_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {fqtn};
                 CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, filler TEXT);"
            ))
            .await
            .unwrap();

        // Connect replication client (non-replication mode) and drive connection
        let (mut rc, conn) = ReplicationClient::connect(&uri, false)
            .await
            .expect("connect rc");
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("connection error: {}", e);
            }
        });

        let tn = TableName {
            schema: "public".to_string(),
            name: table.clone(),
        };

        // Initial block count (empty table)
        let b1 = rc
            .estimate_relation_block_count(&tn)
            .await
            .expect("estimate blocks");

        // Insert enough rows to reasonably grow relation size
        sql_client
            .simple_query(&format!(
                "INSERT INTO {fqtn}
                 SELECT gs, repeat('x', 200)
                 FROM generate_series(1, 5000) AS gs;"
            ))
            .await
            .unwrap();

        // Estimate again
        let b2 = rc
            .estimate_relation_block_count(&tn)
            .await
            .expect("re-estimate blocks");

        assert!(b1 >= 0, "initial blocks should be >= 0, got {}", b1);
        assert!(b2 >= b1, "blocks should be monotonic: b1={}, b2={}", b1, b2);

        // Cleanup
        sql_client
            .simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_export_snapshot_and_lsn_isolation_and_boundary() {
        let uri = get_database_uri();
        let (ddl_client, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("snap_test_{}", suffix);
        let fqtn = format!("public.{}", table);

        // Create and seed baseline
        ddl_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {fqtn};
                 CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);
                 INSERT INTO {fqtn} VALUES (1,'a'),(2,'b'),(3,'c');"
            ))
            .await
            .unwrap();

        // Coordinator: export snapshot + lsn (keep txn open)
        let (mut coord, coord_conn) = ReplicationClient::connect(&uri, false)
            .await
            .expect("connect coord");
        tokio::spawn(async move {
            if let Err(e) = coord_conn.await {
                eprintln!("coord connection error: {}", e);
            }
        });
        let (snapshot_id, lsn_at_export) = coord
            .export_snapshot_and_lsn()
            .await
            .expect("export snapshot");

        // Concurrent: insert more rows after snapshot
        ddl_client
            .simple_query(&format!("INSERT INTO {fqtn} VALUES (4,'d'),(5,'e');"))
            .await
            .unwrap();

        // Session bound to snapshot should see only baseline rows (3)
        // Use a plain SQL client, import the snapshot and count
        let (snap_sql, _snap_conn) = connect_to_postgres(&uri).await;
        snap_sql
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await
            .unwrap();
        snap_sql
            .simple_query(&format!("SET TRANSACTION SNAPSHOT '{}';", snapshot_id))
            .await
            .unwrap();
        let snapshot_count_row = snap_sql
            .query_one(&format!("SELECT COUNT(*) FROM {fqtn};"), &[])
            .await
            .expect("count under snapshot");
        let snapshot_count: i64 = snapshot_count_row.get(0);
        assert_eq!(snapshot_count, 3, "snapshot should see only baseline rows");
        // Commit the snapshot-bound read-only txn to release relation locks
        snap_sql.simple_query("COMMIT;").await.unwrap();

        // Fresh session should see all rows (5)
        let fresh_count = ddl_client
            .query_one(&format!("SELECT COUNT(*) FROM {fqtn};"), &[])
            .await
            .expect("fresh count");
        let fresh: i64 = fresh_count.get(0);
        assert_eq!(fresh, 5, "fresh session sees new rows");

        // Current WAL LSN should be >= exported lsn
        let (mut lsn_sess, lsn_conn) = ReplicationClient::connect(&uri, false)
            .await
            .expect("connect lsn sess");
        tokio::spawn(async move {
            if let Err(e) = lsn_conn.await {
                eprintln!("lsn connection error: {}", e);
            }
        });
        let current_lsn = lsn_sess.get_current_wal_lsn().await.expect("current lsn");
        assert!(u64::from(current_lsn) >= u64::from(lsn_at_export));

        // Commit the coordinator transaction that exported the snapshot
        coord.commit_txn().await.expect("commit coord txn");

        // Cleanup
        ddl_client
            .simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_copy_out_with_predicate_snapshot_filtered_rows() {
        use futures::StreamExt;
        use moonlink_connectors::pg_replicate::postgres_source::PostgresSource;

        let uri = get_database_uri();
        let (ddl_client, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("copy_pred_{}", suffix);
        let fqtn = format!("public.{}", table);

        // Create and seed baseline rows 1..10
        ddl_client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {fqtn};
                 CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);
                 INSERT INTO {fqtn}
                 SELECT gs, 'base'
                 FROM generate_series(1, 10) AS gs;"
            ))
            .await
            .unwrap();
        // Ensure FULL replica identity for schema fetch expectations
        ddl_client
            .simple_query(&format!("ALTER TABLE {fqtn} REPLICA IDENTITY FULL;"))
            .await
            .unwrap();

        // Coordinator: export snapshot + lsn (keep txn open)
        let (mut coord, coord_conn) = ReplicationClient::connect(&uri, false)
            .await
            .expect("connect coord");
        tokio::spawn(async move {
            if let Err(e) = coord_conn.await {
                eprintln!("coord connection error: {}", e);
            }
        });
        let (snapshot_id, _lsn_at_export) = coord
            .export_snapshot_and_lsn()
            .await
            .expect("export snapshot");

        // Insert rows AFTER snapshot (should not be visible under imported snapshot)
        ddl_client
            .simple_query(&format!(
                "INSERT INTO {fqtn} VALUES (11,'new'),(12,'new'),(14,'new');"
            ))
            .await
            .unwrap();

        // Fetch column schemas using PostgresSource to drive copy_out_with_predicate
        let ps = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("psource new");
        let schema = ps
            .fetch_table_schema(None, Some(&fqtn), None)
            .await
            .expect("fetch schema");
        let columns = schema.column_schemas;

        // Reader session: import snapshot and COPY with predicate id % 2 = 0
        let (mut reader, reader_conn) = ReplicationClient::connect(&uri, false)
            .await
            .expect("connect reader");
        tokio::spawn(async move {
            if let Err(e) = reader_conn.await {
                eprintln!("reader connection error: {}", e);
            }
        });
        reader
            .begin_with_snapshot(&snapshot_id)
            .await
            .expect("import snapshot");

        let tn = TableName {
            schema: "public".to_string(),
            name: table.clone(),
        };
        let stream = reader
            .copy_out_with_predicate(&tn, &columns, "id % 2 = 0")
            .await
            .expect("copy out with predicate");

        // Count rows from COPY stream
        let mut even_count = 0u32;
        futures::pin_mut!(stream);
        while let Some(row_res) = stream.next().await {
            let _ = row_res.expect("row bytes");
            even_count += 1;
        }

        // Baseline 1..10 => evens: 5 (2,4,6,8,10). New evens (12,14) must NOT be visible.
        assert_eq!(
            even_count, 5,
            "snapshot copy should include only baseline evens"
        );

        // Commit reader and coordinator txns to release locks
        reader.commit_txn().await.expect("commit reader");
        coord.commit_txn().await.expect("commit coord");

        // Cleanup
        ddl_client
            .simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }
}
