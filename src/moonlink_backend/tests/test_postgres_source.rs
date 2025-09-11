mod common;

#[cfg(test)]
mod tests {
    use super::common::{connect_to_postgres, get_database_uri};
    use serial_test::serial;
    use std::time::{SystemTime, UNIX_EPOCH};

    use futures::StreamExt;
    use moonlink_connectors::pg_replicate::initial_copy_writer::create_batch_channel;
    use moonlink_connectors::pg_replicate::postgres_source::PostgresSource;
    use moonlink_connectors::pg_replicate::table::TableName;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_plan_ctid_shards_coverage_and_disjointness() {
        let uri = get_database_uri();
        let (sql, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("ctid_shards_{}", suffix);
        let fqtn = format!("public.{}", table);

        // Create and seed rows
        sql.simple_query(&format!(
            "DROP TABLE IF EXISTS {fqtn};
             CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);
             INSERT INTO {fqtn}
             SELECT gs, 'v'
             FROM generate_series(1, 5000) AS gs;"
        ))
        .await
        .unwrap();
        // Ensure FULL replica identity (defensive)
        sql.simple_query(&format!("ALTER TABLE {fqtn} REPLICA IDENTITY FULL;"))
            .await
            .unwrap();

        // Baseline count
        let baseline: i64 = sql
            .query_one(&format!("SELECT COUNT(*) FROM {fqtn};"), &[])
            .await
            .unwrap()
            .get(0);

        // Build shards via PostgresSource
        let mut ps = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("psource new");
        let tn = TableName {
            schema: "public".to_string(),
            name: table.clone(),
        };
        let shards = ps.plan_ctid_shards(&tn, 4).await.expect("plan shards");
        assert!(!shards.is_empty(), "should produce at least one shard");

        // Coverage: sum counts across shards equals baseline
        let mut sum_counts = 0i64;
        for pred in &shards {
            let cnt: i64 = sql
                .query_one(&format!("SELECT COUNT(*) FROM {fqtn} WHERE {pred};"), &[])
                .await
                .unwrap()
                .get(0);
            sum_counts += cnt;
        }
        assert_eq!(sum_counts, baseline, "shard coverage must equal baseline");

        // Disjointness: pairwise intersections are zero
        for i in 0..shards.len() {
            for j in (i + 1)..shards.len() {
                let p1 = &shards[i];
                let p2 = &shards[j];
                let inter: i64 = sql
                    .query_one(
                        &format!("SELECT COUNT(*) FROM {fqtn} WHERE ({p1}) AND ({p2});"),
                        &[],
                    )
                    .await
                    .unwrap()
                    .get(0);
                assert_eq!(inter, 0, "shards must be disjoint");
            }
        }

        // Cleanup
        sql.simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_get_sharded_copy_stream_snapshot_filtered_rows() {
        let uri = get_database_uri();
        let (ddl, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("gscs_{}", suffix);
        let fqtn = format!("public.{}", table);

        // Create and seed baseline 1..10
        ddl.simple_query(&format!(
            "DROP TABLE IF EXISTS {fqtn};
             CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);
             INSERT INTO {fqtn}
             SELECT gs, 'base'
             FROM generate_series(1, 10) AS gs;"
        ))
        .await
        .unwrap();
        ddl.simple_query(&format!("ALTER TABLE {fqtn} REPLICA IDENTITY FULL;"))
            .await
            .unwrap();

        // Coordinator source: export snapshot
        let mut coord = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("coord source");
        let (snapshot_id, _lsn) = coord
            .export_snapshot_and_lsn()
            .await
            .expect("export snapshot");

        // Insert rows after snapshot
        ddl.simple_query(&format!(
            "INSERT INTO {fqtn} VALUES (11,'n'),(12,'n'),(14,'n');"
        ))
        .await
        .unwrap();

        // Reader source: import snapshot and copy with predicate id % 2 = 0
        let mut reader = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("reader source");
        reader
            .begin_with_snapshot(&snapshot_id)
            .await
            .expect("import snapshot");

        // Fetch schema and start sharded copy stream with predicate
        let schema = reader
            .fetch_table_schema(None, Some(&fqtn), None)
            .await
            .expect("fetch schema via reader");
        let stream = reader
            .get_sharded_copy_stream(&schema.table_name, &schema.column_schemas, "id % 2 = 0")
            .await
            .expect("get sharded copy stream");

        // Count rows from stream
        let mut even_count = 0u32;
        futures::pin_mut!(stream);
        while let Some(row_res) = stream.next().await {
            let _ = row_res.expect("row conversion");
            even_count += 1;
        }
        assert_eq!(even_count, 5, "should see only baseline evens (2,4,6,8,10)");

        // Finalize transactions
        reader.commit_transaction().await.expect("commit reader");
        coord.commit_transaction().await.expect("commit coord");

        // Cleanup
        ddl.simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_spawn_sharded_copy_reader_batches_and_counts() {
        let uri = get_database_uri();
        let (ddl, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("sscr_{}", suffix);
        let fqtn = format!("public.{}", table);

        // Create and seed baseline 1..10
        ddl.simple_query(&format!(
            "DROP TABLE IF EXISTS {fqtn};
             CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);
             INSERT INTO {fqtn}
             SELECT gs, 'base'
             FROM generate_series(1, 10) AS gs;"
        ))
        .await
        .unwrap();
        ddl.simple_query(&format!("ALTER TABLE {fqtn} REPLICA IDENTITY FULL;"))
            .await
            .unwrap();

        // Coordinator source: export snapshot
        let mut coord = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("coord source");
        let (snapshot_id, _lsn) = coord
            .export_snapshot_and_lsn()
            .await
            .expect("export snapshot");

        // Insert rows after snapshot
        ddl.simple_query(&format!(
            "INSERT INTO {fqtn} VALUES (11,'n'),(12,'n'),(14,'n');"
        ))
        .await
        .unwrap();

        // Reader-side: prepare channel and drain task
        let (tx, mut rx) = create_batch_channel(8);
        let drain_handle = tokio::spawn(async move {
            let mut total_rows: u64 = 0;
            while let Some(batch) = rx.recv().await {
                total_rows += batch.num_rows() as u64;
            }
            total_rows
        });

        // Fetch schema via coord and spawn reader with predicate id % 2 = 0
        let schema = coord
            .fetch_table_schema(None, Some(&fqtn), None)
            .await
            .expect("fetch schema via coord");
        let reader_handle = coord
            .spawn_sharded_copy_reader(
                uri.clone(),
                snapshot_id.clone(),
                schema.clone(),
                "id % 2 = 0".to_string(),
                tx,
                1024,
            )
            .await
            .expect("spawn reader");

        let rows_copied = reader_handle
            .await
            .expect("join reader")
            .expect("rows copied");
        let drained = drain_handle.await.expect("join drain");

        assert_eq!(
            rows_copied, drained,
            "reader-reported rows must equal drained rows"
        );
        assert_eq!(rows_copied, 5, "should be 5 baseline even rows");

        // Finalize snapshot (commit)
        coord
            .finalize_snapshot(true)
            .await
            .expect("finalize snapshot");

        // Cleanup
        ddl.simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_spawn_sharded_copy_readers_aggregate_coverage() {
        let uri = get_database_uri();
        let (ddl, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("sscrs_{}", suffix);
        let fqtn = format!("public.{}", table);

        // Create and seed baseline 1..100
        ddl.simple_query(&format!(
            "DROP TABLE IF EXISTS {fqtn};
             CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);
             INSERT INTO {fqtn}
             SELECT gs, 'base'
             FROM generate_series(1, 100) AS gs;"
        ))
        .await
        .unwrap();
        ddl.simple_query(&format!("ALTER TABLE {fqtn} REPLICA IDENTITY FULL;"))
            .await
            .unwrap();

        // Coordinator source: export snapshot
        let mut coord = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("coord source");
        let (snapshot_id, _lsn) = coord
            .export_snapshot_and_lsn()
            .await
            .expect("export snapshot");

        // Insert rows after snapshot (should not be visible)
        ddl.simple_query(&format!(
            "INSERT INTO {fqtn} VALUES (101,'n'),(102,'n'),(103,'n');"
        ))
        .await
        .unwrap();

        // Prepare channel and drain task
        let (tx, mut rx) = create_batch_channel(8);
        let drain_handle = tokio::spawn(async move {
            let mut total_rows: u64 = 0;
            while let Some(batch) = rx.recv().await {
                total_rows += batch.num_rows() as u64;
            }
            total_rows
        });

        // Fetch schema and shard predicates, then spawn readers
        let schema = coord
            .fetch_table_schema(None, Some(&fqtn), None)
            .await
            .expect("fetch schema");
        let tn = TableName {
            schema: "public".to_string(),
            name: table.clone(),
        };
        let preds = coord.plan_ctid_shards(&tn, 4).await.expect("plan shards");
        assert!(!preds.is_empty(), "expected at least one shard predicate");

        let handles = coord
            .spawn_sharded_copy_readers(
                uri.clone(),
                snapshot_id.clone(),
                schema.clone(),
                preds,
                tx,
                2048,
            )
            .await
            .expect("spawn readers");

        // Sum rows_copied across readers
        let mut summed_rows: u64 = 0;
        for h in handles {
            let n = h.await.expect("join reader").expect("rows_copied");
            summed_rows += n;
        }

        // Get drained rows
        let drained = drain_handle.await.expect("join drain");

        assert_eq!(
            summed_rows, drained,
            "sum of reader rows must equal drained rows"
        );
        assert_eq!(
            drained, 100,
            "total rows copied should equal baseline count"
        );

        // Finalize snapshot (commit)
        coord
            .finalize_snapshot(true)
            .await
            .expect("finalize snapshot");

        // Cleanup
        ddl.simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_finalize_snapshot_commit_releases_locks() {
        let uri = get_database_uri();
        let (sql, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("fs_commit_{}", suffix);
        let fqtn = format!("public.{}", table);

        // Create and seed
        sql.simple_query(&format!(
            "DROP TABLE IF EXISTS {fqtn};
             CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);
             INSERT INTO {fqtn} VALUES (1,'a');"
        ))
        .await
        .unwrap();

        // Export snapshot, then finalize with success=true
        let mut ps = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("ps new");
        let _ = ps.export_snapshot_and_lsn().await.expect("export snapshot");
        ps.finalize_snapshot(true).await.expect("finalize commit");

        // DROP TABLE should succeed (no lingering locks)
        sql.simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_finalize_snapshot_rollback_releases_locks() {
        let uri = get_database_uri();
        let (sql, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("fs_rollback_{}", suffix);
        let fqtn = format!("public.{}", table);

        // Create and seed
        sql.simple_query(&format!(
            "DROP TABLE IF EXISTS {fqtn};
             CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);
             INSERT INTO {fqtn} VALUES (1,'a');"
        ))
        .await
        .unwrap();

        // Export snapshot, then finalize with success=false
        let mut ps = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("ps new");
        let _ = ps.export_snapshot_and_lsn().await.expect("export snapshot");
        ps.finalize_snapshot(false)
            .await
            .expect("finalize rollback");

        // DROP TABLE should succeed (no lingering locks)
        sql.simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }
}
