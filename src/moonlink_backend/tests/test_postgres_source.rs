mod common;

#[cfg(test)]
mod tests {
    use super::common::{connect_to_postgres, get_database_uri};
    use futures::StreamExt;
    use moonlink::TableEvent;
    use moonlink_connectors::pg_replicate::initial_copy::{
        copy_table_stream, InitialCopyConfig, InitialCopyReaderConfig,
    };
    use moonlink_connectors::pg_replicate::initial_copy_writer::{
        create_batch_channel, InitialCopyWriterConfig,
    };
    use moonlink_connectors::pg_replicate::postgres_source::PostgresSource;
    use moonlink_connectors::pg_replicate::table::TableName;
    use serial_test::serial;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

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
        let table = format!("ctid_shards_{suffix}");
        let fqtn = format!("public.{table}");

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
        let table = format!("gscs_{suffix}");
        let fqtn = format!("public.{table}");

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
        let table = format!("sscr_{suffix}");
        let fqtn = format!("public.{table}");

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
    async fn test_initial_copy_uses_base_path() {
        let uri = get_database_uri();
        let (ddl, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("ic_basepath_{suffix}");
        let fqtn = format!("public.{table}");

        // Create and seed baseline 2 rows
        ddl.simple_query(&format!(
            "DROP TABLE IF EXISTS {fqtn};
             CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);
             INSERT INTO {fqtn} VALUES (1,'a'),(2,'b');"
        ))
        .await
        .unwrap();
        ddl.simple_query(&format!("ALTER TABLE {fqtn} REPLICA IDENTITY FULL;"))
            .await
            .unwrap();

        // Fetch schema
        let ps = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("ps new");
        let schema = ps
            .fetch_table_schema(None, Some(&fqtn), None)
            .await
            .expect("fetch schema");

        // Prepare base_path and config
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().to_str().unwrap();
        let ic_cfg = InitialCopyConfig {
            reader: InitialCopyReaderConfig {
                uri: uri.clone(),
                shard_count: 1,
            },
            writer: InitialCopyWriterConfig::default(),
        };

        // Channel for LoadFiles
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);

        // Run initial copy directly
        let _progress = copy_table_stream(schema.clone(), &tx, base_path, ic_cfg)
            .await
            .expect("copy_table_stream");

        // Expect LoadFiles event and verify root_directory
        if let Some(TableEvent::LoadFiles {
            storage_config,
            files,
            ..
        }) = rx.recv().await
        {
            let expected_dir = std::path::Path::new(base_path)
                .join("initial_copy")
                .join(format!("table_{}", schema.src_table_id));
            assert_eq!(
                storage_config.get_root_path(),
                expected_dir.to_str().unwrap()
            );
            assert!(!files.is_empty(), "should have at least one parquet file");
            // Files exist
            for f in files {
                assert!(std::path::Path::new(&f).exists());
            }
        } else {
            panic!("expected LoadFiles event");
        }

        // Cleanup
        ddl.simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn ic_writer_error_does_not_emit_loadfiles() {
        let uri = get_database_uri();
        let (ddl, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("ic_writer_err_{suffix}");
        let fqtn = format!("public.{table}");

        // Create and seed baseline rows
        ddl.simple_query(&format!(
            "DROP TABLE IF EXISTS {fqtn};
             CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);
             INSERT INTO {fqtn} VALUES (1,'a');"
        ))
        .await
        .unwrap();
        ddl.simple_query(&format!("ALTER TABLE {fqtn} REPLICA IDENTITY FULL;"))
            .await
            .unwrap();

        // Fetch schema to get src_table_id
        let ps = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("ps new");
        let schema = ps
            .fetch_table_schema(None, Some(&fqtn), None)
            .await
            .expect("fetch schema");

        // Prepare base_path and create a FILE at the would-be output directory to force writer failure
        let tmp = tempdir().unwrap();
        let base_path = tmp.path();
        let collide_dir = base_path
            .join("initial_copy")
            .join(format!("table_{}", schema.src_table_id));
        // Ensure parent exists and then create a file at collide_dir
        if let Some(parent) = collide_dir.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&collide_dir, b"block").await.unwrap();

        // Build config (single reader)
        let ic_cfg = InitialCopyConfig {
            reader: InitialCopyReaderConfig {
                uri: uri.clone(),
                shard_count: 1,
            },
            writer: InitialCopyWriterConfig::default(),
        };

        // Channel for LoadFiles
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);

        // Run initial copy and expect failure due to writer error
        let err = copy_table_stream(schema.clone(), &tx, base_path.to_str().unwrap(), ic_cfg)
            .await
            .expect_err("expected failure");
        let s = err.to_string().to_lowercase();
        assert!(
            s.contains("io") || s.contains("parquet") || s.contains("create") || s.contains("file")
        );

        // Ensure no LoadFiles event was emitted
        let recv_res = timeout(Duration::from_millis(300), rx.recv()).await;
        assert!(
            recv_res.is_err(),
            "no LoadFiles event should be emitted on failure"
        );

        // Cleanup
        ddl.simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn ic_empty_with_n_gt_1_parallel() {
        let uri = get_database_uri();
        let (ddl, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("ic_empty_parallel_{suffix}");
        let fqtn = format!("public.{table}");

        // Create empty table
        ddl.simple_query(&format!(
            "DROP TABLE IF EXISTS {fqtn};
             CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);"
        ))
        .await
        .unwrap();
        ddl.simple_query(&format!("ALTER TABLE {fqtn} REPLICA IDENTITY FULL;"))
            .await
            .unwrap();

        // Fetch schema
        let ps = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("ps new");
        let schema = ps
            .fetch_table_schema(None, Some(&fqtn), None)
            .await
            .expect("fetch schema");

        // Config: shard_count=4
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().to_str().unwrap();
        let ic_cfg = InitialCopyConfig {
            reader: InitialCopyReaderConfig {
                uri: uri.clone(),
                shard_count: 4,
            },
            writer: InitialCopyWriterConfig::default(),
        };

        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let _progress = copy_table_stream(schema.clone(), &tx, base_path, ic_cfg)
            .await
            .expect("copy_table_stream");

        if let Some(TableEvent::LoadFiles { files, .. }) = rx.recv().await {
            assert!(
                files.is_empty(),
                "empty table should yield empty files list"
            );
        } else {
            panic!("expected LoadFiles event");
        }

        ddl.simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_parallel_multi_reader_end_to_end_non_empty() {
        let uri = get_database_uri();
        let (ddl, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("ic_parallel_non_empty_{suffix}");
        let fqtn = format!("public.{table}");

        // Create and seed baseline rows (non-empty)
        let baseline_rows = 1000i64;
        ddl.simple_query(&format!(
            "DROP TABLE IF EXISTS {fqtn};
             CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);
             INSERT INTO {fqtn}
             SELECT gs, 'base'
             FROM generate_series(1, {baseline_rows}) AS gs;"
        ))
        .await
        .unwrap();
        ddl.simple_query(&format!("ALTER TABLE {fqtn} REPLICA IDENTITY FULL;"))
            .await
            .unwrap();

        // Baseline count
        let baseline: i64 = ddl
            .query_one(&format!("SELECT COUNT(*) FROM {fqtn};"), &[])
            .await
            .unwrap()
            .get(0);
        assert!(baseline > 0, "baseline must be > 0");

        // Fetch schema
        let ps = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("ps new");
        let schema = ps
            .fetch_table_schema(None, Some(&fqtn), None)
            .await
            .expect("fetch schema");

        // Config: shard_count=4
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().to_str().unwrap();
        let ic_cfg = InitialCopyConfig {
            reader: InitialCopyReaderConfig {
                uri: uri.clone(),
                shard_count: 4,
            },
            writer: InitialCopyWriterConfig::default(),
        };

        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let progress = copy_table_stream(schema.clone(), &tx, base_path, ic_cfg)
            .await
            .expect("copy_table_stream");

        // Rows copied equals baseline
        assert_eq!(progress.rows_copied, baseline as u64);

        // Exactly one LoadFiles event; verify lsn and files
        if let Some(TableEvent::LoadFiles { files, lsn, .. }) = rx.recv().await {
            assert!(!files.is_empty(), "non-empty table should yield files");
            assert_eq!(
                lsn,
                u64::from(progress.boundary_lsn),
                "LoadFiles lsn must equal boundary_lsn"
            );

            // Current WAL LSN should be >= boundary
            let mut lsn_src = PostgresSource::new(&uri, None, None, false)
                .await
                .expect("lsn source");
            let current_wal = lsn_src.get_current_wal_lsn().await.expect("current wal");
            assert!(
                lsn <= u64::from(current_wal),
                "boundary lsn must be <= current WAL"
            );
        } else {
            panic!("expected LoadFiles event");
        }

        // Ensure no additional events
        let more = timeout(Duration::from_millis(300), rx.recv()).await;
        assert!(more.is_err(), "should emit exactly one LoadFiles event");

        // Cleanup
        ddl.simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_spawn_sharded_copy_reader_fails_on_poison_predicate() {
        let uri = get_database_uri();
        let (ddl, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("reader_poison_{suffix}");
        let fqtn = format!("public.{table}");

        // Create and seed baseline 1..100
        let baseline_rows = 100i64;
        ddl.simple_query(&format!(
            "DROP TABLE IF EXISTS {fqtn};
             CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);
             INSERT INTO {fqtn}
             SELECT gs, 'base'
             FROM generate_series(1, {baseline_rows}) AS gs;"
        ))
        .await
        .unwrap();
        ddl.simple_query(&format!("ALTER TABLE {fqtn} REPLICA IDENTITY FULL;"))
            .await
            .unwrap();

        // Coordinator: export snapshot (keeps txn open)
        let mut coord = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("coord source");
        let (snapshot_id, _lsn) = coord
            .export_snapshot_and_lsn()
            .await
            .expect("export snapshot");

        // Prepare a poison predicate that errors at a specific row id = K
        let k = 50i64;
        let poison_pred = format!("CASE WHEN id = {k} THEN 1/(id - {k}) ELSE 1 END > 0");

        // Prepare channel and drain task
        let (tx, mut rx) = create_batch_channel(8);
        let drain_handle = tokio::spawn(async move {
            let mut total_rows: u64 = 0;
            while let Some(batch) = rx.recv().await {
                total_rows += batch.num_rows() as u64;
            }
            total_rows
        });

        // Fetch schema and spawn the single reader with the poison predicate
        let schema = coord
            .fetch_table_schema(None, Some(&fqtn), None)
            .await
            .expect("fetch schema");

        let reader_handle = coord
            .spawn_sharded_copy_reader(
                uri.clone(),
                snapshot_id.clone(),
                schema.clone(),
                poison_pred,
                tx,
                64,
            )
            .await
            .expect("spawn reader");

        // Expect the reader to fail due to division-by-zero during COPY
        let res = reader_handle.await.expect("join reader");
        assert!(res.is_err(), "reader should return Err on poison predicate");

        // Drain finishes because sender dropped when task exited with Err
        let _drained = drain_handle.await.expect("join drain");

        // Finalize coordinator snapshot as failure (rollback)
        coord
            .finalize_snapshot(false)
            .await
            .expect("finalize rollback");

        // DROP TABLE should succeed (no lingering locks)
        ddl.simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_ctid_sharding_tiny_table_many_shards() {
        let uri = get_database_uri();
        let (sql, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("ctid_tiny_{suffix}");
        let fqtn = format!("public.{table}");

        // Create tiny table with 3 rows
        sql.simple_query(&format!(
            "DROP TABLE IF EXISTS {fqtn};
             CREATE TABLE {fqtn} (id BIGINT PRIMARY KEY, name TEXT);
             INSERT INTO {fqtn} VALUES (1,'a'),(2,'b'),(3,'c');"
        ))
        .await
        .unwrap();
        sql.simple_query(&format!("ALTER TABLE {fqtn} REPLICA IDENTITY FULL;"))
            .await
            .unwrap();

        // Baseline count
        let baseline: i64 = sql
            .query_one(&format!("SELECT COUNT(*) FROM {fqtn};"), &[])
            .await
            .unwrap()
            .get(0);
        assert!(baseline > 0 && baseline <= 3);

        // Plan shards with shard_count=8
        let mut ps = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("psource new");
        let tn = TableName {
            schema: "public".to_string(),
            name: table.clone(),
        };
        let preds = ps.plan_ctid_shards(&tn, 8).await.expect("plan shards");
        assert!(
            !preds.is_empty(),
            "should produce at least one shard predicate"
        );

        // Coverage check: sum counts across shards equals baseline
        let mut sum_counts = 0i64;
        for pred in &preds {
            let cnt: i64 = sql
                .query_one(&format!("SELECT COUNT(*) FROM {fqtn} WHERE {pred};"), &[])
                .await
                .unwrap()
                .get(0);
            sum_counts += cnt;
        }
        assert_eq!(
            sum_counts, baseline,
            "shard coverage must equal baseline for tiny table"
        );

        // Disjointness: intersections are zero
        for i in 0..preds.len() {
            for j in (i + 1)..preds.len() {
                let inter: i64 = sql
                    .query_one(
                        &format!(
                            "SELECT COUNT(*) FROM {fqtn} WHERE ({p1}) AND ({p2});",
                            p1 = preds[i],
                            p2 = preds[j]
                        ),
                        &[],
                    )
                    .await
                    .unwrap()
                    .get(0);
                assert_eq!(inter, 0, "tiny-table shard predicates must be disjoint");
            }
        }

        // Now run actual readers under a consistent snapshot and ensure aggregate rows match baseline
        let mut coord = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("coord source");
        let (snapshot_id, _lsn) = coord
            .export_snapshot_and_lsn()
            .await
            .expect("export snapshot");

        // Prepare batch channel and drain
        let (tx, mut rx) = create_batch_channel(4);
        let drain_handle = tokio::spawn(async move {
            let mut total_rows: u64 = 0;
            while let Some(batch) = rx.recv().await {
                total_rows += batch.num_rows() as u64;
            }
            total_rows
        });

        let schema = coord
            .fetch_table_schema(None, Some(&fqtn), None)
            .await
            .expect("fetch schema");

        let handles = coord
            .spawn_sharded_copy_readers(
                uri.clone(),
                snapshot_id.clone(),
                schema.clone(),
                preds,
                tx,
                8,
            )
            .await
            .expect("spawn readers");

        let mut summed_rows: u64 = 0;
        for h in handles {
            let n = h.await.expect("join reader").expect("rows_copied");
            summed_rows += n;
        }
        let drained = drain_handle.await.expect("join drain");

        assert_eq!(
            summed_rows, drained,
            "sum of reader rows must equal drained rows"
        );
        assert_eq!(
            summed_rows as i64, baseline,
            "aggregate rows must equal baseline"
        );

        // Commit snapshot
        coord
            .finalize_snapshot(true)
            .await
            .expect("finalize commit");

        // Cleanup
        sql.simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_initial_copy_no_primary_key_replica_identity_full() {
        let uri = get_database_uri();
        let (ddl, _conn) = connect_to_postgres(&uri).await;

        // Unique table
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let table = format!("ic_no_pk_{suffix}");
        let fqtn = format!("public.{table}");

        // Create table WITHOUT primary key; seed rows; set REPLICA IDENTITY FULL
        let rows = 123i64;
        ddl.simple_query(&format!(
            "DROP TABLE IF EXISTS {fqtn};
             CREATE TABLE {fqtn} (id BIGINT, name TEXT);
             INSERT INTO {fqtn}
             SELECT gs, 'base'
             FROM generate_series(1, {rows}) AS gs;
             ALTER TABLE {fqtn} REPLICA IDENTITY FULL;"
        ))
        .await
        .unwrap();

        // Baseline count
        let baseline: i64 = ddl
            .query_one(&format!("SELECT COUNT(*) FROM {fqtn};"), &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(baseline, rows);

        // Fetch schema
        let ps = PostgresSource::new(&uri, None, None, false)
            .await
            .expect("ps new");
        let schema = ps
            .fetch_table_schema(None, Some(&fqtn), None)
            .await
            .expect("fetch schema");

        // Run initial copy (parallel readers)
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().to_str().unwrap();
        let ic_cfg = InitialCopyConfig {
            reader: InitialCopyReaderConfig {
                uri: uri.clone(),
                shard_count: 4,
            },
            writer: InitialCopyWriterConfig::default(),
        };

        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let progress = copy_table_stream(schema.clone(), &tx, base_path, ic_cfg)
            .await
            .expect("copy_table_stream");

        // Rows copied should match baseline
        assert_eq!(progress.rows_copied, baseline as u64);

        // Expect LoadFiles event with files
        if let Some(TableEvent::LoadFiles { files, .. }) = rx.recv().await {
            assert!(!files.is_empty(), "should produce at least one file");
            for f in files {
                assert!(std::path::Path::new(&f).exists());
            }
        } else {
            panic!("expected LoadFiles event");
        }

        // Cleanup
        ddl.simple_query(&format!("DROP TABLE IF EXISTS {fqtn};"))
            .await
            .unwrap();
    }
}
