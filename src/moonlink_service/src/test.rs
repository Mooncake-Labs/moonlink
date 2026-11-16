//! Service tests over the unix-socket RPC interface used by pg_mooncake and duckdb_mooncake.

use crate::test_utils::*;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn test_list_tables_on_empty_service() {
    let service = TestService::start().await;
    let mut stream = service.connect().await;
    let tables = moonlink_rpc::list_tables(&mut stream).await.unwrap();
    assert!(tables.is_empty(), "unexpected tables: {tables:?}");
}

/// Error case: create_snapshot with (database, table) that does not exist is reported as an RPC error, and the connection stays usable.
#[tokio::test]
#[serial]
async fn test_rpc_error_propagation_nonexistent_table() {
    let service = TestService::start().await;
    let mut stream = service.connect().await;

    let res = moonlink_rpc::create_snapshot(
        &mut stream,
        "nonexistent-db".to_string(),
        "nonexistent-table".to_string(),
        123,
    )
    .await;
    assert!(
        matches!(res, Err(moonlink_rpc::Error::Rpc(_))),
        "Expected Rpc error, but got: {res:?}"
    );

    let res = moonlink_rpc::scan_table_begin(
        &mut stream,
        "nonexistent-db".to_string(),
        "nonexistent-table".to_string(),
        0,
    )
    .await;
    assert!(
        matches!(res, Err(moonlink_rpc::Error::Rpc(_))),
        "Expected Rpc error, but got: {res:?}"
    );

    // The same connection keeps serving requests after errors.
    assert!(moonlink_rpc::list_tables(&mut stream)
        .await
        .unwrap()
        .is_empty());
}

/// Dead RPCs keep their wire slot but fail loudly instead of silently succeeding.
#[tokio::test]
#[serial]
async fn test_unsupported_rpcs() {
    let service = TestService::start().await;
    let mut stream = service.connect().await;

    let res = moonlink_rpc::load_files(
        &mut stream,
        "postgres".to_string(),
        "public.t".to_string(),
        vec!["/tmp/a.parquet".to_string()],
    )
    .await;
    match res {
        Err(moonlink_rpc::Error::Rpc(e)) => assert!(e.message.contains("load_files"), "{e:?}"),
        other => panic!("expected unsupported error, got {other:?}"),
    }

    let res = moonlink_rpc::get_parquet_metadatas(&mut stream, vec!["a.parquet".to_string()]).await;
    match res {
        Err(moonlink_rpc::Error::Rpc(e)) => {
            assert!(e.message.contains("get_parquet_metadatas"), "{e:?}")
        }
        other => panic!("expected unsupported error, got {other:?}"),
    }

    // The connection keeps serving requests.
    assert!(moonlink_rpc::list_tables(&mut stream)
        .await
        .unwrap()
        .is_empty());
}

/// A complete frame that does not decode (client/server version skew) gets an error reply, and
/// the connection keeps serving requests.
#[tokio::test]
#[serial]
async fn test_malformed_request_gets_error_and_connection_survives() {
    use tokio::io::AsyncWriteExt;
    let service = TestService::start().await;
    let mut stream = service.connect().await;
    // Variant index 99 does not exist.
    let payload = [99u8, 0, 0];
    stream
        .write_all(&(payload.len() as u32).to_ne_bytes())
        .await
        .unwrap();
    stream.write_all(&payload).await.unwrap();
    let res: moonlink_rpc::RpcResult<()> = moonlink_rpc::read(&mut stream).await.unwrap();
    assert!(res.is_err(), "expected an error reply, got {res:?}");
    assert!(moonlink_rpc::list_tables(&mut stream)
        .await
        .unwrap()
        .is_empty());
}

/// An absurd length prefix closes only that connection, without allocating the frame.
#[tokio::test]
#[serial]
async fn test_oversized_frame_closes_only_that_connection() {
    use tokio::io::AsyncWriteExt;
    let service = TestService::start().await;
    let mut stream = service.connect().await;
    stream.write_all(&u32::MAX.to_ne_bytes()).await.unwrap();
    let res: moonlink_rpc::Result<moonlink_rpc::RpcResult<()>> = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        moonlink_rpc::read(&mut stream),
    )
    .await
    .expect("server kept the connection open waiting for a 4 GiB frame");
    assert!(matches!(res, Err(moonlink_rpc::Error::Io(_))), "{res:?}");
    let mut other = service.connect().await;
    assert!(moonlink_rpc::list_tables(&mut other)
        .await
        .unwrap()
        .is_empty());
}

/// ScanTableEnd without a matching ScanTableBegin is a no-op, not a dead connection.
#[tokio::test]
#[serial]
async fn test_scan_end_without_begin_is_a_noop() {
    let service = TestService::start().await;
    let mut stream = service.connect().await;
    moonlink_rpc::scan_table_end(&mut stream, "postgres".to_string(), "public.t".to_string())
        .await
        .unwrap();
    assert!(moonlink_rpc::list_tables(&mut stream)
        .await
        .unwrap()
        .is_empty());
}

#[cfg(feature = "postgres-integration")]
mod postgres {
    use super::*;
    use arrow_ipc::reader::StreamReader;
    use moonlink::decode_serialized_read_state_for_testing;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    async fn create_mirror_table(
        stream: &mut tokio::net::UnixStream,
        table: &str,
        table_config: &str,
    ) -> moonlink_rpc::Result<()> {
        moonlink_rpc::create_table(
            stream,
            DATABASE.to_string(),
            table.to_string(),
            format!("public.{table}"),
            pg::database_uri(),
            table_config.to_string(),
        )
        .await
    }

    /// Count rows across all data files returned by a scan, asserting there are no deletions.
    fn count_rows_in_read_state(serialized_read_state: Vec<u8>) -> usize {
        let (data_files, puffin_files, deletion_vectors, positional_deletes) =
            decode_serialized_read_state_for_testing(serialized_read_state);
        assert!(puffin_files.is_empty());
        assert!(deletion_vectors.is_empty());
        assert!(positional_deletes.is_empty());
        data_files
            .iter()
            .map(|path| {
                let file = std::fs::File::open(path).unwrap();
                ParquetRecordBatchReaderBuilder::try_new(file)
                    .unwrap()
                    .build()
                    .unwrap()
                    .map(|batch| batch.unwrap().num_rows())
                    .sum::<usize>()
            })
            .sum()
    }

    async fn scan_row_count(stream: &mut tokio::net::UnixStream, table: &str, lsn: u64) -> usize {
        let read_state =
            moonlink_rpc::scan_table_begin(stream, DATABASE.to_string(), table.to_string(), lsn)
                .await
                .unwrap();
        let rows = count_rows_in_read_state(read_state);
        moonlink_rpc::scan_table_end(stream, DATABASE.to_string(), table.to_string())
            .await
            .unwrap();
        rows
    }

    #[tokio::test]
    #[serial]
    async fn test_create_table_from_postgres() {
        let pg_client = pg::connect().await;
        let table = "service_mirror_test";
        pg::create_source_table(&pg_client, table, 2).await;

        let service = TestService::start().await;
        let mut stream = service.connect().await;
        create_mirror_table(&mut stream, table, "{}").await.unwrap();

        let tables = moonlink_rpc::list_tables(&mut stream).await.unwrap();
        assert_eq!(tables.len(), 1, "{tables:?}");
        assert_eq!(tables[0].database, DATABASE);
        assert_eq!(tables[0].table, table);
        assert_eq!(tables[0].cardinality, 2);

        // duckdb_mooncake reads the arrow schema over RPC.
        let schema_bytes =
            moonlink_rpc::get_table_schema(&mut stream, DATABASE.to_string(), table.to_string())
                .await
                .unwrap();
        let schema = StreamReader::try_new(std::io::Cursor::new(schema_bytes), None)
            .unwrap()
            .schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["id", "name"]);

        moonlink_rpc::drop_table(&mut stream, DATABASE.to_string(), table.to_string())
            .await
            .unwrap();
        pg::drop_source_table(&pg_client, table).await;
    }

    /// Replication changes become visible to scans, and a snapshot at the latest LSN can be created.
    #[tokio::test]
    #[serial]
    async fn test_create_snapshot_and_scan() {
        let pg_client = pg::connect().await;
        let table = "service_snapshot_test";
        pg::create_source_table(&pg_client, table, 3).await;

        let service = TestService::start().await;
        let mut stream = service.connect().await;
        create_mirror_table(&mut stream, table, "{}").await.unwrap();

        pg_client
            .simple_query(&format!(
                "INSERT INTO {table} SELECT i, 'name-' || i FROM generate_series(4, 10) AS i;"
            ))
            .await
            .unwrap();
        let lsn = pg::current_wal_lsn(&pg_client).await;

        moonlink_rpc::create_snapshot(&mut stream, DATABASE.to_string(), table.to_string(), lsn)
            .await
            .unwrap();
        assert_eq!(scan_row_count(&mut stream, table, lsn).await, 10);

        let tables = moonlink_rpc::list_tables(&mut stream).await.unwrap();
        let status = tables.iter().find(|t| t.table == table).unwrap();
        assert!(status.flush_lsn.unwrap() >= lsn, "{status:?}");

        moonlink_rpc::drop_table(&mut stream, DATABASE.to_string(), table.to_string())
            .await
            .unwrap();
        pg::drop_source_table(&pg_client, table).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_optimize_table() {
        let pg_client = pg::connect().await;
        let table = "service_optimize_test";
        pg::create_source_table(&pg_client, table, 5).await;

        let service = TestService::start().await;
        let mut stream = service.connect().await;
        create_mirror_table(&mut stream, table, "{}").await.unwrap();
        let lsn = pg::current_wal_lsn(&pg_client).await;
        moonlink_rpc::create_snapshot(&mut stream, DATABASE.to_string(), table.to_string(), lsn)
            .await
            .unwrap();

        for mode in ["data", "index", "full"] {
            moonlink_rpc::optimize_table(
                &mut stream,
                DATABASE.to_string(),
                table.to_string(),
                mode.to_string(),
            )
            .await
            .unwrap_or_else(|e| panic!("optimize_table({mode}) failed: {e:?}"));
            assert_eq!(scan_row_count(&mut stream, table, lsn).await, 5);
        }

        let res = moonlink_rpc::optimize_table(
            &mut stream,
            DATABASE.to_string(),
            table.to_string(),
            "invalid-mode".to_string(),
        )
        .await;
        assert!(matches!(res, Err(moonlink_rpc::Error::Rpc(_))), "{res:?}");

        moonlink_rpc::drop_table(&mut stream, DATABASE.to_string(), table.to_string())
            .await
            .unwrap();
        pg::drop_source_table(&pg_client, table).await;
    }

    #[tokio::test]
    #[serial]
    async fn test_drop_and_recreate_tables() {
        let pg_client = pg::connect().await;
        let tables = ["service_drop_test_1", "service_drop_test_2"];
        for table in tables {
            pg::create_source_table(&pg_client, table, 1).await;
        }

        let service = TestService::start().await;
        let mut stream = service.connect().await;
        for table in tables {
            create_mirror_table(&mut stream, table, "{}").await.unwrap();
        }
        let mut listed: Vec<String> = moonlink_rpc::list_tables(&mut stream)
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.table)
            .collect();
        listed.sort();
        assert_eq!(listed, tables);

        moonlink_rpc::drop_table(&mut stream, DATABASE.to_string(), tables[0].to_string())
            .await
            .unwrap();
        let listed = moonlink_rpc::list_tables(&mut stream).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].table, tables[1]);

        // Dropping an unknown table is a no-op.
        moonlink_rpc::drop_table(&mut stream, DATABASE.to_string(), tables[0].to_string())
            .await
            .unwrap();

        create_mirror_table(&mut stream, tables[0], "{}")
            .await
            .unwrap();
        assert_eq!(
            moonlink_rpc::list_tables(&mut stream).await.unwrap().len(),
            2
        );

        for table in tables {
            moonlink_rpc::drop_table(&mut stream, DATABASE.to_string(), table.to_string())
                .await
                .unwrap();
            pg::drop_source_table(&pg_client, table).await;
        }
        assert!(moonlink_rpc::list_tables(&mut stream)
            .await
            .unwrap()
            .is_empty());
    }

    /// Error case: an invalid table_config (as passed through mooncake.create_table) is rejected and the service keeps serving.
    #[tokio::test]
    #[serial]
    async fn test_create_table_with_invalid_config() {
        let pg_client = pg::connect().await;
        let table = "service_invalid_config_test";
        pg::create_source_table(&pg_client, table, 1).await;

        let service = TestService::start().await;
        let mut stream = service.connect().await;
        for table_config in ["not json", r#"{"mooncake": {"append_only": "yes"}}"#] {
            let res = create_mirror_table(&mut stream, table, table_config).await;
            assert!(
                matches!(res, Err(moonlink_rpc::Error::Rpc(_))),
                "config {table_config:?} should be rejected, got {res:?}"
            );
        }
        assert!(moonlink_rpc::list_tables(&mut stream)
            .await
            .unwrap()
            .is_empty());
        pg::drop_source_table(&pg_client, table).await;
    }

    /// Files of a scan's payload, decoded the way duckdb_mooncake reads them.
    fn payload_files(serialized_read_state: &[u8]) -> Vec<String> {
        let (data_files, puffin_files, _, _) =
            decode_serialized_read_state_for_testing(serialized_read_state.to_vec());
        data_files.into_iter().chain(puffin_files).collect()
    }

    /// Two overlapping scans of one table on one connection (two transactions of a catalog
    /// connection) across a snapshot change: neither the second Begin nor the first End releases
    /// any of their files (the server cannot tell which scan an End is for), and the last End
    /// releases them.
    #[tokio::test]
    #[serial]
    async fn test_overlapping_scans_of_one_table_keep_their_files() {
        let pg_client = pg::connect().await;
        let table = "service_overlapping_scans_test";
        pg::create_source_table(&pg_client, table, 3).await;

        let service = TestService::start().await;
        let mut stream = service.connect().await;
        create_mirror_table(&mut stream, table, "{}").await.unwrap();

        // Unflushed rows, so each scan gets its own in-memory temp parquet.
        let insert = |ids: &str| {
            format!("INSERT INTO {table} SELECT i, 'n' FROM generate_series({ids}) AS i;")
        };
        pg_client.simple_query(&insert("4, 5")).await.unwrap();
        let lsn = pg::current_wal_lsn(&pg_client).await;
        let first = moonlink_rpc::scan_table_begin(
            &mut stream,
            DATABASE.to_string(),
            table.to_string(),
            lsn,
        )
        .await
        .unwrap();
        assert_eq!(count_rows_in_read_state(first.clone()), 5);
        let first_files = payload_files(&first);

        pg_client.simple_query(&insert("6, 7")).await.unwrap();
        let lsn = pg::current_wal_lsn(&pg_client).await;
        let second = moonlink_rpc::scan_table_begin(
            &mut stream,
            DATABASE.to_string(),
            table.to_string(),
            lsn,
        )
        .await
        .unwrap();
        assert_eq!(count_rows_in_read_state(second.clone()), 7);
        let only_first: Vec<String> = first_files
            .iter()
            .filter(|f| !payload_files(&second).contains(f))
            .cloned()
            .collect();
        assert!(
            !only_first.is_empty(),
            "precondition: the first scan has files of its own"
        );
        let all_exist = |files: &[String]| files.iter().all(|f| std::path::Path::new(f).exists());

        // Releases happen in spawned tasks; give them time to run before checking.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert!(
            all_exist(&first_files),
            "a second ScanTableBegin released the first scan's files"
        );
        moonlink_rpc::scan_table_end(&mut stream, DATABASE.to_string(), table.to_string())
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert!(
            all_exist(&first_files) && all_exist(&payload_files(&second)),
            "the first ScanTableEnd released files of a scan that may still be open"
        );
        moonlink_rpc::scan_table_end(&mut stream, DATABASE.to_string(), table.to_string())
            .await
            .unwrap();
        assert_eq!(scan_row_count(&mut stream, table, lsn).await, 7);

        // With both scans ended and the cache on the newer snapshot, the first scan's own files go.
        let mut released = false;
        for _ in 0..100 {
            if only_first.iter().all(|f| !std::path::Path::new(f).exists()) {
                released = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            released,
            "files of ended scans were never released: {only_first:?}"
        );

        moonlink_rpc::drop_table(&mut stream, DATABASE.to_string(), table.to_string())
            .await
            .unwrap();
        pg::drop_source_table(&pg_client, table).await;
    }

    /// Surviving `id`s of a scan payload, applying deletes the way duckdb_mooncake does:
    /// deletion-vector blobs are portable roaring bitmaps at `offset + 20` with `size - 24` bytes,
    /// positional deletes are (data file, row) pairs.
    fn surviving_ids(serialized_read_state: Vec<u8>) -> (Vec<i64>, usize, usize) {
        use arrow_array::Int64Array;
        use std::io::{Read, Seek, SeekFrom};

        let (data_files, puffin_files, deletion_vectors, positional_deletes) =
            decode_serialized_read_state_for_testing(serialized_read_state);
        let mut deleted: Vec<roaring::RoaringBitmap> =
            vec![roaring::RoaringBitmap::new(); data_files.len()];
        for dv in &deletion_vectors {
            let mut file =
                std::fs::File::open(&puffin_files[dv.puffin_file_number as usize]).unwrap();
            file.seek(SeekFrom::Start(dv.offset as u64 + 20)).unwrap();
            let mut blob = vec![0u8; dv.size as usize - 24];
            file.read_exact(&mut blob).unwrap();
            deleted[dv.data_file_number as usize] |=
                roaring::RoaringBitmap::deserialize_from(&blob[..]).unwrap();
        }
        for pd in &positional_deletes {
            deleted[pd.data_file_number as usize].insert(pd.data_file_row_number);
        }
        let mut ids = vec![];
        for (file_number, path) in data_files.iter().enumerate() {
            let file = std::fs::File::open(path).unwrap();
            let mut row = 0u32;
            for batch in ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .build()
                .unwrap()
            {
                let batch = batch.unwrap();
                let column = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..column.len() {
                    if !deleted[file_number].contains(row) {
                        ids.push(column.value(i));
                    }
                    row += 1;
                }
            }
        }
        ids.sort_unstable();
        (ids, deletion_vectors.len(), positional_deletes.len())
    }

    async fn scan_surviving_ids(
        stream: &mut tokio::net::UnixStream,
        table: &str,
        lsn: u64,
    ) -> (Vec<i64>, usize, usize) {
        let read_state =
            moonlink_rpc::scan_table_begin(stream, DATABASE.to_string(), table.to_string(), lsn)
                .await
                .unwrap();
        let result = surviving_ids(read_state);
        moonlink_rpc::scan_table_end(stream, DATABASE.to_string(), table.to_string())
            .await
            .unwrap();
        result
    }

    /// Persisted deletes reach the client as deletion vectors in puffin files, and deletes after
    /// the last snapshot as positional deletes.
    #[tokio::test]
    #[serial]
    async fn test_scan_payload_carries_deletes() {
        let pg_client = pg::connect().await;
        let table = "service_scan_deletes_test";
        pg::create_source_table(&pg_client, table, 10).await;

        let service = TestService::start().await;
        let mut stream = service.connect().await;
        create_mirror_table(&mut stream, table, "{}").await.unwrap();

        pg_client
            .simple_query(&format!(
                "DELETE FROM {table} WHERE id IN (1, 2, 3); UPDATE {table} SET name = 'x' WHERE id = 4;"
            ))
            .await
            .unwrap();
        let lsn = pg::current_wal_lsn(&pg_client).await;
        moonlink_rpc::create_snapshot(&mut stream, DATABASE.to_string(), table.to_string(), lsn)
            .await
            .unwrap();
        // The iceberg result (deletion-vector puffins) is imported by the next mooncake snapshot;
        // until then the same deletes are served as positional deletes. Both must be correct.
        let mut saw_deletion_vectors = false;
        for _ in 0..100 {
            let (ids, deletion_vectors, _) = scan_surviving_ids(&mut stream, table, lsn).await;
            assert_eq!(ids, vec![4, 5, 6, 7, 8, 9, 10]);
            if deletion_vectors > 0 {
                saw_deletion_vectors = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(
            saw_deletion_vectors,
            "persisted deletes never arrived as deletion vectors"
        );

        pg_client
            .simple_query(&format!("DELETE FROM {table} WHERE id IN (5, 6);"))
            .await
            .unwrap();
        let lsn = pg::current_wal_lsn(&pg_client).await;
        let (ids, _, positional_deletes) = scan_surviving_ids(&mut stream, table, lsn).await;
        assert_eq!(ids, vec![4, 7, 8, 9, 10]);
        assert!(
            positional_deletes > 0,
            "unpersisted deletes must arrive as positional deletes"
        );

        moonlink_rpc::drop_table(&mut stream, DATABASE.to_string(), table.to_string())
            .await
            .unwrap();
        pg::drop_source_table(&pg_client, table).await;
    }

    /// A moonlink service in its own runtime. Dropping it shuts the runtime down, which drops
    /// every task of the service, like a crashed background worker.
    struct IsolatedService {
        runtime: Option<tokio::runtime::Runtime>,
        socket_path: std::path::PathBuf,
    }

    impl IsolatedService {
        async fn start(base_path: &std::path::Path) -> Self {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let config = crate::ServiceConfig {
                base_path: base_path.to_str().unwrap().to_string(),
            };
            runtime.spawn(async move {
                crate::start_with_config(config).await.unwrap();
            });
            let socket_path = base_path.join("moonlink.sock");
            for _ in 0..1200 {
                if let Ok(mut stream) = tokio::net::UnixStream::connect(&socket_path).await {
                    if moonlink_rpc::list_tables(&mut stream).await.is_ok() {
                        return Self {
                            runtime: Some(runtime),
                            socket_path,
                        };
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            panic!("moonlink service did not start on {socket_path:?}");
        }

        async fn connect(&self) -> tokio::net::UnixStream {
            tokio::net::UnixStream::connect(&self.socket_path)
                .await
                .unwrap()
        }
    }

    impl Drop for IsolatedService {
        fn drop(&mut self) {
            if let Some(runtime) = self.runtime.take() {
                runtime.shutdown_background();
            }
        }
    }

    /// After the service restarts on the same base path, the mirror is recovered from its
    /// persisted state and keeps replicating.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_restart_recovers_mirror() {
        let pg_client = pg::connect().await;
        let table = "service_restart_test";
        pg::create_source_table(&pg_client, table, 5).await;
        let base_dir = tempfile::tempdir().unwrap();

        {
            let service = IsolatedService::start(base_dir.path()).await;
            let mut stream = service.connect().await;
            create_mirror_table(&mut stream, table, "{}").await.unwrap();
            pg_client
                .simple_query(&format!("DELETE FROM {table} WHERE id = 1;"))
                .await
                .unwrap();
            let lsn = pg::current_wal_lsn(&pg_client).await;
            moonlink_rpc::create_snapshot(
                &mut stream,
                DATABASE.to_string(),
                table.to_string(),
                lsn,
            )
            .await
            .unwrap();
        }
        // Writes while the service is down are replayed from the replication slot.
        pg_client
            .simple_query(&format!(
                "INSERT INTO {table} VALUES (6, 'six'); DELETE FROM {table} WHERE id = 2;"
            ))
            .await
            .unwrap();

        let service = IsolatedService::start(base_dir.path()).await;
        let mut stream = service.connect().await;
        let tables = moonlink_rpc::list_tables(&mut stream).await.unwrap();
        assert_eq!(tables.len(), 1, "{tables:?}");
        let lsn = pg::current_wal_lsn(&pg_client).await;
        let (ids, _, _) = scan_surviving_ids(&mut stream, table, lsn).await;
        assert_eq!(ids, vec![3, 4, 5, 6]);

        moonlink_rpc::drop_table(&mut stream, DATABASE.to_string(), table.to_string())
            .await
            .unwrap();
        pg::drop_source_table(&pg_client, table).await;
    }
}
