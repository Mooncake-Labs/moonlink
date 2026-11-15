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
}
