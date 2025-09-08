mod common;

#[cfg(test)]
mod tests {
    use super::common::{
        assert_scan_ids_eq, crash_and_recover_backend_with_guard, create_backend_from_base_path,
        current_wal_lsn, get_database_uri, get_serialized_table_config, TestGuard, TestGuardMode,
        DATABASE, TABLE,
    };
    use moonlink_backend::RowEventOperation;
    use moonlink_backend::REST_API_URI;
    use moonlink_backend::{EventRequest, IngestRequestPayload, MoonlinkBackend, RowEventRequest};
    use moonlink_metadata_store::{base_metadata_store::MetadataStoreTrait, SqliteMetadataStore};

    use arrow::datatypes::Schema as ArrowSchema;
    use arrow_schema::{DataType, Field};
    use serde_json::json;
    use serial_test::serial;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    /// Testing scenario: perform table creation and drop operations, and check metadata store table states.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_metadata_store() {
        let (guard, _) = TestGuard::new(Some("metadata_store"), true).await;
        // Till now, table [`metadata_store`] has been created at both row storage and column storage database.
        let backend = guard.backend();
        let database_directory = guard.tmp().as_ref().unwrap().path().to_str().unwrap();
        let metadata_store = SqliteMetadataStore::new_with_directory(database_directory)
            .await
            .unwrap();

        // Check metadata storage after table creation.
        let metadata_entries = metadata_store
            .get_all_table_metadata_entries()
            .await
            .unwrap();
        assert_eq!(metadata_entries.len(), 1);
        let table = &metadata_entries[0].table;
        assert_eq!(table, TABLE);
        assert_eq!(
            metadata_entries[0]
                .moonlink_table_config
                .iceberg_table_config
                .namespace,
            vec![format!("{DATABASE}")],
        );
        assert_eq!(
            metadata_entries[0]
                .moonlink_table_config
                .iceberg_table_config
                .table_name,
            format!("{TABLE}")
        );

        // Drop table and check metadata storage.
        backend
            .drop_table(DATABASE.to_string(), TABLE.to_string())
            .await
            .unwrap();
        let metadata_entries = metadata_store
            .get_all_table_metadata_entries()
            .await
            .unwrap();
        assert!(metadata_entries.is_empty());
    }

    /// Test recovery.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery() {
        let uri = get_database_uri();
        let (mut guard, client) = TestGuard::new(Some("recovery"), true).await;
        guard.set_test_mode(TestGuardMode::Crash);
        let backend = guard.backend();

        // Drop the table that setup_backend created so we can test the full cycle
        backend
            .drop_table(DATABASE.to_string(), TABLE.to_string())
            .await
            .unwrap();

        // First cycle: add table, insert data, verify it works
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

        client
            .simple_query("INSERT INTO recovery VALUES (1,'first');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        // Wait until changes reflected to mooncake snapshot, and force create iceberg snapshot to test mooncake/iceberg table recovery.
        backend
            .scan_table(DATABASE.to_string(), TABLE.to_string(), Some(lsn))
            .await
            .unwrap();
        backend
            .create_snapshot(DATABASE.to_string(), TABLE.to_string(), lsn)
            .await
            .unwrap();

        let (backend, _testing_directory_before_recovery) =
            crash_and_recover_backend_with_guard(guard).await;
        assert_scan_ids_eq(&backend, DATABASE.to_string(), TABLE.to_string(), lsn, [1]).await;

        // Insert new rows to make sure recovered mooncake table works as usual.
        client
            .simple_query("INSERT INTO recovery VALUES (2,'second');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        // Wait until changes reflected to mooncake snapshot, and force create iceberg snapshot to test mooncake/iceberg table recovery.
        assert_scan_ids_eq(
            &backend,
            DATABASE.to_string(),
            TABLE.to_string(),
            lsn,
            [1, 2],
        )
        .await;
    }

    /// Test recovery for rest ingested table.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery_for_rest_table() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_store_accessor =
            SqliteMetadataStore::new_with_directory(temp_dir.path().to_str().unwrap())
                .await
                .unwrap();
        let mut backend = MoonlinkBackend::new(
            temp_dir.path().to_str().unwrap().into(),
            /*data_server_uri=*/ None,
            Box::new(metadata_store_accessor),
        )
        .await
        .unwrap();
        backend.initialize_event_api().await.unwrap();

        // Create a rest table.
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "0".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "1".to_string(),
            )])),
            Field::new("age", DataType::Int32, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "2".to_string(),
            )])),
        ]);
        backend
            .create_table(
                DATABASE.to_string(),
                TABLE.to_string(),
                "public.recovery_for_rest_table".to_string(),
                REST_API_URI.to_string(),
                get_serialized_table_config(&temp_dir),
                Some(arrow_schema),
            )
            .await
            .unwrap();

        // Ingest data into table.
        let (tx, mut rx) = mpsc::channel(1);
        let row_event_request = RowEventRequest {
            src_table_name: "public.recovery_for_rest_table".to_string(),
            operation: RowEventOperation::Insert,
            payload: IngestRequestPayload::Json(json!({
                "id": 1,
                "name": "Alice Johnson",
                "age": 30
            })),
            timestamp: SystemTime::now(),
            tx: Some(tx),
        };
        let rest_event_request = EventRequest::RowRequest(row_event_request);
        backend
            .send_event_request(rest_event_request)
            .await
            .unwrap();

        // Force snapshot to make sure all writes are persisted into iceberg so they could be recovered.
        let lsn = rx.recv().await.unwrap();
        backend
            .create_snapshot(DATABASE.to_string(), TABLE.to_string(), lsn)
            .await
            .unwrap();

        // Crash backend recovery and recreate backend.
        backend
            .shutdown_connection(REST_API_URI, /*postgres_drop_al*/ true)
            .await;
        backend =
            create_backend_from_base_path(temp_dir.path().to_str().unwrap().to_string()).await;
        assert_scan_ids_eq(&backend, DATABASE.to_string(), TABLE.to_string(), lsn, [1]).await;
    }

    /// Test scenario: perform a few requests on non-existent databases and tables, make sure error is correctly propagated.

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_on_non_existent_table() {
        const NON_EXISTENT_TABLE: &str = "non-existent-table";

        let (mut guard, client) = TestGuard::new(Some("non_existent_table"), true).await;
        guard.set_test_mode(TestGuardMode::Crash);

        let lsn = current_wal_lsn(&client).await;
        let non_existent_schema: &str = "non-existent-schema";

        // Scan table on non-existent database.
        let backend = guard.backend();
        let res = backend
            .scan_table(
                non_existent_schema.to_string(),
                NON_EXISTENT_TABLE.to_string(),
                Some(lsn),
            )
            .await;
        assert!(res.is_err());

        // Scan table on non-existent table.
        let res = backend
            .scan_table(
                DATABASE.to_string(),
                NON_EXISTENT_TABLE.to_string(),
                Some(lsn),
            )
            .await;
        assert!(res.is_err());

        // Read schema on non-existent database.
        let res = backend
            .get_table_schema(
                non_existent_schema.to_string(),
                NON_EXISTENT_TABLE.to_string(),
            )
            .await;
        assert!(res.is_err());

        // Read schema on non-existent table.
        let res = backend
            .get_table_schema(DATABASE.to_string(), NON_EXISTENT_TABLE.to_string())
            .await;
        assert!(res.is_err());
    }
}
