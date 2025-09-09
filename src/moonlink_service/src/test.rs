use moonlink::row::{moonlink_row_to_proto, MoonlinkRow, RowValue};
use more_asserts as ma;
use serde_json::json;
use serial_test::serial;
use tokio::net::TcpStream;

use crate::rest_api::{CreateTableResponse, FileUploadResponse, HealthResponse, IngestResponse};
use crate::start_with_config;
use crate::test_guard::TestGuard;
use crate::test_utils::*;
use moonlink::decode_serialized_read_state_for_testing;
use moonlink_rpc::{load_files, scan_table_begin, scan_table_end};

#[tokio::test]
#[serial]
async fn test_health_check_endpoint() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{REST_ADDR}/health"))
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();

    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    let response: HealthResponse = response.json().await.unwrap();
    assert_eq!(response.service, "moonlink-rest-api");
    assert_eq!(response.status, "healthy");
    ma::assert_gt!(response.timestamp, 0);
}

#[tokio::test]
#[serial]
async fn test_schema() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create test table.
    let client = reqwest::Client::new();
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let payload = json!({
        "database": DATABASE,
        "table": TABLE,
        "schema": [
            {"name": "int32", "data_type": "int32", "nullable": false},
            {"name": "int64", "data_type": "int64", "nullable": false},
            {"name": "string", "data_type": "string", "nullable": false},
            {"name": "text", "data_type": "text", "nullable": false},
            {"name": "bool", "data_type": "bool", "nullable": false},
            {"name": "boolean", "data_type": "boolean", "nullable": false},
            {"name": "float32", "data_type": "float32", "nullable": false},
            {"name": "float64", "data_type": "float64", "nullable": false},
            {"name": "date32", "data_type": "date32", "nullable": false},
            {"name": "decimal(10)", "data_type": "decimal(10,2)", "nullable": false}, // only precision value
            {"name": "decimal(10,2)", "data_type": "decimal(10,2)", "nullable": false}, // lowercase
            {"name": "Decimal(10,2)", "data_type": "decimal(10,2)", "nullable": false}, // uppercase
            {"name": "Decimal(10,0)", "data_type": "decimal(10,0)", "nullable": false}, // scale = 0
            {"name": "Decimal(2,-3)", "data_type": "decimal(2,-3)", "nullable": false}, // negative scale value
            {"name": "props", "data_type": "struct", "nullable": true, "fields": [
                {"name": "score", "data_type": "float64", "nullable": true},
                {"name": "labels", "data_type": "list", "nullable": true, "item": {"name": "label", "data_type": "string", "nullable": true}},
                {"name": "coords", "data_type": "struct", "nullable": true, "fields": [
                    {"name": "x", "data_type": "int32", "nullable": true},
                    {"name": "y", "data_type": "int32", "nullable": true}
                ]}
            ]},
            {"name": "history", "data_type": "list", "nullable": true, "item": {"name": "ts", "data_type": "int64", "nullable": true}}
        ],
        "table_config": {
            "mooncake": {
                "append_only": true
            }
        }
    });
    let response = client
        .post(format!("{REST_ADDR}/tables/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    let response: CreateTableResponse = response.json().await.unwrap();
    assert_eq!(response.database, DATABASE);
    assert_eq!(response.table, crafted_src_table_name);
    assert_eq!(response.lsn, 1);
}

/// Util function to test optimize table
async fn run_optimize_table_test(mode: &str) {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Ingest some data.
    let insert_payload = json!({
        "operation": "insert",
        "request_mode": "async",
        "data": {
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 30
        }
    });
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response = client
        .post(format!("{REST_ADDR}/ingest/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&insert_payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );

    // test for optimize table on mode 'full'
    optimize_table(&client, DATABASE, TABLE, mode).await;

    // Scan table and get data file and puffin files back.
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        /*lsn=*/ 1,
    )
    .await
    .unwrap();
    let (data_file_paths, puffin_file_paths, puffin_deletion, positional_deletion) =
        decode_serialized_read_state_for_testing(bytes);
    assert_eq!(data_file_paths.len(), 1);
    let record_batches = read_all_batches(&data_file_paths[0]).await;
    let expected_arrow_batch = create_test_arrow_batch();
    assert_eq!(record_batches, vec![expected_arrow_batch]);

    assert!(puffin_file_paths.is_empty());
    assert!(puffin_deletion.is_empty());
    assert!(positional_deletion.is_empty());

    scan_table_end(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
    )
    .await
    .unwrap();
}

#[tokio::test]
#[serial]
async fn test_optimize_table_on_full_mode() {
    run_optimize_table_test("full").await;
}

#[tokio::test]
#[serial]
async fn test_optimize_table_on_index_mode() {
    run_optimize_table_test("index").await;
}

#[tokio::test]
#[serial]
async fn test_optimize_table_on_data_mode() {
    run_optimize_table_test("data").await;
}

/// Test Create Snapshot
#[tokio::test]
#[serial]
async fn test_create_snapshot() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Ingest some data.
    let insert_payload = create_test_json_payload();
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response: IngestResponse =
        send_test_ingest(&client, &crafted_src_table_name, &insert_payload).await;
    let lsn = response.lsn.unwrap();

    // After all changes reflected at mooncake snapshot, flush table and trigger an iceberg snapshot.
    flush_table(&client, DATABASE, TABLE, lsn).await;
    create_snapshot(&client, DATABASE, TABLE, lsn).await;

    assert_data_and_puffin(TABLE, lsn).await;
}

/// Test basic table creation, insertion and query.
#[tokio::test]
#[serial]
async fn test_moonlink_standalone_data_ingestion() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create test table (nested schema).
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ true).await;

    // Ingest nested data.
    let insert_payload = create_test_arrow_insert_payload_nested();
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    send_test_ingest(&client, &crafted_src_table_name, &insert_payload).await;

    // Scan table and get data file and puffin files back.
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        /*lsn=*/ 1,
    )
    .await
    .unwrap();
    let (data_file_paths, puffin_file_paths, puffin_deletion, positional_deletion) =
        decode_serialized_read_state_for_testing(bytes);
    assert_eq!(data_file_paths.len(), 1);
    let record_batches = read_all_batches(&data_file_paths[0]).await;
    let expected_arrow_batch = create_test_arrow_batch_nested();
    assert_eq!(record_batches, vec![expected_arrow_batch]);

    assert!(puffin_file_paths.is_empty());
    assert!(puffin_deletion.is_empty());
    assert!(positional_deletion.is_empty());

    scan_table_end(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
    )
    .await
    .unwrap();
}

/// Test basic table creation, file upload and query.
#[tokio::test]
#[serial]
async fn test_moonlink_standalone_file_upload() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Upload a file.
    let file_upload_payload = create_test_load_parquet_payload(&get_moonlink_backend_dir()).await;
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response: FileUploadResponse =
        send_test_upload(&client, &crafted_src_table_name, &file_upload_payload).await;

    let lsn = response.lsn.unwrap();

    // Scan table and get data file and puffin files back.
    assert_data_and_puffin(TABLE, lsn).await;
}

#[tokio::test]
#[serial]
async fn test_moonlink_standalone_protobuf_ingestion() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Build protobuf payload for a single row.
    let row = MoonlinkRow::new(vec![
        RowValue::Int32(1),
        RowValue::ByteArray("Alice Johnson".as_bytes().to_vec()),
        RowValue::ByteArray("alice@example.com".as_bytes().to_vec()),
        RowValue::Int32(30),
    ]);
    let proto_row = moonlink_row_to_proto(row);
    let mut buf = Vec::new();
    prost::Message::encode(&proto_row, &mut buf).unwrap();

    let insert_payload = json!({
        "operation": "insert",
        "request_mode": "sync",
        "data": buf
    });
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response = client
        .post(format!("{REST_ADDR}/ingestpb/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&insert_payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    let response: IngestResponse = response.json().await.unwrap();
    let lsn = response.lsn.unwrap();

    // Scan table and get data file and puffin files back.
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        lsn,
    )
    .await
    .unwrap();
    let (data_file_paths, puffin_file_paths, puffin_deletion, positional_deletion) =
        decode_serialized_read_state_for_testing(bytes);
    assert_eq!(data_file_paths.len(), 1);
    let record_batches = read_all_batches(&data_file_paths[0]).await;
    let expected_arrow_batch = create_test_arrow_batch();
    assert_eq!(record_batches, vec![expected_arrow_batch]);

    assert!(puffin_file_paths.is_empty());
    assert!(puffin_deletion.is_empty());
    assert!(positional_deletion.is_empty());

    scan_table_end(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
    )
    .await
    .unwrap();
}

/// Test basic table creation, file insert and query.
#[tokio::test]
#[serial]
async fn test_moonlink_standalone_file_insert() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Upload a file.
    let file_upload_payload = create_test_insert_parquet_payload(&get_moonlink_backend_dir()).await;
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response: FileUploadResponse =
        send_test_upload(&client, &crafted_src_table_name, &file_upload_payload).await;
    let lsn = response.lsn.unwrap();

    // Scan table and get data file and puffin files back.
    assert_data_and_puffin(TABLE, lsn).await;
}

/// Testing scenario: payload insert and query on multiple tables, under same database.
#[tokio::test]
#[serial]
async fn test_moonlink_standalone_multiple_tables_data_ingestion() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create test tables.
    let table_a = "test-table-A";
    let table_b = "test-table-B";
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, table_a, /*nested=*/ false).await;
    create_table(&client, DATABASE, table_b, /*nested=*/ false).await;

    // Create test payload.
    let insert_payload = create_test_json_payload();

    // Ingest some data into table A.
    let crafted_src_table_a_name = format!("{DATABASE}.{table_a}");
    send_test_ingest(&client, &crafted_src_table_a_name, &insert_payload).await;

    // Ingest some data into table B.
    let crafted_src_table_b_name = format!("{DATABASE}.{table_b}");
    send_test_ingest(&client, &crafted_src_table_b_name, &insert_payload).await;

    // Scan table A and get data file and puffin files back.
    assert_data_and_puffin(table_a, 1).await;

    // Scan table B and get data file and puffin files back.
    assert_data_and_puffin(table_b, 1).await;
}

/// Testing scenario: file upload and query on multiple tables, under same database.
#[tokio::test]
#[serial]
async fn test_moonlink_standalone_multiple_tables_file_upload() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create test tables.
    let table_a = "test-table-A";
    let table_b = "test-table-B";
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, table_a, /*nested=*/ false).await;
    create_table(&client, DATABASE, table_b, /*nested=*/ false).await;

    // Create test parquest payload.
    let file_upload_payload = create_test_load_parquet_payload(&get_moonlink_backend_dir()).await;

    // Upload a file into table A.
    let crafted_src_table_a_name = format!("{DATABASE}.{table_a}");
    let response: FileUploadResponse =
        send_test_upload(&client, &crafted_src_table_a_name, &file_upload_payload).await;
    let lsn_a = response.lsn.unwrap();

    // Upload a file into table B.
    let crafted_src_table_b_name = format!("{DATABASE}.{table_b}");
    let response: FileUploadResponse =
        send_test_upload(&client, &crafted_src_table_b_name, &file_upload_payload).await;
    let lsn_b = response.lsn.unwrap();

    // Scan table A and get data file and puffin files back.
    assert_data_and_puffin(table_a, lsn_a).await;

    // Scan table B and get data file and puffin files back.
    assert_data_and_puffin(table_b, lsn_b).await;
}

/// Testing scenario: file insert and query on multiple tables, under same database.
#[tokio::test]
#[serial]
async fn test_moonlink_standalone_multiple_tables_file_insert() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create test tables.
    let table_a = "test-table-A";
    let table_b = "test-table-B";
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, table_a, /*nested=*/ false).await;
    create_table(&client, DATABASE, table_b, /*nested=*/ false).await;

    let file_upload_payload = create_test_insert_parquet_payload(&get_moonlink_backend_dir()).await;

    // Insert file into table A.
    let crafted_src_table_a_name = format!("{DATABASE}.{table_a}");
    let response: FileUploadResponse =
        send_test_upload(&client, &crafted_src_table_a_name, &file_upload_payload).await;
    let lsn_a = response.lsn.unwrap();

    // Insert file into table B.
    let crafted_src_table_b_name = format!("{DATABASE}.{table_b}");
    let response: FileUploadResponse =
        send_test_upload(&client, &crafted_src_table_b_name, &file_upload_payload).await;
    let lsn_b = response.lsn.unwrap();

    // Scan table A and get data file and puffin files back.
    assert_data_and_puffin(table_a, lsn_a).await;

    // Scan table B and get data file and puffin files back.
    assert_data_and_puffin(table_b, lsn_b).await;
}

/// Testing scenario: two tables with the same name, but under different databases are created.
#[tokio::test]
#[serial]
async fn test_multiple_tables_creation() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create the first test table.
    let client: reqwest::Client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Create the second test table.
    create_table(&client, "second-database", TABLE, /*nested=*/ false).await;
}

#[tokio::test]
#[serial]
async fn test_drop_and_recreate_table() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // List table before drop.
    let list_results = list_tables(&client).await;
    assert_eq!(list_results.len(), 1);

    // Drop test table.
    drop_table(&client, DATABASE, TABLE).await;

    // List table after drop.
    let list_results = list_tables(&client).await;
    assert_eq!(list_results.len(), 0);

    // Recreate the same table.
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // List table after recreation.
    let list_results = list_tables(&client).await;
    assert_eq!(list_results.len(), 1);
}

/// Testing scenario: create multiple tables, and check list table result.
#[tokio::test]
#[serial]
async fn test_list_tables() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create two test tables.
    let client: reqwest::Client = reqwest::Client::new();
    create_table(&client, "database-1", "table-1", /*nested=*/ false).await;
    create_table(&client, "database-2", "table-2", /*nested=*/ false).await;

    // List test tables.
    let mut list_results = list_tables(&client).await;
    assert_eq!(list_results.len(), 2);
    list_results.sort_by(|a, b| {
        (
            &a.database,
            &a.table,
            a.commit_lsn,
            a.flush_lsn,
            &a.iceberg_warehouse_location,
        )
            .cmp(&(
                &b.database,
                &b.table,
                b.commit_lsn,
                b.flush_lsn,
                &b.iceberg_warehouse_location,
            ))
    });

    // Validate list results.
    assert_eq!(list_results[0].database, "database-1");
    assert_eq!(list_results[0].table, "table-1");

    assert_eq!(list_results[1].database, "database-2");
    assert_eq!(list_results[1].table, "table-2");
}

/// Dummy testing for bulk ingest files into mooncake table.
#[tokio::test]
#[serial]
async fn test_bulk_ingest_files() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    let client: reqwest::Client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // A dummy stub-level interface testing.
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    load_files(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        /*files=*/ vec![],
    )
    .await
    .unwrap();
}

/// ==========================
/// Failure tests
/// ==========================
///
/// Error case: invalid operation name.
#[tokio::test]
#[serial]
async fn test_invalid_operation() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;

    // Test invalid operation to upload a file.
    let file_upload_payload = create_test_invalid_upload_operation(&get_moonlink_backend_dir());
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response =
        send_test_invalid_upload(&client, &crafted_src_table_name, &file_upload_payload).await;
    assert!(!response.status().is_success());

    // Test invalid operation to ingest data.
    let insert_payload = create_test_invalid_ingest_operation();
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response =
        send_test_invalid_ingest(&client, &crafted_src_table_name, &insert_payload).await;
    assert!(!response.status().is_success());
}

/// Error case: non-existent source table.
#[tokio::test]
#[serial]
async fn test_non_existent_table() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Create the test table.
    let client: reqwest::Client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE, /*nested=*/ false).await;
    let wrong_table = "non_existent_source_table";

    // Test invalid operation to upload a file.
    let file_upload_payload = create_test_invalid_parquet_file_upload(&get_moonlink_backend_dir());
    send_test_invalid_upload(&client, wrong_table, &file_upload_payload).await;
    // Make sure service doesn't crash.

    // Test invalid operation to ingest data.
    let insert_payload = create_test_invalid_ingest_operation();
    send_test_invalid_ingest(&client, wrong_table, &insert_payload).await;
    // Make sure service doesn't crash.
}

/// Error case: create a mooncake table with an invalid table config.
#[tokio::test]
#[serial]
async fn test_create_table_with_invalid_config() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    let client: reqwest::Client = reqwest::Client::new();
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");

    // ==================
    // Invalid config 1
    // ==================
    //
    // Create an invalid config payload.
    let payload = create_test_invalid_config_payload(DATABASE, TABLE);
    send_test_tables(&client, &crafted_src_table_name, &payload).await;

    // ==================
    // Invalid config 2
    // ==================
    //
    // Create an invalid config payload.
    let payload = create_test_invalid_config_payload(DATABASE, TABLE);
    send_test_tables(&client, &crafted_src_table_name, &payload).await;
}

#[tokio::test]
#[serial]
async fn test_schema_invalid_struct_missing_fields() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    let client = reqwest::Client::new();
    let crafted_src_table_name = format!("{DATABASE}.invalid-struct-table");
    let payload = json!({
        "database": DATABASE,
        "table": "invalid-struct-table",
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false},
            {"name": "bad_struct", "data_type": "struct", "nullable": true}
        ],
        "table_config": {"mooncake": {"append_only": true}}
    });

    let response = client
        .post(format!("{REST_ADDR}/tables/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        !response.status().is_success(),
        "Response status is {response:?}"
    );
}

#[tokio::test]
#[serial]
async fn test_schema_invalid_list_missing_item() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    let client = reqwest::Client::new();
    let crafted_src_table_name = format!("{DATABASE}.invalid-list-table");
    let payload = json!({
        "database": DATABASE,
        "table": "invalid-list-table",
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false},
            {"name": "bad_list", "data_type": "list", "nullable": true}
        ],
        "table_config": {"mooncake": {"append_only": true}}
    });

    let response = client
        .post(format!("{REST_ADDR}/tables/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());
}
