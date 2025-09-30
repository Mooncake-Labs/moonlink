#![cfg_attr(feature = "otel-integration", allow(dead_code))]
use crate::rest_api::{FileUploadResponse, IngestResponse, ListTablesResponse};
use crate::{ServiceConfig, READINESS_PROBE_PORT};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use arrow_array::{Int32Array, RecordBatch, StringArray};
use bytes::Bytes;
use moonlink::decode_serialized_read_state_for_testing;
use moonlink_backend::table_status::TableStatus;
use moonlink_rpc::{scan_table_begin, scan_table_end};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::AsyncArrowWriter;
use reqwest::Client;
use reqwest::Response;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;

/// Moonlink backend directory.
pub(crate) fn get_moonlink_backend_dir() -> String {
    if let Ok(backend_dir) = std::env::var("MOONLINK_BACKEND_DIR") {
        backend_dir
    } else {
        "/workspaces/moonlink/.shared-nginx".to_string()
    }
}

/// Util function to get database URI.
#[cfg(feature = "postgres-integration")]
pub(crate) fn get_database_uri() -> String {
    pub const SRC_URI: &str =
        "postgresql://postgres:postgres@postgres:5432/postgres?sslmode=disable";
    std::env::var("DATABASE_URL").unwrap_or_else(|_| SRC_URI.to_string())
}

/// Util function to get nginx address
pub(crate) fn get_nginx_addr() -> String {
    std::env::var("NGINX_ADDR").unwrap_or_else(|_| NGINX_ADDR.to_string())
}

/// REST API port.
pub(crate) const REST_API_PORT: u16 = 3030;
/// OTEL API port.
pub(crate) const OTEL_API_PORT: u16 = 3435;
/// TCP port.
pub(crate) const TCP_PORT: u16 = 3031;
/// Local nginx server IP/port address.
pub(crate) const NGINX_ADDR: &str = "http://nginx.local:80";
/// Local moonlink REST API IP/port address.
pub(crate) const REST_ADDR: &str = const_format::formatcp!("http://127.0.0.1:{}", REST_API_PORT);
/// Local moonlink server IP/port address.
pub(crate) const MOONLINK_ADDR: &str = const_format::formatcp!("127.0.0.1:{}", TCP_PORT);
/// Test database name.
pub(crate) const DATABASE: &str = "test-database";
/// Test table name.
pub(crate) const TABLE: &str = "test-table";

pub(crate) fn get_service_config() -> ServiceConfig {
    let moonlink_backend_dir = get_moonlink_backend_dir();
    let nginx_addr = get_nginx_addr();

    ServiceConfig {
        base_path: moonlink_backend_dir.clone(),
        data_server_uri: Some(nginx_addr),
        rest_api_port: Some(REST_API_PORT),
        otel_ingestion_api_port: Some(OTEL_API_PORT),
        tcp_port: Some(TCP_PORT),
        log_directory: None,
        otel_export_target: None,
    }
}

/// Send request to readiness endpoint and wait until the server is ready.
pub(crate) async fn wait_for_server_ready() {
    let url = format!("http://127.0.0.1:{READINESS_PROBE_PORT}/ready");
    loop {
        if let Ok(resp) = reqwest::get(&url).await {
            if resp.status() == reqwest::StatusCode::OK {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Util function to create test arrow schema.
pub(crate) fn create_test_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, /*nullable=*/ false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "0".to_string(),
        )])),
        Field::new("name", DataType::Utf8, /*nullable=*/ false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "1".to_string(),
        )])),
        Field::new("email", DataType::Utf8, /*nullable=*/ true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "2".to_string(),
        )])),
        Field::new("age", DataType::Int32, /*nullable=*/ true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "3".to_string(),
        )])),
    ]))
}

/// Test util function to create json payload.
pub(crate) fn create_test_json_payload() -> serde_json::Value {
    json!({
        "operation": "insert",
        "request_mode": "sync",
        "data": {
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 30
        }
    })
}

/// Test util function to create invalid upload operation.
pub(crate) fn create_test_invalid_upload_operation(directory: &str) -> serde_json::Value {
    json!({
        "operation": "invalid_upload_operation",
        "files": ["parquet_file"],
        "storage_config": {
            "fs": {
                "root_directory": directory,
                "atomic_write_dir": directory
            }
        }
    })
}

/// Test util function to create invalid parquet upload.
pub(crate) fn create_test_invalid_parquet_file_upload(directory: &str) -> serde_json::Value {
    json!({
        "operation": "upload",
        "request_mode": "async",
        "files": ["parquet_file"],
        "storage_config": {
            "fs": {
                "root_directory": directory,
                "atomic_write_dir": directory
            }
        }
    })
}

/// Test util function to create invalid ingest operation.
pub(crate) fn create_test_invalid_ingest_operation() -> serde_json::Value {
    json!({
        "operation": "invalid_ingest_operation",
        "data": {
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 30
        }
    })
}

/// Test util function to create an invalid config payload.
pub(crate) fn create_test_invalid_config_payload(database: &str, table: &str) -> serde_json::Value {
    json!({
        "database": database,
        "table": table,
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false}
        ],
        "table_config": {
            "mooncake": {
                "append_only": true,
                "row_identity": "FullRow"
            }
        }
    })
}

/// Test util function to create load parquet file payload.
pub(crate) async fn create_test_load_parquet_payload(directory: &str) -> serde_json::Value {
    let parquet_file = generate_parquet_file(directory).await;
    json!({
        "operation": "upload",
        "request_mode": "sync",
        "files": [parquet_file],
        "storage_config": {
            "fs": {
                "root_directory": directory,
                "atomic_write_dir": directory
            }
        }
    })
}

/// Test util function to create ingest parquet file payload.
pub(crate) async fn create_test_insert_parquet_payload(directory: &str) -> serde_json::Value {
    let parquet_file = generate_parquet_file(directory).await;
    json!({
        "operation": "insert",
        "request_mode": "sync",
        "files": [parquet_file],
        "storage_config": {
            "fs": {
                "root_directory": directory,
                "atomic_write_dir": directory
            }
        }
    })
}

/// Test util function to send ingest request.
pub(crate) async fn execute_test_ingest(
    client: &Client,
    table_name: &str,
    payload: &serde_json::Value,
) -> IngestResponse {
    let response = client
        .post(format!("{REST_ADDR}/ingest/{table_name}"))
        .header("content-type", "application/json")
        .json(payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    response.json().await.unwrap()
}

/// Test util function to send invalid ingest request.
pub(crate) async fn execute_test_invalid_ingest(
    client: &Client,
    table_name: &str,
    payload: &serde_json::Value,
) -> Response {
    client
        .post(format!("{REST_ADDR}/ingest/{table_name}"))
        .header("content-type", "application/json")
        .json(payload)
        .send()
        .await
        .unwrap()
}

/// Test util function to send upload request.
pub(crate) async fn execute_test_upload(
    client: &Client,
    table_name: &str,
    payload: &serde_json::Value,
) -> FileUploadResponse {
    let response = client
        .post(format!("{REST_ADDR}/upload/{table_name}"))
        .header("content-type", "application/json")
        .json(payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    response.json().await.unwrap()
}

/// Test util function to send invalid upload request.
pub(crate) async fn execute_test_invalid_upload(
    client: &Client,
    table_name: &str,
    payload: &serde_json::Value,
) -> Response {
    client
        .post(format!("{REST_ADDR}/upload/{table_name}"))
        .header("content-type", "application/json")
        .json(payload)
        .send()
        .await
        .unwrap()
}

// Test util function to send tables request.
pub(crate) async fn execute_test_tables(
    client: &Client,
    table_name: &str,
    payload: &serde_json::Value,
) {
    let response = client
        .post(format!("{REST_ADDR}/tables/{table_name}"))
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

/// Util function to create test arrow batch.
pub(crate) fn create_test_arrow_batch() -> RecordBatch {
    RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alice Johnson".to_string()])),
            Arc::new(StringArray::from(vec!["alice@example.com".to_string()])),
            Arc::new(Int32Array::from(vec![30])),
        ],
    )
    .unwrap()
}

pub(crate) fn create_test_arrow_insert_payload_nested() -> serde_json::Value {
    let insert_payload = json!({
        "operation": "insert",
        "request_mode": "async",
        "data": {
            "id": 1,
            "user": {
                "name": "Alice Johnson",
                "age": 30,
                "emails": ["alice@example.com", "alice2@example.com"],
                "location": {"lat": 37.7749, "lon": -122.4194}
            },
            "events": [1712345678901_i64, 1712345678902_i64]
        }
    });
    insert_payload
}

/// Util function to create a nested test arrow schema matching [`get_create_table_payload_nested`].
pub(crate) fn create_test_arrow_schema_nested() -> Arc<ArrowSchema> {
    use arrow::datatypes::{DataType, Field};

    // Helper to build metadata with a specific field id
    fn meta(id: i32) -> std::collections::HashMap<String, String> {
        HashMap::from([("PARQUET:field_id".to_string(), id.to_string())])
    }

    // top-level id field
    let id = Field::new("id", DataType::Int32, /*nullable=*/ false).with_metadata(meta(0));

    // user struct fields
    let user_name = Field::new("name", DataType::Utf8, /*nullable=*/ true).with_metadata(meta(1));
    let user_age = Field::new("age", DataType::Int32, /*nullable=*/ true).with_metadata(meta(2));

    // user.emails is list<utf8>
    let emails_item = Field::new("item", DataType::Utf8, /*nullable=*/ true).with_metadata(meta(3));
    let emails_list = Field::new(
        "emails",
        DataType::List(Arc::new(emails_item.clone())),
        /*nullable=*/ true,
    )
    .with_metadata(meta(4));

    // user.location struct fields
    let loc_lat = Field::new("lat", DataType::Float64, /*nullable=*/ true).with_metadata(meta(5));
    let loc_lon = Field::new("lon", DataType::Float64, /*nullable=*/ true).with_metadata(meta(6));
    let location_struct = Field::new_struct(
        "location",
        vec![loc_lat.clone(), loc_lon.clone()],
        /*nullable=*/ true,
    )
    .with_metadata(meta(7));

    let user_struct = Field::new_struct(
        "user",
        vec![
            user_name.clone(),
            user_age.clone(),
            emails_list.clone(),
            location_struct.clone(),
        ],
        /*nullable=*/ true,
    )
    .with_metadata(meta(8));

    // events is list<int64>
    let events_item =
        Field::new("item", DataType::Int64, /*nullable=*/ true).with_metadata(meta(9));
    let events_list = Field::new(
        "events",
        DataType::List(Arc::new(events_item.clone())),
        /*nullable=*/ true,
    )
    .with_metadata(meta(10));

    Arc::new(ArrowSchema::new(vec![id, user_struct, events_list]))
}

/// Util function to create a nested test arrow batch matching [`get_create_table_payload_nested`].
pub(crate) fn create_test_arrow_batch_nested() -> RecordBatch {
    use arrow::datatypes::DataType;
    use arrow_array::{ArrayRef, Float64Array, Int32Array, ListArray, StringArray, StructArray};
    use arrow_buffer::OffsetBuffer;

    // Build schema pieces (reuse to ensure data types, names, and metadata match)
    let schema = create_test_arrow_schema_nested();

    // Extract child field definitions for constructing nested arrays (by name for robustness)
    let user_field = schema.field_with_name("user").unwrap().clone();
    let events_field = Arc::new(schema.field_with_name("events").unwrap().clone());

    // id column
    let id_array: ArrayRef = Arc::new(Int32Array::from(vec![1]));

    // user struct column children: name, age, emails(list<utf8>), location(struct)
    let user_fields = match user_field.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => panic!("unexpected user field type"),
    };

    let name_array: ArrayRef = Arc::new(StringArray::from(vec![Some("Alice Johnson")]));
    let age_array: ArrayRef = Arc::new(Int32Array::from(vec![Some(30)]));

    // emails list<utf8>
    let emails_field = user_fields.find("emails").unwrap().1.clone();
    let emails_child_field = match emails_field.data_type() {
        DataType::List(child) => child.clone(),
        _ => panic!("unexpected emails field type"),
    };
    let emails_values: ArrayRef = Arc::new(StringArray::from(vec![
        Some("alice@example.com"),
        Some("alice2@example.com"),
    ]));
    let emails_offsets = OffsetBuffer::new(vec![0_i32, 2_i32].into());
    let emails_array: ArrayRef = Arc::new(ListArray::new(
        emails_child_field,
        emails_offsets,
        emails_values,
        None,
    ));

    // location struct
    let location_field = user_fields.find("location").unwrap().1.clone();
    let location_children = match location_field.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => panic!("unexpected location field type"),
    };
    let lat_array: ArrayRef = Arc::new(Float64Array::from(vec![Some(37.7749)]));
    let lon_array: ArrayRef = Arc::new(Float64Array::from(vec![Some(-122.4194)]));
    let location_array: ArrayRef = Arc::new(StructArray::new(
        location_children,
        vec![lat_array, lon_array],
        None,
    ));

    let user_array: ArrayRef = Arc::new(StructArray::new(
        match user_field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        },
        vec![name_array, age_array, emails_array, location_array],
        None,
    ));

    // events list<int64>
    let events_values: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![
        Some(1712345678901_i64),
        Some(1712345678902_i64),
    ]));
    let events_offsets = OffsetBuffer::new(vec![0_i32, 2_i32].into());
    let events_child_field = match events_field.data_type() {
        DataType::List(child) => child.clone(),
        _ => panic!("unexpected events field type"),
    };
    let events_array: ArrayRef = Arc::new(ListArray::new(
        events_child_field,
        events_offsets,
        events_values,
        None,
    ));

    RecordBatch::try_new(schema, vec![id_array, user_array, events_array]).unwrap()
}

/// Test util function to generate a parquet under the given [`tempdir`].
pub(crate) async fn generate_parquet_file(directory: &str) -> String {
    let schema = create_test_arrow_schema();
    let batch = create_test_arrow_batch();
    let dir_path = std::path::Path::new(directory);
    let file_path = dir_path.join("test.parquet");
    let file_path_str = file_path.to_str().unwrap().to_string();
    let file = tokio::fs::File::create(file_path).await.unwrap();
    let mut writer: AsyncArrowWriter<tokio::fs::File> =
        AsyncArrowWriter::try_new(file, schema, /*props=*/ None).unwrap();
    writer.write(&batch).await.unwrap();
    writer.close().await.unwrap();
    file_path_str
}

/// Util function to get table creation payload.
fn get_create_table_payload(database: &str, table: &str) -> serde_json::Value {
    let create_table_payload = json!({
        "database": database,
        "table": table,
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false},
            {"name": "name", "data_type": "string", "nullable": false},
            {"name": "email", "data_type": "string", "nullable": true},
            {"name": "age", "data_type": "int32", "nullable": true}
        ],
        "table_config": {
            "mooncake": {
                "append_only": true,
                "row_identity": "None"
            }
        }
    });
    create_table_payload
}

/// Optional nested schema payload for testing nested struct/list without adding a new test.
fn get_create_table_payload_nested(database: &str, table: &str) -> serde_json::Value {
    json!({
        "database": database,
        "table": table,
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false},
            {"name": "user", "data_type": "struct", "nullable": true, "fields": [
                {"name": "name", "data_type": "string", "nullable": true},
                {"name": "age", "data_type": "int32", "nullable": true},
                {"name": "emails", "data_type": "list", "nullable": true, "item": {"name": "email", "data_type": "string", "nullable": true}},
                {"name": "location", "data_type": "struct", "nullable": true, "fields": [
                    {"name": "lat", "data_type": "float64", "nullable": true},
                    {"name": "lon", "data_type": "float64", "nullable": true}
                ]}
            ]},
            {"name": "events", "data_type": "list", "nullable": true, "item": {"name": "ts", "data_type": "int64", "nullable": true}}
        ],
        "table_config": {"mooncake": {"append_only": true, "row_identity": "None"}}
    })
}

/// Util function to get table drop payload.
fn get_drop_table_payload(database: &str, table: &str) -> serde_json::Value {
    let drop_table_payload = json!({
        "database": database,
        "table": table
    });
    drop_table_payload
}

/// Util function to get table optimize payload.
pub(crate) fn get_optimize_table_payload(
    database: &str,
    table: &str,
    mode: &str,
) -> serde_json::Value {
    let optimize_table_payload = json!({
        "database": database,
        "table": table,
        "mode": mode
    });
    optimize_table_payload
}

/// Util function to get create snapshot payload.
pub(crate) fn get_create_snapshot_payload(
    database: &str,
    table: &str,
    lsn: u64,
) -> serde_json::Value {
    let snapshot_creation_payload = json!({
        "database": database,
        "table": table,
        "lsn": lsn
    });
    snapshot_creation_payload
}

/// Util function to get table flush payload.
pub(crate) fn get_flush_table_payload(database: &str, table: &str, lsn: u64) -> serde_json::Value {
    let flush_table_payload = json!({
        "database": database,
        "table": table,
        "lsn": lsn
    });
    flush_table_payload
}

/// Util function to get create table from PostgreSQL payload.
pub(crate) fn get_create_table_from_postgres_payload(
    database: &str,
    table: &str,
    src_uri: &str,
    src_table_name: &str,
) -> serde_json::Value {
    let create_table_payload = json!({
        "database": database,
        "table": table,
        "src_uri": src_uri,
        "src_table_name": src_table_name,
        "table_config": {
            "mooncake": {
                "append_only": true
            }
        }
    });
    create_table_payload
}

/// Util function to create table via REST API.
pub(crate) async fn create_table(
    client: &reqwest::Client,
    database: &str,
    table: &str,
    nested: bool,
) {
    // REST API doesn't allow duplicate source table name.
    let crafted_src_table_name = format!("{database}.{table}");

    // Use nested schema when explicitly requested to keep tests lightweight and explicit.
    let payload = if nested {
        get_create_table_payload_nested(database, table)
    } else {
        get_create_table_payload(database, table)
    };
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
}

/// Util function to create table from PostgreSQL via REST API.
#[allow(dead_code)]
pub(crate) async fn create_table_from_postgres(
    client: &reqwest::Client,
    database: &str,
    table: &str,
    src_uri: &str,
    src_table_name: &str,
) {
    // REST API doesn't allow duplicate source table name.
    let crafted_src_table_name = format!("{database}.{table}");

    let payload = get_create_table_from_postgres_payload(database, table, src_uri, src_table_name);
    let response = client
        .post(format!(
            "{REST_ADDR}/tables/{crafted_src_table_name}/from_postgres"
        ))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
}

/// Util function to drop table via REST API.
pub(crate) async fn drop_table(client: &reqwest::Client, database: &str, table: &str) {
    let payload = get_drop_table_payload(database, table);
    let crafted_src_table_name = format!("{database}.{table}");
    let response = client
        .delete(format!("{REST_ADDR}/tables/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
}

/// Util function to optimize table via REST API.
pub(crate) async fn optimize_table(
    client: &reqwest::Client,
    database: &str,
    table: &str,
    mode: &str,
) {
    let payload = get_optimize_table_payload(database, table, mode);
    let crafted_src_table_name = format!("{database}.{table}");
    let response = client
        .post(format!(
            "{REST_ADDR}/tables/{crafted_src_table_name}/optimize"
        ))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
}

pub(crate) async fn list_tables(client: &reqwest::Client) -> Vec<TableStatus> {
    let response = client
        .get(format!("{REST_ADDR}/tables"))
        .header("content-type", "application/json")
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    let response: ListTablesResponse = response.json().await.unwrap();
    response.tables
}

/// Util function to sync flush via REST API.
pub(crate) async fn flush_table(client: &reqwest::Client, database: &str, table: &str, lsn: u64) {
    let payload = get_flush_table_payload(database, table, lsn);
    let crafted_src_table_name = format!("{database}.{table}");
    let response = client
        .post(format!("{REST_ADDR}/tables/{crafted_src_table_name}/flush"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
}

/// Util function to load all record batches for the given [`url`].
pub(crate) async fn read_all_batches(url: &str) -> Vec<RecordBatch> {
    let resp = reqwest::get(url).await.unwrap();
    assert!(resp.status().is_success(), "Response status is {resp:?}");
    let data: Bytes = resp.bytes().await.unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    reader.into_iter().map(|b| b.unwrap()).collect()
}

/// Util function to create snapshot via REST API.
pub(crate) async fn create_snapshot(
    client: &reqwest::Client,
    database: &str,
    table: &str,
    lsn: u64,
) {
    let payload = get_create_snapshot_payload(database, table, lsn);
    let crafted_src_table_name = format!("{database}.{table}");
    let response = client
        .post(format!(
            "{REST_ADDR}/tables/{crafted_src_table_name}/snapshot"
        ))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
}

/// Util function to check data file and puffin.
/// Called after insert/upload of a payload/parquet file.
pub(crate) async fn assert_data_and_puffin(table: &str, lsn: u64) {
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        table.to_string(),
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
        table.to_string(),
    )
    .await
    .unwrap();
}
