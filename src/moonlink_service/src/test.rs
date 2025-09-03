use arrow_array::{ListArray, RecordBatch, StructArray};
use async_recursion::async_recursion;
use bytes::Bytes;
use more_asserts as ma;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use serial_test::serial;
use std::env;
use tokio::net::TcpStream;

use crate::rest_api::{
    CreateTableResponse, FileUploadResponse, HealthResponse, IngestResponse, ListTablesResponse,
};
use crate::test_utils::*;
use crate::{start_with_config, ServiceConfig, READINESS_PROBE_PORT};
use moonlink::decode_serialized_read_state_for_testing;
use moonlink_backend::table_status::TableStatus;
use moonlink_rpc::{load_files, scan_table_begin, scan_table_end};

/// Moonlink backend directory.
fn get_moonlink_backend_dir() -> String {
    if let Ok(backend_dir) = env::var("MOONLINK_BACKEND_DIR") {
        backend_dir
    } else {
        "/workspaces/moonlink/.shared-nginx".to_string()
    }
}

/// Util function to get nginx address
fn get_nginx_addr() -> String {
    env::var("NGINX_ADDR").unwrap_or_else(|_| NGINX_ADDR.to_string())
}

/// Local nginx server IP/port address.
const NGINX_ADDR: &str = "http://nginx.local:80";
/// Local moonlink REST API IP/port address.
const REST_ADDR: &str = "http://127.0.0.1:3030";
/// Local moonlink server IP/port address.
const MOONLINK_ADDR: &str = "127.0.0.1:3031";
/// Test database name.
const DATABASE: &str = "test-database";
/// Test table name.
const TABLE: &str = "test-table";

fn get_service_config() -> ServiceConfig {
    let moonlink_backend_dir = get_moonlink_backend_dir();
    let nginx_addr = get_nginx_addr();

    ServiceConfig {
        base_path: moonlink_backend_dir.clone(),
        data_server_uri: Some(nginx_addr),
        rest_api_port: Some(3030),
        tcp_port: Some(3031),
    }
}

/// Util function to delete and all subdirectories and files in the given directory.
#[async_recursion]
async fn cleanup_directory(dir: &str) {
    let dir = std::path::Path::new(dir);
    let mut entries = tokio::fs::read_dir(dir).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let entry_path = entry.path();
        if entry_path.is_dir() {
            cleanup_directory(entry_path.to_str().unwrap()).await;
            tokio::fs::remove_dir_all(&entry_path).await.unwrap();
        } else {
            tokio::fs::remove_file(&entry_path).await.unwrap();
        }
    }
}

struct TestGuard {
    dir: String,
}

impl TestGuard {
    async fn new(dir: &str) -> Self {
        cleanup_directory(dir).await;
        Self {
            dir: dir.to_string(),
        }
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        fn cleanup_sync(dir: &str) {
            let dir = std::path::Path::new(dir);
            if dir.exists() {
                for entry in std::fs::read_dir(dir).unwrap() {
                    let entry = entry.unwrap();
                    let path = entry.path();
                    if path.is_dir() {
                        cleanup_sync(path.to_str().unwrap());
                        std::fs::remove_dir_all(path).unwrap();
                    } else {
                        std::fs::remove_file(path).unwrap();
                    }
                }
            }
        }

        cleanup_sync(&self.dir);
    }
}

/// Send request to readiness endpoint.
async fn test_readiness_probe() {
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

/// Util function to get table drop payload.
fn get_drop_table_payload(database: &str, table: &str) -> serde_json::Value {
    let drop_table_payload = json!({
        "database": database,
        "table": table
    });
    drop_table_payload
}

/// Util function to get table optimize payload.
fn get_optimize_table_payload(database: &str, table: &str, mode: &str) -> serde_json::Value {
    let optimize_table_payload = json!({
        "database": database,
        "table": table,
        "mode": mode
    });
    optimize_table_payload
}

/// Util function to get create snapshot payload.
fn get_create_snapshot_payload(database: &str, table: &str, lsn: u64) -> serde_json::Value {
    let snapshot_creation_payload = json!({
        "database": database,
        "table": table,
        "lsn": lsn
    });
    snapshot_creation_payload
}

/// Util function to create table via REST API.
async fn create_table(client: &reqwest::Client, database: &str, table: &str) {
    // REST API doesn't allow duplicate source table name.
    let crafted_src_table_name = format!("{database}.{table}");

    let payload = get_create_table_payload(database, table);
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

/// Util function to drop table via REST API.
async fn drop_table(client: &reqwest::Client, database: &str, table: &str) {
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

async fn list_tables(client: &reqwest::Client) -> Vec<TableStatus> {
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

/// Util function to load all record batches inside of the given [`path`].
async fn read_all_batches(url: &str) -> Vec<RecordBatch> {
    let resp = reqwest::get(url).await.unwrap();
    assert!(resp.status().is_success(), "Response status is {resp:?}");
    let data: Bytes = resp.bytes().await.unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    reader.into_iter().map(|b| b.unwrap()).collect()
}

#[tokio::test]
#[serial]
async fn test_health_check_endpoint() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

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
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

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
            {"name": "Decimal(2,-3)", "data_type": "decimal(2,-3)", "nullable": false} // negative scale value
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

#[tokio::test]
#[serial]
async fn test_schema_with_struct_and_list() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table with struct and list types.
    let client = reqwest::Client::new();
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let payload = json!({
        "database": DATABASE,
        "table": TABLE,
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false},
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

#[tokio::test]
#[serial]
async fn test_schema_with_nested_lists() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    let client = reqwest::Client::new();
    let crafted_src_table_name = format!("{DATABASE}.array-alias-table");
    let payload = json!({
        "database": DATABASE,
        "table": "array-alias-table",
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false},
            // Use 'array' alias and nested list: List<List<Int32>>
            {"name": "matrix", "data_type": "array", "nullable": true, "item": {
                "name": "row", "data_type": "list", "nullable": true, "item": {
                    "name": "cell", "data_type": "int32", "nullable": true
                }
            }}
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
        response.status().is_success(),
        "Response status is {response:?}"
    );
    let response: CreateTableResponse = response.json().await.unwrap();
    assert_eq!(response.database, DATABASE);
    assert_eq!(response.table, crafted_src_table_name);
    assert_eq!(response.lsn, 1);
}

#[tokio::test]
#[serial]
async fn test_schema_invalid_struct_missing_fields() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

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
    assert!(!response.status().is_success());
}

#[tokio::test]
#[serial]
async fn test_schema_invalid_list_missing_item() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

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

#[tokio::test]
#[serial]
async fn test_nested_schema_and_ingest_data() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // 1) Create table with nested struct and list fields.
    let client = reqwest::Client::new();
    let table = "nested-table";
    let crafted_src_table_name = format!("{DATABASE}.{table}");
    let payload = json!({
        "database": DATABASE,
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
        "table_config": {"mooncake": {"append_only": true}}
    });
    let response = client
        .post(format!("{REST_ADDR}/tables/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());

    // 2) Ingest nested row.
    let insert_payload = json!({
        "operation": "insert",
        "request_mode": "async",
        "data": {
            "id": 1,
            "user": {
                "name": "Alice",
                "age": 30,
                "emails": ["alice@example.com", "a@ex.io"],
                "location": {"lat": 1.23, "lon": 4.56}
            },
            "events": [1000, 2000]
        }
    });
    let response = client
        .post(format!("{REST_ADDR}/ingest/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&insert_payload)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());

    // 3) Scan the table and validate the produced Arrow batch contains the nested shapes.
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        table.to_string(),
        /*lsn=*/ 1,
    )
    .await
    .unwrap();
    let (data_file_paths, puffin_file_paths, puffin_deletion, positional_deletion) =
        decode_serialized_read_state_for_testing(bytes);
    assert_eq!(data_file_paths.len(), 1);
    assert!(puffin_file_paths.is_empty());
    assert!(puffin_deletion.is_empty());
    assert!(positional_deletion.is_empty());

    let batches = read_all_batches(&data_file_paths[0]).await;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 3);
    assert_eq!(batch.num_rows(), 1);

    // Column 0: id (Int32)
    {
        use arrow_array::Int32Array;
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 1);
    }

    // Column 1: user (Struct)
    {
        let struct_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(struct_array.columns().len(), 4);
        // name
        {
            let arr = struct_array
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            assert_eq!(arr.value(0), "Alice");
        }
        // age
        {
            let arr = struct_array
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap();
            assert_eq!(arr.value(0), 30);
        }
        // emails (List<Utf8>)
        {
            let list = struct_array
                .column(2)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let values = list
                .values()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let offsets = list.value_offsets();
            let offset_start = offsets[0] as usize;
            let offset_end = offsets[1] as usize;
            assert_eq!(offset_end - offset_start, 2);
            assert_eq!(values.value(offset_start), "alice@example.com");
            assert_eq!(values.value(offset_start + 1), "a@ex.io");
        }
        // location (Struct{lat,lon})
        {
            let location = struct_array
                .column(3)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let lat = location
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .unwrap();
            let lon = location
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .unwrap();
            assert_eq!(lat.value(0), 1.23);
            assert_eq!(lon.value(0), 4.56);
        }
    }

    // Column 2: events (List<Int64>)
    {
        let list = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = list
            .values()
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        let offsets = list.value_offsets();
        let start = offsets[0] as usize;
        let end = offsets[1] as usize;
        assert_eq!(end - start, 2);
        assert_eq!(values.value(start), 1000);
        assert_eq!(values.value(start + 1), 2000);
    }

    scan_table_end(
        &mut moonlink_stream,
        DATABASE.to_string(),
        table.to_string(),
    )
    .await
    .unwrap();
}

/// Util function to optimize table via REST API.
async fn optimize_table(client: &reqwest::Client, database: &str, table: &str, mode: &str) {
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

/// Util function to test optimize table
async fn run_optimize_table_test(mode: &str) {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE).await;

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

/// Util function to create snapshot via REST API.
async fn create_snapshot(client: &reqwest::Client, database: &str, table: &str, lsn: u64) {
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

/// Test Create Snapshot
#[tokio::test]
#[serial]
async fn test_create_snapshot() {
    cleanup_directory(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE).await;

    // Ingest some data.
    let insert_payload = json!({
        "operation": "insert",
        "request_mode": "sync",
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
    let response: IngestResponse = response.json().await.unwrap();
    let lsn = response.lsn.unwrap();

    // After all changes reflected at mooncake snapshot, trigger an iceberg snapshot.
    create_snapshot(&client, DATABASE, TABLE, lsn).await;

    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        /*lsn=*/ lsn,
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

    // Cleanup shared directory.
    cleanup_directory(&get_moonlink_backend_dir()).await;
}

/// Test basic table creation, insertion and query.
#[tokio::test]
#[serial]
async fn test_moonlink_standalone_data_ingestion() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE).await;

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

/// Test basic table creation, file upload and query.
#[tokio::test]
#[serial]
async fn test_moonlink_standalone_file_upload() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE).await;

    // Upload a file.
    let parquet_file = generate_parquet_file(&get_moonlink_backend_dir()).await;
    let file_upload_payload = json!({
        "operation": "upload",
        "request_mode": "sync",
        "files": [parquet_file],
        "storage_config": {
            "fs": {
                "root_directory": get_moonlink_backend_dir(),
                "atomic_write_dir": get_moonlink_backend_dir()
            }
        }
    });
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response = client
        .post(format!("{REST_ADDR}/upload/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&file_upload_payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );
    let response: FileUploadResponse = response.json().await.unwrap();
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
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE).await;

    // Upload a file.
    let parquet_file = generate_parquet_file(&get_moonlink_backend_dir()).await;
    let file_upload_payload = json!({
        "operation": "insert",
        "files": [parquet_file],
        "request_mode": "async",
        "storage_config": {
            "fs": {
                "root_directory": get_moonlink_backend_dir(),
                "atomic_write_dir": get_moonlink_backend_dir()
            }
        }
    });
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response = client
        .post(format!("{REST_ADDR}/upload/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&file_upload_payload)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Response status is {response:?}"
    );

    // Scan table and get data file and puffin files back.
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        // Four events generated: one append and one commit, with commit LSN 2.
        /*lsn=*/
        2,
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

/// Testing scenario: two tables with the same name, but under different databases are created.
#[tokio::test]
#[serial]
async fn test_multiple_tables_creation() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create the first test table.
    let client: reqwest::Client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE).await;

    // Create the second test table.
    create_table(&client, "second-database", TABLE).await;
}

#[tokio::test]
#[serial]
async fn test_drop_table() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE).await;

    // List table before drop.
    let list_results = list_tables(&client).await;
    assert_eq!(list_results.len(), 1);

    // Drop test table.
    drop_table(&client, DATABASE, TABLE).await;

    // List table before drop.
    let list_results = list_tables(&client).await;
    assert_eq!(list_results.len(), 0);
}

/// Testing scenario: create multiple tables, and check list table result.
#[tokio::test]
#[serial]
async fn test_list_tables() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create two test tables.
    let client: reqwest::Client = reqwest::Client::new();
    create_table(&client, "database-1", "table-1").await;
    create_table(&client, "database-2", "table-2").await;

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
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    let client: reqwest::Client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE).await;

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
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create test table.
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE).await;

    // Test invalid operation to upload a file.
    let file_upload_payload = json!({
        "operation": "invalid_upload_operation",
        "files": ["parquet_file"],
        "storage_config": {
            "fs": {
                "root_directory": get_moonlink_backend_dir(),
                "atomic_write_dir": get_moonlink_backend_dir()
            }
        }
    });
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    let response = client
        .post(format!("{REST_ADDR}/upload/{crafted_src_table_name}"))
        .header("content-type", "application/json")
        .json(&file_upload_payload)
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success());

    // Test invalid operation to ingest data.
    let insert_payload = json!({
        "operation": "invalid_ingest_operation",
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
    assert!(!response.status().is_success());
}

/// Error case: non-existent source table.
#[tokio::test]
#[serial]
async fn test_non_existent_table() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    // Create the test table.
    let client: reqwest::Client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE).await;

    // Test invalid operation to upload a file.
    let file_upload_payload = json!({
        "operation": "upload",
        "request_mode": "async",
        "files": ["parquet_file"],
        "storage_config": {
            "fs": {
                "root_directory": get_moonlink_backend_dir(),
                "atomic_write_dir": get_moonlink_backend_dir()
            }
        }
    });
    let _response = client
        .post(format!("{REST_ADDR}/upload/non_existent_source_table"))
        .header("content-type", "application/json")
        .json(&file_upload_payload)
        .send()
        .await
        .unwrap();
    // Make sure service doesn't crash.

    // Test invalid operation to ingest data.
    let insert_payload = json!({
        "operation": "invalid_ingest_operation",
        "data": {
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 30
        }
    });
    let _response = client
        .post(format!("{REST_ADDR}/ingest/non_existent_source_table"))
        .header("content-type", "application/json")
        .json(&insert_payload)
        .send()
        .await
        .unwrap();
    // Make sure service doesn't crash.
}

/// Error case: create a mooncake table with an invalid table config.
#[tokio::test]
#[serial]
async fn test_create_table_with_invalid_config() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir()).await;
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

    let client: reqwest::Client = reqwest::Client::new();
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");

    // ==================
    // Invalid config 1
    // ==================
    //
    // Create an invalid config payload.
    let payload = json!({
        "database": DATABASE,
        "table": TABLE,
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false}
        ],
        "table_config": {
            "mooncake": {
                "append_only": false,
                "row_identity": "None"
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
    assert!(!response.status().is_success());

    // ==================
    // Invalid config 2
    // ==================
    //
    // Create an invalid config payload.
    let payload = json!({
        "database": DATABASE,
        "table": TABLE,
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false}
        ],
        "table_config": {
            "mooncake": {
                "append_only": true,
                "row_identity": "FullRow"
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
    assert!(!response.status().is_success());
}
