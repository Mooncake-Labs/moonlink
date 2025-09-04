use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use arrow_array::{Int32Array, RecordBatch, StringArray};
use parquet::arrow::AsyncArrowWriter;
use reqwest::Client;
use reqwest::Response;
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Local moonlink REST API IP/port address.
const REST_ADDR: &str = "http://127.0.0.1:3030";

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
pub(crate) fn create_test_json_payload() -> Value {
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
pub(crate) fn create_test_invalid_upload_operation(directory: &str) -> Value {
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
pub(crate) fn create_test_invalid_parquet_file_upload(directory: &str) -> Value {
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
pub(crate) fn create_test_invalid_ingest_operation() -> Value {
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
pub(crate) fn create_test_invalid_config_payload(database: &str, table: &str) -> Value {
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
pub(crate) async fn create_test_load_parquet_payload(directory: &str) -> Value {
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
pub(crate) async fn create_test_insert_parquet_payload(directory: &str) -> Value {
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
pub(crate) async fn send_test_ingest(
    client: &Client,
    table_name: &str,
    payload: &Value,
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
pub(crate) async fn send_test_upload(
    client: &Client,
    table_name: &str,
    payload: &Value,
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
pub(crate) async fn send_test_tables(
    client: &Client,
    table_name: &str,
    payload: &Value,
) -> Response {
    client
        .post(format!("{REST_ADDR}/tables/{table_name}"))
        .header("content-type", "application/json")
        .json(&payload)
        .send()
        .await
        .unwrap()
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
