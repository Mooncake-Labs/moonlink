use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use arrow_array::{Int32Array, RecordBatch, StringArray};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::AsyncArrowWriter;
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use crate::rest_api::ListTablesResponse;
use crate::{ServiceConfig, READINESS_PROBE_PORT};
use moonlink_backend::table_status::TableStatus;

/// Local nginx server IP/port address.
pub(crate) const NGINX_ADDR: &str = "http://nginx.local:80";
/// Local moonlink REST API IP/port address.
pub(crate) const REST_ADDR: &str = "http://127.0.0.1:3030";
/// Local moonlink server IP/port address.
pub(crate) const MOONLINK_ADDR: &str = "127.0.0.1:3031";
/// Test database name.
pub(crate) const DATABASE: &str = "test-database";
/// Test table name.
pub(crate) const TABLE: &str = "test-table";

pub(crate) struct TestGuard {
    dir: String,
}

impl TestGuard {
    pub(crate) async fn new(dir: &str) -> Self {
        cleanup_directory(dir);
        Self {
            dir: dir.to_string(),
        }
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        cleanup_directory(&self.dir);
    }
}

fn cleanup_directory(dir: &str) {
    let dir = std::path::Path::new(dir);
    if dir.exists() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                cleanup_directory(path.to_str().unwrap());
                std::fs::remove_dir_all(path).unwrap();
            } else {
                std::fs::remove_file(path).unwrap();
            }
        }
    }
}

/// Util function to create test arrow schema.
fn create_test_arrow_schema() -> Arc<ArrowSchema> {
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

/// Moonlink backend directory.
pub(crate) fn get_moonlink_backend_dir() -> String {
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

pub(crate) fn get_service_config() -> ServiceConfig {
    let moonlink_backend_dir = get_moonlink_backend_dir();
    let nginx_addr = get_nginx_addr();

    ServiceConfig {
        base_path: moonlink_backend_dir.clone(),
        data_server_uri: Some(nginx_addr),
        rest_api_port: Some(3030),
        tcp_port: Some(3031),
    }
}

/// Send request to readiness endpoint.
pub(crate) async fn test_readiness_probe() {
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

/// Util function to create table via REST API.
pub(crate) async fn create_table(client: &reqwest::Client, database: &str, table: &str) {
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
