use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use arrow_array::{Int32Array, RecordBatch, StringArray};
use bytes::Bytes;
use moonlink::decode_serialized_read_state_for_testing;
use moonlink_rpc::{scan_table_begin, scan_table_end};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;

/// Local moonlink REST API IP/port address.
pub(crate) const REST_ADDR: &str = "http://127.0.0.1:3030";
/// Local moonlink TCP IP/port address.
pub(crate) const TCP_ADDR: &str = "127.0.0.1:3031";

const DATABASE: &str = "test_database_1";
const TABLE: &str = "test_table_1";

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

/// Util function to drop table.
pub(crate) fn get_drop_table_payload(database: &str, table: &str) -> serde_json::Value {
    let drop_table_payload = json!({
        "database":database,
        "table":table,
    });
    drop_table_payload
}

/// Util function to get insert payload.
pub(crate) fn get_create_insert_payload() -> serde_json::Value {
    let insert_payload = json!({
        "operation": "insert",
        "request_mode": "async",
        "data": {
            "id": 1,
            "name": "William",
            "email": "william@example.com",
            "age": 23,
        }
    });
    insert_payload
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

/// Util function to create test arrow batch.
pub(crate) fn create_test_arrow_batch() -> RecordBatch {
    RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["William".to_string()])),
            Arc::new(StringArray::from(vec!["william@example.com".to_string()])),
            Arc::new(Int32Array::from(vec![23])),
        ],
    )
    .unwrap()
}

/// Util function to create table via REST API.
pub(crate) async fn create_table(client: &reqwest::Client, database: &str, table: &str) {
    // REST API doesn't allow duplicate source table name.
    let crafted_src_table_name = format!("{database}.{table}");

    // Use nested schema when explicitly requested to keep tests lightweight and explicit.
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

pub(crate) async fn drop_table(client: &reqwest::Client, database: &str, table: &str) {
    let crafted_src_table_name = format!("{database}.{table}");
    let payload = get_drop_table_payload(database, table);
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
    )
}

pub(crate) async fn execute_test_ingest(
    client: &reqwest::Client,
    table_name: &str,
    payload: &serde_json::Value,
) {
    let response: reqwest::Response = client
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

#[tokio::main]
async fn main() {
    // Create test table (nested schema).
    let client = reqwest::Client::new();
    create_table(&client, DATABASE, TABLE).await;

    // Ingest nested data.
    let insert_payload = get_create_insert_payload();
    let crafted_src_table_name = format!("{DATABASE}.{TABLE}");
    execute_test_ingest(&client, &crafted_src_table_name, &insert_payload).await;

    // Scan table and get data file and puffin files back.
    let mut moonlink_stream = TcpStream::connect(TCP_ADDR).await.unwrap();
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
    drop_table(&client, DATABASE, TABLE).await;
}
