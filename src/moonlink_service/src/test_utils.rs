use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use arrow_array::{Int32Array, RecordBatch, StringArray};
use parquet::arrow::AsyncArrowWriter;
use serde_json::json;

use std::collections::HashMap;
use std::sync::Arc;

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
