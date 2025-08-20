use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use arrow_array::{Int32Array, StringArray};
use parquet::arrow::AsyncArrowWriter;

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

/// Test util function to generate a parquet under the given [`tempdir`].
pub(crate) async fn generate_parquet_file(directory: &str) -> String {
    let schema = create_test_arrow_schema();
    let ids = Int32Array::from(vec![1, 2, 3]);
    let names = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    let emails = StringArray::from(vec![
        "Alice@gmail.com",
        "Bob@gmail.com",
        "Charlie@gmail.com",
    ]);
    let ages = Int32Array::from(vec![10, 20, 30]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids),
            Arc::new(names),
            Arc::new(emails),
            Arc::new(ages),
        ],
    )
    .unwrap();
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
