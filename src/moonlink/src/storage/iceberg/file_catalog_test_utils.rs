use iceberg::spec::PrimitiveType;
use iceberg::spec::Type as IcebergType;
use iceberg::spec::{NestedField, Schema};
use std::sync::Arc;
use tempfile::TempDir;

use crate::storage::filesystem::filesystem_config::FileSystemConfig;
use crate::storage::iceberg::file_catalog::FileCatalog;

/// Test util to create file catalog.
pub(crate) fn create_test_file_catalog(tmp_dir: &TempDir, iceberg_schema: Schema) -> FileCatalog {
    let warehouse_path = tmp_dir.path().to_str().unwrap();
    FileCatalog::new(
        FileSystemConfig::FileSystem {
            root_directory: warehouse_path.to_string(),
        },
        iceberg_schema,
    )
    .unwrap()
}

// Test util function to get iceberg schema,
pub(crate) fn get_test_schema() -> Schema {
    let field = NestedField::required(
        /*id=*/ 1,
        "field_name".to_string(),
        IcebergType::Primitive(PrimitiveType::Int),
    );

    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![Arc::new(field)])
        .build()
        .unwrap()
}

// Test util function to get new table schema, used in schema evolution tests.
pub(crate) fn get_updated_test_schema() -> Schema {
    let field_1 = NestedField::required(
        /*id=*/ 10,
        "new_field_10".to_string(),
        IcebergType::Primitive(PrimitiveType::Double),
    );
    let field_2 = NestedField::required(
        /*id=*/ 20,
        "new_field_20".to_string(),
        IcebergType::Primitive(PrimitiveType::String),
    );

    Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![Arc::new(field_1), Arc::new(field_2)])
        .build()
        .unwrap()
}
