use std::sync::Arc;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type as IcebergType};
use iceberg::Result as IcebergResult;
use iceberg::{NamespaceIdent, TableCreation};

// Test util function to get test table schema.
pub(crate) fn create_test_table_schema() -> IcebergResult<Schema> {
    let field = NestedField::required(
        /*id=*/ 1,
        "field_name".to_string(),
        IcebergType::Primitive(PrimitiveType::Int),
    );
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![Arc::new(field)])
        .build()?;

    Ok(schema)
}

// Test util function to get table schema for the given table.
pub(crate) fn create_test_table_creation(
    namespace_ident: &NamespaceIdent,
    table_name: &str,
) -> IcebergResult<TableCreation> {
    let schema = create_test_table_schema()?;
    let table_creation = TableCreation::builder()
        .name(table_name.to_string())
        .location(format!(
            "file:///tmp/iceberg-test/{}/{}",
            namespace_ident.to_url_string(),
            table_name
        ))
        .schema(schema.clone())
        .build();
    Ok(table_creation)
}
