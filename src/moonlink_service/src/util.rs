use arrow_schema::{DataType, Field, Schema};
use moonlink_backend::{RowEventOperation, RowEventRequest};
use serde_json::Value;
use std::collections::HashMap;
use std::time::SystemTime;

// Convert field schemas to Arrow schema with proper field IDs (like PostgreSQL)
pub fn field_schemas_to_arrow_schema(
    schema_fields: &[moonlink_rpc::FieldSchema],
) -> Result<Schema, String> {
    let mut fields = Vec::with_capacity(schema_fields.len());

    for (field_id, field) in (0_i32..).zip(schema_fields.iter()) {
        let data_type = match field.data_type.as_str() {
            "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "string" | "text" => DataType::Utf8,
            "boolean" | "bool" => DataType::Boolean,
            "float32" => DataType::Float32,
            "float64" => DataType::Float64,
            "date32" => DataType::Date32,
            // Decimal type.
            dt if dt.starts_with("decimal(") && dt.ends_with(')') => {
                let inner = &dt[8..dt.len() - 1];
                let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
                // Arrow type allows no "scale", which defaults to 0.
                if parts.len() == 1 {
                    let precision: u8 = parts[0].parse().map_err(|_| {
                        format!("Invalid decimal precision in: {}", field.data_type)
                    })?;
                    DataType::Decimal128(precision, 0)
                } else if parts.len() == 2 {
                    // decimal(precision, scale)
                    let precision: u8 = parts[0].parse().map_err(|_| {
                        format!("Invalid decimal precision in: {}", field.data_type)
                    })?;
                    let scale: i8 = parts[1]
                        .parse()
                        .map_err(|_| format!("Invalid decimal scale in: {}", field.data_type))?;
                    DataType::Decimal128(precision, scale)
                } else {
                    return Err(format!("Invalid decimal type: {}", field.data_type));
                }
            }
            other => return Err(format!("Unsupported data type: {other}")),
        };

        // Create field with metadata (like PostgreSQL does)
        let mut metadata = HashMap::new();
        metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());

        fields.push(Field::new(&field.name, data_type, field.nullable).with_metadata(metadata));
    }

    Ok(Schema::new(fields))
}

/// Parse operation string to RowEventOperation
pub fn parse_row_operation(operation: &str) -> Result<RowEventOperation, String> {
    match operation {
        "insert" => Ok(RowEventOperation::Insert),
        "update" => Ok(RowEventOperation::Update),
        "delete" => Ok(RowEventOperation::Delete),
        _ => Err(format!(
            "Invalid operation '{operation}'. Must be 'insert', 'update', or 'delete'"
        )),
    }
}

/// Create RowEventRequest
pub fn create_row_event_request(
    src_table_name: String,
    operation: &str,
    payload: Value,
) -> Result<RowEventRequest, String> {
    let operation = parse_row_operation(operation)?;

    Ok(RowEventRequest {
        src_table_name,
        operation,
        payload,
        timestamp: SystemTime::now(),
    })
}
