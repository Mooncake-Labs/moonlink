use apache_avro::schema::{RecordField, Schema as AvroSchema};
use apache_avro::types::Value as AvroValue;
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use moonlink::row::{MoonlinkRow, RowValue};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AvroToMoonlinkRowError {
    #[error("unsupported avro type: {0}")]
    UnsupportedType(String),
    #[error("conversion failed for value: {0}")]
    ConversionFailed(String),
}

#[derive(Debug, Error)]
pub enum AvroToArrowSchemaError {
    #[error("unsupported avro schema type: {0}")]
    UnsupportedSchemaType(String),
    #[error("invalid avro schema: {0}")]
    InvalidSchema(String),
}

pub struct AvroToMoonlinkRowConverter;

impl AvroToMoonlinkRowConverter {
    pub fn convert(avro_value: &AvroValue) -> Result<MoonlinkRow, AvroToMoonlinkRowError> {
        match avro_value {
            AvroValue::Record(fields) => {
                let mut values = Vec::with_capacity(fields.len());

                for (_, field_value) in fields {
                    let row_value = Self::convert_value(field_value)?;
                    values.push(row_value);
                }
                Ok(MoonlinkRow::new(values))
            }
            _ => Err(AvroToMoonlinkRowError::UnsupportedType(format!(
                "{avro_value:?}"
            ))),
        }
    }

    fn convert_value(value: &AvroValue) -> Result<RowValue, AvroToMoonlinkRowError> {
        match value {
            // Null
            AvroValue::Null => Ok(RowValue::Null),

            // Primitive types - direct mapping
            AvroValue::Boolean(b) => Ok(RowValue::Bool(*b)),
            AvroValue::Int(i) => Ok(RowValue::Int32(*i)),
            AvroValue::Long(l) => Ok(RowValue::Int64(*l)),
            AvroValue::Float(f) => Ok(RowValue::Float32(*f)),
            AvroValue::Double(d) => Ok(RowValue::Float64(*d)),

            // String and binary types
            AvroValue::String(s) => Ok(RowValue::ByteArray(s.as_bytes().to_vec())),
            AvroValue::Bytes(b) => Ok(RowValue::ByteArray(b.clone())),

            // Fixed length binary (only support 16-byte for UUIDs)
            AvroValue::Fixed(16, bytes) => {
                let mut buf = [0u8; 16];
                buf.copy_from_slice(bytes);
                Ok(RowValue::FixedLenByteArray(buf))
            }
            AvroValue::Fixed(size, _) => Err(AvroToMoonlinkRowError::UnsupportedType(format!(
                "Fixed({size}) - only Fixed(16) is supported"
            ))),

            // Array types
            AvroValue::Array(items) => {
                let mut converted_elements = Vec::with_capacity(items.len());
                for item in items.iter() {
                    let converted_element = Self::convert_value(item)?;
                    converted_elements.push(converted_element);
                }
                Ok(RowValue::Array(converted_elements))
            }

            // Struct/Record types
            AvroValue::Record(record_fields) => {
                let mut values = Vec::with_capacity(record_fields.len());
                for (_, field_value) in record_fields {
                    let converted = Self::convert_value(field_value)?;
                    values.push(converted);
                }
                Ok(RowValue::Struct(values))
            }

            AvroValue::Map(map) => {
                let mut values = Vec::with_capacity(map.len());
                for (key, value) in map.iter() {
                    let converted = Self::convert_value(value)?;
                    values.push(RowValue::Struct(vec![
                        RowValue::ByteArray(key.as_bytes().to_vec()),
                        converted,
                    ]));
                }
                Ok(RowValue::Array(values))
            }

            // Union types (handle the boxed value directly)
            AvroValue::Union(_, boxed_value) => Self::convert_value(boxed_value),

            // Unsupported types
            _ => Err(AvroToMoonlinkRowError::UnsupportedType(format!(
                "{value:?}"
            ))),
        }
    }
}

/// Convert an Avro schema to an Arrow schema
pub fn convert_avro_to_arrow_schema(
    avro_schema: &AvroSchema,
) -> Result<ArrowSchema, AvroToArrowSchemaError> {
    match avro_schema {
        AvroSchema::Record(record_schema) => {
            let mut arrow_fields = Vec::with_capacity(record_schema.fields.len());
            let mut field_id = 0i32;

            for field in &record_schema.fields {
                let arrow_field = convert_field(field, &mut field_id)?;
                arrow_fields.push(arrow_field);
            }

            Ok(ArrowSchema::new(arrow_fields))
        }
        _ => Err(AvroToArrowSchemaError::UnsupportedSchemaType(
            "Only record schemas are supported at the top level".to_string(),
        )),
    }
}

fn convert_field(field: &RecordField, field_id: &mut i32) -> Result<Field, AvroToArrowSchemaError> {
    let name = &field.name;
    let (data_type, nullable) = convert_schema_type(field_id, &field.schema)?;

    let mut metadata = HashMap::new();
    metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
    *field_id += 1;

    Ok(Field::new(name, data_type, nullable).with_metadata(metadata))
}

fn convert_schema_type(
    field_id: &mut i32,
    schema: &AvroSchema,
) -> Result<(DataType, bool), AvroToArrowSchemaError> {
    match schema {
        AvroSchema::Null => Ok((DataType::Null, true)),
        AvroSchema::Boolean => Ok((DataType::Boolean, false)),
        AvroSchema::Int => Ok((DataType::Int32, false)),
        AvroSchema::Long => Ok((DataType::Int64, false)),
        AvroSchema::Float => Ok((DataType::Float32, false)),
        AvroSchema::Double => Ok((DataType::Float64, false)),
        AvroSchema::Bytes => Ok((DataType::Binary, false)),
        AvroSchema::String => Ok((DataType::Utf8, false)),
        AvroSchema::Array(item_schema) => {
            let (item_type, item_nullable) = convert_schema_type(field_id, &item_schema.items)?;
            let mut list_metadata = HashMap::new();
            list_metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            let list_field =
                Field::new("item", item_type, item_nullable).with_metadata(list_metadata);
            Ok((DataType::List(Arc::new(list_field)), false))
        }
        AvroSchema::Map(value_schema) => {
            let (value_type, value_nullable) = convert_schema_type(field_id, &value_schema.types)?;
            // Represent map as array of structs with key and value fields
            let mut key_metadata = HashMap::new();
            key_metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            let key_field = Field::new("key", DataType::Utf8, false).with_metadata(key_metadata);
            let mut value_metadata = HashMap::new();
            value_metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            let value_field =
                Field::new("value", value_type, value_nullable).with_metadata(value_metadata);
            let mut struct_metadata = HashMap::new();
            struct_metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            let struct_field = Field::new(
                "entries",
                DataType::Struct(vec![key_field, value_field].into()),
                false,
            )
            .with_metadata(struct_metadata);
            Ok((DataType::List(Arc::new(struct_field)), false))
        }
        AvroSchema::Union(union_schema) => {
            // Handle nullable unions (null + another type)
            if union_schema.variants().len() == 2 {
                let mut non_null_schema = None;
                let mut has_null = false;

                for variant in union_schema.variants() {
                    if matches!(variant, AvroSchema::Null) {
                        has_null = true;
                    } else if non_null_schema.is_none() {
                        non_null_schema = Some(variant);
                    } else {
                        // Complex union, not supported
                        return Err(AvroToArrowSchemaError::UnsupportedSchemaType(
                            "Complex unions are not supported".to_string(),
                        ));
                    }
                }

                if has_null && non_null_schema.is_some() {
                    let (data_type, _) = convert_schema_type(field_id, non_null_schema.unwrap())?;
                    Ok((data_type, true))
                } else {
                    Err(AvroToArrowSchemaError::UnsupportedSchemaType(
                        "Unsupported union type".to_string(),
                    ))
                }
            } else {
                Err(AvroToArrowSchemaError::UnsupportedSchemaType(
                    "Complex unions are not supported".to_string(),
                ))
            }
        }
        AvroSchema::Record(record_schema) => {
            let mut struct_fields = Vec::with_capacity(record_schema.fields.len());

            for field in &record_schema.fields {
                let arrow_field = convert_field(field, field_id)?;
                struct_fields.push(arrow_field);
            }

            Ok((DataType::Struct(struct_fields.into()), false))
        }
        AvroSchema::Fixed(fixed_schema) => {
            Ok((DataType::FixedSizeBinary(fixed_schema.size as i32), false))
        }
        _ => Err(AvroToArrowSchemaError::UnsupportedSchemaType(format!(
            "Unsupported Avro schema type: {schema:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value as AvroValue;

    fn make_avro_record() -> AvroValue {
        AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Int(42)),
            (
                "name".to_string(),
                AvroValue::String("moonlink".to_string()),
            ),
            ("is_active".to_string(), AvroValue::Boolean(true)),
            ("score".to_string(), AvroValue::Double(95.5)),
        ])
    }

    #[test]
    fn test_successful_conversion() {
        let avro_record = make_avro_record();

        let row = AvroToMoonlinkRowConverter::convert(&avro_record).unwrap();
        assert_eq!(row.values.len(), 4);
        assert_eq!(row.values[0], RowValue::Int32(42));
        assert_eq!(row.values[1], RowValue::ByteArray(b"moonlink".to_vec()));
        assert_eq!(row.values[2], RowValue::Bool(true));
        assert_eq!(row.values[3], RowValue::Float64(95.5));
    }

    #[test]
    fn test_array_conversion() {
        let avro_record = AvroValue::Record(vec![(
            "tags".to_string(),
            AvroValue::Array(vec![
                AvroValue::String("tag1".to_string()),
                AvroValue::String("tag2".to_string()),
                AvroValue::String("tag3".to_string()),
            ]),
        )]);

        let row = AvroToMoonlinkRowConverter::convert(&avro_record).unwrap();
        assert_eq!(row.values.len(), 1);
        if let RowValue::Array(arr) = &row.values[0] {
            assert_eq!(arr.len(), 3);
            assert_eq!(arr[0], RowValue::ByteArray(b"tag1".to_vec()));
            assert_eq!(arr[1], RowValue::ByteArray(b"tag2".to_vec()));
            assert_eq!(arr[2], RowValue::ByteArray(b"tag3".to_vec()));
        } else {
            panic!("Expected array value");
        }
    }

    #[test]
    fn test_struct_conversion() {
        let avro_record = AvroValue::Record(vec![(
            "user".to_string(),
            AvroValue::Record(vec![
                ("id".to_string(), AvroValue::Int(123)),
                ("name".to_string(), AvroValue::String("alice".to_string())),
            ]),
        )]);

        let row = AvroToMoonlinkRowConverter::convert(&avro_record).unwrap();
        assert_eq!(row.values.len(), 1);
        if let RowValue::Struct(struct_vals) = &row.values[0] {
            assert_eq!(struct_vals.len(), 2);
            assert_eq!(struct_vals[0], RowValue::Int32(123));
            assert_eq!(struct_vals[1], RowValue::ByteArray(b"alice".to_vec()));
        } else {
            panic!("Expected struct value");
        }
    }

    #[test]
    fn test_avro_to_arrow_schema_conversion() {
        let avro_schema_str = r#"
        {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"]},
                {"name": "age", "type": ["null", "int"]}
            ]
        }
        "#;

        let avro_schema = apache_avro::Schema::parse_str(avro_schema_str).unwrap();
        let arrow_schema = convert_avro_to_arrow_schema(&avro_schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 4);

        let fields = arrow_schema.fields();
        assert_eq!(fields[0].name(), "id");
        assert_eq!(fields[0].data_type(), &DataType::Int64);
        assert!(!fields[0].is_nullable());

        assert_eq!(fields[1].name(), "name");
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
        assert!(!fields[1].is_nullable());

        assert_eq!(fields[2].name(), "email");
        assert_eq!(fields[2].data_type(), &DataType::Utf8);
        assert!(fields[2].is_nullable());

        assert_eq!(fields[3].name(), "age");
        assert_eq!(fields[3].data_type(), &DataType::Int32);
        assert!(fields[3].is_nullable());
    }

    #[test]
    fn test_avro_array_schema_conversion() {
        let avro_schema_str = r#"
        {
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "order_id", "type": "string"},
                {"name": "items", "type": {"type": "array", "items": "string"}}
            ]
        }
        "#;

        let avro_schema = apache_avro::Schema::parse_str(avro_schema_str).unwrap();
        let arrow_schema = convert_avro_to_arrow_schema(&avro_schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);

        let fields = arrow_schema.fields();
        assert_eq!(fields[0].name(), "order_id");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);

        assert_eq!(fields[1].name(), "items");
        if let DataType::List(list_field) = fields[1].data_type() {
            assert_eq!(list_field.name(), "item");
            assert_eq!(list_field.data_type(), &DataType::Utf8);
        } else {
            panic!("Expected List type for items field");
        }
    }

    #[test]
    fn test_null_value() {
        let avro_record = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Int(1)),
            ("optional_field".to_string(), AvroValue::Null),
        ]);

        let row = AvroToMoonlinkRowConverter::convert(&avro_record).unwrap();
        assert_eq!(row.values.len(), 2);
        assert_eq!(row.values[0], RowValue::Int32(1));
        assert_eq!(row.values[1], RowValue::Null);
    }
}

#[test]
fn test_complex_avro_schema_with_maps() {
    let avro_schema_str = r#"{
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
                {"name": "age", "type": "int"},
                {"name": "metadata", "type": {"type": "map", "values": "string"}},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "profile", "type": {
                    "type": "record",
                    "name": "Profile",
                    "fields": [
                        {"name": "bio", "type": "string"},
                        {"name": "location", "type": "string"}
                    ]
                }}
            ]
        }"#;

    let avro_schema = apache_avro::Schema::parse_str(avro_schema_str).unwrap();
    let arrow_schema = convert_avro_to_arrow_schema(&avro_schema).unwrap();

    assert_eq!(arrow_schema.fields().len(), 7);

    let fields = arrow_schema.fields();
    assert_eq!(fields[0].name(), "id");
    assert_eq!(fields[0].data_type(), &DataType::Int32);

    assert_eq!(fields[1].name(), "name");
    assert_eq!(fields[1].data_type(), &DataType::Utf8);

    assert_eq!(fields[4].name(), "metadata");
    // Map should be converted to List of Struct type
    if let DataType::List(list_field) = fields[4].data_type() {
        assert_eq!(list_field.name(), "entries");
        if let DataType::Struct(struct_fields) = list_field.data_type() {
            assert_eq!(struct_fields.len(), 2);
            assert_eq!(struct_fields[0].name(), "key");
            assert_eq!(struct_fields[0].data_type(), &DataType::Utf8);
            assert_eq!(struct_fields[1].name(), "value");
            assert_eq!(struct_fields[1].data_type(), &DataType::Utf8);
        } else {
            panic!("Expected Struct type for map entries");
        }
    } else {
        panic!("Expected List type for metadata field (map converted to list)");
    }

    assert_eq!(fields[5].name(), "tags");
    if let DataType::List(list_field) = fields[5].data_type() {
        assert_eq!(list_field.name(), "item");
        assert_eq!(list_field.data_type(), &DataType::Utf8);
    } else {
        panic!("Expected List type for tags field");
    }

    assert_eq!(fields[6].name(), "profile");
    if let DataType::Struct(struct_fields) = fields[6].data_type() {
        assert_eq!(struct_fields.len(), 2);
        assert_eq!(struct_fields[0].name(), "bio");
        assert_eq!(struct_fields[0].data_type(), &DataType::Utf8);
        assert_eq!(struct_fields[1].name(), "location");
        assert_eq!(struct_fields[1].data_type(), &DataType::Utf8);
    } else {
        panic!("Expected Struct type for profile field");
    }
}
