use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldSchema {
    pub name: String,
    pub data_type: String, // case insensitive
    pub nullable: bool,
    #[serde(default)]
    pub fields: Option<Vec<FieldSchema>>, // for struct
    #[serde(default)]
    pub item: Option<Box<FieldSchema>>, // for list/array
}

#[derive(thiserror::Error, Debug)]
pub enum SchemaBuildError {
    #[error("invalid decimal type: {0}")]
    InvalidDecimal(String),
    #[error("invalid schema: {0}")]
    InvalidSchema(String),
    #[error("unsupported data type: {0}")]
    UnsupportedType(String),
}

struct DecimalType {
    precision: u8,
    scale: i8,
}

/// Parse a decimal type string in the form of
///   - decimal(precision)
///   - decimal(precision, scale)
fn parse_decimal(data_type_str: &str) -> Result<DecimalType, SchemaBuildError> {
    let inner = &data_type_str[8..data_type_str.len() - 1];
    let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    // Arrow type allows no "scale", which defaults to 0.
    if parts.len() == 1 {
        let precision: u8 = parts[0]
            .parse()
            .map_err(|_| SchemaBuildError::InvalidDecimal(data_type_str.to_string()))?;
        Ok(DecimalType {
            precision,
            scale: 0,
        })
    } else if parts.len() == 2 {
        // decimal(precision,
        let precision: u8 = parts[0]
            .parse()
            .map_err(|_| SchemaBuildError::InvalidDecimal(data_type_str.to_string()))?;
        let scale: i8 = parts[1]
            .parse()
            .map_err(|_| SchemaBuildError::InvalidDecimal(data_type_str.to_string()))?;
        Ok(DecimalType { precision, scale })
    } else {
        Err(SchemaBuildError::InvalidDecimal(data_type_str.to_string()))
    }
}

/// Build an Arrow `Field` from a `FieldSchema`.
///
/// Returns an Arrow field and mutates the `field_id` as a side effect
/// Returns an error if the field schema is invalid.
fn build_field_from_schema(
    field_schema: &FieldSchema,
    override_name: Option<&str>,
    field_id: &mut i32,
) -> Result<Field, SchemaBuildError> {
    let name: String = override_name
        .map(|s| s.to_string())
        .unwrap_or_else(|| field_schema.name.clone());
    let nullable: bool = field_schema.nullable;
    let data_type_str = field_schema.data_type.to_lowercase();
    match data_type_str.as_str() {
        "int16" => {
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            Ok(Field::new(&name, DataType::Int16, nullable).with_metadata(metadata))
        }
        "int32" => {
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            Ok(Field::new(&name, DataType::Int32, nullable).with_metadata(metadata))
        }
        "int64" => {
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            Ok(Field::new(&name, DataType::Int64, nullable).with_metadata(metadata))
        }
        "string" | "text" => {
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            Ok(Field::new(&name, DataType::Utf8, nullable).with_metadata(metadata))
        }
        "boolean" | "bool" => {
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            Ok(Field::new(&name, DataType::Boolean, nullable).with_metadata(metadata))
        }
        "float32" => {
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            Ok(Field::new(&name, DataType::Float32, nullable).with_metadata(metadata))
        }
        "float64" => {
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            Ok(Field::new(&name, DataType::Float64, nullable).with_metadata(metadata))
        }
        "date32" => {
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            Ok(Field::new(&name, DataType::Date32, nullable).with_metadata(metadata))
        }
        // Decimal type: decimal(precision[, scale])
        dt if dt.starts_with("decimal(") && dt.ends_with(')') => {
            let DecimalType { precision, scale } = parse_decimal(&data_type_str)?;
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            Ok(
                Field::new(&name, DataType::Decimal128(precision, scale), nullable)
                    .with_metadata(metadata),
            )
        }
        // Struct type: { data_type: "struct", fields: [...] }
        "struct" => {
            let child_schemas = field_schema.fields.as_ref().ok_or_else(|| {
                SchemaBuildError::InvalidSchema(format!(
                    "Missing 'fields' for struct '{}'",
                    field_schema.name
                ))
            })?;
            let children: Result<Vec<Field>, SchemaBuildError> = child_schemas
                .iter()
                .map(|child| build_field_from_schema(child, /*override_name=*/ None, field_id))
                .collect();
            let children = children?;
            let field = Field::new_struct(&name, children, nullable);
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            Ok(field.with_metadata(metadata))
        }
        // List type: { data_type: "list", item: { ... } }
        "list" | "array" => {
            let item_schema = field_schema.item.as_ref().ok_or_else(|| {
                SchemaBuildError::InvalidSchema(format!(
                    "Missing 'item' for list '{}'",
                    field_schema.name
                ))
            })?;

            if matches!(item_schema.data_type.as_str(), "list" | "array" | "struct") {
                return Err(SchemaBuildError::UnsupportedType(format!(
                    "Invalid 'item' for list '{}': list/array/struct is not supported as a list item",
                    field_schema.name
                )));
            }
            // We override the field as "item" to indicate it is the item of the list.
            let item_field = build_field_from_schema(
                item_schema,
                /*override_name=*/ Some("item"),
                field_id,
            )?;
            let list_type = DataType::List(std::sync::Arc::new(item_field));
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            Ok(Field::new(&name, list_type, nullable).with_metadata(metadata))
        }
        other => Err(SchemaBuildError::UnsupportedType(other.to_string())),
    }
}

pub fn build_arrow_schema_impl(fields: &[FieldSchema]) -> Result<Schema, SchemaBuildError> {
    let mut field_id: i32 = 0;
    let built_fields: Result<Vec<Field>, SchemaBuildError> = fields
        .iter()
        .map(|fs| build_field_from_schema(fs, /*override_name=*/ None, &mut field_id))
        .collect();
    Ok(Schema::new(built_fields?))
}

pub fn build_arrow_schema(fields: &[FieldSchema]) -> crate::Result<Schema> {
    build_arrow_schema_impl(fields).map_err(crate::Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_simple_schema() {
        let fields = vec![FieldSchema {
            name: "id".into(),
            data_type: "int32".into(),
            nullable: false,
            fields: None,
            item: None,
        }];
        let schema = build_arrow_schema_impl(&fields).unwrap();
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.fields[0].name(), "id");
    }

    #[test]
    fn test_build_struct_and_list_schema() {
        let fields = vec![
            FieldSchema {
                name: "id".into(),
                data_type: "int32".into(),
                nullable: false,
                fields: None,
                item: None,
            },
            FieldSchema {
                name: "props".into(),
                data_type: "struct".into(),
                nullable: true,
                fields: Some(vec![
                    FieldSchema {
                        name: "score".into(),
                        data_type: "float64".into(),
                        nullable: true,
                        fields: None,
                        item: None,
                    },
                    FieldSchema {
                        name: "labels".into(),
                        data_type: "list".into(),
                        nullable: true,
                        fields: None,
                        item: Some(Box::new(FieldSchema {
                            name: "label".into(),
                            data_type: "string".into(),
                            nullable: true,
                            fields: None,
                            item: None,
                        })),
                    },
                ]),
                item: None,
            },
            FieldSchema {
                name: "history".into(),
                data_type: "list".into(),
                nullable: true,
                fields: None,
                item: Some(Box::new(FieldSchema {
                    name: "ts".into(),
                    data_type: "int64".into(),
                    nullable: true,
                    fields: None,
                    item: None,
                })),
            },
        ];
        let schema = build_arrow_schema_impl(&fields).unwrap();
        assert_eq!(schema.fields.len(), 3);
    }

    #[test]
    fn test_build_nested_lists_rejected() {
        let fields = vec![FieldSchema {
            name: "matrix".into(),
            data_type: "array".into(),
            nullable: true,
            fields: None,
            item: Some(Box::new(FieldSchema {
                name: "row".into(),
                data_type: "list".into(),
                nullable: true,
                fields: None,
                item: Some(Box::new(FieldSchema {
                    name: "cell".into(),
                    data_type: "int32".into(),
                    nullable: true,
                    fields: None,
                    item: None,
                })),
            })),
        }];
        let err = build_arrow_schema_impl(&fields).unwrap_err();
        match err {
            SchemaBuildError::UnsupportedType(_) => {}
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_invalid_struct_missing_fields() {
        let fields = vec![FieldSchema {
            name: "bad_struct".into(),
            data_type: "struct".into(),
            nullable: true,
            fields: None,
            item: None,
        }];
        let err = build_arrow_schema_impl(&fields).unwrap_err();
        match err {
            SchemaBuildError::InvalidSchema(_) => {}
            _ => panic!("unexpected error"),
        }
    }

    #[test]
    fn test_invalid_list_missing_item() {
        let fields = vec![FieldSchema {
            name: "bad_list".into(),
            data_type: "list".into(),
            nullable: true,
            fields: None,
            item: None,
        }];
        let err = build_arrow_schema_impl(&fields).unwrap_err();
        match err {
            SchemaBuildError::InvalidSchema(_) => {}
            _ => panic!("unexpected error"),
        }
    }

    #[test]
    fn test_decimal_parsing_variants() {
        let fields = vec![
            FieldSchema {
                name: "d1".into(),
                data_type: "decimal(10,2)".into(),
                nullable: false,
                fields: None,
                item: None,
            },
            FieldSchema {
                name: "d2".into(),
                data_type: "decimal(10)".into(),
                nullable: false,
                fields: None,
                item: None,
            },
            FieldSchema {
                name: "d3".into(),
                data_type: "Decimal(10,0)".into(),
                nullable: false,
                fields: None,
                item: None,
            },
        ];
        let schema = build_arrow_schema_impl(&fields).unwrap();
        assert_eq!(schema.fields.len(), 3);
    }
}
