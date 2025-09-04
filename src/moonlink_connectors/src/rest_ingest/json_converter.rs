use crate::rest_ingest::datetime_utils::{parse_date, parse_time, parse_timestamp_with_timezone};
use crate::rest_ingest::decimal_utils::convert_decimal_to_row_value;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use moonlink::row::{MoonlinkRow, RowValue};
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum JsonToMoonlinkRowError {
    #[error("missing field: {0}")]
    MissingField(String),
    #[error("type mismatch for field: {0}")]
    TypeMismatch(String),
    #[error("invalid value for field: {0}")]
    InvalidValue(String),
    #[error("serde json error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("Unsupported data type {0} in field {1}")]
    UnsupportedDataType(String, String),
    #[error("invalid value for field: {0} with cause: {1}")]
    InvalidValueWithCause(String, Box<dyn std::error::Error + Send + Sync>),
}

pub struct JsonToMoonlinkRowConverter {
    schema: Arc<Schema>,
}

impl JsonToMoonlinkRowConverter {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }

    pub fn convert(&self, json: &Value) -> Result<MoonlinkRow, JsonToMoonlinkRowError> {
        let mut values = Vec::with_capacity(self.schema.fields.len());
        for field in &self.schema.fields {
            let field_name = field.name();
            let value = json
                .get(field_name)
                .ok_or_else(|| JsonToMoonlinkRowError::MissingField(field_name.clone()))?;
            let row_value = Self::convert_value(field, value)?;
            values.push(row_value);
        }
        Ok(MoonlinkRow::new(values))
    }

    fn convert_value(field: &Field, value: &Value) -> Result<RowValue, JsonToMoonlinkRowError> {
        match field.data_type() {
            DataType::Boolean => {
                if let Some(b) = value.as_bool() {
                    Ok(RowValue::Bool(b))
                } else if value.is_null() && field.is_nullable() {
                    Ok(RowValue::Null)
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Int32 => {
                if let Some(i) = value.as_i64() {
                    Ok(RowValue::Int32(i as i32))
                } else if value.is_null() && field.is_nullable() {
                    Ok(RowValue::Null)
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Int64 => {
                if let Some(i) = value.as_i64() {
                    Ok(RowValue::Int64(i))
                } else if value.is_null() && field.is_nullable() {
                    Ok(RowValue::Null)
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Float32 => {
                if let Some(f) = value.as_f64() {
                    Ok(RowValue::Float32(f as f32))
                } else if value.is_null() && field.is_nullable() {
                    Ok(RowValue::Null)
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Float64 => {
                if let Some(f) = value.as_f64() {
                    Ok(RowValue::Float64(f))
                } else if value.is_null() && field.is_nullable() {
                    Ok(RowValue::Null)
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Date32 => {
                if let Some(s) = value.as_str() {
                    parse_date(s)
                        .map_err(|_| JsonToMoonlinkRowError::InvalidValue(field.name().clone()))
                } else if value.is_null() && field.is_nullable() {
                    Ok(RowValue::Null)
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                if let Some(s) = value.as_str() {
                    parse_time(s)
                        .map_err(|_| JsonToMoonlinkRowError::InvalidValue(field.name().clone()))
                } else if value.is_null() && field.is_nullable() {
                    Ok(RowValue::Null)
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                if let Some(s) = value.as_str() {
                    parse_timestamp_with_timezone(s, tz.as_deref())
                        .map_err(|_| JsonToMoonlinkRowError::InvalidValue(field.name().clone()))
                } else if value.is_null() && field.is_nullable() {
                    Ok(RowValue::Null)
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Utf8 => {
                if let Some(s) = value.as_str() {
                    Ok(RowValue::ByteArray(s.as_bytes().to_vec()))
                } else if value.is_null() && field.is_nullable() {
                    Ok(RowValue::Null)
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Decimal128(precision, scale) => {
                if let Some(s) = value.as_str() {
                    convert_decimal_to_row_value(s, *precision, *scale).map_err(|e| {
                        JsonToMoonlinkRowError::InvalidValueWithCause(
                            field.name().clone(),
                            Box::new(e),
                        )
                    })
                } else if value.is_null() && field.is_nullable() {
                    Ok(RowValue::Null)
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Decimal256(_precision, _scale) => {
                Err(JsonToMoonlinkRowError::UnsupportedDataType(
                    "Decimal256".to_string(),
                    field.name().clone(),
                ))
            }
            DataType::List(child_field) => {
                if let Some(array) = value.as_array() {
                    let mut converted_elements = Vec::with_capacity(array.len());
                    for (index, ele) in array.iter().enumerate() {
                        let converted_element =
                            Self::convert_value(child_field, ele).map_err(|e| {
                                match e {
                                    JsonToMoonlinkRowError::TypeMismatch(existing_path) => {
                                        // Transform error to include full path with index
                                        // (e.g., "int_list.item[1]", "nested_list.item[1].item[0]")
                                        let full_path = format!(
                                            "{}.{}",
                                            field.name(),
                                            existing_path.replacen(
                                                child_field.name(),
                                                &format!("{}[{}]", child_field.name(), index),
                                                1
                                            )
                                        );
                                        JsonToMoonlinkRowError::TypeMismatch(full_path)
                                    }
                                    other => other,
                                }
                            })?;
                        converted_elements.push(converted_element);
                    }
                    Ok(RowValue::Array(converted_elements))
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Struct(child_fields) => {
                if let Some(obj) = value.as_object() {
                    let mut values = Vec::with_capacity(child_fields.len());
                    for child_field in child_fields {
                        let child_name = child_field.name();
                        let child_value = match obj.get(child_name) {
                            Some(v) => v,
                            None => {
                                if !child_field.is_nullable() {
                                    return Err(JsonToMoonlinkRowError::MissingField(format!(
                                        "{}.{}",
                                        field.name(),
                                        child_name
                                    )));
                                }
                                values.push(RowValue::Null);
                                continue;
                            }
                        };
                        let converted =
                            Self::convert_value(child_field, child_value).map_err(|e| {
                                match e {
                                    JsonToMoonlinkRowError::TypeMismatch(existing_path) => {
                                        // Prepend parent struct name for clarity
                                        JsonToMoonlinkRowError::TypeMismatch(format!(
                                            "{}.{}",
                                            field.name(),
                                            existing_path
                                        ))
                                    }
                                    other => other,
                                }
                            })?;
                        values.push(converted);
                    }
                    Ok(RowValue::Struct(values))
                } else if value.is_null() && field.is_nullable() {
                    Ok(RowValue::Null)
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            _ => Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rest_ingest::decimal_utils::DecimalConversionError;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use bigdecimal::num_bigint::TryFromBigIntError;
    use bigdecimal::ParseBigDecimalError::ParseInt;
    use chrono::{NaiveDate, TimeZone, Utc};
    use serde_json::json;
    use std::sync::Arc;

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, /*nullable=*/ true),
            Field::new("name", DataType::Utf8, /*nullable=*/ true),
            Field::new("is_active", DataType::Boolean, /*nullable=*/ true),
            Field::new("score", DataType::Float64, /*nullable=*/ true),
            Field::new("id_int64", DataType::Int64, /*nullable=*/ true),
            Field::new("score_float32", DataType::Float32, /*nullable=*/ true),
            Field::new(
                "decimal128",
                DataType::Decimal128(5, 2),
                /*nullable=*/ true,
            ),
        ]))
    }

    fn make_schema_with_decimal128_overflow() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "decimal128",
            DataType::Decimal128(40, 3),
            false,
        )]))
    }

    fn make_schema_with_decimal256() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "decimal256",
            DataType::Decimal256(38, 10),
            false,
        )]))
    }

    fn make_schema_with_negative_scale() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "decimal_field",
            DataType::Decimal128(2, -3),
            false,
        )]))
    }

    fn make_schema_with_fractional_only() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "decimal_field",
            DataType::Decimal128(3, 5),
            false,
        )]))
    }

    fn make_datetime_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, /*nullable=*/ false),
            Field::new(
                "time",
                DataType::Time64(TimeUnit::Microsecond),
                /*nullable=*/ false,
            ),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                /*nullable=*/ false,
            ),
            Field::new(
                "timestamp_utc",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                /*nullable=*/ false,
            ),
        ]))
    }

    fn make_list_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "int_list",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                false,
            ),
            Field::new(
                "string_list",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                false,
            ),
            Field::new(
                "bool_list",
                DataType::List(Arc::new(Field::new("item", DataType::Boolean, false))),
                false,
            ),
            Field::new(
                "float_list",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
                false,
            ),
        ]))
    }

    fn make_nested_list_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "nested_list",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                true,
            ))),
            false,
        )]))
    }

    fn make_struct_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "user",
            DataType::Struct(arrow_schema::Fields::from(vec![
                Field::new("id", DataType::Int32, /*nullable=*/ false),
                Field::new("name", DataType::Utf8, /*nullable=*/ false),
                Field::new("is_active", DataType::Boolean, /*nullable=*/ true),
            ])),
            /*nullable=*/ false,
        )]))
    }

    fn make_list_with_nullable_elements_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "int_list_nullable",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int32,
                /*nullable=*/ true,
            ))),
            /*nullable=*/ false,
        )]))
    }

    #[test]
    fn test_successful_conversion() {
        let schema = make_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "id": 42,
            "name": "moonlink",
            "is_active": true,
            "score": 100.0,
            "id_int64": 123,
            "score_float32": 100.0,
            "decimal128": "123.45",
        });
        let row = converter.convert(&input).unwrap();
        assert_eq!(row.values.len(), 7);
        assert_eq!(row.values[0], RowValue::Int32(42));
        assert_eq!(row.values[1], RowValue::ByteArray(b"moonlink".to_vec()));
        assert_eq!(row.values[2], RowValue::Bool(true));
        assert_eq!(row.values[3], RowValue::Float64(100.0));
        assert_eq!(row.values[4], RowValue::Int64(123));
        assert_eq!(row.values[5], RowValue::Float32(100.0));
        assert_eq!(row.values[6], RowValue::Decimal(12345));
    }

    #[test]
    fn test_conversion_with_null() {
        let schema = make_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "id": null,
            "name": null,
            "is_active": null,
            "score": null,
            "id_int64": null,
            "score_float32": null,
            "decimal128": null
        });
        let row = converter.convert(&input).unwrap();
        assert_eq!(row.values.len(), 7);
        assert_eq!(row.values[0], RowValue::Null);
        assert_eq!(row.values[1], RowValue::Null);
        assert_eq!(row.values[2], RowValue::Null);
        assert_eq!(row.values[3], RowValue::Null);
        assert_eq!(row.values[4], RowValue::Null);
        assert_eq!(row.values[5], RowValue::Null);
        assert_eq!(row.values[6], RowValue::Null);
    }

    #[test]
    fn test_missing_field() {
        let schema = make_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "id": 1
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::MissingField(f) => assert_eq!(f, "name"),
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_type_mismatch() {
        let schema = make_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "id": "not_an_int",
            "name": "moonlink",
            "is_active": true,
            "score": 100.0,
            "id_int64": 123,
            "score_float32": 100.0,
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::TypeMismatch(f) => assert_eq!(f, "id"),
            _ => panic!("unexpected error: {err:?}"),
        }
        let input = json!({
            "is_active": "true",
            "name": "moonlink",
            "score": 100.0,
            "id_int64": 123,
            "score_float32": 100.0,
            "id": 1,
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::TypeMismatch(f) => assert_eq!(f, "is_active"),
            _ => panic!("unexpected error: {err:?}"),
        }
        let input = json!({
            "score": "not_a_float",
            "name": "moonlink",
            "is_active": true,
            "id_int64": 123,
            "score_float32": 100.0,
            "id": 1,
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::TypeMismatch(f) => assert_eq!(f, "score"),
            _ => panic!("unexpected error: {err:?}"),
        }
        let input = json!({
            "score_float32": "not_a_float",
            "name": "moonlink",
            "is_active": true,
            "id_int64": 123,
            "score": 100.0,
            "id": 1,
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::TypeMismatch(f) => assert_eq!(f, "score_float32"),
            _ => panic!("unexpected error: {err:?}"),
        }
        let input = json!({
            "id_int64": "not_an_int",
            "name": "moonlink",
            "is_active": true,
            "score": 100.0,
            "score_float32": 100.0,
            "id": 1,
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::TypeMismatch(f) => assert_eq!(f, "id_int64"),
            _ => panic!("unexpected error: {err:?}"),
        }

        let input = json!({
            "id": 42,
            "name": "moonlink",
            "is_active": true,
            "score": 100.0,
            "id_int64": 123,
            "score_float32": 100.0,
            "decimal128": 123.45, // number instead of string
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::TypeMismatch(f) => assert_eq!(f, "decimal128"),
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    /* Test to ensure that decimal conversion fails when precision is out of range
     * decimal128: Decimal128(precision=8, scale=2)
     * "1234567.89" => digits=9 (> precision=8), scale=2 (OK) → violates precision only
     */
    fn test_decimal_conversion_precision_out_of_range() {
        let schema = make_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "id": 42,
            "name": "moonlink",
            "is_active": true,
            "score": 100.0,
            "id_int64": 123,
            "score_float32": 100.0,
            "decimal128": "12333.456",
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::InvalidValueWithCause(f, e) => {
                assert_eq!(f, "decimal128");
                let decimal_conversion_err = e
                    .downcast_ref::<DecimalConversionError>()
                    .expect("Expected DecimalConversionError, got different error type");

                match decimal_conversion_err {
                    DecimalConversionError::PrecisionOutOfRange {
                        value,
                        expected_precision,
                        actual_precision,
                    } => {
                        assert_eq!(*value, "12333.456");
                        assert_eq!(*expected_precision, 5);
                        assert_eq!(*actual_precision, 8);
                    }
                    _ => panic!("Expected PrecisionOutOfRange, but got another DecimalConversionError: {decimal_conversion_err:?}"),
                }
            }
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_decimal_conversion_integer_part_out_of_range_error() {
        let schema = make_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "id": 42,
            "name": "moonlink",
            "is_active": true,
            "score": 100.0,
            "id_int64": 123,
            "score_float32": 100.0,
            "decimal128": "1235.4",
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::InvalidValueWithCause(f, e) => {
                assert_eq!(f, "decimal128");
                let decimal_conversion_err = e
                    .downcast_ref::<DecimalConversionError>()
                    .expect("Expected DecimalConversionError, got different error type");

                match decimal_conversion_err {
                    DecimalConversionError::IntegerPartOutOfRange {
                        value,
                        expected_len,
                        actual_len,
                    } => {
                        assert_eq!(*value, "1235.4");
                        assert_eq!(*expected_len, 3);
                        assert_eq!(*actual_len, 4);
                    }
                    _ => panic!("Expected IntegerPartOutOfRange, but got another DecimalConversionError: {decimal_conversion_err:?}"),
                }
            }
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_decimal_conversion_overflow() {
        let schema = make_schema_with_decimal128_overflow();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "decimal128": "1234567890123456789012345678901234567.789"
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::InvalidValueWithCause(f, e) => {
                assert_eq!(f, "decimal128");
                let decimal_conversion_err = e
                    .downcast_ref::<DecimalConversionError>()
                    .expect("Expected DecimalConversionError, got different error type");

                match decimal_conversion_err {
                    DecimalConversionError::Overflow { mantissa, error } => {
                        assert_eq!(mantissa, "1234567890123456789012345678901234567789");
                        assert!(error.is::<TryFromBigIntError<()>>());
                    }
                    _ => panic!(
                        "Expected Overflow, but got another DecimalConversionError: {decimal_conversion_err:?}"
                    ),
                }
            }
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_decimal_invalid_value() {
        let schema = make_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "id": 42,
            "name": "moonlink",
            "is_active": true,
            "score": 100.0,
            "id_int64": 123,
            "score_float32": 100.0,
            "decimal128": "not_a_decimal",
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::InvalidValueWithCause(f, e) => {
                assert_eq!(f, "decimal128");
                let decimal_conversion_err = e
                    .downcast_ref::<DecimalConversionError>()
                    .expect("Expected DecimalConversionError, got different error type");

                match decimal_conversion_err {
                    DecimalConversionError::InvalidValue { value, error } => {
                        assert_eq!(*value, "not_a_decimal");
                        let parse_big_decimal_err = error
                            .downcast_ref::<bigdecimal::ParseBigDecimalError>()
                            .expect("Expected ParseBigDecimalError, got different error type");
                        assert!(matches!(parse_big_decimal_err, ParseInt { .. }));
                    }
                    _ => panic!(
                        "Expected InvalidValue, but got another DecimalConversionError: {decimal_conversion_err:?}"
                    ),
                }
            }
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_decimal_conversion_negative_scale_precision_out_of_range_error() {
        // Test IntegerPartOutOfRange error propagation for fractional only
        let schema = make_schema_with_negative_scale();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "decimal_field": "-990010" // Has integer part but scale > precision
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::InvalidValueWithCause(f, e) => {
                assert_eq!(f, "decimal_field");
                let decimal_conversion_err = e
                    .downcast_ref::<DecimalConversionError>()
                    .expect("Expected DecimalConversionError, got different error type");

                match decimal_conversion_err {
                    DecimalConversionError::PrecisionOutOfRange {
                        expected_precision,
                        actual_precision,
                        ..
                    } => {
                        assert_eq!(*expected_precision, 2);
                        assert_eq!(*actual_precision, 6);
                    }
                    _ => {
                        panic!("Expected PrecisionOutOfRange error, got {decimal_conversion_err:?}")
                    }
                }
            }
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_decimal_conversion_fractional_only_integer_precision_out_of_range_error() {
        // Test InvalidValue error propagation
        let schema = make_schema_with_fractional_only();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "decimal_field": "0.12345"
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::InvalidValueWithCause(f, e) => {
                assert_eq!(f, "decimal_field");
                let decimal_conversion_err = e
                    .downcast_ref::<DecimalConversionError>()
                    .expect("Expected DecimalConversionError, got different error type");

                match decimal_conversion_err {
                    DecimalConversionError::PrecisionOutOfRange {
                        expected_precision,
                        actual_precision,
                        ..
                    } => {
                        assert_eq!(*expected_precision, 3);
                        assert_eq!(*actual_precision, 5);
                    }
                    _ => panic!("Expected InvalidValue, got {decimal_conversion_err:?}"),
                }
            }
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_decimal_256_not_supported() {
        let schema = make_schema_with_decimal256();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "decimal256": "9876.5432"
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::UnsupportedDataType(f, e) => {
                assert_eq!(f, "Decimal256");
                assert_eq!(e, "decimal256");
            }
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_date_conversion() {
        let schema = make_datetime_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);

        // Test valid date
        let input = json!({
            "date": "2024-03-15",
            "time": "12:00:00",
            "timestamp": "2024-03-15T12:00:00Z",
            "timestamp_utc": "2024-03-15T12:00:00Z",
        });
        let row = converter.convert(&input).unwrap();

        // Date: 2024-03-15 is 19797 days since 1970-01-01
        let expected_days = NaiveDate::from_ymd_opt(2024, 3, 15)
            .unwrap()
            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        assert_eq!(row.values[0], RowValue::Int32(expected_days));
    }

    #[test]
    fn test_time_conversion() {
        let schema = make_datetime_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);

        // Test time without fractional seconds
        let input = json!({
            "date": "2024-01-01",
            "time": "14:30:45",
            "timestamp": "2024-01-01T00:00:00Z",
            "timestamp_utc": "2024-01-01T00:00:00Z",
        });
        let row = converter.convert(&input).unwrap();

        // 14:30:45 = 14*3600 + 30*60 + 45 = 52245 seconds = 52245000000 microseconds
        assert_eq!(row.values[1], RowValue::Int64(52245000000));

        // Test time with fractional seconds
        let input = json!({
            "date": "2024-01-01",
            "time": "09:15:30.123456",
            "timestamp": "2024-01-01T00:00:00Z",
            "timestamp_utc": "2024-01-01T00:00:00Z",
        });
        let row = converter.convert(&input).unwrap();

        // 9:15:30.123456 = 33330123456 microseconds
        assert_eq!(row.values[1], RowValue::Int64(33330123456));
    }

    #[test]
    fn test_timestamp_conversion() {
        let schema = make_datetime_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);

        // Test UTC timestamp
        let input = json!({
            "date": "2024-01-01",
            "time": "00:00:00",
            "timestamp": "2024-03-15T10:30:45.123Z",
            "timestamp_utc": "2024-03-15T10:30:45.123Z",
        });
        let row = converter.convert(&input).unwrap();

        // Verify it's converted to microseconds since epoch
        // Using chrono to calculate the exact value
        let dt = Utc
            .with_ymd_and_hms(2024, 3, 15, 10, 30, 45)
            .unwrap()
            .timestamp_micros()
            + 123000;
        assert_eq!(row.values[2], RowValue::Int64(dt));
        assert_eq!(row.values[3], RowValue::Int64(dt));

        // Test timestamp with timezone offset (should be normalized to UTC)
        let input = json!({
            "date": "2024-01-01",
            "time": "00:00:00",
            "timestamp": "2024-03-15T10:30:45+05:00",
            "timestamp_utc": "2024-03-15T10:30:45-08:00",
        });
        let row = converter.convert(&input).unwrap();

        // Both should be normalized to UTC
        // 2024-03-15T10:30:45+05:00 -> 2024-03-15T05:30:45Z
        let expected_micros1 = Utc
            .with_ymd_and_hms(2024, 3, 15, 5, 30, 45)
            .unwrap()
            .timestamp_micros();
        // 2024-03-15T10:30:45-08:00 -> 2024-03-15T18:30:45Z
        let expected_micros2 = Utc
            .with_ymd_and_hms(2024, 3, 15, 18, 30, 45)
            .unwrap()
            .timestamp_micros();
        assert_eq!(row.values[2], RowValue::Int64(expected_micros1));
        assert_eq!(row.values[3], RowValue::Int64(expected_micros2));

        // Test timestamp without timezone (should be treated as UTC)
        let input = json!({
            "date": "2024-01-01",
            "time": "00:00:00",
            "timestamp": "2024-03-15T10:30:45",
            "timestamp_utc": "2024-03-15T10:30:45.123456",
        });
        let row = converter.convert(&input).unwrap();

        // Both should be treated as UTC
        let expected_micros3 = Utc
            .with_ymd_and_hms(2024, 3, 15, 10, 30, 45)
            .unwrap()
            .timestamp_micros();
        let expected_micros4 = Utc
            .with_ymd_and_hms(2024, 3, 15, 10, 30, 45)
            .unwrap()
            .timestamp_micros()
            + 123456;
        assert_eq!(row.values[2], RowValue::Int64(expected_micros3));
        assert_eq!(row.values[3], RowValue::Int64(expected_micros4));
    }

    #[test]
    fn test_timezone_aware_timestamp_conversion() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp_pst",
                DataType::Timestamp(TimeUnit::Microsecond, Some("America/Los_Angeles".into())),
                /*nullable=*/ false,
            ),
            Field::new(
                "timestamp_est",
                DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
                /*nullable=*/ false,
            ),
        ]));
        let converter = JsonToMoonlinkRowConverter::new(schema);

        // Test naive datetime interpreted in schema timezone
        // Use January 15 to avoid daylight saving time issues
        let input = json!({
            "timestamp_pst": "2024-01-15T10:30:45",
            "timestamp_est": "2024-01-15T10:30:45",
        });
        let row = converter.convert(&input).unwrap();

        // 2024-01-15T10:30:45 in PST (UTC-8) should be 2024-01-15T18:30:45 UTC
        let expected_pst = Utc
            .with_ymd_and_hms(2024, 1, 15, 18, 30, 45)
            .unwrap()
            .timestamp_micros();

        // 2024-01-15T10:30:45 in EST (UTC-5) should be 2024-01-15T15:30:45 UTC
        let expected_est = Utc
            .with_ymd_and_hms(2024, 1, 15, 15, 30, 45)
            .unwrap()
            .timestamp_micros();

        assert_eq!(row.values[0], RowValue::Int64(expected_pst));
        assert_eq!(row.values[1], RowValue::Int64(expected_est));

        // Test that explicit timezone in input still takes precedence
        let input = json!({
            "timestamp_pst": "2024-01-15T10:30:45Z", // Explicit UTC
            "timestamp_est": "2024-01-15T10:30:45+03:00", // Explicit +3
        });
        let row = converter.convert(&input).unwrap();

        // Explicit UTC should be 2024-01-15T10:30:45 UTC
        let expected_utc = Utc
            .with_ymd_and_hms(2024, 1, 15, 10, 30, 45)
            .unwrap()
            .timestamp_micros();

        // Explicit +3 should be 2024-01-15T07:30:45 UTC
        let expected_plus3 = Utc
            .with_ymd_and_hms(2024, 1, 15, 7, 30, 45)
            .unwrap()
            .timestamp_micros();

        assert_eq!(row.values[0], RowValue::Int64(expected_utc));
        assert_eq!(row.values[1], RowValue::Int64(expected_plus3));
    }

    #[test]
    fn test_invalid_date_time_formats() {
        let schema = make_datetime_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);

        // Invalid date format
        let input = json!({
            "date": "2024/03/15", // Wrong separator
            "time": "12:00:00",
            "timestamp": "2024-03-15T12:00:00Z",
            "timestamp_utc": "2024-03-15T12:00:00Z",
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::InvalidValue(f) => assert_eq!(f, "date"),
            _ => panic!("unexpected error: {err:?}"),
        }

        // Invalid time format
        let input = json!({
            "date": "2024-03-15",
            "time": "25:00:00", // Invalid hour
            "timestamp": "2024-03-15T12:00:00Z",
            "timestamp_utc": "2024-03-15T12:00:00Z",
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::InvalidValue(f) => assert_eq!(f, "time"),
            _ => panic!("unexpected error: {err:?}"),
        }

        // Invalid timestamp format
        let input = json!({
            "date": "2024-03-15",
            "time": "12:00:00",
            "timestamp": "2024-03-15 12:00:00", // Not RFC3339
            "timestamp_utc": "2024-03-15T12:00:00Z",
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::InvalidValue(f) => assert_eq!(f, "timestamp"),
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_datetime_edge_cases() {
        let schema = make_datetime_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);

        // Test epoch date
        let input = json!({
            "date": "1970-01-01",
            "time": "00:00:00",
            "timestamp": "1970-01-01T00:00:00Z",
            "timestamp_utc": "1970-01-01T00:00:00Z",
        });
        let row = converter.convert(&input).unwrap();
        assert_eq!(row.values[0], RowValue::Int32(0)); // 0 days since epoch
        assert_eq!(row.values[1], RowValue::Int64(0)); // 0 microseconds since midnight
        assert_eq!(row.values[2], RowValue::Int64(0)); // 0 microseconds since epoch
        assert_eq!(row.values[3], RowValue::Int64(0)); // 0 microseconds since epoch

        // Test leap year date
        let input = json!({
            "date": "2024-02-29",
            "time": "23:59:59.999999",
            "timestamp": "2024-02-29T23:59:59.999999Z",
            "timestamp_utc": "2024-02-29T23:59:59.999999Z",
        });
        let row = converter.convert(&input).unwrap();

        let expected_days = NaiveDate::from_ymd_opt(2024, 2, 29)
            .unwrap()
            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        assert_eq!(row.values[0], RowValue::Int32(expected_days));

        // 23:59:59.999999 = 86399999999 microseconds
        assert_eq!(row.values[1], RowValue::Int64(86399999999));

        // 2024-02-29T23:59:59.999999Z = 1709251199999999 microseconds since epoch
        assert_eq!(row.values[2], RowValue::Int64(1709251199999999));
        assert_eq!(row.values[3], RowValue::Int64(1709251199999999));
    }

    #[test]
    fn test_list_conversion_success() {
        let int_values = vec![1, 2, 3, 42];
        let string_values = vec!["hello", "world", "moonlink"];
        let bool_values = vec![true, false, true];
        let float_values = vec![1.1, 2.2, 3.3];

        let schema = make_list_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "int_list": int_values,
            "string_list": string_values,
            "bool_list": bool_values,
            "float_list": float_values
        });
        let row = converter.convert(&input).unwrap();
        assert_eq!(row.values.len(), 4);

        // Check int_list
        assert_eq!(
            row.values[0],
            RowValue::Array(int_values.into_iter().map(RowValue::Int32).collect())
        );

        // Check string_list
        assert_eq!(
            row.values[1],
            RowValue::Array(
                string_values
                    .into_iter()
                    .map(|s| RowValue::ByteArray(s.as_bytes().to_vec()))
                    .collect()
            )
        );

        // Check bool_list
        assert_eq!(
            row.values[2],
            RowValue::Array(bool_values.into_iter().map(RowValue::Bool).collect())
        );

        // Check float_list
        assert_eq!(
            row.values[3],
            RowValue::Array(float_values.into_iter().map(RowValue::Float64).collect())
        );
    }

    #[test]
    fn test_nested_list_conversion() {
        let nested_values = vec![vec![1, 2], vec![3, 4, 5], vec![]];

        let schema = make_nested_list_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "nested_list": nested_values
        });
        let row = converter.convert(&input).unwrap();
        assert_eq!(row.values.len(), 1);

        assert_eq!(
            row.values[0],
            RowValue::Array(
                nested_values
                    .into_iter()
                    .map(|inner_vec| RowValue::Array(
                        inner_vec.into_iter().map(RowValue::Int32).collect()
                    ))
                    .collect()
            )
        );
    }

    #[test]
    fn test_list_type_mismatch_non_array() {
        let schema = make_list_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "int_list": "not_an_array",
            "string_list": [],
            "bool_list": [],
            "float_list": []
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::TypeMismatch(f) => assert_eq!(f, "int_list"),
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_list_element_type_mismatch() {
        let schema = make_list_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "int_list": [1, "not_an_int", 3],
            "string_list": [],
            "bool_list": [],
            "float_list": []
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::TypeMismatch(f) => assert_eq!(f, "int_list.item[1]"),
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_nested_list_element_type_mismatch() {
        let schema = make_nested_list_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "nested_list": [[1, 2], [3, "not_an_int", 5], []]
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::TypeMismatch(f) => assert_eq!(f, "nested_list.item[1].item[1]"),
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_struct_parsing_success() {
        let schema = make_struct_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "user": {
                "id": 7,
                "name": "alice",
                "is_active": true
            }
        });
        let row = converter.convert(&input).unwrap();
        assert_eq!(row.values.len(), 1);
        assert_eq!(
            row.values[0],
            RowValue::Struct(vec![
                RowValue::Int32(7),
                RowValue::ByteArray(b"alice".to_vec()),
                RowValue::Bool(true),
            ])
        );
    }

    #[test]
    fn test_struct_missing_required_child() {
        let schema = make_struct_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "user": {
                "id": 7,
                // name is required but missing
                "is_active": true
            }
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::MissingField(f) => assert_eq!(f, "user.name"),
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_struct_missing_optional_child() {
        let schema = make_struct_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "user": {
                "id": 7,
                "name": "alice"
                // is_active is optional and missing
            }
        });
        let row = converter.convert(&input).unwrap();
        assert_eq!(row.values.len(), 1);
        assert_eq!(
            row.values[0],
            RowValue::Struct(vec![
                RowValue::Int32(7),
                RowValue::ByteArray(b"alice".to_vec()),
                RowValue::Null,
            ])
        );
    }

    #[test]
    fn test_struct_mismatched_types() {
        let schema = make_struct_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "user": {
                "id": "oops", // should be int
                "name": "alice",
                "is_active": true
            }
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::TypeMismatch(f) => assert_eq!(f, "user.id"),
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_list_null_elements_allowed() {
        let schema = make_list_with_nullable_elements_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "int_list_nullable": [1, null, 3]
        });
        let row = converter.convert(&input).unwrap();
        assert_eq!(row.values.len(), 1);
        assert_eq!(
            row.values[0],
            RowValue::Array(vec![RowValue::Int32(1), RowValue::Null, RowValue::Int32(3)])
        );
    }

    // Tests that combine schema_builder and json_converter in unit tests
    use crate::rest_ingest::schema_builder::{build_arrow_schema_impl, FieldSchema};

    fn make_schema_via_field_schema() -> Arc<Schema> {
        let fields = vec![
            FieldSchema {
                name: "id".into(),
                data_type: "int32".into(),
                nullable: false,
                fields: None,
                item: None,
            },
            FieldSchema {
                name: "name".into(),
                data_type: "string".into(),
                nullable: true,
                fields: None,
                item: None,
            },
            FieldSchema {
                name: "score".into(),
                data_type: "float64".into(),
                nullable: true,
                fields: None,
                item: None,
            },
            FieldSchema {
                name: "created".into(),
                data_type: "date32".into(),
                nullable: false,
                fields: None,
                item: None,
            },
            FieldSchema {
                name: "tags".into(),
                data_type: "list".into(),
                nullable: false,
                fields: None,
                item: Some(Box::new(FieldSchema {
                    name: "tag".into(),
                    data_type: "string".into(),
                    nullable: true,
                    fields: None,
                    item: None,
                })),
            },
            FieldSchema {
                name: "price".into(),
                data_type: "decimal(6,2)".into(),
                nullable: true,
                fields: None,
                item: None,
            },
            FieldSchema {
                name: "profile".into(),
                data_type: "struct".into(),
                nullable: true,
                fields: Some(vec![
                    FieldSchema {
                        name: "active".into(),
                        data_type: "boolean".into(),
                        nullable: false,
                        fields: None,
                        item: None,
                    },
                    FieldSchema {
                        name: "level".into(),
                        data_type: "int64".into(),
                        nullable: true,
                        fields: None,
                        item: None,
                    },
                ]),
                item: None,
            },
        ];
        Arc::new(build_arrow_schema_impl(&fields).expect("schema should build"))
    }

    #[test]
    fn test_schema_builder_integration_success() {
        let schema = make_schema_via_field_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema.clone());
        let input = json!({
            "id": 7,
            "name": "alice",
            "score": 88.5,
            "created": "2024-03-15",
            "tags": ["a", "b", "c"],
            "price": "123.45",
            "profile": { "active": true, "level": 3 }
        });
        let row = converter.convert(&input).unwrap();
        assert_eq!(row.values.len(), schema.fields.len());
        assert_eq!(row.values[0], RowValue::Int32(7));
    }

    #[test]
    fn test_schema_builder_missing_required_field() {
        let schema = make_schema_via_field_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            // missing id
            "name": "bob",
            "score": 10.0,
            "created": "2024-01-01",
            "tags": [],
            "price": null,
            "profile": {"active": false}
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::MissingField(f) => assert_eq!(f, "id"),
            _ => panic!("unexpected error"),
        }
    }

    #[test]
    fn test_schema_builder_list_type_mismatch() {
        let schema = make_schema_via_field_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "id": 1,
            "name": null,
            "score": null,
            "created": "2024-05-05",
            "tags": "not-an-array",
            "price": null,
            "profile": {"active": true}
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::TypeMismatch(f) => assert_eq!(f, "tags"),
            _ => panic!("unexpected error"),
        }
    }

    #[test]
    fn test_schema_builder_decimal_number_type_mismatch() {
        let schema = make_schema_via_field_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "id": 2,
            "name": "x",
            "score": 1.0,
            "created": "2024-03-01",
            "tags": [],
            "price": 12.34,
            "profile": {"active": true}
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::TypeMismatch(f) => assert_eq!(f, "price"),
            _ => panic!("unexpected error"),
        }
    }

    #[test]
    fn test_schema_builder_struct_child_missing_required() {
        let schema = make_schema_via_field_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "id": 3,
            "name": null,
            "score": null,
            "created": "2024-04-01",
            "tags": ["t"],
            "price": null,
            "profile": {"level": 10}
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::MissingField(f) => assert_eq!(f, "profile.active"),
            _ => panic!("unexpected error"),
        }
    }

    #[test]
    fn test_schema_builder_decimal_precision_out_of_range() {
        let schema = make_schema_via_field_schema();
        let converter = JsonToMoonlinkRowConverter::new(schema);
        let input = json!({
            "id": 4,
            "name": "y",
            "score": 2.0,
            "created": "2024-04-02",
            "tags": [],
            "price": "12345.67", // 7 digits total, exceeds precision 6
            "profile": {"active": false}
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::InvalidValueWithCause(field, _e) => assert_eq!(field, "price"),
            _ => panic!("unexpected error"),
        }
    }
}
