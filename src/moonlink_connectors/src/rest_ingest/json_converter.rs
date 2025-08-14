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
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Int32 => {
                if let Some(i) = value.as_i64() {
                    Ok(RowValue::Int32(i as i32))
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Int64 => {
                if let Some(i) = value.as_i64() {
                    Ok(RowValue::Int64(i))
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Float32 => {
                if let Some(f) = value.as_f64() {
                    Ok(RowValue::Float32(f as f32))
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Float64 => {
                if let Some(f) = value.as_f64() {
                    Ok(RowValue::Float64(f))
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Date32 => {
                if let Some(s) = value.as_str() {
                    parse_date(s)
                        .map_err(|_| JsonToMoonlinkRowError::InvalidValue(field.name().clone()))
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                if let Some(s) = value.as_str() {
                    parse_time(s)
                        .map_err(|_| JsonToMoonlinkRowError::InvalidValue(field.name().clone()))
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                if let Some(s) = value.as_str() {
                    parse_timestamp_with_timezone(s, tz.as_deref())
                        .map_err(|_| JsonToMoonlinkRowError::InvalidValue(field.name().clone()))
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Utf8 => {
                if let Some(s) = value.as_str() {
                    Ok(RowValue::ByteArray(s.as_bytes().to_vec()))
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            DataType::Decimal128(precision, scale) => {
                if let Some(s) = value.as_str() {
                    convert_decimal_to_row_value(s, *precision, *scale)
                        .map_err(|_| JsonToMoonlinkRowError::InvalidValue(field.name().clone()))
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
            _ => Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use chrono::{NaiveDate, TimeZone, Utc};
    use serde_json::json;
    use std::sync::Arc;

    fn make_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("is_active", DataType::Boolean, false),
            Field::new("score", DataType::Float64, false),
            Field::new("id_int64", DataType::Int64, false),
            Field::new("score_float32", DataType::Float32, false),
            Field::new("decimal128", DataType::Decimal128(5, 2), false),
        ]))
    }

    fn make_schema_with_decimal256() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "decimal256",
            DataType::Decimal256(38, 10),
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
    fn test_decimal_conversion_invalid_precision() {
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
            JsonToMoonlinkRowError::InvalidValue(f) => assert_eq!(f, "decimal128"),
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
            JsonToMoonlinkRowError::InvalidValue(f) => assert_eq!(f, "decimal128"),
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
}
