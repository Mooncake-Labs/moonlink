use arrow_schema::{DataType, Field, Schema};
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
            DataType::Int32 => {
                if let Some(i) = value.as_i64() {
                    Ok(RowValue::Int32(i as i32))
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
            DataType::Boolean => {
                if let Some(b) = value.as_bool() {
                    Ok(RowValue::Bool(b))
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
            DataType::Int64 => {
                if let Some(i) = value.as_i64() {
                    Ok(RowValue::Int64(i))
                } else {
                    Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone()))
                }
            }
            // TODO: Add more type conversions (Bool, Float, Date, etc.)
            _ => Err(JsonToMoonlinkRowError::TypeMismatch(field.name().clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
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
        });
        let row = converter.convert(&input).unwrap();
        assert_eq!(row.values.len(), 6);
        assert_eq!(row.values[0], RowValue::Int32(42));
        assert_eq!(row.values[1], RowValue::ByteArray(b"moonlink".to_vec()));
        assert_eq!(row.values[2], RowValue::Bool(true));
        assert_eq!(row.values[3], RowValue::Float64(100.0));
        assert_eq!(row.values[4], RowValue::Int64(123));
        assert_eq!(row.values[5], RowValue::Float32(100.0));
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
            "name": "moonlink"
        });
        let err = converter.convert(&input).unwrap_err();
        match err {
            JsonToMoonlinkRowError::TypeMismatch(f) => assert_eq!(f, "id"),
            _ => panic!("unexpected error: {err:?}"),
        }
    }
}
