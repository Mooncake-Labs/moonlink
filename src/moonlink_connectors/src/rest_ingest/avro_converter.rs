use moonlink::row::{MoonlinkRow, RowValue};
use apache_avro::types::Value as AvroValue;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AvroToMoonlinkRowError {
    #[error("unsupported avro type: {0}")]
    UnsupportedType(String),
    #[error("conversion failed for value: {0}")]
    ConversionFailed(String),
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
            _ => Err(AvroToMoonlinkRowError::UnsupportedType(format!("{:?}", avro_value))),
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
            AvroValue::Fixed(size, _) => {
                Err(AvroToMoonlinkRowError::UnsupportedType(
                    format!("Fixed({}) - only Fixed(16) is supported", size)
                ))
            }
            
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
                    values.push(RowValue::Struct(vec![RowValue::ByteArray(key.as_bytes().to_vec()), converted]));
                }
                Ok(RowValue::Array(values))
            }
            
            // Union types (handle the boxed value directly)
            AvroValue::Union(_, boxed_value) => {
                Self::convert_value(boxed_value)
            }
            
            // Unsupported types
            _ => Err(AvroToMoonlinkRowError::UnsupportedType(format!("{:?}", value))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value as AvroValue;

    fn make_avro_record() -> AvroValue {
        AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Int(42)),
            ("name".to_string(), AvroValue::String("moonlink".to_string())),
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
    fn test_conversion_with_nulls() {
        let avro_record = AvroValue::Record(vec![
            ("id".to_string(), AvroValue::Int(1)),
            ("name".to_string(), AvroValue::Null),
            ("is_active".to_string(), AvroValue::Null),
            ("score".to_string(), AvroValue::Null),
        ]);
        
        let row = AvroToMoonlinkRowConverter::convert(&avro_record).unwrap();
        assert_eq!(row.values.len(), 4);
        assert_eq!(row.values[0], RowValue::Int32(1));
        assert_eq!(row.values[1], RowValue::Null);
        assert_eq!(row.values[2], RowValue::Null);
        assert_eq!(row.values[3], RowValue::Null);
    }

    #[test]
    fn test_unsupported_root_type() {
        let avro_value = AvroValue::String("not a record".to_string());
        
        let err = AvroToMoonlinkRowConverter::convert(&avro_value).unwrap_err();
        match err {
            AvroToMoonlinkRowError::UnsupportedType(_) => {
                // Expected
            }
            _ => panic!("unexpected error: {err:?}"),
        }
    }

    #[test]
    fn test_array_conversion() {
        let avro_record = AvroValue::Record(vec![
            ("tags".to_string(), AvroValue::Array(vec![
                AvroValue::String("tag1".to_string()),
                AvroValue::String("tag2".to_string()),
                AvroValue::String("tag3".to_string()),
            ])),
        ]);
        
        let row = AvroToMoonlinkRowConverter::convert(&avro_record).unwrap();
        assert_eq!(row.values.len(), 1);
        
        match &row.values[0] {
            RowValue::Array(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], RowValue::ByteArray(b"tag1".to_vec()));
                assert_eq!(items[1], RowValue::ByteArray(b"tag2".to_vec()));
                assert_eq!(items[2], RowValue::ByteArray(b"tag3".to_vec()));
            }
            _ => panic!("Expected array value"),
        }
    }

    #[test]
    fn test_struct_conversion() {
        let avro_record = AvroValue::Record(vec![
            ("user".to_string(), AvroValue::Record(vec![
                ("id".to_string(), AvroValue::Int(123)),
                ("name".to_string(), AvroValue::String("alice".to_string())),
            ])),
        ]);
        
        let row = AvroToMoonlinkRowConverter::convert(&avro_record).unwrap();
        assert_eq!(row.values.len(), 1);
        
        match &row.values[0] {
            RowValue::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0], RowValue::Int32(123));
                assert_eq!(fields[1], RowValue::ByteArray(b"alice".to_vec()));
            }
            _ => panic!("Expected struct value"),
        }
    }
} 