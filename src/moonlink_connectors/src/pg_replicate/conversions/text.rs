use core::str;
use std::{
    fmt::format,
    num::{ParseFloatError, ParseIntError},
};

use bigdecimal::ParseBigDecimalError;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use thiserror::Error;
use tokio_postgres::types::{Kind, Type};
use uuid::Uuid;

use crate::pg_replicate::conversions::{bool::parse_bool, hex};

use super::{bool::ParseBoolError, hex::ByteaHexParseError, numeric::PgNumeric, ArrayCell, Cell};

#[derive(Debug, Error)]
pub enum FromTextError {
    #[error("invalid text conversion, unsupported type: {0}")]
    InvalidConversion(String),

    #[error("invalid bool value")]
    InvalidBool(#[from] ParseBoolError),

    #[error("invalid int value")]
    InvalidInt(#[from] ParseIntError),

    #[error("invalid float value")]
    InvalidFloat(#[from] ParseFloatError),

    #[error("invalid numeric: {0}")]
    InvalidNumeric(#[from] ParseBigDecimalError),

    #[error("invalid bytea: {0}")]
    InvalidBytea(#[from] ByteaHexParseError),

    #[error("invalid uuid: {0}")]
    InvalidUuid(#[from] uuid::Error),

    #[error("invalid json: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("invalid timestamp: {0} ")]
    InvalidTimestamp(#[from] chrono::ParseError),

    #[error("invalid array: {0}")]
    InvalidArray(#[from] ArrayParseError),

    #[error("invalid composite: {0}")]
    InvalidComposite(#[from] CompositeParseError),

    #[error("row get error: {0:?}")]
    RowGetError(#[from] Box<dyn std::error::Error + Sync + Send>),
}

pub struct TextFormatConverter;

#[derive(Debug, Error)]
pub enum ArrayParseError {
    #[error("input too short")]
    InputTooShort,

    #[error("missing braces")]
    MissingBraces,
}

#[derive(Debug, Error)]
pub enum CompositeParseError {
    #[error("input too short")]
    InputTooShort,

    #[error("missing parentheses")]
    MissingParentheses,

    #[error("field count mismatch")]
    FieldCountMismatch,
}

impl TextFormatConverter {
    pub fn is_supported_type(typ: &Type) -> bool {
        match typ.kind() {
            Kind::Simple => matches!(
                *typ,
                Type::BOOL
                    | Type::BOOL_ARRAY
                    | Type::CHAR
                    | Type::BPCHAR
                    | Type::VARCHAR
                    | Type::NAME
                    | Type::TEXT
                    | Type::CHAR_ARRAY
                    | Type::BPCHAR_ARRAY
                    | Type::VARCHAR_ARRAY
                    | Type::NAME_ARRAY
                    | Type::TEXT_ARRAY
                    | Type::INT2
                    | Type::INT2_ARRAY
                    | Type::INT4
                    | Type::INT4_ARRAY
                    | Type::INT8
                    | Type::INT8_ARRAY
                    | Type::FLOAT4
                    | Type::FLOAT4_ARRAY
                    | Type::FLOAT8
                    | Type::FLOAT8_ARRAY
                    | Type::NUMERIC
                    | Type::NUMERIC_ARRAY
                    | Type::BYTEA
                    | Type::BYTEA_ARRAY
                    | Type::DATE
                    | Type::DATE_ARRAY
                    | Type::TIME
                    | Type::TIME_ARRAY
                    | Type::TIMESTAMP
                    | Type::TIMESTAMP_ARRAY
                    | Type::TIMESTAMPTZ
                    | Type::TIMESTAMPTZ_ARRAY
                    | Type::UUID
                    | Type::UUID_ARRAY
                    | Type::JSON
                    | Type::JSON_ARRAY
                    | Type::JSONB
                    | Type::JSONB_ARRAY
                    | Type::OID
                    | Type::OID_ARRAY
            ),
            Kind::Array(_) => true,
            Kind::Composite(_) => true,
            _ => false,
        }
    }

    pub fn default_value(typ: &Type) -> Cell {
        match *typ {
            Type::BOOL => Cell::Bool(bool::default()),
            Type::BOOL_ARRAY => Cell::Array(ArrayCell::Bool(Vec::default())),
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                Cell::String(String::default())
            }
            Type::CHAR_ARRAY
            | Type::BPCHAR_ARRAY
            | Type::VARCHAR_ARRAY
            | Type::NAME_ARRAY
            | Type::TEXT_ARRAY => Cell::Array(ArrayCell::String(Vec::default())),
            Type::INT2 => Cell::I16(i16::default()),
            Type::INT2_ARRAY => Cell::Array(ArrayCell::I16(Vec::default())),
            Type::INT4 => Cell::I32(i32::default()),
            Type::INT4_ARRAY => Cell::Array(ArrayCell::I32(Vec::default())),
            Type::INT8 => Cell::I64(i64::default()),
            Type::INT8_ARRAY => Cell::Array(ArrayCell::I64(Vec::default())),
            Type::FLOAT4 => Cell::F32(f32::default()),
            Type::FLOAT4_ARRAY => Cell::Array(ArrayCell::F32(Vec::default())),
            Type::FLOAT8 => Cell::F64(f64::default()),
            Type::FLOAT8_ARRAY => Cell::Array(ArrayCell::F64(Vec::default())),
            Type::NUMERIC => Cell::Numeric(PgNumeric::default()),
            Type::NUMERIC_ARRAY => Cell::Array(ArrayCell::Numeric(Vec::default())),
            Type::BYTEA => Cell::Bytes(Vec::default()),
            Type::BYTEA_ARRAY => Cell::Array(ArrayCell::Bytes(Vec::default())),
            Type::DATE => Cell::Date(NaiveDate::MIN),
            Type::DATE_ARRAY => Cell::Array(ArrayCell::Date(Vec::default())),
            Type::TIME => Cell::Time(NaiveTime::MIN),
            Type::TIME_ARRAY => Cell::Array(ArrayCell::Time(Vec::default())),
            Type::TIMESTAMP => Cell::TimeStamp(NaiveDateTime::MIN),
            Type::TIMESTAMP_ARRAY => Cell::Array(ArrayCell::TimeStamp(Vec::default())),
            Type::TIMESTAMPTZ => {
                let val = DateTime::<Utc>::from_naive_utc_and_offset(NaiveDateTime::MIN, Utc);
                Cell::TimeStampTz(val)
            }
            Type::TIMESTAMPTZ_ARRAY => Cell::Array(ArrayCell::TimeStampTz(Vec::default())),
            Type::UUID => Cell::Uuid(Uuid::default()),
            Type::UUID_ARRAY => Cell::Array(ArrayCell::Uuid(Vec::default())),
            Type::JSON | Type::JSONB => Cell::Json(serde_json::Value::default()),
            Type::JSON_ARRAY | Type::JSONB_ARRAY => Cell::Array(ArrayCell::Json(Vec::default())),
            Type::OID => Cell::U32(u32::default()),
            Type::OID_ARRAY => Cell::Array(ArrayCell::U32(Vec::default())),
            _ => match typ.kind() {
                Kind::Composite(_) => Cell::Composite(Vec::default()),
                Kind::Array(inner_type) => {
                    // Handle arrays of composite types.
                    // Note: inner_type here refers to the element type of the array.
                    // PostgreSQL supports multi-dimensional arrays (e.g., text[][]),
                    // but we currently only handle arrays of composite types here.
                    match inner_type.kind() {
                        Kind::Composite(_) => Cell::Array(ArrayCell::Composite(Vec::default())),
                        Kind::Array(_) => Cell::Null, // TODO: Multi-dimensional arrays not yet handled
                        _ => Cell::Null,              // Unknown array type
                    }
                }
                _ => Cell::Null,
            },
        }
    }

    pub fn try_from_str(typ: &Type, str: &str) -> Result<Cell, FromTextError> {
        match *typ {
            Type::BOOL => Ok(Cell::Bool(parse_bool(str)?)),
            Type::BOOL_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(parse_bool(str)?)),
                ArrayCell::Bool,
            ),
            Type::CHAR | Type::BPCHAR => Ok(Cell::String(str.trim_end().to_string())),
            Type::VARCHAR | Type::NAME | Type::TEXT => Ok(Cell::String(str.to_string())),
            Type::CHAR_ARRAY | Type::BPCHAR_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(str.trim_end().to_string())),
                ArrayCell::String,
            ),
            Type::VARCHAR_ARRAY | Type::NAME_ARRAY | Type::TEXT_ARRAY => {
                TextFormatConverter::parse_array(
                    str,
                    |str| Ok(Some(str.to_string())),
                    ArrayCell::String,
                )
            }
            Type::INT2 => Ok(Cell::I16(str.parse()?)),
            Type::INT2_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::I16)
            }
            Type::INT4 => Ok(Cell::I32(str.parse()?)),
            Type::INT4_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::I32)
            }
            Type::INT8 => Ok(Cell::I64(str.parse()?)),
            Type::INT8_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::I64)
            }
            Type::FLOAT4 => Ok(Cell::F32(str.parse()?)),
            Type::FLOAT4_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::F32)
            }
            Type::FLOAT8 => Ok(Cell::F64(str.parse()?)),
            Type::FLOAT8_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::F64)
            }
            Type::NUMERIC => Ok(Cell::Numeric(str.parse()?)),
            Type::NUMERIC_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(str.parse()?)),
                ArrayCell::Numeric,
            ),
            Type::BYTEA => Ok(Cell::Bytes(hex::from_bytea_hex(str)?)),
            Type::BYTEA_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(hex::from_bytea_hex(str)?)),
                ArrayCell::Bytes,
            ),
            Type::DATE => {
                let val = NaiveDate::parse_from_str(str, "%Y-%m-%d")?;
                Ok(Cell::Date(val))
            }
            Type::DATE_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(NaiveDate::parse_from_str(str, "%Y-%m-%d")?)),
                ArrayCell::Date,
            ),
            Type::TIME => {
                let val = NaiveTime::parse_from_str(str, "%H:%M:%S%.f")?;
                Ok(Cell::Time(val))
            }
            Type::TIME_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(NaiveTime::parse_from_str(str, "%H:%M:%S%.f")?)),
                ArrayCell::Time,
            ),
            Type::TIMESTAMP => {
                let val = NaiveDateTime::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f")?;
                Ok(Cell::TimeStamp(val))
            }
            Type::TIMESTAMP_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| {
                    Ok(Some(NaiveDateTime::parse_from_str(
                        str,
                        "%Y-%m-%d %H:%M:%S%.f",
                    )?))
                },
                ArrayCell::TimeStamp,
            ),
            Type::TIMESTAMPTZ => {
                let val =
                    match DateTime::<FixedOffset>::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f%#z") {
                        Ok(val) => val,
                        Err(_) => {
                            DateTime::<FixedOffset>::parse_from_str(str, "%Y-%m-%d %H:%M:%S%.f%:z")?
                        }
                    };
                Ok(Cell::TimeStampTz(val.into()))
            }
            Type::TIMESTAMPTZ_ARRAY => {
                match TextFormatConverter::parse_array(
                    str,
                    |str| {
                        Ok(Some(
                            DateTime::<FixedOffset>::parse_from_str(
                                str,
                                "%Y-%m-%d %H:%M:%S%.f%#z",
                            )?
                            .into(),
                        ))
                    },
                    ArrayCell::TimeStampTz,
                ) {
                    Ok(val) => Ok(val),
                    Err(_) => TextFormatConverter::parse_array(
                        str,
                        |str| {
                            Ok(Some(
                                DateTime::<FixedOffset>::parse_from_str(
                                    str,
                                    "%Y-%m-%d %H:%M:%S%.f%:z",
                                )?
                                .into(),
                            ))
                        },
                        ArrayCell::TimeStampTz,
                    ),
                }
            }
            Type::UUID => {
                let val = Uuid::parse_str(str)?;
                Ok(Cell::Uuid(val))
            }
            Type::UUID_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(Uuid::parse_str(str)?)),
                ArrayCell::Uuid,
            ),
            Type::JSON | Type::JSONB => {
                let val = serde_json::from_str(str)?;
                Ok(Cell::Json(val))
            }
            Type::JSON_ARRAY | Type::JSONB_ARRAY => TextFormatConverter::parse_array(
                str,
                |str| Ok(Some(serde_json::from_str(str)?)),
                ArrayCell::Json,
            ),
            Type::OID => {
                let val: u32 = str.parse()?;
                Ok(Cell::U32(val))
            }
            Type::OID_ARRAY => {
                TextFormatConverter::parse_array(str, |str| Ok(Some(str.parse()?)), ArrayCell::U32)
            }
            _ => match typ.kind() {
                Kind::Composite(fields) => TextFormatConverter::parse_composite(str, fields),
                Kind::Array(inner_type) => {
                    // Check if the array contains composite types.
                    // PostgreSQL supports multi-dimensional arrays (e.g., int[][], text[][]),
                    // but here we currently only handle arrays of composite types.
                    match inner_type.kind() {
                        Kind::Composite(fields) => {
                            TextFormatConverter::parse_composite_array(str, fields)
                        }
                        // TODO: Multi-dimensional arrays not yet implemented
                        _ => Err(FromTextError::InvalidConversion(format!(
                            "{} with inner type: {}",
                            typ, inner_type
                        ))),
                    }
                }
                _ => Err(FromTextError::InvalidConversion(format!("{:?}", typ))),
            },
        }
    }

    /// Parse Postgres text arrays: respect quotes/escapes; unquoted NULL is None, quoted "null" is a string
    fn parse_array<P, M, T>(str: &str, mut parse: P, m: M) -> Result<Cell, FromTextError>
    where
        P: FnMut(&str) -> Result<Option<T>, FromTextError>,
        M: FnOnce(Vec<Option<T>>) -> ArrayCell,
    {
        if str.len() < 2 {
            return Err(ArrayParseError::InputTooShort.into());
        }

        if !str.starts_with('{') || !str.ends_with('}') {
            return Err(ArrayParseError::MissingBraces.into());
        }

        let mut res = vec![];
        let str = &str[1..(str.len() - 1)];
        let mut val_str = String::with_capacity(10);
        let mut in_quotes = false;
        let mut in_escape = false;
        let mut val_quoted = false;
        let mut chars = str.chars().peekable();
        let mut done = str.is_empty();

        while !done {
            loop {
                match chars.next() {
                    Some(c) => match c {
                        c if in_escape => {
                            val_str.push(c);
                            in_escape = false;
                        }
                        '"' => {
                            if in_quotes {
                                // support doubled quotes inside quoted value
                                if let Some('"') = chars.peek().copied() {
                                    // consume next quote and append a single quote to value
                                    // means we are encapsulating a composite value
                                    let _ = chars.next();
                                    val_str.push('"');
                                } else {
                                    in_quotes = false;
                                }
                            } else {
                                val_quoted = true;
                                in_quotes = true;
                            }
                        }
                        '\\' => in_escape = true,
                        ',' if !in_quotes => {
                            break;
                        }
                        c => {
                            val_str.push(c);
                        }
                    },
                    None => {
                        done = true;
                        break;
                    }
                }
            }
            let val = if !val_quoted && val_str.to_lowercase() == "null" {
                None
            } else {
                parse(&val_str)?
            };
            res.push(val);
            val_str.clear();
            val_quoted = false;
        }

        Ok(Cell::Array(m(res)))
    }

    /// Parses a PostgreSQL composite type from its text representation.
    ///
    /// PostgreSQL composite types are represented as `(field1,field2,...)` where:
    /// - Fields are comma-separated
    /// - NULL values are represented as empty or the literal 'null' (case-insensitive)
    /// - Quoted values preserve all characters including commas and parentheses
    /// - Escaped characters within quotes are handled with backslash
    /// - Don't split on commas inside quotes
    ///
    /// Reference: https://www.postgresql.org/docs/current/rowtypes.html#ROWTYPES-IO-SYNTAX
    fn parse_composite(
        s: &str,
        fields: &[tokio_postgres::types::Field],
    ) -> Result<Cell, FromTextError> {
        if s.len() < 2 {
            return Err(CompositeParseError::InputTooShort.into());
        }

        if !s.starts_with('(') || !s.ends_with(')') {
            return Err(CompositeParseError::MissingParentheses.into());
        }

        let mut res = Vec::with_capacity(fields.len());
        let inner = &s[1..(s.len() - 1)];
        let mut val_str = String::with_capacity(10);
        let mut in_quotes = false;
        let mut in_escape = false;
        let mut val_quoted = false;
        let mut chars = inner.chars().peekable();
        let mut field_iter = fields.iter();
        let mut done = inner.is_empty();

        while !done {
            loop {
                match chars.next() {
                    Some(c) => match c {
                        c if in_escape => {
                            val_str.push(c);
                            in_escape = false;
                        }
                        '"' => {
                            if in_quotes {
                                // support doubled quotes inside quoted value
                                if let Some('"') = chars.peek().copied() {
                                    // consume next quote and append a single quote to value
                                    let _ = chars.next();
                                    val_str.push('"');
                                } else {
                                    in_quotes = false;
                                }
                            } else {
                                val_quoted = true;
                                in_quotes = true;
                            }
                        }
                        '\\' if in_quotes => in_escape = true,
                        ',' if !in_quotes => {
                            break;
                        }
                        c => {
                            val_str.push(c);
                        }
                    },
                    None => {
                        done = true;
                        break;
                    }
                }
            }

            let field = field_iter
                .next()
                .ok_or(CompositeParseError::FieldCountMismatch)?;

            let val = if !val_quoted && val_str.is_empty() {
                Cell::Null
            } else {
                TextFormatConverter::try_from_str(field.type_(), &val_str)?
            };

            res.push(val);
            val_str.clear();
            val_quoted = false;
        }

        if field_iter.next().is_some() {
            return Err(CompositeParseError::FieldCountMismatch.into());
        }

        Ok(Cell::Composite(res))
    }

    /// Parses a PostgreSQL array of composite types from its text representation.
    ///
    /// PostgreSQL arrays of composite types are represented as `{"(field1,field2)","(field3,field4)"}` where:
    /// - The array is enclosed in curly braces `{}`
    /// - Each composite element is enclosed in double quotes if it contains special characters
    /// - Composite elements follow the same format as regular composites: `(field1,field2,...)`
    /// - NULL array elements are represented as the literal 'null' (case-insensitive)
    /// - Empty arrays are represented as `{}`
    ///
    /// Example formats:
    /// - Simple: `{"(1,hello)","(2,world)"}`
    /// - With NULLs: `{"(1,hello)",null,"(3,test)"}`
    /// - With special chars: `{"(1,\"hello, world\")","(2,\"test\")"}`
    /// - Empty: `{}`
    ///
    /// Reference: https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
    fn parse_composite_array(
        s: &str,
        fields: &[tokio_postgres::types::Field],
    ) -> Result<Cell, FromTextError> {
        // Delegate to the generic array parser
        TextFormatConverter::parse_array(
            s,
            |str| {
                let cell = TextFormatConverter::parse_composite(str, fields)?;
                match cell {
                    Cell::Composite(values) => Ok(Some(values)),
                    _ => unreachable!("parse_composite should always return Cell::Composite"),
                }
            },
            ArrayCell::Composite,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_text_array_quoted_null_as_string() {
        let cell =
            TextFormatConverter::try_from_str(&Type::TEXT_ARRAY, "{\"a\",\"null\"}").unwrap();
        match cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(v, vec![Some("a".to_string()), Some("null".to_string())]);
            }
            _ => panic!("unexpected cell: {cell:?}"),
        }
    }

    #[test]
    fn parse_text_array_unquoted_null_is_none() {
        let cell = TextFormatConverter::try_from_str(&Type::TEXT_ARRAY, "{a,NULL}").unwrap();
        match cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(v, vec![Some("a".to_string()), None]);
            }
            _ => panic!("unexpected cell: {cell:?}"),
        }
    }

    #[test]
    fn parse_char_vs_varchar_trailing_spaces() {
        // CHAR/BPCHAR should trim trailing spaces
        let char_cell = TextFormatConverter::try_from_str(&Type::CHAR, "hello   ").unwrap();
        match char_cell {
            Cell::String(s) => assert_eq!(s, "hello"),
            _ => panic!("expected string cell, got: {char_cell:?}"),
        }

        let bpchar_cell = TextFormatConverter::try_from_str(&Type::BPCHAR, "world   ").unwrap();
        match bpchar_cell {
            Cell::String(s) => assert_eq!(s, "world"),
            _ => panic!("expected string cell, got: {bpchar_cell:?}"),
        }

        // VARCHAR/NAME/TEXT should preserve trailing spaces
        let varchar_cell = TextFormatConverter::try_from_str(&Type::VARCHAR, "hello   ").unwrap();
        match varchar_cell {
            Cell::String(s) => assert_eq!(s, "hello   "),
            _ => panic!("expected string cell, got: {varchar_cell:?}"),
        }

        let text_cell = TextFormatConverter::try_from_str(&Type::TEXT, "world   ").unwrap();
        match text_cell {
            Cell::String(s) => assert_eq!(s, "world   "),
            _ => panic!("expected string cell, got: {text_cell:?}"),
        }
    }

    #[test]
    fn parse_char_array_vs_varchar_array_trailing_spaces() {
        // CHAR_ARRAY/BPCHAR_ARRAY should trim trailing spaces
        let char_array_cell =
            TextFormatConverter::try_from_str(&Type::CHAR_ARRAY, "{\"hello   \",\"world   \"}")
                .unwrap();
        match char_array_cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(
                    v,
                    vec![Some("hello".to_string()), Some("world".to_string())]
                );
            }
            _ => panic!("expected string array cell, got: {char_array_cell:?}"),
        }

        // VARCHAR_ARRAY should preserve trailing spaces
        let varchar_array_cell =
            TextFormatConverter::try_from_str(&Type::VARCHAR_ARRAY, "{\"hello   \",\"world   \"}")
                .unwrap();
        match varchar_array_cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(
                    v,
                    vec![Some("hello   ".to_string()), Some("world   ".to_string())]
                );
            }
            _ => panic!("expected string array cell, got: {varchar_array_cell:?}"),
        }
    }

    #[test]
    fn parse_composite_basic() {
        use tokio_postgres::types::Field;

        // Create mock field definitions for a composite type with (int4, text)
        let fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("name".to_string(), Type::TEXT),
        ];

        // Test parsing a basic composite value
        let composite_str = "(42,\"hello world\")";
        let cell = TextFormatConverter::parse_composite(composite_str, &fields).unwrap();

        match cell {
            Cell::Composite(values) => {
                assert_eq!(values.len(), 2);
                assert!(matches!(values[0], Cell::I32(42)));
                assert!(matches!(values[1], Cell::String(ref s) if s == "hello world"));
            }
            _ => panic!("expected composite cell, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_composite_with_nulls() {
        use tokio_postgres::types::Field;

        // Create mock field definitions
        let fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("name".to_string(), Type::TEXT),
            Field::new("active".to_string(), Type::BOOL),
        ];

        // Test parsing with null values (PostgreSQL uses 't' for true)
        let composite_str = "(42,,t)";
        let cell = TextFormatConverter::parse_composite(composite_str, &fields).unwrap();

        match cell {
            Cell::Composite(values) => {
                assert_eq!(values.len(), 3);
                assert!(matches!(values[0], Cell::I32(42)));
                assert!(matches!(values[1], Cell::Null));
                assert!(matches!(values[2], Cell::Bool(true)));
            }
            _ => panic!("expected composite cell, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_composite_with_array_field() {
        use tokio_postgres::types::Field;

        // Create a composite type with an array field
        let fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("tags".to_string(), Type::TEXT_ARRAY),
            Field::new("scores".to_string(), Type::INT4_ARRAY),
        ];

        // Test parsing composite with array fields
        let composite_str = "(1,\"{\\\"tag1\\\",\\\"tag2\\\"}\",\"{10,20,30}\")";
        let cell = TextFormatConverter::parse_composite(composite_str, &fields).unwrap();

        match cell {
            Cell::Composite(values) => {
                assert_eq!(values.len(), 3);
                assert!(matches!(values[0], Cell::I32(1)));

                // Check text array field
                match &values[1] {
                    Cell::Array(ArrayCell::String(tags)) => {
                        assert_eq!(tags.len(), 2);
                        assert_eq!(tags[0], Some("tag1".to_string()));
                        assert_eq!(tags[1], Some("tag2".to_string()));
                    }
                    _ => panic!("expected text array"),
                }

                // Check int array field
                match &values[2] {
                    Cell::Array(ArrayCell::I32(scores)) => {
                        assert_eq!(scores.len(), 3);
                        assert_eq!(scores[0], Some(10));
                        assert_eq!(scores[1], Some(20));
                        assert_eq!(scores[2], Some(30));
                    }
                    _ => panic!("expected int array"),
                }
            }
            _ => panic!("expected composite cell, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_composite_nested() {
        use tokio_postgres::types::Field;

        // Create a nested composite type structure
        let inner_fields = vec![
            Field::new("x".to_string(), Type::INT4),
            Field::new("y".to_string(), Type::INT4),
        ];

        // Mock a composite type that contains another composite
        let composite_type = Type::new(
            "point".to_string(),
            0, // OID doesn't matter for this test
            Kind::Composite(inner_fields.clone()),
            "public".to_string(),
        );

        let outer_fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("point".to_string(), composite_type),
        ];

        // Test parsing nested composite
        let composite_str = "(1,\"(10,20)\")";
        let cell = TextFormatConverter::parse_composite(composite_str, &outer_fields).unwrap();

        match cell {
            Cell::Composite(values) => {
                assert_eq!(values.len(), 2);
                assert!(matches!(values[0], Cell::I32(1)));

                // The nested composite should be parsed as well
                match &values[1] {
                    Cell::Composite(inner_values) => {
                        assert_eq!(inner_values.len(), 2);
                        assert!(matches!(inner_values[0], Cell::I32(10)));
                        assert!(matches!(inner_values[1], Cell::I32(20)));
                    }
                    _ => panic!("expected nested composite"),
                }
            }
            _ => panic!("expected composite cell, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_composite_deeply_nested_with_arrays() {
        use tokio_postgres::types::Field;

        // Create a complex nested structure:
        // outer_type {
        //   id: int4,
        //   data: inner_type {
        //     values: int4[],
        //     metadata: text
        //   }
        // }

        let inner_fields = vec![
            Field::new("values".to_string(), Type::INT4_ARRAY),
            Field::new("metadata".to_string(), Type::TEXT),
        ];

        let inner_type = Type::new(
            "inner_type".to_string(),
            0,
            Kind::Composite(inner_fields.clone()),
            "public".to_string(),
        );

        let outer_fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("data".to_string(), inner_type),
        ];

        // Test parsing deeply nested composite with arrays
        let composite_str = "(99,\"(\\\"{1,2,3}\\\",\\\"meta info\\\")\")";
        let cell = TextFormatConverter::parse_composite(composite_str, &outer_fields).unwrap();

        match cell {
            Cell::Composite(outer_values) => {
                assert_eq!(outer_values.len(), 2);
                assert!(matches!(outer_values[0], Cell::I32(99)));

                // Check nested composite
                match &outer_values[1] {
                    Cell::Composite(inner_values) => {
                        assert_eq!(inner_values.len(), 2);

                        // Check array within nested composite
                        match &inner_values[0] {
                            Cell::Array(ArrayCell::I32(values)) => {
                                assert_eq!(values.len(), 3);
                                assert_eq!(values[0], Some(1));
                                assert_eq!(values[1], Some(2));
                                assert_eq!(values[2], Some(3));
                            }
                            _ => panic!("expected int array in nested composite"),
                        }

                        // Check text field in nested composite
                        assert!(matches!(inner_values[1], Cell::String(ref s) if s == "meta info"));
                    }
                    _ => panic!("expected nested composite"),
                }
            }
            _ => panic!("expected composite cell, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_array_of_composites() {
        use tokio_postgres::types::Field;

        // Create a composite type definition
        let fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("name".to_string(), Type::TEXT),
        ];

        // Test parsing array of composites - PostgreSQL format uses quotes around the whole composite
        let array_str = r#"{"(1,\"alice\")","(2,\"bob\")","(3,\"charlie\")"}"#;
        let cell = TextFormatConverter::parse_composite_array(array_str, &fields).unwrap();

        match cell {
            Cell::Array(ArrayCell::Composite(composites)) => {
                assert_eq!(composites.len(), 3);

                // Check first composite
                let first = composites[0].as_ref().unwrap();
                assert_eq!(first.len(), 2);
                assert!(matches!(first[0], Cell::I32(1)));
                assert!(matches!(first[1], Cell::String(ref s) if s == "alice"));

                // Check second composite
                let second = composites[1].as_ref().unwrap();
                assert_eq!(second.len(), 2);
                assert!(matches!(second[0], Cell::I32(2)));
                assert!(matches!(second[1], Cell::String(ref s) if s == "bob"));

                // Check third composite
                let third = composites[2].as_ref().unwrap();
                assert_eq!(third.len(), 2);
                assert!(matches!(third[0], Cell::I32(3)));
                assert!(matches!(third[1], Cell::String(ref s) if s == "charlie"));
            }
            _ => panic!("expected array of composites, got: {cell:?}"),
        }

        let addr_fields = vec![
            Field::new("street".to_string(), Type::TEXT),
            Field::new("city".to_string(), Type::TEXT),
            Field::new("zip".to_string(), Type::INT4),
        ];
        let pgoutput_like = r#"{"(\"789 Pine St\",Chicago,60601)","(\"321 Elm St\",Boston,2101)"}"#;
        let cell = TextFormatConverter::parse_composite_array(pgoutput_like, &addr_fields).unwrap();
        match cell {
            Cell::Array(ArrayCell::Composite(composites)) => {
                assert_eq!(composites.len(), 2);
                let first = composites[0].as_ref().unwrap();
                assert!(matches!(first[0], Cell::String(ref s) if s == "789 Pine St"));
                assert!(matches!(first[1], Cell::String(ref s) if s == "Chicago"));
                assert!(matches!(first[2], Cell::I32(60601)));
                let second = composites[1].as_ref().unwrap();
                assert!(matches!(second[0], Cell::String(ref s) if s == "321 Elm St"));
                assert!(matches!(second[1], Cell::String(ref s) if s == "Boston"));
                assert!(matches!(second[2], Cell::I32(2101)));
            }
            _ => panic!("expected array of composites, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_array_of_composites_with_nulls() {
        use tokio_postgres::types::Field;

        // Create a composite type definition
        let fields = vec![
            Field::new("x".to_string(), Type::INT4),
            Field::new("y".to_string(), Type::INT4),
        ];

        // Test parsing array with null composite and composites with null fields
        let array_str = r#"{"(1,2)",NULL,"(3,)"}"#;
        let cell = TextFormatConverter::parse_composite_array(array_str, &fields).unwrap();

        match cell {
            Cell::Array(ArrayCell::Composite(composites)) => {
                assert_eq!(composites.len(), 3);

                // First composite: (1,2)
                let first = composites[0].as_ref().unwrap();
                assert_eq!(first.len(), 2);
                assert!(matches!(first[0], Cell::I32(1)));
                assert!(matches!(first[1], Cell::I32(2)));

                // Second composite: NULL
                assert!(composites[1].is_none());

                // Third composite: (3,NULL)
                let third = composites[2].as_ref().unwrap();
                assert_eq!(third.len(), 2);
                assert!(matches!(third[0], Cell::I32(3)));
                assert!(matches!(third[1], Cell::Null));
            }
            _ => panic!("expected array of composites, got: {cell:?}"),
        }

        // Quoted "null" is not a NULL element; it should fail composite parsing (missing parens)
        let array_str = r#"{"\"null\""}"#;
        let err = TextFormatConverter::parse_composite_array(array_str, &fields).unwrap_err();
        assert!(matches!(
            err,
            FromTextError::InvalidComposite(CompositeParseError::MissingParentheses)
        ));
    }

    #[test]
    fn parse_empty_composite_array() {
        use tokio_postgres::types::Field;

        let fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("name".to_string(), Type::TEXT),
        ];

        // Test empty array
        let array_str = "{}";
        let cell = TextFormatConverter::parse_composite_array(array_str, &fields).unwrap();

        match cell {
            Cell::Array(ArrayCell::Composite(composites)) => {
                assert_eq!(composites.len(), 0);
            }
            _ => panic!("expected empty array of composites, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_composite_array_via_type_system() {
        use tokio_postgres::types::Field;

        // Create composite type fields
        let composite_fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("active".to_string(), Type::BOOL),
        ];

        // Create the composite type
        let composite_type = Type::new(
            "user_info".to_string(),
            0,
            Kind::Composite(composite_fields.clone()),
            "public".to_string(),
        );

        // Create array of composite type
        let array_type = Type::new(
            "_user_info".to_string(),
            0,
            Kind::Array(composite_type),
            "public".to_string(),
        );

        // Test parsing through the main try_from_str function
        let array_str = r#"{"(1,t)","(2,f)"}"#;
        let cell = TextFormatConverter::try_from_str(&array_type, array_str).unwrap();

        match cell {
            Cell::Array(ArrayCell::Composite(composites)) => {
                assert_eq!(composites.len(), 2);

                let first = composites[0].as_ref().unwrap();
                assert!(matches!(first[0], Cell::I32(1)));
                assert!(matches!(first[1], Cell::Bool(true)));

                let second = composites[1].as_ref().unwrap();
                assert!(matches!(second[0], Cell::I32(2)));
                assert!(matches!(second[1], Cell::Bool(false)));
            }
            _ => panic!("expected array of composites, got: {cell:?}"),
        }
    }

    #[test]
    fn parse_empty_composite() {
        use tokio_postgres::types::Field;

        // Create an empty composite type (no fields)
        let fields: Vec<Field> = vec![];

        // Test parsing empty composite
        let composite_str = "()";
        let cell = TextFormatConverter::parse_composite(composite_str, &fields).unwrap();

        match cell {
            Cell::Composite(values) => {
                assert_eq!(values.len(), 0);
            }
            _ => panic!("expected empty composite cell, got: {cell:?}"),
        }
    }

    #[test]
    fn test_composite_parse_errors() {
        use tokio_postgres::types::Field;

        let fields = vec![
            Field::new("id".to_string(), Type::INT4),
            Field::new("name".to_string(), Type::TEXT),
        ];

        // Test input too short
        let result = TextFormatConverter::parse_composite("", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::InputTooShort
            ))
        ));

        let result = TextFormatConverter::parse_composite("(", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::InputTooShort
            ))
        ));

        // Test missing parentheses
        let result = TextFormatConverter::parse_composite("1,hello", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::MissingParentheses
            ))
        ));

        let result = TextFormatConverter::parse_composite("(1,hello", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::MissingParentheses
            ))
        ));

        let result = TextFormatConverter::parse_composite("1,hello)", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::MissingParentheses
            ))
        ));

        // Test field count mismatch - too many fields
        let result = TextFormatConverter::parse_composite("(1,hello,extra)", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::FieldCountMismatch
            ))
        ));

        // Test field count mismatch - too few fields
        let result = TextFormatConverter::parse_composite("(1)", &fields);
        assert!(matches!(
            result,
            Err(FromTextError::InvalidComposite(
                CompositeParseError::FieldCountMismatch
            ))
        ));
    }
}
