use core::str;
use std::num::{ParseFloatError, ParseIntError};

use bigdecimal::ParseBigDecimalError;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use thiserror::Error;
use tokio_postgres::types::Type;
use uuid::Uuid;

use crate::pg_replicate::conversions::{bool::parse_bool, hex};

use super::{bool::ParseBoolError, hex::ByteaHexParseError, numeric::PgNumeric, ArrayCell, Cell};

#[derive(Debug, Error)]
pub enum FromTextError {
    #[error("invalid text conversion")]
    InvalidConversion(),

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

impl TextFormatConverter {
    pub fn is_supported_type(typ: &Type) -> bool {
        matches!(
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
        )
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
            _ => Cell::Null,
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
            _ => Err(FromTextError::InvalidConversion()),
        }
    }

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
        let mut chars = str.chars();
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
                            if !in_quotes {
                                val_quoted = true;
                            }
                            in_quotes = !in_quotes;
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
            _ => panic!("unexpected cell"),
        }
    }

    #[test]
    fn parse_text_array_unquoted_null_is_none() {
        let cell = TextFormatConverter::try_from_str(&Type::TEXT_ARRAY, "{a,NULL}").unwrap();
        match cell {
            Cell::Array(ArrayCell::String(v)) => {
                assert_eq!(v, vec![Some("a".to_string()), None]);
            }
            _ => panic!("unexpected cell"),
        }
    }

    #[test]
    fn parse_char_vs_varchar_trailing_spaces() {
        // CHAR/BPCHAR should trim trailing spaces
        let char_cell = TextFormatConverter::try_from_str(&Type::CHAR, "hello   ").unwrap();
        match char_cell {
            Cell::String(s) => assert_eq!(s, "hello"),
            _ => panic!("expected string cell"),
        }

        let bpchar_cell = TextFormatConverter::try_from_str(&Type::BPCHAR, "world   ").unwrap();
        match bpchar_cell {
            Cell::String(s) => assert_eq!(s, "world"),
            _ => panic!("expected string cell"),
        }

        // VARCHAR/NAME/TEXT should preserve trailing spaces
        let varchar_cell = TextFormatConverter::try_from_str(&Type::VARCHAR, "hello   ").unwrap();
        match varchar_cell {
            Cell::String(s) => assert_eq!(s, "hello   "),
            _ => panic!("expected string cell"),
        }

        let text_cell = TextFormatConverter::try_from_str(&Type::TEXT, "world   ").unwrap();
        match text_cell {
            Cell::String(s) => assert_eq!(s, "world   "),
            _ => panic!("expected string cell"),
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
            _ => panic!("expected string array cell"),
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
            _ => panic!("expected string array cell"),
        }
    }
}
