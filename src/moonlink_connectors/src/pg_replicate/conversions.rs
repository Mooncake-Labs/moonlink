use std::fmt::Debug;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use numeric::PgNumeric;
use uuid::Uuid;

pub mod bool;
pub mod cdc_event;
pub mod hex;
pub mod numeric;
pub mod table_row;
pub mod text;

#[derive(Debug, Clone)]
pub enum Cell {
    Null,
    Bool(bool),
    String(String),
    I16(i16),
    I32(i32),
    U32(u32),
    I64(i64),
    F32(f32),
    F64(f64),
    Numeric(PgNumeric),
    Date(NaiveDate),
    Time(NaiveTime),
    TimeStamp(NaiveDateTime),
    TimeStampTz(DateTime<Utc>),
    Uuid(Uuid),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    Array(ArrayCell),
    /// Represents a PostgreSQL composite type (custom type with multiple fields)
    /// Each Cell in the Vec represents one field of the composite type
    Composite(Vec<Cell>),
}

#[derive(Debug, Clone)]
pub enum ArrayCell {
    Null,
    Bool(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    U32(Vec<Option<u32>>),
    I64(Vec<Option<i64>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    Numeric(Vec<Option<PgNumeric>>),
    Date(Vec<Option<NaiveDate>>),
    Time(Vec<Option<NaiveTime>>),
    TimeStamp(Vec<Option<NaiveDateTime>>),
    TimeStampTz(Vec<Option<DateTime<Utc>>>),
    Uuid(Vec<Option<Uuid>>),
    Json(Vec<Option<serde_json::Value>>),
    Bytes(Vec<Option<Vec<u8>>>),
    /// Represents an array of PostgreSQL composite types
    /// Vec<Option<Vec<Cell>>> structure:
    /// - Outer Vec: array elements
    /// - Option: nullable array elements (None = null element)
    /// - Inner Vec<Cell>: composite type fields for each element
    Composite(Vec<Option<Vec<Cell>>>),
}
