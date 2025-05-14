use crate::pg_replicate::{
    conversions::{numeric::PgNumeric, table_row::TableRow, ArrayCell, Cell},
    table::{LookupKey, TableSchema},
};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::Timelike;
use moonlink::row::RowValue;
use moonlink::row::{IdentityProp, MoonlinkRow};
use num_traits::cast::ToPrimitive;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::types::{Kind, Type};

pub fn postgres_primitive_to_arrow_type(typ: &Type, name: &str, nullable: bool) -> Field {
    let (data_type, extension_name) = match *typ {
        Type::BOOL => (DataType::Boolean, None),
        Type::INT2 => (DataType::Int16, None),
        Type::INT4 => (DataType::Int32, None),
        Type::INT8 => (DataType::Int64, None),
        Type::FLOAT4 => (DataType::Float32, None),
        Type::FLOAT8 => (DataType::Float64, None),
        Type::NUMERIC => (DataType::Decimal128(38, 0), None),
        Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::CHAR | Type::NAME => {
            (DataType::Utf8, None)
        }
        Type::DATE => (DataType::Date32, None),
        Type::TIMESTAMP => (
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            None,
        ),
        Type::TIMESTAMPTZ => (
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            None,
        ),
        Type::TIME => (
            DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            None,
        ),
        Type::TIMETZ => (
            DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            None,
        ),
        Type::UUID => (DataType::FixedSizeBinary(16), Some("uuid".to_string())),
        Type::JSON | Type::JSONB => (DataType::Utf8, Some("json".to_string())),
        Type::BYTEA => (DataType::Binary, None),
        _ => (DataType::Utf8, None), // Default to string for unknown types
    };

    let mut field = Field::new(name, data_type, nullable);

    // Apply extension type if specified
    if let Some(ext_name) = extension_name {
        let mut metadata = HashMap::new();
        metadata.insert("ARROW:extension:name".to_string(), ext_name);
        field = field.with_metadata(metadata);
    }

    field
}

pub fn postgres_type_to_arrow_type(typ: &Type, name: &str, nullable: bool) -> Field {
    match typ.kind() {
        Kind::Simple => postgres_primitive_to_arrow_type(typ, name, nullable),
        Kind::Array(inner) => {
            let item_type = postgres_type_to_arrow_type(inner, "item", false);
            Field::new_list(name, Arc::new(item_type), nullable)
        }
        Kind::Composite(fields) => {
            let fields: Vec<Field> = fields
                .iter()
                .map(|f| postgres_type_to_arrow_type(f.type_(), f.name(), true))
                .collect();
            Field::new_struct(name, fields, nullable)
        }
        Kind::Enum(_) => Field::new(name, DataType::Utf8, nullable),
        _ => {
            panic!("Unsupported type: {:?}", typ);
        }
    }
}

/// Convert a PostgreSQL TableSchema to an Arrow Schema
pub fn postgres_schema_to_moonlink_schema(table_schema: &TableSchema) -> (Schema, IdentityProp) {
    let fields: Vec<Field> = table_schema
        .column_schemas
        .iter()
        .map(|col| postgres_type_to_arrow_type(&col.typ, &col.name, col.nullable))
        .collect();

    let identity = match &table_schema.lookup_key {
        LookupKey::Key { name: _, columns } => {
            let columns = columns
                .iter()
                .map(|c| {
                    table_schema
                        .column_schemas
                        .iter()
                        .position(|cs| cs.name == *c)
                        .unwrap()
                })
                .collect();
            IdentityProp::new_key(columns, &fields)
        }
        LookupKey::FullRow => IdentityProp::FullRow,
    };
    (Schema::new(fields), identity)
}

pub fn _table_schema_to_iceberg_schema(_table_schema: &TableSchema) -> Schema {
    todo!("Iceberg: convert postgres table schema to iceberg schema!")
}

pub(crate) struct PostgresTableRow(pub TableRow);

const ARROW_EPOCH: chrono::NaiveDate = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

fn convert_array_cell(cell: ArrayCell) -> Vec<RowValue> {
    match cell {
        ArrayCell::Null => vec![],
        ArrayCell::Bool(values) => values
            .into_iter()
            .map(|v| v.map(RowValue::Bool).unwrap_or(RowValue::Null))
            .collect(),
        ArrayCell::String(values) => values
            .into_iter()
            .map(|v| {
                v.map(|s| RowValue::ByteArray(s.as_bytes().to_vec()))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::I16(values) => values
            .into_iter()
            .map(|v| {
                v.map(|i| RowValue::Int32(i as i32))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::I32(values) => values
            .into_iter()
            .map(|v| v.map(RowValue::Int32).unwrap_or(RowValue::Null))
            .collect(),
        ArrayCell::U32(values) => values
            .into_iter()
            .map(|v| {
                v.map(|i| RowValue::Int32(i as i32))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::I64(values) => values
            .into_iter()
            .map(|v| v.map(RowValue::Int64).unwrap_or(RowValue::Null))
            .collect(),
        ArrayCell::F32(values) => values
            .into_iter()
            .map(|v| v.map(RowValue::Float32).unwrap_or(RowValue::Null))
            .collect(),
        ArrayCell::F64(values) => values
            .into_iter()
            .map(|v| v.map(RowValue::Float64).unwrap_or(RowValue::Null))
            .collect(),
        ArrayCell::Numeric(values) => values
            .into_iter()
            .map(|v| {
                v.map(|n| match n {
                    PgNumeric::Value(bigdecimal) => {
                        RowValue::Decimal(bigdecimal.to_i128().unwrap())
                    }
                    _ => RowValue::Null,
                })
                .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::Date(values) => values
            .into_iter()
            .map(|v| {
                v.map(|d| RowValue::Int32(d.signed_duration_since(ARROW_EPOCH).num_days() as i32))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::Time(values) => values
            .into_iter()
            .map(|v| {
                v.map(|t| {
                    RowValue::Int64(
                        t.num_seconds_from_midnight() as i64 * 1_000_000
                            + t.nanosecond() as i64 / 1_000,
                    )
                })
                .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::TimeStamp(values) => values
            .into_iter()
            .map(|v| {
                v.map(|t| RowValue::Int64(t.and_utc().timestamp_micros()))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::TimeStampTz(values) => values
            .into_iter()
            .map(|v| {
                v.map(|t| RowValue::Int64(t.timestamp_micros()))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::Uuid(values) => values
            .into_iter()
            .map(|v| {
                v.map(|u| RowValue::FixedLenByteArray(*u.as_bytes()))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::Json(values) => values
            .into_iter()
            .map(|v| {
                v.map(|j| RowValue::ByteArray(j.to_string().as_bytes().to_vec()))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
        ArrayCell::Bytes(values) => values
            .into_iter()
            .map(|v| {
                v.map(|b| RowValue::ByteArray(b.to_vec()))
                    .unwrap_or(RowValue::Null)
            })
            .collect(),
    }
}

impl From<PostgresTableRow> for MoonlinkRow {
    fn from(row: PostgresTableRow) -> Self {
        let mut values = Vec::new();
        for cell in row.0.values {
            match cell {
                Cell::I16(value) => {
                    values.push(RowValue::Int32(value as i32));
                }
                Cell::I32(value) => {
                    values.push(RowValue::Int32(value));
                }
                Cell::U32(value) => {
                    values.push(RowValue::Int32(value as i32));
                }
                Cell::I64(value) => {
                    values.push(RowValue::Int64(value));
                }
                Cell::F32(value) => {
                    values.push(RowValue::Float32(value));
                }
                Cell::F64(value) => {
                    values.push(RowValue::Float64(value));
                }
                Cell::Bool(value) => {
                    values.push(RowValue::Bool(value));
                }
                Cell::String(value) => {
                    values.push(RowValue::ByteArray(value.as_bytes().to_vec()));
                }
                Cell::Date(value) => {
                    values.push(RowValue::Int32(
                        value.signed_duration_since(ARROW_EPOCH).num_days() as i32,
                    ));
                }
                Cell::Time(value) => {
                    let seconds = value.num_seconds_from_midnight() as i64;
                    let nanos = value.nanosecond() as i64;
                    values.push(RowValue::Int64(seconds * 1_000_000 + nanos / 1_000))
                }
                Cell::TimeStamp(value) => {
                    values.push(RowValue::Int64(value.and_utc().timestamp_micros()))
                }
                Cell::TimeStampTz(value) => values.push(RowValue::Int64(value.timestamp_micros())),
                Cell::Uuid(value) => {
                    values.push(RowValue::FixedLenByteArray(*value.as_bytes()));
                }
                Cell::Json(value) => {
                    values.push(RowValue::ByteArray(value.to_string().as_bytes().to_vec()));
                }
                Cell::Bytes(value) => {
                    values.push(RowValue::ByteArray(value));
                }
                Cell::Array(value) => {
                    values.push(RowValue::Array(convert_array_cell(value)));
                }
                Cell::Numeric(value) => {
                    match value {
                        PgNumeric::Value(bigdecimal) => {
                            values.push(RowValue::Decimal(bigdecimal.to_i128().unwrap()));
                        }
                        _ => {
                            // DevNote:
                            // nan, inf, -inf will be converted to null
                            values.push(RowValue::Null);
                        }
                    }
                }
                Cell::Null => {
                    values.push(RowValue::Null);
                }
            }
        }
        MoonlinkRow::new(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_replicate::table::{ColumnSchema, LookupKey, TableName, TableSchema};
    use arrow::array::{Date32Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::DataType;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use moonlink::row::RowValue;

    #[test]
    fn test_table_schema_to_arrow_schema() {
        let table_schema = TableSchema {
            table_name: TableName {
                schema: "public".to_string(),
                name: "test_table".to_string(),
            },
            table_id: 1,
            column_schemas: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    typ: Type::INT4,
                    modifier: 0,
                    nullable: false,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    typ: Type::TEXT,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "created_at".to_string(),
                    typ: Type::TIMESTAMP,
                    modifier: 0,
                    nullable: true,
                },
            ],
            lookup_key: LookupKey::Key {
                name: "id".to_string(),
                columns: vec!["id".to_string()],
            },
        };

        let (arrow_schema, identity) = postgres_schema_to_moonlink_schema(&table_schema);

        assert_eq!(arrow_schema.fields().len(), 3);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int32);
        assert!(!arrow_schema.field(0).is_nullable());

        assert_eq!(arrow_schema.field(1).name(), "name");
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Utf8);
        assert!(arrow_schema.field(1).is_nullable());

        assert_eq!(arrow_schema.field(2).name(), "created_at");
        assert!(matches!(
            arrow_schema.field(2).data_type(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        ));
        assert!(arrow_schema.field(2).is_nullable());

        assert_eq!(identity, IdentityProp::SinglePrimitiveKey(0));
    }

    #[test]
    fn test_postgres_table_row_to_moonlink_row() {
        let postgres_table_row = PostgresTableRow(TableRow {
            values: vec![
                Cell::I32(1),
                Cell::I64(2),
                Cell::F32(std::f32::consts::PI),
                Cell::F64(std::f64::consts::E),
                Cell::Bool(true),
                Cell::String("test".to_string()),
                Cell::Date(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
                Cell::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
                Cell::TimeStamp(
                    NaiveDateTime::parse_from_str("2024-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")
                        .unwrap(),
                ),
                Cell::TimeStampTz(
                    DateTime::parse_from_rfc3339("2024-01-01T14:00:00+02:00")
                        .unwrap()
                        .with_timezone(&Utc),
                ),
                Cell::Null,
            ],
        });

        let moonlink_row: MoonlinkRow = postgres_table_row.into();
        assert_eq!(moonlink_row.values.len(), 11);
        assert_eq!(moonlink_row.values[0], RowValue::Int32(1));
        assert_eq!(moonlink_row.values[1], RowValue::Int64(2));
        assert_eq!(
            moonlink_row.values[2],
            RowValue::Float32(std::f32::consts::PI)
        );
        assert_eq!(
            moonlink_row.values[3],
            RowValue::Float64(std::f64::consts::E)
        );
        assert_eq!(moonlink_row.values[4], RowValue::Bool(true));
        let vec = "test".as_bytes().to_vec();
        assert_eq!(moonlink_row.values[5], RowValue::ByteArray(vec.clone()));
        let string = unsafe { std::str::from_utf8_unchecked(&vec) };
        let array = StringArray::from(vec![string]);
        assert_eq!(array.value(0), "test");
        assert_eq!(moonlink_row.values[6], RowValue::Int32(19723)); // 2024-01-01 days since epoch
        let array = Date32Array::from(vec![19723]);
        assert_eq!(
            array.value_as_date(0),
            Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(moonlink_row.values[7], RowValue::Int64(43200000000)); // 12:00:00 in microseconds
        assert_eq!(moonlink_row.values[8], RowValue::Int64(1704110400000000)); // 2024-01-01 12:00:00 in microseconds
        let array = TimestampMicrosecondArray::from(vec![1704110400000000]);
        assert_eq!(
            array.value_as_datetime(0),
            Some(
                NaiveDateTime::parse_from_str("2024-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        );
        assert_eq!(moonlink_row.values[9], RowValue::Int64(1704110400000000)); // 2024-01-01 12:00:00 UTC in microseconds
        let array = TimestampMicrosecondArray::from(vec![1704110400000000]);
        assert_eq!(
            array.value_as_datetime(0),
            Some(
                NaiveDateTime::parse_from_str("2024-01-01 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        );
        assert_eq!(moonlink_row.values[10], RowValue::Null);
    }

    #[test]
    fn test_postgres_array_to_moonlink_row() {
        let postgres_table_row = PostgresTableRow(TableRow {
            values: vec![
                // Array of integers
                Cell::Array(ArrayCell::I32(vec![Some(1), Some(2), Some(3)])),
                // Array of strings
                Cell::Array(ArrayCell::String(vec![
                    Some("hello".to_string()),
                    Some("world".to_string()),
                ])),
                // Array with null values
                Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)])),
                // Empty array
                Cell::Array(ArrayCell::I32(vec![])),
                // Array of booleans
                Cell::Array(ArrayCell::Bool(vec![Some(true), Some(false)])),
                // Array of timestamps
                Cell::Array(ArrayCell::TimeStamp(vec![
                    Some(
                        NaiveDateTime::parse_from_str("2024-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")
                            .unwrap(),
                    ),
                    Some(
                        NaiveDateTime::parse_from_str("2024-01-02 12:00:00", "%Y-%m-%d %H:%M:%S")
                            .unwrap(),
                    ),
                ])),
            ],
        });

        let moonlink_row: MoonlinkRow = postgres_table_row.into();
        assert_eq!(moonlink_row.values.len(), 6);

        // Test array of integers
        let int_array = match &moonlink_row.values[0] {
            RowValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(int_array.len(), 3);
        assert_eq!(int_array[0], RowValue::Int32(1));
        assert_eq!(int_array[1], RowValue::Int32(2));
        assert_eq!(int_array[2], RowValue::Int32(3));

        // Test array of strings
        let str_array = match &moonlink_row.values[1] {
            RowValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(str_array.len(), 2);
        assert_eq!(
            str_array[0],
            RowValue::ByteArray("hello".as_bytes().to_vec())
        );
        assert_eq!(
            str_array[1],
            RowValue::ByteArray("world".as_bytes().to_vec())
        );

        // Test array with null values
        let null_array = match &moonlink_row.values[2] {
            RowValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(null_array.len(), 3);
        assert_eq!(null_array[0], RowValue::Int32(1));
        assert_eq!(null_array[1], RowValue::Null);
        assert_eq!(null_array[2], RowValue::Int32(3));

        // Test empty array
        let empty_array = match &moonlink_row.values[3] {
            RowValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(empty_array.len(), 0);

        // Test array of booleans
        let bool_array = match &moonlink_row.values[4] {
            RowValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(bool_array.len(), 2);
        assert_eq!(bool_array[0], RowValue::Bool(true));
        assert_eq!(bool_array[1], RowValue::Bool(false));

        // Test array of timestamps
        let timestamp_array = match &moonlink_row.values[5] {
            RowValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        assert_eq!(timestamp_array.len(), 2);
        assert_eq!(
            timestamp_array[0],
            RowValue::Int64(1704110400000000) // 2024-01-01 12:00:00 in microseconds
        );
        assert_eq!(
            timestamp_array[1],
            RowValue::Int64(1704196800000000) // 2024-01-02 12:00:00 in microseconds
        );
    }
}
