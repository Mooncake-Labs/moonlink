use crate::pg_replicate::{
    conversions::{numeric::PgNumeric, table_row::TableRow, Cell},
    table::{LookupKey, TableSchema},
};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::Timelike;
use moonlink::row::RowValue;
use moonlink::row::{Identity, MoonlinkRow};
use num_traits::cast::ToPrimitive;
use std::collections::HashMap;
use tokio_postgres::types::Type;

/// Convert a PostgreSQL TableSchema to an Arrow Schema
pub fn postgres_schema_to_moonlink_schema(table_schema: &TableSchema) -> (Schema, Identity) {
    let fields: Vec<Field> = table_schema
        .column_schemas
        .iter()
        .map(|col| {
            let (data_type, extension_name) = match col.typ {
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
                    DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Microsecond,
                        Some("UTC".into()),
                    ),
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

            let mut field = Field::new(&col.name, data_type, col.nullable);

            // Apply extension type if specified
            if let Some(ext_name) = extension_name {
                let mut metadata = HashMap::new();
                metadata.insert("ARROW:extension:name".to_string(), ext_name);
                field = field.with_metadata(metadata);
            }

            field
        })
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
            Identity::new_key(columns, &fields)
        }
        LookupKey::FullRow => Identity::FullRow,
    };
    (Schema::new(fields), identity)
}

pub fn _table_schema_to_iceberg_schema(_table_schema: &TableSchema) -> Schema {
    todo!("Iceberg: convert postgres table schema to iceberg schema!")
}

pub(crate) struct PostgresTableRow(pub TableRow);

const ARROW_EPOCH: chrono::NaiveDate = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

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
                Cell::Array(_value) => {
                    todo!("Moonlink: Array")
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

        assert_eq!(identity, Identity::SinglePrimitiveKey(0));
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
}
