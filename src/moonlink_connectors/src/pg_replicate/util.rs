use crate::pg_replicate::{
    conversions::{numeric::PgNumeric, table_row::TableRow, ArrayCell, Cell},
    table::{LookupKey, TableSchema},
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_schema::extension::{ExtensionType, Json as ArrowJson, Uuid as ArrowUuid};
use arrow_schema::{DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE};
use chrono::Timelike;
use moonlink::row::RowValue;
use moonlink::row::{IdentityProp, MoonlinkRow};
use num_traits::cast::ToPrimitive;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::types::{Kind, Type};

fn numeric_precision_scale(modifier: i32) -> Option<(u8, i8)> {
    const VARHDRSZ: i32 = 4;
    if modifier < VARHDRSZ {
        return None;
    }
    let typmod = modifier - VARHDRSZ;
    // Derived from: [https://github.com/postgres/postgres/blob/4fbb46f61271f4b7f46ecad3de608fc2f4d7d80f/src/backend/utils/adt/numeric.c#L929v]
    let precision = ((typmod >> 16) & 0xffff) as u8;
    // Derived from: [https://github.com/postgres/postgres/blob/4fbb46f61271f4b7f46ecad3de608fc2f4d7d80f/src/backend/utils/adt/numeric.c#L944]
    let raw_scale = (typmod & 0x7fff);
    let scale = ((raw_scale ^ 1024) - 1024) as i8;
    Some((precision, scale))
}

enum ArrowExtensionType {
    Uuid,
    Json,
}

fn postgres_primitive_to_arrow_type(
    typ: &Type,
    modifier: i32,
    name: &str,
    mut nullable: bool,
    field_id: &mut i32,
) -> Field {
    let (data_type, extension_name) = match *typ {
        Type::BOOL => (DataType::Boolean, None),
        Type::INT2 => (DataType::Int16, None),
        Type::INT4 => (DataType::Int32, None),
        Type::INT8 => (DataType::Int64, None),
        Type::FLOAT4 => (DataType::Float32, None),
        Type::FLOAT8 => (DataType::Float64, None),
        Type::NUMERIC => {
            // Numeric type can contain invalid values, we will cast them to NULL
            // so make it nullable.
            nullable = true;
            let (precision, scale) = numeric_precision_scale(modifier)
                .unwrap_or((DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE));
            (DataType::Decimal128(precision, scale), None)
        }
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
        Type::UUID => (
            DataType::FixedSizeBinary(16),
            Some(ArrowExtensionType::Uuid),
        ),
        Type::JSON | Type::JSONB => (DataType::Utf8, Some(ArrowExtensionType::Json)),
        Type::BYTEA => (DataType::Binary, None),
        _ => (DataType::Utf8, None), // Default to string for unknown types
    };

    let mut field = Field::new(name, data_type, nullable);
    let mut metadata = HashMap::new();
    metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
    *field_id += 1;
    field = field.with_metadata(metadata);

    // Apply extension type if specified
    if let Some(ext_name) = extension_name {
        match ext_name {
            ArrowExtensionType::Uuid => {
                field = field.with_extension_type(ArrowUuid::default());
            }
            ArrowExtensionType::Json => {
                field = field.with_extension_type(ArrowJson::default());
            }
        }
    }

    field
}

fn postgres_type_to_arrow_type(
    typ: &Type,
    modifier: i32,
    name: &str,
    nullable: bool,
    field_id: &mut i32,
) -> Field {
    match typ.kind() {
        Kind::Simple => postgres_primitive_to_arrow_type(typ, modifier, name, nullable, field_id),
        Kind::Array(inner) => {
            let item_type = postgres_type_to_arrow_type(
                inner, /*modifier=*/ -1, /*name=*/ "item", /*nullable=*/ true,
                field_id,
            );
            let field = Field::new_list(name, Arc::new(item_type), nullable);
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            *field_id += 1;
            field.with_metadata(metadata)
        }
        Kind::Composite(fields) => {
            let fields: Vec<Field> = fields
                .iter()
                .map(|f| {
                    postgres_type_to_arrow_type(
                        f.type_(),
                        /*modifier=*/ -1,
                        f.name(),
                        /*nullable=*/ true,
                        field_id,
                    )
                })
                .collect();
            Field::new_struct(name, fields, nullable)
        }
        Kind::Enum(_) => Field::new(name, DataType::Utf8, nullable),
        _ => {
            todo!("Unsupported type: {:?}", typ);
        }
    }
}

/// Convert a PostgreSQL TableSchema to an Arrow Schema
pub fn postgres_schema_to_moonlink_schema(table_schema: &TableSchema) -> (Schema, IdentityProp) {
    let mut field_id = 0; // Used to indicate different columns, including internal fields within complex type.
    let fields: Vec<Field> = table_schema
        .column_schemas
        .iter()
        .map(|col| {
            postgres_type_to_arrow_type(
                &col.typ,
                col.modifier,
                &col.name,
                col.nullable,
                &mut field_id,
            )
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
        let mut values = Vec::with_capacity(row.0.values.len());
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
                            let (int_val, scale) = bigdecimal.as_bigint_and_exponent();
                            let multiplier = 10_i128.pow(scale as u32);
                            if let Some(scaled_integer) =
                                int_val.to_i128().and_then(|v| v.checked_mul(multiplier))
                            {
                                values.push(RowValue::Decimal(scaled_integer));
                            } else {
                                values.push(RowValue::Null); // handle overflow safely
                                eprintln!(
                                    "Decimal value too large to fit in i128 — storing as NULL"
                                );
                            }
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
    use iceberg::arrow as IcebergArrow;
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
                    name: "bool_field".to_string(),
                    typ: Type::BOOL,
                    modifier: 0,
                    nullable: false,
                },
                ColumnSchema {
                    name: "int2_field".to_string(),
                    typ: Type::INT2,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "int4_field".to_string(),
                    typ: Type::INT4,
                    modifier: 0,
                    nullable: false,
                },
                ColumnSchema {
                    name: "int8_field".to_string(),
                    typ: Type::INT8,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "float4_field".to_string(),
                    typ: Type::FLOAT4,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "float8_field".to_string(),
                    typ: Type::FLOAT8,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "numeric_field".to_string(),
                    typ: Type::NUMERIC,
                    modifier: ((12 << 16) | 5) + 4, // NUMERIC(12,5)
                    nullable: true,
                },
                ColumnSchema {
                    name: "varchar_field".to_string(),
                    typ: Type::VARCHAR,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "text_field".to_string(),
                    typ: Type::TEXT,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "bpchar_field".to_string(),
                    typ: Type::BPCHAR,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "char_field".to_string(),
                    typ: Type::CHAR,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "name_field".to_string(),
                    typ: Type::NAME,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "date_field".to_string(),
                    typ: Type::DATE,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "timestamp_field".to_string(),
                    typ: Type::TIMESTAMP,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "timestamptz_field".to_string(),
                    typ: Type::TIMESTAMPTZ,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "time_field".to_string(),
                    typ: Type::TIME,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "timetz_field".to_string(),
                    typ: Type::TIMETZ,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "uuid_field".to_string(),
                    typ: Type::UUID,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "json_field".to_string(),
                    typ: Type::JSON,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "jsonb_field".to_string(),
                    typ: Type::JSONB,
                    modifier: 0,
                    nullable: true,
                },
                ColumnSchema {
                    name: "bytea_field".to_string(),
                    typ: Type::BYTEA,
                    modifier: 0,
                    nullable: true,
                },
                // Array type.
                ColumnSchema {
                    name: "bool_array_field".to_string(),
                    typ: Type::BOOL_ARRAY,
                    modifier: 0,
                    nullable: true,
                },
                // TODO(hjiang): Add composite type handling.
            ],
            lookup_key: LookupKey::Key {
                name: "uuid_field".to_string(),
                columns: vec!["uuid_field".to_string()],
            },
        };

        let (arrow_schema, identity) = postgres_schema_to_moonlink_schema(&table_schema);
        assert_eq!(arrow_schema.fields().len(), 22);

        assert_eq!(arrow_schema.field(0).name(), "bool_field");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Boolean);
        assert!(!arrow_schema.field(0).is_nullable());

        assert_eq!(arrow_schema.field(1).name(), "int2_field");
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Int16);
        assert!(arrow_schema.field(1).is_nullable());

        assert_eq!(arrow_schema.field(2).name(), "int4_field");
        assert_eq!(arrow_schema.field(2).data_type(), &DataType::Int32);
        assert!(!arrow_schema.field(2).is_nullable());

        assert_eq!(arrow_schema.field(3).name(), "int8_field");
        assert_eq!(arrow_schema.field(3).data_type(), &DataType::Int64);
        assert!(arrow_schema.field(3).is_nullable());

        assert_eq!(arrow_schema.field(4).name(), "float4_field");
        assert_eq!(arrow_schema.field(4).data_type(), &DataType::Float32);

        assert_eq!(arrow_schema.field(5).name(), "float8_field");
        assert_eq!(arrow_schema.field(5).data_type(), &DataType::Float64);

        assert_eq!(arrow_schema.field(6).name(), "numeric_field");
        assert_eq!(
            arrow_schema.field(6).data_type(),
            &DataType::Decimal128(12, 5)
        );

        assert_eq!(arrow_schema.field(7).name(), "varchar_field");
        assert_eq!(arrow_schema.field(7).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(8).name(), "text_field");
        assert_eq!(arrow_schema.field(8).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(9).name(), "bpchar_field");
        assert_eq!(arrow_schema.field(9).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(10).name(), "char_field");
        assert_eq!(arrow_schema.field(10).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(11).name(), "name_field");
        assert_eq!(arrow_schema.field(11).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(12).name(), "date_field");
        assert_eq!(arrow_schema.field(12).data_type(), &DataType::Date32);

        assert_eq!(arrow_schema.field(13).name(), "timestamp_field");
        assert!(matches!(
            arrow_schema.field(13).data_type(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        ));

        assert_eq!(arrow_schema.field(14).name(), "timestamptz_field");
        assert!(matches!(
            arrow_schema.field(14).data_type(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(_))
        ));

        assert_eq!(arrow_schema.field(15).name(), "time_field");
        assert_eq!(
            arrow_schema.field(15).data_type(),
            &DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
        );

        assert_eq!(arrow_schema.field(16).name(), "timetz_field");
        assert_eq!(
            arrow_schema.field(16).data_type(),
            &DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)
        );

        assert_eq!(arrow_schema.field(17).name(), "uuid_field");
        assert_eq!(
            arrow_schema.field(17).data_type(),
            &DataType::FixedSizeBinary(16)
        );

        assert_eq!(arrow_schema.field(18).name(), "json_field");
        assert_eq!(arrow_schema.field(18).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(19).name(), "jsonb_field");
        assert_eq!(arrow_schema.field(19).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(20).name(), "bytea_field");
        assert_eq!(arrow_schema.field(20).data_type(), &DataType::Binary);

        assert_eq!(arrow_schema.field(21).name(), "bool_array_field");
        let mut expected_field = Field::new("item", DataType::Boolean, /*nullable=*/ true);
        let mut field_metadata = HashMap::new();
        field_metadata.insert("PARQUET:field_id".to_string(), "21".to_string());
        expected_field.set_metadata(field_metadata);
        assert_eq!(
            arrow_schema.field(21).data_type(),
            &DataType::List(expected_field.into()),
        );

        // Check identity property.
        assert_eq!(identity, IdentityProp::Keys(vec![17]));

        // Convert Arrow schema to Iceberg schema and check field id/name mapping.
        let iceberg_arrow = IcebergArrow::arrow_schema_to_schema(&arrow_schema).unwrap();
        for (field_id, expected_name) in [
            (0, "bool_field"),
            (1, "int2_field"),
            (2, "int4_field"),
            (3, "int8_field"),
            (4, "float4_field"),
            (5, "float8_field"),
            (6, "numeric_field"),
            (7, "varchar_field"),
            (8, "text_field"),
            (9, "bpchar_field"),
            (10, "char_field"),
            (11, "name_field"),
            (12, "date_field"),
            (13, "timestamp_field"),
            (14, "timestamptz_field"),
            (15, "time_field"),
            (16, "timetz_field"),
            (17, "uuid_field"),
            (18, "json_field"),
            (19, "jsonb_field"),
            (20, "bytea_field"),
            (21, "bool_array_field.element"),
            (22, "bool_array_field"),
        ] {
            assert_eq!(
                iceberg_arrow.name_by_field_id(field_id).unwrap(),
                expected_name
            );
        }
        assert!(iceberg_arrow.name_by_field_id(23).is_none());
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
                Cell::Uuid(uuid::Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap()),
            ],
        });

        let moonlink_row: MoonlinkRow = postgres_table_row.into();
        assert_eq!(moonlink_row.values.len(), 12);
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
        if let RowValue::FixedLenByteArray(bytes) = moonlink_row.values[11] {
            assert_eq!(
                uuid::Uuid::from_bytes(bytes).to_string(),
                "123e4567-e89b-12d3-a456-426614174000"
            );
        } else {
            panic!("Expected fixed length byte array");
        };
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
