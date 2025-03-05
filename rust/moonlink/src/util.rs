use arrow::datatypes::{DataType, Field, Schema};
use pg_replicate::table::TableSchema;
use tokio_postgres::types::Type;

/// Convert a PostgreSQL TableSchema to an Arrow Schema
pub fn table_schema_to_arrow_schema(table_schema: &TableSchema) -> Schema {
    let fields: Vec<Field> = table_schema
        .column_schemas
        .iter()
        .map(|col| {
            let data_type = match col.typ {
                Type::BOOL => DataType::Boolean,
                Type::INT2 => DataType::Int16,
                Type::INT4 => DataType::Int32,
                Type::INT8 => DataType::Int64,
                Type::FLOAT4 => DataType::Float32,
                Type::FLOAT8 => DataType::Float64,
                Type::NUMERIC => DataType::Float64, // Simplified mapping
                Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::CHAR | Type::NAME => {
                    DataType::Utf8
                }
                Type::DATE => DataType::Date32,
                Type::TIMESTAMP => {
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
                }
                Type::TIMESTAMPTZ => {
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
                }
                Type::TIME => DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                Type::TIMETZ => DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                Type::UUID => DataType::FixedSizeBinary(16),
                Type::JSON | Type::JSONB => DataType::Utf8,
                Type::BYTEA => DataType::Binary,
                _ => DataType::Utf8, // Default to string for unknown types
            };

            Field::new(&col.name, data_type, col.nullable)
        })
        .collect();

    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_replicate::table::{ColumnSchema, TableName, TableSchema};

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
                    primary: true,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    typ: Type::TEXT,
                    modifier: 0,
                    nullable: true,
                    primary: false,
                },
                ColumnSchema {
                    name: "created_at".to_string(),
                    typ: Type::TIMESTAMP,
                    modifier: 0,
                    nullable: true,
                    primary: false,
                },
            ],
        };

        let arrow_schema = table_schema_to_arrow_schema(&table_schema);

        assert_eq!(arrow_schema.fields().len(), 3);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(arrow_schema.field(0).is_nullable(), false);

        assert_eq!(arrow_schema.field(1).name(), "name");
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(arrow_schema.field(1).is_nullable(), true);

        assert_eq!(arrow_schema.field(2).name(), "created_at");
        assert!(matches!(
            arrow_schema.field(2).data_type(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        ));
        assert_eq!(arrow_schema.field(2).is_nullable(), true);
    }
}
