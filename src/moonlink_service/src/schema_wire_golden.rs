//! Pins the GetTableSchema response: Postgres column types -> Arrow schema (moonlink_connectors
//! pg_replicate::util) -> Arrow IPC stream (`rpc_server::encode_table_schema`). duckdb_mooncake decodes
//! these bytes with nanoarrow and derives DuckDB column types from them, so a type-mapping change or
//! an arrow upgrade that alters the stream must fail here. Golden generated from upstream main 401c1d1.
//!
//! Field metadata is a HashMap, so streams with multi-key metadata (UUID/JSON extension fields) are not
//! byte-stable across runs. The byte golden therefore covers every type with only `PARQUET:field_id`;
//! the extension types are pinned structurally through the same encoder in
//! [`test_get_table_schema_type_mapping`]. Set `WIRE_GOLDEN_PRINT=1` to print the actual hex.

use crate::rpc_server::encode_table_schema;
use arrow_ipc::reader::StreamReader;
use moonlink_connectors::pg_replicate::table::{ColumnSchema, LookupKey, TableName, TableSchema};
use moonlink_connectors::pg_replicate::util::postgres_schema_to_moonlink_schema;
use tokio_postgres::types::{Field as PgField, Kind, Type};

/// Postgres typmod for numeric(precision, scale).
fn numeric_typmod(precision: i32, scale: i32) -> i32 {
    ((precision << 16) | scale) + 4
}

fn column(name: &str, typ: Type, modifier: i32, nullable: bool) -> ColumnSchema {
    ColumnSchema {
        name: name.to_string(),
        typ,
        modifier,
        nullable,
    }
}

fn table_schema(column_schemas: Vec<ColumnSchema>) -> TableSchema {
    TableSchema {
        table_name: TableName {
            schema: "public".to_string(),
            name: "all_types".to_string(),
        },
        src_table_id: 16384,
        column_schemas,
        lookup_key: LookupKey::Key {
            name: "all_types_pkey".to_string(),
            columns: vec!["c_int8".to_string()],
        },
    }
}

/// Every type `postgres_primitive_to_arrow_type` maps whose Arrow field carries a single metadata key.
fn byte_stable_columns() -> Vec<ColumnSchema> {
    vec![
        column("c_bool", Type::BOOL, -1, true),
        column("c_int2", Type::INT2, -1, true),
        column("c_int4", Type::INT4, -1, true),
        column("c_int8", Type::INT8, -1, false),
        column("c_float4", Type::FLOAT4, -1, true),
        column("c_float8", Type::FLOAT8, -1, true),
        column(
            "c_numeric_10_2",
            Type::NUMERIC,
            numeric_typmod(10, 2),
            false,
        ),
        column("c_numeric", Type::NUMERIC, -1, true),
        column("c_varchar", Type::VARCHAR, 36, true),
        column("c_text", Type::TEXT, -1, true),
        column("c_bpchar", Type::BPCHAR, 8, true),
        column("c_char", Type::CHAR, -1, true),
        column("c_name", Type::NAME, -1, true),
        column("c_date", Type::DATE, -1, true),
        column("c_timestamp", Type::TIMESTAMP, -1, true),
        column("c_timestamptz", Type::TIMESTAMPTZ, -1, true),
        column("c_time", Type::TIME, -1, true),
        column("c_timetz", Type::TIMETZ, -1, true),
        column("c_bytea", Type::BYTEA, -1, true),
        column("c_oid", Type::OID, -1, true),
        column("c_inet_fallback", Type::INET, -1, true),
        column("c_int4_array", Type::INT4_ARRAY, -1, true),
        column("c_text_array", Type::TEXT_ARRAY, -1, true),
    ]
}

const SCHEMA_GOLDEN: &str = "ffffffff380b00001000000000000a000c000a00090004000a00000010000000000104000800080000000400080000000400000017000000680a0000e4090000780900000c090000880800000c0800009807000028070000c00600005c060000f4050000900500002c050000c404000050040000cc03000058030000e80200008402000018020000ac010000d40000000400000006f6ffff94000000180000000c0000000000010c700000000100000008000000f8f5ffff2af6ffff28000000140000000c000000000001050c0000000000000018f6ffff040000006974656d00000000010000000400000014f6ffff080000000c000000020000003233000010000000504152515545543a6669656c645f6964000000000c000000635f746578745f61727261790000000001000000040000005cf6ffff080000000c000000020000003234000010000000504152515545543a6669656c645f696400000000d2f6ffff9c000000180000000c0000000000010c780000000100000008000000c4f6fffff6f6ffff300000001000000018000000000001021400000064f7ffff200000000000000100000000040000006974656d000000000100000004000000e8f6ffff080000000c000000020000003231000010000000504152515545543a6669656c645f6964000000000c000000635f696e74345f617272617900000000010000000400000030f7ffff080000000c000000020000003232000010000000504152515545543a6669656c645f696400000000a6f7ffff30000000140000000c000000000001050c0000000000000094f7ffff0f000000635f696e65745f66616c6c6261636b00010000000400000098f7ffff080000000c000000020000003230000010000000504152515545543a6669656c645f6964000000000ef8ffff30000000100000001800000000000102140000007cf8ffff40000000000000010000000005000000635f6f6964000000010000000400000000f8ffff080000000c000000020000003139000010000000504152515545543a6669656c645f69640000000076f8ffff28000000140000000c000000000001040c0000000000000064f8ffff07000000635f627974656100010000000400000060f8ffff080000000c000000020000003138000010000000504152515545543a6669656c645f696400000000d6f8ffff34000000140000000c00000000000109140000000000000094ffffff000002004000000008000000635f74696d65747a000000000100000004000000ccf8ffff080000000c000000020000003137000010000000504152515545543a6669656c645f69640000000042f9ffff380000001c0000000c000000000001091c0000000000000008000c000600080008000000000002004000000006000000635f74696d65000001000000040000003cf9ffff080000000c000000020000003136000010000000504152515545543a6669656c645f696400000000b2f9ffff480000001c0000000c0000000000010a240000000000000008000c000a00040008000000080000000000020003000000555443000d000000635f74696d657374616d70747a0000000100000004000000bcf9ffff080000000c000000020000003135000010000000504152515545543a6669656c645f69640000000032faffff38000000140000000c0000000000010a1800000000000000f2fbffff0000020000000000000000000b000000635f74696d657374616d700001000000040000002cfaffff080000000c000000020000003134000010000000504152515545543a6669656c645f696400000000a2faffff2c000000140000000c00000000000108100000000000000062fcffff0000000006000000635f646174650000010000000400000090faffff080000000c000000020000003133000010000000504152515545543a6669656c645f69640000000006fbffff28000000140000000c000000000001050c00000000000000f4faffff06000000635f6e616d6500000100000004000000f0faffff080000000c000000020000003132000010000000504152515545543a6669656c645f69640000000066fbffff28000000140000000c000000000001050c0000000000000054fbffff06000000635f636861720000010000000400000050fbffff080000000c000000020000003131000010000000504152515545543a6669656c645f696400000000c6fbffff2c000000140000000c000000000001050c00000000000000b4fbffff08000000635f627063686172000000000100000004000000b4fbffff080000000c000000020000003130000010000000504152515545543a6669656c645f6964000000002afcffff28000000140000000c000000000001050c0000000000000018fcffff06000000635f746578740000010000000400000014fcffff080000000c000000010000003900000010000000504152515545543a6669656c645f6964000000008afcffff2c000000140000000c000000000001050c0000000000000078fcffff09000000635f76617263686172000000010000000400000078fcffff080000000c000000010000003800000010000000504152515545543a6669656c645f696400000000eefcffff34000000140000000c000000000001071400000000000000c0fcffff0a0000002600000009000000635f6e756d657269630000000100000004000000e4fcffff080000000c000000010000003700000010000000504152515545543a6669656c645f6964000000005afdffff38000000140000000c0000000000010714000000000000002cfdffff020000000a0000000e000000635f6e756d657269635f31305f320000010000000400000054fdffff080000000c000000010000003600000010000000504152515545543a6669656c645f696400000000cafdffff300000001000000014000000000001031000000086ffffff000002000000000008000000635f666c6f617438000000000100000004000000bcfdffff080000000c000000010000003500000010000000504152515545543a6669656c645f6964000012001a00140012001300080000000c0004001200000038000000180000001c0000000000010318000000000006000800060006000000000001000000000008000000635f666c6f6174340000000001000000040000003cfeffff080000000c000000010000003400000010000000504152515545543a6669656c645f6964000012001800140000001300080000000c00040012000000300000001000000018000000000000021400000030ffffff40000000000000010000000006000000635f696e743800000100000004000000b4feffff080000000c000000010000003300000010000000504152515545543a6669656c645f6964000000002affffff300000001000000018000000000001021400000098ffffff20000000000000010000000006000000635f696e7434000001000000040000001cffffff080000000c000000010000003200000010000000504152515545543a6669656c645f69640000000092ffffff380000001800000020000000000001021c00000008000c0004000b000800000010000000000000010000000006000000635f696e7432000001000000040000008cffffff080000000c000000010000003100000010000000504152515545543a6669656c645f6964000012001800140012001300080000000c000400120000002c000000180000000c000000000001061000000000000000040004000400000006000000635f626f6f6c0000010000000c00000008000c000800040008000000080000000c000000010000003000000010000000504152515545543a6669656c645f69640000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ffffffff00000000";

#[test]
fn test_get_table_schema_ipc_golden() {
    let (schema, _) = postgres_schema_to_moonlink_schema(&table_schema(byte_stable_columns()));
    let bytes = encode_table_schema(&schema).unwrap();
    let actual: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    if std::env::var_os("WIRE_GOLDEN_PRINT").is_some() {
        println!("{actual}");
    }
    // IPC stream framing nanoarrow expects: continuation marker first, end-of-stream marker last.
    assert_eq!(&bytes[..4], &[0xff, 0xff, 0xff, 0xff]);
    assert_eq!(
        &bytes[bytes.len() - 8..],
        &[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]
    );
    assert_eq!(actual, SCHEMA_GOLDEN, "GetTableSchema IPC stream drift");
}

/// (column name, Arrow data type, nullable, sorted field metadata) for every mapped Postgres type,
/// decoded back from the IPC stream the server sends.
#[test]
fn test_get_table_schema_type_mapping() {
    let mood = Type::new(
        "mood".to_string(),
        90001,
        Kind::Enum(vec!["sad".to_string(), "happy".to_string()]),
        "public".to_string(),
    );
    let point2 = Type::new(
        "point2".to_string(),
        90002,
        Kind::Composite(vec![
            PgField::new("x".to_string(), Type::INT4),
            PgField::new("y".to_string(), Type::FLOAT8),
        ]),
        "public".to_string(),
    );
    let mut columns = byte_stable_columns();
    columns.extend([
        column("c_uuid", Type::UUID, -1, true),
        column("c_json", Type::JSON, -1, true),
        column("c_jsonb", Type::JSONB, -1, true),
        column("c_enum", mood, -1, true),
        column("c_composite", point2, -1, true),
    ]);
    let (schema, _) = postgres_schema_to_moonlink_schema(&table_schema(columns));
    let bytes = encode_table_schema(&schema).unwrap();
    let decoded = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .unwrap()
        .schema();

    let actual: Vec<String> = decoded
        .fields()
        .iter()
        .map(|f| {
            let mut metadata: Vec<String> = f
                .metadata()
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            metadata.sort();
            format!(
                "{} {:?} nullable={} [{}]",
                f.name(),
                f.data_type(),
                f.is_nullable(),
                metadata.join(", ")
            )
        })
        .collect();
    if std::env::var_os("WIRE_GOLDEN_PRINT").is_some() {
        println!("MAPPING_BEGIN\n{}\nMAPPING_END", actual.join("\n"));
    }
    let expected = EXPECTED_MAPPING
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty());
    assert_eq!(actual, expected.collect::<Vec<_>>());
}

const EXPECTED_MAPPING: &str = r#"
    c_bool Boolean nullable=true [PARQUET:field_id=0]
    c_int2 Int16 nullable=true [PARQUET:field_id=1]
    c_int4 Int32 nullable=true [PARQUET:field_id=2]
    c_int8 Int64 nullable=false [PARQUET:field_id=3]
    c_float4 Float32 nullable=true [PARQUET:field_id=4]
    c_float8 Float64 nullable=true [PARQUET:field_id=5]
    c_numeric_10_2 Decimal128(10, 2) nullable=true [PARQUET:field_id=6]
    c_numeric Decimal128(38, 10) nullable=true [PARQUET:field_id=7]
    c_varchar Utf8 nullable=true [PARQUET:field_id=8]
    c_text Utf8 nullable=true [PARQUET:field_id=9]
    c_bpchar Utf8 nullable=true [PARQUET:field_id=10]
    c_char Utf8 nullable=true [PARQUET:field_id=11]
    c_name Utf8 nullable=true [PARQUET:field_id=12]
    c_date Date32 nullable=true [PARQUET:field_id=13]
    c_timestamp Timestamp(Microsecond, None) nullable=true [PARQUET:field_id=14]
    c_timestamptz Timestamp(Microsecond, Some("UTC")) nullable=true [PARQUET:field_id=15]
    c_time Time64(Microsecond) nullable=true [PARQUET:field_id=16]
    c_timetz Time64(Microsecond) nullable=true [PARQUET:field_id=17]
    c_bytea Binary nullable=true [PARQUET:field_id=18]
    c_oid Int64 nullable=true [PARQUET:field_id=19]
    c_inet_fallback Utf8 nullable=true [PARQUET:field_id=20]
    c_int4_array List(Field { name: "item", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "21"} }) nullable=true [PARQUET:field_id=22]
    c_text_array List(Field { name: "item", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "23"} }) nullable=true [PARQUET:field_id=24]
    c_uuid FixedSizeBinary(16) nullable=true [ARROW:extension:name=arrow.uuid, PARQUET:field_id=25]
    c_json Utf8 nullable=true [ARROW:extension:metadata=, ARROW:extension:name=arrow.json, PARQUET:field_id=26]
    c_jsonb Utf8 nullable=true [ARROW:extension:metadata=, ARROW:extension:name=arrow.json, PARQUET:field_id=27]
    c_enum Utf8 nullable=true []
    c_composite Struct([Field { name: "x", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "28"} }, Field { name: "y", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "29"} }]) nullable=true [PARQUET:field_id=30]
"#;
