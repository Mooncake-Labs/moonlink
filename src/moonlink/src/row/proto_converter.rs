use crate::row::{MoonlinkRow, RowValue};
use moonlink_proto::moonlink as proto;

pub fn moonlink_row_to_proto(row: &MoonlinkRow) -> proto::MoonlinkRow {
    proto::MoonlinkRow {
        values: row.values.iter().map(row_value_to_proto).collect(),
    }
}

pub fn proto_to_moonlink_row(p: &proto::MoonlinkRow) -> MoonlinkRow {
    MoonlinkRow::new(p.values.iter().map(proto_to_row_value).collect())
}

fn row_value_to_proto(v: &RowValue) -> proto::RowValue {
    use proto::row_value::Kind;
    let kind = match v {
        RowValue::Int32(x) => Kind::Int32(*x),
        RowValue::Int64(x) => Kind::Int64(*x),
        RowValue::Float32(x) => Kind::Float32(*x),
        RowValue::Float64(x) => Kind::Float64(*x),
        RowValue::Decimal(x) => Kind::Decimal128Be(x.to_be_bytes().to_vec()),
        RowValue::Bool(x) => Kind::Bool(*x),
        RowValue::ByteArray(b) => Kind::Bytes(b.clone().into()),
        RowValue::FixedLenByteArray(arr) => Kind::FixedLenBytes(arr.to_vec().into()),
        RowValue::Array(items) => Kind::Array(proto::Array {
            values: items.iter().map(row_value_to_proto).collect(),
        }),
        RowValue::Struct(fields) => Kind::Struct(proto::Struct {
            fields: fields.iter().map(row_value_to_proto).collect(),
        }),
        RowValue::Null => Kind::Null(proto::Null {}),
    };
    proto::RowValue { kind: Some(kind) }
}

fn proto_to_row_value(p: &proto::RowValue) -> RowValue {
    use proto::row_value::Kind;
    match p.kind.as_ref().expect("RowValue.kind is required") {
        Kind::Int32(x) => RowValue::Int32(*x),
        Kind::Int64(x) => RowValue::Int64(*x),
        Kind::Float32(x) => RowValue::Float32(*x),
        Kind::Float64(x) => RowValue::Float64(*x),
        Kind::Decimal128Be(bytes) => {
            let mut arr = [0u8; 16];
            let copy_len = bytes.len().min(16);
            arr[16 - copy_len..].copy_from_slice(&bytes[bytes.len() - copy_len..]);
            RowValue::Decimal(i128::from_be_bytes(arr))
        }
        Kind::Bool(x) => RowValue::Bool(*x),
        Kind::Bytes(b) => RowValue::ByteArray(b.clone().to_vec()),
        Kind::FixedLenBytes(b) => {
            assert!(b.len() == 16, "fixed_len_bytes must be 16 bytes");
            let mut arr = [0u8; 16];
            arr.copy_from_slice(b);
            RowValue::FixedLenByteArray(arr)
        }
        Kind::Array(a) => RowValue::Array(a.values.iter().map(proto_to_row_value).collect()),
        Kind::Struct(s) => RowValue::Struct(s.fields.iter().map(proto_to_row_value).collect()),
        Kind::Null(_) => RowValue::Null,
    }
}
