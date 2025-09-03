use crate::row::{MoonlinkRow, RowValue};
use crate::{Error, Result};
use moonlink_proto::moonlink as moonlink_pb;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtoToMoonlinkRowError {
    #[error("invalid value for field: {0}")]
    InvalidValue(String),
}

pub fn moonlink_row_to_proto(row: MoonlinkRow) -> moonlink_pb::MoonlinkRow {
    moonlink_pb::MoonlinkRow {
        values: row.values.into_iter().map(row_value_to_proto).collect(),
    }
}

pub fn proto_to_moonlink_row(p: moonlink_pb::MoonlinkRow) -> Result<MoonlinkRow> {
    Ok(MoonlinkRow::new(
        p.values
            .into_iter()
            .map(proto_to_row_value)
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn row_value_to_proto(v: RowValue) -> moonlink_pb::RowValue {
    match v {
        RowValue::Int32(x) => moonlink_pb::RowValue::int32(x),
        RowValue::Int64(x) => moonlink_pb::RowValue::int64(x),
        RowValue::Float32(x) => moonlink_pb::RowValue::float32(x),
        RowValue::Float64(x) => moonlink_pb::RowValue::float64(x),
        RowValue::Decimal(x) => moonlink_pb::RowValue::decimal128_be(x.to_be_bytes().to_vec()),
        RowValue::Bool(x) => moonlink_pb::RowValue::bool(x),
        RowValue::ByteArray(b) => moonlink_pb::RowValue::bytes(b),
        RowValue::FixedLenByteArray(arr) => moonlink_pb::RowValue::fixed_len_bytes(Vec::from(arr)),
        RowValue::Array(items) => moonlink_pb::RowValue::array(moonlink_pb::Array {
            values: items.into_iter().map(row_value_to_proto).collect(),
        }),
        RowValue::Struct(fields) => moonlink_pb::RowValue::struct_(moonlink_pb::Struct {
            fields: fields.into_iter().map(row_value_to_proto).collect(),
        }),
        RowValue::Null => moonlink_pb::RowValue::null(),
    }
}

fn proto_to_row_value(p: moonlink_pb::RowValue) -> Result<RowValue> {
    use moonlink_pb::row_value::Kind;
    let row_value = match p.kind.expect("RowValue.kind is required") {
        Kind::Int32(x) => RowValue::Int32(x),
        Kind::Int64(x) => RowValue::Int64(x),
        Kind::Float32(x) => RowValue::Float32(x),
        Kind::Float64(x) => RowValue::Float64(x),
        Kind::Decimal128Be(bytes) => {
            if bytes.len() != 16 {
                return Err(Error::pb_conversion_error(
                    "decimal128_be must be 16 bytes".to_string(),
                ));
            }
            let arr: [u8; 16] = bytes.as_slice().try_into().unwrap();
            RowValue::Decimal(i128::from_be_bytes(arr))
        }
        Kind::Bool(x) => RowValue::Bool(x),
        Kind::Bytes(b) => RowValue::ByteArray(b.to_vec()),
        Kind::FixedLenBytes(b) => {
            assert_eq!(b.len(), 16, "fixed_len_bytes must be 16 bytes");
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&b);
            RowValue::FixedLenByteArray(arr)
        }
        Kind::Array(a) => RowValue::Array(
            a.values
                .into_iter()
                .map(proto_to_row_value)
                .collect::<Result<Vec<_>>>()?,
        ),
        Kind::Struct(s) => RowValue::Struct(
            s.fields
                .into_iter()
                .map(proto_to_row_value)
                .collect::<Result<Vec<_>>>()?,
        ),
        Kind::Null(_) => RowValue::Null,
    };
    Ok(row_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proto_roundtrip_basic() {
        let row = MoonlinkRow::new(vec![
            RowValue::Int32(1),
            RowValue::Int64(2),
            RowValue::Float32(3.5),
            RowValue::Float64(4.5),
            RowValue::Bool(true),
            RowValue::ByteArray(b"abc".to_vec()),
            RowValue::FixedLenByteArray(*b"0123456789abcdef"),
            RowValue::Array(vec![RowValue::Int32(9), RowValue::Null]),
            RowValue::Struct(vec![RowValue::Int32(7), RowValue::Bool(false)]),
            RowValue::Null,
        ]);

        let p = moonlink_row_to_proto(row.clone());
        let row2 = proto_to_moonlink_row(p).unwrap();
        assert_eq!(row, row2);
    }

    #[test]
    fn test_decimal_conversion() {
        let val: i128 = -123456789012345678901234567890i128;
        let row = MoonlinkRow::new(vec![RowValue::Decimal(val)]);
        let p = moonlink_row_to_proto(row.clone());
        let row2 = proto_to_moonlink_row(p).unwrap();
        assert_eq!(row2.values.len(), 1);
        if let RowValue::Decimal(v2) = row2.values[0] {
            assert_eq!(v2, val);
        } else {
            panic!("expected decimal row value");
        }
    }
}
