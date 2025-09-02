pub(super) mod arrow_converter;
mod column_array_builder;
mod moonlink_row;
mod moonlink_type;
mod proto_converter;

pub(crate) use column_array_builder::ColumnArrayBuilder;
pub use moonlink_row::{IdentityProp, MoonlinkRow};
pub use moonlink_type::RowValue;
pub use proto_converter::{moonlink_row_to_proto, proto_to_moonlink_row};

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

        let p = moonlink_row_to_proto(&row);
        let row2 = proto_to_moonlink_row(&p);
        assert_eq!(row, row2);
    }

    #[test]
    fn test_decimal_conversion() {
        let val: i128 = -123456789012345678901234567890i128;
        let row = MoonlinkRow::new(vec![RowValue::Decimal(val)]);
        let p = moonlink_row_to_proto(&row);
        let row2 = proto_to_moonlink_row(&p);
        assert_eq!(row2.values.len(), 1);
        if let RowValue::Decimal(v2) = row2.values[0] {
            assert_eq!(v2, val);
        } else {
            panic!("expected decimal row value");
        }
    }
}
