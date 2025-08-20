use super::moonlink_type::RowValue;
use crate::row::MoonlinkRow;
use arrow::array::Array;
use arrow_array::{
    BinaryArray, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array,
    Int64Array, RecordBatch, StringArray,
};

/// Convert arrow record batch to moonlink rows.
pub(super) fn record_batch_to_moonlink_row(batch: &RecordBatch) -> Vec<MoonlinkRow> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut values = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let arr = batch.column(col_idx).as_ref();
            let v = arrow_value_to_rowvalue(arr, row_idx);
            values.push(v);
        }
        rows.push(MoonlinkRow::new(values));
    }
    rows
}

/// From arrow value to moonlink row value.
fn arrow_value_to_rowvalue(arr: &dyn Array, row_idx: usize) -> RowValue {
    if arr.is_null(row_idx) {
        return RowValue::Null;
    }

    match arr.data_type() {
        arrow_schema::DataType::Int32 => {
            let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
            RowValue::Int32(a.value(row_idx))
        }
        arrow_schema::DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            RowValue::Int64(a.value(row_idx))
        }
        arrow_schema::DataType::Float32 => {
            let a = arr.as_any().downcast_ref::<Float32Array>().unwrap();
            RowValue::Float32(a.value(row_idx))
        }
        arrow_schema::DataType::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            RowValue::Float64(a.value(row_idx))
        }
        arrow_schema::DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            RowValue::Bool(a.value(row_idx))
        }
        arrow_schema::DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            RowValue::ByteArray(a.value(row_idx).as_bytes().to_vec())
        }
        arrow_schema::DataType::Binary => {
            let a = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
            RowValue::ByteArray(a.value(row_idx).to_vec())
        }
        arrow_schema::DataType::FixedSizeBinary(16) => {
            let a = arr.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            let slice = a.value(row_idx);
            let mut buf = [0u8; 16];
            buf.copy_from_slice(slice);
            RowValue::FixedLenByteArray(buf)
        }
        _ => {
            panic!("Unimplemented type {:?}", arr.data_type());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float32Builder, Float64Builder,
        Int32Builder, Int64Builder, StringBuilder,
    };
    use arrow_array::ArrayRef;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_arrow_to_moonlink_row_conversion() {
        let mut i32_b = Int32Builder::new();
        i32_b.append_value(42);
        i32_b.append_null();

        let mut i64_b = Int64Builder::new();
        i64_b.append_value(9_223_372_036_854_775_i64);
        i64_b.append_null();

        let mut f32_b = Float32Builder::new();
        f32_b.append_value(3.5);
        f32_b.append_null();

        let mut f64_b = Float64Builder::new();
        f64_b.append_value(6.25);
        f64_b.append_null();

        let mut bool_b = BooleanBuilder::new();
        bool_b.append_value(true);
        bool_b.append_null();

        let mut utf8_b = StringBuilder::new();
        utf8_b.append_value("hello");
        utf8_b.append_null();

        let mut bin_b = BinaryBuilder::new();
        bin_b.append_value(b"\x01\x02\x03");
        bin_b.append_null();

        let mut fxb_b = FixedSizeBinaryBuilder::new(16);
        fxb_b.append_value([0xAB; 16]).unwrap();
        fxb_b.append_null();

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(i32_b.finish()),
            Arc::new(i64_b.finish()),
            Arc::new(f32_b.finish()),
            Arc::new(f64_b.finish()),
            Arc::new(bool_b.finish()),
            Arc::new(utf8_b.finish()),
            Arc::new(bin_b.finish()),
            Arc::new(fxb_b.finish()),
        ];

        let schema = Schema::new(vec![
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("bin", DataType::Binary, true),
            Field::new("uuid_like", DataType::FixedSizeBinary(16), true),
        ]);

        let batch = RecordBatch::try_new(Arc::new(schema), arrays).unwrap();

        let rows = MoonlinkRow::from_record_batch(&batch);
        assert_eq!(rows.len(), 2);

        assert_eq!(
            rows[0].values,
            vec![
                RowValue::Int32(42),
                RowValue::Int64(9_223_372_036_854_775),
                RowValue::Float32(3.5),
                RowValue::Float64(6.25),
                RowValue::Bool(true),
                RowValue::ByteArray(b"hello".to_vec()),
                RowValue::ByteArray(vec![1, 2, 3]),
                RowValue::FixedLenByteArray([0xABu8; 16]),
            ]
        );

        for v in &rows[1].values {
            assert_eq!(*v, RowValue::Null);
        }
    }
}
