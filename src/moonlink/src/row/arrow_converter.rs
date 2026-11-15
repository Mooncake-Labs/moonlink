use super::moonlink_type::RowValue;
use crate::row::MoonlinkRow;
use crate::Result;
use arrow::array::{Array, AsArray};
use arrow::error::ArrowError;
use arrow_array::{
    BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
};

/// Convert arrow record batch to moonlink rows.
pub(super) fn record_batch_to_moonlink_row(batch: &RecordBatch) -> Result<Vec<MoonlinkRow>> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut values = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let arr = batch.column(col_idx).as_ref();
            values.push(arrow_value_to_rowvalue(arr, row_idx)?);
        }
        rows.push(MoonlinkRow::new(values));
    }
    Ok(rows)
}

/// Every element of a list-like array slice.
fn array_elements(values: &dyn Array) -> Result<RowValue> {
    let items = (0..values.len())
        .map(|i| arrow_value_to_rowvalue(values, i))
        .collect::<Result<Vec<_>>>()?;
    Ok(RowValue::Array(items))
}

/// From arrow value to moonlink row value; the inverse of `ColumnArrayBuilder::append_value`, so
/// a row read back from parquet equals (and hashes like) the row CDC produced.
fn arrow_value_to_rowvalue(arr: &dyn Array, row_idx: usize) -> Result<RowValue> {
    if arr.is_null(row_idx) {
        return Ok(RowValue::Null);
    }

    let value = match arr.data_type() {
        arrow_schema::DataType::Int16 => {
            let a = arr.as_any().downcast_ref::<Int16Array>().unwrap();
            RowValue::Int32(a.value(row_idx) as i32)
        }
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
        arrow_schema::DataType::Date32 => {
            let a = arr.as_any().downcast_ref::<Date32Array>().unwrap();
            RowValue::Int32(a.value(row_idx))
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
        arrow_schema::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, tz_opt)
            if tz_opt.as_deref().is_none_or(|tz| tz == "UTC") =>
        {
            let a = arr
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                .unwrap();
            RowValue::Int64(a.value(row_idx))
        }
        arrow_schema::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
            let a = arr
                .as_any()
                .downcast_ref::<arrow_array::Time64MicrosecondArray>()
                .unwrap();
            RowValue::Int64(a.value(row_idx))
        }
        arrow_schema::DataType::Decimal128(_, _) => {
            let a = arr
                .as_any()
                .downcast_ref::<arrow_array::Decimal128Array>()
                .unwrap();
            if a.is_null(row_idx) {
                RowValue::Null
            } else {
                let v = a.value(row_idx);
                RowValue::Decimal(v)
            }
        }
        arrow_schema::DataType::List(_) => {
            return array_elements(arr.as_list::<i32>().value(row_idx).as_ref());
        }
        arrow_schema::DataType::LargeList(_) => {
            return array_elements(arr.as_list::<i64>().value(row_idx).as_ref());
        }
        arrow_schema::DataType::FixedSizeList(_, _) => {
            return array_elements(arr.as_fixed_size_list().value(row_idx).as_ref());
        }
        // Written from a RowValue::Array of key/value structs.
        arrow_schema::DataType::Map(_, _) => {
            return array_elements(&arr.as_map().value(row_idx));
        }
        arrow_schema::DataType::Struct(_) => {
            let fields = arr
                .as_struct()
                .columns()
                .iter()
                .map(|column| arrow_value_to_rowvalue(column.as_ref(), row_idx))
                .collect::<Result<Vec<_>>>()?;
            RowValue::Struct(fields)
        }
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "reading column type {other:?} back into rows"
            ))
            .into());
        }
    };
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder,
        Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
        Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    };
    use arrow_array::ArrayRef;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_arrow_to_moonlink_row_conversion() {
        let mut i16_b = Int16Builder::new();
        i16_b.append_value(16);
        i16_b.append_null();

        let mut i32_b = Int32Builder::new();
        i32_b.append_value(42);
        i32_b.append_null();

        let mut i64_b = Int64Builder::new();
        i64_b.append_value(9_223_372_036_854_775_i64);
        i64_b.append_null();

        let mut date32_b = Date32Builder::new();
        date32_b.append_value(1); // 1970-01-02
        date32_b.append_null();

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

        let mut ts_b = TimestampMicrosecondBuilder::new();
        ts_b.append_value(1_234_567_i64); // 1970-01-01 00:00:01.234567 UTC
        ts_b.append_null();

        let mut t64_us = Time64MicrosecondBuilder::new();
        t64_us.append_value(1_234_567_i64);
        t64_us.append_null();

        let mut dec128_b = Decimal128Builder::new();
        dec128_b.append_value(12345);
        dec128_b.append_null();

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(i16_b.finish()),
            Arc::new(i32_b.finish()),
            Arc::new(i64_b.finish()),
            Arc::new(f32_b.finish()),
            Arc::new(f64_b.finish()),
            Arc::new(date32_b.finish()),
            Arc::new(bool_b.finish()),
            Arc::new(utf8_b.finish()),
            Arc::new(bin_b.finish()),
            Arc::new(fxb_b.finish()),
            Arc::new(ts_b.finish()),
            Arc::new(t64_us.finish()),
            Arc::new(dec128_b.finish()),
        ];

        let schema = Schema::new(vec![
            Field::new("i16", DataType::Int16, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("date32", DataType::Date32, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("bin", DataType::Binary, true),
            Field::new("uuid_like", DataType::FixedSizeBinary(16), true),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "t64_us",
                DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                true,
            ),
            Field::new("dec128", DataType::Decimal128(38, 10), true),
        ]);

        let batch = RecordBatch::try_new(Arc::new(schema), arrays).unwrap();

        let rows = MoonlinkRow::from_record_batch(&batch).unwrap();
        assert_eq!(rows.len(), 2);

        assert_eq!(
            rows[0].values,
            vec![
                RowValue::Int32(16),
                RowValue::Int32(42),
                RowValue::Int64(9_223_372_036_854_775),
                RowValue::Float32(3.5),
                RowValue::Float64(6.25),
                RowValue::Int32(1),
                RowValue::Bool(true),
                RowValue::ByteArray(b"hello".to_vec()),
                RowValue::ByteArray(vec![1, 2, 3]),
                RowValue::FixedLenByteArray([0xABu8; 16]),
                RowValue::Int64(1_234_567_i64),
                RowValue::Int64(1_234_567),
                RowValue::Decimal(12345),
            ]
        );

        for v in &rows[1].values {
            assert_eq!(*v, RowValue::Null);
        }
    }

    /// Rows shaped like the ones CDC produces (pg_replicate `From<Cell> for RowValue`: an
    /// ArrayCell::Null becomes an empty array, NULL elements stay NULL, composites become
    /// structs) survive the trip through the disk-slice writer and parquet unchanged, so a file
    /// index rebuilt from parquet finds the rows CDC deletes.
    #[test]
    fn test_cdc_shaped_rows_round_trip_through_parquet() {
        use crate::row::column_array_builder::ColumnArrayBuilder;
        use crate::row::moonlink_row::stable_lookup_key;
        use arrow_schema::Fields;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::arrow::ArrowWriter;

        let item = |dt: DataType| Arc::new(Field::new("item", dt, true));
        let struct_fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::List(item(DataType::Utf8)), true),
        ]);
        let map_entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            false,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("ints", DataType::List(item(DataType::Int32)), true),
            Field::new("texts", DataType::List(item(DataType::Utf8)), true),
            Field::new("dates", DataType::List(item(DataType::Date32)), true),
            Field::new(
                "decimals",
                DataType::List(item(DataType::Decimal128(10, 2))),
                true,
            ),
            Field::new("composite", DataType::Struct(struct_fields.clone()), true),
            Field::new(
                "composites",
                DataType::List(item(DataType::Struct(struct_fields))),
                true,
            ),
            Field::new("map", DataType::Map(map_entries, false), true),
        ]));
        let text = |s: &str| RowValue::ByteArray(s.as_bytes().to_vec());
        let rows = vec![
            MoonlinkRow::new(vec![
                RowValue::Int32(1),
                RowValue::Array(vec![RowValue::Int32(1), RowValue::Null, RowValue::Int32(3)]),
                RowValue::Array(vec![text("a"), RowValue::Null]),
                RowValue::Array(vec![RowValue::Int32(19000)]),
                RowValue::Array(vec![RowValue::Decimal(12345), RowValue::Null]),
                RowValue::Struct(vec![RowValue::Int32(7), RowValue::Array(vec![text("x")])]),
                RowValue::Array(vec![
                    RowValue::Struct(vec![RowValue::Null, RowValue::Array(vec![])]),
                    RowValue::Null,
                ]),
                RowValue::Array(vec![RowValue::Struct(vec![text("k"), text("v")])]),
            ]),
            // Empty arrays (CDC's ArrayCell::Null) and NULL values everywhere else.
            MoonlinkRow::new(vec![
                RowValue::Int32(2),
                RowValue::Array(vec![]),
                RowValue::Null,
                RowValue::Array(vec![]),
                RowValue::Null,
                RowValue::Null,
                RowValue::Array(vec![]),
                RowValue::Null,
            ]),
        ];

        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(col, field)| {
                let mut builder = ColumnArrayBuilder::new(field.data_type(), rows.len());
                for row in &rows {
                    builder.append_value(&row.values[col]).unwrap();
                }
                builder.finish(field.data_type())
            })
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

        let mut parquet_bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut parquet_bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(parquet_bytes))
            .unwrap()
            .build()
            .unwrap();
        let mut read_back = Vec::new();
        for batch in reader {
            read_back.extend(MoonlinkRow::from_record_batch(&batch.unwrap()).unwrap());
        }

        assert_eq!(read_back.len(), rows.len());
        for (written, read) in rows.iter().zip(read_back.iter()) {
            assert_eq!(written.values, read.values);
            assert_eq!(
                stable_lookup_key(written.values.iter()),
                stable_lookup_key(read.values.iter())
            );
        }
    }

    #[test]
    fn test_unsupported_column_type_is_an_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("u", DataType::UInt8, false)]));
        let column: ArrayRef = Arc::new(arrow_array::UInt8Array::from(vec![1u8]));
        let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
        assert!(MoonlinkRow::from_record_batch(&batch).is_err());
    }
}
