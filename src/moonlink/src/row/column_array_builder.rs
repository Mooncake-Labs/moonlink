use crate::error::Error;
use crate::row::RowValue;
use arrow::array::builder::{BinaryBuilder, BooleanBuilder, PrimitiveBuilder, StringBuilder};
use arrow::array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
use arrow::array::{ArrayRef, FixedSizeBinaryBuilder};
use arrow::compute::kernels::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;
/// A column array builder that can handle different types
pub(crate) enum ColumnArrayBuilder {
    Boolean(BooleanBuilder),
    Int32(PrimitiveBuilder<Int32Type>),
    Int64(PrimitiveBuilder<Int64Type>),
    Float32(PrimitiveBuilder<Float32Type>),
    Float64(PrimitiveBuilder<Float64Type>),
    Utf8(StringBuilder),
    FixedSizeBinary(FixedSizeBinaryBuilder),
    Binary(BinaryBuilder),
}

impl ColumnArrayBuilder {
    /// Create a new column array builder for a specific data type
    pub(crate) fn new(data_type: DataType, capacity: usize) -> Self {
        match &data_type {
            DataType::Boolean => {
                ColumnArrayBuilder::Boolean(BooleanBuilder::with_capacity(capacity))
            }
            DataType::Int16 | DataType::Int32 | DataType::Date32 => {
                ColumnArrayBuilder::Int32(PrimitiveBuilder::<Int32Type>::with_capacity(capacity))
            }
            DataType::Timestamp(_, _) | DataType::Int64 | DataType::Time64(_) => {
                ColumnArrayBuilder::Int64(PrimitiveBuilder::<Int64Type>::with_capacity(capacity))
            }
            DataType::Float32 => ColumnArrayBuilder::Float32(
                PrimitiveBuilder::<Float32Type>::with_capacity(capacity),
            ),
            DataType::Float64 => ColumnArrayBuilder::Float64(
                PrimitiveBuilder::<Float64Type>::with_capacity(capacity),
            ),
            DataType::Utf8 => {
                ColumnArrayBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 10))
            }
            DataType::FixedSizeBinary(_size) => {
                assert_eq!(*_size, 16);
                ColumnArrayBuilder::FixedSizeBinary(FixedSizeBinaryBuilder::with_capacity(
                    capacity, 16,
                ))
            }
            DataType::Binary => {
                ColumnArrayBuilder::Binary(BinaryBuilder::with_capacity(capacity, capacity * 10))
            }
            _ => panic!("data type: {:?}", data_type),
        }
    }
    /// Append a value to this builder
    pub(crate) fn append_value(&mut self, value: &RowValue) -> Result<(), Error> {
        match self {
            ColumnArrayBuilder::Boolean(builder) => {
                match value {
                    RowValue::Bool(v) => builder.append_value(*v),
                    RowValue::Null => builder.append_null(),
                    _ => unreachable!("Bool expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Int32(builder) => {
                match value {
                    RowValue::Int32(v) => builder.append_value(*v),
                    RowValue::Null => builder.append_null(),
                    _ => unreachable!("Int32 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Int64(builder) => {
                match value {
                    RowValue::Int64(v) => builder.append_value(*v),
                    RowValue::Null => builder.append_null(),
                    _ => unreachable!("Int64 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Float32(builder) => {
                match value {
                    RowValue::Float32(v) => builder.append_value(*v),
                    RowValue::Null => builder.append_null(),
                    _ => unreachable!("Float32 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Float64(builder) => {
                match value {
                    RowValue::Float64(v) => builder.append_value(*v),
                    RowValue::Null => builder.append_null(),
                    _ => unreachable!("Float64 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Utf8(builder) => {
                match value {
                    RowValue::ByteArray(v) => {
                        builder.append_value(unsafe { std::str::from_utf8_unchecked(v) })
                    }
                    RowValue::Null => builder.append_null(),
                    _ => unreachable!("ByteArray expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::FixedSizeBinary(builder) => {
                match value {
                    RowValue::FixedLenByteArray(v) => builder.append_value(v)?,
                    RowValue::Null => builder.append_null(),
                    _ => unreachable!("FixedLenByteArray expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Binary(builder) => {
                match value {
                    RowValue::ByteArray(v) => builder.append_value(v),
                    RowValue::Null => builder.append_null(),
                    _ => unreachable!("ByteArray expected from well-typed input"),
                };
                Ok(())
            }
        }
    }
    /// Finish building and return the array
    pub(crate) fn finish(&mut self, logical_type: &DataType) -> ArrayRef {
        let array: ArrayRef = match self {
            ColumnArrayBuilder::Boolean(builder) => Arc::new(builder.finish()),
            ColumnArrayBuilder::Int32(builder) => Arc::new(builder.finish()),
            ColumnArrayBuilder::Int64(builder) => Arc::new(builder.finish()),
            ColumnArrayBuilder::Float32(builder) => Arc::new(builder.finish()),
            ColumnArrayBuilder::Float64(builder) => Arc::new(builder.finish()),
            ColumnArrayBuilder::Utf8(builder) => Arc::new(builder.finish()),
            ColumnArrayBuilder::FixedSizeBinary(builder) => Arc::new(builder.finish()),
            ColumnArrayBuilder::Binary(builder) => Arc::new(builder.finish()),
        };
        cast(&array, logical_type).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array,
        Int64Array, StringArray,
    };
    use arrow::datatypes::DataType;
    #[test]
    fn test_column_array_builder() {
        // Test Int32 type
        let mut builder = ColumnArrayBuilder::new(DataType::Int32, 2);
        builder.append_value(&RowValue::Int32(1)).unwrap();
        builder.append_value(&RowValue::Int32(2)).unwrap();
        let array = builder.finish(&DataType::Int32);
        assert_eq!(array.len(), 2);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 1);
        assert_eq!(int32_array.value(1), 2);

        // Test Int64 type
        let mut builder = ColumnArrayBuilder::new(DataType::Int64, 2);
        builder.append_value(&RowValue::Int64(100)).unwrap();
        builder.append_value(&RowValue::Int64(200)).unwrap();
        let array = builder.finish(&DataType::Int64);
        assert_eq!(array.len(), 2);
        let int64_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_array.value(0), 100);
        assert_eq!(int64_array.value(1), 200);

        // Test Float32 type
        let mut builder = ColumnArrayBuilder::new(DataType::Float32, 2);
        builder
            .append_value(&RowValue::Float32(std::f32::consts::PI))
            .unwrap();
        builder
            .append_value(&RowValue::Float32(std::f32::consts::E))
            .unwrap();
        let array = builder.finish(&DataType::Float32);
        assert_eq!(array.len(), 2);
        let float32_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
        assert!((float32_array.value(0) - std::f32::consts::PI).abs() < 0.0001);
        assert!((float32_array.value(1) - std::f32::consts::E).abs() < 0.0001);

        // Test Float64 type
        let mut builder = ColumnArrayBuilder::new(DataType::Float64, 2);
        builder
            .append_value(&RowValue::Float64(std::f64::consts::PI))
            .unwrap();
        builder
            .append_value(&RowValue::Float64(std::f64::consts::E))
            .unwrap();
        let array = builder.finish(&DataType::Float64);
        assert_eq!(array.len(), 2);
        let float64_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((float64_array.value(0) - std::f64::consts::PI).abs() < 0.00001);
        assert!((float64_array.value(1) - std::f64::consts::E).abs() < 0.00001);

        // Test Boolean type
        let mut builder = ColumnArrayBuilder::new(DataType::Boolean, 2);
        builder.append_value(&RowValue::Bool(true)).unwrap();
        builder.append_value(&RowValue::Bool(false)).unwrap();
        let array = builder.finish(&DataType::Boolean);
        assert_eq!(array.len(), 2);
        let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));

        // Test Utf8 (ByteArray) type
        let mut builder = ColumnArrayBuilder::new(DataType::Utf8, 2);
        builder
            .append_value(&RowValue::ByteArray("hello".as_bytes().to_vec()))
            .unwrap();
        builder
            .append_value(&RowValue::ByteArray("world".as_bytes().to_vec()))
            .unwrap();
        let array = builder.finish(&DataType::Utf8);
        assert_eq!(array.len(), 2);
        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.value(0), "hello");
        assert_eq!(string_array.value(1), "world");

        // Test FixedSizeBinary type
        let mut builder = ColumnArrayBuilder::new(DataType::FixedSizeBinary(16), 2);
        let bytes1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let bytes2 = [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1];
        builder
            .append_value(&RowValue::FixedLenByteArray(bytes1))
            .unwrap();
        builder
            .append_value(&RowValue::FixedLenByteArray(bytes2))
            .unwrap();
        let array = builder.finish(&DataType::FixedSizeBinary(16));
        assert_eq!(array.len(), 2);
        let binary_array = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(binary_array.value(0), bytes1);
        assert_eq!(binary_array.value(1), bytes2);

        // Test null values
        let mut builder = ColumnArrayBuilder::new(DataType::Int32, 3);
        builder.append_value(&RowValue::Int32(1)).unwrap();
        builder.append_value(&RowValue::Null).unwrap();
        builder.append_value(&RowValue::Int32(3)).unwrap();
        let array = builder.finish(&DataType::Int32);
        assert_eq!(array.len(), 3);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 1);
        assert!(int32_array.is_null(1));
        assert_eq!(int32_array.value(2), 3);

        // Test using null values directly from RowValue::Null
        let mut builder = ColumnArrayBuilder::new(DataType::Int32, 3);
        builder.append_value(&RowValue::Int32(1)).unwrap();
        builder.append_value(&RowValue::Null).unwrap();
        builder.append_value(&RowValue::Int32(3)).unwrap();
        let array = builder.finish(&DataType::Int32);
        assert_eq!(array.len(), 3);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 1);
        assert!(int32_array.is_null(1));
        assert_eq!(int32_array.value(2), 3);
    }
}
