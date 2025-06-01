use crate::error::Error;
use crate::row::RowValue;
use arrow::array::builder::{
    BinaryBuilder, BooleanBuilder, NullBufferBuilder, PrimitiveBuilder, StringBuilder,
};
use arrow::array::types::{Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type};
use arrow::array::{ArrayBuilder, ArrayRef, FixedSizeBinaryBuilder, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::kernels::cast;
use arrow::datatypes::DataType;
use std::mem::take;
use std::sync::Arc;

pub(crate) struct ArrayBuilderHelper {
    offset_builder: Vec<i32>,
    null_builder: NullBufferBuilder,
}

impl ArrayBuilderHelper {
    fn push(&mut self, value: i32) {
        self.offset_builder.push(value);
        self.null_builder.append_non_null();
    }
    fn push_null(&mut self) {
        self.offset_builder
            .push(self.offset_builder.last().copied().unwrap_or(0));
        self.null_builder.append_null();
    }
}
/// A column array builder that can handle different types
pub(crate) enum ColumnArrayBuilder {
    Boolean(BooleanBuilder, Option<ArrayBuilderHelper>),
    Int32(PrimitiveBuilder<Int32Type>, Option<ArrayBuilderHelper>),
    Int64(PrimitiveBuilder<Int64Type>, Option<ArrayBuilderHelper>),
    Float32(PrimitiveBuilder<Float32Type>, Option<ArrayBuilderHelper>),
    Float64(PrimitiveBuilder<Float64Type>, Option<ArrayBuilderHelper>),
    Decimal128(PrimitiveBuilder<Decimal128Type>, Option<ArrayBuilderHelper>),
    Utf8(StringBuilder, Option<ArrayBuilderHelper>),
    FixedSizeBinary(FixedSizeBinaryBuilder, Option<ArrayBuilderHelper>),
    Binary(BinaryBuilder, Option<ArrayBuilderHelper>),
}

impl ColumnArrayBuilder {
    /// Create a new column array builder for a specific data type
    pub(crate) fn new(data_type: &DataType, capacity: usize, is_list: bool) -> Self {
        let array_builder = if is_list {
            Some(ArrayBuilderHelper {
                offset_builder: Vec::with_capacity(capacity),
                null_builder: NullBufferBuilder::new(capacity),
            })
        } else {
            None
        };
        match data_type {
            DataType::Boolean => {
                ColumnArrayBuilder::Boolean(BooleanBuilder::with_capacity(capacity), array_builder)
            }
            DataType::Int16 | DataType::Int32 | DataType::Date32 => ColumnArrayBuilder::Int32(
                PrimitiveBuilder::<Int32Type>::with_capacity(capacity),
                array_builder,
            ),
            DataType::Timestamp(_, _) | DataType::Int64 | DataType::Time64(_) => {
                ColumnArrayBuilder::Int64(
                    PrimitiveBuilder::<Int64Type>::with_capacity(capacity),
                    array_builder,
                )
            }
            DataType::Float32 => ColumnArrayBuilder::Float32(
                PrimitiveBuilder::<Float32Type>::with_capacity(capacity),
                array_builder,
            ),
            DataType::Float64 => ColumnArrayBuilder::Float64(
                PrimitiveBuilder::<Float64Type>::with_capacity(capacity),
                array_builder,
            ),
            DataType::Utf8 => ColumnArrayBuilder::Utf8(
                StringBuilder::with_capacity(capacity, capacity * 10),
                array_builder,
            ),
            DataType::FixedSizeBinary(_size) => {
                assert_eq!(*_size, 16);
                ColumnArrayBuilder::FixedSizeBinary(
                    FixedSizeBinaryBuilder::with_capacity(capacity, 16),
                    array_builder,
                )
            }
            DataType::Decimal128(_, _) => ColumnArrayBuilder::Decimal128(
                PrimitiveBuilder::<Decimal128Type>::with_capacity(capacity),
                array_builder,
            ),
            DataType::Binary => ColumnArrayBuilder::Binary(
                BinaryBuilder::with_capacity(capacity, capacity * 10),
                array_builder,
            ),
            DataType::List(inner) => ColumnArrayBuilder::new(inner.data_type(), capacity, true),
            _ => panic!("data type: {:?}", data_type),
        }
    }
    /// Append a value to this builder
    pub(crate) fn append_value(&mut self, value: &RowValue) -> Result<(), Error> {
        match self {
            ColumnArrayBuilder::Boolean(builder, array_helper) => {
                match value {
                    RowValue::Bool(v) => builder.append_value(*v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::Bool(v) => builder.append_value(*v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("Bool expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("Bool expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Int32(builder, array_helper) => {
                match value {
                    RowValue::Int32(v) => builder.append_value(*v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::Int32(v) => builder.append_value(*v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("Int32 expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("Int32 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Int64(builder, array_helper) => {
                match value {
                    RowValue::Int64(v) => builder.append_value(*v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::Int64(v) => builder.append_value(*v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("Int64 expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("Int64 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Float32(builder, array_helper) => {
                match value {
                    RowValue::Float32(v) => builder.append_value(*v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::Float32(v) => builder.append_value(*v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("Float32 expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("Float32 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Float64(builder, array_helper) => {
                match value {
                    RowValue::Float64(v) => builder.append_value(*v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::Float64(v) => builder.append_value(*v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("Float64 expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("Float64 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Decimal128(builder, array_helper) => {
                match value {
                    RowValue::Decimal(v) => builder.append_value(*v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::Decimal(v) => builder.append_value(*v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("Decimal128 expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("Decimal128 expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Utf8(builder, array_helper) => {
                match value {
                    RowValue::ByteArray(v) => {
                        builder.append_value(unsafe { std::str::from_utf8_unchecked(v) })
                    }
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::ByteArray(v) => builder
                                    .append_value(unsafe { std::str::from_utf8_unchecked(v) }),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("ByteArray expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("ByteArray expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::FixedSizeBinary(builder, array_helper) => {
                match value {
                    RowValue::FixedLenByteArray(v) => builder.append_value(v)?,
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::FixedLenByteArray(v) => builder.append_value(v)?,
                                RowValue::Null => builder.append_null(),
                                _ => {
                                    unreachable!("FixedLenByteArray expected from well-typed input")
                                }
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("FixedLenByteArray expected from well-typed input"),
                };
                Ok(())
            }
            ColumnArrayBuilder::Binary(builder, array_helper) => {
                match value {
                    RowValue::ByteArray(v) => builder.append_value(v),
                    RowValue::Array(v) => {
                        array_helper.as_mut().unwrap().push(builder.len() as i32);
                        for i in 0..v.len() {
                            match &v[i] {
                                RowValue::ByteArray(v) => builder.append_value(v),
                                RowValue::Null => builder.append_null(),
                                _ => unreachable!("ByteArray expected from well-typed input"),
                            }
                        }
                    }
                    RowValue::Null => {
                        if let Some(helper) = array_helper.as_mut() {
                            helper.push_null();
                        } else {
                            builder.append_null();
                        }
                    }
                    _ => unreachable!("ByteArray expected from well-typed input"),
                };
                Ok(())
            }
        }
    }
    /// Finish building and return the array
    pub(crate) fn finish(&mut self, logical_type: &DataType) -> ArrayRef {
        let (array, array_helper): (ArrayRef, &mut Option<ArrayBuilderHelper>) = match self {
            ColumnArrayBuilder::Boolean(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Int32(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Int64(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Float32(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Float64(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Decimal128(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Utf8(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::FixedSizeBinary(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
            ColumnArrayBuilder::Binary(builder, array_helper) => {
                (Arc::new(builder.finish()), array_helper)
            }
        };
        if let Some(helper) = array_helper.as_mut() {
            let mut offset_array = take(&mut helper.offset_builder);
            offset_array.push(array.len() as i32);
            let null_array = helper.null_builder.finish();
            let inner_field = match logical_type {
                DataType::List(inner) => inner,
                _ => panic!("List expected from well-typed input"),
            };
            let list_array = ListArray::new(
                inner_field.clone(),
                OffsetBuffer::new(offset_array.into()),
                array,
                null_array,
            );
            Arc::new(list_array)
        } else {
            cast(&array, logical_type).unwrap()
        }
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
        let mut builder = ColumnArrayBuilder::new(&DataType::Int32, 2, false);
        builder.append_value(&RowValue::Int32(1)).unwrap();
        builder.append_value(&RowValue::Int32(2)).unwrap();
        let array = builder.finish(&DataType::Int32);
        assert_eq!(array.len(), 2);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 1);
        assert_eq!(int32_array.value(1), 2);

        // Test Int64 type
        let mut builder = ColumnArrayBuilder::new(&DataType::Int64, 2, false);
        builder.append_value(&RowValue::Int64(100)).unwrap();
        builder.append_value(&RowValue::Int64(200)).unwrap();
        let array = builder.finish(&DataType::Int64);
        assert_eq!(array.len(), 2);
        let int64_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_array.value(0), 100);
        assert_eq!(int64_array.value(1), 200);

        // Test Float32 type
        let mut builder = ColumnArrayBuilder::new(&DataType::Float32, 2, false);
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
        let mut builder = ColumnArrayBuilder::new(&DataType::Float64, 2, false);
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
        let mut builder = ColumnArrayBuilder::new(&DataType::Boolean, 2, false);
        builder.append_value(&RowValue::Bool(true)).unwrap();
        builder.append_value(&RowValue::Bool(false)).unwrap();
        let array = builder.finish(&DataType::Boolean);
        assert_eq!(array.len(), 2);
        let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));

        // Test Utf8 (ByteArray) type
        let mut builder = ColumnArrayBuilder::new(&DataType::Utf8, 2, false);
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
        let mut builder = ColumnArrayBuilder::new(&DataType::FixedSizeBinary(16), 2, false);
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
        let mut builder = ColumnArrayBuilder::new(&DataType::Int32, 3, false);
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
        let mut builder = ColumnArrayBuilder::new(&DataType::Int32, 3, false);
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

    #[test]
    fn test_column_array_builder_list() {
        // Test List<Int32> type
        let mut builder = ColumnArrayBuilder::new(
            &DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            2,
            true,
        );

        // Add a list of integers [1, 2, 3]
        builder
            .append_value(&RowValue::Array(vec![
                RowValue::Int32(1),
                RowValue::Int32(2),
                RowValue::Int32(3),
            ]))
            .unwrap();

        // Add another list of integers [4, 5]
        builder
            .append_value(&RowValue::Array(vec![
                RowValue::Int32(4),
                RowValue::Int32(5),
            ]))
            .unwrap();

        let array = builder.finish(&DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Int32,
            true,
        ))));

        assert_eq!(array.len(), 2);
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();

        // Check first list [1, 2, 3]
        let first_list = list_array.value(0);
        let first_int_array = first_list.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(first_int_array.len(), 3);
        assert_eq!(first_int_array.value(0), 1);
        assert_eq!(first_int_array.value(1), 2);
        assert_eq!(first_int_array.value(2), 3);

        // Check second list [4, 5]
        let second_list = list_array.value(1);
        let second_int_array = second_list.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(second_int_array.len(), 2);
        assert_eq!(second_int_array.value(0), 4);
        assert_eq!(second_int_array.value(1), 5);
    }
}
