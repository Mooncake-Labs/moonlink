use crate::error::Result;
use crate::row::RowValue;
use arrow::array::builder::{
    BinaryBuilder, BooleanBuilder, NullBufferBuilder, PrimitiveBuilder, StringBuilder,
};
use arrow::array::types::{Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type};
use arrow::array::{ArrayBuilder, ArrayRef, FixedSizeBinaryBuilder, ListArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::cast;
use arrow::datatypes::{DataType, FieldRef};
use arrow::error::ArrowError;
use std::sync::Arc;

/// Helper for building list arrays recursively
pub(crate) struct ListBuilderHelper {
    field: FieldRef,
    /// Tracks the starting position of each list element in the values array.
    /// The last offset always points to the total length of values.
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
    inner: Box<ColumnArrayBuilder>,
    len: usize,
}

impl ListBuilderHelper {
    pub fn with_capacity(field: FieldRef, capacity: usize) -> Self {
        Self {
            inner: Box::new(ColumnArrayBuilder::new(field.data_type(), 0)),
            field,
            offsets: Vec::with_capacity(capacity + 1),
            nulls: NullBufferBuilder::new(capacity),
            len: 0,
        }
    }

    pub fn append_items(&mut self, items: &[RowValue]) -> Result<()> {
        self.offsets.push(self.inner.len() as i32);
        for it in items {
            self.inner.append_value(it)?;
        }
        self.nulls.append_non_null();
        self.len += 1;
        Ok(())
    }

    pub fn append_null(&mut self) {
        let cur = *self.offsets.last().unwrap_or(&0);
        self.offsets.push(cur); // keeps child length unchanged
        self.nulls.append_null();
        self.len += 1;
    }

    pub fn finish(mut self) -> ArrayRef {
        let values = self.inner.finish(self.field.data_type());
        self.offsets.push(values.len() as i32); // closing offset
        Arc::new(ListArray::new(
            self.field,
            OffsetBuffer::new(self.offsets.into()),
            values,
            self.nulls.finish(),
        ))
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

/// Helper for building struct recursively
pub(crate) struct StructBuilderHelper {
    fields: Vec<(FieldRef, ColumnArrayBuilder)>,
    nulls: NullBufferBuilder,
    len: usize,
}

impl StructBuilderHelper {
    pub fn with_capacity(fields: &arrow::datatypes::Fields, capacity: usize) -> Self {
        let mut children = Vec::with_capacity(fields.len());
        for f in fields.iter() {
            children.push((f.clone(), ColumnArrayBuilder::new(f.data_type(), capacity)));
        }
        Self {
            fields: children,
            nulls: NullBufferBuilder::new(capacity),
            len: 0,
        }
    }

    /// Appends a struct with the given field values.
    /// The number of values must match the number of fields exactly.
    pub fn append_values(&mut self, vals: &[RowValue]) -> Result<()> {
        if vals.len() != self.fields.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Struct field count mismatch: expected {} fields, got {} values",
                self.fields.len(),
                vals.len()
            ))
            .into());
        }
        for (i, (_f, child)) in self.fields.iter_mut().enumerate() {
            child.append_value(&vals[i])?;
        }
        self.nulls.append_non_null();
        self.len += 1;
        Ok(())
    }

    pub fn append_null(&mut self) -> Result<()> {
        for (_f, child) in self.fields.iter_mut() {
            child.append_value(&RowValue::Null)?;
        }
        self.nulls.append_null();
        self.len += 1;
        Ok(())
    }

    pub fn finish(mut self) -> ArrayRef {
        let mut arrays = Vec::with_capacity(self.fields.len());
        let mut schema_fields = Vec::with_capacity(self.fields.len());
        for (f, b) in self.fields.drain(..) {
            schema_fields.push(f.clone());
            arrays.push(b.finish(f.data_type()));
        }
        let validity = self.nulls.finish();
        if schema_fields.is_empty() {
            Arc::new(StructArray::new_empty_fields(self.len, validity))
        } else {
            Arc::new(StructArray::new(schema_fields.into(), arrays, validity))
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

/// A column array builder that can handle different types
pub(crate) enum ColumnArrayBuilder {
    // Primitive leaves
    Boolean(BooleanBuilder),
    Int32(PrimitiveBuilder<Int32Type>),
    Int64(PrimitiveBuilder<Int64Type>),
    Float32(PrimitiveBuilder<Float32Type>),
    Float64(PrimitiveBuilder<Float64Type>),
    Decimal128(PrimitiveBuilder<Decimal128Type>),
    Utf8(StringBuilder),
    FixedSizeBinary(FixedSizeBinaryBuilder),
    Binary(BinaryBuilder),

    // Composite nodes (recursive)
    List(ListBuilderHelper),
    Struct(StructBuilderHelper),
}

impl ColumnArrayBuilder {
    /// Create a new column array builder for a specific data type
    pub(crate) fn new(dt: &DataType, capacity: usize) -> Self {
        match dt {
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Int16 | DataType::Int32 | DataType::Date32 => {
                Self::Int32(PrimitiveBuilder::<Int32Type>::with_capacity(capacity))
            }
            DataType::Timestamp(_, _) | DataType::Int64 | DataType::Time64(_) => {
                Self::Int64(PrimitiveBuilder::<Int64Type>::with_capacity(capacity))
            }
            DataType::Float32 => {
                Self::Float32(PrimitiveBuilder::<Float32Type>::with_capacity(capacity))
            }
            DataType::Float64 => {
                Self::Float64(PrimitiveBuilder::<Float64Type>::with_capacity(capacity))
            }
            DataType::Decimal128(precision, scale) => {
                let b = PrimitiveBuilder::<Decimal128Type>::with_capacity(capacity)
                    .with_precision_and_scale(*precision, *scale)
                    .expect(
                        "Failed to create Decimal128Type with precision {precision} and {scale}",
                    );
                Self::Decimal128(b)
            }
            DataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 8)),
            DataType::FixedSizeBinary(n) => {
                Self::FixedSizeBinary(FixedSizeBinaryBuilder::with_capacity(capacity, *n))
            }
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(capacity, capacity * 8)),
            DataType::List(child) => {
                Self::List(ListBuilderHelper::with_capacity(child.clone(), capacity))
            }
            DataType::Struct(fields) => {
                ColumnArrayBuilder::Struct(StructBuilderHelper::with_capacity(fields, capacity))
            }
            other => panic!("unsupported type in builder: {other:?}"),
        }
    }

    /// Append a value to this builder
    pub(crate) fn append_value(&mut self, v: &RowValue) -> Result<()> {
        match (self, v) {
            // ===== leaves
            (Self::Boolean(b), RowValue::Bool(x)) => {
                b.append_value(*x);
                Ok(())
            }
            (Self::Boolean(b), RowValue::Null) => {
                b.append_null();
                Ok(())
            }

            (Self::Int32(b), RowValue::Int32(x)) => {
                b.append_value(*x);
                Ok(())
            }
            (Self::Int32(b), RowValue::Null) => {
                b.append_null();
                Ok(())
            }

            (Self::Int64(b), RowValue::Int64(x)) => {
                b.append_value(*x);
                Ok(())
            }
            (Self::Int64(b), RowValue::Null) => {
                b.append_null();
                Ok(())
            }

            (Self::Float32(b), RowValue::Float32(x)) => {
                b.append_value(*x);
                Ok(())
            }
            (Self::Float32(b), RowValue::Null) => {
                b.append_null();
                Ok(())
            }

            (Self::Float64(b), RowValue::Float64(x)) => {
                b.append_value(*x);
                Ok(())
            }
            (Self::Float64(b), RowValue::Null) => {
                b.append_null();
                Ok(())
            }

            (Self::Decimal128(b), RowValue::Decimal(x)) => {
                b.append_value(*x);
                Ok(())
            }
            (Self::Decimal128(b), RowValue::Null) => {
                b.append_null();
                Ok(())
            }

            (Self::Utf8(b), RowValue::ByteArray(bytes)) => {
                let s = String::from_utf8(bytes.clone())?;
                b.append_value(&s);
                Ok(())
            }
            (Self::Utf8(b), RowValue::Null) => {
                b.append_null();
                Ok(())
            }

            (Self::FixedSizeBinary(b), RowValue::FixedLenByteArray(x)) => {
                b.append_value(*x)?;
                Ok(())
            }
            (Self::FixedSizeBinary(b), RowValue::Null) => {
                b.append_null();
                Ok(())
            }

            (Self::Binary(b), RowValue::ByteArray(x)) => {
                b.append_value(x);
                Ok(())
            }
            (Self::Binary(b), RowValue::Null) => {
                b.append_null();
                Ok(())
            }

            // ===== lists (recursive)
            (Self::List(b), RowValue::Array(items)) => {
                b.append_items(items)?;
                Ok(())
            }
            (Self::List(b), RowValue::Null) => {
                // push same offset -> empty list; mark null element
                b.append_null();
                Ok(())
            }

            // ===== structs (recursive)
            (Self::Struct(b), RowValue::Struct(vals)) => {
                b.append_values(vals)?;
                Ok(())
            }
            (Self::Struct(b), RowValue::Null) => {
                // null struct: append null to each child field to keep lengths aligned
                b.append_null()?;
                Ok(())
            }

            // ===== list-of-any (Array inside a non-List builder)
            (b @ Self::Boolean(_), RowValue::Array(_))
            | (b @ Self::Int32(_), RowValue::Array(_))
            | (b @ Self::Int64(_), RowValue::Array(_))
            | (b @ Self::Float32(_), RowValue::Array(_))
            | (b @ Self::Float64(_), RowValue::Array(_))
            | (b @ Self::Decimal128(_), RowValue::Array(_))
            | (b @ Self::Utf8(_), RowValue::Array(_))
            | (b @ Self::FixedSizeBinary(_), RowValue::Array(_))
            | (b @ Self::Binary(_), RowValue::Array(_)) => {
                Err(ArrowError::InvalidArgumentError(format!(
                    "Got RowValue::Array for non-list builder: {:?}",
                    std::mem::discriminant(b)
                ))
                .into())
            }

            // Type mismatch
            (b, other) => Err(ArrowError::InvalidArgumentError(format!(
                "Type mismatch: builder={:?}, value={:?}",
                std::mem::discriminant(b),
                other
            ))
            .into()),
        }
    }

    /// Returns the number of elements in the array
    fn len(&self) -> usize {
        match self {
            Self::List(b) => b.len(),
            Self::Struct(b) => b.len(),
            Self::Boolean(b) => b.len(),
            Self::Int32(b) => b.len(),
            Self::Int64(b) => b.len(),
            Self::Float32(b) => b.len(),
            Self::Float64(b) => b.len(),
            Self::Decimal128(b) => b.len(),
            Self::Utf8(b) => b.len(),
            Self::FixedSizeBinary(b) => b.len(),
            Self::Binary(b) => b.len(),
        }
    }

    /// Finish building and return the array
    pub(crate) fn finish(self, logical_type: &DataType) -> ArrayRef {
        match self {
            Self::Boolean(mut b) => Arc::new(b.finish()),
            Self::Int32(mut b) => {
                let arr: ArrayRef = Arc::new(b.finish());
                match logical_type {
                    DataType::Date32 => cast(&arr, logical_type).unwrap(),
                    _ => arr,
                }
            }
            Self::Int64(mut b) => {
                let arr: ArrayRef = Arc::new(b.finish());
                match logical_type {
                    DataType::Timestamp(_, _) | DataType::Time64(_) => {
                        cast(&arr, logical_type).unwrap()
                    }
                    _ => arr,
                }
            }
            Self::Float32(mut b) => Arc::new(b.finish()),
            Self::Float64(mut b) => Arc::new(b.finish()),
            Self::Decimal128(mut b) => Arc::new(b.finish()),
            Self::Utf8(mut b) => Arc::new(b.finish()),
            Self::FixedSizeBinary(mut b) => Arc::new(b.finish()),
            Self::Binary(mut b) => Arc::new(b.finish()),
            Self::List(h) => h.finish(),
            Self::Struct(h) => h.finish(),
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
        let mut builder = ColumnArrayBuilder::new(&DataType::Int32, /*capacity=*/ 2);
        builder.append_value(&RowValue::Int32(1)).unwrap();
        builder.append_value(&RowValue::Int32(2)).unwrap();
        let array = builder.finish(&DataType::Int32);
        assert_eq!(array.len(), 2);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 1);
        assert_eq!(int32_array.value(1), 2);

        // Test Int64 type
        let mut builder = ColumnArrayBuilder::new(&DataType::Int64, /*capacity=*/ 2);
        builder.append_value(&RowValue::Int64(100)).unwrap();
        builder.append_value(&RowValue::Int64(200)).unwrap();
        let array = builder.finish(&DataType::Int64);
        assert_eq!(array.len(), 2);
        let int64_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_array.value(0), 100);
        assert_eq!(int64_array.value(1), 200);

        // Test Float32 type
        let mut builder = ColumnArrayBuilder::new(&DataType::Float32, /*capacity=*/ 2);
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
        let mut builder = ColumnArrayBuilder::new(&DataType::Float64, /*capacity=*/ 2);
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
        let mut builder = ColumnArrayBuilder::new(&DataType::Boolean, /*capacity=*/ 2);
        builder.append_value(&RowValue::Bool(true)).unwrap();
        builder.append_value(&RowValue::Bool(false)).unwrap();
        let array = builder.finish(&DataType::Boolean);
        assert_eq!(array.len(), 2);
        let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));

        // Test Utf8 (ByteArray) type
        let mut builder = ColumnArrayBuilder::new(&DataType::Utf8, /*capacity=*/ 2);
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
        let mut builder =
            ColumnArrayBuilder::new(&DataType::FixedSizeBinary(16), /*capacity=*/ 2);
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
        let mut builder = ColumnArrayBuilder::new(&DataType::Int32, /*capacity=*/ 3);
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
        let mut builder = ColumnArrayBuilder::new(&DataType::Int32, /*capacity=*/ 3);
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
                /*nullable=*/ true,
            ))),
            /*capacity=*/ 2,
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
            /*nullable=*/ true,
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

    #[test]
    fn test_column_array_builder_struct() {
        use arrow::array::StructArray;

        // Test struct with primitive types
        let struct_fields = vec![
            Arc::new(arrow::datatypes::Field::new(
                "id",
                DataType::Int32,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "name",
                DataType::Utf8,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "active",
                DataType::Boolean,
                /*nullable=*/ true,
            )),
        ];

        let mut builder = ColumnArrayBuilder::new(
            &DataType::Struct(struct_fields.clone().into()),
            /*capacity=*/ 2,
        );

        // Add a struct with values
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Int32(1),
                RowValue::ByteArray(b"Alice".to_vec()),
                RowValue::Bool(true),
            ]))
            .unwrap();

        // Add another struct
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Int32(2),
                RowValue::ByteArray(b"Bob".to_vec()),
                RowValue::Bool(false),
            ]))
            .unwrap();

        let array = builder.finish(&DataType::Struct(struct_fields.into()));
        assert_eq!(array.len(), 2);

        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.num_columns(), 3);

        // Check the id column
        let id_column = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 1);
        assert_eq!(id_column.value(1), 2);

        // Check the name column
        let name_column = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_column.value(0), "Alice");
        assert_eq!(name_column.value(1), "Bob");

        // Check the active column
        let active_column = struct_array
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(active_column.value(0));
        assert!(!active_column.value(1));
    }

    #[test]
    fn test_column_array_builder_struct_with_nulls() {
        use arrow::array::StructArray;

        // Test struct with nulls
        let struct_fields = vec![
            Arc::new(arrow::datatypes::Field::new(
                "id",
                DataType::Int32,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "score",
                DataType::Float64,
                /*nullable=*/ true,
            )),
        ];

        let mut builder = ColumnArrayBuilder::new(
            &DataType::Struct(struct_fields.clone().into()),
            /*capacity=*/ 3,
        );

        // Add struct with all values
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Int32(1),
                RowValue::Float64(95.5),
            ]))
            .unwrap();

        // Add struct with null field
        builder
            .append_value(&RowValue::Struct(vec![RowValue::Int32(2), RowValue::Null]))
            .unwrap();

        // Add null struct
        builder.append_value(&RowValue::Null).unwrap();

        let array = builder.finish(&DataType::Struct(struct_fields.into()));
        assert_eq!(array.len(), 3);

        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();

        // Check struct validity
        assert!(!struct_array.is_null(0));
        assert!(!struct_array.is_null(1));
        assert!(struct_array.is_null(2));

        // Check values
        let id_column = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 1);
        assert_eq!(id_column.value(1), 2);
        assert!(id_column.is_null(2));

        let score_column = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(score_column.value(0), 95.5);
        assert!(score_column.is_null(1));
        assert!(score_column.is_null(2));
    }

    #[test]
    fn test_column_array_builder_struct_all_primitive_types() {
        use arrow::array::StructArray;

        // Test struct with all primitive types
        let struct_fields = vec![
            Arc::new(arrow::datatypes::Field::new(
                "bool_field",
                DataType::Boolean,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "int32_field",
                DataType::Int32,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "int64_field",
                DataType::Int64,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "float32_field",
                DataType::Float32,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "float64_field",
                DataType::Float64,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "decimal_field",
                DataType::Decimal128(10, 2),
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "string_field",
                DataType::Utf8,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "binary_field",
                DataType::Binary,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "fixed_binary_field",
                DataType::FixedSizeBinary(16),
                /*nullable=*/ true,
            )),
        ];

        let mut builder = ColumnArrayBuilder::new(
            &DataType::Struct(struct_fields.clone().into()),
            /*capacity=*/ 1,
        );

        // Add struct with all types
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Bool(true),
                RowValue::Int32(42),
                RowValue::Int64(1000),
                RowValue::Float32(std::f32::consts::PI),
                RowValue::Float64(std::f64::consts::E),
                RowValue::Decimal(12345),
                RowValue::ByteArray(b"test string".to_vec()),
                RowValue::ByteArray(b"binary data".to_vec()),
                RowValue::FixedLenByteArray([
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                ]),
            ]))
            .unwrap();

        let array = builder.finish(&DataType::Struct(struct_fields.into()));
        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();

        // Verify all fields
        assert_eq!(struct_array.num_columns(), 9);
        let bool_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0);
        assert!(bool_array);
        let int32_array = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(int32_array, 42);
        let int64_array = struct_array
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(int64_array, 1000);
        let float32_array = struct_array
            .column(3)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0);
        assert_eq!(float32_array, std::f32::consts::PI);
        let float64_array = struct_array
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(float64_array, std::f64::consts::E);
        let decimal_array = struct_array
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap()
            .value(0);
        assert_eq!(decimal_array, 12345);
        let string_array = struct_array
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(string_array, "test string");
        let binary_array = struct_array
            .column(7)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap()
            .value(0);
        assert_eq!(binary_array, b"binary data");
        let fixed_binary_array = struct_array
            .column(8)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap()
            .value(0);
        assert_eq!(
            fixed_binary_array,
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
    }

    #[test]
    fn test_column_array_builder_struct_list() {
        use arrow::array::{ListArray, StructArray};

        // Test List<Struct> type
        let struct_fields = vec![
            Arc::new(arrow::datatypes::Field::new(
                "id",
                DataType::Int32,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "name",
                DataType::Utf8,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "active",
                DataType::Boolean,
                /*nullable=*/ true,
            )),
        ];

        let mut builder = ColumnArrayBuilder::new(
            &DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Struct(struct_fields.clone().into()),
                /*nullable=*/ true,
            ))),
            /*capacity=*/ 2,
        );

        // Add a list of structs
        builder
            .append_value(&RowValue::Array(vec![
                RowValue::Struct(vec![
                    RowValue::Int32(1),
                    RowValue::ByteArray(b"Alice".to_vec()),
                    RowValue::Bool(true),
                ]),
                RowValue::Struct(vec![
                    RowValue::Int32(2),
                    RowValue::ByteArray(b"Bob".to_vec()),
                    RowValue::Bool(false),
                ]),
                RowValue::Null, // null struct in the list
            ]))
            .unwrap();

        // Add another list with mixed content
        builder
            .append_value(&RowValue::Array(vec![RowValue::Struct(vec![
                RowValue::Int32(3),
                RowValue::ByteArray(b"Charlie".to_vec()),
                RowValue::Bool(true),
            ])]))
            .unwrap();

        let array = builder.finish(&DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Struct(struct_fields.into()),
            /*nullable=*/ true,
        ))));

        assert_eq!(array.len(), 2);
        let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();

        // Check first list [struct1, struct2, null]
        let first_list = list_array.value(0);
        let first_struct_array = first_list.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(first_struct_array.len(), 3);

        // Check struct validity in first list
        assert!(!first_struct_array.is_null(0)); // first struct
        assert!(!first_struct_array.is_null(1)); // second struct
        assert!(first_struct_array.is_null(2)); // null struct

        // Check values in first list
        let id_column = first_struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 1);
        assert_eq!(id_column.value(1), 2);
        assert!(id_column.is_null(2));

        let name_column = first_struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_column.value(0), "Alice");
        assert_eq!(name_column.value(1), "Bob");
        assert!(name_column.is_null(2));

        let active_column = first_struct_array
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(active_column.value(0));
        assert!(!active_column.value(1));
        assert!(active_column.is_null(2));

        // Check second list [struct3]
        let second_list = list_array.value(1);
        let second_struct_array = second_list.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(second_struct_array.len(), 1);

        // Check struct validity in second list
        assert!(!second_struct_array.is_null(0));

        // Check values in second list
        let id_column = second_struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_column.value(0), 3);

        let name_column = second_struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_column.value(0), "Charlie");

        let active_column = second_struct_array
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(active_column.value(0));
    }

    #[test]
    fn test_column_array_builder_empty_struct() {
        use arrow::array::StructArray;
        use arrow::datatypes::Fields;

        // Test struct with no fields (empty struct)
        let struct_fields: Fields = Vec::<Arc<arrow::datatypes::Field>>::new().into();

        let mut builder = ColumnArrayBuilder::new(
            &DataType::Struct(struct_fields.clone()),
            /*capacity=*/ 3,
        );

        // Add empty structs
        builder.append_value(&RowValue::Struct(vec![])).unwrap();
        builder.append_value(&RowValue::Struct(vec![])).unwrap();

        // Add null struct
        builder.append_value(&RowValue::Null).unwrap();

        let array = builder.finish(&DataType::Struct(struct_fields));
        assert_eq!(array.len(), 3);

        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();

        // Check struct has no columns
        assert_eq!(struct_array.num_columns(), 0);

        // Check validity - first two are valid, third is null
        assert!(!struct_array.is_null(0));
        assert!(!struct_array.is_null(1));
        assert!(struct_array.is_null(2));
    }

    #[test]
    fn test_column_array_builder_nested_struct() {
        use arrow::array::StructArray;

        // Create nested struct: { outer: { inner: { value: int32 } } }
        let inner_fields = vec![Arc::new(arrow::datatypes::Field::new(
            "value",
            DataType::Int32,
            /*nullable=*/ true,
        ))];

        let outer_fields = vec![Arc::new(arrow::datatypes::Field::new(
            "inner",
            DataType::Struct(inner_fields.clone().into()),
            /*nullable=*/ true,
        ))];

        let root_fields = vec![Arc::new(arrow::datatypes::Field::new(
            "outer",
            DataType::Struct(outer_fields.clone().into()),
            /*nullable=*/ true,
        ))];

        let mut builder = ColumnArrayBuilder::new(
            &DataType::Struct(root_fields.clone().into()),
            /*capacity=*/ 2,
        );

        // Add nested struct with values
        builder
            .append_value(&RowValue::Struct(vec![RowValue::Struct(vec![
                RowValue::Struct(vec![RowValue::Int32(42)]),
            ])]))
            .unwrap();

        // Add nested struct with null inner struct
        builder
            .append_value(&RowValue::Struct(vec![RowValue::Struct(vec![
                RowValue::Null,
            ])]))
            .unwrap();

        let array = builder.finish(&DataType::Struct(root_fields.into()));
        assert_eq!(array.len(), 2);

        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.num_columns(), 1);

        // Check outer struct column
        let outer_column = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // Check first row - valid nested struct
        assert!(!outer_column.is_null(0));
        let inner_struct = outer_column
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(!inner_struct.is_null(0));
        let value_array = inner_struct
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(value_array.value(0), 42);

        // Check second row - null inner struct
        assert!(!outer_column.is_null(1));
        let inner_struct = outer_column
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(inner_struct.is_null(1));
    }

    #[test]
    fn test_column_array_builder_error_cases() {
        // Test type mismatch error - trying to append string to int32 builder
        let mut builder = ColumnArrayBuilder::new(&DataType::Int32, /*capacity=*/ 1);
        let result = builder.append_value(&RowValue::ByteArray(b"not_an_int".to_vec()));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Type mismatch"));

        // Test appending array to non-list builder
        let mut builder = ColumnArrayBuilder::new(&DataType::Boolean, /*capacity=*/ 1);
        let result = builder.append_value(&RowValue::Array(vec![RowValue::Bool(true)]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Got RowValue::Array for non-list builder"));

        // Test appending wrong type to float builder
        let mut builder = ColumnArrayBuilder::new(&DataType::Float32, /*capacity=*/ 1);
        let result = builder.append_value(&RowValue::Int32(42));
        assert!(result.is_err());

        // Test appending struct to primitive builder
        let mut builder = ColumnArrayBuilder::new(&DataType::Int64, /*capacity=*/ 1);
        let result = builder.append_value(&RowValue::Struct(vec![RowValue::Int64(100)]));
        assert!(result.is_err());
    }

    #[test]
    fn test_column_array_builder_struct_field_length_mismatch() {
        // Test struct with 3 fields
        let struct_fields = vec![
            Arc::new(arrow::datatypes::Field::new(
                "field1",
                DataType::Int32,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "field2",
                DataType::Utf8,
                /*nullable=*/ true,
            )),
            Arc::new(arrow::datatypes::Field::new(
                "field3",
                DataType::Boolean,
                /*nullable=*/ true,
            )),
        ];

        let mut builder = ColumnArrayBuilder::new(
            &DataType::Struct(struct_fields.clone().into()),
            /*capacity=*/ 2,
        );

        // Test 1: Append with fewer values than fields (should error)
        let result = builder.append_value(&RowValue::Struct(vec![
            RowValue::Int32(1),
            // field2 and field3 missing
        ]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Struct field count mismatch: expected 3 fields, got 1 values"));

        // Test 2: Append with exact number of values (should succeed)
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Int32(2),
                RowValue::ByteArray(b"test".to_vec()),
                RowValue::Bool(true),
            ]))
            .unwrap();

        // Test 3: Append with more values than fields (should error)
        let result = builder.append_value(&RowValue::Struct(vec![
            RowValue::Int32(3),
            RowValue::ByteArray(b"extra".to_vec()),
            RowValue::Bool(false),
            RowValue::Int32(999),                     // Extra value
            RowValue::ByteArray(b"ignored".to_vec()), // Extra value
        ]));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Struct field count mismatch: expected 3 fields, got 5 values"));

        // Test 4: Append with NULLs explicitly provided (should succeed)
        builder
            .append_value(&RowValue::Struct(vec![
                RowValue::Int32(4),
                RowValue::Null, // Explicit NULL
                RowValue::Bool(false),
            ]))
            .unwrap();

        let array = builder.finish(&DataType::Struct(struct_fields.into()));
        assert_eq!(array.len(), 2); // Only 2 successful appends

        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.num_columns(), 3);

        // Check field1 values
        let field1 = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(field1.value(0), 2);
        assert_eq!(field1.value(1), 4);

        // Check field2 values
        let field2 = struct_array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(field2.value(0), "test");
        assert!(field2.is_null(1)); // Explicit NULL

        // Check field3 values
        let field3 = struct_array
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(field3.value(0));
        assert!(!field3.value(1));
    }
}
