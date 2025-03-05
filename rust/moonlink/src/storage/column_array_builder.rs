use arrow::array::builder::{ArrayBuilder, PrimitiveBuilder, StringBuilder};
use arrow::array::types::{Float32Type, Float64Type, Int32Type, Int64Type};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use pg_replicate::conversions::Cell;

/// A column array builder that can handle different types
pub struct ColumnArrayBuilder {
    builder: Box<dyn ArrayBuilder>,
    data_type: DataType,
}

impl ColumnArrayBuilder {
    /// Create a new column array builder for a specific data type
    pub fn new(data_type: DataType, capacity: usize) -> Self {
        let builder: Box<dyn ArrayBuilder> = match data_type {
            DataType::Int32 => Box::new(PrimitiveBuilder::<Int32Type>::with_capacity(capacity)),
            DataType::Int64 => Box::new(PrimitiveBuilder::<Int64Type>::with_capacity(capacity)),
            DataType::Float32 => Box::new(PrimitiveBuilder::<Float32Type>::with_capacity(capacity)),
            DataType::Float64 => Box::new(PrimitiveBuilder::<Float64Type>::with_capacity(capacity)),
            DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 10)),
            _ => panic!("Unsupported data type: {:?}", data_type),
        };

        Self { builder, data_type }
    }

    /// Append a value to this builder
    pub fn append_value(&mut self, value: &Cell) {
        match (&self.data_type, value) {
            // Int32 handling
            (DataType::Int32, Cell::I32(v)) => {
                if let Some(builder) = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveBuilder<Int32Type>>()
                {
                    builder.append_value(*v);
                }
            }
            // Int64 handling
            (DataType::Int64, Cell::I64(v)) => {
                if let Some(builder) = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveBuilder<Int64Type>>()
                {
                    builder.append_value(*v);
                }
            }
            // Float32 handling
            (DataType::Float32, Cell::F32(v)) => {
                if let Some(builder) = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveBuilder<Float32Type>>()
                {
                    builder.append_value(*v);
                }
            }
            // Float64 handling
            (DataType::Float64, Cell::F64(v)) => {
                if let Some(builder) = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveBuilder<Float64Type>>()
                {
                    builder.append_value(*v);
                }
            }
            // String handling
            (DataType::Utf8, Cell::String(v)) => {
                if let Some(builder) = self.builder.as_any_mut().downcast_mut::<StringBuilder>() {
                    builder.append_value(v);
                }
            }
            // Null handling for any type
            (_, Cell::Null) => self.append_null(),
            // Type mismatch
            _ => {
                panic!(
                    "Type mismatch: expected {:?}, got {:?}",
                    self.data_type, value
                );
            }
        }
    }

    /// Append a null value to this builder
    pub fn append_null(&mut self) {
        match self.data_type {
            DataType::Int32 => {
                if let Some(builder) = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveBuilder<Int32Type>>()
                {
                    builder.append_null();
                }
            }
            DataType::Int64 => {
                if let Some(builder) = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveBuilder<Int64Type>>()
                {
                    builder.append_null();
                }
            }
            DataType::Float32 => {
                if let Some(builder) = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveBuilder<Float32Type>>()
                {
                    builder.append_null();
                }
            }
            DataType::Float64 => {
                if let Some(builder) = self
                    .builder
                    .as_any_mut()
                    .downcast_mut::<PrimitiveBuilder<Float64Type>>()
                {
                    builder.append_null();
                }
            }
            DataType::Utf8 => {
                if let Some(builder) = self.builder.as_any_mut().downcast_mut::<StringBuilder>() {
                    builder.append_null();
                }
            }
            _ => panic!(
                "Unsupported data type for append_null: {:?}",
                self.data_type
            ),
        }
    }

    /// Finish building and return the array
    pub fn finish(&mut self) -> ArrayRef {
        self.builder.finish()
    }

    pub fn finish_cloned(&self) -> ArrayRef {
        self.builder.finish_cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_array_builder() {
        let mut builder = ColumnArrayBuilder::new(DataType::Int32, 10);
        builder.append_value(&Cell::I32(1));
        builder.append_value(&Cell::I32(2));
        builder.append_value(&Cell::I32(3));
        let array = builder.finish();
        assert_eq!(array.len(), 3);
        println!("{:?}", array);
    }
}
