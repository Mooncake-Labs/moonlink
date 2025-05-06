use std::hash::{Hash, Hasher};

// Corresponds to the Parquet Types
#[derive(Debug, Clone, PartialEq, Default)]
pub enum RowValue {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Decimal(i128),
    Bool(bool),
    ByteArray(Vec<u8>),
    FixedLenByteArray([u8; 16]), // uuid & certain numeric
    #[default]
    Null,
}

impl RowValue {
    pub fn to_u64_key(&self) -> u64 {
        match self {
            RowValue::Int32(value) => *value as u64,
            RowValue::Int64(value) => *value as u64,
            RowValue::Float32(value) => value.to_bits() as u64,
            RowValue::Float64(value) => value.to_bits(),
            RowValue::Bool(value) => *value as u64,
            _ => {
                panic!("unsupported type for directly converting to u64 key");
            }
        }
    }
}

impl Hash for RowValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the variant discriminant to distinguish types
        std::mem::discriminant(self).hash(state);
        match self {
            RowValue::Int32(value) => value.hash(state),
            RowValue::Int64(value) => value.hash(state),
            RowValue::Float32(value) => value.to_bits().hash(state),
            RowValue::Float64(value) => value.to_bits().hash(state),
            RowValue::Decimal(value) => value.hash(state),
            RowValue::Bool(value) => value.hash(state),
            RowValue::ByteArray(bytes) => bytes.hash(state),
            RowValue::FixedLenByteArray(bytes) => bytes.hash(state),
            RowValue::Null => {} // Null: only the discriminant is hashed
        }
    }
}
