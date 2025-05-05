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
    pub fn to_u64(&self) -> u64 {
        match self {
            RowValue::Int32(value) => *value as u64,
            RowValue::Int64(value) => *value as u64,
            RowValue::Float32(value) => *value as u64,
            RowValue::Float64(value) => *value as u64,
            RowValue::Decimal(value) => *value as u64,
            RowValue::Bool(value) => *value as u64,
            RowValue::ByteArray(_value) => {
                todo!("Hash the byte array")
            }
            RowValue::FixedLenByteArray(_value) => {
                todo!("Hash the fixed length byte array")
            }
            RowValue::Null => {
                todo!("Hash the null value")
            }
        }
    }
}