pub mod moonlink {
    include!(concat!(env!("OUT_DIR"), "/moonlink.rs"));

    impl RowValue {
        pub fn int32(v: i32) -> Self {
            Self {
                kind: Some(row_value::Kind::Int32(v)),
            }
        }
        pub fn int64(v: i64) -> Self {
            Self {
                kind: Some(row_value::Kind::Int64(v)),
            }
        }
        pub fn float32(v: f32) -> Self {
            Self {
                kind: Some(row_value::Kind::Float32(v)),
            }
        }
        pub fn float64(v: f64) -> Self {
            Self {
                kind: Some(row_value::Kind::Float64(v)),
            }
        }
        pub fn decimal128_be<B: Into<Vec<u8>>>(b: B) -> Self {
            Self {
                kind: Some(row_value::Kind::Decimal128Be(b.into())),
            }
        }
        pub fn bool(v: bool) -> Self {
            Self {
                kind: Some(row_value::Kind::Bool(v)),
            }
        }
        pub fn bytes<B: Into<Vec<u8>>>(b: B) -> Self {
            Self {
                kind: Some(row_value::Kind::Bytes(b.into().into())),
            }
        }
        pub fn fixed_len_bytes<B: Into<Vec<u8>>>(b: B) -> Self {
            Self {
                kind: Some(row_value::Kind::FixedLenBytes(b.into().into())),
            }
        }
        pub fn array(a: Array) -> Self {
            Self {
                kind: Some(row_value::Kind::Array(a)),
            }
        }
        pub fn struct_(s: Struct) -> Self {
            Self {
                kind: Some(row_value::Kind::Struct(s)),
            }
        }
        pub fn null() -> Self {
            Self {
                kind: Some(row_value::Kind::Null(Null {})),
            }
        }
    }
}
