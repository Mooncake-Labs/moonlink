// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Code adapted from iceberg-rust: https://github.com/apache/iceberg-rust

use iceberg::spec::{Datum, PrimitiveLiteral, PrimitiveType, SchemaRef, Type};
use iceberg::Result as IcebergResult;
use iceberg::{Error, ErrorKind};
use num_bigint::BigInt;
use num_traits::cast::ToPrimitive;
use parquet::file::statistics::Statistics;
use uuid::Uuid;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

/// Bound for a decimal column from its unscaled value. Built from the raw bytes with the column's
/// own type: `Datum::decimal` goes through rust_decimal, which panics above 96 bits (decimal(38,s)
/// holds 127). None when the value does not fit the declared precision, so no wrong bound is
/// written.
fn decimal_datum(primitive_type: &PrimitiveType, unscaled: i128) -> Option<Datum> {
    let PrimitiveType::Decimal { precision, .. } = primitive_type else {
        return None;
    };
    let limit = 10u128.checked_pow(*precision)?;
    if unscaled.unsigned_abs() >= limit {
        return None;
    }
    Datum::try_from_bytes(&unscaled.to_be_bytes(), primitive_type.clone()).ok()
}

/// Order of two bounds of the same column. Decimals are compared by their unscaled values (same
/// type, same scale): `Datum`'s own ordering converts them to rust_decimal, which panics above 96
/// bits.
fn compare_bounds(a: &Datum, b: &Datum) -> Option<Ordering> {
    match (a.literal(), b.literal()) {
        (PrimitiveLiteral::Int128(x), PrimitiveLiteral::Int128(y))
            if a.data_type() == b.data_type() =>
        {
            Some(x.cmp(y))
        }
        _ => a.partial_cmp(b),
    }
}

// ================================
// get_parquet_stat_min_as_datum
// ================================
//
pub(crate) fn get_parquet_stat_min_as_datum(
    primitive_type: &PrimitiveType,
    stats: &Statistics,
) -> IcebergResult<Option<Datum>> {
    Ok(match (primitive_type, stats) {
        (PrimitiveType::Boolean, Statistics::Boolean(stats)) => {
            stats.min_opt().map(|val| Datum::bool(*val))
        }
        (PrimitiveType::Int, Statistics::Int32(stats)) => {
            stats.min_opt().map(|val| Datum::int(*val))
        }
        (PrimitiveType::Date, Statistics::Int32(stats)) => {
            stats.min_opt().map(|val| Datum::date(*val))
        }
        (PrimitiveType::Long, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::long(*val))
        }
        (PrimitiveType::Time, Statistics::Int64(stats)) => {
            let Some(val) = stats.min_opt() else {
                return Ok(None);
            };

            Some(Datum::time_micros(*val)?)
        }
        (PrimitiveType::Timestamp, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::timestamp_micros(*val))
        }
        (PrimitiveType::Timestamptz, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::timestamptz_micros(*val))
        }
        (PrimitiveType::TimestampNs, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::timestamp_nanos(*val))
        }
        (PrimitiveType::TimestamptzNs, Statistics::Int64(stats)) => {
            stats.min_opt().map(|val| Datum::timestamptz_nanos(*val))
        }
        (PrimitiveType::Float, Statistics::Float(stats)) => {
            stats.min_opt().map(|val| Datum::float(*val))
        }
        (PrimitiveType::Double, Statistics::Double(stats)) => {
            stats.min_opt().map(|val| Datum::double(*val))
        }
        (PrimitiveType::String, Statistics::ByteArray(stats)) => {
            let Some(val) = stats.min_opt() else {
                return Ok(None);
            };

            Some(Datum::string(val.as_utf8()?))
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            },
            Statistics::ByteArray(stats),
        ) => {
            let Some(bytes) = stats.min_bytes_opt() else {
                return Ok(None);
            };
            let Ok(bytes) = <[u8; 16]>::try_from(bytes) else {
                return Ok(None);
            };
            decimal_datum(primitive_type, i128::from_be_bytes(bytes))
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            },
            Statistics::FixedLenByteArray(stats),
        ) => {
            let Some(bytes) = stats.min_bytes_opt() else {
                return Ok(None);
            };
            let Some(value) = BigInt::from_signed_bytes_be(bytes).to_i128() else {
                return Ok(None);
            };
            decimal_datum(primitive_type, value)
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            },
            Statistics::Int32(stats),
        ) => stats
            .min_opt()
            .and_then(|val| decimal_datum(primitive_type, i128::from(*val))),
        (
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            },
            Statistics::Int64(stats),
        ) => stats
            .min_opt()
            .and_then(|val| decimal_datum(primitive_type, i128::from(*val))),
        (PrimitiveType::Uuid, Statistics::FixedLenByteArray(stats)) => {
            let Some(bytes) = stats.min_bytes_opt() else {
                return Ok(None);
            };
            if bytes.len() != 16 {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Invalid length of uuid bytes.",
                ));
            }
            Some(Datum::uuid(Uuid::from_bytes(
                bytes[..16].try_into().unwrap(),
            )))
        }
        (PrimitiveType::Fixed(len), Statistics::FixedLenByteArray(stat)) => {
            let Some(bytes) = stat.min_bytes_opt() else {
                return Ok(None);
            };
            if bytes.len() != *len as usize {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Invalid length of fixed bytes.",
                ));
            }
            Some(Datum::fixed(bytes.to_vec()))
        }
        (PrimitiveType::Binary, Statistics::ByteArray(stat)) => {
            return Ok(stat
                .min_bytes_opt()
                .map(|bytes| Datum::binary(bytes.to_vec())));
        }
        _ => {
            return Ok(None);
        }
    })
}

// ================================
// get_parquet_stat_max_as_datum
// ================================
//
pub(crate) fn get_parquet_stat_max_as_datum(
    primitive_type: &PrimitiveType,
    stats: &Statistics,
) -> IcebergResult<Option<Datum>> {
    Ok(match (primitive_type, stats) {
        (PrimitiveType::Boolean, Statistics::Boolean(stats)) => {
            stats.max_opt().map(|val| Datum::bool(*val))
        }
        (PrimitiveType::Int, Statistics::Int32(stats)) => {
            stats.max_opt().map(|val| Datum::int(*val))
        }
        (PrimitiveType::Date, Statistics::Int32(stats)) => {
            stats.max_opt().map(|val| Datum::date(*val))
        }
        (PrimitiveType::Long, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::long(*val))
        }
        (PrimitiveType::Time, Statistics::Int64(stats)) => {
            let Some(val) = stats.max_opt() else {
                return Ok(None);
            };

            Some(Datum::time_micros(*val)?)
        }
        (PrimitiveType::Timestamp, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::timestamp_micros(*val))
        }
        (PrimitiveType::Timestamptz, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::timestamptz_micros(*val))
        }
        (PrimitiveType::TimestampNs, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::timestamp_nanos(*val))
        }
        (PrimitiveType::TimestamptzNs, Statistics::Int64(stats)) => {
            stats.max_opt().map(|val| Datum::timestamptz_nanos(*val))
        }
        (PrimitiveType::Float, Statistics::Float(stats)) => {
            stats.max_opt().map(|val| Datum::float(*val))
        }
        (PrimitiveType::Double, Statistics::Double(stats)) => {
            stats.max_opt().map(|val| Datum::double(*val))
        }
        (PrimitiveType::String, Statistics::ByteArray(stats)) => {
            let Some(val) = stats.max_opt() else {
                return Ok(None);
            };

            Some(Datum::string(val.as_utf8()?))
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            },
            Statistics::ByteArray(stats),
        ) => {
            let Some(bytes) = stats.max_bytes_opt() else {
                return Ok(None);
            };
            let Ok(bytes) = <[u8; 16]>::try_from(bytes) else {
                return Ok(None);
            };
            decimal_datum(primitive_type, i128::from_be_bytes(bytes))
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            },
            Statistics::FixedLenByteArray(stats),
        ) => {
            let Some(bytes) = stats.max_bytes_opt() else {
                return Ok(None);
            };
            let Some(value) = BigInt::from_signed_bytes_be(bytes).to_i128() else {
                return Ok(None);
            };
            decimal_datum(primitive_type, value)
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            },
            Statistics::Int32(stats),
        ) => stats
            .max_opt()
            .and_then(|val| decimal_datum(primitive_type, i128::from(*val))),
        (
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            },
            Statistics::Int64(stats),
        ) => stats
            .max_opt()
            .and_then(|val| decimal_datum(primitive_type, i128::from(*val))),
        (PrimitiveType::Uuid, Statistics::FixedLenByteArray(stats)) => {
            let Some(bytes) = stats.max_bytes_opt() else {
                return Ok(None);
            };
            if bytes.len() != 16 {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Invalid length of uuid bytes.",
                ));
            }
            Some(Datum::uuid(Uuid::from_bytes(
                bytes[..16].try_into().unwrap(),
            )))
        }
        (PrimitiveType::Fixed(len), Statistics::FixedLenByteArray(stat)) => {
            let Some(bytes) = stat.max_bytes_opt() else {
                return Ok(None);
            };
            if bytes.len() != *len as usize {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Invalid length of fixed bytes.",
                ));
            }
            Some(Datum::fixed(bytes.to_vec()))
        }
        (PrimitiveType::Binary, Statistics::ByteArray(stat)) => {
            return Ok(stat
                .max_bytes_opt()
                .map(|bytes| Datum::binary(bytes.to_vec())));
        }
        _ => {
            return Ok(None);
        }
    })
}

// ================================
// MinMaxColAggregator
// ================================
//
// Used to aggregate min and max value of each column.
pub(crate) struct MinMaxColAggregator {
    lower_bounds: HashMap<i32, Datum>,
    upper_bounds: HashMap<i32, Datum>,
    /// Columns with a row group whose min (or max) is unknown: the file then has no valid bound.
    no_lower_bound: HashSet<i32>,
    no_upper_bound: HashSet<i32>,
    schema: SchemaRef,
}

impl MinMaxColAggregator {
    /// Creates new and empty `MinMaxColAggregator`
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self {
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            no_lower_bound: HashSet::new(),
            no_upper_bound: HashSet::new(),
            schema,
        }
    }

    fn merge_bound(
        bounds: &mut HashMap<i32, Datum>,
        unknown: &mut HashSet<i32>,
        field_id: i32,
        datum: Option<Datum>,
        keep: Ordering,
    ) {
        if unknown.contains(&field_id) {
            return;
        }
        let Some(datum) = datum else {
            bounds.remove(&field_id);
            unknown.insert(field_id);
            return;
        };
        let Some(current) = bounds.get(&field_id) else {
            bounds.insert(field_id, datum);
            return;
        };
        match compare_bounds(&datum, current) {
            Some(order) if order == keep => {
                bounds.insert(field_id, datum);
            }
            Some(_) => {}
            None => {
                bounds.remove(&field_id);
                unknown.insert(field_id);
            }
        }
    }

    pub(crate) fn update_state_min(&mut self, field_id: i32, datum: Option<Datum>) {
        Self::merge_bound(
            &mut self.lower_bounds,
            &mut self.no_lower_bound,
            field_id,
            datum,
            Ordering::Less,
        );
    }

    pub(crate) fn update_state_max(&mut self, field_id: i32, datum: Option<Datum>) {
        Self::merge_bound(
            &mut self.upper_bounds,
            &mut self.no_upper_bound,
            field_id,
            datum,
            Ordering::Greater,
        );
    }

    /// Update statistics with one row group's column chunk. A chunk without min/max has no
    /// non-null values and does not affect the bounds; one whose min or max is inexact or cannot be
    /// represented leaves the column without that bound (bounds are optional, wrong ones prune
    /// rows on read).
    pub(crate) fn update(&mut self, field_id: i32, value: Statistics) -> IcebergResult<()> {
        let Some(ty) = self
            .schema
            .field_by_id(field_id)
            .map(|f| f.field_type.as_ref())
        else {
            // Following java implementation: https://github.com/apache/iceberg/blob/29a2c456353a6120b8c882ed2ab544975b168d7b/parquet/src/main/java/org/apache/iceberg/parquet/ParquetUtil.java#L163
            // Ignore the field if it is not in schema.
            return Ok(());
        };
        let Type::Primitive(ty) = ty.clone() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Composed type {ty} is not supported for min max aggregation."),
            ));
        };

        if value.min_bytes_opt().is_some() {
            let min = if value.min_is_exact() {
                get_parquet_stat_min_as_datum(&ty, &value)?
            } else {
                None
            };
            self.update_state_min(field_id, min);
        }
        if value.max_bytes_opt().is_some() {
            let max = if value.max_is_exact() {
                get_parquet_stat_max_as_datum(&ty, &value)?
            } else {
                None
            };
            self.update_state_max(field_id, max);
        }

        Ok(())
    }

    /// Returns lower and upper bounds
    pub(crate) fn produce(self) -> (HashMap<i32, Datum>, HashMap<i32, Datum>) {
        (self.lower_bounds, self.upper_bounds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decimal(precision: u32, scale: u32) -> PrimitiveType {
        PrimitiveType::Decimal { precision, scale }
    }

    #[test]
    fn test_decimal_datum_keeps_the_column_type_and_rejects_values_beyond_its_precision() {
        let max = 10i128.pow(38) - 1;
        for value in [max, -max, 0, 1 << 100, -(1 << 100)] {
            let datum = decimal_datum(&decimal(38, 10), value).unwrap();
            assert_eq!(datum.data_type(), &decimal(38, 10));
            assert_eq!(datum.literal(), &PrimitiveLiteral::Int128(value));
        }
        assert!(decimal_datum(&decimal(38, 10), max + 1).is_none());
        assert!(decimal_datum(&decimal(38, 10), i128::MIN).is_none());
        assert!(decimal_datum(&decimal(9, 2), 1_000_000_000).is_none());
        assert!(decimal_datum(&decimal(9, 2), -999_999_999).is_some());
        assert!(decimal_datum(&PrimitiveType::Long, 1).is_none());
    }

    #[test]
    fn test_decimal_bounds_beyond_96_bits_compare_by_value() {
        let big = decimal_datum(&decimal(38, 10), 1 << 120).unwrap();
        let small = decimal_datum(&decimal(38, 10), -(1 << 120)).unwrap();
        assert_eq!(compare_bounds(&small, &big), Some(Ordering::Less));
        assert_eq!(compare_bounds(&big, &small), Some(Ordering::Greater));
        // Different scales are not compared by unscaled value.
        let other_scale = decimal_datum(&decimal(38, 0), 1).unwrap();
        let one = decimal_datum(&decimal(38, 10), 1).unwrap();
        assert_ne!(compare_bounds(&other_scale, &one), Some(Ordering::Equal));
    }
}
