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

use iceberg::spec::{Datum, PrimitiveType, SchemaRef, Type};
use iceberg::Result as IcebergResult;
use iceberg::{Error, ErrorKind};
use num_bigint::BigInt;
use num_traits::cast::ToPrimitive;
use parquet::file::statistics::Statistics;
use uuid::Uuid;

use std::collections::HashMap;

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
            // TODO(hjiang): Add unit test.
            let value = i128::from_be_bytes(bytes.try_into()?);
            Some(Datum::decimal(value)?)
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
            let unscaled_value = BigInt::from_signed_bytes_be(bytes);
            // TODO(hjiang): Add unit test.
            let value = unscaled_value.to_i128().ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Can't convert bytes to i128: {bytes:?}"),
                )
            })?;
            Some(Datum::decimal(value)?)
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            },
            Statistics::Int32(stats),
        ) => stats.min_opt().map(|val| {
            // TODO(hjiang): Add unit test.
            let value = i128::from(*val);
            Datum::decimal(value).unwrap()
        }),
        (
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            },
            Statistics::Int64(stats),
        ) => stats.min_opt().map(|val| {
            // TODO(hjiang): Add unit test.
            let value = i128::from(*val);
            Datum::decimal(value).unwrap()
        }),
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
            // TODO(hjiang): Add unit test.
            let value = i128::from_be_bytes(bytes.try_into()?);
            Some(Datum::decimal(value)?)
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
            // TODO(hjiang): Add unit test.
            let unscaled_value = BigInt::from_signed_bytes_be(bytes);
            let value = unscaled_value.to_i128().ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Can't convert bytes to i128: {bytes:?}"),
                )
            })?;
            Some(Datum::decimal(value)?)
        }
        (
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            },
            Statistics::Int32(stats),
        ) => stats.max_opt().map(|val| {
            // TODO(hjiang): Add unit test.
            let value = i128::from(*val);
            Datum::decimal(value).unwrap()
        }),
        (
            PrimitiveType::Decimal {
                precision: _,
                scale: _,
            },
            Statistics::Int64(stats),
        ) => stats.max_opt().map(|val| {
            // TODO(hjiang): Add unit test.
            let value = i128::from(*val);
            Datum::decimal(value).unwrap()
        }),
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
    schema: SchemaRef,
}

impl MinMaxColAggregator {
    /// Creates new and empty `MinMaxColAggregator`
    pub(crate) fn new(schema: SchemaRef) -> Self {
        Self {
            lower_bounds: HashMap::new(),
            upper_bounds: HashMap::new(),
            schema,
        }
    }

    pub(crate) fn update_state_min(&mut self, field_id: i32, datum: Datum) {
        self.lower_bounds
            .entry(field_id)
            .and_modify(|e| {
                if *e > datum {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    pub(crate) fn update_state_max(&mut self, field_id: i32, datum: Datum) {
        self.upper_bounds
            .entry(field_id)
            .and_modify(|e| {
                if *e > datum {
                    *e = datum.clone()
                }
            })
            .or_insert(datum);
    }

    /// Update statistics
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

        if value.min_is_exact() {
            let Some(min_datum) = get_parquet_stat_min_as_datum(&ty, &value)? else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Statistics {value} is not match with field type {ty}."),
                ));
            };

            self.update_state_min(field_id, min_datum);
        }

        if value.max_is_exact() {
            let Some(max_datum) = get_parquet_stat_max_as_datum(&ty, &value)? else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!("Statistics {value} is not match with field type {ty}."),
                ));
            };

            self.update_state_max(field_id, max_datum);
        }

        Ok(())
    }

    /// Returns lower and upper bounds
    pub(crate) fn produce(self) -> (HashMap<i32, Datum>, HashMap<i32, Datum>) {
        (self.lower_bounds, self.upper_bounds)
    }
}
