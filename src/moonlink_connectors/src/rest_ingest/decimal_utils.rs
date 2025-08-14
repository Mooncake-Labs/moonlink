use bigdecimal::num_bigint::{BigInt, TryFromBigIntError};
use bigdecimal::BigDecimal;
use moonlink::row::RowValue;
use num_traits::Signed;
use std::convert::TryInto;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum DecimalConversionError {
    #[error("Decimal normalization precision failed (value: {value}, parsed precision: {parsed_precision})")]
    NormalizationPrecision {
        value: String,
        parsed_precision: usize,
    },
    #[error("Decimal normalization scale failed (value: {value}, parsed scale: {parsed_scale})")]
    NormalizationScale { value: String, parsed_scale: i64 },
    #[error("Decimal precision exceeds the specified precision (value: {value}, expected ≤ {expected_precision}, actual {actual_precision})")]
    PrecisionOutOfRange {
        value: String,
        expected_precision: u8,
        actual_precision: u8,
    },
    #[error("Decimal scale exceeds the specified scale (value: {value}, expected ≤ {expected_scale}, actual {actual_scale})")]
    ScaleOutOfRange {
        value: String,
        expected_scale: i8,
        actual_scale: i8,
    },
    #[error("Decimal integer part exceeds the specified length (value: {value}, expected ≤ {expected_len}, actual {actual_len})")]
    IntegerPartOutOfRange {
        value: String,
        expected_len: i8,
        actual_len: i8,
    },
    #[error("Decimal value is invalid: {value})")]
    InvalidValue { value: String },
    #[error("Decimal scale is unsupported: {value})")]
    UnsupportedScale { value: String },
    #[error("Decimal mantissa overflow: {mantissa}, error: {err_msg}")]
    Overflow { mantissa: String, err_msg: String },
}

pub fn convert_decimal_to_row_value(
    value: &str,
    precision: u8,
    scale: i8,
) -> Result<RowValue, DecimalConversionError> {
    let decimal =
        BigDecimal::from_str(value).map_err(|_| DecimalConversionError::InvalidValue {
            value: value.to_string(),
        })?;
    let (mut decimal_mantissa, decimal_scale) = decimal.as_bigint_and_exponent();
    // Consider the negative sign
    let decimal_precision = if decimal_mantissa.is_negative() {
        decimal_mantissa.to_string().len() - 1
    } else {
        decimal_mantissa.to_string().len()
    };

    let actual_decimal_precision: u8 = decimal_precision.try_into().map_err(|_| {
        DecimalConversionError::NormalizationPrecision {
            value: value.to_string(),
            parsed_precision: decimal_precision,
        }
    })?;
    let actual_decimal_scale: i8 =
        decimal_scale
            .try_into()
            .map_err(|_| DecimalConversionError::NormalizationScale {
                value: value.to_string(),
                parsed_scale: decimal_scale,
            })?;

    // TODO: block the scale if it is negative
    if scale <= 0 {
        return Err(DecimalConversionError::UnsupportedScale {
            value: value.to_string(),
        });
    }

    if actual_decimal_precision > precision {
        return Err(DecimalConversionError::PrecisionOutOfRange {
            value: value.to_string(),
            expected_precision: precision,
            actual_precision: actual_decimal_precision,
        });
    }

    if actual_decimal_scale > scale {
        return Err(DecimalConversionError::ScaleOutOfRange {
            value: value.to_string(),
            expected_scale: scale,
            actual_scale: actual_decimal_scale,
        });
    }

    let max_integer_len = precision as i8 - scale;
    let actual_integer_len = actual_decimal_precision as i8 - actual_decimal_scale;

    if actual_integer_len > max_integer_len {
        return Err(DecimalConversionError::IntegerPartOutOfRange {
            value: value.to_string(),
            expected_len: max_integer_len,
            actual_len: actual_integer_len,
        });
    }

    if scale - actual_decimal_scale > 0 {
        // add the missing 0s to the decimal mantissa
        decimal_mantissa *= BigInt::from(10).pow((scale - actual_decimal_scale) as u32);
    }

    let actual_decimal_mantissa: i128 =
        (&decimal_mantissa)
            .try_into()
            .map_err(
                |e: TryFromBigIntError<()>| DecimalConversionError::Overflow {
                    mantissa: decimal_mantissa.to_string(),
                    err_msg: e.to_string(),
                },
            )?;
    Ok(RowValue::Decimal(actual_decimal_mantissa))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_decimal_invalid_value_error() {
        // Testing invalid decimal string format (double dots)
        let invalid_value = "123..45";
        let precision = 5;
        let scale = 2;
        let err = convert_decimal_to_row_value(invalid_value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::InvalidValue { value } => {
                assert_eq!(value, invalid_value.to_string());
            }
            _ => panic!("Expected an InvalidValue error, but got a different variant: {err:?}"),
        }
    }

    #[test]
    fn test_convert_decimal_precision_out_of_range_error() {
        // Testing decimal precision exceeding the specified limit (7 digits > 5 precision)
        let precision_exceeding_value = "123.4567";
        let precision = 5;
        let scale = 2;
        let err =
            convert_decimal_to_row_value(precision_exceeding_value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::PrecisionOutOfRange {
                value,
                expected_precision,
                actual_precision,
            } => {
                assert_eq!(value, precision_exceeding_value.to_string());
                assert_eq!(expected_precision, precision);
                assert_eq!(actual_precision, 7);
            }
            _ => {
                panic!("Expected a PrecisionOutOfRange error, but got a different variant: {err:?}")
            }
        }
    }

    #[test]
    fn test_convert_decimal_scale_out_of_range_error() {
        // Testing decimal scale exceeding the specified limit (4 fractional digits > 3 scale)
        let scale_exceeding_value = "123.4567";
        let precision = 8;
        let scale = 3;
        let err =
            convert_decimal_to_row_value(scale_exceeding_value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::ScaleOutOfRange {
                value,
                expected_scale,
                actual_scale,
            } => {
                assert_eq!(value, scale_exceeding_value.to_string());
                assert_eq!(expected_scale, scale);
                assert_eq!(actual_scale, 4);
            }
            _ => panic!("Expected a ScaleOutOfRange error, but got a different variant: {err:?}"),
        }

        // Testing negative scale with positive fractional digits (2 fractional digits > -2 scale)
        let negative_scale_value = "-123.45";
        let precision = 5;
        let scale = -2;
        let err = convert_decimal_to_row_value(negative_scale_value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::UnsupportedScale { value } => {
                assert_eq!(value, negative_scale_value.to_string());
            }
            _ => panic!("Expected an UnsupportedScale error, but got a different variant: {err:?}"),
        }
    }

    #[test]
    fn test_convert_decimal_integer_part_out_of_range_error() {
        // Testing integer part exceeding the allowed length (3 integer digits > 2 allowed)
        // With precision=5, scale=3: max integer digits = 5-3 = 2, but "123" has 3 digits
        let integer_part_exceeding_value = "123.4";
        let precision = 5;
        let scale = 3;
        let err = convert_decimal_to_row_value(integer_part_exceeding_value, precision, scale)
            .unwrap_err();
        match err {
            DecimalConversionError::IntegerPartOutOfRange {
                value,
                expected_len,
                actual_len,
            } => {
                assert_eq!(value, integer_part_exceeding_value.to_string());
                assert_eq!(expected_len, 2);
                assert_eq!(actual_len, 3);
            }
            _ => panic!(
                "Expected an IntegerPartOutOfRange error, but got a different variant: {err:?}"
            ),
        }

        // Testing negative value with integer part exceeding the allowed length
        // Sign is not counted towards precision, so "-123" still has 3 integer digits
        let integer_part_exceeding_negative_value = "-123.4";
        let precision = 5;
        let scale = 3;
        let err =
            convert_decimal_to_row_value(integer_part_exceeding_negative_value, precision, scale)
                .unwrap_err();
        match err {
            DecimalConversionError::IntegerPartOutOfRange {
                value,
                expected_len,
                actual_len,
            } => {
                assert_eq!(value, integer_part_exceeding_negative_value.to_string());
                assert_eq!(expected_len, 2);
                assert_eq!(actual_len, 3);
            }
            _ => panic!(
                "Expected an IntegerPartOutOfRange error, but got a different variant: {err:?}"
            ),
        }
    }

    #[test]
    fn test_convert_decimal_overflow_error() {
        // Testing mantissa overflow when the normalized decimal exceeds i128 range
        let overflow_value = "1234567890123456789012345678901234567.789";
        let precision = 40;
        let scale = 3;
        let err = convert_decimal_to_row_value(overflow_value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::Overflow { mantissa, err_msg } => {
                assert_eq!(mantissa, "1234567890123456789012345678901234567789");
                assert!(err_msg.contains("out of range")); // Ensure the error message contains means it is out of range
            }
            _ => panic!("Expected an Overflow error, but got a different variant: {err:?}"),
        }

        // Testing negative mantissa overflow when the normalized decimal exceeds i128 range
        let overflow_negative_value = "-1234567890123456789012345678901234567.789";
        let precision = 40;
        let scale = 3;
        let err =
            convert_decimal_to_row_value(overflow_negative_value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::Overflow { mantissa, err_msg } => {
                assert_eq!(mantissa, "-1234567890123456789012345678901234567789");
                assert!(err_msg.contains("out of range")); // Ensure the error message contains means it is out of range
            }
            _ => panic!("Expected an Overflow error, but got a different variant: {err:?}"),
        }
    }

    #[test]
    fn test_convert_decimal_to_row_value_valid() {
        let valid_value_1 = "123.45";
        let precision = 5;
        let scale = 2;
        let result = convert_decimal_to_row_value(valid_value_1, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(12345));

        let valid_value_2 = "12.4";
        let precision = 5;
        let scale = 3;
        let result = convert_decimal_to_row_value(valid_value_2, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(12400));

        let valid_negative_value = "-12.4";
        let precision = 5;
        let scale = 3;
        let result = convert_decimal_to_row_value(valid_negative_value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(-12400));

        let large_scale_value = "123456789012345678901234567890123456.789";
        let precision = 39;
        let scale = 3;
        let result = convert_decimal_to_row_value(large_scale_value, precision, scale).unwrap();
        assert_eq!(
            result,
            RowValue::Decimal(123456789012345678901234567890123456789)
        );

        let large_negative_scale_value = "-123456789012345678901234567890123456.789";
        let precision = 39;
        let scale = 3;
        let result =
            convert_decimal_to_row_value(large_negative_scale_value, precision, scale).unwrap();
        assert_eq!(
            result,
            RowValue::Decimal(-123456789012345678901234567890123456789)
        );
    }
}
