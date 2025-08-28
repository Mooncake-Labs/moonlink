use bigdecimal::num_bigint::{BigInt, TryFromBigIntError};
use bigdecimal::BigDecimal;
use moonlink::row::RowValue;
use num_traits::Signed;
use std::convert::TryInto;
use std::num::TryFromIntError;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DecimalConversionError {
    #[error("Decimal normalization precision failed (value: {value}, parsed precision: {parsed_precision}, err: {error})")]
    NormalizationPrecision {
        value: String,
        parsed_precision: usize,
        #[source]
        error: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Decimal normalization scale failed (value: {value}, parsed scale: {parsed_scale}, err: {error})")]
    NormalizationScale {
        value: String,
        parsed_scale: i64,
        #[source]
        error: Box<dyn std::error::Error + Send + Sync>,
    },
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
    #[error("Decimal value is invalid: {value}, err: {error}")]
    InvalidValue {
        value: String,
        #[source]
        error: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Decimal scale is unsupported: {value})")]
    UnsupportedScale { value: String },
    #[error("Decimal mantissa overflow: {mantissa}, error: {error}")]
    Overflow {
        mantissa: String,
        #[source]
        error: Box<dyn std::error::Error + Send + Sync>,
    },
}

fn convert_mantissa_to_row_value(mantissa: BigInt) -> Result<RowValue, DecimalConversionError> {
    let actual_decimal_mantissa: i128 =
        (&mantissa)
            .try_into()
            .map_err(
                |e: TryFromBigIntError<()>| DecimalConversionError::Overflow {
                    mantissa: mantissa.to_string(),
                    error: Box::new(e),
                },
            )?;
    Ok(RowValue::Decimal(actual_decimal_mantissa))
}

fn handle_negative_scale(
    value: BigDecimal,
    scale: i8,
    precision: u8,
) -> Result<RowValue, DecimalConversionError> {
    let (mantissa, _) = value.as_bigint_and_exponent();
    let power_of_10 = BigInt::from(10).pow(scale.unsigned_abs() as u32);
    let half_power = &power_of_10 / 2;
    let max_precision = precision as usize + scale.unsigned_abs() as usize;

    // Proper rounding for negative scale
    let rounded_mantissa: BigInt = if !mantissa.is_negative() {
        (&mantissa + &half_power) / &power_of_10 * &power_of_10
    } else {
        (&mantissa - &half_power) / &power_of_10 * &power_of_10
    };

    // Calculate the actual precision (number of significant digits)
    let rounded_precision = if rounded_mantissa == BigInt::from(0) {
        1 // Zero has precision 1
    } else {
        // Count the number of digits, excluding the negative sign
        let abs_mantissa = rounded_mantissa.abs();
        abs_mantissa.to_string().len()
    };

    if rounded_precision > max_precision {
        return Err(DecimalConversionError::PrecisionOutOfRange {
            value: rounded_mantissa.to_string(),
            expected_precision: precision,
            actual_precision: rounded_precision as u8,
        });
    }
    convert_mantissa_to_row_value(rounded_mantissa)
}

fn handle_fractional_only(
    value: BigDecimal,
    scale: i8,
    precision: u8,
) -> Result<RowValue, DecimalConversionError> {
    use bigdecimal::RoundingMode;
    let rounded_decimal = value.with_scale_round(scale as i64, RoundingMode::HalfUp);

    let (rounded_mantissa, rounded_exponent) = rounded_decimal.as_bigint_and_exponent();
    let rounded_precision = if rounded_mantissa == BigInt::from(0) {
        1
    } else {
        rounded_mantissa.abs().to_string().len()
    };

    let actual_scale = rounded_exponent as i8;
    let integer_part_length = rounded_precision as i8 - actual_scale;

    if integer_part_length > 0 {
        return Err(DecimalConversionError::IntegerPartOutOfRange {
            value: value.to_string(),
            expected_len: 0,
            actual_len: integer_part_length,
        });
    }

    if rounded_precision > precision as usize {
        return Err(DecimalConversionError::PrecisionOutOfRange {
            value: value.to_string(),
            expected_precision: precision,
            actual_precision: rounded_precision as u8,
        });
    }

    let mut final_mantissa = rounded_mantissa;
    if scale > actual_scale {
        let scale_diff = scale - actual_scale;
        final_mantissa *= BigInt::from(10).pow(scale_diff as u32);
    }

    convert_mantissa_to_row_value(final_mantissa)
}

fn handle_standard(
    value: BigDecimal,
    scale: i8,
    precision: u8,
) -> Result<RowValue, DecimalConversionError> {
    let (mut decimal_mantissa, decimal_scale) = value.as_bigint_and_exponent();
    // Consider the negative sign
    let decimal_precision = if decimal_mantissa.is_negative() {
        decimal_mantissa.to_string().len() - 1
    } else {
        decimal_mantissa.to_string().len()
    };

    let actual_decimal_precision: u8 =
        decimal_precision.try_into().map_err(|e: TryFromIntError| {
            DecimalConversionError::NormalizationPrecision {
                value: value.to_string(),
                parsed_precision: decimal_precision,
                error: Box::new(e),
            }
        })?;
    let actual_decimal_scale: i8 = decimal_scale.try_into().map_err(|e: TryFromIntError| {
        DecimalConversionError::NormalizationScale {
            value: value.to_string(),
            parsed_scale: decimal_scale,
            error: Box::new(e),
        }
    })?;

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

    convert_mantissa_to_row_value(decimal_mantissa)
}

// Based on https://www.postgresql.org/docs/17/datatype-numeric.html,
// Decimal values fall into 3 categories:
//
// 1. scale < 0
//    Negative scale → requires rounding to the nearest 10^|scale|
//    (e.g. NUMERIC(2, -3): 9976 → 10000).
//
// 2. scale > precision
//    Pure fractional case (no integer part). Validate exactness or error if rounding needed.
//
// 3. 0 <= scale <= precision
//    Normal case: integer and fractional parts handled directly.
//
// TODO: Infinity / NaN not supported yet — open issue if needed.
pub fn convert_decimal_to_row_value(
    value: &str,
    precision: u8,
    scale: i8,
) -> Result<RowValue, DecimalConversionError> {
    let decimal = BigDecimal::from_str(value).map_err(|e: bigdecimal::ParseBigDecimalError| {
        DecimalConversionError::InvalidValue {
            value: value.to_string(),
            error: Box::new(e),
        }
    })?;

    if scale < 0 {
        handle_negative_scale(decimal, scale, precision)
    } else if scale > precision as i8 {
        return handle_fractional_only(decimal, scale, precision);
    } else {
        return handle_standard(decimal, scale, precision);
    }
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
            DecimalConversionError::InvalidValue { value, error } => {
                assert_eq!(value, invalid_value.to_string());
                assert!(error.to_string().contains("invalid digit found in string"));
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

        // Testing decimal precision exceeding the specified limit (4 digits > 3 precision)
        let value = "0.009999";
        let precision = 3;
        let scale = 5;
        let err = convert_decimal_to_row_value(value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::PrecisionOutOfRange {
                expected_precision,
                actual_precision,
                ..
            } => {
                assert_eq!(expected_precision, 3);
                assert_eq!(actual_precision, 4);
            }
            _ => panic!("Expected PrecisionOutOfRange error, got {err:?}"),
        }

        // Test NUMERIC(3, 5) - only fractional values allowed
        let value = "0.12345";
        let precision = 3;
        let scale = 5;
        let err = convert_decimal_to_row_value(value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::PrecisionOutOfRange {
                expected_precision,
                actual_precision,
                ..
            } => {
                assert_eq!(expected_precision, 3);
                assert_eq!(actual_precision, 5);
            }
            _ => panic!("Expected PrecisionOutOfRange error, got {err:?}"),
        }

        // Testing decimal precision exceeding the specified limit (6 digits > 2 precision)
        let value = "99500";
        let precision = 2;
        let scale = -3;
        let err = convert_decimal_to_row_value(value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::PrecisionOutOfRange {
                expected_precision,
                actual_precision,
                ..
            } => {
                assert_eq!(expected_precision, 2);
                assert_eq!(actual_precision, 6);
            }
            _ => panic!("Expected PrecisionOutOfRange error, got {err:?}"),
        }

        // Testing negative decimal precision exceeding the specified limit (6 digits > 2 precision)
        let value = "-99500";
        let precision = 2;
        let scale = -3;
        let err = convert_decimal_to_row_value(value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::PrecisionOutOfRange {
                expected_precision,
                actual_precision,
                ..
            } => {
                assert_eq!(expected_precision, 2);
                assert_eq!(actual_precision, 6);
            }
            _ => panic!("Expected PrecisionOutOfRange error, got {err:?}"),
        }

        // Testing decimal precision exceeding the specified limit (6 digits > 2 precision)
        let value = "990010";
        let precision = 2;
        let scale = -3;
        let err = convert_decimal_to_row_value(value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::PrecisionOutOfRange {
                expected_precision,
                actual_precision,
                ..
            } => {
                assert_eq!(expected_precision, 2);
                assert_eq!(actual_precision, 6);
            }
            _ => panic!("Expected PrecisionOutOfRange error, got {err:?}"),
        }

        // Testing negative decimal precision exceeding the specified limit (6 digits > 2 precision)
        let value = "-990010";
        let precision = 2;
        let scale = -3;
        let err = convert_decimal_to_row_value(value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::PrecisionOutOfRange {
                expected_precision,
                actual_precision,
                ..
            } => {
                assert_eq!(expected_precision, 2);
                assert_eq!(actual_precision, 6);
            }
            _ => panic!("Expected PrecisionOutOfRange error, got {err:?}"),
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

        // Testing decimal integer part should be 0
        let value = "1.009999";
        let precision = 3;
        let scale = 5;
        let err = convert_decimal_to_row_value(value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::IntegerPartOutOfRange {
                expected_len,
                actual_len,
                ..
            } => {
                assert_eq!(expected_len, 0);
                assert_eq!(actual_len, 1);
            }
            _ => panic!("Expected PrecisionOutOfRange error, got {err:?}"),
        }

        // Testing decimal integer part should be 0
        let value = "89.009999";
        let precision = 3;
        let scale = 5;
        let err = convert_decimal_to_row_value(value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::IntegerPartOutOfRange {
                expected_len,
                actual_len,
                ..
            } => {
                assert_eq!(expected_len, 0);
                assert_eq!(actual_len, 2);
            }
            _ => panic!("Expected PrecisionOutOfRange error, got {err:?}"),
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
            DecimalConversionError::Overflow { mantissa, error } => {
                assert_eq!(mantissa, "1234567890123456789012345678901234567789");
                assert!(
                    error.is::<TryFromBigIntError<()>>(),
                    "The source error of Overflow should be TryFromBigIntError"
                );
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
            DecimalConversionError::Overflow { mantissa, error } => {
                assert_eq!(mantissa, "-1234567890123456789012345678901234567789");
                assert!(
                    error.is::<TryFromBigIntError<()>>(),
                    "The source error of Overflow should be TryFromBigIntError"
                );
            }
            _ => panic!("Expected an Overflow error, but got a different variant: {err:?}"),
        }
    }

    #[test]
    fn test_convert_decimal_to_row_value_valid_standard() {
        let value = "123.45";
        let precision = 5;
        let scale = 2;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
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

    #[test]
    fn test_convert_decimal_to_row_value_valid_negative_scale() {
        // Test NUMERIC(2, -3) - values rounded to nearest thousand
        // Test Positive values
        let value = "444";
        let precision = 2;
        let scale = -3;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(0));

        let value = "500";
        let precision = 2;
        let scale = -3;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(1000));

        let value = "9976";
        let precision = 3;
        let scale = -2;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(10000));

        let value = "12345";
        let precision = 3;
        let scale = -2;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(12300));

        // Test negative values
        let value = "-444";
        let precision = 2;
        let scale = -3;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(0));

        let value = "-500";
        let precision = 2;
        let scale = -3;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(-1000));

        let value = "-9976";
        let precision = 3;
        let scale = -2;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(-10000));

        let value = "-12345";
        let precision = 3;
        let scale = -2;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(-12300));
    }

    #[test]
    fn test_convert_decimal_to_row_value_valid_fractional_only() {
        let value = "0";
        let precision = 3;
        let scale = 5;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(0));

        let value = "0.0099945";
        let precision = 3;
        let scale = 5;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(999));

        let value = "0.009945";
        let precision = 3;
        let scale = 5;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(995));

        let value = "0.00994";
        let precision = 3;
        let scale = 5;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(994));

        let value = "0.0099";
        let precision = 3;
        let scale = 5;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(990));

        let value = "-0.0099945";
        let precision = 3;
        let scale = 5;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(-999));

        let value = "-0.009945";
        let precision = 3;
        let scale = 5;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(-995));

        let value = "-0.00994";
        let precision = 3;
        let scale = 5;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(-994));

        let value = "-0.0099";
        let precision = 3;
        let scale = 5;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(-990));
    }
}
