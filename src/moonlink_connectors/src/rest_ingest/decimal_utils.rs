use bigdecimal::num_bigint::BigInt;
use bigdecimal::BigDecimal;
use moonlink::row::RowValue;
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
    PrecisionExceeds {
        value: String,
        expected_precision: u8,
        actual_precision: u8,
    },
    #[error("Decimal scale exceeds the specified scale (value: {value}, expected ≤ {expected_scale}, actual {actual_scale})")]
    ScaleExceeds {
        value: String,
        expected_scale: i8,
        actual_scale: i8,
    },
    #[error("Decimal value is invalid: {value})")]
    InvalidValue { value: String },
    #[error("Decimal mantissa overflow: {mantissa}, error: {err_msg}")]
    Overflow { mantissa: String, err_msg: String },
}

/*
Convert a decimal string to an **unscaled** integer (`RowValue::Decimal`)
under schema precision `p` (max total digits) and scale `s` (max fractional).

Rules:
- Up to `p` digits (sign excluded) and up to `s` fractional digits accepted.
- If fractional digits < `s`, normalize by zero-padding to `s`.
- Store as `mantissa = value * 10^s` (no rounding).

Examples (`p=10, s=4`):
- "123" → 1230000   (123.0000)
- "123.45" → 1234500 (123.4500)
- "-123.45" → -1234500 (-123.4500)
- "2.333" with `s=2` → error
*/
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
    let decimal_precision = if decimal_mantissa < BigInt::from(0) {
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

    if actual_decimal_precision > precision {
        return Err(DecimalConversionError::PrecisionExceeds {
            value: value.to_string(),
            expected_precision: precision,
            actual_precision: actual_decimal_precision,
        });
    }

    if actual_decimal_scale > scale {
        return Err(DecimalConversionError::ScaleExceeds {
            value: value.to_string(),
            expected_scale: scale,
            actual_scale: actual_decimal_scale,
        });
    }

    // add the missing 0s to the decimal mantissa
    decimal_mantissa *= BigInt::from(10).pow((scale - actual_decimal_scale).max(0) as u32);

    let actual_decimal_mantissa: i128 = (&decimal_mantissa).try_into().map_err(
        |e: bigdecimal::num_bigint::TryFromBigIntError<()>| DecimalConversionError::Overflow {
            mantissa: decimal_mantissa.to_string(),
            err_msg: e.to_string(),
        },
    )?;
    Ok(RowValue::Decimal(actual_decimal_mantissa))
}

#[cfg(test)]
mod tests {
    use super::*;

    /*
    In this test, we are testing the error cases of the `convert_decimal_to_row_value` function.
    - InvalidValue: when the decimal string is invalid.
    - PrecisionExceeds: when the decimal string exceeds the precision.
    - ScaleExceeds: when the decimal string exceeds the scale.
    - Overflow: when the decimal string exceeds the i128 range.
    */

    #[test]
    fn test_convert_decimal_to_row_value_invalid_value() {
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

        let precision_exceeding_value = "123.4567";
        let err =
            convert_decimal_to_row_value(precision_exceeding_value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::PrecisionExceeds {
                value,
                expected_precision,
                actual_precision,
            } => {
                assert_eq!(value, precision_exceeding_value.to_string());
                assert_eq!(expected_precision, precision);
                assert_eq!(actual_precision, 7);
            }
            _ => panic!("Expected a PrecisionExceeds error, but got a different variant: {err:?}"),
        }

        let scale_exceeding_value = "123.4567";
        let precision = 8;
        let scale = 3;
        let err =
            convert_decimal_to_row_value(scale_exceeding_value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::ScaleExceeds {
                value,
                expected_scale,
                actual_scale,
            } => {
                assert_eq!(value, scale_exceeding_value.to_string());
                assert_eq!(expected_scale, scale);
                assert_eq!(actual_scale, 4);
            }
            _ => panic!("Expected a ScaleExceeds error, but got a different variant: {err:?}"),
        }

        let negative_scale_value = "123.45";
        let precision = 5;
        let scale = -2;
        let err = convert_decimal_to_row_value(negative_scale_value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::ScaleExceeds {
                value,
                expected_scale,
                actual_scale,
            } => {
                assert_eq!(value, negative_scale_value.to_string());
                assert_eq!(expected_scale, scale);
                assert_eq!(actual_scale, 2);
            }
            _ => panic!("Expected a ScaleExceeds error, but got a different variant: {err:?}"),
        }

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
    }

    #[test]
    fn test_convert_decimal_to_row_value_valid() {
        let valid_value_1 = "123.45";
        let precision = 5;
        let scale = 2;
        let result = convert_decimal_to_row_value(valid_value_1, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(12345));

        let valid_value_2 = "123.4";
        let precision = 5;
        let scale = 3;
        let result = convert_decimal_to_row_value(valid_value_2, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(123400));

        let valid_negative_value = "-123.4";
        let precision = 5;
        let scale = 3;
        let result = convert_decimal_to_row_value(valid_negative_value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(-123400));

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
