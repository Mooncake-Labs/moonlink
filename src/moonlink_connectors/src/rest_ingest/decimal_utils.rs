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
    PrecisionMismatch {
        value: String,
        expected_precision: u8,
        actual_precision: u8,
    },
    #[error("Decimal scale exceeds the specified scale (value: {value}, expected ≤ {expected_scale}, actual {actual_scale})")]
    ScaleMismatch {
        value: String,
        expected_scale: i8,
        actual_scale: i8,
    },
    #[error("Decimal value is invalid: {value})")]
    InvalidValue { value: String },
    #[error("Decimal mantissa overflow: {mantissa}, error: {error}")]
    Overflow { mantissa: String, error: String },
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
    let (decimal_mantissa, decimal_scale) = decimal.as_bigint_and_exponent();
    let decimal_precision = decimal_mantissa.to_string().len();

    let decimal_precision_u8: u8 = decimal_precision.try_into().map_err(|_| {
        DecimalConversionError::NormalizationPrecision {
            value: value.to_string(),
            parsed_precision: decimal_precision,
        }
    })?;
    let decimal_scale_u8: i8 =
        decimal_scale
            .try_into()
            .map_err(|_| DecimalConversionError::NormalizationScale {
                value: value.to_string(),
                parsed_scale: decimal_scale,
            })?;

    if decimal_precision_u8 > precision {
        return Err(DecimalConversionError::PrecisionMismatch {
            value: value.to_string(),
            expected_precision: precision,
            actual_precision: decimal_precision_u8,
        });
    }

    if decimal_scale_u8 > scale {
        return Err(DecimalConversionError::ScaleMismatch {
            value: value.to_string(),
            expected_scale: scale,
            actual_scale: decimal_scale_u8,
        });
    }

    let decimal_mantissa_i128: i128 = (&decimal_mantissa).try_into().map_err(
        |e: bigdecimal::num_bigint::TryFromBigIntError<()>| DecimalConversionError::Overflow {
            mantissa: decimal_mantissa.to_string(),
            error: e.to_string(),
        },
    )?;
    Ok(RowValue::Decimal(decimal_mantissa_i128))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            DecimalConversionError::PrecisionMismatch {
                value,
                expected_precision,
                actual_precision,
            } => {
                assert_eq!(value, precision_exceeding_value.to_string());
                assert_eq!(expected_precision, precision);
                assert_eq!(actual_precision, 7);
            }
            _ => panic!("Expected a PrecisionMismatch error, but got a different variant: {err:?}"),
        }

        let scale_exceeding_value = "123.4567";
        let precision = 8;
        let scale = 3;
        let err =
            convert_decimal_to_row_value(scale_exceeding_value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::ScaleMismatch {
                value,
                expected_scale,
                actual_scale,
            } => {
                assert_eq!(value, scale_exceeding_value.to_string());
                assert_eq!(expected_scale, scale);
                assert_eq!(actual_scale, 4);
            }
            _ => panic!("Expected a ScaleMismatch error, but got a different variant: {err:?}"),
        }

        let negative_scale_value = "123.45";
        let precision = 5;
        let scale = -2;
        let err = convert_decimal_to_row_value(negative_scale_value, precision, scale).unwrap_err();
        match err {
            DecimalConversionError::ScaleMismatch {
                value,
                expected_scale,
                actual_scale,
            } => {
                assert_eq!(value, negative_scale_value.to_string());
                assert_eq!(expected_scale, scale);
                assert_eq!(actual_scale, 2);
            }
            _ => panic!("Expected a ScaleMismatch error, but got a different variant: {err:?}"),
        }

        let overflow_value = "1234567890123456789012345678901234567.789";
        let precision = 40;
        let scale = 3;
        let err = convert_decimal_to_row_value(overflow_value, precision, scale).unwrap_err();

        match err {
            DecimalConversionError::Overflow { mantissa, error } => {
                assert_eq!(mantissa, "1234567890123456789012345678901234567789");
                assert!(error.contains("out of range")); // Ensure the error message contains means it is out of range
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
        assert_eq!(result, RowValue::Decimal(1234));

        let large_scale_value = "123456789012345678901234567890123456.789";
        let precision = 39;
        let scale = 3;
        let result = convert_decimal_to_row_value(large_scale_value, precision, scale).unwrap();
        assert_eq!(
            result,
            RowValue::Decimal(123456789012345678901234567890123456789)
        );
    }
}
