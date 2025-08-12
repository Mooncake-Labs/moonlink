use bigdecimal::BigDecimal;
use moonlink::row::RowValue;
use std::convert::TryInto;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum DecimalConversionError {
    #[error("Decimal normalization precision failed")]
    NormalizationPrecision,
    #[error("Decimal normalization scale failed")]
    NormalizationScale,
    #[error("Decimal precision exceeds the specified precision")]
    PrecisionMismatch,
    #[error("Decimal scale exceeds the specified scale")]
    ScaleMismatch,
    #[error("Decimal value is invalid")]
    InvalidValue,
    #[error("Decimal mantissa conversion to i128 failed")]
    Overflow,
}

pub fn convert_decimal_to_row_value(
    value: &str,
    precision: u8,
    scale: i8,
) -> Result<RowValue, DecimalConversionError> {
    let decimal = BigDecimal::from_str(value).map_err(|_| DecimalConversionError::InvalidValue)?;
    let (decimal_mantissa, decimal_scale) = decimal.as_bigint_and_exponent();
    let decimal_precision = decimal_mantissa.to_string().len();

    let decimal_precision_u8: u8 = decimal_precision
        .try_into()
        .map_err(|_| DecimalConversionError::NormalizationPrecision)?;
    let decimal_scale_u8: i8 = decimal_scale
        .try_into()
        .map_err(|_| DecimalConversionError::NormalizationScale)?;

    if decimal_precision_u8 != precision {
        return Err(DecimalConversionError::PrecisionMismatch);
    }

    if decimal_scale_u8 != scale {
        return Err(DecimalConversionError::ScaleMismatch);
    }

    let decimal_mantissa_i128: i128 = decimal_mantissa
        .try_into()
        .map_err(|_| DecimalConversionError::Overflow)?;
    Ok(RowValue::Decimal(decimal_mantissa_i128))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_decimal_to_row_value_invalid_precision() {
        let value = "123.456";
        let precision = 5;
        let scale = 3;
        let err = convert_decimal_to_row_value(value, precision, scale).unwrap_err();
        assert_eq!(err, DecimalConversionError::PrecisionMismatch);
    }

    #[test]
    fn test_convert_decimal_to_row_value_invalid_scale() {
        let value = "123.45";
        let precision = 5;
        let scale = -2;
        let err = convert_decimal_to_row_value(value, precision, scale).unwrap_err();
        assert_eq!(err, DecimalConversionError::ScaleMismatch);
    }

    #[test]
    fn test_convert_decimal_to_row_value_invalid_value() {
        let value = "123..45";
        let precision = 5;
        let scale = 2;
        let err = convert_decimal_to_row_value(value, precision, scale).unwrap_err();
        assert_eq!(err, DecimalConversionError::InvalidValue);
    }

    #[test]
    fn test_convert_decimal_to_row_value_valid() {
        let value = "123.45";
        let precision = 5;
        let scale = 2;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(result, RowValue::Decimal(12345));
    }

    #[test]
    fn test_convert_decimal_to_row_value_valid_large_scale() {
        let value = "123456789012345678901234567890123456.789";
        let precision = 39;
        let scale = 3;
        let result = convert_decimal_to_row_value(value, precision, scale).unwrap();
        assert_eq!(
            result,
            RowValue::Decimal(123456789012345678901234567890123456789)
        );
    }

    #[test]
    fn test_convert_decimal_to_row_value_invalid_large_scale() {
        let value = "1234567890123456789012345678901234567.789";
        let precision = 40;
        let scale = 3;
        let err = convert_decimal_to_row_value(value, precision, scale).unwrap_err();
        assert_eq!(err, DecimalConversionError::Overflow);
    }
}
