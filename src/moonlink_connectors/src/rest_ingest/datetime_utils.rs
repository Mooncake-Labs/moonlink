use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use moonlink::row::RowValue;

/// Parse a date string in YYYY-MM-DD format to Date32 (days since epoch)
pub fn parse_date(date_str: &str) -> Result<RowValue, String> {
    const ARROW_EPOCH: NaiveDate = match NaiveDate::from_ymd_opt(1970, 1, 1) {
        Some(date) => date,
        None => panic!("Failed to create epoch date"),
    };

    NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .map(|date| {
            let days_since_epoch = date.signed_duration_since(ARROW_EPOCH).num_days() as i32;
            RowValue::Int32(days_since_epoch)
        })
        .map_err(|e| format!("Invalid date format: {e}"))
}

/// Parse a time string in HH:MM:SS[.fraction] format to Time64 (microseconds since midnight)
pub fn parse_time(time_str: &str) -> Result<RowValue, String> {
    // Try parsing with fractional seconds first, then without
    let time = NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(time_str, "%H:%M:%S"))
        .map_err(|e| format!("Invalid time format: {e}"))?;

    // Convert to microseconds since midnight
    let duration = time.signed_duration_since(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
    let microseconds = duration
        .num_microseconds()
        .ok_or_else(|| "Time value too large".to_string())?;

    Ok(RowValue::Int64(microseconds))
}

/// Parse an RFC3339/ISO8601 timestamp and normalize to UTC
pub fn parse_timestamp(timestamp_str: &str) -> Result<RowValue, String> {
    // Parse RFC3339/ISO8601 timestamp
    let dt = DateTime::parse_from_rfc3339(timestamp_str)
        .map_err(|e| format!("Invalid timestamp format: {e}"))?;

    // Convert to UTC
    let utc_dt = dt.with_timezone(&Utc);
    let timestamp_micros = utc_dt.timestamp_micros();

    Ok(RowValue::Int64(timestamp_micros))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_date() {
        // Valid date
        let result = parse_date("2024-03-15").unwrap();
        let expected_days = NaiveDate::from_ymd_opt(2024, 3, 15)
            .unwrap()
            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32;
        assert_eq!(result, RowValue::Int32(expected_days));

        // Invalid date
        assert!(parse_date("2024/03/15").is_err());
    }

    #[test]
    fn test_parse_time() {
        // Time without fractional seconds
        let result = parse_time("14:30:45").unwrap();
        assert_eq!(result, RowValue::Int64(52245000000));

        // Time with fractional seconds
        let result = parse_time("09:15:30.123456").unwrap();
        assert_eq!(result, RowValue::Int64(33330123456));

        // Invalid time
        assert!(parse_time("25:00:00").is_err());
    }

    #[test]
    fn test_parse_timestamp() {
        // UTC timestamp
        let result = parse_timestamp("2024-03-15T10:30:45.123Z").unwrap();
        use chrono::TimeZone;
        let expected = Utc
            .with_ymd_and_hms(2024, 3, 15, 10, 30, 45)
            .unwrap()
            .timestamp_micros()
            + 123000;
        assert_eq!(result, RowValue::Int64(expected));

        // Timestamp with timezone offset
        let result = parse_timestamp("2024-03-15T10:30:45+05:00").unwrap();
        let expected = Utc
            .with_ymd_and_hms(2024, 3, 15, 5, 30, 45)
            .unwrap()
            .timestamp_micros();
        assert_eq!(result, RowValue::Int64(expected));

        // Invalid timestamp
        assert!(parse_timestamp("2024-03-15 10:30:45").is_err());
    }
}
