use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz;
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

    const MIDNIGHT: NaiveTime = match NaiveTime::from_hms_opt(0, 0, 0) {
        Some(time) => time,
        None => panic!("Failed to create midnight time"),
    };

    // Convert to microseconds since midnight
    let duration = time.signed_duration_since(MIDNIGHT);
    let microseconds = duration
        .num_microseconds()
        .ok_or_else(|| format!("Time value too large: {duration:?}"))?;

    Ok(RowValue::Int64(microseconds))
}

/// Parse an RFC3339/ISO8601 timestamp and normalize to UTC
/// Follows the same behavior as pg -> moonlink: canonicalize to UTC timezone
pub fn parse_timestamp(timestamp_str: &str) -> Result<RowValue, String> {
    parse_timestamp_with_timezone(timestamp_str, /*schema_timezone=*/ None)
}

/// Parse an RFC3339/ISO8601 timestamp with optional schema timezone and normalize to UTC
/// Follows the same behavior as pg -> moonlink: canonicalize to UTC timezone
///
/// There're two things worth noticing:
/// - If [`timestamp_str`] contains timezone information, [`schema_timezone`] will be overridden and ignored.
/// - If the timestamp/timezone combination represents multiple timestamp possibilities (i.e., due to DST change), the earliest timestamp will be returned.
pub fn parse_timestamp_with_timezone(
    timestamp_str: &str,
    schema_timezone: Option<&str>,
) -> Result<RowValue, String> {
    // First try parsing as RFC3339/ISO8601 (with timezone info)
    if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp_str) {
        // Convert to UTC
        let utc_dt = dt.with_timezone(&Utc);
        let timestamp_micros = utc_dt.timestamp_micros();
        return Ok(RowValue::Int64(timestamp_micros));
    }

    // If that fails, try parsing as naive datetime
    let naive_dt = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%.f"))
        .map_err(|e| format!("Invalid timestamp format: {e}"))?;

    // If schema specifies a timezone, interpret the naive datetime in that timezone
    if let Some(tz_str) = schema_timezone {
        if let Ok(tz) = tz_str.parse::<chrono_tz::Tz>() {
            let local_dt = tz
                .from_local_datetime(&naive_dt)
                .earliest()
                .ok_or_else(|| {
                    format!("Non-existent timestamp {timestamp_str} in timezone {tz_str}.")
                })?;
            let utc_dt = local_dt.with_timezone(&Utc);
            let timestamp_micros = utc_dt.timestamp_micros();
            return Ok(RowValue::Int64(timestamp_micros));
        }
    }

    // Default behavior: treat naive datetime as UTC
    // This matches pg_replicate behavior to use microseconds as value.
    let timestamp_micros = naive_dt.and_utc().timestamp_micros();
    Ok(RowValue::Int64(timestamp_micros))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_date() {
        // Valid date (2024-03-15 is 19797 days since epoch)
        let result = parse_date("2024-03-15").unwrap();
        assert_eq!(result, RowValue::Int32(19797));

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

    /// Testing scenario: parse timestamp without timezone.
    #[test]
    fn test_parse_timestamp() {
        use chrono::TimeZone;

        // UTC timestamp
        let result = parse_timestamp("2024-03-15T10:30:45.123Z").unwrap();
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

        // Timestamp without timezone (treated as UTC)
        let result = parse_timestamp("2024-03-15T10:30:45").unwrap();
        let expected = Utc
            .with_ymd_and_hms(2024, 3, 15, 10, 30, 45)
            .unwrap()
            .timestamp_micros();
        assert_eq!(result, RowValue::Int64(expected));

        // Timestamp without timezone with fractional seconds
        let result = parse_timestamp("2024-03-15T10:30:45.123456").unwrap();
        let expected = Utc
            .with_ymd_and_hms(2024, 3, 15, 10, 30, 45)
            .unwrap()
            .timestamp_micros()
            + 123456;
        assert_eq!(result, RowValue::Int64(expected));

        // Invalid timestamp
        assert!(parse_timestamp("2024-03-15 10:30:45").is_err());
    }

    #[test]
    fn test_parse_timestamp_with_timezone() {
        use chrono::TimeZone;

        // Unique timestamp.
        let utc_timezone = "UTC";
        let result =
            parse_timestamp_with_timezone("2024-03-15T10:30:45.123", Some(utc_timezone)).unwrap();
        let expected = Utc
            .with_ymd_and_hms(2024, 3, 15, 10, 30, 45)
            .unwrap()
            .timestamp_micros()
            + 123000;
        assert_eq!(result, RowValue::Int64(expected));

        // Ambiguous timestamp due to DST (Daylight Saving Time) change.
        let est_timezone = "America/New_York";
        let result =
            parse_timestamp_with_timezone("2024-11-03T01:30:00", Some(est_timezone)).unwrap();
        let expected = Utc
            .with_ymd_and_hms(2024, 11, 3, 5, 30, 00)
            .unwrap()
            .timestamp_micros();
        assert_eq!(result, RowValue::Int64(expected));

        // Non-existent timestamp due to DST (Daylight Saving Time) change.
        let est_timezone = "America/New_York";
        let result = parse_timestamp_with_timezone("2024-03-10T02:30:00", Some(est_timezone));
        assert!(result.is_err());
    }
}
