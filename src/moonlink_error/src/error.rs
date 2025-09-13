use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::panic::Location;
use std::sync::Arc;

/// Error status categories
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorStatus {
    /// Temporary errors that can be resolved by retrying (e.g., rate limits, timeouts)
    Temporary,
    /// Permanent errors that cannot be solved by retrying (e.g., not found, permission denied)
    Permanent,
}

impl fmt::Display for ErrorStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorStatus::Temporary => write!(f, "temporary"),
            ErrorStatus::Permanent => write!(f, "permanent"),
        }
    }
}

/// Custom error struct for moonlink
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorStruct {
    pub message: String,
    pub status: ErrorStatus,
    #[serde(
        serialize_with = "serialize_error_source",
        deserialize_with = "deserialize_error_source"
    )]
    pub source: Option<Arc<anyhow::Error>>,
    pub location: Option<String>,
}

fn serialize_error_source<S>(
    error: &Option<Arc<anyhow::Error>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = error.as_ref().map(|e| e.to_string());
    s.serialize(serializer)
}

fn deserialize_error_source<'de, D>(deserializer: D) -> Result<Option<Arc<anyhow::Error>>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    Ok(s.map(|msg| Arc::new(anyhow::anyhow!(msg))))
}

impl fmt::Display for ErrorStruct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message, self.status)?;

        if let Some(location) = &self.location {
            write!(f, " at {location}")?;
        }

        if let Some(source) = &self.source {
            write!(f, ", source: {source}")?;
        }

        Ok(())
    }
}

impl ErrorStruct {
    /// Creates a new ErrorStruct with the provided location.
    #[track_caller]
    pub fn new(message: String, status: ErrorStatus) -> Self {
        let location = Location::caller();
        Self {
            message,
            status,
            source: None,
            location: Some(format!(
                "{}:{}:{}",
                location.file(),
                location.line(),
                location.column()
            )),
        }
    }

    /// Sets the source error for this error struct.
    ///
    /// # Panics
    ///
    /// Panics if the source error has already been set.
    pub fn with_source(mut self, src: impl Into<anyhow::Error>) -> Self {
        assert!(self.source.is_none(), "the source error has been set");
        self.source = Some(Arc::new(src.into()));
        self
    }

    /// Returns the underlying source error for accessing structured information.
    ///
    /// # Example
    /// ```ignore
    /// if let Some(source) = error_struct.source() {
    ///     if let Some(io_err) = source.downcast_ref::<std::io::Error>() {
    ///         println!("IO error kind: {:?}", io_err.kind());
    ///     }
    /// }
    /// ```
    pub fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|arc| arc.as_ref().as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode;
    use regex::Regex;
    use std::io;

    #[test]
    fn test_error_struct_without_source() {
        let error = ErrorStruct {
            message: "Test error".to_string(),
            status: ErrorStatus::Temporary,
            source: None,
            location: None,
        };
        assert_eq!(error.to_string(), "Test error (temporary)");
        assert!(error.source.is_none());
    }

    #[test]
    fn test_error_struct_with_source() {
        let io_error = io::Error::new(io::ErrorKind::NotFound, "File not found");
        let error = ErrorStruct {
            message: "Test error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(io_error.into())),
            location: None,
        };
        assert_eq!(
            error.to_string(),
            "Test error (permanent), source: File not found"
        );
        assert!(error.source.is_some());

        // Test accessing structured error information
        let source = error.source().unwrap();
        let io_err = source.downcast_ref::<io::Error>().unwrap();
        assert_eq!(io_err.kind(), io::ErrorKind::NotFound);
        assert_eq!(io_err.to_string(), "File not found");
    }

    #[test]
    fn test_error_struct_new_with_location() {
        // ErrorStruct::new will automatically capture the location where the error is raised
        let error = ErrorStruct::new("Test error".to_string(), ErrorStatus::Temporary);
        assert!(error.location.is_some());
        let location_str = error.to_string();

        assert!(location_str.contains("Test error (temporary) at"));
        assert!(location_str.contains("error.rs:"));

        // Check the location matches the pattern error.rs:number:number
        let re_pattern = Regex::new(r"error\.rs:\d+:\d+").unwrap();
        assert!(re_pattern.is_match(&location_str));
    }

    /// Test util function to check error serde via roundtrip.
    fn roundtrip_check<F, T>(serialize: F, deserialize: T)
    where
        F: Fn(&ErrorStruct) -> Vec<u8>,
        T: Fn(&[u8]) -> ErrorStruct,
    {
        use std::io;

        // Construct the error with a source
        let io_error = io::Error::new(io::ErrorKind::NotFound, "Test file not found");
        let error = ErrorStruct::new("IO operation failed".to_string(), ErrorStatus::Permanent)
            .with_source(io_error);

        // Serialize and deserialize
        let bytes = serialize(&error);
        let deserialized = deserialize(&bytes);

        // Common assertions
        assert_eq!(deserialized.message, error.message);
        assert_eq!(deserialized.status, error.status);
        assert!(deserialized.source.is_some());
        let source = deserialized.source.unwrap();
        assert!(source.to_string().contains("Test file not found"));
    }

    #[test]
    fn test_error_struct_with_source_json_serde() {
        roundtrip_check(
            |err| serde_json::to_vec(err).unwrap(),
            |bytes| serde_json::from_slice::<ErrorStruct>(bytes).unwrap(),
        );
    }

    #[test]
    fn test_error_struct_with_source_bincode_serde() {
        roundtrip_check(
            |err| {
                let config = bincode::config::standard();
                bincode::serde::encode_to_vec(Err::<(), ErrorStruct>(err.clone()), config).unwrap()
            },
            |bytes| {
                let config = bincode::config::standard();
                let decoded: Result<(), ErrorStruct> =
                    bincode::serde::decode_from_slice(bytes, config).unwrap().0;
                decoded.unwrap_err()
            },
        );
    }
}
