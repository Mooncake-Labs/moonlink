use std::error;
use std::fmt;
use std::panic::Location;
use std::sync::Arc;

/// Error status categories
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug)]
pub struct ErrorStruct {
    pub message: String,
    pub status: ErrorStatus,
    pub source: Option<Arc<anyhow::Error>>,
    pub location: Option<&'static Location<'static>>,
}

impl fmt::Display for ErrorStruct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message, self.status)?;

        if let Some(location) = &self.location {
            write!(
                f,
                " at {}:{}:{}",
                location.file(),
                location.line(),
                location.column()
            )?;
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
        Self {
            message,
            status,
            source: None,
            location: Some(Location::caller()),
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
}

impl error::Error for ErrorStruct {
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
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|arc| arc.as_ref().as_ref())
    }
}
