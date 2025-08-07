use arrow::error::ArrowError;
use iceberg::Error as IcebergError;
use parquet::errors::ParquetError;
use std::error;
use std::fmt;
use std::io;
use std::result;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

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
}

impl fmt::Display for ErrorStruct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message, self.status)?;

        if let Some(source) = &self.source {
            write!(f, ", source: {source}")?;
        }

        Ok(())
    }
}

impl ErrorStruct {
    /// Sets the source error for this error struct.
    ///
    /// # Panics
    ///
    /// Panics if the source error has already been set.
    pub fn with_source(mut self, src: impl Into<anyhow::Error>) -> Self {
        debug_assert!(self.source.is_none(), "the source error has been set");
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

/// Custom error type for moonlink
#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Arrow(ErrorStruct),

    #[error("{0}")]
    Io(ErrorStruct),

    #[error("{0}")]
    Parquet(ErrorStruct),

    #[error("Transaction {0} not found")]
    TransactionNotFound(u32),

    #[error("{0}")]
    WatchChannelRecvError(ErrorStruct),

    #[error("Tokio join error: {0}. This typically occurs when a spawned task fails to complete successfully. Check the task's execution or panic status for more details.")]
    TokioJoinError(String),

    #[error("{0}")]
    IcebergError(ErrorStruct),

    #[error("Iceberg error: {0}")]
    IcebergMessage(String),

    #[error("{0}")]
    OpenDal(ErrorStruct),

    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("{0}")]
    JoinError(ErrorStruct),

    #[error("{0}")]
    Json(ErrorStruct),
}

pub type Result<T> = result::Result<T, Error>;

impl From<watch::error::RecvError> for Error {
    fn from(source: watch::error::RecvError) -> Self {
        Error::WatchChannelRecvError(ErrorStruct {
            message: format!("Watch channel receiver error: {source}"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
        })
    }
}

impl From<ArrowError> for Error {
    fn from(source: ArrowError) -> Self {
        let status = match source {
            ArrowError::MemoryError(_) | ArrowError::IoError(_, _) => ErrorStatus::Temporary,

            // All other errors are regard as permanent
            _ => ErrorStatus::Permanent,
        };

        Error::Arrow(ErrorStruct {
            message: format!("Arrow error: {source}"),
            status,
            source: Some(Arc::new(source.into())),
        })
    }
}

impl From<IcebergError> for Error {
    fn from(source: IcebergError) -> Self {
        Error::IcebergError(ErrorStruct {
            message: format!("Iceberg error: {source}"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
        })
    }
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        let status = match source.kind() {
            io::ErrorKind::TimedOut
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset => ErrorStatus::Temporary,

            // All other errors are permanent
            _ => ErrorStatus::Permanent,
        };

        Error::Io(ErrorStruct {
            message: format!("IO error: {source}"),
            status,
            source: Some(Arc::new(source.into())),
        })
    }
}

impl From<opendal::Error> for Error {
    fn from(source: opendal::Error) -> Self {
        let status = match source.kind() {
            opendal::ErrorKind::RateLimited | opendal::ErrorKind::Unexpected => {
                ErrorStatus::Temporary
            }

            // All other errors are permanent
            _ => ErrorStatus::Permanent,
        };

        Error::OpenDal(ErrorStruct {
            message: format!("OpenDAL error: {source}"),
            status,
            source: Some(Arc::new(source.into())),
        })
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(source: tokio::task::JoinError) -> Self {
        Error::JoinError(ErrorStruct {
            message: format!("Join error: {source}"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
        })
    }
}

impl From<ParquetError> for Error {
    fn from(source: ParquetError) -> Self {
        let status = match source {
            ParquetError::EOF(_) | ParquetError::NeedMoreData(_) => ErrorStatus::Temporary,

            // All other errors are permanent
            _ => ErrorStatus::Permanent,
        };

        Error::Parquet(ErrorStruct {
            message: format!("Parquet error: {source}"),
            status,
            source: Some(Arc::new(source.into())),
        })
    }
}

impl From<serde_json::Error> for Error {
    fn from(source: serde_json::Error) -> Self {
        let status = match source.classify() {
            serde_json::error::Category::Io => ErrorStatus::Temporary,

            // All other errors are permanent - data format/syntax issues
            _ => ErrorStatus::Permanent,
        };

        Error::Json(ErrorStruct {
            message: format!("JSON serialization/deserialization error: {source}"),
            status,
            source: Some(Arc::new(source.into())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::io;

    #[test]
    fn test_error_struct_without_source() {
        let error = ErrorStruct {
            message: "Test error".to_string(),
            status: ErrorStatus::Temporary,
            source: None,
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
}
