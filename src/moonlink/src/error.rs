use arrow::error::ArrowError;
use iceberg::Error as IcebergError;
use parquet::errors::ParquetError;
use std::fmt;
use std::io;
use std::result;
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
    // TODO: take out source now to deal with other field first, should add it back
}

impl fmt::Display for ErrorStruct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message, self.status)
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
        })
    }
}

impl From<IcebergError> for Error {
    fn from(source: IcebergError) -> Self {
        Error::IcebergError(ErrorStruct {
            message: format!("Iceberg error: {source}"),
            status: ErrorStatus::Permanent,
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
        })
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(source: tokio::task::JoinError) -> Self {
        Error::JoinError(ErrorStruct {
            message: format!("Join error: {source}"),
            status: ErrorStatus::Permanent,
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
        })
    }
}
