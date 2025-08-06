use arrow::error::ArrowError;
use iceberg::Error as IcebergError;
use parquet::errors::ParquetError;
use std::fmt;
use std::io;
use std::result;
use thiserror::Error;
use tokio::sync::watch;

/// Error status indicating whether an error is retryable
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorStatus {
    Permanent,
    Temporary,
    Persistent,
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
        write!(f, "{}", self.message)
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
        Error::Arrow(ErrorStruct {
            message: format!("Arrow error: {source}"),
            status: ErrorStatus::Permanent,
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
        Error::Io(ErrorStruct {
            message: format!("IO error: {source}"),
            status: ErrorStatus::Permanent,
        })
    }
}

impl From<opendal::Error> for Error {
    fn from(source: opendal::Error) -> Self {
        Error::OpenDal(ErrorStruct {
            message: format!("OpenDAL error: {source}"),
            status: ErrorStatus::Permanent,
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
        Error::Parquet(ErrorStruct {
            message: format!("Parquet error: {source}"),
            status: ErrorStatus::Permanent,
        })
    }
}

impl From<serde_json::Error> for Error {
    fn from(source: serde_json::Error) -> Self {
        Error::Parquet(ErrorStruct {
            message: format!("JSON serialization/deserialization error: {source}"),
            status: ErrorStatus::Permanent,
        })
    }
}
