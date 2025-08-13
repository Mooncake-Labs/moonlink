use arrow::error::ArrowError;
use iceberg::Error as IcebergError;
use moonlink_error::{ErrorStatus, ErrorStruct};
use parquet::errors::ParquetError;
use std::io;
use std::panic::Location;
use std::result;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

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
    #[track_caller]
    fn from(source: watch::error::RecvError) -> Self {
        Error::WatchChannelRecvError(ErrorStruct {
            message: format!("Watch channel receiver error: {source}"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<ArrowError> for Error {
    #[track_caller]
    fn from(source: ArrowError) -> Self {
        let status = match source {
            ArrowError::IoError(_, _) => ErrorStatus::Temporary,

            // All other errors are regard as permanent
            _ => ErrorStatus::Permanent,
        };

        Error::Arrow(ErrorStruct {
            message: format!("Arrow error: {source}"),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<IcebergError> for Error {
    #[track_caller]
    fn from(source: IcebergError) -> Self {
        let status = match source.kind() {
            iceberg::ErrorKind::CatalogCommitConflicts | iceberg::ErrorKind::Unexpected => {
                ErrorStatus::Temporary
            }

            // All other errors are permanent
            _ => ErrorStatus::Permanent,
        };

        Error::IcebergError(ErrorStruct {
            message: format!("Iceberg error: {source}"),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<io::Error> for Error {
    #[track_caller]
    fn from(source: io::Error) -> Self {
        let status = match source.kind() {
            io::ErrorKind::TimedOut
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NetworkDown
            | io::ErrorKind::ResourceBusy
            | io::ErrorKind::QuotaExceeded => ErrorStatus::Temporary,

            // All other errors are permanent
            _ => ErrorStatus::Permanent,
        };

        Error::Io(ErrorStruct {
            message: format!("IO error: {source}"),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<opendal::Error> for Error {
    #[track_caller]
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
            location: Some(Location::caller()),
        })
    }
}

impl From<tokio::task::JoinError> for Error {
    #[track_caller]
    fn from(source: tokio::task::JoinError) -> Self {
        Error::JoinError(ErrorStruct {
            message: format!("Join error: {source}"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<ParquetError> for Error {
    #[track_caller]
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
            location: Some(Location::caller()),
        })
    }
}

impl From<serde_json::Error> for Error {
    #[track_caller]
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
            location: Some(Location::caller()),
        })
    }
}
