use arrow::error::ArrowError;
use iceberg::Error as IcebergError;
use parquet::errors::ParquetError;
use std::io;
use std::result;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

/// Custom error type for moonlink
#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Arrow error: {source}")]
    Arrow { source: Arc<ArrowError> },

    #[error("IO error: {source}")]
    Io { source: Arc<io::Error> },

    #[error("Parquet error: {source}")]
    Parquet { source: Arc<ParquetError> },

    #[error("Transaction {0} not found")]
    TransactionNotFound(u32),

    #[error("Watch channel receiver error: {source}")]
    WatchChannelRecvError {
        #[source]
        source: watch::error::RecvError,
    },

    #[error("Tokio join error: {0}. This typically occurs when a spawned task fails to complete successfully. Check the task's execution or panic status for more details.")]
    TokioJoinError(String),

    #[error("Iceberg error: {source}")]
    IcebergError { source: Arc<IcebergError> },

    #[error("Iceberg error: {0}")]
    IcebergMessage(String),

    #[error("OpenDAL error: {source}")]
    OpenDal { source: Arc<opendal::Error> },

    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("Join error: {source}")]
    JoinError { source: Arc<tokio::task::JoinError> },

    #[error("JSON serialization/deserialization error: {source}")]
    Json { source: Arc<serde_json::Error> },
}

pub type Result<T> = result::Result<T, Error>;

impl From<ArrowError> for Error {
    fn from(source: ArrowError) -> Self {
        Error::Arrow {
            source: Arc::new(source),
        }
    }
}

impl From<IcebergError> for Error {
    fn from(source: IcebergError) -> Self {
        Error::IcebergError {
            source: Arc::new(source),
        }
    }
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Error::Io {
            source: Arc::new(source),
        }
    }
}

impl From<opendal::Error> for Error {
    fn from(source: opendal::Error) -> Self {
        Error::OpenDal {
            source: Arc::new(source),
        }
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(source: tokio::task::JoinError) -> Self {
        Error::JoinError {
            source: Arc::new(source),
        }
    }
}

impl From<ParquetError> for Error {
    fn from(source: ParquetError) -> Self {
        Error::Parquet {
            source: Arc::new(source),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(source: serde_json::Error) -> Self {
        Error::Json {
            source: Arc::new(source),
        }
    }
}
