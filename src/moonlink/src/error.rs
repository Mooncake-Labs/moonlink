use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use std::io;
use std::result;
use thiserror::Error;
use tokio::sync::watch;
/// Custom error type for moonlink
#[derive(Debug, Error)]
pub enum Error {
    #[error("Arrow error: {source}")]
    Arrow { source: ArrowError },

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),

    #[error("Transaction {0} not found")]
    TransactionNotFound(u32),

    #[error("Watch channel receiver error: {source}")]
    WatchChannelRecvError {
        #[source]
        source: watch::error::RecvError,
    },
}

pub type Result<T> = result::Result<T, Error>;

impl From<ArrowError> for Error {
    fn from(source: ArrowError) -> Self {
        Error::Arrow { source }
    }
}
