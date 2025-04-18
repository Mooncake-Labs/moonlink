use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use std::io;
use std::result;
use thiserror::Error;

/// Custom error type for moonlink
#[derive(Debug, Error)]
pub enum Error {
    #[error("Arrow error: {source}")]
    Arrow { source: ArrowError },

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),
}

pub type Result<T> = result::Result<T, Error>;

impl From<ArrowError> for Error {
    fn from(source: ArrowError) -> Self {
        Error::Arrow { source }
    }
}
