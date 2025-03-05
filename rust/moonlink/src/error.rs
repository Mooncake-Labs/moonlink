use arrow::error::ArrowError;
use std::result;
use thiserror::Error;

/// Custom error type for moonlink
#[derive(Debug, Error)]
pub enum Error {
    #[error("Arrow error: {source}")]
    Arrow { source: ArrowError },
}

pub type Result<T> = result::Result<T, Error>;

impl From<ArrowError> for Error {
    fn from(source: ArrowError) -> Self {
        Error::Arrow { source }
    }
}
