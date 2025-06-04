use moonlink::Error as MoonlinkError;
use moonlink_connectors::Error as MoonlinkConnectorError;
use moonlink_connectors::PostgresSourceError;
use std::result;
use thiserror::Error;

/// Custom error type for moonlink_backend
#[derive(Debug, Error)]
pub enum Error {
    #[error("Postgres source error: {0}")]
    PostgresSource(#[from] PostgresSourceError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Moonlink connector error: {source}")]
    MoonlinkConnectorError { source: MoonlinkConnectorError },
    #[error("Moonlink error: {source}")]
    MoonlinkError { source: MoonlinkError },
}

pub type Result<T> = result::Result<T, Error>;

impl From<MoonlinkConnectorError> for Error {
    fn from(source: MoonlinkConnectorError) -> Self {
        Error::MoonlinkConnectorError { source }
    }
}

impl From<MoonlinkError> for Error {
    fn from(source: MoonlinkError) -> Self {
        Error::MoonlinkError { source }
    }
}
