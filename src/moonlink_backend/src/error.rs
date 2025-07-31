use moonlink::Error as MoonlinkError;
use moonlink_connectors::Error as MoonlinkConnectorError;
use moonlink_connectors::PostgresSourceError;
use moonlink_metadata_store::error::Error as MoonlinkMetadataStoreError;
use std::num::ParseIntError;
use std::result;
use std::sync::Arc;
use thiserror::Error;

/// Custom error type for moonlink_backend
#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Parse integer error: {source}")]
    ParseIntError { source: ParseIntError },
    #[error("Postgres source error: {source}")]
    PostgresSource { source: Arc<PostgresSourceError> },
    #[error("IO error: {source}")]
    Io { source: Arc<std::io::Error> },
    #[error("Moonlink connector error: {source}")]
    MoonlinkConnectorError { source: MoonlinkConnectorError },
    #[error("Moonlink error: {source}")]
    MoonlinkError { source: MoonlinkError },
    #[error("Moonlink metadata error: {source}")]
    MoonlinkMetadataStoreError { source: MoonlinkMetadataStoreError },
    #[error("Invalid argument: {0}")]
    InvalidArgumentError(String),
    #[error("Watch channel receive error: {source}")]
    TokioWatchRecvError {
        source: tokio::sync::watch::error::RecvError,
    },
    #[error("JSON serialization/deserialization error: {source}")]
    Json { source: Arc<serde_json::Error> },
}

pub type Result<T> = result::Result<T, Error>;

impl From<PostgresSourceError> for Error {
    fn from(source: PostgresSourceError) -> Self {
        Error::PostgresSource {
            source: Arc::new(source),
        }
    }
}

impl From<ParseIntError> for Error {
    fn from(source: ParseIntError) -> Self {
        Error::ParseIntError { source }
    }
}

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

impl From<MoonlinkMetadataStoreError> for Error {
    fn from(source: MoonlinkMetadataStoreError) -> Self {
        Error::MoonlinkMetadataStoreError { source }
    }
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Error::Io {
            source: Arc::new(source),
        }
    }
}

impl From<tokio::sync::watch::error::RecvError> for Error {
    fn from(source: tokio::sync::watch::error::RecvError) -> Self {
        Error::TokioWatchRecvError { source }
    }
}

impl From<serde_json::Error> for Error {
    fn from(source: serde_json::Error) -> Self {
        Error::Json {
            source: Arc::new(source),
        }
    }
}
