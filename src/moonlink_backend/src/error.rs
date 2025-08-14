use moonlink::Error as MoonlinkError;
use moonlink_connectors::Error as MoonlinkConnectorError;
use moonlink_connectors::PostgresSourceError;
use moonlink_error::{ErrorStatus, ErrorStruct};
use moonlink_metadata_store::error::Error as MoonlinkMetadataStoreError;
use std::num::ParseIntError;
use std::panic::Location;
use std::result;
use std::sync::Arc;
use thiserror::Error;

/// Custom error type for moonlink_backend
#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("{0}")]
    ParseIntError(ErrorStruct),

    #[error("{0}")]
    PostgresSource(ErrorStruct),

    #[error("{0}")]
    Io(ErrorStruct),

    #[error("{0}")]
    MoonlinkConnectorError(ErrorStruct),

    #[error("{0}")]
    MoonlinkError(ErrorStruct),

    #[error("{0}")]
    MoonlinkMetadataStoreError(ErrorStruct),

    #[error("Invalid argument: {0}")]
    InvalidArgumentError(String),

    #[error("{0}")]
    TokioWatchRecvError(ErrorStruct),
    #[error("{0}")]
    Json(ErrorStruct),
}

pub type Result<T> = result::Result<T, Error>;

impl From<PostgresSourceError> for Error {
    #[track_caller]
    fn from(source: PostgresSourceError) -> Self {
        Error::PostgresSource(ErrorStruct {
            message: format!("Postgres source error: {source}"),
            status: ErrorStatus::Temporary,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<ParseIntError> for Error {
    #[track_caller]
    fn from(source: ParseIntError) -> Self {
        Error::ParseIntError(ErrorStruct {
            message: format!("Parse integer error: {source}"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<MoonlinkConnectorError> for Error {
    #[track_caller]
    fn from(source: MoonlinkConnectorError) -> Self {
        Error::MoonlinkConnectorError(ErrorStruct {
            message: format!("Moonlink connector error: {source}"),
            status: ErrorStatus::Temporary,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<MoonlinkError> for Error {
    #[track_caller]
    fn from(source: MoonlinkError) -> Self {
        Error::MoonlinkError(ErrorStruct {
            message: format!("Moonlink error: {source}"),
            status: ErrorStatus::Temporary,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<MoonlinkMetadataStoreError> for Error {
    #[track_caller]
    fn from(source: MoonlinkMetadataStoreError) -> Self {
        Error::MoonlinkMetadataStoreError(ErrorStruct {
            message: format!("Moonlink metadata store error: {source}"),
            status: ErrorStatus::Temporary,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<std::io::Error> for Error {
    #[track_caller]
    fn from(source: std::io::Error) -> Self {
        let status = match source.kind() {
            std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::NetworkDown
            | std::io::ErrorKind::ResourceBusy
            | std::io::ErrorKind::QuotaExceeded => ErrorStatus::Temporary,

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

impl From<tokio::sync::watch::error::RecvError> for Error {
    #[track_caller]
    fn from(source: tokio::sync::watch::error::RecvError) -> Self {
        Error::TokioWatchRecvError(ErrorStruct {
            message: format!("Watch channel receive error: {source}"),
            status: ErrorStatus::Permanent,
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
