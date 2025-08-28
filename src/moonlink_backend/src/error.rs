use moonlink::Error as MoonlinkError;
use moonlink_connectors::Error as MoonlinkConnectorError;
use moonlink_connectors::PostgresSourceError;
use moonlink_error::io_error_utils;
use moonlink_error::{ErrorStatus, ErrorStruct};
use moonlink_metadata_store::error::Error as MoonlinkMetadataStoreError;
use serde::{Deserialize, Serialize};
use std::num::ParseIntError;
use std::panic::Location;
use std::result;
use std::sync::Arc;
use thiserror::Error;

/// Custom error type for moonlink_backend
#[derive(Clone, Debug, Error, Deserialize, Serialize)]
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

    #[error("{0}")]
    InvalidArgumentError(ErrorStruct),

    #[error("{0}")]
    DataCorruptionError(ErrorStruct),

    #[error("{0}")]
    TokioWatchRecvError(ErrorStruct),

    #[error("{0}")]
    Json(ErrorStruct),

    #[error("{0}")]
    MpscChannelSendError(ErrorStruct),

    #[error("{0}")]
    InvalidConfig(ErrorStruct),
}

pub type Result<T> = result::Result<T, Error>;

impl Error {
    #[track_caller]
    pub fn invalid_argument(message: String) -> Self {
        Self::InvalidArgumentError(ErrorStruct::new(message, ErrorStatus::Permanent))
    }
    #[track_caller]
    pub fn data_corruption(message: String) -> Self {
        Self::DataCorruptionError(ErrorStruct::new(message, ErrorStatus::Permanent))
    }
    #[track_caller]
    pub fn invalid_config(message: String) -> Self {
        Self::InvalidConfig(ErrorStruct::new(message, ErrorStatus::Permanent))
    }

    pub fn get_status(&self) -> ErrorStatus {
        match self {
            Error::ParseIntError(es) => es.status,
            Error::PostgresSource(es) => es.status,
            Error::Io(es) => es.status,
            Error::MoonlinkConnectorError(es) => es.status,
            Error::MoonlinkError(es) => es.status,
            Error::MoonlinkMetadataStoreError(es) => es.status,
            Error::InvalidArgumentError(es) => es.status,
            Error::TokioWatchRecvError(es) => es.status,
            Error::Json(es) => es.status,
            Error::MpscChannelSendError(es) => es.status,
            Error::DataCorruptionError(es) => es.status,
            Error::InvalidConfig(es) => es.status,
        }
    }
}

impl From<PostgresSourceError> for Error {
    #[track_caller]
    fn from(source: PostgresSourceError) -> Self {
        // TODO: have finer error categorization for pg error
        Error::PostgresSource(ErrorStruct {
            message: "Postgres error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<ParseIntError> for Error {
    #[track_caller]
    fn from(source: ParseIntError) -> Self {
        Error::ParseIntError(ErrorStruct {
            message: "Parse integer error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<MoonlinkConnectorError> for Error {
    #[track_caller]
    fn from(source: MoonlinkConnectorError) -> Self {
        let status = match &source {
            MoonlinkConnectorError::PostgresSourceError(es)
            | MoonlinkConnectorError::TokioPostgres(es)
            | MoonlinkConnectorError::CdcStream(es)
            | MoonlinkConnectorError::TableCopyStream(es)
            | MoonlinkConnectorError::MoonlinkError(es)
            | MoonlinkConnectorError::Io(es)
            | MoonlinkConnectorError::MpscChannelSendError(es)
            | MoonlinkConnectorError::RestSource(es)
            | MoonlinkConnectorError::RestPayloadConversion(es)
            | MoonlinkConnectorError::ParquetError(es) => es.status,
            _ => ErrorStatus::Permanent,
        };
        Error::MoonlinkConnectorError(ErrorStruct {
            message: "Moonlink connector error".to_string(),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<MoonlinkError> for Error {
    #[track_caller]
    fn from(source: MoonlinkError) -> Self {
        Error::MoonlinkError(ErrorStruct {
            message: "Moonlink source error".to_string(),
            status: source.get_status(),
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<MoonlinkMetadataStoreError> for Error {
    #[track_caller]
    fn from(source: MoonlinkMetadataStoreError) -> Self {
        Error::MoonlinkMetadataStoreError(ErrorStruct {
            message: "Moonlink metadata store error".to_string(),
            status: source.get_status(),
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<std::io::Error> for Error {
    #[track_caller]
    fn from(source: std::io::Error) -> Self {
        Error::Io(ErrorStruct {
            message: "IO error".to_string(),
            status: io_error_utils::get_io_error_status(&source),
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<tokio::sync::watch::error::RecvError> for Error {
    #[track_caller]
    fn from(source: tokio::sync::watch::error::RecvError) -> Self {
        Error::TokioWatchRecvError(ErrorStruct {
            message: "Watch channel receive error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
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
            message: "JSON serialization/deserialization error".to_string(),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl<T: Send + Sync + 'static> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    #[track_caller]
    fn from(source: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Error::MpscChannelSendError(ErrorStruct {
            message: "mpsc channel send error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}
