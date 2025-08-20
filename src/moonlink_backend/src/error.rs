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

    #[error("{0}")]
    MpscChannelSendError(ErrorStruct),
}

pub type Result<T> = result::Result<T, Error>;

impl From<PostgresSourceError> for Error {
    #[track_caller]
    fn from(source: PostgresSourceError) -> Self {
        // TODO: have finer error categorization for pg error
        Error::PostgresSource(ErrorStruct {
            message: format!("Postgres source error: {source}"),
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
            message: format!("Parse integer error: {source}"),
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
        let status = match &source {
            MoonlinkError::Arrow(es)
            | MoonlinkError::Io(es)
            | MoonlinkError::Parquet(es)
            | MoonlinkError::WatchChannelRecvError(es)
            | MoonlinkError::IcebergError(es)
            | MoonlinkError::OpenDal(es)
            | MoonlinkError::JoinError(es)
            | MoonlinkError::Json(es) => es.status,
        };
        Error::MoonlinkError(ErrorStruct {
            message: "Moonlink source error".to_string(),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<MoonlinkMetadataStoreError> for Error {
    #[track_caller]
    fn from(source: MoonlinkMetadataStoreError) -> Self {
        let status = match &source {
            #[cfg(feature = "metadata-postgres")]
            MoonlinkMetadataStoreError::TokioPostgres(es) => es.status,
            MoonlinkMetadataStoreError::PostgresRowCountError(es)
            | MoonlinkMetadataStoreError::Sqlx(es)
            | MoonlinkMetadataStoreError::SqliteRowCountError(es)
            | MoonlinkMetadataStoreError::MetadataStoreFailedPrecondition(es)
            | MoonlinkMetadataStoreError::SerdeJson(es)
            | MoonlinkMetadataStoreError::TableIdNotFound(es)
            | MoonlinkMetadataStoreError::ConfigFieldNotExist(es)
            | MoonlinkMetadataStoreError::Io(es) => es.status,
        };
        Error::MoonlinkMetadataStoreError(ErrorStruct {
            message: "Moonlink metadata store error".to_string(),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
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
            location: Some(Location::caller().to_string()),
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
            message: format!("JSON serialization/deserialization error: {source}"),
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
