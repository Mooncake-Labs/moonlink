use arrow::error::ArrowError;
use iceberg::Error as IcebergError;
use moonlink_error::io_error_utils::get_io_error_status;
use moonlink_error::{ErrorStatus, ErrorStruct};
use parquet::errors::ParquetError;
use serde::{Deserialize, Serialize};
use std::io;
use std::result;
use thiserror::Error;
use tokio::sync::watch;

/// Custom error type for moonlink
#[derive(Clone, Debug, Error, Deserialize, Serialize)]
pub enum Error {
    #[error("{0}")]
    Arrow(ErrorStruct),

    #[error("{0}")]
    Io(ErrorStruct),

    #[error("{0}")]
    Parquet(ErrorStruct),

    #[error("{0}")]
    WatchChannelRecvError(ErrorStruct),

    #[error("{0}")]
    IcebergError(ErrorStruct),

    #[error("{0}")]
    OpenDal(ErrorStruct),

    #[error("{0}")]
    JoinError(ErrorStruct),

    #[error("{0}")]
    Json(ErrorStruct),
}

pub type Result<T> = result::Result<T, Error>;

impl From<watch::error::RecvError> for Error {
    #[track_caller]
    fn from(source: watch::error::RecvError) -> Self {
        Error::WatchChannelRecvError(
            ErrorStruct::new(
                "Watch channel receiver error".to_string(),
                ErrorStatus::Permanent,
            )
            .with_source(source),
        )
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

        Error::Arrow(ErrorStruct::new("Arrow error".to_string(), status).with_source(source))
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

        Error::IcebergError(
            ErrorStruct::new("Iceberg error".to_string(), status).with_source(source),
        )
    }
}

impl From<io::Error> for Error {
    #[track_caller]
    fn from(source: io::Error) -> Self {
        let status = get_io_error_status(&source);
        Error::Io(ErrorStruct::new("IO error".to_string(), status).with_source(source))
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

        Error::OpenDal(ErrorStruct::new("OpenDAL error".to_string(), status).with_source(source))
    }
}

impl From<tokio::task::JoinError> for Error {
    #[track_caller]
    fn from(source: tokio::task::JoinError) -> Self {
        Error::JoinError(
            ErrorStruct::new("Join error".to_string(), ErrorStatus::Permanent).with_source(source),
        )
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

        Error::Parquet(ErrorStruct::new("Parquet error".to_string(), status).with_source(source))
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

        Error::Json(
            ErrorStruct::new(
                "JSON serialization/deserialization error".to_string(),
                status,
            )
            .with_source(source),
        )
    }
}

impl From<std::string::FromUtf8Error> for Error {
    #[track_caller]
    fn from(source: std::string::FromUtf8Error) -> Self {
        let status = ErrorStatus::Permanent;

        Error::Json(
            ErrorStruct::new("UTF8 conversion error".to_string(), status).with_source(source),
        )
    }
}

impl Error {
    pub fn get_status(&self) -> ErrorStatus {
        match self {
            Error::Arrow(err)
            | Error::Io(err)
            | Error::Parquet(err)
            | Error::WatchChannelRecvError(err)
            | Error::IcebergError(err)
            | Error::OpenDal(err)
            | Error::JoinError(err)
            | Error::Json(err) => err.status,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test util functions to create error.
    fn create_source_error() -> Result<()> {
        std::fs::File::open("/some/non/existent/file")?;
        Ok(())
    }
    fn propagate_error() -> Result<()> {
        create_source_error()?;
        Ok(())
    }
    fn another_propagate_error() -> Result<()> {
        propagate_error()?;
        Ok(())
    }

    /// Test location information is kept for the very source error.
    #[test]
    fn test_error_propagation_with_source() {
        let io_error = another_propagate_error().unwrap_err();
        if let Error::Io(ref inner) = io_error {
            let loc = inner.location.as_ref().unwrap();
            assert!(loc.contains("src/moonlink/src/error.rs"));
            assert!(loc.contains("186"));
            assert!(loc.contains("9"));
        }
    }
}
