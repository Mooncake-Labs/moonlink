use arrow::error::ArrowError;
use iceberg::Error as IcebergError;
use moonlink_error::{ErrorStatus, ErrorStruct};
use parquet::errors::ParquetError;
use std::io;
use std::panic::Location;
use std::result;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

/// Custom error type for moonlink
#[derive(Clone, Debug, Error)]
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

    // TODO(hjiang): Improve error propagation.
    #[error("Iceberg error: {0}")]
    IcebergMessage(String),

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
        Error::WatchChannelRecvError(ErrorStruct {
            message: format!("Watch channel receiver error: {source}"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
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

        Error::Arrow(ErrorStruct {
            message: format!("Arrow error: {source}"),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
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

        Error::IcebergError(ErrorStruct {
            message: format!("Iceberg error: {source}"),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<io::Error> for Error {
    #[track_caller]
    fn from(source: io::Error) -> Self {
        let status = match source.kind() {
            io::ErrorKind::TimedOut
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NetworkDown
            | io::ErrorKind::ResourceBusy
            | io::ErrorKind::QuotaExceeded => ErrorStatus::Temporary,

            // All other errors are permanent
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

        Error::OpenDal(ErrorStruct {
            message: format!("OpenDAL error: {source}"),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<tokio::task::JoinError> for Error {
    #[track_caller]
    fn from(source: tokio::task::JoinError) -> Self {
        Error::JoinError(ErrorStruct {
            message: format!("Join error: {source}"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
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

        Error::Parquet(ErrorStruct {
            message: format!("Parquet error: {source}"),
            status,
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

            // All other errors are permanent - data format/syntax issues
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

impl From<std::string::FromUtf8Error> for Error {
    #[track_caller]
    fn from(source: std::string::FromUtf8Error) -> Self {
        let status = ErrorStatus::Permanent;

        Error::Json(ErrorStruct {
            message: format!("UTF8 conversion error: {source}"),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
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
            let loc = inner.location.unwrap();
            assert_eq!(loc.file(), "src/moonlink/src/error.rs");
            assert_eq!(loc.line(), 217);
            assert_eq!(loc.column(), 9);
        }
    }
}
