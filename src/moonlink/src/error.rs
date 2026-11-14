use arrow::error::ArrowError;
use deltalake::DeltaTableError;
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
    DeltaLakeError(ErrorStruct),

    #[error("{0}")]
    OpenDal(ErrorStruct),

    #[error("{0}")]
    JoinError(ErrorStruct),

    #[error("{0}")]
    Json(ErrorStruct),

    #[error("{0}")]
    PbToMoonlinkRowError(ErrorStruct),

    #[error("{0}")]
    TableErrored(ErrorStruct),

    /// The table handler is gone (table dropped, or its event loop stopped); unlike
    /// [`Error::TableErrored`] this does not say anything about the table's data.
    #[error("{0}")]
    TableHandlerStopped(ErrorStruct),

    /// An iceberg commit failed in a way that may or may not have changed the committed table;
    /// retrying could duplicate or lose data.
    #[error("{0}")]
    IcebergCommitOutcomeUnknown(ErrorStruct),
}

pub type Result<T> = result::Result<T, Error>;

impl Error {
    #[track_caller]
    pub fn iceberg_commit_outcome_unknown(message: String) -> Self {
        Self::IcebergCommitOutcomeUnknown(ErrorStruct::new(message, ErrorStatus::Permanent))
    }

    /// User-facing message for an errored mirror table.
    pub fn errored_table_message(table_name: &str, reason: &str) -> String {
        format!(
            "mirror {table_name} is out of sync: {reason}; drop and re-create it with mooncake.create_table"
        )
    }

    /// Error returned for any request on a table in the errored state; `message` comes from
    /// [`Error::errored_table_message`].
    #[track_caller]
    pub fn table_errored(message: &str) -> Self {
        Self::TableErrored(ErrorStruct::new(
            message.to_string(),
            ErrorStatus::Permanent,
        ))
    }

    #[track_caller]
    pub fn table_handler_stopped(message: &str) -> Self {
        Self::TableHandlerStopped(ErrorStruct::new(
            message.to_string(),
            ErrorStatus::Permanent,
        ))
    }

    #[track_caller]
    pub fn pb_conversion_error(message: String) -> Self {
        Self::PbToMoonlinkRowError(ErrorStruct::new(message, ErrorStatus::Permanent))
    }
    #[track_caller]
    pub fn delta_generic_error(message: String) -> Self {
        Self::DeltaLakeError(ErrorStruct::new(message, ErrorStatus::Permanent))
    }
}

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

impl From<DeltaTableError> for Error {
    #[track_caller]
    fn from(source: DeltaTableError) -> Self {
        let status = ErrorStatus::Permanent;

        Error::Json(ErrorStruct::new("Delta table error".to_string(), status).with_source(source))
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

impl From<url::ParseError> for Error {
    #[track_caller]
    fn from(source: url::ParseError) -> Self {
        let status = ErrorStatus::Permanent;

        Error::Json(ErrorStruct::new("URL parse error".to_string(), status).with_source(source))
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
            | Error::DeltaLakeError(err)
            | Error::OpenDal(err)
            | Error::JoinError(err)
            | Error::PbToMoonlinkRowError(err)
            | Error::Json(err)
            | Error::TableErrored(err)
            | Error::TableHandlerStopped(err)
            | Error::IcebergCommitOutcomeUnknown(err) => err.status,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Line of the `?` in [`create_source_error`], where the error location must point.
    const SOURCE_ERROR_LINE: u32 = line!() + 4;

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
            assert!(
                loc.contains(&format!("src/moonlink/src/error.rs:{SOURCE_ERROR_LINE}:9")),
                "unexpected location {loc}"
            );
        }
    }
}
