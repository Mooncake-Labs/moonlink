use moonlink_error::io_error_utils::get_io_error_status;
use moonlink_error::{ErrorStatus, ErrorStruct};
use serde::{Deserialize, Serialize};
use serde_json::Error as SerdeJsonError;
use thiserror::Error;

#[derive(Clone, Debug, Error, Deserialize, Serialize)]
pub enum Error {
    #[error("{0}")]
    Sqlx(ErrorStruct),

    #[error("{0}")]
    SqliteRowCountError(ErrorStruct),

    #[error("{0}")]
    MetadataStoreFailedPrecondition(ErrorStruct),

    #[error("{0}")]
    SerdeJson(ErrorStruct),

    #[error("{0}")]
    TableIdNotFound(ErrorStruct),

    #[error("{0}")]
    ConfigFieldNotExist(ErrorStruct),

    #[error("{0}")]
    Io(ErrorStruct),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<sqlx::Error> for Error {
    #[track_caller]
    fn from(source: sqlx::Error) -> Self {
        Error::Sqlx(
            ErrorStruct::new("sqlx error".to_string(), ErrorStatus::Permanent).with_source(source),
        )
    }
}

impl From<SerdeJsonError> for Error {
    #[track_caller]
    fn from(source: SerdeJsonError) -> Self {
        Error::SerdeJson(
            ErrorStruct::new("serde json error".to_string(), ErrorStatus::Permanent)
                .with_source(source),
        )
    }
}

impl From<std::io::Error> for Error {
    #[track_caller]
    fn from(source: std::io::Error) -> Self {
        let status = get_io_error_status(&source);
        Error::Io(ErrorStruct::new("IO error".to_string(), status).with_source(source))
    }
}

impl Error {
    pub fn get_status(&self) -> ErrorStatus {
        match self {
            Error::Sqlx(err)
            | Error::SqliteRowCountError(err)
            | Error::MetadataStoreFailedPrecondition(err)
            | Error::SerdeJson(err)
            | Error::TableIdNotFound(err)
            | Error::ConfigFieldNotExist(err)
            | Error::Io(err) => err.status,
        }
    }
}
