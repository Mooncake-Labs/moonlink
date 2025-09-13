use arrow_schema::ArrowError;
use moonlink_error::io_error_utils::get_io_error_status;
use moonlink_error::{ErrorStatus, ErrorStruct};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::io;
use std::result;
use thiserror::Error;

#[derive(Clone, Debug, Error, Deserialize, Serialize)]
pub enum Error {
    #[error("{0}")]
    Arrow(ErrorStruct),

    #[error("{0}")]
    Backend(ErrorStruct),

    #[error("{0}")]
    Io(ErrorStruct),

    #[error("{0}")]
    Rpc(ErrorStruct),

    #[error("{0}")]
    TaskJoin(ErrorStruct),

    #[error("{0}")]
    Http(ErrorStruct),

    #[error("{0}")]
    HttpRequest(ErrorStruct),
}

pub type Result<T> = result::Result<T, Error>;

impl Error {
    // TODO(hjiang): Finer-granular http status code.
    pub(crate) fn http_error(status_code: StatusCode) -> Self {
        let error_message =
            format!("Failed to make HTTP request with HTTP status code {status_code:?}");
        Self::Http(ErrorStruct::new(error_message, ErrorStatus::Permanent))
    }
}

impl From<reqwest::Error> for Error {
    #[track_caller]
    fn from(source: reqwest::Error) -> Self {
        let status = ErrorStatus::Permanent;
        Error::HttpRequest(
            ErrorStruct::new("HTTP request error".to_string(), status).with_source(source),
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

impl From<moonlink_backend::Error> for Error {
    #[track_caller]
    fn from(source: moonlink_backend::Error) -> Self {
        Error::Backend(
            ErrorStruct::new("Backend error".to_string(), ErrorStatus::Permanent)
                .with_source(source),
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

impl From<moonlink_rpc::Error> for Error {
    #[track_caller]
    fn from(source: moonlink_rpc::Error) -> Self {
        Error::Rpc(
            ErrorStruct::new("RPC error".to_string(), source.get_status()).with_source(source),
        )
    }
}

impl From<tokio::task::JoinError> for Error {
    #[track_caller]
    fn from(source: tokio::task::JoinError) -> Self {
        Error::TaskJoin(
            ErrorStruct::new("Join error".to_string(), ErrorStatus::Permanent).with_source(source),
        )
    }
}
