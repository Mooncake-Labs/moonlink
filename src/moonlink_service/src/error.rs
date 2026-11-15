use moonlink_error::io_error_utils::get_io_error_status;
use moonlink_error::{ErrorStatus, ErrorStruct};
use serde::{Deserialize, Serialize};
use std::io;
use std::result;
use thiserror::Error;

#[derive(Clone, Debug, Error, Deserialize, Serialize)]
pub enum Error {
    #[error("{0}")]
    Backend(ErrorStruct),

    #[error("{0}")]
    Io(ErrorStruct),

    #[error("{0}")]
    Rpc(ErrorStruct),
}

pub type Result<T> = result::Result<T, Error>;

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
