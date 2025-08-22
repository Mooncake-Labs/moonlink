use arrow::error::ArrowError;
use bincode::error::DecodeError;
use moonlink_error::{io_error_utils, ErrorStatus, ErrorStruct};
use moonlink_rpc::Error as MoonlinkRPCError;
use std::{panic::Location, sync::Arc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Arrow(ErrorStruct),
    #[error("{0}")]
    Bincode(ErrorStruct),
    #[error("{0}")]
    Io(ErrorStruct),
    #[error("{0}")]
    Rpc(ErrorStruct),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<ArrowError> for Error {
    #[track_caller]
    fn from(source: ArrowError) -> Self {
        Error::Arrow(ErrorStruct {
            message: "Arrow error".to_string(),
            status: match source {
                ArrowError::IoError(_, _) => ErrorStatus::Temporary,
                _ => ErrorStatus::Permanent,
            },
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<DecodeError> for Error {
    fn from(source: DecodeError) -> Self {
        Error::Bincode(ErrorStruct {
            message: "DecodeError error".to_string(),
            status: ErrorStatus::Permanent,
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

impl From<MoonlinkRPCError> for Error {
    #[track_caller]
    fn from(source: MoonlinkRPCError) -> Self {
        let status = source.get_status();
        Error::Rpc(ErrorStruct {
            message: "Moonlink RPC error".to_string(),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}
