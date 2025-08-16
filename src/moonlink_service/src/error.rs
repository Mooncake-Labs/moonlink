use arrow_schema::ArrowError;
use moonlink_error::{ErrorStatus, ErrorStruct};
use std::io;
use std::panic::Location;
use std::result;
use std::sync::Arc;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
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
}

pub type Result<T> = result::Result<T, Error>;

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

impl From<moonlink_backend::Error> for Error {
    #[track_caller]
    fn from(source: moonlink_backend::Error) -> Self {
        Error::Backend(ErrorStruct {
            message: format!("Backend error: {source}"),
            status: ErrorStatus::Permanent,
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

impl From<moonlink_rpc::Error> for Error {
    #[track_caller]
    fn from(source: moonlink_rpc::Error) -> Self {
        Error::Rpc(ErrorStruct {
            message: format!("RPC error: {source}"),
            status: match &source {
                moonlink_rpc::Error::Io(err) => err.status,
                moonlink_rpc::Error::Decode(err) => err.status,
                moonlink_rpc::Error::Encode(err) => err.status,
                moonlink_rpc::Error::PacketTooLong(err) => err.status,
            },
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}

impl From<tokio::task::JoinError> for Error {
    #[track_caller]
    fn from(source: tokio::task::JoinError) -> Self {
        Error::TaskJoin(ErrorStruct {
            message: format!("Join error: {source}"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller()),
        })
    }
}
