use moonlink_error::io_error_utils::get_io_error_status;
use moonlink_error::{ErrorStatus, ErrorStruct};
use std::io;
use std::result;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Decode(ErrorStruct),

    #[error("{0}")]
    Encode(ErrorStruct),

    #[error("{0}")]
    Io(ErrorStruct),

    #[error("{0}")]
    PacketTooLong(ErrorStruct),
}

pub type Result<T> = result::Result<T, Error>;

impl From<bincode::error::DecodeError> for Error {
    #[track_caller]
    fn from(source: bincode::error::DecodeError) -> Self {
        Error::Decode(
            ErrorStruct::new("Decode error".to_string(), ErrorStatus::Permanent)
                .with_source(source),
        )
    }
}

impl From<bincode::error::EncodeError> for Error {
    #[track_caller]
    fn from(source: bincode::error::EncodeError) -> Self {
        Error::Encode(
            ErrorStruct::new("Encode error".to_string(), ErrorStatus::Permanent)
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

impl From<std::num::TryFromIntError> for Error {
    #[track_caller]
    fn from(source: std::num::TryFromIntError) -> Self {
        Error::PacketTooLong(
            ErrorStruct::new("Packet too long".to_string(), ErrorStatus::Permanent)
                .with_source(source),
        )
    }
}

impl Error {
    pub fn get_status(&self) -> ErrorStatus {
        match self {
            Error::Decode(err)
            | Error::Encode(err)
            | Error::Io(err)
            | Error::PacketTooLong(err) => err.status,
        }
    }
}
