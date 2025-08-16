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
            ErrorStruct::new(format!("Decode error: {source}"), ErrorStatus::Permanent)
                .with_source(source),
        )
    }
}

impl From<bincode::error::EncodeError> for Error {
    #[track_caller]
    fn from(source: bincode::error::EncodeError) -> Self {
        Error::Encode(
            ErrorStruct::new(format!("Encode error: {source}"), ErrorStatus::Permanent)
                .with_source(source),
        )
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

            _ => ErrorStatus::Permanent,
        };

        Error::Io(ErrorStruct::new(format!("IO error: {source}"), status).with_source(source))
    }
}

impl From<std::num::TryFromIntError> for Error {
    #[track_caller]
    fn from(source: std::num::TryFromIntError) -> Self {
        Error::PacketTooLong(
            ErrorStruct::new(format!("Packet too long: {source}"), ErrorStatus::Permanent)
                .with_source(source),
        )
    }
}
