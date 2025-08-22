use std::panic::Location;
use std::sync::Arc;

use arrow::error::ArrowError;
use bincode::error::DecodeError;
use datafusion::common::DataFusionError;
use moonlink_error::{io_error_utils, ErrorStatus, ErrorStruct};
use moonlink_rpc::Error as MoonlinkRPCError;
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
    MoonlinkRPCError(ErrorStruct),
    #[error("{0}")]
    DataFusionError(ErrorStruct),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<ArrowError> for Error {
    #[track_caller]
    fn from(source: ArrowError) -> Self {
        Error::Arrow(ErrorStruct {
            message: "Arrow error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<DataFusionError> for Error {
    #[track_caller]
    fn from(source: DataFusionError) -> Self {
        Error::DataFusionError(ErrorStruct {
            message: "Datafusion error".to_string(),
            status: ErrorStatus::Permanent,
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
        let status = match &source {
            MoonlinkRPCError::Decode(es)
            | MoonlinkRPCError::Encode(es)
            | MoonlinkRPCError::Io(es)
            | MoonlinkRPCError::PacketTooLong(es) => es.status,
        };
        Error::MoonlinkRPCError(ErrorStruct {
            message: "Moonlink RPC error".to_string(),
            status,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}
