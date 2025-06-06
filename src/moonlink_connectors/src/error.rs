use crate::pg_replicate::moonlink_sink::SinkError;
use crate::pg_replicate::postgres_source::{
    CdcStreamError, PostgresSourceError, StatusUpdateError,
};
use moonlink::Error as MoonlinkError;
use std::result;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Postgres source error: {0}")]
    PostgresSourceError(#[from] PostgresSourceError),

    #[error("Postgres cdc stream error: {0}")]
    CdcStreamError(#[from] CdcStreamError),

    #[error("status update error: {0}")]
    StatusUpdateError(#[from] StatusUpdateError),

    #[error("Moonlink source error: {source}")]
    MoonlinkError { source: MoonlinkError },

    #[error("sink error: {0}")]
    SinkError(#[from] SinkError),
}

pub type Result<T> = result::Result<T, Error>;

impl From<MoonlinkError> for Error {
    fn from(source: MoonlinkError) -> Self {
        Error::MoonlinkError { source }
    }
}
