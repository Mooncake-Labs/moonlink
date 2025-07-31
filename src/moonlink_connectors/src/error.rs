use crate::pg_replicate::postgres_source::{
    CdcStreamError, PostgresSourceError, TableCopyStreamError,
};
use moonlink::Error as MoonlinkError;
use std::result;
use std::sync::Arc;
use thiserror::Error;
use tokio_postgres::Error as TokioPostgresError;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("Postgres source error: {source}")]
    PostgresSourceError { source: Arc<PostgresSourceError> },

    #[error("tokio postgres error: {source}")]
    TokioPostgres { source: Arc<TokioPostgresError> },

    #[error("Postgres cdc stream error: {source}")]
    CdcStream { source: Arc<CdcStreamError> },

    #[error("Table copy stream error: {source}")]
    TableCopyStream { source: Arc<TableCopyStreamError> },

    #[error("Moonlink source error: {source}")]
    MoonlinkError { source: MoonlinkError },

    #[error("IO error: {source}")]
    Io { source: Arc<std::io::Error> },

    // Requested database table not found.
    #[error("Table {0} not found")]
    TableNotFound(String),
}

pub type Result<T> = result::Result<T, Error>;

impl From<MoonlinkError> for Error {
    fn from(source: MoonlinkError) -> Self {
        Error::MoonlinkError { source }
    }
}

impl From<PostgresSourceError> for Error {
    fn from(source: PostgresSourceError) -> Self {
        Error::PostgresSourceError {
            source: Arc::new(source),
        }
    }
}

impl From<TokioPostgresError> for Error {
    fn from(source: TokioPostgresError) -> Self {
        Error::TokioPostgres {
            source: Arc::new(source),
        }
    }
}

impl From<CdcStreamError> for Error {
    fn from(source: CdcStreamError) -> Self {
        Error::CdcStream {
            source: Arc::new(source),
        }
    }
}

impl From<TableCopyStreamError> for Error {
    fn from(source: TableCopyStreamError) -> Self {
        Error::TableCopyStream {
            source: Arc::new(source),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Error::Io {
            source: Arc::new(source),
        }
    }
}
