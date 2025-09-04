use crate::pg_replicate::postgres_source::{
    CdcStreamError, PostgresSourceError, TableCopyStreamError,
};
use crate::rest_ingest::rest_source::RestSourceError;
use crate::rest_ingest::{json_converter, schema_builder, SrcTableId};
use moonlink::Error as MoonlinkError;
use moonlink_error::{io_error_utils, ErrorStatus, ErrorStruct};
use serde::{Deserialize, Serialize};
use std::panic::Location;
use std::result;
use std::sync::Arc;
use thiserror::Error;
use tokio_postgres::Error as TokioPostgresError;

#[derive(Clone, Debug, Error, Deserialize, Serialize)]
pub enum Error {
    #[error("{0}")]
    PostgresSourceError(ErrorStruct),

    #[error("{0}")]
    TokioPostgres(ErrorStruct),

    #[error("{0}")]
    CdcStream(ErrorStruct),

    #[error("{0}")]
    TableCopyStream(ErrorStruct),

    #[error("{0}")]
    MoonlinkError(ErrorStruct),

    #[error("{0}")]
    Io(ErrorStruct),

    #[error("{0}")]
    MpscChannelSendError(ErrorStruct),

    // Requested database table not found.
    #[error("{0}")]
    TableNotFound(ErrorStruct),

    // Table replication error: duplicate table.
    #[error("{0}")]
    ReplDuplicateTable(ErrorStruct),

    // REST API error.
    #[error("{0}")]
    RestApi(ErrorStruct),

    // REST source error.
    #[error("{0}")]
    RestSource(ErrorStruct),

    // REST source error: duplicate source table to add.
    #[error("{0}")]
    RestDuplicateTable(ErrorStruct),

    // REST source error: non-existent source table to remove.
    #[error("{0}")]
    RestNonExistentTable(ErrorStruct),

    // REST source error: conversion from payload to moonlink row fails.
    #[error("{0}")]
    RestPayloadConversion(ErrorStruct),

    // Parquet parse error.
    #[error("{0}")]
    ParquetError(ErrorStruct),

    // Background writer task failed (panic/cancel/join error).
    #[error("{0}")]
    WriterTaskFailed(ErrorStruct),

    // Schema building error
    #[error("{0}")]
    SchemaBuildError(ErrorStruct),
}

pub type Result<T> = result::Result<T, Error>;

impl Error {
    #[track_caller]
    pub fn rest_duplicate_table(id: SrcTableId) -> Self {
        Error::RestDuplicateTable(ErrorStruct {
            message: format!("REST source error: duplicate source table to add with table id {id}"),
            status: ErrorStatus::Permanent,
            source: None,
            location: Some(Location::caller().to_string()),
        })
    }

    #[track_caller]
    pub fn table_not_found(table_name: String) -> Self {
        Error::TableNotFound(ErrorStruct {
            message: format!("Table {table_name} not found"),
            status: ErrorStatus::Permanent,
            source: None,
            location: Some(Location::caller().to_string()),
        })
    }

    #[track_caller]
    pub fn repl_duplicate_table(table_name: String) -> Self {
        Error::ReplDuplicateTable(ErrorStruct {
            message: format!("Duplicate table added to replication: {table_name}"),
            status: ErrorStatus::Permanent,
            source: None,
            location: Some(Location::caller().to_string()),
        })
    }

    #[track_caller]
    pub fn rest_api(err_msg: String, err: Option<Arc<anyhow::Error>>) -> Self {
        Error::RestApi(ErrorStruct {
            message: format!("REST API error: {err_msg}"),
            status: ErrorStatus::Permanent,
            source: err,
            location: Some(Location::caller().to_string()),
        })
    }

    #[track_caller]
    pub fn rest_non_existent_table(id: SrcTableId) -> Self {
        Error::RestNonExistentTable(ErrorStruct {
            message: format!(
                "REST source error: non-existent source table to remove with table id {id}"
            ),
            status: ErrorStatus::Permanent,
            source: None,
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<tokio::task::JoinError> for Error {
    #[track_caller]
    fn from(source: tokio::task::JoinError) -> Self {
        Error::WriterTaskFailed(ErrorStruct {
            message: format!("Writer task failed: {source}"),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<MoonlinkError> for Error {
    #[track_caller]
    fn from(source: MoonlinkError) -> Self {
        Error::MoonlinkError(ErrorStruct {
            message: "Moonlink source error".to_string(),
            status: source.get_status(),
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<PostgresSourceError> for Error {
    #[track_caller]
    fn from(source: PostgresSourceError) -> Self {
        Error::PostgresSourceError(ErrorStruct {
            message: "Postgres source error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<TokioPostgresError> for Error {
    #[track_caller]
    fn from(source: TokioPostgresError) -> Self {
        Error::TokioPostgres(ErrorStruct {
            message: "tokio postgres error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<CdcStreamError> for Error {
    #[track_caller]
    fn from(source: CdcStreamError) -> Self {
        Error::CdcStream(ErrorStruct {
            message: "Postgres cdc stream error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<TableCopyStreamError> for Error {
    #[track_caller]
    fn from(source: TableCopyStreamError) -> Self {
        Error::TableCopyStream(ErrorStruct {
            message: "Table copy stream error".to_string(),
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

impl<T: Send + Sync + 'static> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    #[track_caller]
    fn from(source: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Error::MpscChannelSendError(ErrorStruct {
            message: "mpsc channel send error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<RestSourceError> for Error {
    #[track_caller]
    fn from(source: RestSourceError) -> Self {
        Error::RestSource(ErrorStruct {
            message: "rest source error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<json_converter::JsonToMoonlinkRowError> for Error {
    #[track_caller]
    fn from(source: json_converter::JsonToMoonlinkRowError) -> Self {
        Error::RestPayloadConversion(ErrorStruct {
            message: "REST API payload conversion error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<parquet::errors::ParquetError> for Error {
    #[track_caller]
    fn from(source: parquet::errors::ParquetError) -> Self {
        Error::ParquetError(ErrorStruct {
            message: "Parquet error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}

impl From<schema_builder::SchemaBuildError> for Error {
    #[track_caller]
    fn from(source: schema_builder::SchemaBuildError) -> Self {
        Error::SchemaBuildError(ErrorStruct {
            message: "Schema building error".to_string(),
            status: ErrorStatus::Permanent,
            source: Some(Arc::new(source.into())),
            location: Some(Location::caller().to_string()),
        })
    }
}
