use crate::pg_replicate::postgres_source::{
    CdcStreamError, PostgresSourceError, TableCopyStreamError,
};
use crate::rest_ingest::rest_source::RestSourceError;
use crate::rest_ingest::{json_converter, SrcTableId};
use moonlink::Error as MoonlinkError;
use moonlink_error::{io_error_utils, ErrorStatus, ErrorStruct};
use std::panic::Location;
use std::result;
use std::sync::Arc;
use thiserror::Error;
use tokio_postgres::Error as TokioPostgresError;

#[derive(Clone, Debug, Error)]
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
    #[error("Table {0} not found")]
    TableNotFound(String),

    // Invalid source type for operation.
    #[error("Invalid source type: {0}")]
    InvalidSourceType(String),

    // REST API error.
    #[error("REST API error: {0}")]
    RestApi(String),

    // REST source error.
    #[error("{0}")]
    RestSource(ErrorStruct),

    // REST source error: duplicate source table to add.
    #[error("REST source error: duplicate source table to add with table id {0}")]
    RestDuplicateTable(SrcTableId),

    // REST source error: non-existent source table to remove.
    #[error("REST source error: non-existent source table to remove with table id {0}")]
    RestNonExistentTable(SrcTableId),

    // REST source error: conversion from payload to moonlink row fails.
    #[error("{0}")]
    RestPayloadConversion(ErrorStruct),

    // Parquet parse error.
    #[error("{0}")]
    ParquetError(ErrorStruct),
}

pub type Result<T> = result::Result<T, Error>;

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
