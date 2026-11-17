use crate::pg_replicate::postgres_source::{
    CdcStreamError, PostgresSourceError, TableCopyStreamError,
};
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

    // Parquet parse error.
    #[error("{0}")]
    ParquetError(ErrorStruct),

    // Background writer task failed (panic/cancel/join error).
    #[error("{0}")]
    WriterTaskFailed(ErrorStruct),
    // The mirror is in the errored state; the message tells the user what to do.
    #[error("{0}")]
    TableErrored(ErrorStruct),
    // The source table cannot be mirrored faithfully.
    #[error("{0}")]
    UnsupportedSourceTable(ErrorStruct),
}

pub type Result<T> = result::Result<T, Error>;

impl Error {
    pub fn error_struct(&self) -> &ErrorStruct {
        match self {
            Error::PostgresSourceError(es)
            | Error::TokioPostgres(es)
            | Error::CdcStream(es)
            | Error::TableCopyStream(es)
            | Error::MoonlinkError(es)
            | Error::Io(es)
            | Error::MpscChannelSendError(es)
            | Error::TableNotFound(es)
            | Error::ReplDuplicateTable(es)
            | Error::ParquetError(es)
            | Error::WriterTaskFailed(es)
            | Error::TableErrored(es)
            | Error::UnsupportedSourceTable(es) => es,
        }
    }

    /// Whether the failure is about reaching the source (refused or lost connections, the server
    /// starting or shutting down, too many connections, authentication, lock timeouts), so trying
    /// again later can succeed, rather than about the table itself.
    pub fn is_transient_source_error(&self) -> bool {
        error_struct_has_cause(self.error_struct(), &is_transient_cause)
    }

    /// A new connection to a source must wait for an earlier one to finish shutting down.
    #[track_caller]
    pub fn source_shutting_down(source: &str) -> Self {
        Error::Io(ErrorStruct {
            message: format!(
                "an earlier connection to {source} is still shutting down (dropping its \
                 publication, replication slot or a dropped mirror's publication entry); retry"
            ),
            status: ErrorStatus::Temporary,
            // A timeout, so recovery (`is_transient_cause`) also treats it as transient.
            source: Some(Arc::new(
                std::io::Error::new(std::io::ErrorKind::TimedOut, "waiting for the shutdown")
                    .into(),
            )),
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

    /// Request on a mirror that is in the errored state.
    #[track_caller]
    pub fn table_errored(message: &str) -> Self {
        Error::TableErrored(ErrorStruct {
            message: message.to_string(),
            status: ErrorStatus::Permanent,
            source: None,
            location: Some(Location::caller().to_string()),
        })
    }

    #[track_caller]
    pub fn unsupported_source_table(src_table_name: &str, reason: &str) -> Self {
        Error::UnsupportedSourceTable(ErrorStruct {
            message: format!("source table {src_table_name} cannot be mirrored: {reason}"),
            status: ErrorStatus::Permanent,
            source: None,
            location: Some(Location::caller().to_string()),
        })
    }

    /// A source table changed in a way its existing mirror cannot follow.
    #[track_caller]
    pub fn source_table_changed(src_table_name: &str, reason: &str) -> Self {
        Error::UnsupportedSourceTable(ErrorStruct {
            message: format!(
                "source table {src_table_name} changed in a way its mirror cannot follow: {reason}"
            ),
            status: ErrorStatus::Permanent,
            source: None,
            location: Some(Location::caller().to_string()),
        })
    }

    /// A second mirror of a source table that is already mirrored.
    #[track_caller]
    pub fn source_already_mirrored(src_table_name: &str) -> Self {
        Error::ReplDuplicateTable(ErrorStruct {
            message: format!(
                "source table {src_table_name} is already mirrored; a source table can have only one mirror"
            ),
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

/// Whether `es` or anything it wraps (through nested connector errors too) satisfies `pred`.
pub fn error_struct_has_cause(
    es: &ErrorStruct,
    pred: &dyn Fn(&(dyn std::error::Error + 'static)) -> bool,
) -> bool {
    let Some(source) = &es.source else {
        return false;
    };
    source.chain().any(|cause| {
        pred(cause)
            || cause
                .downcast_ref::<Error>()
                .is_some_and(|inner| error_struct_has_cause(inner.error_struct(), pred))
    })
}

pub fn is_transient_cause(cause: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(e) = cause.downcast_ref::<TokioPostgresError>() {
        return match e.code() {
            // Not a server error: connecting, a closed connection, I/O, a timeout.
            None => true,
            Some(code) => {
                let code = code.code();
                // connection exception, insufficient resources, invalid authorization
                code.starts_with("08")
                    || code.starts_with("53")
                    || code.starts_with("28")
                    // admin/crash shutdown, cannot connect now (starting up)
                    || code.starts_with("57P0")
                    // lock_not_available (lock_timeout), serialization failure, deadlock
                    || code == "55P03"
                    || code == "40001"
                    || code == "40P01"
            }
        };
    }
    if let Some(e) = cause.downcast_ref::<std::io::Error>() {
        use std::io::ErrorKind::*;
        return matches!(
            e.kind(),
            ConnectionRefused
                | ConnectionReset
                | ConnectionAborted
                | NotConnected
                | TimedOut
                | BrokenPipe
                | AddrNotAvailable
                | UnexpectedEof
        );
    }
    false
}
