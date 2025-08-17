use moonlink_error::{ErrorStatus, ErrorStruct};
use serde_json::Error as SerdeJsonError;
use thiserror::Error;

#[cfg(feature = "metadata-postgres")]
use tokio_postgres::Error as TokioPostgresError;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[cfg(feature = "metadata-postgres")]
    #[error("{0}")]
    TokioPostgres(ErrorStruct),

    #[error("{0}")]
    PostgresRowCountError(ErrorStruct),

    #[error("{0}")]
    Sqlx(ErrorStruct),

    #[error("{0}")]
    SqliteRowCountError(ErrorStruct),

    #[error("{0}")]
    MetadataStoreFailedPrecondition(ErrorStruct),

    #[error("{0}")]
    SerdeJson(ErrorStruct),

    #[error("{0}")]
    TableIdNotFound(ErrorStruct),

    #[error("{0}")]
    ConfigFieldNotExist(ErrorStruct),

    #[error("{0}")]
    Io(ErrorStruct),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(feature = "metadata-postgres")]
impl From<TokioPostgresError> for Error {
    #[track_caller]
    fn from(source: TokioPostgresError) -> Self {
        Error::TokioPostgres(
            ErrorStruct::new(
                format!("tokio postgres error: {source}"),
                ErrorStatus::Permanent,
            )
            .with_source(source),
        )
    }
}

impl From<sqlx::Error> for Error {
    #[track_caller]
    fn from(source: sqlx::Error) -> Self {
        Error::Sqlx(
            ErrorStruct::new(format!("sqlx error: {source}"), ErrorStatus::Permanent)
                .with_source(source),
        )
    }
}

impl From<SerdeJsonError> for Error {
    #[track_caller]
    fn from(source: SerdeJsonError) -> Self {
        Error::SerdeJson(
            ErrorStruct::new(
                format!("serde json error: {source}"),
                ErrorStatus::Permanent,
            )
            .with_source(source),
        )
    }
}

impl From<std::io::Error> for Error {
    #[track_caller]
    fn from(source: std::io::Error) -> Self {
        let status = match source.kind() {
            std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::NetworkDown
            | std::io::ErrorKind::ResourceBusy
            | std::io::ErrorKind::QuotaExceeded => ErrorStatus::Temporary,

            _ => ErrorStatus::Permanent,
        };

        Error::Io(ErrorStruct::new("IO error".to_string(), status).with_source(source))
    }
}
