use crate::pg_replicate::pipeline::sources::postgres::PostgresSourceError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Postgres source error: {0}")]
    PostgresSource(#[from] PostgresSourceError),
}

pub type Result<T> = std::result::Result<T, Error>;
