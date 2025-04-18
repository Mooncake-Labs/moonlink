use moonlink;
use moonlink_connectors;
use thiserror::Error;

/// Custom error type for moonlink_backend that can wrap errors from both
/// moonlink and moonlink_connectors
#[derive(Debug, Error)]
pub enum Error {
    #[error("Moonlink error: {0}")]
    Moonlink(#[from] moonlink::error::Error),

    #[error("Connector error: {0}")]
    Connector(#[from] moonlink_connectors::Error),

    #[error("Other error: {0}")]
    Other(String),
}

/// Result type alias for moonlink_backend
pub type Result<T> = std::result::Result<T, Error>;

