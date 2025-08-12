#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::error::DecodeError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("RPC error: {0}")]
    Rpc(#[from] moonlink_rpc::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
