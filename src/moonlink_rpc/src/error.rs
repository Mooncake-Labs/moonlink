#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Decode error: {0}")]
    Decode(#[from] bincode::error::DecodeError),
    #[error("Encode error: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Packet too long: {0}")]
    PacketTooLong(#[from] std::num::TryFromIntError),
}

pub type Result<T> = std::result::Result<T, Error>;
