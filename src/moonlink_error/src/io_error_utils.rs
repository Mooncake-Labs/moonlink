use crate::ErrorStatus;

pub fn get_io_error_status(source: &std::io::Error) -> ErrorStatus {
    match source.kind() {
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

        // All other errors are permanent
        _ => ErrorStatus::Permanent,
    }
}
