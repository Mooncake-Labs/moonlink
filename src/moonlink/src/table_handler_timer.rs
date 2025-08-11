use crate::storage::timer::base_timer::Ticker;
use crate::storage::timer::tokio_timer::TokioTicker;

use std::time::Duration;

/// Event timers.
pub struct TableHandlerTimer {
    /// Timer for periodical mooncake snapshot.
    pub mooncake_snapshot_timer: Box<dyn Ticker>,
    /// Timer for periodical force snapshot.
    pub force_snapshot_timer: Box<dyn Ticker>,
    /// Timer for periodical WAL operations.
    pub wal_snapshot_timer: Box<dyn Ticker>,
}

/// Util function to create table handler timers, with default config.
pub fn create_table_handler_timers() -> TableHandlerTimer {
    TableHandlerTimer {
        mooncake_snapshot_timer: Box::new(TokioTicker::new(Duration::from_millis(500))),
        force_snapshot_timer: Box::new(TokioTicker::new(Duration::from_secs(300))),
        wal_snapshot_timer: Box::new(TokioTicker::new(Duration::from_millis(500))),
    }
}
