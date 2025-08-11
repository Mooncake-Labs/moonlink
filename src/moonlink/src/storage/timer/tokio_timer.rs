use crate::storage::timer::base_timer::Ticker;

use async_trait::async_trait;

pub struct TokioTicker {
    inner: tokio::time::Interval,
}

impl TokioTicker {
    pub fn new(duration: std::time::Duration) -> Self {
        Self {
            inner: tokio::time::interval(duration),
        }
    }
}

#[async_trait]
impl Ticker for TokioTicker {
    async fn tick(&mut self) {
        self.inner.tick().await;
    }
}
