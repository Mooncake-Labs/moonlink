use crate::storage::timer::base_timer::Ticker;

use async_trait::async_trait;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

#[derive(Clone)]
pub struct ManualTicker {
    rx: std::sync::Arc<tokio::sync::Mutex<UnboundedReceiver<()>>>,
}

impl ManualTicker {
    #[allow(unused)]
    pub fn new() -> (Self, ManualTickerHandle) {
        let (tx, rx) = unbounded_channel();
        let ticker = ManualTicker {
            rx: std::sync::Arc::new(tokio::sync::Mutex::new(rx)),
        };
        let handle = ManualTickerHandle { tx };
        (ticker, handle)
    }
}

#[allow(unused)]
pub struct ManualTickerHandle {
    tx: UnboundedSender<()>,
}

impl ManualTickerHandle {
    #[allow(unused)]
    pub fn trigger(&self) {
        let _ = self.tx.send(());
    }
}

#[async_trait]
impl Ticker for ManualTicker {
    async fn tick(&mut self) {
        let mut rx = self.rx.lock().await;
        let _ = rx.recv().await;
    }
}
