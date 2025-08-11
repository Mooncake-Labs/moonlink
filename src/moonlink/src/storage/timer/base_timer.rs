use async_trait::async_trait;

#[async_trait]
pub trait Ticker: Send + Sync {
    async fn tick(&mut self);
}
