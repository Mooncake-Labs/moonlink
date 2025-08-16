use crate::storage::cache::metadata::{cache_config::MetadataCacheConfig, moka_cache::MokaCache};
use std::{fmt::Debug, time::Duration};

pub struct MokaCacheTestBuilder {
    max_size: u64,
    ttl: Duration,
}

impl MokaCacheTestBuilder {
    pub fn new() -> Self {
        let default_cfg = MetadataCacheConfig::default();
        Self {
            max_size: 2,
            ttl: default_cfg.ttl,
        }
    }

    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn build<K, V>(self) -> MokaCache<K, V>
    where
        K: std::hash::Hash + Eq + Clone + Send + Sync + Debug + 'static,
        V: Clone + Send + Sync + 'static,
    {
        let config = MetadataCacheConfig {
            max_size: self.max_size,
            ttl: self.ttl,
        };

        MokaCache::new(config)
    }
}
