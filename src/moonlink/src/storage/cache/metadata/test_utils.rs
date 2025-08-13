use crate::storage::cache::metadata::{cache_config::MetadataCacheConfig, moka_cache::MokaCache};
use std::{fmt::Debug, time::Duration};

pub struct MokaCacheTestBuilder {
    max_size: u64,
    ttl: Duration,
}

impl MokaCacheTestBuilder {
    pub fn new() -> Self {
        Self {
            max_size: 2,
            ttl: Duration::from_secs(3),
        }
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
