use std::fmt::Debug;

use crate::storage::cache::metadata::{
    base_cache::MetadataCacheTrait, cache_config::MetadataCacheConfig,
};
use async_trait::async_trait;
use moka::future::Cache;

pub struct MokaCache<K, V> {
    cache: Cache<K, V>,
}

#[allow(dead_code)]
impl<K, V> MokaCache<K, V>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + Debug + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(config: MetadataCacheConfig) -> Self {
        let cache = Cache::builder()
            .max_capacity(config.max_size)
            .time_to_live(config.ttl)
            .eviction_policy(moka::policy::EvictionPolicy::lru())
            .build();

        Self { cache }
    }

    #[cfg(test)]
    pub async fn initialize_for_test(&self, entries: Vec<(K, V)>) {
        for (k, v) in entries {
            self.cache.insert(k, v).await;
        }
    }

    #[cfg(test)]
    pub async fn dump_all_for_test(&self) -> Vec<(K, V)> {
        self.cache
            .iter()
            .map(|(k_arc, v)| ((*k_arc).clone(), v.clone()))
            .collect()
    }

    #[cfg(test)]
    pub async fn force_cleanup_for_test(&self) {
        self.cache.run_pending_tasks().await;
    }
}

#[async_trait]
impl<K, V> MetadataCacheTrait<K, V> for MokaCache<K, V>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    async fn get(&self, key: &K) -> Option<V> {
        self.cache.get(key).await
    }

    async fn put(&self, key: K, value: V) {
        self.cache.insert(key, value).await;
    }

    async fn clear(&self) {
        self.cache.invalidate_all();
    }

    async fn evict(&self, key: &K) {
        self.cache.invalidate(key).await;
    }
}
