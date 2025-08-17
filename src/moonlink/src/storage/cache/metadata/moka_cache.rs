use std::fmt::Debug;

use crate::storage::cache::metadata::{
    base_cache::MetadataCacheTrait, cache_config::MetadataCacheConfig,
};
use async_trait::async_trait;
use moka::future::Cache;

#[cfg(test)]
mod test_utils;

/// A wrapper around [`moka::future::Cache`] providing async cache operations.
///
/// # Eviction Policy
/// Uses a **size-based eviction** policy: when the cache reaches its `max_size`,
/// the **least recently inserted** entries are evicted according to Moka's internal policy.
///
/// # Consistency Guarantee
/// `MokaCache` provides **strong consistency** for individual operations:
/// `get`, `put`, `evict`, and `clear` are atomic for a single entry.
/// However, operations are **not transactional** across multiple entries.
///
/// # Supported Features
/// - **TTL (time-to-live)**: entries expire after a fixed duration since insertion.
/// - **Max size**: limits the number of entries (or total weight if using a custom weigher).
/// - **Asynchronous operations**: all API methods are `async`.
///
/// # Example
/// ```rust
/// let config = MetadataCacheConfig::new(100, Duration::from_secs(60));
/// let cache: MokaCache<String, String> = MokaCache::new(config);
/// cache.put("key".to_string(), "value".to_string()).await;
/// ```
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

    /// Inserts a key-value pair into the cache.
    ///
    /// - If the key already exists:
    ///   - The old value will be **overwritten** with the new one.
    ///   - The entry's **expiration time (TTL)** will be refreshed (reset from now).
    ///
    /// - If the key does not exist:
    ///   - A new entry will be inserted with the configured TTL.
    ///
    /// # Example
    /// ```
    /// cache.put("user:1".to_string(), "Alice".to_string()).await;
    /// cache.put("user:1".to_string(), "Bob".to_string()).await;
    ///
    /// // "Alice" is replaced by "Bob", and TTL is refreshed.
    /// ```
    async fn put(&self, key: K, value: V) {
        self.cache.insert(key, value).await;
    }

    async fn clear(&self) {
        self.cache.invalidate_all();
    }

    async fn evict(&self, key: &K) {
        self.cache.invalidate(key).await;
    }

    async fn len(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.entry_count()
    }
}
