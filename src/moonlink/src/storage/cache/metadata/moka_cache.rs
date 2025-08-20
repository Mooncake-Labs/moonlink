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
    /// Retrieves a value for the given key.
    ///
    /// **Note:** This returns a cloned copy of the value stored in the cache.
    /// Modifying the returned value does not affect the cached value.
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

    /// Removes the entry for the specified `key` from the cache.
    ///
    /// # Behavior
    /// - If the key **exists**:
    ///   The entry is removed and the previous value is returned as `Some(V)`.
    ///
    /// - If the key **does not exist**:
    ///   Returns `None`.
    ///   No panic, no error, no log - the operation silently passes.
    ///
    /// # Example
    /// ```
    /// use your_crate::MokaCache;
    ///
    /// # async fn example() {
    ///     let cache = MokaCache::new();
    ///     cache.insert("a", 1).await;
    ///
    ///     assert_eq!(cache.evict(&"a").await, Some(1)); // removed value
    ///     assert_eq!(cache.evict(&"a").await, None);    // already gone, silently ignored
    /// # }
    /// ``
    async fn evict(&self, key: &K) -> Option<V> {
        self.cache.remove(key).await
    }

    async fn len(&self) -> u64 {
        self.cache.run_pending_tasks().await;
        self.cache.entry_count()
    }
}
