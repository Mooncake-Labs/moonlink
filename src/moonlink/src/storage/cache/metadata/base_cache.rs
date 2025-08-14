use async_trait::async_trait;

#[async_trait]
#[allow(dead_code)]
pub trait MetadataCacheTrait<K, V>: Send + Sync
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Retrieves a value for the given key.
    ///
    /// **Note:** This returns a cloned copy of the value stored in the cache.
    /// Modifying the returned value does not affect the cached value.
    async fn get(&self, key: &K) -> Option<V>
    where
        V: Clone;

    async fn put(&self, key: K, value: V);

    async fn clear(&self);

    async fn evict(&self, key: &K);
}
