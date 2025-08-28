use async_trait::async_trait;

#[async_trait]
#[allow(dead_code)]
pub trait MetadataCacheTrait<K, V>: Send + Sync
// TODO: Do we really need 'static here? Revisit later.
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

    /// Inserts a key-value pair into the cache.
    ///
    /// **Behavior:**
    /// - If the key does not exist, the key-value pair is inserted.
    /// - If the key already exists, the old value is overwritten with the new value.
    ///
    /// **Note:** This does not return the old value.
    async fn put(&self, key: K, value: V);

    /// Removes all entries from the cache.
    ///
    /// **Behavior:**
    /// - After calling this, the cache will be empty.
    /// - This operation does not fail if the cache is already empty.
    async fn clear(&self);

    /// Removes the entry for the given key.
    ///
    /// **Behavior:**
    /// - If the key exists, the entry is removed and its value is returned.
    /// - If the key does not exist, returns `None`.
    /// - This operation does not panic or log errors for missing keys.
    ///
    /// **Note:** The returned value is a cloned copy of the cached value.
    async fn evict(&self, key: &K) -> Option<V>
    where
        V: Clone;

    /// Returns the number of entries currently stored in the cache.
    async fn len(&self) -> u64;
}
