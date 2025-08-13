use async_trait::async_trait;

#[async_trait]
#[allow(dead_code)]
pub trait MetadataCacheTrait<K, V>: Send + Sync
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    async fn get(&self, key: &K) -> Option<V>
    where
        V: Clone;

    async fn put(&self, key: K, value: V);

    async fn clear(&self);

    async fn evict(&self, key: &K);
}
