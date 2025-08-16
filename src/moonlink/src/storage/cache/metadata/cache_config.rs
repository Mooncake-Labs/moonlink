use std::time::Duration;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MetadataCacheConfig {
    /// Maximum number of entries the cache can hold.
    ///
    /// Once this limit is reached, inserting a new entry will evict
    /// old entries according to the cache's eviction policy (e.g., LRU).
    ///
    /// Note: This does **not** limit the memory usage of the values themselves.
    /// Each entry (key-value pair) counts as 1 toward this limit.
    pub max_size: u64,

    /// Time-to-live (TTL) for each cache entry since it was inserted.
    ///
    /// Once this duration has passed, the entry will be automatically evicted
    /// from the cache, even if it has not been accessed.
    ///
    /// Note: This is **not** an idle expiration (i.e., it does not reset on access).
    pub ttl: Duration,
}

#[allow(dead_code)]
impl MetadataCacheConfig {
    pub(crate) const DEFAULT_MAX_SIZE: u64 = 1000;
    pub(crate) const DEFAULT_TTL_SECS: u64 = 3600; // 1 hour

    pub fn new(max_size: u64, ttl: Duration) -> Self {
        Self { max_size, ttl }
    }

    pub fn default_max_size() -> u64 {
        Self::DEFAULT_MAX_SIZE
    }

    pub fn default_ttl() -> Duration {
        Duration::from_secs(Self::DEFAULT_TTL_SECS)
    }
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        Self {
            max_size: Self::default_max_size(),
            ttl: Self::default_ttl(),
        }
    }
}
