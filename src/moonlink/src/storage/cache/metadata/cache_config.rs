use std::time::Duration;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MetadataCacheConfig {
    pub max_size: u64,
    pub ttl: Duration,
}

#[allow(dead_code)]
impl MetadataCacheConfig {
    pub fn new(max_size: u64, ttl: Duration) -> Self {
        Self { max_size, ttl }
    }
}
