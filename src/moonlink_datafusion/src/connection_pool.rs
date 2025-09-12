use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};
use tokio::{net::UnixStream, sync::Mutex, time::sleep};

const DEFAULT_MAX_ENTRIES_PER_URI: usize = 30;
const DEFAULT_IDLE_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) static POOL: LazyLock<Arc<Pool>> = LazyLock::new(|| {
    Arc::new(Pool::new(
        DEFAULT_MAX_ENTRIES_PER_URI,
        DEFAULT_IDLE_TIMEOUT_MS,
    ))
});

#[derive(Debug)]
pub struct PooledEntry {
    stream: UnixStream,
    inserted_at: Instant,
}

#[derive(Debug)]
/// Global connection pool.
///
/// Key: URI (String)
/// Value: Mutex-protected VecDeque of PooledEntry (the pool for that URI)
pub struct Pool {
    pub inner: Mutex<HashMap<String, VecDeque<PooledEntry>>>,
    pub max_entries_per_uri: usize,
    pub idle_timeout_ms: u64,
    pub maintenance_interval: Duration,
}

impl Pool {
    pub fn new(max_per_uri: usize, idle_timeout_ms: u64) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            max_entries_per_uri: max_per_uri,
            idle_timeout_ms,
            maintenance_interval: DEFAULT_MAINTENANCE_INTERVAL,
        }
    }

    pub fn idle_timeout(&self) -> Duration {
        Duration::from_millis(self.idle_timeout_ms)
    }

    pub async fn start_maintenance_pool_task(self: Arc<Self>) {
        loop {
            sleep(self.maintenance_interval).await;
            let idle_timeout = self.idle_timeout();
            let mut pool = self.inner.lock().await;

            for vec in pool.values_mut() {
                vec.retain(|entry| entry.inserted_at.elapsed() <= idle_timeout);
            }
        }
    }
}

#[derive(Debug)]
/// Represents a pooled Unix stream connection associated with a specific URI.
///
/// ## Fields
/// - `uri`: The URI associated with the pooled stream.
/// - `stream`: An optional `UnixStream` representing the actual connection.
///   This is wrapped in an `Option` to allow for ownership transfer when the
///   stream is dropped or taken out of the pool.
///
/// ## Note
/// The use of `Option` for `stream` is intentional to facilitate ownership transfer at drop.
pub struct PooledStream {
    pub uri: String,
    pub stream: Option<UnixStream>,
    pub pool: Arc<Pool>,
}

impl PooledStream {
    pub fn new(uri: String, stream: UnixStream, pool: Arc<Pool>) -> Self {
        Self {
            uri,
            stream: Some(stream),
            pool,
        }
    }

    pub fn stream_mut(&mut self) -> &mut UnixStream {
        self.stream.as_mut().unwrap()
    }
}

impl Drop for PooledStream {
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            let uri = self.uri.clone();
            let pool = self.pool.clone();
            tokio::spawn(async move {
                let mut pool_inner = pool.inner.lock().await;
                let pool_vec = pool_inner.entry(uri).or_default();

                if pool_vec.len() >= pool.max_entries_per_uri {
                    pool_vec.pop_front();
                }

                pool_vec.push_back(PooledEntry {
                    stream,
                    inserted_at: Instant::now(),
                });
            });
        }
    }
}

pub(crate) async fn get_stream_with_pool(
    uri: &str,
    pool: &Arc<Pool>,
) -> crate::Result<PooledStream> {
    {
        let mut pool_inner = pool.inner.lock().await;

        if let Some(vec) = pool_inner.get_mut(uri) {
            // Remove expired streams
            vec.retain(|entry| entry.inserted_at.elapsed() <= pool.idle_timeout());
            if !vec.is_empty() {
                return Ok(PooledStream::new(
                    uri.to_string(),
                    vec.pop_front().unwrap().stream,
                    pool.clone(),
                ));
            }
        }
    }
    // If there are no available streams, create a new one
    let stream = UnixStream::connect(uri).await?;
    Ok(PooledStream::new(uri.to_string(), stream, pool.clone()))
}

pub(crate) async fn get_stream(uri: &str) -> crate::Result<PooledStream> {
    get_stream_with_pool(uri, &POOL).await
}

pub fn start_global_maintenance_pool_task() {
    let pool = POOL.clone();
    tokio::spawn(async move {
        pool.start_maintenance_pool_task().await;
    });
}

#[cfg(test)]
mod tests {
    use crate::connection_pool::{get_stream_with_pool, Pool};
    use std::{sync::Arc, time::Duration};
    use tempfile::tempdir;
    use tokio::{net::UnixListener, time::sleep};

    fn create_test_pool() -> Arc<Pool> {
        Arc::new(Pool::new(30, 50)) // 50ms idle timeout for testing
    }

    #[tokio::test]
    async fn test_connection_pool_basic() {
        let test_pool = create_test_pool();
        let dir = tempdir().unwrap();
        let uri = dir.path().join("test_basic.sock");
        let uri_str = uri.to_str().unwrap().to_string();

        let listener = UnixListener::bind(&uri_str).expect("failed to bind socket");
        tokio::spawn(async move {
            loop {
                let _ = listener.accept().await;
            }
        });

        let stream1 = get_stream_with_pool(&uri_str, &test_pool)
            .await
            .expect("should connect");
        drop(stream1);

        let mut stream2 = get_stream_with_pool(&uri_str, &test_pool)
            .await
            .expect("should reuse from pool");
        let unix_stream = stream2.stream.take().unwrap();
        assert!(unix_stream.into_std().is_ok());
    }

    #[tokio::test]
    async fn test_pool_concurrent_multiple_uris() {
        let test_pool = create_test_pool();
        let dir = tempdir().unwrap();

        // URI 1
        let path1 = dir.path().join("test1.sock");
        let uri1 = path1.to_str().unwrap();
        let listener1 = UnixListener::bind(uri1).unwrap();
        tokio::spawn(async move {
            loop {
                let _ = listener1.accept().await;
            }
        });

        // URI 2
        let path2 = dir.path().join("test2.sock");
        let uri2 = path2.to_str().unwrap();
        let listener2 = UnixListener::bind(uri2).unwrap();
        tokio::spawn(async move {
            loop {
                let _ = listener2.accept().await;
            }
        });

        // Retrieve streams for URI1 and URI2 concurrently
        let (stream1, stream2) = tokio::join!(
            get_stream_with_pool(uri1, &test_pool),
            get_stream_with_pool(uri2, &test_pool)
        );

        let mut stream1 = stream1.expect("connect URI1");
        let mut stream2 = stream2.expect("connect URI2");

        let addr1 = stream1.stream_mut().peer_addr().unwrap();
        let addr2 = stream2.stream_mut().peer_addr().unwrap();

        drop(stream1);
        drop(stream2);

        // Retrieve streams for both URIs again; should reuse connections from the pool
        let (stream1b, stream2b) = tokio::join!(
            get_stream_with_pool(uri1, &test_pool),
            get_stream_with_pool(uri2, &test_pool)
        );

        let addr1b = stream1b.unwrap().stream_mut().peer_addr().unwrap();
        let addr2b = stream2b.unwrap().stream_mut().peer_addr().unwrap();

        assert_eq!(
            addr1.as_pathname(),
            addr1b.as_pathname(),
            "URI1 should reuse its connection"
        );
        assert_eq!(
            addr2.as_pathname(),
            addr2b.as_pathname(),
            "URI2 should reuse its connection"
        );
    }

    #[tokio::test]
    async fn test_pool_max_capacity_with_mock() {
        use crate::connection_pool::PooledStream;
        use tempfile::tempdir;
        use tokio::net::UnixStream;

        let test_pool = create_test_pool();
        let dir = tempdir().unwrap();
        let uri_path = dir.path().join("test_max_capacity.sock");
        let uri_str = uri_path.to_str().unwrap();

        // Create a UnixListener for the test socket
        let listener = tokio::net::UnixListener::bind(uri_str).unwrap();
        tokio::spawn(async move {
            loop {
                let _ = listener.accept().await;
            }
        });

        // Create MAX_PER_URI + 5 UnixStream connections
        let mut streams: Vec<PooledStream> = Vec::new();
        for _ in 0..(test_pool.max_entries_per_uri + 5) {
            let stream = UnixStream::connect(uri_str).await.unwrap();
            let ps = PooledStream::new(uri_str.to_string(), stream, test_pool.clone());
            streams.push(ps);
        }

        // Return all streams to pool
        for stream in streams {
            drop(stream);
        }
        // Give the spawned tasks time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let pool = test_pool.inner.lock().await;
        let vec = pool.get(uri_str);
        assert!(
            vec.is_some(),
            "pool should contain an entry for the test URI"
        );
        assert_eq!(
            vec.unwrap().len(),
            test_pool.max_entries_per_uri,
            "pool should not exceed MAX_PER_URI"
        );
    }

    #[tokio::test]
    async fn test_maintenance_task_cleanup() {
        let test_pool = Arc::new(Pool {
            inner: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            max_entries_per_uri: 30,
            idle_timeout_ms: 50,
            maintenance_interval: Duration::from_millis(30),
        });

        let dir = tempdir().unwrap();
        let uri1 = dir.path().join("test_maintenance1.sock");
        let uri1_str = uri1.to_str().unwrap().to_string();
        let uri2 = dir.path().join("test_maintenance2.sock");
        let uri2_str = uri2.to_str().unwrap().to_string();

        // Set up listeners for both URIs
        let listener1 = UnixListener::bind(&uri1_str).expect("failed to bind socket1");
        tokio::spawn(async move {
            loop {
                let _ = listener1.accept().await;
            }
        });

        let listener2 = UnixListener::bind(&uri2_str).expect("failed to bind socket2");
        tokio::spawn(async move {
            loop {
                let _ = listener2.accept().await;
            }
        });

        // Create streams for both URIs
        let stream1 = get_stream_with_pool(&uri1_str, &test_pool)
            .await
            .expect("should connect to uri1");
        let stream2 = get_stream_with_pool(&uri2_str, &test_pool)
            .await
            .expect("should connect to uri2");

        // Return streams to pool
        drop(stream1);
        drop(stream2);

        // Give spawned tasks time to complete
        sleep(Duration::from_millis(20)).await;

        // Verify both streams are in pool
        {
            let pool = test_pool.inner.lock().await;
            assert!(pool.contains_key(&uri1_str), "pool should contain uri1");
            assert!(pool.contains_key(&uri2_str), "pool should contain uri2");

            let vec1 = pool.get(&uri1_str).unwrap();
            let vec2 = pool.get(&uri2_str).unwrap();
            assert_eq!(vec1.len(), 1, "uri1 should have 1 stream");
            assert_eq!(vec2.len(), 1, "uri2 should have 1 stream");
        }

        // Wait for streams to expire
        sleep(test_pool.idle_timeout() + Duration::from_millis(50)).await;

        // Start maintenance task with timeout to prevent infinite loop
        let maintenance_result = tokio::time::timeout(
            Duration::from_millis(100), // Let it run for 100ms (should do 3+ cycles)
            test_pool.clone().start_maintenance_pool_task(),
        )
        .await;

        // We expect timeout (because start_maintenance_task runs forever)
        assert!(
            maintenance_result.is_err(),
            "maintenance task should timeout"
        );

        // Verify expired streams were cleaned up
        {
            let pool = test_pool.inner.lock().await;

            assert_eq!(
                pool.get(&uri1_str).unwrap().len(),
                0,
                "expired streams should be cleaned up for uri1"
            );

            assert_eq!(
                pool.get(&uri2_str).unwrap().len(),
                0,
                "expired streams should be cleaned up for uri2"
            );
        }
    }
}
