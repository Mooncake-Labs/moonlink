use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};
use tokio::{net::UnixStream, sync::Mutex};

const DEFAULT_MAX_ENTRIES_PER_URI: usize = 30;
const DEFAULT_IDLE_TIMEOUT_MS: Duration = Duration::from_millis(30_000);

static POOL: LazyLock<Arc<Pool>> = LazyLock::new(|| {
    Arc::new(Pool::new(
        DEFAULT_MAX_ENTRIES_PER_URI,
        DEFAULT_IDLE_TIMEOUT_MS,
    ))
});

#[derive(Debug)]
struct PooledEntry {
    stream: UnixStream,
    inserted_at: Instant,
}

#[derive(Debug)]
/// ### Global connection pool.
///
/// - Key: URI (String)
/// - Value: Mutex-protected VecDeque of PooledEntry (the pool for that URI)
pub(crate) struct Pool {
    inner: Mutex<HashMap<String, VecDeque<PooledEntry>>>,
    max_entries_per_uri: usize,
    idle_timeout_ms: Duration,
}

impl Pool {
    pub fn new(max_per_uri: usize, idle_timeout: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            max_entries_per_uri: max_per_uri,
            idle_timeout_ms: idle_timeout,
        }
    }

    pub(crate) fn idle_timeout(&self) -> Duration {
        self.idle_timeout_ms
    }

    pub(crate) async fn get_stream_with_pool(
        self: &Arc<Self>,
        uri: &str,
    ) -> crate::Result<PooledStream> {
        {
            let mut pool_inner = self.inner.lock().await;

            if let Some(vec) = pool_inner.get_mut(uri) {
                // Remove expired streams
                while let Some(entry) = vec.front() {
                    if entry.inserted_at.elapsed() > self.idle_timeout() {
                        vec.pop_front();
                    } else {
                        break;
                    }
                }

                if !vec.is_empty() {
                    return Ok(PooledStream::new(
                        uri.to_string(),
                        vec.pop_front().unwrap().stream,
                        self.clone(),
                    ));
                }
            }
        }
        // If there are no available streams, create a new one
        let stream = UnixStream::connect(uri).await?;
        Ok(PooledStream::new(uri.to_string(), stream, self.clone()))
    }

    pub(crate) async fn get_stream(uri: &str) -> crate::Result<PooledStream> {
        POOL.get_stream_with_pool(uri).await
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
pub(crate) struct PooledStream {
    pub uri: String,
    pub stream: Option<UnixStream>,
    pub pool: Arc<Pool>,
}

impl PooledStream {
    pub(crate) fn new(uri: String, stream: UnixStream, pool: Arc<Pool>) -> Self {
        Self {
            uri,
            stream: Some(stream),
            pool,
        }
    }

    pub(crate) fn stream_mut(&mut self) -> &mut UnixStream {
        self.stream
            .as_mut()
            .expect("stream already taken from PooledStream")
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

                while pool_vec.len() >= pool.max_entries_per_uri {
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

#[cfg(test)]
mod tests {
    use crate::connection_pool::Pool;
    use std::{sync::Arc, time::Duration};
    use tempfile::tempdir;
    use tokio::net::UnixListener;

    fn create_test_pool() -> Arc<Pool> {
        Arc::new(Pool::new(30, Duration::from_millis(50)))
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

        let stream1 = test_pool
            .get_stream_with_pool(&uri_str)
            .await
            .expect("should connect");
        drop(stream1);

        let mut stream2 = test_pool
            .get_stream_with_pool(&uri_str)
            .await
            .expect("should reuse from pool");
        let unix_stream = stream2.stream.take().unwrap();
        let _ = unix_stream.into_std().unwrap();
    }

    #[tokio::test]
    async fn test_pool_concurrent_multiple_uris() {
        use std::os::unix::io::AsRawFd;

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
            test_pool.get_stream_with_pool(uri1),
            test_pool.get_stream_with_pool(uri2)
        );

        let mut stream1 = stream1.expect("connect URI1");
        let mut stream2 = stream2.expect("connect URI2");

        let fd1 = stream1.stream_mut().as_raw_fd();
        let fd2 = stream2.stream_mut().as_raw_fd();

        drop(stream1);
        drop(stream2);
        // Give some time for the connections to be returned to the pool
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Invalidate both URIs so any new connects would fail; only reuse from pool should succeed.
        let _ = std::fs::remove_file(uri1);
        let _ = std::fs::remove_file(uri2);

        // Retrieve streams for both URIs again; should reuse connections from the pool
        let (stream1b, stream2b) = tokio::join!(
            test_pool.get_stream_with_pool(uri1),
            test_pool.get_stream_with_pool(uri2)
        );

        let mut stream1b = stream1b.expect("reuse uri1");
        let mut stream2b = stream2b.expect("reuse uri2");

        assert_eq!(
            fd1,
            stream1b.stream_mut().as_raw_fd(),
            "URI1 should reuse its connection"
        );
        assert_eq!(
            fd2,
            stream2b.stream_mut().as_raw_fd(),
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
}
