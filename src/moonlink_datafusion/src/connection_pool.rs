use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        LazyLock,
    },
    time::{Duration, Instant},
};
use tokio::{net::UnixStream, sync::Mutex, time::sleep};

struct PooledEntry {
    stream: UnixStream,
    last_used: Instant,
}

static POOL: LazyLock<Mutex<HashMap<String, Mutex<Vec<PooledEntry>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const MAX_PER_URI: usize = 30;

static IDLE_TIMEOUT_MS: AtomicU64 = AtomicU64::new(30_000);

pub fn idle_timeout() -> Duration {
    Duration::from_millis(IDLE_TIMEOUT_MS.load(Ordering::Relaxed))
}

#[derive(Debug)]
pub struct PooledStream {
    pub uri: String,
    pub stream: Option<UnixStream>,
}

impl PooledStream {
    pub fn new(uri: String, stream: UnixStream) -> Self {
        Self {
            uri,
            stream: Some(stream),
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
            tokio::spawn(async move {
                let mut pool = POOL.lock().await;
                let mut pool_vec = pool.entry(uri).or_default().lock().await;

                if pool_vec.len() < MAX_PER_URI {
                    pool_vec.push(PooledEntry {
                        stream,
                        last_used: Instant::now(),
                    });
                }
            });
        }
    }
}

pub(crate) async fn get_stream(uri: &str) -> crate::Result<PooledStream> {
    {
        let mut pool = POOL.lock().await;
        if let Some(mutex_vec) = pool.get_mut(uri) {
            let mut vec = mutex_vec.lock().await;
            // Remove expired streams
            vec.retain(|entry| entry.last_used.elapsed() <= idle_timeout());

            while let Some(entry) = vec.pop() {
                if is_connection_alive(&entry.stream).await {
                    return Ok(PooledStream::new(uri.to_string(), entry.stream));
                }
            }
        }
    }
    // If there are no available streams, create a new one
    let stream = UnixStream::connect(uri).await?;
    Ok(PooledStream::new(uri.to_string(), stream))
}

async fn is_connection_alive(stream: &UnixStream) -> bool {
    match stream.try_read(&mut []) {
        Ok(_) => true,
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => true,
        Err(_) => false,
    }
}

pub async fn start_maintenance_task() {
    loop {
        sleep(Duration::from_secs(5)).await;

        let mut pool = POOL.lock().await;
        for (_uri, vec_mutex) in pool.iter_mut() {
            let mut vec = vec_mutex.lock().await;
            vec.retain(|entry| entry.last_used.elapsed() <= idle_timeout());
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{sync::atomic::Ordering, time::Duration};

    use crate::connection_pool::{get_stream, idle_timeout, IDLE_TIMEOUT_MS, POOL};
    use tempfile::tempdir;
    use tokio::{net::UnixListener, time::sleep};

    pub fn set_idle_timeout(timeout: Duration) {
        IDLE_TIMEOUT_MS.store(timeout.as_millis() as u64, Ordering::Relaxed);
    }

    #[tokio::test]
    async fn test_connection_pool_basic() {
        let dir = tempdir().unwrap();
        let uri = dir.path().join("test_basic.sock");
        let uri_str = uri.to_str().unwrap().to_string();

        let listener = UnixListener::bind(&uri_str).expect("failed to bind socket");
        tokio::spawn(async move {
            loop {
                let _ = listener.accept().await;
            }
        });

        let stream1 = get_stream(&uri_str).await.expect("should connect");
        drop(stream1);

        let mut stream2 = get_stream(&uri_str).await.expect("should reuse from pool");
        let unix_stream = stream2.stream.take().unwrap();
        assert!(unix_stream.into_std().is_ok());
    }

    #[tokio::test]
    async fn test_pool_concurrent_multiple_uris() {
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
        let (stream1, stream2) = tokio::join!(get_stream(uri1), get_stream(uri2));

        let mut stream1 = stream1.expect("connect URI1");
        let mut stream2 = stream2.expect("connect URI2");

        let addr1 = stream1.stream_mut().peer_addr().unwrap();
        let addr2 = stream2.stream_mut().peer_addr().unwrap();

        drop(stream1);
        drop(stream2);

        // Retrieve streams for both URIs again; should reuse connections from the pool
        let (stream1b, stream2b) = tokio::join!(get_stream(uri1), get_stream(uri2));

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
        use crate::connection_pool::{PooledStream, MAX_PER_URI, POOL};
        use tempfile::tempdir;
        use tokio::net::UnixStream;

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
        for _ in 0..(MAX_PER_URI + 5) {
            let stream = UnixStream::connect(uri_str).await.unwrap();
            let ps = PooledStream::new(uri_str.to_string(), stream);
            streams.push(ps);
        }

        // Return all streams to POOL
        for stream in streams {
            drop(stream);
        }
        // Give the spawned tasks time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let pool = POOL.lock().await;
        let vec_mutex = pool.get(uri_str);
        assert!(
            vec_mutex.is_some(),
            "pool should contain an entry for the test URI"
        );
        let vec = vec_mutex.unwrap().lock().await;
        assert_eq!(vec.len(), MAX_PER_URI, "pool should not exceed MAX_PER_URI");
    }

    #[tokio::test]
    async fn test_stream_expiration() {
        let dir = tempdir().unwrap();
        let uri = dir.path().join("test_expiration.sock");
        let uri_str = uri.to_str().unwrap().to_string();
        set_idle_timeout(Duration::from_millis(50));

        let listener = UnixListener::bind(&uri_str).expect("failed to bind socket");
        tokio::spawn(async move {
            loop {
                let _ = listener.accept().await;
            }
        });

        // Create a stream and return it to the pool
        let stream = get_stream(&uri_str).await.expect("should connect");
        drop(stream);

        // Give the spawned task time to add to pool
        sleep(Duration::from_millis(10)).await;

        // Verify stream is in pool
        {
            let pool = POOL.lock().await;
            let vec_mutex = pool.get(&uri_str).unwrap();
            let vec = vec_mutex.lock().await;
            assert_eq!(vec.len(), 1, "pool should contain 1 stream");
        }

        // Wait for stream to expire (idle_timeout() + buffer)
        sleep(idle_timeout() + Duration::from_millis(100)).await;

        // Try to get a stream - expired one should be removed, new one created
        let stream2 = get_stream(&uri_str).await.expect("should connect");

        // Verify the expired stream was cleaned up during get_stream
        {
            let pool = POOL.lock().await;
            let vec_mutex = pool.get(&uri_str).unwrap();
            let vec = vec_mutex.lock().await;
            assert_eq!(vec.len(), 0, "expired stream should have been removed");
        }

        drop(stream2);
    }

    #[tokio::test]
    async fn test_maintenance_task_cleanup() {
        let dir = tempdir().unwrap();
        let uri1 = dir.path().join("test_maintenance1.sock");
        let uri1_str = uri1.to_str().unwrap().to_string();
        let uri2 = dir.path().join("test_maintenance2.sock");
        let uri2_str = uri2.to_str().unwrap().to_string();
        set_idle_timeout(Duration::from_millis(50));

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
        let stream1 = get_stream(&uri1_str).await.expect("should connect to uri1");
        let stream2 = get_stream(&uri2_str).await.expect("should connect to uri2");

        // Return streams to pool
        drop(stream1);
        drop(stream2);

        // Give spawned tasks time to complete
        sleep(Duration::from_millis(10)).await;

        // Verify both streams are in pool
        {
            let pool = POOL.lock().await;
            assert!(pool.contains_key(&uri1_str), "pool should contain uri1");
            assert!(pool.contains_key(&uri2_str), "pool should contain uri2");

            let vec1 = pool.get(&uri1_str).unwrap().lock().await;
            let vec2 = pool.get(&uri2_str).unwrap().lock().await;
            assert_eq!(vec1.len(), 1, "uri1 should have 1 stream");
            assert_eq!(vec2.len(), 1, "uri2 should have 1 stream");
        }

        // Wait for streams to expire
        sleep(idle_timeout() + Duration::from_millis(100)).await;

        // Manually trigger one maintenance cycle
        {
            let mut pool = POOL.lock().await;
            for (_uri, vec_mutex) in pool.iter_mut() {
                let mut vec = vec_mutex.lock().await;
                vec.retain(|entry| entry.last_used.elapsed() <= idle_timeout());
            }
        }

        // Verify expired streams were cleaned up
        {
            let pool = POOL.lock().await;
            if let Some(vec_mutex) = pool.get(&uri1_str) {
                let vec1 = vec_mutex.lock().await;
                assert_eq!(
                    vec1.len(),
                    0,
                    "expired streams should be cleaned up for uri1"
                );
            }
            if let Some(vec_mutex) = pool.get(&uri2_str) {
                let vec2 = vec_mutex.lock().await;
                assert_eq!(
                    vec2.len(),
                    0,
                    "expired streams should be cleaned up for uri2"
                );
            }
        }
    }
}
