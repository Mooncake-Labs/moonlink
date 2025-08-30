use std::{collections::HashMap, sync::LazyLock};
use tokio::{net::UnixStream, sync::Mutex};

#[derive(Debug)]
pub struct PooledStream {
    pub uri: String,
    pub stream: UnixStream,
}

impl PooledStream {
    pub fn new(uri: String, stream: UnixStream) -> Self {
        PooledStream { uri, stream }
    }
}

static POOL: LazyLock<Mutex<HashMap<String, Mutex<Vec<UnixStream>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) async fn get_stream(uri: &str) -> crate::Result<PooledStream> {
    let mut pool = POOL.lock().await;
    if let Some(mutex_vec) = pool.get_mut(uri) {
        let mut vec = mutex_vec.lock().await;
        if let Some(stream) = vec.pop() {
            return Ok(PooledStream::new(uri.to_string(), stream));
        }
    }
    let stream = UnixStream::connect(uri).await?;
    Ok(PooledStream::new(uri.to_string(), stream))
}

pub(crate) async fn return_stream(pooled_stream: PooledStream) {
    let mut pool = POOL.lock().await;

    pool.entry(pooled_stream.uri.clone())
        .or_default()
        .lock()
        .await
        .push(pooled_stream.stream);
}

#[cfg(test)]
mod tests {
    use crate::connection_pool::{get_stream, return_stream};
    use tempfile::tempdir;
    use tokio::net::UnixListener;

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
        return_stream(stream1).await;

        let stream2 = get_stream(&uri_str).await.expect("should reuse from pool");
        assert!(stream2.stream.into_std().is_ok());
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

        let stream1 = stream1.expect("connect URI1");
        let stream2 = stream2.expect("connect URI2");

        let addr1 = stream1.stream.peer_addr().unwrap();
        let addr2 = stream2.stream.peer_addr().unwrap();

        drop(stream1);
        drop(stream2);

        // Retrieve streams for both URIs again; should reuse connections from the pool
        let (stream1b, stream2b) = tokio::join!(get_stream(uri1), get_stream(uri2));

        let addr1b = stream1b.unwrap().stream.peer_addr().unwrap();
        let addr2b = stream2b.unwrap().stream.peer_addr().unwrap();

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
}
