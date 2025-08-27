use crate::{error::Error, Result};
use arrow_ipc::writer::StreamWriter;
use moonlink_backend::MoonlinkBackend;
use moonlink_rpc::{read, write, Request, Table};
use std::collections::HashMap;
use std::error::Error as _;
use std::io::ErrorKind::{BrokenPipe, ConnectionReset, UnexpectedEof};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, UnixListener};
use tracing::info;

fn is_disconnect(io: &std::io::Error) -> bool {
    matches!(io.kind(), BrokenPipe | ConnectionReset | UnexpectedEof)
}

fn is_closed_connection(err: &Error) -> bool {
    // Direct IO error path
    if let Error::Io(error_struct) = err {
        if let Some(io_err) = error_struct
            .source()
            .and_then(|e| e.downcast_ref::<std::io::Error>())
        {
            return is_disconnect(io_err);
        }
    }

    // RPC wraps an RPC error which can wrap an IO error
    if let Error::Rpc(error_struct) = err {
        if let Some(moonlink_rpc::Error::Io(inner)) = error_struct
            .source()
            .and_then(|e| e.downcast_ref::<moonlink_rpc::Error>())
        {
            if let Some(io_err) = inner
                .source()
                .and_then(|e| e.downcast_ref::<std::io::Error>())
            {
                return is_disconnect(io_err);
            }
        }
    }

    false
}

/// Start the Unix socket RPC server and serve requests until the task is aborted.
pub async fn start_unix_server(
    backend: Arc<MoonlinkBackend>,
    socket_path: std::path::PathBuf,
) -> Result<()> {
    if fs::metadata(&socket_path).await.is_ok() {
        fs::remove_file(&socket_path).await?;
    }
    let listener = UnixListener::bind(&socket_path)?;
    info!(
        "Moonlink RPC server listening on Unix socket: {:?}",
        socket_path
    );

    loop {
        let (stream, _addr) = listener.accept().await?;
        let backend = Arc::clone(&backend);
        tokio::spawn(async move {
            match handle_stream(backend, stream).await {
                Err(e) if is_closed_connection(&e) => {}
                Err(e) => panic!("Unexpected Unix RPC server error: {e}"),
                Ok(()) => {}
            }
        });
    }
}

/// Start the TCP socket RPC server and serve requests until the task is aborted.
pub async fn start_tcp_server(backend: Arc<MoonlinkBackend>, addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Moonlink RPC server listening on TCP: {}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let backend = Arc::clone(&backend);
        tokio::spawn(async move {
            match handle_stream(backend, stream).await {
                Err(e) if is_closed_connection(&e) => {}
                Err(e) => panic!("Unexpected TCP RPC server error: {e}"),
                Ok(()) => {}
            }
        });
    }
}

async fn handle_stream<S>(backend: Arc<MoonlinkBackend>, mut stream: S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut map = HashMap::new();
    loop {
        let request = read(&mut stream).await?;
        match request {
            Request::CreateSnapshot {
                database,
                table,
                lsn,
            } => {
                backend.create_snapshot(database, table, lsn).await?;
                write(&mut stream, &()).await?;
            }
            Request::CreateTable {
                database,
                table,
                src,
                src_uri,
                table_config,
            } => {
                // Use default mooncake config, and local filesystem for storage layer.
                backend
                    .create_table(
                        database,
                        table,
                        src,
                        src_uri,
                        table_config,
                        None, /* input_database */
                    )
                    .await?;
                write(&mut stream, &()).await?;
            }
            Request::DropTable { database, table } => {
                backend.drop_table(database, table).await?;
                write(&mut stream, &()).await?;
            }
            Request::GetParquetMetadata { data_file } => {
                let metadata = backend.get_parquet_metadata(data_file).await?;
                write(&mut stream, &metadata).await?;
            }
            Request::GetTableSchema { database, table } => {
                let database = backend.get_table_schema(database, table).await?;
                let writer = StreamWriter::try_new(vec![], &database)?;
                let data = writer.into_inner()?;
                write(&mut stream, &data).await?;
            }
            Request::ListTables {} => {
                let tables = backend.list_tables().await?;
                let tables: Vec<Table> = tables
                    .into_iter()
                    .map(|table| Table {
                        database: table.database,
                        table: table.table,
                        cardinality: table.cardinality,
                        commit_lsn: table.commit_lsn,
                        flush_lsn: table.flush_lsn,
                        iceberg_warehouse_location: table.iceberg_warehouse_location,
                    })
                    .collect();
                write(&mut stream, &tables).await?;
            }
            Request::OptimizeTable {
                database,
                table,
                mode,
            } => {
                backend.optimize_table(database, table, &mode).await?;
                write(&mut stream, &()).await?;
            }
            Request::ScanTableBegin {
                database,
                table,
                lsn,
            } => {
                let state = backend
                    .scan_table(database.to_string(), table.to_string(), Some(lsn))
                    .await?;
                write(&mut stream, &state.data).await?;
                assert!(map.insert((database, table), state).is_none());
            }
            Request::ScanTableEnd { database, table } => {
                assert!(map.remove(&(database, table)).is_some());
                write(&mut stream, &()).await?;
            }
            Request::LoadFiles {
                database,
                table,
                files,
            } => {
                backend.load_files(database, table, files).await?;
                write(&mut stream, &()).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::rpc_server::start_unix_server;
    use moonlink_backend::MoonlinkBackend;
    use moonlink_metadata_store::SqliteMetadataStore;
    use serial_test::serial;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc as StdArc,
    };
    use tempfile::TempDir;
    use tokio::net::UnixStream;

    #[tokio::test]
    #[serial]
    async fn unix_server_ignores_client_disconnect() {
        // Capture panics from background tasks
        let panic_count = StdArc::new(AtomicUsize::new(0));
        let prev_hook = std::panic::take_hook();
        {
            let panic_count = StdArc::clone(&panic_count);
            std::panic::set_hook(Box::new(move |_| {
                panic_count.fetch_add(1, Ordering::SeqCst);
            }));
        }

        let tempdir = TempDir::new().unwrap();
        let base_path = tempdir.path().to_str().unwrap().to_string();
        let sqlite_store = SqliteMetadataStore::new_with_directory(&base_path)
            .await
            .unwrap();
        let backend = MoonlinkBackend::new(base_path.clone(), None, Box::new(sqlite_store))
            .await
            .unwrap();

        let socket_path = tempdir.path().join("moonlink_test.sock");
        let server_handle = tokio::spawn({
            let backend = std::sync::Arc::new(backend);
            let socket_path = socket_path.clone();
            async move {
                // Ignore the result since we abort the task at the end of the test
                let _ = start_unix_server(backend, socket_path).await;
            }
        });

        // Wait for the socket file to appear
        for _ in 0..50 {
            if tokio::fs::metadata(&socket_path).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Connect and immediately drop to simulate client closing connection
        let stream = UnixStream::connect(&socket_path).await.unwrap();
        drop(stream);

        // Give the server a brief moment to process the disconnect
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Ensure no panic occurred
        assert_eq!(panic_count.load(Ordering::SeqCst), 0);

        // Cleanup
        server_handle.abort();
        let _ = server_handle.await;
        std::panic::set_hook(prev_hook);
    }
}
