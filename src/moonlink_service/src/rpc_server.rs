use crate::{error::Error, Result};
use arrow_ipc::writer::StreamWriter;
use moonlink_backend::MoonlinkBackend;
use moonlink_error::{ErrorStatus, ErrorStruct};
use moonlink_rpc::{read, write, Request, RpcResult, Table};
use std::collections::HashMap;
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

fn into_error_struct<E: Into<anyhow::Error>>(e: E) -> ErrorStruct {
    ErrorStruct::new("backend error".to_string(), ErrorStatus::Permanent).with_source(e)
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
                let res: RpcResult<()> = backend
                    .create_snapshot(database, table, lsn)
                    .await
                    .map_err(into_error_struct);
                write(&mut stream, &res).await?;
            }
            Request::CreateTable {
                database,
                table,
                src,
                src_uri,
                table_config,
            } => {
                // Use default mooncake config, and local filesystem for storage layer.
                let res: RpcResult<()> = backend
                    .create_table(
                        database,
                        table,
                        src,
                        src_uri,
                        table_config,
                        None, /* input_database */
                    )
                    .await
                    .map_err(into_error_struct);
                write(&mut stream, &res).await?;
            }
            Request::DropTable { database, table } => {
                let res: RpcResult<()> = backend
                    .drop_table(database, table)
                    .await
                    .map_err(into_error_struct);

                write(&mut stream, &res).await?;
            }
            Request::GetParquetMetadatas { data_files } => {
                let metadata_res: RpcResult<Vec<Vec<u8>>> = backend
                    .get_parquet_metadatas(data_files)
                    .await
                    .map_err(into_error_struct);
                write(&mut stream, &metadata_res).await?;
            }
            Request::GetTableSchema { database, table } => {
                let result: anyhow::Result<Vec<u8>> = async {
                    let schema = backend.get_table_schema(database, table).await?;
                    let writer = StreamWriter::try_new(Vec::new(), &schema)?;
                    Ok(writer.into_inner()?)
                }
                .await;
                let res: RpcResult<Vec<u8>> = result.map_err(into_error_struct);
                write(&mut stream, &res).await?;
            }
            Request::ListTables {} => {
                let tables_res = backend.list_tables().await;
                let tables_res: RpcResult<Vec<Table>> = tables_res
                    .map(|tables| {
                        tables
                            .into_iter()
                            .map(|table| Table {
                                database: table.database,
                                table: table.table,
                                cardinality: table.cardinality,
                                commit_lsn: table.commit_lsn,
                                flush_lsn: table.flush_lsn,
                                iceberg_warehouse_location: table.iceberg_warehouse_location,
                            })
                            .collect()
                    })
                    .map_err(into_error_struct);
                write(&mut stream, &tables_res).await?;
            }
            Request::OptimizeTable {
                database,
                table,
                mode,
            } => {
                let res: RpcResult<()> = backend
                    .optimize_table(database, table, &mode)
                    .await
                    .map_err(into_error_struct);
                write(&mut stream, &res).await?;
            }
            Request::ScanTableBegin {
                database,
                table,
                lsn,
            } => {
                match backend
                    .scan_table(database.to_string(), table.to_string(), Some(lsn))
                    .await
                    .map_err(into_error_struct)
                {
                    Ok(state) => {
                        let res: RpcResult<Vec<u8>> = Ok(state.data.clone());
                        write(&mut stream, &res).await?;
                        assert!(map.insert((database, table), state).is_none());
                    }
                    Err(err) => {
                        let res: RpcResult<Vec<u8>> = Err(err);
                        write(&mut stream, &res).await?;
                    }
                }
            }
            Request::ScanTableEnd { database, table } => {
                assert!(map.remove(&(database, table)).is_some());
                write(&mut stream, &RpcResult::<()>::Ok(())).await?;
            }
            Request::LoadFiles {
                database,
                table,
                files,
            } => {
                let res: RpcResult<()> = backend
                    .load_files(database, table, files)
                    .await
                    .map_err(into_error_struct);
                write(&mut stream, &res).await?;
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
