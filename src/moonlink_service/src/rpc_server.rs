use crate::{error::Error, Result};
use arrow_ipc::writer::StreamWriter;
use moonlink_backend::MoonlinkBackend;
use moonlink_rpc::{read, write, Request, Table};
use std::collections::HashMap;
use std::error;
use std::io::ErrorKind::{BrokenPipe, ConnectionReset, UnexpectedEof};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, UnixListener};
use tracing::info;

/// Start the Unix socket RPC server and serve requests until the task is aborted.
pub async fn start_unix_server(
    backend: Arc<MoonlinkBackend<u32, u32>>,
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
                Err(Error::Rpc(moonlink_rpc::Error::Io(e)))
                    if error::Error::source(&e)
                        .and_then(|src| src.downcast_ref::<std::io::Error>())
                        .map(|io_err| {
                            matches!(io_err.kind(), BrokenPipe | ConnectionReset | UnexpectedEof)
                        })
                        .unwrap_or(false) => {}
                Err(e) => panic!("{e}"),
                Ok(()) => {}
            }
        });
    }
}

/// Start the TCP socket RPC server and serve requests until the task is aborted.
///
/// TODO(hjiang): Better error handling.
pub async fn start_tcp_server(
    backend: Arc<MoonlinkBackend<u32, u32>>,
    addr: SocketAddr,
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Moonlink RPC server listening on TCP: {}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let backend = Arc::clone(&backend);
        tokio::spawn(async move {
            match handle_stream(backend, stream).await {
                Err(Error::Rpc(moonlink_rpc::Error::Io(e)))
                    if error::Error::source(&e)
                        .and_then(|src| src.downcast_ref::<std::io::Error>())
                        .map(|io_err| {
                            matches!(io_err.kind(), BrokenPipe | ConnectionReset | UnexpectedEof)
                        })
                        .unwrap_or(false) => {}
                Err(e) => panic!("{e}"),
                Ok(()) => {}
            }
        });
    }
}

async fn handle_stream<S>(backend: Arc<MoonlinkBackend<u32, u32>>, mut stream: S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut map = HashMap::new();
    loop {
        let request = read(&mut stream).await?;
        match request {
            Request::CreateSnapshot {
                database_id,
                table_id,
                lsn,
            } => {
                backend
                    .create_snapshot(database_id, table_id, lsn)
                    .await
                    .unwrap();
                write(&mut stream, &()).await?;
            }
            Request::CreateTable {
                database_id,
                table_id,
                src,
                src_uri,
                table_config,
            } => {
                // Use default mooncake config, and local filesystem for storage layer.
                backend
                    .create_table(
                        database_id,
                        table_id,
                        src,
                        src_uri,
                        table_config,
                        None, /* input_schema */
                    )
                    .await
                    .unwrap();
                write(&mut stream, &()).await?;
            }
            Request::DropTable {
                database_id,
                table_id,
            } => {
                backend.drop_table(database_id, table_id).await;
                write(&mut stream, &()).await?;
            }
            Request::GetTableSchema {
                database_id,
                table_id,
            } => {
                let schema = backend.get_table_schema(database_id, table_id).await?;
                let writer = StreamWriter::try_new(vec![], &schema)?;
                let data = writer.into_inner()?;
                write(&mut stream, &data).await?;
            }
            Request::ListTables {} => {
                let tables = backend.list_tables().await?;
                let tables: Vec<Table> = tables
                    .into_iter()
                    .map(|table| Table {
                        database_id: table.database_id,
                        table_id: table.table_id,
                        commit_lsn: table.commit_lsn,
                        flush_lsn: table.flush_lsn,
                        iceberg_warehouse_location: table.iceberg_warehouse_location,
                    })
                    .collect();
                write(&mut stream, &tables).await?;
            }
            Request::OptimizeTable {
                database_id,
                table_id,
                mode,
            } => {
                backend
                    .optimize_table(database_id, table_id, &mode)
                    .await
                    .unwrap();
                write(&mut stream, &()).await?;
            }
            Request::ScanTableBegin {
                database_id,
                table_id,
                lsn,
            } => {
                let state = backend
                    .scan_table(database_id, table_id, Some(lsn))
                    .await
                    .unwrap();
                write(&mut stream, &state.data).await?;
                assert!(map.insert((database_id, table_id), state).is_none());
            }
            Request::ScanTableEnd {
                database_id,
                table_id,
            } => {
                assert!(map.remove(&(database_id, table_id)).is_some());
                write(&mut stream, &()).await?;
            }
        }
    }
}
