use crate::{error::Error, Result};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use moonlink_backend::MoonlinkBackend;
use moonlink_error::{ErrorStatus, ErrorStruct};
use moonlink_rpc::{
    read, write, CreateSnapshotResponse, CreateTableResponse, DropTableResponse,
    GetParquetMetadatasResponse, GetTableSchemaResponse, ListTablesResponse, LoadFilesResponse,
    OptimizeTableResponse, Request, RpcResult, ScanTableBeginResponse, ScanTableEndResponse, Table,
};
use std::collections::HashMap;
use std::io::ErrorKind::{BrokenPipe, ConnectionReset, UnexpectedEof};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::net::UnixListener;
use tracing::{debug, info, warn};

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
                // One misbehaving client must never take the service down; just drop it.
                Err(e) => warn!("closing RPC connection after error: {e}"),
                Ok(()) => {}
            }
        });
    }
}

/// Bound on concurrently open scans of one table per connection.
const MAX_OPEN_SCANS_PER_TABLE: usize = 64;

/// Largest request frame accepted. Real requests are a few hundred bytes; the cap stops a
/// garbage length prefix from allocating up to 4 GiB.
const MAX_REQUEST_FRAME_BYTES: usize = 64 << 20;

enum Incoming {
    Request(Request),
    /// A complete frame that does not decode (unknown variant, client/server version skew).
    /// Framing is still in sync, so the server can answer with an error and continue.
    Malformed(moonlink_rpc::Error),
}

/// Read one length-prefixed request frame, then decode it with `moonlink_rpc::read` so the wire
/// format stays defined in one place.
async fn read_request<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Incoming> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes).await?;
    let len = u32::from_ne_bytes(len_bytes) as usize;
    if len > MAX_REQUEST_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("request frame of {len} bytes exceeds {MAX_REQUEST_FRAME_BYTES}"),
        )
        .into());
    }
    let mut frame = Vec::with_capacity(4 + len);
    frame.extend_from_slice(&len_bytes);
    frame.resize(4 + len, 0);
    reader.read_exact(&mut frame[4..]).await?;
    match read::<_, Request>(&mut frame.as_slice()).await {
        Ok(request) => Ok(Incoming::Request(request)),
        Err(e) => Ok(Incoming::Malformed(e)),
    }
}

/// Run a request that may wait on replication progress, but give up as soon as the client goes
/// away. Requests are strictly request/response, so nothing may arrive while one is pending: EOF
/// means the client is gone, and any byte breaks framing. Returns `None` if the connection should
/// be closed.
async fn unless_client_disconnects<R, F>(
    reader: &mut R,
    what: &str,
    request: F,
) -> Option<F::Output>
where
    R: AsyncRead + Unpin,
    F: std::future::Future,
{
    let mut probe = [0u8; 1];
    tokio::select! {
        res = request => Some(res),
        peer = reader.read(&mut probe) => {
            match peer {
                Ok(0) | Err(_) => debug!("client disconnected during {what}"),
                Ok(_) => warn!("unexpected bytes while {what} is pending; closing connection"),
            }
            None
        }
    }
}

fn unsupported(rpc: &str) -> ErrorStruct {
    ErrorStruct::new(
        format!("{rpc} is not supported by this moonlink build"),
        ErrorStatus::Permanent,
    )
}

/// Arrow IPC stream (schema message + end-of-stream marker) returned by GetTableSchema; duckdb_mooncake decodes it with nanoarrow.
pub(crate) fn encode_table_schema(schema: &Schema) -> anyhow::Result<Vec<u8>> {
    let writer = StreamWriter::try_new(Vec::new(), schema)?;
    Ok(writer.into_inner()?)
}

fn into_error_struct<E: Into<anyhow::Error>>(e: E) -> ErrorStruct {
    let e = e.into();
    // An errored mirror's message tells users what to do, so it goes into the top-level message.
    if let Some(errored) = e
        .downcast_ref::<moonlink_backend::Error>()
        .and_then(moonlink_backend::Error::table_errored)
    {
        return ErrorStruct::new(errored.message.clone(), errored.status).with_source(e);
    }
    // So is an LSN-wait timeout's (it names the LSNs and likely causes), and it is retryable.
    if let Some(moonlink_backend::Error::LsnWaitTimeout(timeout)) =
        e.downcast_ref::<moonlink_backend::Error>()
    {
        return ErrorStruct::new(timeout.message.clone(), timeout.status).with_source(e);
    }
    ErrorStruct::new("backend error".to_string(), ErrorStatus::Permanent).with_source(e)
}

async fn handle_stream<S>(backend: Arc<MoonlinkBackend>, stream: S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Split so a pending ScanTableBegin can notice the peer closing while it waits for an LSN.
    let (mut reader, mut stream) = tokio::io::split(stream);
    // Open scans per table. A client may overlap scans of one table (two transactions on one
    // connection); each ScanTableEnd releases one of them, which one does not matter.
    let mut open_scans: HashMap<(String, String), Vec<_>> = HashMap::new();
    loop {
        let request = match read_request(&mut reader).await? {
            Incoming::Request(request) => request,
            Incoming::Malformed(e) => {
                warn!("malformed RPC request: {e}");
                // `Err` encodes identically for every response type, so any client can decode it.
                let res: RpcResult<()> = Err(ErrorStruct::new(
                    "malformed or unsupported request (client/server version mismatch?)"
                        .to_string(),
                    ErrorStatus::Permanent,
                )
                .with_source(e));
                write(&mut stream, &res).await?;
                continue;
            }
        };
        match request {
            Request::CreateSnapshot {
                database,
                table,
                lsn,
            } => {
                let what = format!("CreateSnapshot for {database}.{table}");
                let Some(res) = unless_client_disconnects(
                    &mut reader,
                    &what,
                    backend.create_snapshot(database, table, lsn),
                )
                .await
                else {
                    return Ok(());
                };
                let res: RpcResult<CreateSnapshotResponse> = res.map_err(into_error_struct);
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
                let res: RpcResult<CreateTableResponse> = backend
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
                let res: RpcResult<DropTableResponse> = backend
                    .drop_table(database, table)
                    .await
                    .map_err(into_error_struct);

                write(&mut stream, &res).await?;
            }
            // Kept for wire compatibility; no client uses it.
            Request::GetParquetMetadatas { .. } => {
                let res: RpcResult<GetParquetMetadatasResponse> =
                    Err(unsupported("get_parquet_metadatas"));
                write(&mut stream, &res).await?;
            }
            Request::GetTableSchema { database, table } => {
                let result: anyhow::Result<Vec<u8>> = async {
                    let schema = backend.get_table_schema(database, table).await?;
                    encode_table_schema(&schema)
                }
                .await;
                let res: RpcResult<GetTableSchemaResponse> = result.map_err(into_error_struct);
                write(&mut stream, &res).await?;
            }
            Request::ListTables {} => {
                let tables_res = backend.list_tables().await;
                let tables_res: RpcResult<ListTablesResponse> = tables_res
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
                let res: RpcResult<OptimizeTableResponse> = backend
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
                let what = format!("ScanTableBegin for {database}.{table}");
                let Some(scan_result) = unless_client_disconnects(
                    &mut reader,
                    &what,
                    backend.scan_table(database.to_string(), table.to_string(), Some(lsn)),
                )
                .await
                else {
                    return Ok(());
                };
                match scan_result.map_err(into_error_struct) {
                    Ok(state) => {
                        let res: RpcResult<ScanTableBeginResponse> = Ok(state.data.clone());
                        // Register before replying so the files stay pinned while the client reads them.
                        let scans = open_scans
                            .entry((database.clone(), table.clone()))
                            .or_default();
                        if scans.len() >= MAX_OPEN_SCANS_PER_TABLE {
                            // A client that never ends its scans would otherwise pin files forever.
                            warn!("more than {MAX_OPEN_SCANS_PER_TABLE} open scans of {database}.{table} on one connection; releasing the oldest");
                            scans.remove(0);
                        }
                        scans.push(state);
                        write(&mut stream, &res).await?;
                    }
                    Err(err) => {
                        let res: RpcResult<ScanTableBeginResponse> = Err(err);
                        write(&mut stream, &res).await?;
                    }
                }
            }
            Request::ScanTableEnd { database, table } => {
                // Idempotent: duckdb_mooncake ignores ScanTableEnd errors anyway.
                let key = (database, table);
                match open_scans.get_mut(&key) {
                    Some(scans) => {
                        scans.pop();
                        if scans.is_empty() {
                            open_scans.remove(&key);
                        }
                    }
                    None => debug!(
                        "ScanTableEnd for {}.{} without a matching ScanTableBegin",
                        key.0, key.1
                    ),
                }
                write(&mut stream, &RpcResult::<ScanTableEndResponse>::Ok(())).await?;
            }
            // Kept for wire compatibility (pg_mooncake mooncake.load_files); loading files is not implemented.
            Request::LoadFiles { .. } => {
                let res: RpcResult<LoadFilesResponse> = Err(unsupported("load_files"));
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

    /// A client that disconnects while its ScanTableBegin or CreateSnapshot waits for an LSN must
    /// not leave the connection task waiting until the LSN wait times out.
    #[cfg(feature = "postgres-integration")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn pending_request_is_cancelled_when_client_disconnects() {
        use crate::test_utils::pg;
        use std::time::Duration;

        let table = "rpc_disconnect_test";
        let pg_client = pg::connect().await;
        pg::create_source_table(&pg_client, table, 1).await;
        let tempdir = TempDir::new().unwrap();
        let base_path = tempdir.path().to_str().unwrap().to_string();
        let sqlite_store = SqliteMetadataStore::new_with_directory(&base_path)
            .await
            .unwrap();
        let backend = std::sync::Arc::new(
            MoonlinkBackend::new(base_path, None, Box::new(sqlite_store))
                .await
                .unwrap(),
        );
        backend
            .create_table(
                "postgres".to_string(),
                table.to_string(),
                format!("public.{table}"),
                pg::database_uri(),
                "{}".to_string(),
                None,
            )
            .await
            .unwrap();
        let far_lsn = pg::current_wal_lsn(&pg_client).await + (1u64 << 40);

        let requests = [
            moonlink_rpc::Request::ScanTableBegin {
                database: "postgres".to_string(),
                table: table.to_string(),
                lsn: far_lsn,
            },
            moonlink_rpc::Request::CreateSnapshot {
                database: "postgres".to_string(),
                table: table.to_string(),
                lsn: far_lsn,
            },
        ];
        for request in requests {
            let (mut client, server) = tokio::io::duplex(1 << 16);
            let connection = tokio::spawn(super::handle_stream(backend.clone(), server));
            moonlink_rpc::write(&mut client, &request).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
            drop(client);

            let res = tokio::time::timeout(Duration::from_secs(5), connection)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "connection task kept waiting after the client disconnected: {request:?}"
                    )
                });
            assert!(res.unwrap().is_ok());
        }

        backend
            .drop_table("postgres".to_string(), table.to_string())
            .await
            .unwrap();
        pg::drop_source_table(&pg_client, table).await;
    }
    /// An LSN-wait timeout reaches the client with its message at top level and its Temporary
    /// status, and the connection stays usable.
    #[cfg(feature = "postgres-integration")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn lsn_wait_timeout_reaches_the_client_as_a_temporary_error() {
        use crate::test_utils::pg;
        use moonlink_error::ErrorStatus;
        use std::time::Duration;

        let table = "rpc_lsn_timeout_test";
        let pg_client = pg::connect().await;
        pg::create_source_table(&pg_client, table, 1).await;
        let tempdir = TempDir::new().unwrap();
        let base_path = tempdir.path().to_str().unwrap().to_string();
        let sqlite_store = SqliteMetadataStore::new_with_directory(&base_path)
            .await
            .unwrap();
        let backend = std::sync::Arc::new(
            MoonlinkBackend::new(base_path, None, Box::new(sqlite_store))
                .await
                .unwrap(),
        );
        backend.set_lsn_wait_timeout(Duration::from_millis(300));
        backend
            .create_table(
                "postgres".to_string(),
                table.to_string(),
                format!("public.{table}"),
                pg::database_uri(),
                "{}".to_string(),
                None,
            )
            .await
            .unwrap();
        let far_lsn = pg::current_wal_lsn(&pg_client).await + (1u64 << 40);

        let (mut client, server) = tokio::io::duplex(1 << 16);
        let connection = tokio::spawn(super::handle_stream(backend.clone(), server));
        let requests = [
            moonlink_rpc::Request::ScanTableBegin {
                database: "postgres".to_string(),
                table: table.to_string(),
                lsn: far_lsn,
            },
            moonlink_rpc::Request::CreateSnapshot {
                database: "postgres".to_string(),
                table: table.to_string(),
                lsn: far_lsn,
            },
        ];
        for request in requests {
            moonlink_rpc::write(&mut client, &request).await.unwrap();
            // Both responses encode `Err` identically.
            let res: moonlink_rpc::RpcResult<()> =
                tokio::time::timeout(Duration::from_secs(10), moonlink_rpc::read(&mut client))
                    .await
                    .unwrap()
                    .unwrap();
            let err = res.expect_err("waiting for an LSN replication never reaches must time out");
            assert_eq!(err.status, ErrorStatus::Temporary, "{request:?}: {err:?}");
            assert!(
                err.message.contains(&far_lsn.to_string()) && err.message != "backend error",
                "{request:?}: the timeout message must be the top-level message: {err:?}"
            );
        }
        moonlink_rpc::write(&mut client, &moonlink_rpc::Request::ListTables {})
            .await
            .unwrap();
        let tables: moonlink_rpc::RpcResult<Vec<moonlink_rpc::Table>> =
            moonlink_rpc::read(&mut client).await.unwrap();
        assert_eq!(tables.unwrap().len(), 1);
        drop(client);
        let _ = connection.await;

        backend
            .drop_table("postgres".to_string(), table.to_string())
            .await
            .unwrap();
        pg::drop_source_table(&pg_client, table).await;
    }
    /// optimize on an errored mirror answers with the mirror's "out of sync" message instead of
    /// panicking the connection task, and the connection keeps serving requests.
    #[cfg(feature = "postgres-integration")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn optimize_of_an_errored_mirror_returns_its_message() {
        use crate::test_utils::pg;
        use std::time::Duration;

        let table = "rpc_optimize_errored_test";
        let pg_client = pg::connect().await;
        pg::create_source_table(&pg_client, table, 3).await;
        let tempdir = TempDir::new().unwrap();
        let base_path = tempdir.path().to_str().unwrap().to_string();
        let sqlite_store = SqliteMetadataStore::new_with_directory(&base_path)
            .await
            .unwrap();
        let backend = std::sync::Arc::new(
            MoonlinkBackend::new(base_path, None, Box::new(sqlite_store))
                .await
                .unwrap(),
        );
        backend
            .create_table(
                "postgres".to_string(),
                table.to_string(),
                format!("public.{table}"),
                pg::database_uri(),
                "{}".to_string(),
                None,
            )
            .await
            .unwrap();
        backend
            .enter_errored_for_testing(
                "postgres".to_string(),
                table.to_string(),
                "source schema changed".to_string(),
            )
            .await
            .unwrap();

        let (mut client, server) = tokio::io::duplex(1 << 16);
        let connection = tokio::spawn(super::handle_stream(backend.clone(), server));
        for mode in ["data", "index", "full"] {
            let request = moonlink_rpc::Request::OptimizeTable {
                database: "postgres".to_string(),
                table: table.to_string(),
                mode: mode.to_string(),
            };
            moonlink_rpc::write(&mut client, &request).await.unwrap();
            let res: moonlink_rpc::RpcResult<()> =
                tokio::time::timeout(Duration::from_secs(10), moonlink_rpc::read(&mut client))
                    .await
                    .unwrap_or_else(|_| panic!("optimize {mode} did not answer"))
                    .unwrap_or_else(|e| panic!("optimize {mode}: connection broke: {e:?}"));
            let err = res.expect_err("optimize of an errored mirror must fail");
            assert!(
                err.message.contains("out of sync")
                    && err.message.contains("source schema changed"),
                "optimize {mode}: {err:?}"
            );
        }
        moonlink_rpc::write(&mut client, &moonlink_rpc::Request::ListTables {})
            .await
            .unwrap();
        let tables: moonlink_rpc::RpcResult<Vec<moonlink_rpc::Table>> =
            moonlink_rpc::read(&mut client).await.unwrap();
        assert_eq!(tables.unwrap().len(), 1);
        drop(client);
        assert!(connection.await.is_ok(), "connection task panicked");

        backend
            .drop_table("postgres".to_string(), table.to_string())
            .await
            .unwrap();
        pg::drop_source_table(&pg_client, table).await;
    }
}

#[cfg(test)]
mod error_tests {
    use super::into_error_struct;

    #[test]
    fn test_errored_mirror_message_is_the_wire_message() {
        let message = moonlink::Error::errored_table_message("db.public.t", "schema change");
        let core = moonlink::Error::table_errored(&message);
        let connector: moonlink_connectors::Error = core.clone().into();
        for backend in [
            moonlink_backend::Error::from(connector),
            moonlink_backend::Error::from(core),
        ] {
            let es = into_error_struct(backend);
            assert_eq!(es.message, message);
        }

        let other = moonlink_backend::Error::from(moonlink::Error::table_handler_stopped("gone"));
        assert_eq!(into_error_struct(other).message, "backend error");
    }
}
