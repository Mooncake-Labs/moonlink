mod error;

pub use error::{Error, Result};
use moonlink_backend::MoonlinkBackend;
use moonlink_metadata_store::SqliteMetadataStore;
use moonlink_rpc::{read, write, Request};
use std::collections::HashMap;
use std::io::ErrorKind::{BrokenPipe, ConnectionReset, UnexpectedEof};
use std::sync::Arc;
use tokio::fs;
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{signal, SignalKind};

pub async fn start(base_path: String) -> Result<()> {
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let sqlite_metadata_accessor = SqliteMetadataStore::new_with_directory(&base_path)
        .await
        .unwrap();
    let backend =
        MoonlinkBackend::new(base_path.clone(), Box::new(sqlite_metadata_accessor)).await?;
    let backend = Arc::new(backend);
    let socket_path = std::path::PathBuf::from(base_path).join("moonlink.sock");
    if fs::metadata(&socket_path).await.is_ok() {
        fs::remove_file(&socket_path).await?;
    }
    let listener = UnixListener::bind(&socket_path)?;
    loop {
        tokio::select! {
            _ = sigterm.recv() => break,
            Ok((stream, _addr)) = listener.accept() => {
                let backend = Arc::clone(&backend);
                tokio::spawn(async move {
                    match handle_stream(backend, stream).await {
                        Err(Error::Rpc(moonlink_rpc::Error::Io(e)))
                            if matches!(e.kind(), BrokenPipe | ConnectionReset | UnexpectedEof) => {}
                        Err(e) => panic!("{e}"),
                        Ok(()) => {}
                    }
                });
            }
        }
    }
    Ok(())
}

async fn handle_stream(
    backend: Arc<MoonlinkBackend<u32, u32>>,
    mut stream: UnixStream,
) -> Result<()> {
    let mut map = HashMap::new();
    loop {
        match read(&mut stream).await? {
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
            } => {
                backend
                    .create_table(database_id, table_id, src, src_uri)
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
                map.insert((database_id, table_id), state);
            }
            Request::ScanTableEnd {
                database_id,
                table_id,
            } => {
                map.remove(&(database_id, table_id));
                write(&mut stream, &()).await?;
            }
        }
    }
}
