mod error;

pub use error::{Error, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

macro_rules! rpcs {
    (
        $($func:ident($($name:ident: $type:ty),*) -> $res:ty;)*
    ) => {
        paste::paste! {
            #[derive(Debug, Serialize, Deserialize)]
            pub enum Request {
                $([<$func:camel>] {
                    $($name: $type),*
                },)*
            }

            $(pub async fn $func<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S, $($name: $type),*) -> Result<$res> {
                write(stream, &Request::[<$func:camel>] { $($name),* }).await?;
                read(stream).await
            })*
        }
    };
}

rpcs! {
    create_snapshot(database_id: u32, table_id: u32, lsn: u64) -> ();
    create_table(database_id: u32, table_id: u32, src: String, src_uri: String, table_config: String) -> ();
    drop_table(database_id: u32, table_id: u32) -> ();
    get_table_schema(database_id: u32, table_id: u32) -> Vec<u8>;
    list_tables() -> Vec<Table>;
    optimize_table(database_id: u32, table_id: u32, mode: String) -> ();
    scan_table_begin(database_id: u32, table_id: u32, lsn: u64) -> Vec<u8>;
    scan_table_end(database_id: u32, table_id: u32) -> ();
}

pub async fn write<W: AsyncWrite + Unpin, S: Serialize>(writer: &mut W, data: &S) -> Result<()> {
    let bytes = bincode::serde::encode_to_vec(data, BINCODE_CONFIG)?;
    let len = u32::try_from(bytes.len())?;
    writer.write_all(&len.to_ne_bytes()).await?;
    writer.write_all(&bytes).await?;
    Ok(())
}

pub async fn read<R: AsyncRead + Unpin, D: for<'de> Deserialize<'de>>(reader: &mut R) -> Result<D> {
    let mut buf = [0; 4];
    reader.read_exact(&mut buf).await?;
    let len = u32::from_ne_bytes(buf);
    let mut bytes = vec![0; len as usize];
    reader.read_exact(&mut bytes).await?;
    Ok(bincode::serde::decode_from_slice(&bytes, BINCODE_CONFIG)?.0)
}

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub database_id: u32,
    pub table_id: u32,
    pub commit_lsn: u64,
    pub flush_lsn: Option<u64>,
    pub iceberg_warehouse_location: String,
}
