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
    create_snapshot(database: String, table: String, lsn: u64) -> Result<()>;
    create_table(database: String, table: String, src: String, src_uri: String, table_config: String) -> Result<()>;
    drop_table(database: String, table: String) -> Result<()>;
    get_parquet_metadata(data_file: String) -> Result<Vec<u8>>;
    get_table_schema(database: String, table: String) -> Result<Vec<u8>>;
    list_tables() -> Result<Vec<Table>>;
    load_files(database: String, table: String, files: Vec<String>) -> Result<()>;
    optimize_table(database: String, table: String, mode: String) -> Result<()>;
    scan_table_begin(database: String, table: String, lsn: u64) -> Result<Vec<u8>>;
    scan_table_end(database: String, table: String) -> Result<()>;
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
    pub database: String,
    pub table: String,
    pub cardinality: u64,
    pub commit_lsn: u64,
    pub flush_lsn: Option<u64>,
    pub iceberg_warehouse_location: String,
}
