use crate::error::{Error, Result};

use std::io::SeekFrom;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;

#[cfg(test)]
use parquet::format::FileMetaData;

/// Parquet file footer size.
const FOOTER_SIZE: u64 = 8;
/// Parquet file magic bytes ("PAR1").
const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

/// Get serialized uncompressed parquet metadata from the given local filepath.
/// TODO(hjiang): Currently it only supports local filepath.
pub(crate) async fn get_parquet_serialized_metadata(filepath: &str) -> Result<Vec<u8>> {
    let mut file = tokio::fs::File::open(filepath).await?;

    // Validate file size.
    let file_len = file.metadata().await?.len();
    if file_len < FOOTER_SIZE {
        return Err(Error::invalid_argument(format!(
            "File {filepath} is too small to be parquet"
        )));
    }

    // Read last 8 bytes (metadata length + magic bytes).
    file.seek(SeekFrom::End(-(FOOTER_SIZE as i64))).await?;
    let mut footer = [0u8; FOOTER_SIZE as usize];
    file.read_exact(&mut footer).await?;

    // Validate magic bytes.
    if &footer[4..] != PARQUET_MAGIC {
        return Err(Error::data_corruption(format!(
            "File {filepath} magic bytes are corrupted"
        )));
    }

    // Parse metadata length.
    let metadata_len = u32::from_le_bytes([footer[0], footer[1], footer[2], footer[3]]) as u64;

    // File metadata length validation.
    if metadata_len + FOOTER_SIZE > file_len {
        return Err(Error::data_corruption(format!(
            "File {filepath} metadata length is {metadata_len}, file size is {file_len}"
        )));
    }

    // Seek to metadata start and read.
    let metadata_start = file_len - FOOTER_SIZE - metadata_len;
    file.seek(SeekFrom::Start(metadata_start)).await?;

    let mut buf = vec![0u8; metadata_len as usize];
    file.read_exact(&mut buf).await?;

    Ok(buf)
}

#[cfg(test)]
pub(crate) fn deserialize_parquet_metadata(bytes: &[u8]) -> FileMetaData {
    use parquet::thrift::TSerializable;
    use thrift::protocol::TCompactInputProtocol;
    use thrift::transport::TBufferChannel;

    let mut chan = TBufferChannel::with_capacity(bytes.len(), /*write_capacity=*/ 0);
    chan.set_readable_bytes(bytes);
    let mut proto = TCompactInputProtocol::new(chan);
    FileMetaData::read_from_in_protocol(&mut proto).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File as StdFile;

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::format::{FileMetaData, Statistics};
    use tempfile::tempdir;

    // Util function to get min and max.
    fn stats_min_max_i32(stats: &Statistics) -> Option<(i32, i32)> {
        let min_bytes = stats.min_value.as_ref().or(stats.min.as_ref())?;
        let max_bytes = stats.max_value.as_ref().or(stats.max.as_ref())?;

        if min_bytes.len() != 4 || max_bytes.len() != 4 {
            return None;
        }
        let min = i32::from_le_bytes([min_bytes[0], min_bytes[1], min_bytes[2], min_bytes[3]]);
        let max = i32::from_le_bytes([max_bytes[0], max_bytes[1], max_bytes[2], max_bytes[3]]);
        Some((min, max))
    }

    #[tokio::test]
    async fn test_get_parquet_serialized_metadata_basic_stats() {
        let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
        let data = Int32Array::from(vec![Some(1), Some(2), Some(2), Some(5), None]);
        let batch = RecordBatch::try_new(
            std::sync::Arc::new(schema.clone()),
            vec![std::sync::Arc::new(data)],
        )
        .unwrap();

        let tmp_dir = tempdir().unwrap();
        let parquet_path = format!("{}/test.parquet", tmp_dir.path().to_str().unwrap());

        {
            let file = StdFile::create(&parquet_path).unwrap();
            let mut writer =
                ArrowWriter::try_new(file, std::sync::Arc::new(schema), /*prop=*/ None).unwrap();
            writer.write(&batch).unwrap();
            let _file_metadata = writer.close().unwrap();
        }
        let buf = get_parquet_serialized_metadata(&parquet_path)
            .await
            .unwrap();
        let file_md: FileMetaData = deserialize_parquet_metadata(&buf[..]);

        assert_eq!(file_md.num_rows, 5);
        assert_eq!(file_md.row_groups.len(), 1);
        let rg = &file_md.row_groups[0];
        assert_eq!(rg.columns.len(), 1);
        let col = &rg.columns[0];
        let meta = col.meta_data.as_ref().unwrap();
        let stats = meta.statistics.as_ref().unwrap();
        if let Some(nulls) = stats.null_count {
            assert_eq!(nulls, 1);
        } else {
            panic!("expected null_count in column statistics");
        }

        let (min, max) = stats_min_max_i32(stats).unwrap();
        assert_eq!(min, 1);
        assert_eq!(max, 5);
    }
}
