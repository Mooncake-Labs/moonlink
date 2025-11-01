use crate::error::{Error, Result};

use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use std::io::SeekFrom;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
/// Parquet file footer size.
const FOOTER_SIZE: u64 = 8;
/// Parquet file magic bytes ("PAR1").
const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

/// Get serialized uncompressed parquet metadata from the given local filepath.
/// TODO(hjiang): Currently it only supports local filepath.
pub(crate) async fn get_parquet_serialized_metadata(filepath: &str) -> Result<Vec<u8>> {
    let mut file = tokio::fs::File::open(&filepath)
        .await
        .map_err(|e| Error::io(format!("Failed to open file {filepath} with error {e:?}")))?;

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

#[allow(dead_code)]
pub(crate) fn deserialize_parquet_metadata(bytes: &[u8]) -> ParquetMetaData {
    ParquetMetaDataReader::decode_metadata(bytes).expect("Failed to decode metadata")
}
#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::metadata::ParquetMetaData;
    use parquet::file::statistics::Statistics;
    use std::fs::File as StdFile;
    use tempfile::tempdir;

    // // Util function to convert bytes to i32
    fn bytes_to_i32(bytes: &[u8]) -> i32 {
        let arr: [u8; 4] = bytes.try_into().expect("slice with incorrect length");
        i32::from_le_bytes(arr)
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
        let file_md: ParquetMetaData = deserialize_parquet_metadata(&buf[..]);

        assert_eq!(file_md.file_metadata().num_rows(), 5);
        assert_eq!(file_md.row_groups().len(), 1);
        assert_eq!(file_md.row_groups()[0].num_columns(), 1);
        let col_chunk: &Statistics = file_md
            .row_group(0)
            .column(0)
            .statistics()
            .expect("expected statistics");

        match *col_chunk {
            Statistics::Int32(ref stat) => {
                if let Some(min_bytes) = stat.min_bytes_opt() {
                    let min_val = bytes_to_i32(min_bytes);
                    assert_eq!(min_val, 1);
                }
                if let Some(max_bytes) = stat.max_bytes_opt() {
                    let max_val = bytes_to_i32(max_bytes);
                    assert_eq!(max_val, 5);
                }
            }
            _ => panic!("expected int32 statistics"),
        }
        let null_count_opt = col_chunk.null_count_opt().unwrap();
        assert_eq!(null_count_opt, 1);
    }
}
