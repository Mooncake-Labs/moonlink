use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use moonlink_error::{ErrorStatus, ErrorStruct};
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use crate::pg_replicate::conversions::table_row::TableRow;
use crate::Result;
use moonlink::row::RowValue;
use moonlink::{BatchIdCounter, ColumnStoreBuffer, DiskSliceWriterConfig, MooncakeTableConfig};

/// Configuration for initial-copy Parquet writing.
#[derive(Clone, Debug)]
pub struct InitialCopyWriterConfig {
    /// Target max file size in bytes before rotating to a new Parquet file.
    pub target_file_size_bytes: usize,
    /// Max number of rows per Arrow RecordBatch before flushing to the writer.
    pub max_rows_per_batch: usize,
    /// Number of parallel writer tasks to consume from the batch queue.
    pub num_writer_tasks: usize,
    /// Capacity of the bounded channel between producer and writers.
    pub batch_channel_capacity: usize,
}

impl Default for InitialCopyWriterConfig {
    fn default() -> Self {
        Self {
            // Align with disk slice writer default parquet file size
            target_file_size_bytes: DiskSliceWriterConfig::default_disk_slice_parquet_file_size(),
            // Align batch size with mooncake table default batch size
            max_rows_per_batch: MooncakeTableConfig::default_batch_size(),
            // Default to no parallelism
            num_writer_tasks: 1,
            batch_channel_capacity: 16,
        }
    }
}

/// Create a bounded channel for passing Arrow RecordBatches from table copy to writer.
pub fn create_batch_channel(capacity: usize) -> (BatchSender, BatchReceiver) {
    let (tx, rx) = mpsc::channel(capacity);
    (BatchSender(tx), BatchReceiver(rx))
}

/// Sending end of the batch channel.
#[derive(Clone)]
pub struct BatchSender(mpsc::Sender<RecordBatch>);

impl BatchSender {
    /// Send a RecordBatch to the writer. Returns Err if the receiver is closed.
    pub async fn send(&self, batch: RecordBatch) -> Result<()> {
        self.0.send(batch).await.map_err(|_| {
            crate::Error::MpscChannelSendError(ErrorStruct::new(
                "batch sender closed".to_string(),
                ErrorStatus::Permanent,
            ))
        })?;
        Ok(())
    }
}

/// Receiving end of the batch channel.
pub struct BatchReceiver(mpsc::Receiver<RecordBatch>);

impl BatchReceiver {
    /// Receive the next RecordBatch from the channel. Returns None if closed.
    pub async fn recv(&mut self) -> Option<RecordBatch> {
        self.0.recv().await
    }
}

/// Shared receiver wrapper to enable multiple writer tasks to consume from a single queue.
#[derive(Clone)]
pub struct SharedBatchReceiver(Arc<Mutex<BatchReceiver>>);

impl SharedBatchReceiver {
    pub fn new(rx: BatchReceiver) -> Self {
        Self(Arc::new(Mutex::new(rx)))
    }

    pub async fn recv(&self) -> Option<RecordBatch> {
        let mut guard = self.0.lock().await;
        guard.recv().await
    }
}

/// A batch builder that accumulates TableRow values from PG copy and produces RecordBatches.
///
/// Reuses moonlink::ColumnStoreBuffer to avoid duplicating Arrow builder logic.
/// Converts PG TableRow cells to RowValue and delegates to the internal buffer.
pub struct ArrowBatchBuilder {
    buffer: ColumnStoreBuffer,
}

impl ArrowBatchBuilder {
    pub fn new(schema: Arc<Schema>, max_rows: usize) -> Self {
        let batch_id_counter = Arc::new(BatchIdCounter::new(false)); // Temporary instance of batch id counter
        let buffer = ColumnStoreBuffer::new(schema, max_rows, batch_id_counter);
        Self { buffer }
    }

    /// Append a TableRow from PG copy. Returns an immediately-finished
    /// RecordBatch if the buffer is full, otherwise None.
    pub fn append_table_row(&mut self, table_row: TableRow) -> Result<Option<RecordBatch>> {
        // Convert TableRow cells to RowValue
        let row_values: Vec<RowValue> = table_row
            .values
            .into_iter()
            .map(|cell| cell.into())
            .collect();

        let (_batch_id, _row_offset, finished_batch) =
            self.buffer.append_initial_copy_row(row_values)?;

        Ok(finished_batch.map(|(_, batch)| batch.as_ref().clone()))
    }

    /// Finish the current batch and return a RecordBatch.
    pub fn finish(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self
            .buffer
            .finalize_current_batch()?
            .map(|(_, batch)| (*batch).clone()))
    }
}

/// Writes RecordBatches into Parquet files with rotation.
///
/// Implemented with parquet::arrow::AsyncArrowWriter and file rotation based on writer.memory_size().
pub struct ParquetFileWriter {
    pub output_dir: PathBuf,
    pub schema: Arc<Schema>,
    pub config: InitialCopyWriterConfig,
}

impl ParquetFileWriter {
    pub fn new(output_dir: PathBuf, schema: Arc<Schema>, config: InitialCopyWriterConfig) -> Self {
        Self {
            output_dir,
            schema,
            config,
        }
    }

    fn next_file_path(&self) -> PathBuf {
        let filename = format!("ic-{}.parquet", uuid::Uuid::now_v7());
        self.output_dir.join(filename)
    }

    /// Consume RecordBatches from a shared receiver and write Parquet files.
    /// Returns the list of file paths written by this worker.
    pub async fn write_from_shared(
        mut self,
        shared_rx: SharedBatchReceiver,
    ) -> Result<Vec<String>> {
        use moonlink::get_default_parquet_properties;
        use parquet::arrow::AsyncArrowWriter;

        let mut files_written: Vec<String> = Vec::new();
        let mut writer: Option<AsyncArrowWriter<tokio::fs::File>> = None;
        let mut current_file_path: Option<PathBuf> = None;

        while let Some(batch) = shared_rx.recv().await {
            if batch.num_columns() == 0 || batch.num_rows() == 0 {
                continue;
            }

            if writer.is_none() {
                let path = self.next_file_path();
                // Ensure directory exists
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                let file = tokio::fs::File::create(&path).await?;
                let props = get_default_parquet_properties();
                let w = AsyncArrowWriter::try_new(file, self.schema.clone(), Some(props))?;
                writer = Some(w);
                current_file_path = Some(path.clone());
            }

            let w = writer.as_mut().unwrap();
            w.write(&batch).await?;

            // Rotate when current writer exceeds target size.
            if w.memory_size() >= self.config.target_file_size_bytes {
                w.finish().await?;
                if let Some(p) = current_file_path.take() {
                    files_written.push(p.to_string_lossy().to_string());
                }
                writer = None;
            }
        }

        // Finalize any open writer.
        if let Some(mut w) = writer.take() {
            w.finish().await?;
            if let Some(p) = current_file_path.take() {
                files_written.push(p.to_string_lossy().to_string());
            }
        }

        Ok(files_written)
    }
}

// TODO: Add unit tests

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{ArrayRef, Int32Array};
    use std::path::Path;
    use std::sync::Arc;

    use crate::pg_replicate::conversions::Cell;

    fn int32_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]))
    }

    fn make_batch(values: &[i32]) -> RecordBatch {
        let arr = Int32Array::from(values.to_vec());
        RecordBatch::try_new(int32_schema(), vec![Arc::new(arr) as ArrayRef]).unwrap()
    }

    #[test]
    fn config_defaults_align_with_defaults() {
        let cfg = InitialCopyWriterConfig::default();
        assert_eq!(
            cfg.target_file_size_bytes,
            DiskSliceWriterConfig::default_disk_slice_parquet_file_size()
        );
        assert_eq!(
            cfg.max_rows_per_batch,
            MooncakeTableConfig::default_batch_size()
        );
        assert_eq!(cfg.num_writer_tasks, 1);
        assert_eq!(cfg.batch_channel_capacity, 16);
    }

    #[tokio::test]
    async fn batch_channel_send_and_recv() {
        let (tx, mut rx) = create_batch_channel(2);
        let b = make_batch(&[1, 2, 3]);
        tx.send(b.clone()).await.unwrap();
        let got = rx.recv().await.unwrap();
        assert_eq!(got.num_rows(), 3);
        assert_eq!(got.num_columns(), 1);

        // When sender is dropped, receiver should return None
        drop(tx);
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn batch_sender_errors_when_receiver_closed() {
        let (tx, rx) = create_batch_channel(1);
        drop(rx); // close receiver
        let res = tx.send(make_batch(&[1])).await;
        assert!(res.is_err());
    }

    #[test]
    fn arrow_batch_builder_rotation_on_capacity() {
        // max_rows = 2 -> third append should finalize first batch
        let schema = int32_schema();
        let mut builder = ArrowBatchBuilder::new(schema, 2);

        let row1 = TableRow {
            values: vec![Cell::I32(10)],
        };
        let row2 = TableRow {
            values: vec![Cell::I32(20)],
        };
        let row3 = TableRow {
            values: vec![Cell::I32(30)],
        };

        assert!(builder.append_table_row(row1).unwrap().is_none());
        assert!(builder.append_table_row(row2).unwrap().is_none());

        // Third append finalizes previous full batch (rows: 10, 20)
        let maybe_batch = builder.append_table_row(row3).unwrap();
        assert!(maybe_batch.is_some());
        let batch = maybe_batch.unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
        let column = batch.column(0);
        let col = column.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(col.value(0), 10);
        assert_eq!(col.value(1), 20);
    }

    #[test]
    fn arrow_batch_builder_finish_returns_remaining_and_then_none() {
        let schema = int32_schema();
        let mut builder = ArrowBatchBuilder::new(schema, 4);

        // Append fewer rows than capacity
        builder
            .append_table_row(TableRow {
                values: vec![Cell::I32(1)],
            })
            .unwrap();
        builder
            .append_table_row(TableRow {
                values: vec![Cell::I32(2)],
            })
            .unwrap();

        // Finish should flush the 2 rows
        let flushed = builder.finish().unwrap().unwrap();
        assert_eq!(flushed.num_rows(), 2);

        // Second finish without appends should return None
        assert!(builder.finish().unwrap().is_none());
    }

    #[test]
    fn arrow_batch_builder_finish_empty_returns_none() {
        let schema = int32_schema();
        let mut builder = ArrowBatchBuilder::new(schema, 2);
        assert!(builder.finish().unwrap().is_none());
    }

    #[tokio::test]
    async fn parquet_writer_writes_and_rotates_per_batch() {
        // Force rotation after every write by setting target size to 0
        let tempdir = tempfile::tempdir().unwrap();
        let output_dir = tempdir.path().to_path_buf();
        let schema = int32_schema();
        let config = InitialCopyWriterConfig {
            target_file_size_bytes: 0,
            max_rows_per_batch: 1024,
            num_writer_tasks: 4,
            batch_channel_capacity: 8,
        };

        let (tx, rx) = create_batch_channel(8);
        let writer = ParquetFileWriter::new(output_dir.clone(), schema.clone(), config);
        let shared_rx = SharedBatchReceiver::new(rx);
        let handle = tokio::spawn(async move { writer.write_from_shared(shared_rx).await });

        // Send three small batches
        tx.send(make_batch(&[1])).await.unwrap();
        tx.send(make_batch(&[2, 3])).await.unwrap();
        tx.send(make_batch(&[4, 5, 6])).await.unwrap();
        drop(tx); // close to let writer finish

        let files = handle.await.unwrap().unwrap();
        assert_eq!(files.len(), 3);
        for f in files.iter() {
            assert!(Path::new(f).exists());
            assert!(f.contains("ic-"));
            assert!(f.ends_with(".parquet"));
        }

        // Directory should exist
        assert!(output_dir.exists());
    }

    #[tokio::test]
    async fn parquet_writer_ignores_empty_batches() {
        let tempdir = tempfile::tempdir().unwrap();
        let output_dir = tempdir.path().to_path_buf();
        let schema = int32_schema();
        let config = InitialCopyWriterConfig::default();

        let (tx, rx) = create_batch_channel(8);
        let writer = ParquetFileWriter::new(output_dir.clone(), schema.clone(), config);
        let shared_rx = SharedBatchReceiver::new(rx);
        let handle = tokio::spawn(async move { writer.write_from_shared(shared_rx).await });

        // Send an empty batch (0 rows) and a non-empty one
        let empty = RecordBatch::new_empty(schema.clone());
        tx.send(empty).await.unwrap();
        tx.send(make_batch(&[42])).await.unwrap();
        drop(tx);

        let files = handle.await.unwrap().unwrap();
        // Only the non-empty batch should result in a file
        assert_eq!(files.len(), 1);
        assert!(Path::new(&files[0]).exists());
    }

    #[tokio::test]
    async fn parquet_writer_finalizes_on_channel_close() {
        // Large target size so no rotation happens during writes; file is finalized at the end
        let tempdir = tempfile::tempdir().unwrap();
        let output_dir = tempdir.path().to_path_buf();
        let schema = int32_schema();
        let config = InitialCopyWriterConfig {
            target_file_size_bytes: usize::MAX,
            max_rows_per_batch: 1024,
            num_writer_tasks: 1,
            batch_channel_capacity: 4,
        };

        let (tx, rx) = create_batch_channel(4);
        let writer = ParquetFileWriter::new(output_dir.clone(), schema.clone(), config);
        let shared_rx = SharedBatchReceiver::new(rx);
        let handle = tokio::spawn(async move { writer.write_from_shared(shared_rx).await });

        tx.send(make_batch(&[7, 8, 9])).await.unwrap();
        drop(tx);

        let files = handle.await.unwrap().unwrap();
        assert_eq!(files.len(), 1);
        assert!(Path::new(&files[0]).exists());
    }

    #[tokio::test]
    async fn parquet_multi_writer_idle_writers_produce_no_files() {
        // num_writer_tasks = 4, send exactly 1 non-empty batch; expect exactly 1 file
        let tempdir = tempfile::tempdir().unwrap();
        let output_dir = tempdir.path().to_path_buf();
        let schema = int32_schema();
        let config = InitialCopyWriterConfig {
            target_file_size_bytes: usize::MAX, // avoid rotation
            max_rows_per_batch: 1024,
            num_writer_tasks: 4,
            batch_channel_capacity: 4,
        };

        let (tx, rx) = create_batch_channel(4);
        let shared_rx = SharedBatchReceiver::new(rx);

        // Spawn 4 writers
        let mut handles = Vec::new();
        for _ in 0..config.num_writer_tasks {
            let writer = ParquetFileWriter::new(output_dir.clone(), schema.clone(), config.clone());
            let srx = shared_rx.clone();
            handles.push(tokio::spawn(
                async move { writer.write_from_shared(srx).await },
            ));
        }

        // Send exactly one batch
        tx.send(make_batch(&[1, 2, 3])).await.unwrap();
        drop(tx);

        // Collect results
        let mut files_written = Vec::new();
        for h in handles {
            let mut files = h.await.unwrap().unwrap();
            files_written.append(&mut files);
        }

        assert_eq!(
            files_written.len(),
            1,
            "only one writer should produce a file"
        );
        assert!(Path::new(&files_written[0]).exists());
    }

    #[tokio::test]
    async fn parquet_multi_writer_tail_files_bounded_by_writers_and_batches() {
        // num_writer_tasks = 3, send multiple batches; ensure file count is reasonable (no rotation)
        let tempdir = tempfile::tempdir().unwrap();
        let output_dir = tempdir.path().to_path_buf();
        let schema = int32_schema();
        let config = InitialCopyWriterConfig {
            target_file_size_bytes: usize::MAX, // one file per writer that received at least one batch
            max_rows_per_batch: 2,
            num_writer_tasks: 3,
            batch_channel_capacity: 8,
        };

        let (tx, rx) = create_batch_channel(8);
        let shared_rx = SharedBatchReceiver::new(rx);

        // Spawn 3 writers
        let mut handles = Vec::new();
        for _ in 0..config.num_writer_tasks {
            let writer = ParquetFileWriter::new(output_dir.clone(), schema.clone(), config.clone());
            let srx = shared_rx.clone();
            handles.push(tokio::spawn(
                async move { writer.write_from_shared(srx).await },
            ));
        }

        // Send several batches; distribution across writers is nondeterministic
        let batches_sent = 5;
        for i in 0..batches_sent {
            tx.send(make_batch(&[i, i + 1])).await.unwrap();
        }
        drop(tx);

        let mut files_written = Vec::new();
        for h in handles {
            let mut files = h.await.unwrap().unwrap();
            files_written.append(&mut files);
        }

        // With no rotation, each writer produces at most one file if it received any batch.
        // So total files is between 1 and min(num_writer_tasks, batches_sent).
        assert!(
            !files_written.is_empty(),
            "at least one writer should produce a file"
        );
        assert!(
            files_written.len() as u32 <= config.num_writer_tasks as u32,
            "should be at most one file per writer"
        );
        assert!(
            files_written.len() as u32 <= batches_sent as u32,
            "cannot exceed number of batches"
        );
        for f in &files_written {
            assert!(Path::new(f).exists());
        }
    }

    #[tokio::test]
    async fn parquet_writer_error_propagation_on_invalid_output_dir() {
        // Create a path that is a file (not a directory); writer should error when trying to create file under it
        let base = tempfile::tempdir().unwrap();
        let not_a_dir = base.path().join("not_a_dir");
        // Create a file at not_a_dir
        tokio::fs::write(&not_a_dir, b"block").await.unwrap();

        let schema = int32_schema();
        let config = InitialCopyWriterConfig {
            target_file_size_bytes: usize::MAX,
            max_rows_per_batch: 1024,
            num_writer_tasks: 1,
            batch_channel_capacity: 2,
        };

        let (tx, rx) = create_batch_channel(2);
        let shared_rx = SharedBatchReceiver::new(rx);
        let writer = ParquetFileWriter::new(not_a_dir.clone(), schema.clone(), config.clone());
        let handle = tokio::spawn(async move { writer.write_from_shared(shared_rx).await });

        // Send one batch to trigger file creation under a non-directory parent
        tx.send(make_batch(&[42])).await.unwrap();
        drop(tx);

        let res = handle.await.unwrap();
        assert!(
            res.is_err(),
            "writer should return error on invalid output dir"
        );
    }
}
