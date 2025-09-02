use crate::pg_replicate::conversions::table_row::TableRow;
use crate::pg_replicate::initial_copy_writer::{
    create_batch_channel, ArrowBatchBuilder, InitialCopyWriterConfig, ParquetFileWriter,
    SharedBatchReceiver,
};
use crate::pg_replicate::postgres_source::TableCopyStreamError;
use crate::pg_replicate::postgres_source::{PostgresSource, TableCopyStream};
use crate::pg_replicate::table::{ColumnSchema, LookupKey, SrcTableId, TableName, TableSchema};
use crate::pg_replicate::util::postgres_schema_to_moonlink_schema;
use crate::Result;
use futures::{pin_mut, Stream, StreamExt};
use moonlink::{StorageConfig, TableEvent};
use moonlink_error::ErrorStatus;
use moonlink_error::ErrorStruct;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_postgres::types::PgLsn;
use tokio_postgres::types::Type;

/// Represents progress information for an ongoing copy.
#[derive(Debug)]
pub struct CopyProgress {
    /// Last WAL LSN processed from the CDC stream.
    pub last_lsn: PgLsn,
    /// Number of rows copied so far.
    pub rows_copied: u64,
}

/// Reads rows from `stream` and sends them to the provided `event_sender`.
pub async fn copy_table_stream<S>(
    table_schema: TableSchema,
    mut stream: S,
    event_sender: &Sender<TableEvent>,
    start_lsn: u64,
    table_base_path: &str,
    config: Option<InitialCopyWriterConfig>,
) -> Result<CopyProgress>
where
    S: Stream<Item = std::result::Result<TableRow, TableCopyStreamError>>,
{
    // Convert PostgreSQL schema to Arrow schema
    let (arrow_schema, _identity_prop) = postgres_schema_to_moonlink_schema(&table_schema);
    let arrow_schema = Arc::new(arrow_schema);

    let mut config = config.unwrap_or_default();
    // Use 4 parallel writers
    config.num_writer_tasks = 4;

    // Create output directory for initial copy files
    let output_dir = std::path::PathBuf::from(table_base_path)
        .join("initial_copy")
        .join(format!("table_{}", table_schema.src_table_id));

    // Create batch channel for RecordBatches
    let (batch_tx, batch_rx) = create_batch_channel(config.batch_channel_capacity);

    // Create Arrow batch builder
    let mut batch_builder = ArrowBatchBuilder::new(arrow_schema.clone(), config.max_rows_per_batch);

    // Create shared receiver and spawn N Parquet writer tasks
    let shared_rx = SharedBatchReceiver::new(batch_rx);
    let num_workers = config.num_writer_tasks.max(1);
    let mut writer_handles = Vec::with_capacity(num_workers);
    for _ in 0..num_workers {
        let writer = ParquetFileWriter {
            output_dir: output_dir.clone(),
            schema: arrow_schema.clone(),
            config: config.clone(),
        };
        let rx = shared_rx.clone();
        writer_handles.push(tokio::spawn(
            async move { writer.write_from_shared(rx).await },
        ));
    }

    // Stream rows into batches
    pin_mut!(stream);
    let mut rows_copied = 0u64;
    while let Some(row) = stream.next().await {
        let row = row.map_err(|e| crate::Error::from(e))?;

        // Append row to batch builder and check if a batch is ready
        if let Some(finished_batch) = batch_builder.append_table_row(row)? {
            if let Err(e) = batch_tx.send(finished_batch).await {
                tracing::error!(error = ?e, "failed to send batch to Parquet writer");
                break;
            }
        }
        rows_copied += 1;
    }

    // Finalize any remaining batch
    if let Some(final_batch) = batch_builder.finish()? {
        if let Err(e) = batch_tx.send(final_batch).await {
            tracing::error!(error = ?e, "failed to send final batch to Parquet writer");
        }
    }

    // Close channel to signal writer completion
    drop(batch_tx);

    // Wait for all writers to finish and collect file paths
    let mut files_written: Vec<String> = Vec::new();
    for handle in writer_handles {
        let mut files = handle.await??;
        files_written.append(&mut files);
    }

    tracing::info!(
        "Initial copy completed: {} rows, {} files written",
        rows_copied,
        files_written.len()
    );

    // Send LoadFiles event to batch ingest the Parquet files
    if let Err(e) = event_sender
        .send(TableEvent::LoadFiles {
            files: files_written,
            storage_config: StorageConfig::FileSystem {
                root_directory: output_dir.to_str().unwrap().to_string(),
                atomic_write_dir: None,
            },
            lsn: start_lsn,
        })
        .await
    {
        tracing::warn!(error = ?e, "failed to send LoadFiles event");
    }

    Ok(CopyProgress {
        last_lsn: PgLsn::from(start_lsn),
        rows_copied,
    })
}

/// Tests for the initial copy functionality
#[cfg(test)]
mod tests {
    use std::fs;
    use std::panic::Location;
    use std::path::Path;

    use super::*;
    use crate::pg_replicate::conversions::table_row::{TableRow, TableRowConversionError};
    use crate::pg_replicate::conversions::Cell;
    use crate::pg_replicate::postgres_source::TableCopyStreamError;
    use crate::pg_replicate::table::{ColumnSchema, LookupKey, TableName};
    use futures::stream;
    use moonlink_error::ErrorStruct;
    use more_asserts as ma;
    use tempfile;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};
    use tokio_postgres::types::Type;

    //----------------------------------------------------------------------
    // Minimal helpers/stubs
    //----------------------------------------------------------------------

    /// Build a simple TableSchema for testing
    fn make_test_schema(name: &str) -> TableSchema {
        TableSchema {
            table_name: TableName {
                schema: "public".to_string(),
                name: name.to_string(),
            },
            src_table_id: 1,
            column_schemas: vec![ColumnSchema {
                name: "id".to_string(),
                typ: Type::INT4,
                modifier: 0,
                nullable: false,
            }],
            lookup_key: LookupKey::FullRow,
        }
    }

    /// Create a simple test error for testing error propagation
    fn make_test_error() -> crate::Error {
        crate::Error::PostgresSourceError(ErrorStruct {
            message: "Postgres source error".to_string(),
            status: moonlink_error::ErrorStatus::Permanent,
            source: Some(Arc::new(
                crate::pg_replicate::postgres_source::PostgresSourceError::MissingPublication
                    .into(),
            )),
            location: Some(Location::caller().to_string()),
        })
    }

    //----------------------------------------------------------------------
    // 1. Happy-path – emits LoadFiles
    //----------------------------------------------------------------------

    #[tokio::test]
    async fn test_copies_rows_and_reports_progress() {
        // Create a stream of successful TableRow results
        let table_row1 = crate::pg_replicate::conversions::table_row::TableRow {
            values: vec![crate::pg_replicate::conversions::Cell::I32(1)],
        };
        let table_row2 = crate::pg_replicate::conversions::table_row::TableRow {
            values: vec![crate::pg_replicate::conversions::Cell::I32(2)],
        };
        let table_row3 = crate::pg_replicate::conversions::table_row::TableRow {
            values: vec![crate::pg_replicate::conversions::Cell::I32(3)],
        };

        let rows = vec![Ok(table_row1), Ok(table_row2), Ok(table_row3)];
        let stream = stream::iter(rows);

        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let schema = make_test_schema("test");
        let start_lsn = 1000u64;

        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let progress = copy_table_stream(schema, Box::pin(stream), &tx, start_lsn, base_path, None)
            .await
            .expect("copy failed");

        assert_eq!(progress.rows_copied, 3);
        assert_eq!(progress.last_lsn, PgLsn::from(start_lsn));

        // Should receive LoadFiles event
        let evt = timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("channel lag")
            .expect("sender closed");
        let files: Vec<String> = match evt {
            TableEvent::LoadFiles { files, lsn, .. } => {
                assert_eq!(lsn, start_lsn);
                assert!(!files.is_empty(), "should have written at least one file");
                files
            }
            _ => panic!("expected LoadFiles event, got {evt:?}"),
        };
        // Files exist
        for f in &files {
            assert!(std::path::Path::new(f).exists(), "file not found: {}", f);
        }

        // Explicitly drop sender to close channel and avoid hanging
        drop(tx);

        // No more events
        assert!(rx.recv().await.is_none(), "no extra events expected");
    }

    //----------------------------------------------------------------------
    // 2. Empty snapshot – emits LoadFiles(empty)
    //----------------------------------------------------------------------

    #[tokio::test]
    async fn test_handles_empty_stream() {
        let stream =
            stream::iter(vec![] as Vec<std::result::Result<TableRow, TableCopyStreamError>>);
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let schema = make_test_schema("empty");

        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let progress = copy_table_stream(schema, Box::pin(stream), &tx, 0u64, base_path, None)
            .await
            .expect("copy failed");
        assert_eq!(progress.rows_copied, 0);

        // LoadFiles(empty)
        let evt = timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("channel lag")
            .expect("sender closed");
        match evt {
            TableEvent::LoadFiles { files, lsn, .. } => {
                assert!(files.is_empty());
                assert_eq!(lsn, 0);
            }
            _ => panic!("expected LoadFiles event"),
        }

        drop(tx);
        assert!(rx.recv().await.is_none(), "no events expected");
    }

    //----------------------------------------------------------------------
    // 3. Rotation – force multiple files via tiny config
    //----------------------------------------------------------------------

    #[tokio::test]
    async fn test_writes_multiple_files_with_rotation() {
        // Force tiny file size and small batches
        let config = InitialCopyWriterConfig {
            target_file_size_bytes: 1024, // ~1KB
            max_rows_per_batch: 10,
            num_writer_tasks: 1,
            batch_channel_capacity: 16,
        };

        // Build many rows
        let mut rows_vec = Vec::new();
        for i in 0..200 {
            let tr = TableRow {
                values: vec![Cell::I32(i)],
            };
            rows_vec.push(Ok(tr));
        }
        let stream = stream::iter(rows_vec);
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let schema = make_test_schema("rotate");
        let start_lsn = 42u64;

        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let _ = copy_table_stream(
            schema,
            Box::pin(stream),
            &tx,
            start_lsn,
            base_path,
            Some(config),
        )
        .await
        .expect("copy failed");

        // LoadFiles with multiple files
        let evt = timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("channel lag")
            .expect("sender closed");
        let files = match evt {
            TableEvent::LoadFiles { files, lsn, .. } => {
                assert_eq!(lsn, start_lsn);
                ma::assert_ge!(files.len(), 2,);
                files
            }
            _ => panic!("expected LoadFiles"),
        };
        for f in &files {
            assert!(std::path::Path::new(f).exists(), "file not found: {}", f);
        }

        drop(tx);

        // No explicit no-more-events assertion here
    }

    //----------------------------------------------------------------------
    // 4. Stream error – bubbles up to the caller
    //----------------------------------------------------------------------

    #[tokio::test]
    async fn test_propagates_stream_error() {
        // Use a conversion error that maps to our Error type
        let stream = stream::iter(vec![Err(TableCopyStreamError::ConversionError(
            TableRowConversionError::UnterminatedRow,
        ))]
            as Vec<std::result::Result<TableRow, TableCopyStreamError>>);
        let (tx, _) = mpsc::channel::<TableEvent>(1);
        let schema = make_test_schema("broken");

        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let err = copy_table_stream(schema, Box::pin(stream), &tx, 0u64, base_path, None)
            .await
            .expect_err("expected failure");

        // Just verify we got an error - the exact format may vary
        let err_str = err.to_string().to_lowercase();
        assert!(
            err_str.contains("unterminated row") || err_str.contains("conversion error"),
            "unexpected error string: {}",
            err.to_string()
        );
    }

    // ----------------------------------------------------------------------
    // Integration tests
    // ----------------------------------------------------------------------

    #[tokio::test]
    async fn ic_multi_writer_end_to_end() {
        // 50 rows, small batches, rotate each write to maximize file count
        let config = InitialCopyWriterConfig {
            target_file_size_bytes: 0,
            max_rows_per_batch: 10,
            num_writer_tasks: 3,
            batch_channel_capacity: 8,
        };

        let mut rows = Vec::new();
        for i in 0..50 {
            rows.push(Ok(TableRow {
                values: vec![Cell::I32(i)],
            }));
        }
        let stream = stream::iter(rows);
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let schema = make_test_schema("mw_e2e");

        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let _ = copy_table_stream(schema, Box::pin(stream), &tx, 7, base_path, Some(config))
            .await
            .expect("copy failed");

        let evt = timeout(Duration::from_millis(1000), rx.recv())
            .await
            .expect("channel lag")
            .expect("sender closed");
        let files = match evt {
            TableEvent::LoadFiles { files, lsn, .. } => {
                assert_eq!(lsn, 7);
                ma::assert_ge!(files.len(), 3);
                files
            }
            _ => panic!("expected LoadFiles"),
        };
        for f in &files {
            assert!(Path::new(f).exists());
        }
    }

    #[tokio::test]
    async fn ic_empty_with_n_gt_1() {
        // No rows, 4 writers => LoadFiles empty, and directory should be empty
        let table_id = 7654u32;
        let config = InitialCopyWriterConfig {
            target_file_size_bytes: usize::MAX,
            max_rows_per_batch: 1024,
            num_writer_tasks: 4,
            batch_channel_capacity: 4,
        };

        let stream =
            stream::iter(Vec::<std::result::Result<TableRow, TableCopyStreamError>>::new());
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let mut schema = make_test_schema("empty_n");
        schema.src_table_id = table_id;

        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let _ = copy_table_stream(schema, Box::pin(stream), &tx, 0, base_path, Some(config))
            .await
            .expect("copy failed");

        let evt = timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("channel lag")
            .expect("sender closed");
        match evt {
            TableEvent::LoadFiles { files, lsn, .. } => {
                assert!(files.is_empty());
                assert_eq!(lsn, 0);
            }
            _ => panic!("expected LoadFiles"),
        }

        // Check the default directory used by writers
        let default_dir = std::env::temp_dir()
            .join("moonlink_initial_copy")
            .join(format!("table_{}", table_id));
        if default_dir.exists() {
            let mut entries = tokio::fs::read_dir(&default_dir).await.unwrap();
            let mut count = 0;
            while let Ok(Some(_)) = entries.next_entry().await {
                count += 1;
            }
            assert_eq!(count, 0, "output directory should be empty");
        }
    }

    #[tokio::test]
    async fn ic_writer_error_propagation_via_default_dir_collision() {
        let table_id = 7777u32;
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let default_dir = std::path::PathBuf::from(base_path)
            .join("initial_copy")
            .join(format!("table_{}", table_id));
        // Ensure parent exists and create a file at the would-be directory path
        if let Some(parent) = default_dir.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&default_dir, b"block").await.unwrap();

        let config = InitialCopyWriterConfig {
            target_file_size_bytes: usize::MAX,
            max_rows_per_batch: 1024,
            num_writer_tasks: 2,
            batch_channel_capacity: 4,
        };

        let rows = vec![Ok(TableRow {
            values: vec![Cell::I32(1)],
        })];
        let stream = stream::iter(rows);
        let (tx, _rx) = mpsc::channel::<TableEvent>(8);
        let mut schema = make_test_schema("err_out_dir_default");
        schema.src_table_id = table_id;

        let err = copy_table_stream(schema, Box::pin(stream), &tx, 1, base_path, Some(config))
            .await
            .expect_err("expected failure");
        let s = err.to_string().to_lowercase();
        assert!(s.contains("io") || s.contains("parquet") || s.contains("create"));
    }

    #[tokio::test]
    async fn test_initial_copy_uses_base_path() {
        // Test that initial copy files are created under base_path
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let table_id = 9999u32;

        // Create some test data
        let rows = vec![
            Ok(TableRow {
                values: vec![Cell::I32(1)],
            }),
            Ok(TableRow {
                values: vec![Cell::I32(2)],
            }),
        ];
        let stream = stream::iter(rows);
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let mut schema = make_test_schema("base_path_test");
        schema.src_table_id = table_id;

        // Run initial copy
        let _progress = copy_table_stream(schema, Box::pin(stream), &tx, 42, base_path, None)
            .await
            .expect("copy failed");

        // Verify files are created under base_path/initial_copy/table_{id}/
        let expected_dir = std::path::Path::new(base_path)
            .join("initial_copy")
            .join(format!("table_{}", table_id));
        assert!(
            expected_dir.exists(),
            "Expected directory should exist: {:?}",
            expected_dir
        );

        // Verify at least one .parquet file exists in the expected directory
        let entries = std::fs::read_dir(&expected_dir).unwrap();
        let parquet_files: Vec<_> = entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext == "parquet")
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            !parquet_files.is_empty(),
            "Should have at least one parquet file"
        );

        // Verify LoadFiles event contains correct root_directory
        let evt = rx.recv().await.expect("should receive LoadFiles event");
        match evt {
            TableEvent::LoadFiles {
                storage_config,
                files,
                ..
            } => {
                match storage_config {
                    StorageConfig::FileSystem { root_directory, .. } => {
                        assert_eq!(root_directory, expected_dir.to_str().unwrap());
                    }
                    _ => panic!("Expected FileSystem storage config"),
                }
                assert!(!files.is_empty(), "Should have files in LoadFiles event");
            }
            _ => panic!("Expected LoadFiles event, got {:?}", evt),
        }
    }

    #[tokio::test]
    async fn ic_backpressure_sanity() {
        // Channel capacity 1, many rows, small batch size, small rotation threshold
        let config = InitialCopyWriterConfig {
            target_file_size_bytes: 0,
            max_rows_per_batch: 5,
            num_writer_tasks: 2,
            batch_channel_capacity: 1,
        };

        let mut rows = Vec::new();
        for i in 0..200 {
            rows.push(Ok(TableRow {
                values: vec![Cell::I32(i)],
            }));
        }
        let stream = stream::iter(rows);
        let (tx, mut rx) = mpsc::channel::<TableEvent>(8);
        let schema = make_test_schema("backpressure");

        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let _ = copy_table_stream(schema, Box::pin(stream), &tx, 9, base_path, Some(config))
            .await
            .expect("copy failed");

        let evt = timeout(Duration::from_millis(2000), rx.recv())
            .await
            .expect("channel lag")
            .expect("sender closed");
        match evt {
            TableEvent::LoadFiles { files, lsn, .. } => {
                assert_eq!(lsn, 9);
                assert!(!files.is_empty());
            }
            _ => panic!("expected LoadFiles"),
        }
    }
}
