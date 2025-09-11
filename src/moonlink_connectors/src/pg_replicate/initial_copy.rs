use crate::pg_replicate::clients::postgres::ReplicationClient;
use crate::pg_replicate::initial_copy_writer::{
    create_batch_channel, ArrowBatchBuilder, InitialCopyWriterConfig, ParquetFileWriter,
    SharedBatchReceiver,
};
use crate::pg_replicate::postgres_source::PostgresSource;
use crate::pg_replicate::postgres_source::PostgresSourceError;
use crate::pg_replicate::table::{ColumnSchema, LookupKey, SrcTableId, TableName, TableSchema};
use crate::pg_replicate::util::postgres_schema_to_moonlink_schema;
use crate::{Error, Result};
use futures::StreamExt;
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
    /// Snapshot boundary LSN captured at snapshot export (copy start boundary).
    pub boundary_lsn: PgLsn,
    /// Number of rows copied so far.
    pub rows_copied: u64,
}

/// Reader-side configuration for parallel initial copy
#[derive(Clone, Debug)]
pub struct InitialCopyReaderConfig {
    /// Postgres connection URI used to spawn reader sessions
    pub uri: String,
    /// Number of parallel readers (use 1 for single-reader mode)
    pub shard_count: usize,
}

/// Unified configuration for initial copy (readers + writers)
#[derive(Clone, Debug)]
pub struct InitialCopyConfig {
    pub reader: InitialCopyReaderConfig,
    pub writer: InitialCopyWriterConfig,
}

/// Minimal CTID shard descriptor
#[derive(Clone, Debug)]
struct CtidShard {
    /// Inclusive start block (ctid >= (start_block, 1))
    start_block: i64,
    /// Exclusive end block for non-last shards; None means unbounded upper range
    end_block_exclusive: Option<i64>,
}

/// Reads rows using parallel readers and sends them to the provided `event_sender`.
pub async fn copy_table_stream(
    table_schema: TableSchema,
    event_sender: &Sender<TableEvent>,
    table_base_path: &str,
    config: InitialCopyConfig,
) -> Result<CopyProgress> {
    // Convert PostgreSQL schema to Arrow schema
    let (arrow_schema, _identity_prop) = postgres_schema_to_moonlink_schema(&table_schema);
    let arrow_schema = Arc::new(arrow_schema);

    // Prepare writer config
    let writer_cfg = config.writer.clone();

    // Create output directory for initial copy files
    let output_dir = std::path::PathBuf::from(table_base_path)
        .join("initial_copy")
        .join(format!("table_{}", table_schema.src_table_id));

    // Create batch channel for RecordBatches
    let (batch_tx, batch_rx) = create_batch_channel(writer_cfg.batch_channel_capacity);

    // Create Arrow batch builder
    let mut batch_builder =
        ArrowBatchBuilder::new(arrow_schema.clone(), writer_cfg.max_rows_per_batch);

    // Create shared receiver and spawn N Parquet writer tasks
    let shared_rx = SharedBatchReceiver::new(batch_rx);
    let num_workers = writer_cfg.num_writer_tasks.max(1);
    let mut writer_handles = Vec::with_capacity(num_workers);
    for _ in 0..num_workers {
        let writer = ParquetFileWriter {
            output_dir: output_dir.clone(),
            schema: arrow_schema.clone(),
            config: writer_cfg.clone(),
        };
        let rx = shared_rx.clone();
        writer_handles.push(tokio::spawn(
            async move { writer.write_from_shared(rx).await },
        ));
    }

    let mut rows_copied = 0u64;

    let mut source = PostgresSource::new(&config.reader.uri, None, None, false).await?;

    // Snapshot boundary captured here (copy start boundary)
    let (snapshot_id, snapshot_start_lsn) = source.export_snapshot_and_lsn().await?;

    let shards = source
        .plan_ctid_shards(&table_schema.table_name, config.reader.shard_count)
        .await?;

    let reader_handles = source
        .spawn_sharded_copy_readers(
            config.reader.uri.clone(),
            snapshot_id,
            table_schema.clone(),
            shards,
            batch_tx.clone(),
            writer_cfg.max_rows_per_batch,
        )
        .await?;

    // Wait for all readers
    let mut success = true;
    for h in reader_handles {
        match h.await {
            Ok(Ok(n)) => rows_copied += n,
            _ => {
                success = false;
                break;
            }
        }
    }

    // Finalize snapshot (commit on success, rollback on failure)
    source.finalize_snapshot(success).await?;
    if !success {
        // Close the channel to allow writers to finish and return error
        drop(batch_tx);
        for handle in writer_handles {
            let _ = handle.await; // best-effort drain
        }
        return Err(crate::Error::from(std::io::Error::new(
            std::io::ErrorKind::Other,
            "parallel initial copy failed in at least one reader",
        )));
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
            lsn: u64::from(snapshot_start_lsn),
        })
        .await
    {
        tracing::warn!(error = ?e, "failed to send LoadFiles event");
    }

    Ok(CopyProgress {
        boundary_lsn: snapshot_start_lsn,
        rows_copied,
    })
}
