use crate::{
    create_data_file, storage::mooncake_table::transaction_stream::TransactionStreamCommit,
};

use super::*;

use futures::{stream, StreamExt};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::fs::File;

impl MooncakeTable {
    /// Batch ingestion the given [`parquet_files`] into mooncake table.
    ///
    /// TODO(hjiang):
    /// 1. Record table events.
    /// 2. It involves IO operations, should be placed at background thread.s
    pub(crate) async fn batch_ingest(&mut self, parquet_files: Vec<String>, lsn: u64) {
        const MAX_IN_FLIGHT: usize = 64;
        let start_id = self.next_file_id;
        self.next_file_id += parquet_files.len() as u32;

        let disk_files = stream::iter(parquet_files.into_iter().enumerate().map(
            |(idx, cur_file)| {
                let cur_file_id = (start_id as u64) + idx as u64;
                async move {
                    // TODO(hjiang): Handle remote data file ingestion as well.
                    let file = File::open(&cur_file)
                        .await
                        .unwrap_or_else(|_| panic!("Failed to open {cur_file}"));
                    let file_size = file
                        .metadata()
                        .await
                        .unwrap_or_else(|_| panic!("Failed to stat {cur_file}"))
                        .len() as usize;
                    let stream_builder = ParquetRecordBatchStreamBuilder::new(file)
                        .await
                        .unwrap_or_else(|_| panic!("Failed to read parquet footer for {cur_file}"));
                    let num_rows = stream_builder.metadata().file_metadata().num_rows() as usize;

                    let mooncake_data_file = create_data_file(cur_file_id, cur_file.clone());
                    let disk_file_entry = DiskFileEntry {
                        cache_handle: None,
                        num_rows,
                        file_size,
                        batch_deletion_vector: BatchDeletionVector::new(num_rows),
                        puffin_deletion_blob: None,
                    };

                    (mooncake_data_file, disk_file_entry)
                }
            },
        ))
        .buffer_unordered(MAX_IN_FLIGHT) // run up to N at once
        .collect::<hashbrown::HashMap<_, _>>()
        .await;

        // Commit the current crafted streaming transaction.
        let commit = TransactionStreamCommit::from_disk_files(disk_files, lsn);
        self.next_snapshot_task
            .new_streaming_xact
            .push(TransactionStreamOutput::Commit(commit));
        self.next_snapshot_task.new_flush_lsn = Some(lsn);
        self.next_snapshot_task.commit_lsn_baseline = lsn;
    }
}
