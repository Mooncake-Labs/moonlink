use crate::{
    create_data_file, storage::mooncake_table::transaction_stream::TransactionStreamCommit,
};

use super::*;

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

impl MooncakeTable {
    /// Batch ingestion the given [`parquet_files`] into mooncake table.
    ///
    /// TODO(hjiang):
    /// 1. Record table events.
    /// 2. It involves IO operations, should be placed at background thread.s
    pub(crate) async fn batch_ingest(&mut self, parquet_files: Vec<String>, lsn: u64) {
        let mut disk_files = hashbrown::HashMap::with_capacity(parquet_files.len());

        // TODO(hjiang): Parallel IO and error handling.
        // Construct disk file from the given disk files.
        for cur_file in parquet_files.into_iter() {
            let content = tokio::fs::read(&cur_file).await.unwrap();
            let content = Bytes::from(content);
            let file_size = content.len();
            let builder = ParquetRecordBatchReaderBuilder::try_new(content).unwrap();
            let num_rows: usize = builder.metadata().file_metadata().num_rows() as usize;

            let cur_file_id = self.next_file_id;
            self.next_file_id += 1;
            let mooncake_data_file = create_data_file(cur_file_id as u64, cur_file);
            let disk_file_entry = DiskFileEntry {
                cache_handle: None,
                num_rows,
                file_size,
                batch_deletion_vector: BatchDeletionVector::new(num_rows),
                puffin_deletion_blob: None,
            };

            assert!(disk_files
                .insert(mooncake_data_file, disk_file_entry)
                .is_none());
        }

        // Commit the current crafted streaming transaction.
        let commit = TransactionStreamCommit::from_disk_files(disk_files, lsn);
        self.next_snapshot_task
            .new_streaming_xact
            .push(TransactionStreamOutput::Commit(commit));
        self.next_snapshot_task.new_flush_lsn = Some(lsn);
        self.next_snapshot_task.commit_lsn_baseline = lsn;
    }
}
