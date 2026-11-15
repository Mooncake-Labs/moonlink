use crate::observability::latency_exporter::TracingLatencyExporter;

#[derive(Debug)]
pub(crate) struct IcebergPersistenceStats {
    pub(crate) overall: TracingLatencyExporter,
    pub(crate) sync_data_files: TracingLatencyExporter,
    pub(crate) sync_file_indices: TracingLatencyExporter,
    pub(crate) sync_deletion_vectors: TracingLatencyExporter,
    pub(crate) transaction_commit: TracingLatencyExporter,
}

impl IcebergPersistenceStats {
    pub(crate) fn new(mooncake_table_id: String) -> Self {
        let exporter = |metric| TracingLatencyExporter::new(metric, mooncake_table_id.clone());
        Self {
            overall: exporter("snapshot_synchronization_latency"),
            sync_data_files: exporter("sync_data_files_latency"),
            sync_file_indices: exporter("sync_file_indices_latency"),
            sync_deletion_vectors: exporter("sync_deletion_vectors_latency"),
            transaction_commit: exporter("transaction_commit_latency"),
        }
    }
}
