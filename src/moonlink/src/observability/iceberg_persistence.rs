use crate::observability::latency_exporter::BaseLatencyExporter;
use crate::observability::latency_guard::LatencyGuard;
use opentelemetry::metrics::Histogram;
use opentelemetry::{global, KeyValue};
use std::sync::Arc;

enum PersistenceStage {
    Overall,
    DataFiles,
    FileIndices,
    DeletionVectors,
    TransactionCommit,
}

#[derive(Debug)]
pub(crate) struct IcebergPersistenceStats {
    pub(crate) stats_overall: Arc<IcebergPersistenceSingleStats>,
    pub(crate) sync_data_files: Arc<IcebergPersistenceSingleStats>,
    pub(crate) sync_file_indices: Arc<IcebergPersistenceSingleStats>,
    pub(crate) sync_deletion_vectors: Arc<IcebergPersistenceSingleStats>,
    pub(crate) transaction_commit: Arc<IcebergPersistenceSingleStats>,
}

impl IcebergPersistenceStats {
    pub(crate) fn new(mooncake_table_id: String) -> Self {
        fn make_stats(id: &str, stage: PersistenceStage) -> Arc<IcebergPersistenceSingleStats> {
            Arc::new(IcebergPersistenceSingleStats::new(id.to_string(), stage))
        }
        Self {
            stats_overall: make_stats(&mooncake_table_id, PersistenceStage::Overall),
            sync_data_files: make_stats(&mooncake_table_id, PersistenceStage::DataFiles),
            sync_file_indices: make_stats(&mooncake_table_id, PersistenceStage::FileIndices),
            sync_deletion_vectors: make_stats(
                &mooncake_table_id,
                PersistenceStage::DeletionVectors,
            ),
            transaction_commit: make_stats(&mooncake_table_id, PersistenceStage::TransactionCommit),
        }
    }
}

#[derive(Debug)]
pub(crate) struct IcebergPersistenceSingleStats {
    mooncake_table_id: String,
    latency: Histogram<u64>,
}

impl IcebergPersistenceSingleStats {
    fn new(mooncake_table_id: String, stats_type: PersistenceStage) -> Self {
        let meter = global::meter("iceberg_persistence");
        let latency = match stats_type {
            PersistenceStage::Overall => meter
                .u64_histogram("snapshot_synchronization_latency")
                .with_description("Latency (ms) for snapshot synchronization")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
            PersistenceStage::DataFiles => meter
                .u64_histogram("sync_data_files_latency")
                .with_description("Latency (ms) for data files synchronization")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
            PersistenceStage::FileIndices => meter
                .u64_histogram("sync_file_indices_latency")
                .with_description("Latency (ms) for file indices synchronization")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
            PersistenceStage::DeletionVectors => meter
                .u64_histogram("sync_deletion_vectors_latency")
                .with_description("Latency (ms) for deletion vectors synchronization")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
            PersistenceStage::TransactionCommit => meter
                .u64_histogram("transaction_commit_latency")
                .with_description("Latency (ms) for transaction commit")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
        };

        Self {
            mooncake_table_id,
            latency,
        }
    }
}

impl BaseLatencyExporter for IcebergPersistenceSingleStats {
    fn start<'a>(&'a self) -> LatencyGuard<'a> {
        LatencyGuard::new(self.mooncake_table_id.clone(), self)
    }

    fn record(&self, latency: std::time::Duration, mooncake_table_id: String) {
        self.latency.record(
            latency.as_millis() as u64,
            &[KeyValue::new(
                "moonlink.mooncake_table_id",
                mooncake_table_id,
            )],
        );
    }
}
