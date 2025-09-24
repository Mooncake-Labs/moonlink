use opentelemetry::metrics::Histogram;
use opentelemetry::{global, KeyValue};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct IcebergTableRecoveryStats {
    data_files_latency: Histogram<u64>,
    file_indices_deletion_vector_latency: Histogram<u64>,
}

impl IcebergTableRecoveryStats {
    pub fn new() -> Arc<Self> {
        let meter = global::meter("iceberg_table_recovery_latency");
        Arc::new(IcebergTableRecoveryStats {
            data_files_latency: meter
                .u64_histogram("load_data_files_latency")
                .with_description("Latency (ms) for loading data files during table recovery")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
            file_indices_deletion_vector_latency: meter
                .u64_histogram("load_file_indices_and_deletion_vector_latency")
                .with_description("Latency (ms) for loading file indices & deletion vectors")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
        })
    }

    pub fn update(
        &self,
        data_files_latency: u64,
        file_indices_deletion_vector_latency: u64,
        mooncake_table_id: String,
    ) {
        let attrs = [KeyValue::new(
            "moonlink.mooncake_table_id",
            mooncake_table_id,
        )];
        self.data_files_latency.record(data_files_latency, &attrs);
        self.file_indices_deletion_vector_latency
            .record(file_indices_deletion_vector_latency, &attrs);
    }
}
