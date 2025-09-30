use crate::observability::latency_exporter::BaseLatencyExporter;
use crate::observability::latency_guard::LatencyGuard;
use opentelemetry::metrics::Histogram;
use opentelemetry::{global, KeyValue};

#[derive(Debug)]
pub(crate) struct IcebergTableRecoveryStats {
    /// Otel latency histogram exporter.
    latency: Histogram<u64>,
    /// Mooncake table id.
    mooncake_table_id: String,
}

impl IcebergTableRecoveryStats {
    pub fn new(mooncake_table_id: String) -> Self {
        let meter = global::meter("iceberg_table_recovery");
        IcebergTableRecoveryStats {
            mooncake_table_id,
            latency: meter
                .u64_histogram("snapshot_load_latency")
                .with_description("Latency (ms) for iceberg table snapshot loading.")
                .with_boundaries(vec![50.0, 100.0, 200.0, 300.0, 400.0, 500.0])
                .build(),
        }
    }
}

impl BaseLatencyExporter for IcebergTableRecoveryStats {
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
