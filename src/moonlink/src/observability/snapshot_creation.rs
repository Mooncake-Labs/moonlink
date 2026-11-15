use crate::observability::latency_exporter::{BaseLatencyExporter, TracingLatencyExporter};
use crate::observability::latency_guard::LatencyGuard;

#[derive(Debug)]
pub(crate) struct SnapshotCreationStats(TracingLatencyExporter);

impl SnapshotCreationStats {
    pub(crate) fn new(mooncake_table_id: String) -> Self {
        Self(TracingLatencyExporter::new(
            "snapshot_creation_latency",
            mooncake_table_id,
        ))
    }
}

impl BaseLatencyExporter for SnapshotCreationStats {
    fn start<'a>(&'a self) -> LatencyGuard<'a> {
        self.0.start()
    }

    fn record(&self, latency: std::time::Duration, mooncake_table_id: String) {
        self.0.record(latency, mooncake_table_id)
    }
}
