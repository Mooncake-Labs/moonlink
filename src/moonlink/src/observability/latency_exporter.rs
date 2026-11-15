use crate::observability::latency_guard::LatencyGuard;

/// An interface to export latency.
pub(crate) trait BaseLatencyExporter: Send + Sync {
    /// Start recording latency.
    /// Returned latency guard will automatically records latency stats.
    fn start<'a>(&'a self) -> LatencyGuard<'a>;
    /// Export latency stats.
    fn record(&self, latency: std::time::Duration, mooncake_table_id: String);
}

/// Latency exporter which emits one `tracing` debug event per measurement.
#[derive(Debug)]
pub(crate) struct TracingLatencyExporter {
    metric: &'static str,
    mooncake_table_id: String,
}

impl TracingLatencyExporter {
    pub(crate) fn new(metric: &'static str, mooncake_table_id: String) -> Self {
        Self {
            metric,
            mooncake_table_id,
        }
    }
}

impl BaseLatencyExporter for TracingLatencyExporter {
    fn start<'a>(&'a self) -> LatencyGuard<'a> {
        LatencyGuard::new(self.mooncake_table_id.clone(), self)
    }

    fn record(&self, latency: std::time::Duration, mooncake_table_id: String) {
        tracing::debug!(
            target: "moonlink::latency",
            metric = self.metric,
            latency_ms = latency.as_millis() as u64,
            mooncake_table_id,
        );
    }
}
