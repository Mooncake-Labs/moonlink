use crate::otel::metrics_handler::MetricsHandler;
use crate::Result;
use std::sync::Arc;

/// State for otel service.

#[derive(Clone)]
pub struct OtelState {
    /// Metrics handler.
    pub(crate) metrics_handler: Arc<MetricsHandler>,
}

impl OtelState {
    pub async fn new(
        rest_port: u16,
        moonlink_backend: Arc<moonlink_backend::MoonlinkBackend>,
    ) -> Result<Self> {
        let metrics_handler =
            Arc::new(MetricsHandler::new(rest_port, moonlink_backend.clone()).await?);
        Ok(Self { metrics_handler })
    }
}
