use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use tracing_subscriber::EnvFilter;

use crate::start_with_config;
use crate::test_guard::TestGuard;
use crate::test_utils::*;
use tracing::error;

/// Default HTTP opentelemetry endpoint.
const DEFAULT_HTTP_OTEL_ENDPOINT: &str = "http://127.0.0.1:3435/v1/metrics";

#[tokio::test]
async fn test_opentelemetry_export() {
    let _guard = TestGuard::new(&get_moonlink_backend_dir());
    let config = get_service_config();
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    wait_for_server_ready().await;

    // Set the tracing inside of otel sdk, otherwise hard to troubleshoot.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(DEFAULT_HTTP_OTEL_ENDPOINT)
        .with_protocol(Protocol::HttpBinary) // send protobuf message
        .build()
        .unwrap();
    let reader = PeriodicReader::builder(exporter)
        .with_interval(std::time::Duration::from_secs(2))
        .build();

    let meter_provider = SdkMeterProvider::builder().with_reader(reader).build();
    global::set_meter_provider(meter_provider.clone());

    let meter = global::meter("basic");
    let counter = meter
        .u64_counter("test_counter")
        .with_description("a simple counter for testing purposes.")
        .with_unit("unit")
        .build();

    // Sleep for a while.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Update counter.
    for _ in 0..10 {
        counter.add(1, &[KeyValue::new("moonlink.mooncake_table_id", "id")]);
    }

    // Shutdown and flush.
    if let Err(e) = meter_provider.force_flush() {
        error!("Failed to flush metrics: {:?}", e);
    }
    if let Err(e) = meter_provider.shutdown() {
        error!("Failed to shutdown provider: {:?}", e);
    }
}
