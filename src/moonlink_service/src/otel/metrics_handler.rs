use std::collections::HashSet;
use std::sync::Arc;

/// Handler to process metrics ingestion.
use crate::error::{Error, Result};
use crate::otel::otel_schema::otlp_metrics_gsh_schema;
use crate::otel::otel_to_moonlink_pb;
use crate::rest_api::ListTablesResponse;
use moonlink_backend::REST_API_URI;
use moonlink_proto::moonlink as moonlink_pb;

use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use serde_json::json;
use tokio::sync::Mutex;
use tracing::error;

/// Database which manages all moonlink internal metrics.
const DATABASE: &str = "__reserved_moonlink_internal_metrics__";
/// Metrics attributes key for mooncake table id.
const MOONCAKE_TABLE_ID_KEY: &str = "moonlink.mooncake_table_id";

#[derive(Clone)]
pub(crate) struct MetricsHandler {
    /// IP/port for REST API.
    rest_addr: String,
    /// HTTP request client, used to access REST API.
    rest_client: reqwest::Client,
    /// All table names.
    tables: Arc<Mutex<HashSet<String>>>,
    /// Moonlink backend.
    moonlink_backend: Arc<moonlink_backend::MoonlinkBackend>,
}

/// Get string value from otel anyvalue.
fn anyvalue_as_str(v: &opentelemetry_proto::tonic::common::v1::AnyValue) -> Option<&str> {
    match &v.value {
        Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
            Some(s.as_str())
        }
        _ => None,
    }
}

/// Util function to deterministically get table name.
fn get_metrics_table_name(moonlink_table_name: &str, metrics_type: &str) -> String {
    format!("{moonlink_table_name}.{metrics_type}")
}

/// Util function to get metrics table name from the request.
fn get_metrics_table_name_from_request(req: &ExportMetricsServiceRequest) -> String {
    for rm in &req.resource_metrics {
        for sm in &rm.scope_metrics {
            for metric in &sm.metrics {
                match &metric.data {
                    Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(g)) => {
                        for dp in &g.data_points {
                            for attr in &dp.attributes {
                                if attr.key == MOONCAKE_TABLE_ID_KEY {
                                    let attr_value = attr.value.as_ref().unwrap();
                                    let mooncake_table_id = anyvalue_as_str(attr_value).unwrap();
                                    return get_metrics_table_name(mooncake_table_id, "gauge");
                                }
                            }
                        }
                    }
                    Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(s)) => {
                        for dp in &s.data_points {
                            for attr in &dp.attributes {
                                if attr.key == MOONCAKE_TABLE_ID_KEY {
                                    let attr_value = attr.value.as_ref().unwrap();
                                    let mooncake_table_id = anyvalue_as_str(attr_value).unwrap();
                                    return get_metrics_table_name(mooncake_table_id, "sum");
                                }
                            }
                        }
                    }
                    Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(h)) => {
                        for dp in &h.data_points {
                            for attr in &dp.attributes {
                                if attr.key == MOONCAKE_TABLE_ID_KEY {
                                    let attr_value = attr.value.as_ref().unwrap();
                                    let mooncake_table_id = anyvalue_as_str(attr_value).unwrap();
                                    return get_metrics_table_name(mooncake_table_id, "histogram");
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    panic!("Cannot find mooncake table id from the data points");
}

impl MetricsHandler {
    pub(crate) async fn new(
        rest_port: u16,
        moonlink_backend: Arc<moonlink_backend::MoonlinkBackend>,
    ) -> Result<Self> {
        let rest_addr = format!("http://0.0.0.0:{rest_port}");
        let rest_client = reqwest::Client::new();
        let response = rest_client
            .get(format!("{rest_addr}/tables"))
            .header("content-type", "application/json")
            .send()
            .await?;
        // TODO(hjiang): Error propagation.
        if !response.status().is_success() {
            return Err(Error::http_error(response.status()));
        }

        // List all internal metrics tables.
        let response: ListTablesResponse = response.json().await?;
        let tables = response
            .tables
            .into_iter()
            .filter(|cur_table_status| cur_table_status.database == DATABASE)
            .map(|cur_table_status| cur_table_status.table)
            .collect::<HashSet<_>>();
        let tables = Arc::new(Mutex::new(tables));
        Ok(Self {
            rest_addr,
            rest_client,
            tables,
            moonlink_backend,
        })
    }

    /// Create a mooncake table for once, if it hasn't been created.
    async fn create_table_for_once(&self, mooncake_table_id: &str) -> Result<()> {
        let crafted_src_table_name = format!("{DATABASE}.{mooncake_table_id}");
        // Fake REST ingestion.
        let serialized_table_config = json!({
            "mooncake": {
                "append_only": true,
                "row_identity": "None"
            }
        })
        .to_string();

        // Table creation for duplicate table name leads to error, so intentionally place table creation under critical section.
        // Performance is not a big concern here, since it only happens when new table metrics are received.
        {
            let mut guard = self.tables.lock().await;
            if guard.contains(mooncake_table_id) {
                return Ok(());
            }

            self.moonlink_backend
                .create_table(
                    DATABASE.to_string(),
                    mooncake_table_id.to_string(),
                    crafted_src_table_name,
                    REST_API_URI.to_string(),
                    serialized_table_config,
                    Some(otlp_metrics_gsh_schema()),
                )
                .await?;
            assert!(guard.insert(mooncake_table_id.to_string()));
        }

        Ok(())
    }

    /// Insert one single row via REST API, which handles LSN internally.
    /// Here we use asynchronous ingestion as best-effort attempt without flush or snapshot semantics.
    ///
    /// For any errors encountered during ingestion, simply log and proceed.
    async fn insert_row(&self, mooncake_table_id: &str, row_pb: moonlink_pb::MoonlinkRow) {
        let mut buf = Vec::new();
        // Serialization doesn't expect failure.
        prost::Message::encode(&row_pb, &mut buf).unwrap();
        let insert_payload = json!({
            "operation": "insert",
            "request_mode": "async",
            "data": buf
        });
        let crafted_src_table_name = format!("{DATABASE}.{mooncake_table_id}");
        let response = self
            .rest_client
            .post(format!(
                "{}/ingestpb/{}",
                self.rest_addr, crafted_src_table_name
            ))
            .header("content-type", "application/json")
            .json(&insert_payload)
            .send()
            .await;
        if response.is_err() {
            error!("Failed to ingest otel data: {:?}", response.unwrap_err());
        }
    }

    /// Handle request for the incoming metrics request.
    pub(crate) async fn handle_request(
        &self,
        request: ExportMetricsServiceRequest,
    ) -> Result<ExportMetricsServiceResponse> {
        // TODO(hjiang): Currently only supports metrics from one single table.
        let mooncake_table_id = get_metrics_table_name_from_request(&request);
        self.create_table_for_once(&mooncake_table_id).await?;

        let moonlink_row_pbs = otel_to_moonlink_pb::export_metrics_to_moonlink_rows(&request);
        for cur_row_pb in moonlink_row_pbs.into_iter() {
            self.insert_row(&mooncake_table_id, cur_row_pb).await;
        }
        Ok(ExportMetricsServiceResponse::default())
    }
}
