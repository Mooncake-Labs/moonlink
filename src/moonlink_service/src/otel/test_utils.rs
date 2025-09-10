/// Test util function to otel requests.
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    any_value, AnyValue, EntityRef, InstrumentationScope, KeyValue,
};
use opentelemetry_proto::tonic::metrics::v1::{Metric, ResourceMetrics, ScopeMetrics};
use opentelemetry_proto::tonic::resource::v1::Resource;

pub(crate) fn kv_str(key: &str, val: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(val.to_string())),
        }),
    }
}
pub(crate) fn kv_bool(key: &str, val: bool) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::BoolValue(val)),
        }),
    }
}
pub(crate) fn kv_i64(key: &str, val: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(val)),
        }),
    }
}
pub(crate) fn any_array(vals: Vec<AnyValue>) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::ArrayValue(
            opentelemetry_proto::tonic::common::v1::ArrayValue { values: vals },
        )),
    }
}
pub(crate) fn any_kvlist(kvs: Vec<KeyValue>) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::KvlistValue(
            opentelemetry_proto::tonic::common::v1::KeyValueList { values: kvs },
        )),
    }
}

/// Util functions to create otel service request.
pub(crate) fn make_req_with_metrics(
    metrics: Vec<Metric>,
    resource_attrs: Vec<KeyValue>,
    resource_entity_refs: Vec<EntityRef>,
    scope_name: &str,
    scope_attrs: Vec<KeyValue>,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: resource_attrs,
                dropped_attributes_count: 0,
                entity_refs: resource_entity_refs,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: scope_name.to_string(),
                    version: "".into(),
                    attributes: scope_attrs,
                    dropped_attributes_count: 0,
                }),
                metrics,
                schema_url: "".into(),
            }],
            schema_url: "".into(),
        }],
    }
}
