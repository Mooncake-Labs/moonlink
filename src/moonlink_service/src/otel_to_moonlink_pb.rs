use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    number_data_point, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, Sum,
};

use moonlink_pb::{Array, RowValue};
use moonlink_proto::moonlink as moonlink_pb;

/// Convert otel metrics to moonlink row protobuf.
#[allow(unused)]
pub fn export_metrics_to_moonlink_rows(
    req: &ExportMetricsServiceRequest,
) -> Vec<moonlink_pb::MoonlinkRow> {
    let mut rows = Vec::new();

    for rm in &req.resource_metrics {
        let resource_attrs = &rm
            .resource
            .as_ref()
            .map(|r| r.attributes.as_slice())
            .unwrap_or(&[]);
        for sm in &rm.scope_metrics {
            let scope_name = sm
                .scope
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_default();
            let scope_attrs = &sm
                .scope
                .as_ref()
                .map(|s| s.attributes.as_slice())
                .unwrap_or(&[]);

            for metric in &sm.metrics {
                match metric.data.as_ref() {
                    Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(Gauge {
                        data_points,
                    })) => {
                        for dp in data_points {
                            rows.push(number_point_row(
                                b"gauge",
                                metric,
                                resource_attrs,
                                &scope_name,
                                scope_attrs,
                                dp,
                                /*temporality=*/ -1,
                                /*is_monotonic=*/ false,
                            ));
                        }
                    }
                    Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(Sum {
                        data_points,
                        aggregation_temporality,
                        is_monotonic,
                    })) => {
                        let temp = *aggregation_temporality;
                        for dp in data_points {
                            rows.push(number_point_row(
                                b"sum",
                                metric,
                                resource_attrs,
                                &scope_name,
                                scope_attrs,
                                dp,
                                temp,
                                *is_monotonic,
                            ));
                        }
                    }
                    Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(
                        Histogram {
                            data_points,
                            aggregation_temporality,
                        },
                    )) => {
                        let temp = *aggregation_temporality;
                        for dp in data_points {
                            rows.push(hist_point_row(
                                metric,
                                resource_attrs,
                                &scope_name,
                                scope_attrs,
                                dp,
                                temp,
                            ));
                        }
                    }
                    _ => {
                        panic!("Unsupported metrics type: {metric:?}");
                    }
                }
            }
        }
    }

    rows
}

/// Build a [`MoonlinkRow`] representing a single numeric metric data point (Gauge or Sum) from an OpenTelemetry [`NumberDataPoint`].
#[allow(clippy::too_many_arguments)]
#[allow(clippy::vec_init_then_push)]
fn number_point_row(
    kind: &[u8], // "gauge" | "sum"
    metric: &Metric,
    resource_attrs: &[KeyValue],
    scope_name: &str,
    scope_attrs: &[KeyValue],
    dp: &NumberDataPoint,
    temporality: i32, // -1 for gauge
    is_monotonic: bool,
) -> moonlink_pb::MoonlinkRow {
    // TODO(hjiang): Add assertion on kind.
    let mut values = Vec::with_capacity(12);

    // 0: kind
    values.push(RowValue::bytes(kind.to_vec()));
    // 1: resource_attrs
    values.push(kvs_to_rowvalue_array(resource_attrs));
    // 2: scope_name
    values.push(RowValue::bytes(scope_name.to_string()));
    // 3: scope_attrs
    values.push(kvs_to_rowvalue_array(scope_attrs));
    // 4: metric_name
    values.push(RowValue::bytes(metric.name.clone()));
    // 5: metric_unit
    values.push(RowValue::bytes(metric.unit.clone()));
    // 6: point_attrs
    values.push(kvs_to_rowvalue_array(&dp.attributes));
    // 7: start_time_unix_nano
    values.push(RowValue::int64(dp.start_time_unix_nano as i64));
    // 8: time_unix_nano
    values.push(RowValue::int64(dp.time_unix_nano as i64));
    // 9: value (nullable)
    values.push(number_value_to_rowvalue(dp));
    // 10: temporality (int32; -1 for gauge)
    values.push(RowValue::int32(temporality));
    // 11: is_monotonic
    values.push(RowValue::bool(is_monotonic));

    moonlink_pb::MoonlinkRow { values }
}

/// Build a [`MoonlinkRow`] representing a single histogram metric data point from an OpenTelemetry [`HistogramDataPoint`].
#[allow(clippy::vec_init_then_push)]
fn hist_point_row(
    metric: &Metric,
    resource_attrs: &[KeyValue],
    scope_name: &str,
    scope_attrs: &[KeyValue],
    dp: &HistogramDataPoint,
    temporality: i32,
) -> moonlink_pb::MoonlinkRow {
    let mut values = Vec::with_capacity(14);

    // 0: kind
    values.push(RowValue::bytes(b"histogram".to_vec()));
    // 1: resource_attrs
    values.push(kvs_to_rowvalue_array(resource_attrs));
    // 2: scope_name
    values.push(RowValue::bytes(scope_name.to_string()));
    // 3: scope_attrs
    values.push(kvs_to_rowvalue_array(scope_attrs));
    // 4: metric_name
    values.push(RowValue::bytes(metric.name.clone()));
    // 5: metric_unit
    values.push(RowValue::bytes(metric.unit.clone()));
    // 6: point_attrs
    values.push(kvs_to_rowvalue_array(&dp.attributes));
    // 7: start_time_unix_nano
    values.push(RowValue::int64(dp.start_time_unix_nano as i64));
    // 8: time_unix_nano
    values.push(RowValue::int64(dp.time_unix_nano as i64));
    // 9: count
    values.push(RowValue::int64(dp.count as i64));
    // 10: sum (nullable)
    values.push(match dp.sum {
        Some(v) => RowValue::float64(v),
        None => RowValue::null(),
    });
    // 11: explicit_bounds (array of float64)
    values.push(RowValue::array(Array {
        values: dp
            .explicit_bounds
            .iter()
            .map(|v| RowValue::float64(*v))
            .collect(),
    }));
    // 12: bucket_counts (array of int64)
    values.push(RowValue::array(Array {
        values: dp
            .bucket_counts
            .iter()
            .map(|v| RowValue::int64(*v as i64))
            .collect(),
    }));
    // 13: temporality
    values.push(RowValue::int32(temporality));

    moonlink_pb::MoonlinkRow { values }
}

/// Util function to convert key-value into row value array.
/// TODO(hjiang): Worth revisiting whether arrat is a good data structure.
fn kvs_to_rowvalue_array(kvs: &[KeyValue]) -> RowValue {
    // Encoded as: array of [ key(bytes), value(RowValue) ]
    let mut out = Vec::with_capacity(kvs.len());
    for kv in kvs {
        let key = RowValue::bytes(kv.key.clone());
        let val = any_to_rowvalue(kv.value.as_ref());
        out.push(RowValue::array(Array {
            values: vec![key, val],
        }));
    }
    RowValue::array(Array { values: out })
}

/// Util function to convert otel any value to row value.
fn any_to_rowvalue(v: Option<&AnyValue>) -> RowValue {
    match v.and_then(|a| a.value.as_ref()) {
        Some(any_value::Value::StringValue(s)) => RowValue::bytes(s.to_string()),
        Some(any_value::Value::BoolValue(b)) => RowValue::bool(*b),
        Some(any_value::Value::IntValue(i)) => RowValue::int64(*i),
        Some(any_value::Value::DoubleValue(f)) => RowValue::float64(*f),
        Some(any_value::Value::BytesValue(b)) => RowValue::bytes(b.clone()),
        Some(any_value::Value::ArrayValue(arr)) => RowValue::array(Array {
            values: arr
                .values
                .iter()
                .map(|x| any_to_rowvalue(Some(x)))
                .collect(),
        }),
        Some(any_value::Value::KvlistValue(kvl)) => {
            // Turn nested map into array of [key, value] entries
            let entries: Vec<RowValue> = kvl
                .values
                .iter()
                .map(|kv| {
                    RowValue::array(Array {
                        values: vec![
                            RowValue::bytes(kv.key.clone()),
                            any_to_rowvalue(kv.value.as_ref()),
                        ],
                    })
                })
                .collect();
            RowValue::array(Array { values: entries })
        }
        None => RowValue::null(),
    }
}

/// Util function to convert otel number to row value.
fn number_value_to_rowvalue(dp: &NumberDataPoint) -> RowValue {
    match dp.value.as_ref() {
        Some(number_data_point::Value::AsDouble(v)) => RowValue::float64(*v),
        Some(number_data_point::Value::AsInt(v)) => RowValue::int64(*v),
        None => RowValue::null(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moonlink_pb::{row_value, Array, RowValue};
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{
        any_value, AnyValue, InstrumentationScope, KeyValue,
    };
    use opentelemetry_proto::tonic::metrics::v1::{
        metric, AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric,
        NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn kv_str(key: &str, val: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(val.to_string())),
            }),
        }
    }
    fn kv_bool(key: &str, val: bool) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::BoolValue(val)),
            }),
        }
    }
    fn kv_i64(key: &str, val: i64) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(val)),
            }),
        }
    }
    fn any_array(vals: Vec<AnyValue>) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::ArrayValue(
                opentelemetry_proto::tonic::common::v1::ArrayValue { values: vals },
            )),
        }
    }
    fn any_kvlist(kvs: Vec<KeyValue>) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList { values: kvs },
            )),
        }
    }

    fn make_req_with_metrics(
        metrics: Vec<Metric>,
        resource_attrs: Vec<KeyValue>,
        scope_name: &str,
        scope_attrs: Vec<KeyValue>,
    ) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: resource_attrs,
                    dropped_attributes_count: 0,
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

    fn as_bytes(rv: &RowValue) -> Option<Vec<u8>> {
        match rv.kind.as_ref()? {
            row_value::Kind::Bytes(b) => Some(b.clone().to_vec()),
            _ => None,
        }
    }
    fn as_i32(rv: &RowValue) -> Option<i32> {
        match rv.kind.as_ref()? {
            row_value::Kind::Int32(v) => Some(*v),
            _ => None,
        }
    }
    fn as_i64(rv: &RowValue) -> Option<i64> {
        match rv.kind.as_ref()? {
            row_value::Kind::Int64(v) => Some(*v),
            _ => None,
        }
    }
    fn as_f64(rv: &RowValue) -> Option<f64> {
        match rv.kind.as_ref()? {
            row_value::Kind::Float64(v) => Some(*v),
            _ => None,
        }
    }
    fn as_bool(rv: &RowValue) -> Option<bool> {
        match rv.kind.as_ref()? {
            row_value::Kind::Bool(v) => Some(*v),
            _ => None,
        }
    }
    fn as_array(rv: &RowValue) -> Option<&Array> {
        match rv.kind.as_ref()? {
            row_value::Kind::Array(a) => Some(a),
            _ => None,
        }
    }
    fn is_null(rv: &RowValue) -> bool {
        matches!(rv.kind, Some(row_value::Kind::Null(_)))
    }

    #[test]
    fn test_gauge_number_point() {
        let dp = NumberDataPoint {
            attributes: vec![kv_str("dp_k", "dp_v")],
            start_time_unix_nano: 11,
            time_unix_nano: 22,
            value: Some(
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(
                    std::f64::consts::PI,
                ),
            ),
            exemplars: vec![],
            flags: 0,
        };

        let metric = Metric {
            name: "latency".into(),
            description: "".into(),
            unit: "ms".into(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![dp],
            })),
        };

        let req = make_req_with_metrics(
            vec![metric],
            vec![kv_str("res_k", "res_v")],
            "myscope",
            vec![kv_bool("scope_ok", true)],
        );

        let rows = export_metrics_to_moonlink_rows(&req);
        assert_eq!(rows.len(), 1);
        let r = &rows[0].values;
        assert_eq!(r.len(), 12);
        // Columns:
        // 0 kind, 1 resource_attrs, 2 scope_name, 3 scope_attrs, 4 metric_name, 5 unit,
        // 6 point_attrs, 7 start, 8 time, 9 value, 10 temporality (-1), 11 is_monotonic(false)
        assert_eq!(as_bytes(&r[0]).unwrap(), b"gauge".to_vec());
        // resource attrs -> array of [key, val] entries
        let res_attrs = as_array(&r[1]).unwrap();
        assert_eq!(res_attrs.values.len(), 1);
        let entry = as_array(&res_attrs.values[0]).unwrap();
        assert_eq!(as_bytes(&entry.values[0]).unwrap(), b"res_k".to_vec());
        assert_eq!(as_bytes(&entry.values[1]).unwrap(), b"res_v".to_vec());

        assert_eq!(as_bytes(&r[2]).unwrap(), b"myscope".to_vec());
        let scope_attrs = as_array(&r[3]).unwrap();
        assert_eq!(scope_attrs.values.len(), 1);
        let sa0 = as_array(&scope_attrs.values[0]).unwrap();
        assert_eq!(as_bytes(&sa0.values[0]).unwrap(), b"scope_ok".to_vec());
        assert!(as_bool(&sa0.values[1]).unwrap());

        assert_eq!(as_bytes(&r[4]).unwrap(), b"latency".to_vec());
        assert_eq!(as_bytes(&r[5]).unwrap(), b"ms".to_vec());

        let point_attrs = as_array(&r[6]).unwrap();
        let pa0 = as_array(&point_attrs.values[0]).unwrap();
        assert_eq!(as_bytes(&pa0.values[0]).unwrap(), b"dp_k".to_vec());
        assert_eq!(as_bytes(&pa0.values[1]).unwrap(), b"dp_v".to_vec());

        assert_eq!(as_i64(&r[7]).unwrap(), 11);
        assert_eq!(as_i64(&r[8]).unwrap(), 22);
        assert!((as_f64(&r[9]).unwrap() - std::f64::consts::PI).abs() < 1e-9);
        assert_eq!(as_i32(&r[10]).unwrap(), -1);
        assert!(!as_bool(&r[11]).unwrap());
    }

    #[test]
    fn test_sum_number_point_int_and_nested_attrs() {
        let arr_any = any_array(vec![
            AnyValue {
                value: Some(any_value::Value::BoolValue(true)),
            },
            AnyValue {
                value: Some(any_value::Value::DoubleValue(1.5)),
            },
        ]);
        let kvlist_any = any_kvlist(vec![kv_str("x", "y"), kv_i64("n", 42)]);
        let dp_attrs = vec![
            KeyValue {
                key: "arr".into(),
                value: Some(arr_any),
            },
            KeyValue {
                key: "m".into(),
                value: Some(kvlist_any),
            },
        ];

        let dp = NumberDataPoint {
            attributes: dp_attrs,
            start_time_unix_nano: 1000,
            time_unix_nano: 2000,
            value: Some(
                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(7),
            ),
            exemplars: vec![],
            flags: 0,
        };

        let metric = Metric {
            name: "requests".into(),
            description: "".into(),
            unit: "1".into(),
            metadata: vec![],
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![dp],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
        };

        let req = make_req_with_metrics(
            vec![metric],
            /*resource_attrs=*/ vec![],
            /*scope_name=*/ "svc",
            /*scope_attrs=*/ vec![],
        );
        let rows = export_metrics_to_moonlink_rows(&req);
        assert_eq!(rows.len(), 1);
        let r = &rows[0].values;
        assert_eq!(r.len(), 12);

        assert_eq!(as_bytes(&r[0]).unwrap(), b"sum".to_vec());
        let res_attrs = as_array(&r[1]).unwrap();
        assert!(res_attrs.values.is_empty());
        assert_eq!(as_bytes(&r[2]).unwrap(), b"svc".to_vec());
        assert_eq!(as_bytes(&r[4]).unwrap(), b"requests".to_vec());
        assert_eq!(as_bytes(&r[5]).unwrap(), b"1".to_vec());
        assert_eq!(as_i64(&r[9]).unwrap(), 7);
        assert_eq!(
            as_i32(&r[10]).unwrap(),
            AggregationTemporality::Cumulative as i32
        );
        assert!(as_bool(&r[11]).unwrap());

        // Check key-value pair conversion.
        let point_attrs = as_array(&r[6]).unwrap();
        assert_eq!(point_attrs.values.len(), 2);

        // "arr" -> [true, 1.5]
        let arr_entry = as_array(&point_attrs.values[0]).unwrap();
        assert_eq!(as_bytes(&arr_entry.values[0]).unwrap(), b"arr".to_vec());
        let arr_val = as_array(&arr_entry.values[1]).unwrap();
        assert_eq!(arr_val.values.len(), 2);
        assert!(as_bool(&arr_val.values[0]).unwrap());
        assert!((as_f64(&arr_val.values[1]).unwrap() - 1.5).abs() < 1e-9);

        // "m" -> kvlist => array of [key, val]
        let map_entry = as_array(&point_attrs.values[1]).unwrap();
        assert_eq!(as_bytes(&map_entry.values[0]).unwrap(), b"m".to_vec());
        let kv_array = as_array(&map_entry.values[1]).unwrap();
        assert_eq!(kv_array.values.len(), 2);

        let kv0 = as_array(&kv_array.values[0]).unwrap();
        assert_eq!(as_bytes(&kv0.values[0]).unwrap(), b"x".to_vec());
        assert_eq!(as_bytes(&kv0.values[1]).unwrap(), b"y".to_vec());

        let kv1 = as_array(&kv_array.values[1]).unwrap();
        assert_eq!(as_bytes(&kv1.values[0]).unwrap(), b"n".to_vec());
        assert_eq!(as_i64(&kv1.values[1]).unwrap(), 42);
    }

    #[test]
    fn test_histogram_with_and_without_sum() {
        // 1st histogram point with sum
        let dp1 = HistogramDataPoint {
            attributes: vec![kv_str("h", "a")],
            start_time_unix_nano: 1,
            time_unix_nano: 2,
            count: 3,
            sum: Some(4.5),
            bucket_counts: vec![1, 2, 0, 0],
            explicit_bounds: vec![0.0, 5.0, 10.0],
            exemplars: vec![],
            flags: 0,
            min: None,
            max: None,
        };
        // 2nd histogram point without sum
        let dp2 = HistogramDataPoint {
            sum: None,
            ..dp1.clone()
        };

        let metric = Metric {
            name: "latency_hist".into(),
            description: "".into(),
            unit: "ms".into(),
            metadata: vec![],
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![dp1, dp2],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
        };

        let req = make_req_with_metrics(
            vec![metric],
            /*resource_attrs=*/ vec![],
            /*scope_name=*/ "scope",
            /*scope_attrs=*/ vec![],
        );
        let rows = export_metrics_to_moonlink_rows(&req);
        assert_eq!(rows.len(), 2);

        // First row checks.
        {
            let r = &rows[0].values;
            assert_eq!(r.len(), 14);
            assert_eq!(as_bytes(&r[0]).unwrap(), b"histogram".to_vec());
            assert!(as_array(&r[1]).unwrap().values.is_empty()); // resource attrs
            assert_eq!(as_bytes(&r[2]).unwrap(), b"scope".to_vec()); // scope name
            assert!(as_array(&r[3]).unwrap().values.is_empty()); // scope attrs
            assert_eq!(as_bytes(&r[4]).unwrap(), b"latency_hist".to_vec()); // metrics name
            assert_eq!(as_bytes(&r[5]).unwrap(), b"ms".to_vec()); // unit
            assert_eq!(as_array(&r[6]).unwrap().values.len(), 1); // point attrs
            assert_eq!(as_i64(&r[9]).unwrap(), 3); // count
            assert!((as_f64(&r[10]).unwrap() - 4.5).abs() < 1e-9); // sum
            let bounds = as_array(&r[11]).unwrap();
            assert_eq!(bounds.values.len(), 3);
            assert!((as_f64(&bounds.values[0]).unwrap() - 0.0).abs() < 1e-9);
            assert!((as_f64(&bounds.values[1]).unwrap() - 5.0).abs() < 1e-9);
            assert!((as_f64(&bounds.values[2]).unwrap() - 10.0).abs() < 1e-9);

            let counts = as_array(&r[12]).unwrap();
            assert_eq!(counts.values.len(), 4);
            assert_eq!(as_i64(&counts.values[0]).unwrap(), 1);
            assert_eq!(as_i64(&counts.values[1]).unwrap(), 2);
            assert_eq!(as_i64(&counts.values[2]).unwrap(), 0);
            assert_eq!(as_i64(&counts.values[3]).unwrap(), 0);

            assert_eq!(
                as_i32(&r[13]).unwrap(),
                AggregationTemporality::Delta as i32
            );
        }

        // Second row (no sum).
        {
            let r = &rows[1].values;
            assert_eq!(r.len(), 14);
            assert!(is_null(&r[10])); // sum is null
        }
    }
}
