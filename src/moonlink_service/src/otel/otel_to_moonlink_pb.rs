use std::collections::HashMap;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, EntityRef, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    number_data_point, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, Sum,
};

use moonlink_pb::{Array, RowValue};
use moonlink_proto::moonlink as moonlink_pb;

#[allow(unused)]
pub fn export_metrics_to_moonlink_rows(
    req: &ExportMetricsServiceRequest,
) -> Vec<moonlink_pb::MoonlinkRow> {
    let mut rows = Vec::new();

    for rm in &req.resource_metrics {
        let resource_attrs = rm
            .resource
            .as_ref()
            .map(|r| r.attributes.as_slice())
            .unwrap_or(&[]);
        let resource_entity_refs = rm
            .resource
            .as_ref()
            .map(|r| r.entity_refs.as_slice())
            .unwrap_or(&[]);

        for sm in &rm.scope_metrics {
            let scope_name = sm
                .scope
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_default();
            let scope_attrs = sm
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
                                resource_entity_refs,
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
                                resource_entity_refs,
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
                                resource_entity_refs,
                                &scope_name,
                                scope_attrs,
                                dp,
                                temp,
                            ));
                        }
                    }
                    _ => {
                        // Extend as needed for other metric types.
                        continue;
                    }
                }
            }
        }
    }

    rows
}

/// Util function to make moonlink struct type.
fn make_struct(fields: Vec<RowValue>) -> RowValue {
    RowValue {
        kind: Some(moonlink_pb::row_value::Kind::Struct(moonlink_pb::Struct {
            fields,
        })),
    }
}

/// Util function to convert otel [`AnyValue`] to moonlink value.
fn anyvalue_to_struct(v: Option<&AnyValue>) -> RowValue {
    // fixed order: string, int, double, bool, bytes
    let mut fields = vec![
        RowValue::null(),
        RowValue::null(),
        RowValue::null(),
        RowValue::null(),
        RowValue::null(),
    ];

    if let Some(val) = v.and_then(|a| a.value.as_ref()) {
        match val {
            any_value::Value::StringValue(s) => fields[0] = RowValue::bytes(s.to_string()),
            any_value::Value::IntValue(i) => fields[1] = RowValue::int64(*i),
            any_value::Value::DoubleValue(d) => fields[2] = RowValue::float64(*d),
            any_value::Value::BoolValue(b) => fields[3] = RowValue::bool(*b),
            any_value::Value::BytesValue(b) => fields[4] = RowValue::bytes(b.clone()),
            // TODO(hjiang): Support nested type for anytype.
            _ => {}
        }
    }
    make_struct(fields)
}

// attributes => List<Struct{ key: Utf8, value: AnyValueStruct }>
fn kvs_to_rowvalue_array_anyvalue(kvs: &[KeyValue]) -> RowValue {
    let mut out = Vec::with_capacity(kvs.len());
    for kv in kvs {
        // Each list element must be a *Struct* matching the Arrow item type:
        // Struct { key: Utf8, value: AnyValueStruct }
        out.push(make_struct(vec![
            RowValue::bytes(kv.key.clone()),
            anyvalue_to_struct(kv.value.as_ref()),
        ]));
    }
    RowValue::array(Array { values: out })
}

// Convert resource entity_refs into: List<Struct{
//   type: Utf8,
//   id_pairs: List<Struct{key: Utf8, value: AnyValueStruct}>,
//   description_pairs: List<Struct{key: Utf8, value: AnyValueStruct}>,
//   schema_url: Utf8
// }>
fn entityrefs_to_rowvalue_array(entity_refs: &[EntityRef], attrs: &[KeyValue]) -> RowValue {
    // Build a map from resource attrs to fill id/description pairs
    let mut attr_map: HashMap<&str, &AnyValue> = HashMap::with_capacity(attrs.len());
    for kv in attrs {
        if let Some(v) = kv.value.as_ref() {
            assert!(attr_map.insert(kv.key.as_str(), v).is_none());
        }
    }

    let mut out = Vec::with_capacity(entity_refs.len());
    for er in entity_refs {
        // id_pairs: List<Struct{ key, value }>
        let id_pairs = er
            .id_keys
            .iter()
            .map(|k| {
                let val = attr_map.get(k.as_str()).copied();
                make_struct(vec![RowValue::bytes(k.clone()), anyvalue_to_struct(val)])
            })
            .collect::<Vec<_>>();

        // description_pairs: List<Struct{ key, value }>
        let desc_pairs = er
            .description_keys
            .iter()
            .map(|k| {
                let val = attr_map.get(k.as_str()).copied();
                make_struct(vec![RowValue::bytes(k.clone()), anyvalue_to_struct(val)])
            })
            .collect::<Vec<_>>();

        // Each entity ref item itself must be a *Struct* (not an Array) to match the Arrow item type.
        let type_val = if er.r#type.is_empty() {
            RowValue::null()
        } else {
            RowValue::bytes(er.r#type.clone())
        };
        let schema_url_val = if er.schema_url.is_empty() {
            RowValue::null()
        } else {
            RowValue::bytes(er.schema_url.clone())
        };
        out.push(make_struct(vec![
            type_val,
            RowValue::array(Array { values: id_pairs }),
            RowValue::array(Array { values: desc_pairs }),
            schema_url_val,
        ]));
    }

    RowValue::array(Array { values: out })
}

// Split number into (int_col, double_col)
fn number_pair(dp: &NumberDataPoint) -> (RowValue, RowValue) {
    match dp.value.as_ref() {
        Some(number_data_point::Value::AsDouble(v)) => (RowValue::null(), RowValue::float64(*v)),
        Some(number_data_point::Value::AsInt(v)) => (RowValue::int64(*v), RowValue::null()),
        None => (RowValue::null(), RowValue::null()),
    }
}

/// Build a [`MoonlinkRow`] representing a single numeric metric data point (Gauge or Sum) from an OpenTelemetry [`NumberDataPoint`].
#[allow(clippy::too_many_arguments)]
#[allow(clippy::vec_init_then_push)]
fn number_point_row(
    kind: &[u8], // "gauge" | "sum"
    metric: &Metric,
    resource_attrs: &[KeyValue],
    resource_entity_refs: &[EntityRef],
    scope_name: &str,
    scope_attrs: &[KeyValue],
    dp: &NumberDataPoint,
    temporality: i32, // -1 for gauge
    is_monotonic: bool,
) -> moonlink_pb::MoonlinkRow {
    // TODO(hjiang): Add assertion on kind.
    let (num_i, num_d) = number_pair(dp);
    let mut values = Vec::with_capacity(21);

    // 0 kind
    values.push(RowValue::bytes(kind.to_vec()));
    // 1 resource_attributes
    values.push(kvs_to_rowvalue_array_anyvalue(resource_attrs));
    // 2 resource_entity_refs
    values.push(entityrefs_to_rowvalue_array(
        resource_entity_refs,
        resource_attrs,
    ));
    // 3 resource_dropped_attributes_count
    values.push(RowValue::null());
    // 4 resource_schema_url
    values.push(RowValue::null());
    // 5 scope_name
    values.push(RowValue::bytes(scope_name.to_string()));
    // 6 scope_version
    values.push(RowValue::null());
    // 7 scope_attributes
    values.push(kvs_to_rowvalue_array_anyvalue(scope_attrs));
    // 8 scope_dropped_attributes_count
    values.push(RowValue::null());
    // 9 scope_schema_url
    values.push(RowValue::null());
    // 10 metric_name
    values.push(RowValue::bytes(metric.name.clone()));
    // 11 metric_description
    values.push(RowValue::null());
    // 12 metric_unit
    values.push(RowValue::bytes(metric.unit.clone()));
    // 13 start_time_unix_nano
    values.push(RowValue::int64(dp.start_time_unix_nano as i64));
    // 14 time_unix_nano
    values.push(RowValue::int64(dp.time_unix_nano as i64));
    // 15 point_attributes
    values.push(kvs_to_rowvalue_array_anyvalue(&dp.attributes));
    // 16 point_dropped_attributes_count
    values.push(RowValue::null());
    // 17 number_int
    values.push(num_i);
    // 18 number_double
    values.push(num_d);
    // 19 temporality
    values.push(RowValue::int32(temporality));
    // 20 is_monotonic
    values.push(RowValue::bool(is_monotonic));

    moonlink_pb::MoonlinkRow { values }
}

/// Build a [`MoonlinkRow`] representing a single histogram metric data point from an OpenTelemetry [`HistogramDataPoint`].
#[allow(clippy::vec_init_then_push)]
fn hist_point_row(
    metric: &Metric,
    resource_attrs: &[KeyValue],
    resource_entity_refs: &[EntityRef],
    scope_name: &str,
    scope_attrs: &[KeyValue],
    dp: &HistogramDataPoint,
    hist_temporality: i32,
) -> moonlink_pb::MoonlinkRow {
    let mut values = Vec::with_capacity(29);

    // 0 kind
    values.push(RowValue::bytes(b"histogram".to_vec()));
    // 1 resource_attributes TODO
    values.push(kvs_to_rowvalue_array_anyvalue(resource_attrs));
    // 2 resource_entity_refs
    values.push(entityrefs_to_rowvalue_array(
        resource_entity_refs,
        resource_attrs,
    ));
    // 3 resource_dropped_attributes_count
    values.push(RowValue::null());
    // 4 resource_schema_url
    values.push(RowValue::null());
    // 5 scope_name
    values.push(RowValue::bytes(scope_name.to_string()));
    // 6 scope_version
    values.push(RowValue::null());
    // 7 scope_attributes
    values.push(kvs_to_rowvalue_array_anyvalue(scope_attrs));
    // 8 scope_dropped_attributes_count
    values.push(RowValue::null());
    // 9 scope_schema_url
    values.push(RowValue::null());
    // 10 metric_name
    values.push(RowValue::bytes(metric.name.clone()));
    // 11 metric_description
    values.push(RowValue::null());
    // 12 metric_unit
    values.push(RowValue::bytes(metric.unit.clone()));
    // 13 start_time_unix_nano
    values.push(RowValue::int64(dp.start_time_unix_nano as i64));
    // 14 time_unix_nano
    values.push(RowValue::int64(dp.time_unix_nano as i64));
    // 15 point_attributes
    values.push(kvs_to_rowvalue_array_anyvalue(&dp.attributes));
    // 16 point_dropped_attributes_count
    values.push(RowValue::null());
    // 17 number_int        (hist => null)
    values.push(RowValue::null());
    // 18 number_double     (hist => null)
    values.push(RowValue::null());
    // 19 temporality       (hist => null; use hist_temporality at col 27 below)
    values.push(RowValue::null());
    // 20 is_monotonic      (hist => null)
    values.push(RowValue::null());
    // 21 exemplars (number exemplars) => null for histogram rows
    values.push(RowValue::null());
    // 22 hist_count
    values.push(RowValue::int64(dp.count as i64));
    // 23 hist_sum
    values.push(dp.sum.map(RowValue::float64).unwrap_or_else(RowValue::null));
    // 24 hist_min
    values.push(dp.min.map(RowValue::float64).unwrap_or_else(RowValue::null));
    // 25 hist_max
    values.push(dp.max.map(RowValue::float64).unwrap_or_else(RowValue::null));
    // 26 explicit_bounds (array<float64>)
    values.push(RowValue::array(Array {
        values: dp
            .explicit_bounds
            .iter()
            .map(|v| RowValue::float64(*v))
            .collect(),
    }));
    // 27 bucket_counts (array<int64>)
    values.push(RowValue::array(Array {
        values: dp
            .bucket_counts
            .iter()
            .map(|v| RowValue::int64(*v as i64))
            .collect(),
    }));
    // 28 hist_temporality
    values.push(RowValue::int32(hist_temporality));

    moonlink_pb::MoonlinkRow { values }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otel::test_utils::*;
    use moonlink_pb::{row_value, Array, RowValue, Struct as MlStruct};
    use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, EntityRef, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::{
        metric, AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric,
        NumberDataPoint, Sum,
    };

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
    fn as_struct(rv: &RowValue) -> Option<&MlStruct> {
        match rv.kind.as_ref()? {
            row_value::Kind::Struct(s) => Some(s),
            _ => None,
        }
    }
    fn is_null(rv: &RowValue) -> bool {
        matches!(rv.kind, Some(row_value::Kind::Null(_)))
    }
    fn any_slots(rv: &RowValue) -> Option<&[RowValue]> {
        Some(&as_struct(rv)?.fields)
    }
    fn any_is_string_bytes(rv: &RowValue, expected: &[u8]) -> bool {
        any_slots(rv)
            .and_then(|f| f.first())
            .and_then(as_bytes)
            .map(|b| b == expected)
            .unwrap_or(false)
    }
    fn any_get_bool(rv: &RowValue) -> Option<bool> {
        any_slots(rv).and_then(|f| f.get(3)).and_then(as_bool)
    }
    fn any_all_null(rv: &RowValue) -> bool {
        if let Some(fs) = any_slots(rv) {
            fs.iter().all(is_null)
        } else {
            false
        }
    }

    #[test]
    fn test_gauge_number_point_with_entity_refs() {
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
            vec![EntityRef {
                r#type: "service".into(),
                id_keys: vec!["res_k".into()],
                description_keys: vec![],
                schema_url: "".into(),
            }],
            "myscope",
            vec![kv_bool("scope_ok", true)],
        );

        let rows = export_metrics_to_moonlink_rows(&req);
        assert_eq!(rows.len(), 1);
        let r = &rows[0].values;
        assert_eq!(r.len(), 21, "number row should have 21 columns (0..=20)");

        // Indices per new layout:
        // 0 kind, 1 res_attrs, 2 entity_refs, 3 res_drop, 4 res_schema_url,
        // 5 scope_name, 6 scope_version, 7 scope_attrs, 8 scope_drop, 9 scope_schema_url,
        // 10 metric_name, 11 metric_desc, 12 metric_unit,
        // 13 start, 14 time, 15 point_attrs, 16 point_drop,
        // 17 number_int, 18 number_double, 19 temporality, 20 is_monotonic
        assert_eq!(as_bytes(&r[0]).unwrap(), b"gauge".to_vec());

        // resource attrs -> Array<Struct{key, any_struct}>
        let res_attrs = as_array(&r[1]).unwrap();
        assert_eq!(res_attrs.values.len(), 1);
        let entry = as_struct(&res_attrs.values[0]).unwrap();
        assert_eq!(as_bytes(&entry.fields[0]).unwrap(), b"res_k".to_vec());
        assert!(any_is_string_bytes(&entry.fields[1], b"res_v"));

        // entity_refs -> Array<Struct{type, id_pairs, desc_pairs, schema_url}>
        let ers = as_array(&r[2]).unwrap();
        assert_eq!(ers.values.len(), 1);
        let er0 = as_struct(&ers.values[0]).unwrap();
        assert_eq!(as_bytes(&er0.fields[0]).unwrap(), b"service".to_vec());

        // id_pairs: Array<Struct{key, any_struct}>
        let id_pairs = as_array(&er0.fields[1]).unwrap();
        assert_eq!(id_pairs.values.len(), 1);
        let id0 = as_struct(&id_pairs.values[0]).unwrap();
        assert_eq!(as_bytes(&id0.fields[0]).unwrap(), b"res_k".to_vec());
        assert!(any_is_string_bytes(&id0.fields[1], b"res_v"));

        // scope
        assert_eq!(as_bytes(&r[5]).unwrap(), b"myscope".to_vec());
        let scope_attrs = as_array(&r[7]).unwrap();
        assert_eq!(scope_attrs.values.len(), 1);
        let sa0 = as_struct(&scope_attrs.values[0]).unwrap();
        assert_eq!(as_bytes(&sa0.fields[0]).unwrap(), b"scope_ok".to_vec());
        assert!(any_get_bool(&sa0.fields[1]).unwrap());

        // metric
        assert_eq!(as_bytes(&r[10]).unwrap(), b"latency".to_vec());
        assert_eq!(as_bytes(&r[12]).unwrap(), b"ms".to_vec());

        // point
        let point_attrs = as_array(&r[15]).unwrap();
        let pa0 = as_struct(&point_attrs.values[0]).unwrap();
        assert_eq!(as_bytes(&pa0.fields[0]).unwrap(), b"dp_k".to_vec());
        assert!(any_is_string_bytes(&pa0.fields[1], b"dp_v"));

        assert_eq!(as_i64(&r[13]).unwrap(), 11);
        assert_eq!(as_i64(&r[14]).unwrap(), 22);
        assert!((as_f64(&r[18]).unwrap() - std::f64::consts::PI).abs() < 1e-9); // number_double
        assert_eq!(as_i32(&r[19]).unwrap(), -1); // temporality
        assert!(!as_bool(&r[20]).unwrap()); // is_monotonic
    }

    #[test]
    fn test_sum_number_point_int_and_nested_attrs() {
        // These nested AnyValue kinds are currently encoded as an AnyValueStruct with all-null slots.
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
            /*resource_entity_refs=*/ vec![],
            /*scope_name=*/ "svc",
            /*scope_attrs=*/ vec![],
        );
        let rows = export_metrics_to_moonlink_rows(&req);
        assert_eq!(rows.len(), 1);
        let r = &rows[0].values;
        assert_eq!(r.len(), 21);

        assert_eq!(as_bytes(&r[0]).unwrap(), b"sum".to_vec());
        assert!(as_array(&r[1]).unwrap().values.is_empty()); // resource attrs
                                                             // r[2] entity_refs empty

        assert_eq!(as_bytes(&r[5]).unwrap(), b"svc".to_vec());
        assert_eq!(as_bytes(&r[10]).unwrap(), b"requests".to_vec());
        assert_eq!(as_bytes(&r[12]).unwrap(), b"1".to_vec());

        // value is int -> column 17
        assert_eq!(as_i64(&r[17]).unwrap(), 7);
        assert_eq!(
            as_i32(&r[19]).unwrap(),
            AggregationTemporality::Cumulative as i32
        );
        assert!(as_bool(&r[20]).unwrap());

        // point attributes at col 15: Array<Struct{key, any_struct}>
        let point_attrs = as_array(&r[15]).unwrap();
        assert_eq!(point_attrs.values.len(), 2);

        // "arr" -> AnyValueStruct with all-null slots (policy for ArrayValue)
        {
            let arr_entry = as_struct(&point_attrs.values[0]).unwrap();
            assert_eq!(as_bytes(&arr_entry.fields[0]).unwrap(), b"arr".to_vec());
            let any_struct = &arr_entry.fields[1];
            assert!(any_all_null(any_struct));
        }

        // "m" -> AnyValueStruct with all-null slots (policy for KvlistValue)
        {
            let map_entry = as_struct(&point_attrs.values[1]).unwrap();
            assert_eq!(as_bytes(&map_entry.fields[0]).unwrap(), b"m".to_vec());
            let any_struct = &map_entry.fields[1];
            assert!(any_all_null(any_struct));
        }
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
            /*resource_entity_refs=*/ vec![],
            /*scope_name=*/ "scope",
            /*scope_attrs=*/ vec![],
        );
        let rows = export_metrics_to_moonlink_rows(&req);
        assert_eq!(rows.len(), 2);

        // First row checks (with sum).
        {
            let r = &rows[0].values;
            assert_eq!(r.len(), 29, "histogram row should have 29 columns (0..=28)");
            assert_eq!(as_bytes(&r[0]).unwrap(), b"histogram".to_vec());
            assert!(as_array(&r[1]).unwrap().values.is_empty()); // resource attrs
                                                                 // r[2] entity_refs empty
            assert_eq!(as_bytes(&r[5]).unwrap(), b"scope".to_vec()); // scope name
            assert!(as_array(&r[7]).unwrap().values.is_empty()); // scope attrs
            assert_eq!(as_bytes(&r[10]).unwrap(), b"latency_hist".to_vec()); // metric name
            assert_eq!(as_bytes(&r[12]).unwrap(), b"ms".to_vec()); // unit

            // point attrs @ 15
            let pas = as_array(&r[15]).unwrap();
            assert_eq!(pas.values.len(), 1);
            let pa0 = as_struct(&pas.values[0]).unwrap();
            assert_eq!(as_bytes(&pa0.fields[0]).unwrap(), b"h".to_vec());
            assert!(any_is_string_bytes(&pa0.fields[1], b"a"));

            // hist fields
            assert_eq!(as_i64(&r[22]).unwrap(), 3); // count
            assert!((as_f64(&r[23]).unwrap() - 4.5).abs() < 1e-9); // sum

            let bounds = as_array(&r[26]).unwrap();
            assert_eq!(bounds.values.len(), 3);
            assert!((as_f64(&bounds.values[0]).unwrap() - 0.0).abs() < 1e-9);
            assert!((as_f64(&bounds.values[1]).unwrap() - 5.0).abs() < 1e-9);
            assert!((as_f64(&bounds.values[2]).unwrap() - 10.0).abs() < 1e-9);

            let counts = as_array(&r[27]).unwrap();
            assert_eq!(counts.values.len(), 4);
            assert_eq!(as_i64(&counts.values[0]).unwrap(), 1);
            assert_eq!(as_i64(&counts.values[1]).unwrap(), 2);
            assert_eq!(as_i64(&counts.values[2]).unwrap(), 0);
            assert_eq!(as_i64(&counts.values[3]).unwrap(), 0);

            assert_eq!(
                as_i32(&r[28]).unwrap(),
                AggregationTemporality::Delta as i32
            );
        }

        // Second row (no sum).
        {
            let r = &rows[1].values;
            assert_eq!(r.len(), 29);
            assert!(is_null(&r[23])); // sum is null
        }
    }
}
