use arrow_schema::{DataType, Field, Fields, Schema};
use std::{collections::HashMap, sync::Arc};

fn get_next_metadata(ids: &mut i32) -> HashMap<String, String> {
    let mut md = HashMap::new();
    md.insert("PARQUET:field_id".to_string(), ids.to_string());
    *ids += 1;
    md
}

fn field_with_id(name: &str, dt: DataType, nullable: bool, ids: &mut i32) -> Field {
    Field::new(name, dt, nullable).with_metadata(get_next_metadata(ids))
}

/// List<Item> where the *item* field is also tagged with an id.
fn list_of_with_id(name: &str, dt: DataType, nullable: bool, ids: &mut i32) -> DataType {
    let item = field_with_id(name, dt, nullable, ids);
    DataType::List(Arc::new(item))
}

/// AnyValue Struct: {string_value?, int_value?, double_value?, bool_value?, bytes_value?}
fn any_value_struct(ids: &mut i32) -> DataType {
    DataType::Struct(Fields::from(vec![
        field_with_id("string_value", DataType::Utf8, /*nullable=*/ true, ids),
        field_with_id("int_value", DataType::Int64, /*nullable=*/ true, ids),
        field_with_id(
            "double_value",
            DataType::Float64,
            /*nullable=*/ true,
            ids,
        ),
        field_with_id(
            "bool_value",
            DataType::Boolean,
            /*nullable=*/ true,
            ids,
        ),
        field_with_id(
            "bytes_value",
            DataType::Binary,
            /*nullable=*/ true,
            ids,
        ),
    ]))
}

/// attributes: List<Struct{ key: Utf8, value: AnyValueStruct }>
fn attributes_field(name: &str, ids: &mut i32) -> Field {
    let kv_struct = DataType::Struct(Fields::from(vec![
        field_with_id("key", DataType::Utf8, /*nullable=*/ false, ids),
        field_with_id("value", any_value_struct(ids), true, ids),
    ]));
    field_with_id(
        name,
        list_of_with_id("item", kv_struct, /*nullable=*/ true, ids),
        /*nullable=*/ true,
        ids,
    )
}

/// kv_pairs: List<Struct{ key: Utf8, value: AnyValueStruct }>
fn kv_pairs_field(name: &str, ids: &mut i32) -> Field {
    let kv_struct = DataType::Struct(Fields::from(vec![
        field_with_id("key", DataType::Utf8, /*nullable=*/ false, ids),
        field_with_id("value", any_value_struct(ids), /*nullable=*/ true, ids),
    ]));
    field_with_id(
        name,
        list_of_with_id("item", kv_struct, /*nullable=*/ true, ids),
        /*nullable=*/ true,
        ids,
    )
}

/// Get EntityRef: { type: Utf8, id_pairs: List<KV>, description_pairs: List<KV>, schema_url: Utf8 }
fn entity_ref_struct(ids: &mut i32) -> DataType {
    DataType::Struct(Fields::from(vec![
        field_with_id("type", DataType::Utf8, /*nullable=*/ true, ids),
        // Build fresh kv_pairs (assign unique parquet field ids)
        kv_pairs_field("id_pairs", ids).clone(),
        kv_pairs_field("description_pairs", ids).clone(),
        field_with_id("schema_url", DataType::Utf8, /*nullable=*/ true, ids),
    ]))
}

/// resource_entity_refs: List<entity_ref_struct>
fn entity_refs_field(name: &str, ids: &mut i32) -> Field {
    field_with_id(
        name,
        list_of_with_id("item", entity_ref_struct(ids), /*nullable=*/ true, ids),
        /*nullable=*/ true,
        ids,
    )
}

/// Exemplar struct (int or double value) + filtered_attributes (as attributes-field)
fn exemplar_struct(ids: &mut i32) -> DataType {
    DataType::Struct(Fields::from(vec![
        field_with_id(
            "time_unix_nano",
            DataType::Int64,
            /*nullable=*/ false,
            ids,
        ),
        field_with_id("as_int", DataType::Int64, /*nullable=*/ true, ids),
        field_with_id("as_double", DataType::Float64, true, ids),
        field_with_id(
            "trace_id",
            DataType::FixedSizeBinary(16),
            /*nullable=*/ true,
            ids,
        ),
        field_with_id(
            "span_id",
            DataType::FixedSizeBinary(8),
            /*nullable=*/ true,
            ids,
        ),
        attributes_field("filtered_attributes", ids),
    ]))
}

fn common_metric_fields(ids: &mut i32) -> Vec<Field> {
    vec![
        // 0 kind
        field_with_id("kind", DataType::Utf8, /*nullable=*/ false, ids),
        // 1 resource_attributes
        attributes_field("resource_attributes", ids),
        // 2 resource_entity_refs
        entity_refs_field("resource_entity_refs", ids),
        // 3 resource_dropped_attributes_count
        field_with_id(
            "resource_dropped_attributes_count",
            DataType::Int64,
            /*nullable=*/ true,
            ids,
        ),
        // 4 resource_schema_url
        field_with_id(
            "resource_schema_url",
            DataType::Utf8,
            /*nullable=*/ true,
            ids,
        ),
        // 5 scope_name
        field_with_id("scope_name", DataType::Utf8, /*nullable=*/ true, ids),
        // 6 scope_version
        field_with_id(
            "scope_version",
            DataType::Utf8,
            /*nullable=*/ true,
            ids,
        ),
        // 7 scope_attributes
        attributes_field("scope_attributes", ids),
        // 8 scope_dropped_attributes_count
        field_with_id(
            "scope_dropped_attributes_count",
            DataType::Int64,
            /*nullable=*/ true,
            ids,
        ),
        // 9 scope_schema_url
        field_with_id(
            "scope_schema_url",
            DataType::Utf8,
            /*nullable=*/ true,
            ids,
        ),
        // 10 metric_name
        field_with_id("metric_name", DataType::Utf8, /*nullable=*/ false, ids),
        // 11 metric_description
        field_with_id(
            "metric_description",
            DataType::Utf8,
            /*nullable=*/ true,
            ids,
        ),
        // 12 metric_unit
        field_with_id("metric_unit", DataType::Utf8, /*nullable=*/ true, ids),
        // 13 start_time_unix_nano
        field_with_id(
            "start_time_unix_nano",
            DataType::Int64,
            /*nullable=*/ true,
            ids,
        ),
        // 14 time_unix_nano
        field_with_id(
            "time_unix_nano",
            DataType::Int64,
            /*nullable=*/ false,
            ids,
        ),
        // 15 point_attributes
        attributes_field("point_attributes", ids),
        // 16 point_dropped_attributes_count
        field_with_id(
            "point_dropped_attributes_count",
            DataType::Int64,
            /*nullable=*/ true,
            ids,
        ),
    ]
}

fn number_point_fields(ids: &mut i32) -> Vec<Field> {
    vec![
        field_with_id("number_int", DataType::Int64, /*nullable=*/ true, ids),
        field_with_id(
            "number_double",
            DataType::Float64,
            /*nullable=*/ true,
            ids,
        ),
        field_with_id("temporality", DataType::Int32, /*nullable=*/ true, ids),
        field_with_id(
            "is_monotonic",
            DataType::Boolean,
            /*nullable=*/ true,
            ids,
        ),
        field_with_id(
            "exemplars",
            list_of_with_id("item", exemplar_struct(ids), /*nullable=*/ true, ids),
            /*nullable=*/ true,
            ids,
        ),
    ]
}

fn histogram_point_fields(ids: &mut i32) -> Vec<Field> {
    vec![
        field_with_id("hist_count", DataType::Int64, /*nullable=*/ true, ids),
        field_with_id("hist_sum", DataType::Float64, /*nullable=*/ true, ids),
        field_with_id("hist_min", DataType::Float64, /*nullable=*/ true, ids),
        field_with_id("hist_max", DataType::Float64, /*nullable=*/ true, ids),
        field_with_id(
            "explicit_bounds",
            list_of_with_id("item", DataType::Float64, /*nullable=*/ true, ids),
            true,
            ids,
        ),
        field_with_id(
            "bucket_counts",
            list_of_with_id("item", DataType::Int64, true, ids),
            /*nullable=*/ true,
            ids,
        ),
        field_with_id(
            "hist_temporality",
            DataType::Int32,
            /*nullable=*/ true,
            ids,
        ),
        field_with_id(
            "hist_exemplars",
            list_of_with_id("item", exemplar_struct(ids), /*nullable=*/ true, ids),
            /*nullable=*/ true,
            ids,
        ),
    ]
}

/// Unified Arrow schema for Gauge / Sum / Histogram rows (one row per datapoint).
#[allow(unused)]
pub(crate) fn otlp_metrics_gsh_schema() -> Schema {
    let mut ids = 0;
    let mut fields = Vec::new();
    fields.extend(common_metric_fields(&mut ids));
    fields.extend(number_point_fields(&mut ids));
    fields.extend(histogram_point_fields(&mut ids));
    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otel::otel_to_moonlink_pb::export_metrics_to_moonlink_rows;
    use crate::otel::test_utils::*;
    use moonlink::row::proto_to_moonlink_row;
    use moonlink::{
        AccessorConfig, FileSystemAccessor, FsRetryConfig, FsTimeoutConfig, IcebergTableConfig,
        MooncakeTable, MooncakeTableConfig, ObjectStorageCache, ObjectStorageCacheConfig,
        StorageConfig, WalConfig, WalManager,
    };
    use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, EntityRef, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::{
        metric, AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric,
        NumberDataPoint, Sum,
    };
    use tempfile::{tempdir, TempDir};

    /// Util function to create a mooncake table with otel schema.
    async fn create_mooncake_otel_table(table_temp_dir: &TempDir) -> MooncakeTable {
        let table_path = table_temp_dir.path().to_str().unwrap().to_string();
        let iceberg_table_config = IcebergTableConfig::default();
        let table_config = MooncakeTableConfig::new(table_path.clone());
        let wal_config = WalConfig::default();
        let wal_manager = WalManager::new(&wal_config);
        let object_storage_cache_config = ObjectStorageCacheConfig::new(
            /*max_bytes=*/ u64::MAX,
            /*cache_directory=*/ table_path.clone(),
            /*optimize_local_filesystem=*/ true,
        );
        let object_storage_cache = Arc::new(ObjectStorageCache::new(object_storage_cache_config));
        let storage_config = StorageConfig::FileSystem {
            root_directory: table_path.clone(),
            atomic_write_dir: None,
        };
        let accessor_config = AccessorConfig {
            storage_config,
            timeout_config: FsTimeoutConfig::default(),
            retry_config: FsRetryConfig::default(),
            chaos_config: None,
        };
        let table_filesystem_accessor = Arc::new(FileSystemAccessor::new(accessor_config));

        let table = MooncakeTable::new(
            otlp_metrics_gsh_schema(),
            /*name=*/ "table".to_string(),
            /*table_id=*/ 0,
            /*base_path=*/ std::path::PathBuf::from(table_path.clone()),
            iceberg_table_config,
            table_config,
            wal_manager,
            object_storage_cache,
            table_filesystem_accessor,
        )
        .await
        .unwrap();
        table
    }

    #[tokio::test]
    async fn test_gauge_table_creation_ingestion() {
        let table_temp_dir = tempdir().unwrap();
        let mut table = create_mooncake_otel_table(&table_temp_dir).await;

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
        let row_pbs = export_metrics_to_moonlink_rows(&req);
        for cur_row_pb in row_pbs.into_iter() {
            let cur_row = proto_to_moonlink_row(cur_row_pb).unwrap();
            table.append(cur_row).unwrap();
        }
    }

    #[tokio::test]
    async fn test_sum_table_creation_ingestion() {
        let table_temp_dir = tempdir().unwrap();
        let mut table = create_mooncake_otel_table(&table_temp_dir).await;

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
        let row_pbs = export_metrics_to_moonlink_rows(&req);
        for cur_row_pb in row_pbs.into_iter() {
            let cur_row = proto_to_moonlink_row(cur_row_pb).unwrap();
            table.append(cur_row).unwrap();
        }
    }

    #[tokio::test]
    async fn test_histogram_table_creation_ingestion() {
        let table_temp_dir = tempdir().unwrap();
        let mut table = create_mooncake_otel_table(&table_temp_dir).await;

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
        let row_pbs = export_metrics_to_moonlink_rows(&req);
        for cur_row_pb in row_pbs.into_iter() {
            let cur_row = proto_to_moonlink_row(cur_row_pb).unwrap();
            table.append(cur_row).unwrap();
        }
    }
}
