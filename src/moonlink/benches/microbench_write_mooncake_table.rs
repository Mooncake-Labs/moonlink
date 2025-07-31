use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use moonlink::row::{IdentityProp, MoonlinkRow, RowValue};
use moonlink::{AccessorConfig, FileSystemAccessor, StorageConfig, WalConfig};
use moonlink::{IcebergTableConfig, ObjectStorageCache};
use moonlink::{MooncakeTable, MooncakeTableConfig};
use pprof::criterion::{Output, PProfProfiler};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::runtime::Runtime;

fn create_test_row(id: i32) -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(id),
        RowValue::ByteArray(format!("Row {id}").into_bytes()),
        RowValue::Int32(30 + id),
    ])
}

fn generate_batches(batch_size: i32) -> Vec<MoonlinkRow> {
    (0..batch_size).map(create_test_row).collect::<Vec<_>>()
}

fn bench_write_mooncake_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("mooncake_table");
    group.measurement_time(Duration::from_secs(10));

    const BATCH_SIZE: i32 = 10_000;

    // Generate all batches once, outside the benchmark
    let all_batches = generate_batches(BATCH_SIZE);

    let temp_dir = tempdir().unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "1".to_string(),
        )])),
        Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "2".to_string(),
        )])),
        Field::new("age", DataType::Int32, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "3".to_string(),
        )])),
    ]);

    let base_path = temp_dir.path().to_path_buf();
    let warehouse_location = base_path.to_str().unwrap().to_string();
    let table_name = "test_table";
    let iceberg_table_config = IcebergTableConfig {
        namespace: vec!["default".to_string()],
        table_name: table_name.to_string(),
        accessor_config: AccessorConfig::new_with_storage_config(
            moonlink::StorageConfig::FileSystem {
                root_directory: warehouse_location.clone(),
            },
        ),
    };
    let rt = Runtime::new().unwrap();
    let table_config = MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
    // TODO(Paul): May need to tie this to the actual mooncake table ID in the future.
    let wal_config = WalConfig::default_wal_config_local("1", &base_path);
    let mut table = rt
        .block_on(MooncakeTable::new(
            schema,
            table_name.to_string(),
            1,
            base_path,
            IdentityProp::SinglePrimitiveKey(0),
            iceberg_table_config,
            table_config,
            wal_config,
            ObjectStorageCache::default_for_bench(),
            Arc::new(FileSystemAccessor::new(
                AccessorConfig::new_with_storage_config(StorageConfig::FileSystem {
                    root_directory: warehouse_location.clone(),
                }),
            )),
        ))
        .unwrap();

    let mut total_appended = 0;

    group.bench_function("write_rows", |b| {
        b.iter(|| {
            total_appended += 1;
            for row in all_batches.iter() {
                let new_row = MoonlinkRow::new(row.values.clone());
                table.append(new_row).expect("append failed");
            }
            table.commit(total_appended as u64);
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_write_mooncake_table
}
criterion_main!(benches);
