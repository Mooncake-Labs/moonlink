use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use moonlink::row::{IdentityProp, MoonlinkRow, RowValue};
use moonlink::{
    AccessorConfig, FileSystemAccessor, IcebergTableConfig, MooncakeTable, MooncakeTableConfig,
    ObjectStorageCache, StorageConfig, WalConfig, WalManager,
};
use pprof::criterion::{Output, PProfProfiler};
use std::collections::HashMap;
use std::sync::Arc;
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

fn bench_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(10);

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

    let batches = generate_batches(1000000);

    let rt = Runtime::new().unwrap();

    group.bench_function("write_1m_rows", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Create a temporary warehouse location for each benchmark suite, otherwise iceberg table manager loads previous states.
                let temp_warehouse_dir = tempdir().unwrap();
                let temp_warehouse_uri = temp_warehouse_dir.path().to_str().unwrap().to_string();
                let iceberg_table_config = IcebergTableConfig {
                    accessor_config: AccessorConfig::new_with_storage_config(
                        StorageConfig::FileSystem {
                            root_directory: temp_warehouse_uri.clone(),
                            atomic_write_dir: None,
                        },
                    ),
                    ..Default::default()
                };
                let table_config =
                    MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
                // TODO(Paul): May need to tie this to the actual mooncake table ID in the future.
                let wal_config = WalConfig::default_wal_config_local("1", temp_dir.path());
                let wal_manager = WalManager::new(&wal_config);
                let mut table = MooncakeTable::new(
                    schema.clone(),
                    "test_table".to_string(),
                    1,
                    temp_dir.path().to_path_buf(),
                    IdentityProp::SinglePrimitiveKey(0),
                    iceberg_table_config,
                    table_config,
                    wal_manager,
                    ObjectStorageCache::default_for_bench(),
                    Arc::new(FileSystemAccessor::new(
                        AccessorConfig::new_with_storage_config(StorageConfig::FileSystem {
                            root_directory: temp_warehouse_uri.clone(),
                            atomic_write_dir: None,
                        }),
                    )),
                )
                .await
                .unwrap();
                for row in batches.iter() {
                    let _ = table.append(MoonlinkRow {
                        values: row.values.clone(),
                    });
                }
                let _ = table.flush(100000);
            });
        });
    });

    group.bench_function("stream_write_1m_rows", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Create a temporary warehouse location for each benchmark suite, otherwise iceberg table manager loads previous states.
                let temp_warehouse_dir = tempdir().unwrap();
                let temp_warehouse_uri = temp_warehouse_dir.path().to_str().unwrap().to_string();
                let iceberg_table_config = IcebergTableConfig {
                    accessor_config: AccessorConfig::new_with_storage_config(
                        StorageConfig::FileSystem {
                            root_directory: temp_warehouse_uri.clone(),
                            atomic_write_dir: None,
                        },
                    ),
                    ..Default::default()
                };
                let table_config =
                    MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
                // TODO(Paul): May need to tie this to the actual mooncake table ID in the future.
                let wal_config = WalConfig::default_wal_config_local("1", temp_dir.path());
                let wal_manager = WalManager::new(&wal_config);
                let mut table = MooncakeTable::new(
                    schema.clone(),
                    "test_table".to_string(),
                    1,
                    temp_dir.path().to_path_buf(),
                    IdentityProp::SinglePrimitiveKey(0),
                    iceberg_table_config,
                    table_config,
                    wal_manager,
                    ObjectStorageCache::default_for_bench(),
                    Arc::new(FileSystemAccessor::new(
                        AccessorConfig::new_with_storage_config(StorageConfig::FileSystem {
                            root_directory: temp_warehouse_uri.clone(),
                            atomic_write_dir: None,
                        }),
                    )),
                )
                .await
                .unwrap();
                for row in batches.iter() {
                    let _ = table.append_in_stream_batch(
                        MoonlinkRow {
                            values: row.values.clone(),
                        },
                        1,
                    );
                }
                let _ = table.flush(100000);
            });
        });
    });

    group.bench_function("stream_delete_1m_rows", |b| {
        b.iter_batched(
            || {
                // Create a temporary warehouse location for each benchmark suite, otherwise iceberg table manager loads previous states.
                let temp_warehouse_dir = tempdir().unwrap();
                let temp_warehouse_uri = temp_warehouse_dir.path().to_str().unwrap().to_string();
                let iceberg_table_config = IcebergTableConfig {
                    accessor_config: AccessorConfig::new_with_storage_config(
                        StorageConfig::FileSystem {
                            root_directory: temp_warehouse_uri.clone(),
                            atomic_write_dir: None,
                        },
                    ),
                    ..Default::default()
                };
                let table_config =
                    MooncakeTableConfig::new(temp_dir.path().to_str().unwrap().to_string());
                // TODO(Paul): May need to tie this to the actual mooncake table ID in the future.
                let wal_config = WalConfig::default_wal_config_local("1", temp_dir.path());
                let wal_manager = WalManager::new(&wal_config);
                let mut table = rt
                    .block_on(MooncakeTable::new(
                        schema.clone(),
                        "test_table".to_string(),
                        1,
                        temp_dir.path().to_path_buf(),
                        IdentityProp::SinglePrimitiveKey(0),
                        iceberg_table_config,
                        table_config,
                        wal_manager,
                        ObjectStorageCache::default_for_bench(),
                        Arc::new(FileSystemAccessor::new(
                            AccessorConfig::new_with_storage_config(StorageConfig::FileSystem {
                                root_directory: temp_warehouse_uri.clone(),
                                atomic_write_dir: None,
                            }),
                        )),
                    ))
                    .unwrap();
                rt.block_on(async {
                    for row in batches.iter() {
                        let _ = table.append_in_stream_batch(
                            MoonlinkRow {
                                values: row.values.clone(),
                            },
                            1,
                        );
                    }
                    table.flush_stream(1, None).unwrap();
                });
                table
            },
            |mut table| {
                rt.block_on(async {
                    for i in 0..1000000 {
                        table
                            .delete_in_stream_batch(
                                MoonlinkRow {
                                    values: vec![RowValue::Int32(i)],
                                },
                                1,
                            )
                            .await;
                    }
                    table.flush_stream(1, None).unwrap();
                });
            },
            BatchSize::PerIteration,
        );
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_write
}
criterion_main!(benches);
