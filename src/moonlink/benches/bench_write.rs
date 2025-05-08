use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use moonlink::row::{Identity, MoonlinkRow, RowValue};
use moonlink::MooncakeTable;
use tempfile::tempdir;
use tokio::runtime::Runtime;
fn create_test_row(id: i32) -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(id),
        RowValue::ByteArray(format!("Row {}", id).into_bytes()),
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
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, false),
    ]);

    let batches = generate_batches(1000000);

    let rt = Runtime::new().unwrap();

    group.bench_function("write_1m_rows", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut table = MooncakeTable::new(
                    schema.clone(),
                    "test_table".to_string(),
                    1,
                    temp_dir.path().to_path_buf(),
                    Identity::SinglePrimitiveKey(0),
                    None,
                );
                for row in batches.iter() {
                    let _ = table.append(MoonlinkRow {
                        values: row.values.clone(),
                    });
                }
                let handle = table.flush(100000);
                handle.await.unwrap();
            });
        });
    });

    group.bench_function("stream_write_1m_rows", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut table = MooncakeTable::new(
                    schema.clone(),
                    "test_table".to_string(),
                    1,
                    temp_dir.path().to_path_buf(),
                    Identity::SinglePrimitiveKey(0),
                    None,
                );
                for row in batches.iter() {
                    let _ = table.append_in_stream_batch(
                        MoonlinkRow {
                            values: row.values.clone(),
                        },
                        1,
                    );
                }
                let handle = table.flush(100000);
                handle.await.unwrap();
            });
        });
    });

    group.bench_function("stream_delete_1m_rows", |b| {
        b.iter_batched(
            || {
                let mut table = MooncakeTable::new(
                    schema.clone(),
                    "test_table".to_string(),
                    1,
                    temp_dir.path().to_path_buf(),
                    Identity::SinglePrimitiveKey(0),
                    None,
                );
                for row in batches.iter() {
                    let _ = table.append_in_stream_batch(
                        MoonlinkRow {
                            values: row.values.clone(),
                        },
                        1,
                    );
                }
                rt.block_on(async {
                    let handle = table.flush_transaction_stream(1, 100000);
                    let _ = handle.await.unwrap();
                });
                table
            },
            |mut table| {
                rt.block_on(async {
                    for i in 0..1000000 {
                        let _ = table.delete_in_stream_batch(
                            MoonlinkRow {
                                values: vec![RowValue::Int32(i)],
                            },
                            1,
                        );
                    }
                    let handle = table.flush_transaction_stream(1, 10000);
                    let _ = handle.await.unwrap();
                    let handle = table.create_snapshot();
                    let _ = handle.unwrap().await.unwrap();
                });
            },
            BatchSize::PerIteration,
        );
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_write
}
criterion_main!(benches);
