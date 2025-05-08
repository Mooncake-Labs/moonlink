use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use moonlink::row::{Identity, MoonlinkRow, RowValue};
use moonlink::MooncakeTable;
use std::time::Duration;
use tempfile::tempdir;

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

fn bench_write_mooncake_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("mooncake_table");
    group.measurement_time(Duration::from_secs(10));

    const BATCH_SIZE: i32 = 10_000;

    // Generate all batches once, outside the benchmark
    let all_batches = generate_batches(BATCH_SIZE);

    let temp_dir = tempdir().unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, false),
    ]);

    let mut table = MooncakeTable::new(
        schema,
        "test_table".to_string(),
        1,
        temp_dir.path().to_path_buf(),
        Identity::SinglePrimitiveKey(0),
        None,
    );

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
    config = Criterion::default();
    targets = bench_write_mooncake_table
}
criterion_main!(benches);
