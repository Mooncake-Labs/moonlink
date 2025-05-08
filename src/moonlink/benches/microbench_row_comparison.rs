use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use moonlink::row::{Identity, MoonlinkRow, RowValue};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;

fn create_test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
        Field::new("is_active", DataType::Boolean, false),
        Field::new("description", DataType::Utf8, false),
    ]));

    // Create 1000 rows of test data
    let mut ids = Vec::with_capacity(1000);
    let mut names = Vec::with_capacity(1000);
    let mut ages = Vec::with_capacity(1000);
    let mut scores = Vec::with_capacity(1000);
    let mut is_active = Vec::with_capacity(1000);
    let mut descriptions = Vec::with_capacity(1000);

    for i in 0..1000 {
        ids.push(i as i64);
        names.push(format!("User{}", i));
        ages.push(20 + (i % 50) as i64);
        scores.push(50.0 + (i % 50) as f64);
        is_active.push(i % 2 == 0);
        descriptions.push(format!(
            "Description for user {} with some additional text to make it longer",
            i
        ));
    }

    let id_array = Int64Array::from(ids);
    let name_array = StringArray::from(names);
    let age_array = Int64Array::from(ages);
    let score_array = Float64Array::from(scores);
    let is_active_array = BooleanArray::from(is_active);
    let description_array = StringArray::from(descriptions);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(age_array),
            Arc::new(score_array),
            Arc::new(is_active_array),
            Arc::new(description_array),
        ],
    )
    .unwrap()
}

fn create_test_row() -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int64(1),
        RowValue::ByteArray("User1".as_bytes().to_vec()),
        RowValue::Int64(25),
        RowValue::Float64(75.5),
        RowValue::Bool(true),
        RowValue::ByteArray(
            "Description for user 1 with some additional text to make it longer"
                .as_bytes()
                .to_vec(),
        ),
    ])
}

fn bench_equals_record_batch(c: &mut Criterion) {
    let batch = create_test_batch();
    let row = create_test_row();
    let identity = Identity::FullRow;

    c.bench_function("equals_record_batch", |b| {
        b.iter(|| {
            black_box(row.equals_record_batch_at_offset(&batch, 0, &identity));
        })
    });
}

fn bench_equals_parquet(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let parquet_path = temp_dir.path().join("test.parquet");

    // Create and write Parquet file
    let batch = create_test_batch();
    let file = File::create(&parquet_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let row = create_test_row();
    let identity = Identity::FullRow;

    c.bench_function("equals_parquet", |b| {
        b.iter(|| {
            black_box(row.equals_parquet_at_offset(parquet_path.to_str().unwrap(), 0, &identity));
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_equals_record_batch, bench_equals_parquet
}
criterion_main!(benches);
