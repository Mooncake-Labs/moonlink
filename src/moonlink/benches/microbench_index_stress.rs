use criterion::{black_box, criterion_group, criterion_main, Criterion};
use moonlink::create_data_file;
use moonlink::{GlobalIndex, GlobalIndexBuilder};
use rand::Rng;
use tokio::runtime::Runtime;

fn bench_index_stress(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_stress");
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(10);
    let files = vec![create_data_file(
        /*file_id=*/ 0,
        "test.parquet".to_string(),
    )];
    let vec = (0..10000000).map(|i| (i as u64, 0, i)).collect::<Vec<_>>();
    let mut builder = GlobalIndexBuilder::new();
    builder
        .set_files(files)
        .set_directory(tempfile::tempdir().unwrap().keep());
    let rt = Runtime::new().unwrap();
    let index = rt.block_on(builder.build_from_flush(vec));

    group.bench_function("search_10m_entries", |b| {
        b.iter(|| {
            let mut rng = rand::rng();
            let hashes = GlobalIndex::prepare_hashes_for_lookup(
                (vec![rng.random_range(0..10000000) as u64]).into_iter(),
            );
            let result = black_box(rt.block_on(index.search_values(&hashes)));
            black_box(result);
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_index_stress
}
criterion_main!(benches);
