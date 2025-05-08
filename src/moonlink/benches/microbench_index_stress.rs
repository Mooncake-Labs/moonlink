use criterion::{black_box, criterion_group, criterion_main, Criterion};
use moonlink::GlobalIndexBuilder;
use rand::Rng;
use std::path::PathBuf;
use std::sync::Arc;

fn bench_index_stress(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_stress");
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(10);
    let files = vec![Arc::new(PathBuf::from("test.parquet"))];
    let vec = (0..10000000).map(|i| (i as u64, 0, i)).collect::<Vec<_>>();
    let mut builder = GlobalIndexBuilder::new();
    builder
        .set_files(files)
        .set_directory(tempfile::tempdir().unwrap().into_path());
    let index = builder.build_from_flush(vec);

    group.bench_function("search_10m_entries", |b| {
        b.iter(|| {
            let mut rng = rand::rng();
            let result = black_box(index.search(&(rng.random_range(0..10000000) as u64)));
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
