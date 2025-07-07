use criterion::{black_box, criterion_group, criterion_main, Criterion};
use moonlink::create_data_file;
use moonlink::{GlobalIndex, GlobalIndexBuilder};
use pprof::criterion::{Output, PProfProfiler};
use rand::Rng;
use tokio::runtime::Runtime;

fn bench_build_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_build");
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(10);

    let files = vec![create_data_file(0, "test.parquet".to_string())];
    let vec = (0..10_000_000)
        .map(|i| (i as u64, 0, i))
        .collect::<Vec<_>>();

    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();

    let rt = Runtime::new().unwrap();

    group.bench_function("build_index_10m_entries", |b| {
        b.iter(|| {
            let mut builder = GlobalIndexBuilder::new();
            builder
                .set_files(files.clone())
                .set_directory(dir_path.clone());
            let index = rt.block_on(builder.build_from_flush(vec.clone(), 1));
            black_box(index);
        });
    });
}

fn bench_index_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_query");
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
    let index = rt.block_on(builder.build_from_flush(vec, /*file_id=*/ 1));

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
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_build_index, bench_index_query
}
criterion_main!(benches);
