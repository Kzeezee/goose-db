use criterion::{black_box, criterion_group, criterion_main, Criterion};
use goose_db::query::execute_tpch_q1;

/// Configure your data path here
const DATA_PATH: &str = "data/lineitem.parquet";

fn benchmark_tpch_q1(c: &mut Criterion) {
    // Warmup - ensure file is in OS page cache
    let _ = execute_tpch_q1(DATA_PATH);
    
    c.bench_function("tpch_q1", |b| {
        b.iter(|| {
            let result = execute_tpch_q1(black_box(DATA_PATH)).unwrap();
            black_box(result)
        })
    });
}

criterion_group!(benches, benchmark_tpch_q1);
criterion_main!(benches);
