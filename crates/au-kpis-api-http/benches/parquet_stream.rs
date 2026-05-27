use std::time::{Duration, Instant};

use au_kpis_api_http::observations::benchmark_support::{
    PARQUET_STREAM_BENCHMARK_BUDGET, PARQUET_STREAM_BENCHMARK_ROWS, drain_synthetic_parquet_stream,
};
use criterion::{Criterion, SamplingMode, Throughput, black_box, criterion_group, criterion_main};

fn bench_parquet_stream(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("api_parquet_stream");
    group.throughput(Throughput::Elements(PARQUET_STREAM_BENCHMARK_ROWS as u64));
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.bench_function("parquet_stream_1m_rows_under_30s", |b| {
        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                let started = Instant::now();
                let stats = runtime
                    .block_on(drain_synthetic_parquet_stream(
                        PARQUET_STREAM_BENCHMARK_ROWS,
                    ))
                    .expect("drain synthetic parquet stream");
                let iteration_elapsed = started.elapsed();
                assert_eq!(stats.rows, PARQUET_STREAM_BENCHMARK_ROWS);
                assert!(stats.bytes > 0, "parquet stream should emit bytes");
                assert!(stats.chunks > 0, "parquet stream should emit chunks");
                assert!(
                    iteration_elapsed < PARQUET_STREAM_BENCHMARK_BUDGET,
                    "1M-row parquet stream took {iteration_elapsed:?}, exceeding {PARQUET_STREAM_BENCHMARK_BUDGET:?}"
                );
                black_box(stats);
                elapsed += iteration_elapsed;
            }
            elapsed
        });
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(10))
        .sample_size(10);
    targets = bench_parquet_stream
}
criterion_main!(benches);
