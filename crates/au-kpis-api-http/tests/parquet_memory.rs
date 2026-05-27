#![cfg(feature = "dhat-heap")]

use std::time::Instant;

use au_kpis_api_http::observations::benchmark_support::{
    PARQUET_STREAM_BENCHMARK_ROWS, PARQUET_STREAM_DHAT_HEAP_BUDGET_BYTES,
    drain_synthetic_parquet_stream,
};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[tokio::test(flavor = "current_thread")]
#[ignore = "1M-row DHAT memory profile for issue #50"]
async fn parquet_stream_1m_rows_stays_below_100mb_peak_heap_under_dhat() {
    let profiler = dhat::Profiler::builder().testing().build();
    let started = Instant::now();
    let stats = drain_synthetic_parquet_stream(PARQUET_STREAM_BENCHMARK_ROWS)
        .await
        .expect("drain synthetic parquet stream");
    let elapsed = started.elapsed();
    let heap = dhat::HeapStats::get();
    println!(
        "dhat parquet stream: rows={} bytes={} chunks={} elapsed_ms={} max_bytes={} total_bytes={}",
        stats.rows,
        stats.bytes,
        stats.chunks,
        elapsed.as_millis(),
        heap.max_bytes,
        heap.total_bytes
    );

    assert_eq!(stats.rows, PARQUET_STREAM_BENCHMARK_ROWS);
    assert!(stats.bytes > 0, "parquet stream should emit bytes");
    // DHAT instrumentation distorts wall-clock timing; the Criterion bench
    // enforces the uninstrumented 30-second budget for this same path.
    assert!(
        heap.max_bytes < PARQUET_STREAM_DHAT_HEAP_BUDGET_BYTES,
        "peak heap {} bytes exceeded {} byte budget",
        heap.max_bytes,
        PARQUET_STREAM_DHAT_HEAP_BUDGET_BYTES
    );
    drop(profiler);
}
