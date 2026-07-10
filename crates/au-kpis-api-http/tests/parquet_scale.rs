use std::time::Instant;

use au_kpis_api_http::observations::benchmark_support::{
    PARQUET_SCALE_VALIDATION_BUDGET, PARQUET_SCALE_VALIDATION_ROWS, PARQUET_STREAM_BENCHMARK_ROWS,
    drain_synthetic_parquet_stream,
};

#[tokio::test(flavor = "current_thread")]
#[ignore = "10M-row Parquet scale validation for issue #62"]
async fn parquet_stream_10m_rows_completes_under_30s() {
    let started = Instant::now();
    let mut emitted_rows = 0;
    let mut emitted_bytes = 0;
    let mut emitted_chunks = 0;
    for partition in 0..10 {
        let stats = drain_synthetic_parquet_stream(PARQUET_STREAM_BENCHMARK_ROWS)
            .await
            .unwrap_or_else(|err| panic!("drain 1M-row Parquet partition {partition}: {err}"));
        emitted_rows += stats.rows;
        emitted_bytes += stats.bytes;
        emitted_chunks += stats.chunks;
    }
    let elapsed = started.elapsed();

    println!(
        "parquet scale stream: rows={} bytes={} chunks={} elapsed_ms={}",
        emitted_rows,
        emitted_bytes,
        emitted_chunks,
        elapsed.as_millis(),
    );

    assert_eq!(emitted_rows, PARQUET_SCALE_VALIDATION_ROWS);
    assert!(emitted_bytes > 0, "parquet partitions should emit bytes");
    assert!(emitted_chunks >= 10, "every partition should emit chunks");
    assert!(
        elapsed < PARQUET_SCALE_VALIDATION_BUDGET,
        "10M-row parquet stream took {elapsed:?}, exceeding {PARQUET_SCALE_VALIDATION_BUDGET:?}"
    );
}
