use std::time::Instant;

use au_kpis_api_http::observations::benchmark_support::{
    PARQUET_SCALE_VALIDATION_BUDGET, PARQUET_SCALE_VALIDATION_ROWS, drain_synthetic_parquet_stream,
};

#[tokio::test(flavor = "current_thread")]
#[ignore = "10M-row Parquet scale validation for issue #62"]
async fn parquet_stream_10m_rows_completes_under_30s() {
    let started = Instant::now();
    let stats = drain_synthetic_parquet_stream(PARQUET_SCALE_VALIDATION_ROWS)
        .await
        .expect("drain synthetic parquet stream");
    let elapsed = started.elapsed();

    println!(
        "parquet scale stream: rows={} bytes={} chunks={} elapsed_ms={}",
        stats.rows,
        stats.bytes,
        stats.chunks,
        elapsed.as_millis(),
    );

    assert_eq!(stats.rows, PARQUET_SCALE_VALIDATION_ROWS);
    assert!(stats.bytes > 0, "parquet stream should emit bytes");
    assert!(stats.chunks > 0, "parquet stream should emit chunks");
    assert!(
        elapsed < PARQUET_SCALE_VALIDATION_BUDGET,
        "10M-row parquet stream took {elapsed:?}, exceeding {PARQUET_SCALE_VALIDATION_BUDGET:?}"
    );
}
