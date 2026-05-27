# Issue #50 Parquet 1M-Row Baseline

Captured: 2026-05-27 on the local Codex workstation.

## Parquet 1M-row stream

Command:

```bash
cargo bench -p au-kpis-api-http --bench parquet_stream --locked -- --save-baseline issue50
```

Budget: <30 s for 1M streamed rows.

Measured:

```text
api_parquet_stream/parquet_stream_1m_rows_under_30s
time:  [1.2867 s 1.3070 s 1.3402 s]
thrpt: [746.15 Kelem/s 765.10 Kelem/s 777.17 Kelem/s]
```

## dhat memory profile

Command:

```bash
cargo test -p au-kpis-api-http --release --features dhat-heap --test parquet_memory -- --ignored --nocapture
```

Budget: <100 MB peak heap for 1M streamed rows.

Measured:

```text
dhat parquet stream: rows=1000000 bytes=5471814 chunks=41 elapsed_ms=92999 max_bytes=6950516 total_bytes=2957548799
```

Peak heap was 6,950,516 bytes. DHAT instrumentation distorts wall-clock
timing, so the Criterion benchmark is the authoritative <30 s timing check.

## CI regression gate

The PR workflow runs the Parquet Criterion target with the rest of the committed
benchmarks and keeps the blocking merge-queue threshold at:

```bash
critcmp main pr --threshold 5
```
