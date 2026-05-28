# Load Test 2026-05

## Scope

Issue #62 validates the Phase 5 observation-serving path:

- sustained 1000 rps on `/v1/observations` with p99 <1s and error rate <0.1%
- Parquet 10M rows streamed end-to-end in <30 seconds
- measured headroom recorded in the capacity plan

## Environment

- Date: 2026-05-28
- Target: local staging-equivalent compose stack on Apple Silicon
- API stack: `infra/compose/docker-compose.yml`
- Dataset: `apps/web/e2e/fixtures/explorer.sql`
- k6 script: `apps/bench/full-load.js`
- Parquet harness: `crates/au-kpis-api-http/tests/parquet_scale.rs`

## Results

| Check | Result | Budget |
|---|---:|---:|
| `/v1/observations` throughput | 999.90 rps (600,000 iterations over 10m) | 1000 rps |
| `/v1/observations` p99 | 6.956 ms | <1s |
| `/v1/observations` error rate | 0.000% (0/600,001 HTTP requests) | error rate <0.1% |
| k6 dropped iterations | 0 | 0 |
| Parquet scale stream | 10,000,000 rows in 17.45s | 10M rows <30 seconds |

## Measured headroom

- Serving: `999.90 measured rps / 250 projected peak rps = 4.00x`.
- Parquet: `30s budget / 17.45s measured = 1.72x`.

The first uncached local run exposed connection-pool saturation under this
arrival rate. The final run uses the Phase 5 serving configuration from this
change: larger database/cache pools and a bounded Redis-backed cache for the
first JSON observations page. Cursor pages, CSV, and Parquet exports remain on
the streaming path.

## Commands

```bash
AU_KPIS_RATE_LIMITS__ANONYMOUS__PER_SECOND=2000 \
AU_KPIS_RATE_LIMITS__ANONYMOUS__PER_HOUR=1000000 \
AU_KPIS_RATE_LIMITS__ANONYMOUS__BURST_MULTIPLIER=1 \
  docker compose -f infra/compose/docker-compose.yml up -d --build api

AU_KPIS_BASE_URL=http://127.0.0.1:3000 \
  k6 run --summary-export target/k6/full-load-summary.json apps/bench/full-load.js

RUSTC_WRAPPER= cargo test -p au-kpis-api-http --release --test parquet_scale -- --ignored --nocapture
```
