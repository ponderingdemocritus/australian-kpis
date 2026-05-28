# Capacity Plan

This plan records the current serving and Parquet export headroom used for
quarterly capacity reviews.

## Serving Headroom

Rows-per-second per instance is measured from the Phase 5 full-load test. The
current serving baseline is the 1000 rps target on `/v1/observations` with p99
below 1s and error rate below 0.1%.

Measured headroom is computed as:

```text
serving_headroom = measured_rps_per_instance / projected_peak_rps
```

For the May 2026 validation, the measured 999.90 rps per API instance covers
the initial public launch projection of 250 peak rps per instance, leaving 4.0x
serving headroom before horizontal scaling is required. The measured p99 was
6.956 ms with 0.000% request failures and zero dropped k6 iterations.

## Parquet Export Headroom

Parquet 10M row export is validated through the same streaming writer used by
the API response path. The May 2026 target is Parquet 10M rows in less than 30
seconds, which is the release gate for large analytical downloads.

Measured headroom is computed as:

```text
parquet_headroom = parquet_budget_seconds / measured_parquet_seconds
```

For the May 2026 validation, the Parquet scale harness streamed 10,000,000 rows
in 17.45 seconds, producing 54,453,114 bytes across 407 writer chunks. That
leaves 1.72x Parquet export headroom against the 30 second release gate.

The capacity model is reviewed quarterly. A headroom ratio below 2.0x triggers
an autoscaling and query-shape review before the next release window; the May
2026 Parquet result should stay on that review agenda even though it passes the
Phase 5 gate.
