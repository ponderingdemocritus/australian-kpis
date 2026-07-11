# k6 Bench Scenarios

`smoke.js`, `sustained.js`, and `burst.js` are the k6 scenarios described in
`Spec.md § Benchmarking`.

`smoke.js` is the PR and merge-queue smoke scenario. It runs one virtual user
for 30 seconds, covers the current public API surface, and enforces the API
budget:

- p95 HTTP request duration below 200 ms
- failed request rate below 1%

```bash
AU_KPIS_BASE_URL=http://127.0.0.1:3000 \
  k6 run --out influxdb=http://127.0.0.1:8086/k6 apps/bench/smoke.js
```

Set `AU_KPIS_API_KEY` when the target should use an API-key rate-limit tier.
The compose observability stack provisions an InfluxDB v1 `k6` database and a
Grafana dashboard for trend review.

`sustained.js` is the nightly staging scenario. It runs 100 virtual users for
10 minutes with the production query mix from the spec: 70% single-series
observation/detail reads, 20% bulk observation reads, and 10% catalog/search
reads. It enforces:

- p95 HTTP request duration below 500 ms
- p99 HTTP request duration below 1500 ms
- failed request rate below 0.1%

```bash
AU_KPIS_BASE_URL=https://staging.example.test \
  k6 run --out influxdb=https://influxdb.example.test/k6 apps/bench/sustained.js
```

`burst.js` is the autoscale and rate-limit scenario. It ramps from 0 to 2000
virtual users over 2 minutes, holds for 2 minutes, then ramps down. It allows
rate-limited responses but enforces:

- 429 responses are present
- 429 responses stay below 30% of total responses
- 5xx responses stay below 0.5% of total responses

```bash
AU_KPIS_BASE_URL=https://staging.example.test \
  k6 run --out influxdb=https://influxdb.example.test/k6 apps/bench/burst.js
```

`full-load.js` is the production-v1 certification workload. It drives an exact
1,000 RPS mix for 30 minutes: 45% warm single-series JSON, 15% cold/range JSON,
10% rollups, 10% catalog/source/search, 10% APS latest, 5% APS history, and 5%
validation/rate-limit responses. In parallel it holds four one-million-row
Parquet exports open per API replica. It enforces:

- warm single-series and APS latest p95 below 200 ms
- cold/range, rollup, and APS history p95 below 500 ms
- Parquet p95 below 30 seconds
- server error rate below 0.1%
- no dropped k6 iterations

The setup phase validates the JSON page shape once. The load phase checks the
HTTP status and discards response bodies so client-side JSON parsing does not
hide server-side serving capacity.

```bash
mkdir -p target/release-scale-report
AU_KPIS_BASE_URL=https://scale-staging.example.test \
  k6 run --summary-export target/k6/full-load-summary.json apps/bench/full-load.js
```

The target must be backed by the isolated certification database seeded by
`au-kpis-scale-seed`, have a numeric APS snapshot, and keep normal ingestion
workers running. Set `AU_KPIS_API_REPLICAS` when the target does not have the
production default of two replicas. For local smoke-downs, override
`AU_KPIS_FULL_LOAD_RPS` and `AU_KPIS_FULL_LOAD_DURATION`.

`.github/workflows/k6-nightly.yml` runs both long-load scenarios nightly against
staging and writes the exported summaries as artifacts. The full-load scenario
is available through manual dispatch so operators can rerun the Phase 5
capacity validation without making it part of every nightly load run. Adding
the `perf:regression` label to a PR triggers the same workflow, downloads the
latest successful nightly baseline artifact when one exists, and posts a
side-by-side k6 load comparison comment back to the PR.
