# k6 Bench Scenarios

`smoke.js` is the PR and merge-queue smoke scenario described in
`Spec.md § Benchmarking`. It runs one virtual user for 30 seconds, covers the
current public API surface, and enforces the API budget:

- p95 HTTP request duration below 200 ms
- failed request rate below 1%

```bash
AU_KPIS_BASE_URL=http://127.0.0.1:3000 \
  k6 run --out influxdb=http://127.0.0.1:8086/k6 apps/bench/smoke.js
```

Set `AU_KPIS_API_KEY` when the target should use an API-key rate-limit tier.
The compose observability stack provisions an InfluxDB v1 `k6` database and a
Grafana dashboard for trend review. Later issues add sustained and burst
scenarios here without changing the local entrypoint convention.
