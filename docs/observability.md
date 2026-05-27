# Observability

The local observability stack is self-hosted and mirrors the production path in `Spec.md`
with OTLP traces, Prometheus metrics, Loki logs, Tempo traces, Grafana dashboards, and
page-level alert routing.

Start it with:

```bash
docker compose -f infra/compose/docker-compose.yml --profile observability up -d --build
```

Grafana is available at `http://127.0.0.1:3002` with local defaults `admin` / `admin`.
The stack also exposes Prometheus on `:9090`, Alertmanager on `:9093`, Loki on `:3100`,
Tempo on `:3200`, OTLP on `:4317`/`:4318`, Pushgateway on `:9091`, and the k6
InfluxDB database on `:8086`.

## Signal Flow

- Rust services export OTLP spans to `otel-collector`.
- `otel-collector` writes traces to Tempo and exposes OTLP metrics for Prometheus.
- The ingestion worker exposes `/metrics`; Prometheus scrapes it directly.
- Promtail tails Compose container logs and pushes JSON log lines to Loki.
- The k6 smoke scenario writes request duration and failure-rate samples to InfluxDB.
- Grafana provisions Prometheus, Loki, Tempo, and k6 InfluxDB datasources plus the
  required dashboards.

## Dashboards

- Freshness heatmap: `infra/observability/grafana/dashboards/freshness-heatmap.json`
- Per-source error rate: `infra/observability/grafana/dashboards/error-rate.json`
- API latency p50/p95/p99: `infra/observability/grafana/dashboards/api-latency.json`
- Queue depth, worker saturation, and DB state: `infra/observability/grafana/dashboards/queue-db.json`
- SLO burn rates and active page alerts: `infra/observability/grafana/dashboards/slo-burn-rates.json`
- k6 smoke p95, failure rate, and endpoint request rate: `infra/observability/grafana/dashboards/k6-smoke.json`

## Alert Routing

Alertmanager routes `severity="page"` alerts to Slack and PagerDuty. Set these before
starting the observability profile:

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
export SLACK_CHANNEL="#au-kpis-alerts"
export PAGERDUTY_ROUTING_KEY="..."
```

Without those values, Compose uses local dummy values so the stack can still boot for
dashboard and rule validation.

## Chaos Drill

Run the drill after the observability profile is up:

```bash
tools/observability/chaos-drill.sh
```

The script first runs `promtool test rules`, then pushes
`au_kpis_chaos_error_ratio` through Pushgateway and verifies that
`AuKpisChaosDrillCanaryFiring` becomes active via Prometheus `/api/v1/alerts`.
