#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${ROOT}/infra/compose/docker-compose.yml"
RULES_DIR="${ROOT}/infra/observability/prometheus/rules"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://127.0.0.1:9090}"
PUSHGATEWAY_URL="${PUSHGATEWAY_URL:-http://127.0.0.1:9091}"

wait_for_http() {
  local url="$1"
  local name="$2"
  for _ in $(seq 1 60); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  printf '%s\n' "${name} did not become ready at ${url}" >&2
  return 1
}

# Runs promtool test rules against the committed SLO fixture before touching a live stack.
docker run --rm \
  --entrypoint promtool \
  -v "${RULES_DIR}:/rules:ro" \
  --workdir /rules \
  prom/prometheus:v3.0.1 \
  test rules chaos-drill.test.yml

docker compose -f "${COMPOSE_FILE}" --profile observability up -d prometheus alertmanager pushgateway
wait_for_http "${PROMETHEUS_URL}/-/ready" "Prometheus"
wait_for_http "${PUSHGATEWAY_URL}/-/healthy" "Pushgateway"

printf 'au_kpis_chaos_error_ratio{service="chaos-drill"} 0.02\n' | curl -fsS \
  --data-binary @- \
  "${PUSHGATEWAY_URL}/metrics/job/au-kpis-chaos/instance/local"

for _ in $(seq 1 20); do
  alerts_json="$(curl -fsS "${PROMETHEUS_URL}/api/v1/alerts")"
  if python3 -c '
import json
import sys

payload = json.load(sys.stdin)
for alert in payload.get("data", {}).get("alerts", []):
    labels = alert.get("labels", {})
    if (
        labels.get("alertname") == "AuKpisChaosDrillCanaryFiring"
        and labels.get("severity") == "page"
    ):
        sys.exit(0)
sys.exit(1)
' <<<"${alerts_json}"; then
    curl -fsS -X DELETE "${PUSHGATEWAY_URL}/metrics/job/au-kpis-chaos/instance/local" >/dev/null
    printf '%s\n' "Chaos drill alert fired and was observed through Prometheus."
    exit 0
  fi
  sleep 3
done

curl -fsS -X DELETE "${PUSHGATEWAY_URL}/metrics/job/au-kpis-chaos/instance/local" >/dev/null || true
printf '%s\n' "AuKpisChaosDrillCanaryFiring did not become active." >&2
exit 1
