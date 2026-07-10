#!/usr/bin/env bash
set -euo pipefail

: "${GRAFANA_CLOUD_PROMETHEUS_QUERY_URL:?GRAFANA_CLOUD_PROMETHEUS_QUERY_URL is required}"
: "${GRAFANA_CLOUD_INSTANCE_ID:?GRAFANA_CLOUD_INSTANCE_ID is required}"
: "${GRAFANA_CLOUD_API_KEY:?GRAFANA_CLOUD_API_KEY is required}"
: "${AU_KPIS_DB_CPU_QUERY:?AU_KPIS_DB_CPU_QUERY is required because Timescale provider metric names are account-specific}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_DIR="${AU_KPIS_SCALE_REPORT_DIR:-${ROOT}/target/release-scale-report}"
ENVIRONMENT="${AU_KPIS_SCALE_ENVIRONMENT:-staging}"
query_url="${GRAFANA_CLOUD_PROMETHEUS_QUERY_URL%/}/api/v1/query"

prom_query() {
  curl --fail --silent --show-error --max-time 30 \
    --user "${GRAFANA_CLOUD_INSTANCE_ID}:${GRAFANA_CLOUD_API_KEY}" \
    --get --data-urlencode "query=$1" "${query_url}" \
    | jq -er '
        if .status != "success" then error("Prometheus query failed")
        elif (.data.result | length) == 0 then error("Prometheus query returned no series")
        else [.data.result[].value[1] | tonumber] | max
        end
      '
}

db_cpu="$(prom_query "${AU_KPIS_DB_CPU_QUERY}")"
pool_utilization="$(prom_query "max_over_time((sum(au_kpis_db_pool_connections{environment=\"${ENVIRONMENT}\",state=\"in_use\"}) / clamp_min(sum(au_kpis_db_pool_connections{environment=\"${ENVIRONMENT}\",state=\"maximum\"}), 1))[35m:1m])")"
ingestion_jobs="$(prom_query "sum(increase(au_kpis_ingestion_jobs_completed_total{environment=\"${ENVIRONMENT}\"}[35m]))")"

python3 "${ROOT}/tools/release/build-scale-report.py" \
  --current "${REPORT_DIR}/full-load-summary.json" \
  --baseline "${REPORT_DIR}/baseline/full-load-summary.json" \
  --seed "${REPORT_DIR}/seed-manifest.json" \
  --parquet-time-log "${REPORT_DIR}/parquet-scale.log" \
  --parquet-heap-log "${REPORT_DIR}/parquet-memory.log" \
  --db-cpu "${db_cpu}" \
  --pool-utilization "${pool_utilization}" \
  --ingestion-jobs "${ingestion_jobs}" \
  --output "${REPORT_DIR}"
