#!/usr/bin/env bash
set -euo pipefail

: "${GH_TOKEN:?GH_TOKEN is required}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
: "${GRAFANA_CLOUD_PROMETHEUS_QUERY_URL:?GRAFANA_CLOUD_PROMETHEUS_QUERY_URL is required}"
: "${GRAFANA_CLOUD_INSTANCE_ID:?GRAFANA_CLOUD_INSTANCE_ID is required}"
: "${GRAFANA_CLOUD_API_KEY:?GRAFANA_CLOUD_API_KEY is required}"
: "${AU_KPIS_SOAK_REPLICA_QUERIES_JSON:?AU_KPIS_SOAK_REPLICA_QUERIES_JSON is required}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_DIR="${AU_KPIS_SOAK_REPORT_DIR:-${ROOT}/target/release-soak-report}"
WINDOW_DAYS="${AU_KPIS_SOAK_WINDOW_DAYS:-7}"
mkdir -p "${REPORT_DIR}"

since="$(python3 - "${WINDOW_DAYS}" <<'PY'
from datetime import datetime, timedelta, timezone
import sys
print((datetime.now(timezone.utc) - timedelta(days=int(sys.argv[1]))).strftime("%Y-%m-%dT%H:%M:%SZ"))
PY
)"

workflows=(
  "data-quality.yml|168|Data Quality"
  "fuzz-nightly.yml|7|Parser fuzz"
  "contract-nightly.yml|7|Contract fuzz"
  "mutation-weekly.yml|1|Mutation"
  "source-location-audit.yml|1|Source audit"
  "k6-nightly.yml|7|k6"
  "deploy-smoke-daily.yml|7|Deploy smoke"
)

printf '{"window_days":%s,"window_started_at":%s,"checks":[' \
  "${WINDOW_DAYS}" "$(jq -Rn --arg value "${since}" '$value')" \
  > "${REPORT_DIR}/soak-report.json"
printf '# Production staging soak\n\n| Check | Required successes | Successes | Failures | Status |\n|---|---:|---:|---:|---:|\n' \
  > "${REPORT_DIR}/summary.md"

overall=passed
separator=""
for contract in "${workflows[@]}"; do
  IFS='|' read -r workflow required label <<<"${contract}"
  runs_file="$(mktemp)"
  gh api --paginate -X GET \
    "repos/${GITHUB_REPOSITORY}/actions/workflows/${workflow}/runs" \
    -f event=schedule \
    -f status=completed \
    -f created=">=${since}" \
    -f per_page=100 \
    --jq '.workflow_runs[] | {id, created_at, conclusion, html_url}' \
    | jq -s . > "${runs_file}"
  successes="$(jq '[.[] | select(.conclusion == "success")] | length' "${runs_file}")"
  failures="$(jq '[.[] | select(.conclusion != "success")] | length' "${runs_file}")"
  status=passed
  if (( successes < required || failures > 0 )); then
    status=blocked
    overall=blocked
  fi
  check="$(jq -n \
    --arg workflow "${workflow}" \
    --arg label "${label}" \
    --arg status "${status}" \
    --argjson required "${required}" \
    --argjson successes "${successes}" \
    --argjson failures "${failures}" \
    --slurpfile runs "${runs_file}" \
    '{workflow: $workflow, label: $label, required_successes: $required, successes: $successes, failures: $failures, status: $status, runs: $runs[0]}')"
  printf '%s%s' "${separator}" "${check}" >> "${REPORT_DIR}/soak-report.json"
  separator=,
  printf "| %s | %s | %s | %s | \`%s\` |\n" \
    "${label}" "${required}" "${successes}" "${failures}" "${status}" \
    >> "${REPORT_DIR}/summary.md"
  rm -f "${runs_file}"
done

replica_services=(api web pdf-extractor ingestion scheduler webhook-worker otel-collector)
printf '[]\n' > "${REPORT_DIR}/replica-evidence.json"
replica_status=passed
for service in "${replica_services[@]}"; do
  expression="$(jq -er --arg service "${service}" '.[$service]' \
    <<<"${AU_KPIS_SOAK_REPLICA_QUERIES_JSON}")"
  [[ "${expression}" == *"[7d"* ]] || {
    printf 'replica query for %s must measure a seven-day range\n' "${service}" >&2
    exit 1
  }
  response="$(curl --fail --silent --show-error --max-time 30 \
    --user "${GRAFANA_CLOUD_INSTANCE_ID}:${GRAFANA_CLOUD_API_KEY}" \
    --get --data-urlencode "query=${expression}" \
    "${GRAFANA_CLOUD_PROMETHEUS_QUERY_URL%/}/api/v1/query")"
  minimum="$(jq -er '
      if .status != "success" or (.data.result | length) == 0
      then error("replica query returned no data")
      else [.data.result[].value[1] | tonumber] | min
      end
    ' <<<"${response}")"
  service_status=passed
  if awk -v minimum="${minimum}" 'BEGIN { exit !(minimum < 2) }'; then
    service_status=blocked
    replica_status=blocked
    overall=blocked
  fi
  jq --arg service "${service}" --arg expression "${expression}" \
    --arg status "${service_status}" --argjson minimum "${minimum}" \
    '. + [{service: $service, expression: $expression, minimum_replicas: $minimum, required_replicas: 2, status: $status}]' \
    "${REPORT_DIR}/replica-evidence.json" > "${REPORT_DIR}/replica-evidence.tmp.json"
  mv "${REPORT_DIR}/replica-evidence.tmp.json" "${REPORT_DIR}/replica-evidence.json"
done
replica_check="$(jq -n \
  --arg status "${replica_status}" \
  --slurpfile evidence "${REPORT_DIR}/replica-evidence.json" \
  '{workflow: "provider-replica-metrics", label: "Two-replica seven-day continuity", required_successes: 2, successes: ([ $evidence[0][].minimum_replicas ] | min), failures: ([ $evidence[0][] | select(.status != "passed") ] | length), status: $status, runs: $evidence[0]}')"
printf '%s%s' "${separator}" "${replica_check}" >> "${REPORT_DIR}/soak-report.json"
printf "| Two-replica seven-day continuity | 2 | %s | %s | \`%s\` |\n" \
  "$(jq '[.[].minimum_replicas] | min' "${REPORT_DIR}/replica-evidence.json")" \
  "$(jq '[.[] | select(.status != "passed")] | length' "${REPORT_DIR}/replica-evidence.json")" \
  "${replica_status}" >> "${REPORT_DIR}/summary.md"

printf '],"status":%s}\n' "$(jq -Rn --arg value "${overall}" '$value')" \
  >> "${REPORT_DIR}/soak-report.json"
jq . "${REPORT_DIR}/soak-report.json" > "${REPORT_DIR}/soak-report.tmp.json"
mv "${REPORT_DIR}/soak-report.tmp.json" "${REPORT_DIR}/soak-report.json"

printf "\nOverall: \`%s\`\n" "${overall}" >> "${REPORT_DIR}/summary.md"
[[ "${overall}" == passed ]]
