#!/usr/bin/env bash
set -euo pipefail

: "${GRAFANA_CLOUD_PROMETHEUS_QUERY_URL:?GRAFANA_CLOUD_PROMETHEUS_QUERY_URL is required}"
: "${GRAFANA_CLOUD_INSTANCE_ID:?GRAFANA_CLOUD_INSTANCE_ID is required}"
: "${GRAFANA_CLOUD_API_KEY:?GRAFANA_CLOUD_API_KEY is required}"

environment="${RAILWAY_ENVIRONMENT:-production}"
monitor_seconds="${AU_KPIS_RELEASE_MONITOR_SECONDS:-330}"
sample_seconds="${AU_KPIS_RELEASE_SAMPLE_SECONDS:-30}"
route_baselines="${AU_KPIS_ROUTE_BASELINES:-/v1/series/:dataflow/:series_key=0.2,/v1/observations=0.5,/v1/scorecards/aps/latest=0.2,/v1/scorecards/aps/history=0.5}"

query_url="${GRAFANA_CLOUD_PROMETHEUS_QUERY_URL%/}/api/v1/query"

prom_query() {
  local expression="$1"
  curl --fail --silent --show-error --max-time 15 \
    --user "${GRAFANA_CLOUD_INSTANCE_ID}:${GRAFANA_CLOUD_API_KEY}" \
    --get --data-urlencode "query=${expression}" "$query_url" \
    | jq -er '
        if .status != "success" then error("Prometheus query failed")
        elif (.data.result | length) == 0 then 0
        else [.data.result[].value[1] | tonumber] | max
        end
      '
}

float_gt() {
  awk -v left="$1" -v right="$2" 'BEGIN { exit !(left > right) }'
}

escape_prom_label() {
  sed 's/\\/\\\\/g; s/"/\\"/g' <<<"$1"
}

required_five_minute_samples=$((300 / sample_seconds))
required_two_minute_samples=$((120 / sample_seconds))
(( required_five_minute_samples > 0 && required_two_minute_samples > 0 )) || {
  printf 'sample interval must be no greater than 120 seconds\n' >&2
  exit 2
}

five_xx_bad=0
latency_bad=0
not_ready=0
deadline=$((SECONDS + monitor_seconds))

while (( SECONDS < deadline )); do
  ratio="$(prom_query "sum(rate(au_kpis_http_requests_total{environment=\"${environment}\",eligible=\"true\",status=~\"5..\"}[5m])) / clamp_min(sum(rate(au_kpis_http_requests_total{environment=\"${environment}\",eligible=\"true\"}[5m])), 1)")"
  if float_gt "$ratio" 0.01; then
    five_xx_bad=$((five_xx_bad + 1))
  else
    five_xx_bad=0
  fi

  sample_latency_bad=0
  IFS=',' read -r -a baselines <<<"$route_baselines"
  for route_baseline in "${baselines[@]}"; do
    route="${route_baseline%=*}"
    baseline="${route_baseline##*=}"
    escaped_route="$(escape_prom_label "$route")"
    p95="$(prom_query "histogram_quantile(0.95, sum by (le) (rate(au_kpis_http_request_duration_seconds_bucket{environment=\"${environment}\",eligible=\"true\",route=\"${escaped_route}\"}[5m])))")"
    if float_gt "$p95" "$(awk -v baseline="$baseline" 'BEGIN { print 2 * baseline }')"; then
      sample_latency_bad=1
      printf 'route %s p95=%s exceeded 2x baseline=%s\n' "$route" "$p95" "$baseline" >&2
    fi
  done
  if (( sample_latency_bad == 1 )); then
    latency_bad=$((latency_bad + 1))
  else
    latency_bad=0
  fi

  ready="$(prom_query "sum(up{environment=\"${environment}\",job=\"au-kpis-api\"})")"
  if float_gt 1 "$ready"; then
    not_ready=$((not_ready + 1))
  else
    not_ready=0
  fi

  printf 'release monitor: 5xx=%s bad5m=%s latency_bad5m=%s ready=%s no_ready2m=%s\n' \
    "$ratio" "$five_xx_bad" "$latency_bad" "$ready" "$not_ready"

  if (( five_xx_bad >= required_five_minute_samples )); then
    printf 'eligible-request 5xx exceeded 1%% for five minutes\n' >&2
    exit 1
  fi
  if (( latency_bad >= required_five_minute_samples )); then
    printf 'route p95 exceeded twice the certified baseline for five minutes\n' >&2
    exit 1
  fi
  if (( not_ready >= required_two_minute_samples )); then
    printf 'no ready API replica for two minutes\n' >&2
    exit 1
  fi

  sleep "$sample_seconds"
done

printf 'release monitor passed after %s seconds\n' "$monitor_seconds"
