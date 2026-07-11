#!/usr/bin/env bash
set -euo pipefail

: "${AU_KPIS_BASE_URL:?AU_KPIS_BASE_URL is required}"
base_url="${AU_KPIS_BASE_URL%/}"
api_key="${AU_KPIS_API_KEY:-}"
headers=(-H 'accept: application/json')
if [[ -n "$api_key" ]]; then
  headers+=(-H "x-api-key: ${api_key}")
fi

get_json() {
  local path="$1"
  curl --fail --silent --show-error --max-time 30 "${headers[@]}" "${base_url}${path}"
}

get_json /livez | jq -e '.status == "live"' >/dev/null
get_json /readyz | jq -e '.status == "ready" or .status == "degraded"' >/dev/null
get_json /v1/openapi.json | jq -e '.openapi | startswith("3.")' >/dev/null
get_json /v1/sources | jq -e '.sources | length >= 1' >/dev/null
get_json "/v1/observations?dataflow=${AU_KPIS_SMOKE_DATAFLOW:-abs.cpi}&limit=1" \
  | jq -e '.observations | type == "array"' >/dev/null
get_json /v1/scorecards/aps/latest \
  | jq -e '.publication_state == "published" or .publication_state == "insufficient_coverage"' >/dev/null

if [[ -n "${AU_KPIS_BFF_BASE_URL:-}" ]]; then
  curl --fail --silent --show-error --max-time 30 \
    "${AU_KPIS_BFF_BASE_URL%/}/api/au-kpis/v1/scorecards/aps/latest" \
    | jq -e '.publication_state == "published" or .publication_state == "insufficient_coverage"' \
    >/dev/null
fi

if [[ -n "${AU_KPIS_WEBHOOK_CANARY_URL:-}" && -n "$api_key" ]]; then
  created="$(curl --fail --silent --show-error --max-time 30 \
    "${headers[@]}" -H 'content-type: application/json' \
    -X POST "${base_url}/v1/subscriptions" \
    --data "$(jq -cn --arg url "$AU_KPIS_WEBHOOK_CANARY_URL" \
      --arg dataflow "${AU_KPIS_SMOKE_DATAFLOW:-abs.cpi}" \
      '{url:$url,dataflow_ids:[$dataflow]}')")"
  subscription_id="$(jq -er '.subscription.id' <<<"$created")"
  jq -e '.subscription.status == "pending_verification" and (.signing_secret | length >= 43)' \
    <<<"$created" >/dev/null
  curl --fail --silent --show-error --max-time 30 "${headers[@]}" \
    -X POST "${base_url}/v1/subscriptions/${subscription_id}/verify" \
    | jq -e '.status == "active"' >/dev/null
  curl --fail --silent --show-error --max-time 30 "${headers[@]}" \
    -X DELETE "${base_url}/v1/subscriptions/${subscription_id}" >/dev/null
fi

printf 'post-deploy smoke passed for %s\n' "$base_url"
