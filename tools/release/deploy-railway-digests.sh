#!/usr/bin/env bash
set -euo pipefail

: "${RAILWAY_PROJECT_ID:?RAILWAY_PROJECT_ID is required}"
: "${RAILWAY_ENVIRONMENT:?RAILWAY_ENVIRONMENT is required}"
: "${AU_KPIS_IMAGE_REGISTRY:?AU_KPIS_IMAGE_REGISTRY is required}"
: "${AU_KPIS_DIGEST_DIR:?AU_KPIS_DIGEST_DIR is required}"

services=(api web pdf-extractor ingestion scheduler webhook-worker otel-collector)
rollback_manifest="${AU_KPIS_ROLLBACK_MANIFEST:-target/release-evidence/railway-rollback.json}"
mkdir -p "$(dirname "$rollback_manifest")"
printf '{}\n' >"$rollback_manifest"

for service in "${services[@]}"; do
  payload="$(railway deployment list --service "$service" \
    --project "$RAILWAY_PROJECT_ID" --environment "$RAILWAY_ENVIRONMENT" \
    --limit 20 --json)"
  deployment_id="$(jq -r '
      (if type == "array" then . else .deployments end)
      | map(select(.status == "SUCCESS" or .status == "SUCCEEDED" or .status == "HEALTHY"))
      | first | .id // empty
    ' <<<"$payload")"
  if [[ -n "$deployment_id" ]]; then
    temporary="${rollback_manifest}.tmp"
    jq --arg service "$service" --arg id "$deployment_id" \
      '. + {($service): $id}' "$rollback_manifest" >"$temporary"
    mv "$temporary" "$rollback_manifest"
  fi
done

for service in "${services[@]}"; do
  digest_file="${AU_KPIS_DIGEST_DIR}/${service}.digest"
  [[ -s "$digest_file" ]] || {
    printf 'missing image digest for %s: %s\n' "$service" "$digest_file" >&2
    exit 1
  }
  digest="$(tr -d '[:space:]' <"$digest_file")"
  image="${AU_KPIS_IMAGE_REGISTRY}/au-kpis-${service}@${digest}"
  railway service source connect --image "$image" --service "$service" \
    --project "$RAILWAY_PROJECT_ID" --environment "$RAILWAY_ENVIRONMENT" --json >/dev/null
done

printf 'rollback manifest recorded at %s\n' "$rollback_manifest"

for service in "${services[@]}"; do
  deadline=$((SECONDS + 900))
  while (( SECONDS < deadline )); do
    payload="$(railway deployment list --service "$service" \
      --project "$RAILWAY_PROJECT_ID" --environment "$RAILWAY_ENVIRONMENT" \
      --limit 1 --json)"
    status="$(jq -r 'if type == "array" then .[0].status else .deployments[0].status end // "UNKNOWN"' \
      <<<"$payload")"
    case "$status" in
      SUCCESS|SUCCEEDED|HEALTHY)
        printf '%s deployed successfully\n' "$service"
        break
        ;;
      FAILED|CRASHED|REMOVED)
        printf '%s deployment entered terminal status %s\n' "$service" "$status" >&2
        exit 1
        ;;
    esac
    sleep 10
  done
  (( SECONDS < deadline )) || {
    printf 'timed out waiting for %s deployment\n' "$service" >&2
    exit 1
  }
done
