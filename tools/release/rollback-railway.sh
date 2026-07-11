#!/usr/bin/env bash
set -euo pipefail

: "${RAILWAY_PROJECT_ID:?RAILWAY_PROJECT_ID is required}"
: "${RAILWAY_ENVIRONMENT:?RAILWAY_ENVIRONMENT is required}"
: "${RAILWAY_TOKEN:?RAILWAY_TOKEN is required}"

services=(otel-collector webhook-worker scheduler ingestion pdf-extractor web api)
rollback_manifest="${AU_KPIS_ROLLBACK_MANIFEST:-target/release-evidence/railway-rollback.json}"
graphql_url="https://backboard.railway.com/graphql/v2"
failed=0
for service in "${services[@]}"; do
  deployment_id=""
  if [[ -s "$rollback_manifest" ]]; then
    deployment_id="$(jq -r --arg service "$service" '.[$service] // empty' "$rollback_manifest")"
  fi
  if [[ -z "$deployment_id" ]]; then
    printf 'no previous successful deployment for %s; removing latest deployment\n' "$service" >&2
    if ! railway down --yes --service "$service" --project "$RAILWAY_PROJECT_ID" \
      --environment "$RAILWAY_ENVIRONMENT"; then
      printf 'failed to remove latest deployment for %s\n' "$service" >&2
      failed=1
    fi
    continue
  fi

  response="$(curl --fail --silent --show-error --max-time 30 \
    -H "Project-Access-Token: ${RAILWAY_TOKEN}" \
    -H 'content-type: application/json' \
    --data "$(jq -cn --arg id "$deployment_id" '{
      query: "mutation deploymentRollback($id: String!) { deploymentRollback(id: $id) { id status } }",
      variables: {id: $id}
    }')" \
    "$graphql_url")" || response=''
  rollback_id="$(jq -r '.data.deploymentRollback.id // empty' <<<"${response:-{}}")"
  if [[ -z "$rollback_id" ]]; then
    printf 'rollback API failed for %s deployment %s: %s\n' \
      "$service" "$deployment_id" "$response" >&2
    failed=1
    continue
  fi
  printf 'rollback started for %s from deployment %s as %s\n' \
    "$service" "$deployment_id" "$rollback_id"
done

if (( failed == 0 )); then
  for service in "${services[@]}"; do
    [[ -s "$rollback_manifest" ]] || continue
    [[ -n "$(jq -r --arg service "$service" '.[$service] // empty' "$rollback_manifest")" ]] \
      || continue
    deadline=$((SECONDS + 900))
    while (( SECONDS < deadline )); do
      payload="$(railway deployment list --service "$service" \
        --project "$RAILWAY_PROJECT_ID" --environment "$RAILWAY_ENVIRONMENT" \
        --limit 1 --json)"
      status="$(jq -r 'if type == "array" then .[0].status else .deployments[0].status end // "UNKNOWN"' \
        <<<"$payload")"
      case "$status" in
        SUCCESS|SUCCEEDED|HEALTHY) break ;;
        FAILED|CRASHED|REMOVED)
          printf 'rollback deployment for %s entered %s\n' "$service" "$status" >&2
          failed=1
          break
          ;;
      esac
      sleep 10
    done
    if (( SECONDS >= deadline )); then
      printf 'timed out waiting for %s rollback\n' "$service" >&2
      failed=1
    fi
  done
fi
exit "$failed"
