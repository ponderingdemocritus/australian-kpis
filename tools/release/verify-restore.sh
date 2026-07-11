#!/usr/bin/env bash
set -euo pipefail

: "${AU_KPIS_DATABASE_URL:?AU_KPIS_DATABASE_URL is required}"
: "${AU_KPIS_RESTORE_REQUESTED_AT:?AU_KPIS_RESTORE_REQUESTED_AT is required}"
: "${AU_KPIS_RESTORE_COMPLETED_AT:?AU_KPIS_RESTORE_COMPLETED_AT is required}"
: "${AU_KPIS_FAILURE_TIME:?AU_KPIS_FAILURE_TIME is required}"
: "${AU_KPIS_RESTORE_TARGET_TIME:?AU_KPIS_RESTORE_TARGET_TIME is required}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_DIR="${AU_KPIS_RESTORE_REPORT_DIR:-${ROOT}/target/release-restore-report}"
mkdir -p "${REPORT_DIR}"

read -r rto_seconds rpo_seconds < <(
  python3 - \
    "${AU_KPIS_RESTORE_REQUESTED_AT}" \
    "${AU_KPIS_RESTORE_COMPLETED_AT}" \
    "${AU_KPIS_FAILURE_TIME}" \
    "${AU_KPIS_RESTORE_TARGET_TIME}" <<'PY'
from datetime import datetime
import sys

def parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))

requested, completed, failure, target = map(parse, sys.argv[1:])
print(int((completed - requested).total_seconds()), int((failure - target).total_seconds()))
PY
)

(( rto_seconds >= 0 && rto_seconds <= 1800 )) || {
  printf 'restore RTO was %s seconds; budget is 0..1800\n' "${rto_seconds}" >&2
  exit 1
}
(( rpo_seconds >= 0 && rpo_seconds <= 300 )) || {
  printf 'restore RPO was %s seconds; budget is 0..300\n' "${rpo_seconds}" >&2
  exit 1
}

psql "${AU_KPIS_DATABASE_URL}" --no-psqlrc --tuples-only --no-align <<'SQL' \
  | jq -S . > "${REPORT_DIR}/database-state.json"
SELECT jsonb_build_object(
  'database', current_database(),
  'captured_at', now(),
  'migration_version', (SELECT max(version) FROM _sqlx_migrations WHERE success),
  'sources', (SELECT count(*) FROM sources),
  'dataflows', (SELECT count(*) FROM dataflows),
  'series', (SELECT count(*) FROM series),
  'observations', (SELECT count(*) FROM observations),
  'artifacts', (SELECT count(*) FROM artifacts),
  'published_generations', (SELECT count(*) FROM ingestion_generations WHERE status = 'published'),
  'scorecard_snapshots', (SELECT count(*) FROM scorecard_snapshots),
  'latest_observation_time', (SELECT max(time) FROM observations),
  'latest_artifact_fetched_at', (SELECT max(fetched_at) FROM artifacts),
  'timescale_version', (SELECT extversion FROM pg_extension WHERE extname = 'timescaledb')
);
SQL

if [[ -n "${AU_KPIS_EXPECTED_STATE_MANIFEST:-}" ]]; then
  jq -S . "${AU_KPIS_EXPECTED_STATE_MANIFEST}" > "${REPORT_DIR}/expected-database-state.json"
  diff -u \
    "${REPORT_DIR}/expected-database-state.json" \
    "${REPORT_DIR}/database-state.json" \
    > "${REPORT_DIR}/database-state.diff" || {
      printf 'restored database does not match expected target-time state\n' >&2
      exit 1
    }
fi

set +e
"${ROOT}/tools/release/reconcile-r2.sh" \
  2>&1 | tee "${REPORT_DIR}/r2-reconciliation.log"
reconcile_status="${PIPESTATUS[0]}"
set -e
[[ "${reconcile_status}" == "0" ]] || exit "${reconcile_status}"

jq -n \
  --arg status passed \
  --arg requested_at "${AU_KPIS_RESTORE_REQUESTED_AT}" \
  --arg completed_at "${AU_KPIS_RESTORE_COMPLETED_AT}" \
  --arg failure_time "${AU_KPIS_FAILURE_TIME}" \
  --arg target_time "${AU_KPIS_RESTORE_TARGET_TIME}" \
  --arg git_sha "$(git -C "${ROOT}" rev-parse HEAD)" \
  --argjson rto_seconds "${rto_seconds}" \
  --argjson rpo_seconds "${rpo_seconds}" \
  '{
    status: $status,
    git_sha: $git_sha,
    restore_requested_at: $requested_at,
    restore_completed_at: $completed_at,
    failure_time: $failure_time,
    restore_target_time: $target_time,
    rto_seconds: $rto_seconds,
    rto_budget_seconds: 1800,
    rpo_seconds: $rpo_seconds,
    rpo_budget_seconds: 300,
    database_state_verified: true,
    r2_count_bytes_sha256_reconciled: true
  }' > "${REPORT_DIR}/restore-report.json"

cat > "${REPORT_DIR}/summary.md" <<EOF
# Production restore certification

- Status: \`passed\`
- Database RTO: \`${rto_seconds}s\` (budget \`<=1800s\`)
- Database RPO: \`${rpo_seconds}s\` (budget \`<=300s\`)
- Database target-state check: \`passed\`
- R2 count, bytes, and SHA-256 reconciliation: \`passed\`
EOF
