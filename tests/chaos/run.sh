#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/chaos/lib.sh"

SCENARIOS=(
  kill-ingestion-mid-load
  sever-db-connection
  fill-queue-capacity
  source-5xx-circuit-breaker
  vacuum-heavy-writes
)

REQUESTED_SCENARIOS=()

usage() {
  cat <<'EOF'
Usage: tests/chaos/run.sh [--dry-run] [--results-dir DIR] [--scenario NAME]

Runs the scripted chaos suite. Omit --scenario to run every scenario.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      export CHAOS_DRY_RUN=1
      shift
      ;;
    --results-dir)
      export CHAOS_RESULTS_DIR="$2"
      export CHAOS_RESULTS_JSONL="${CHAOS_RESULTS_DIR}/results.jsonl"
      export CHAOS_SUMMARY_MD="${CHAOS_RESULTS_DIR}/summary.md"
      shift 2
      ;;
    --scenario)
      REQUESTED_SCENARIOS+=("$2")
      shift 2
      ;;
    --list)
      printf '%s\n' "${SCENARIOS[@]}"
      exit 0
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "${#REQUESTED_SCENARIOS[@]}" -eq 0 ]]; then
  REQUESTED_SCENARIOS=("${SCENARIOS[@]}")
fi

chaos_reset_results
status=0

for scenario in "${REQUESTED_SCENARIOS[@]}"; do
  found=0
  for known in "${SCENARIOS[@]}"; do
    if [[ "${scenario}" == "${known}" ]]; then
      found=1
      break
    fi
  done
  if [[ "${found}" != "1" ]]; then
    printf 'unknown chaos scenario: %s\n' "${scenario}" >&2
    status=2
    continue
  fi

  if ! "${ROOT}/tests/chaos/${scenario}.sh"; then
    status=1
  fi
done

printf 'Chaos results written to %s\n' "${CHAOS_RESULTS_DIR}"
exit "${status}"
