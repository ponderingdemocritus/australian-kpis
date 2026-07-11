#!/usr/bin/env bash
set -euo pipefail

CHAOS_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHAOS_RESULTS_DIR="${CHAOS_RESULTS_DIR:-${CHAOS_ROOT}/target/release-chaos-report}"
CHAOS_RESULTS_JSONL="${CHAOS_RESULTS_DIR}/results.jsonl"
CHAOS_SUMMARY_MD="${CHAOS_RESULTS_DIR}/summary.md"
CHAOS_DRY_RUN="${CHAOS_DRY_RUN:-0}"
CHAOS_ENVIRONMENT="${CHAOS_ENVIRONMENT:-local}"
CHAOS_INITIALIZED="${CHAOS_INITIALIZED:-0}"

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  printf '%s' "${value}"
}

chaos_reset_results() {
  mkdir -p "${CHAOS_RESULTS_DIR}"
  : > "${CHAOS_RESULTS_JSONL}"
  jq -n \
    --arg suite "production-v1-operational-certification" \
    --arg environment "${CHAOS_ENVIRONMENT}" \
    --arg git_sha "$(git -C "${CHAOS_ROOT}" rev-parse HEAD 2>/dev/null || printf unknown)" \
    --arg started_at "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    '{suite: $suite, environment: $environment, git_sha: $git_sha, started_at: $started_at}' \
    > "${CHAOS_RESULTS_DIR}/manifest.json"
  {
    printf '# Chaos suite results\n\n'
    printf '| Scenario | Status | Invariant | Detail |\n'
    printf '|---|---:|---|---|\n'
  } > "${CHAOS_SUMMARY_MD}"
  export CHAOS_INITIALIZED=1
}

chaos_init_results() {
  if [[ "${CHAOS_INITIALIZED}" != "1" ]]; then
    chaos_reset_results
  fi
}

record_result() {
  local scenario="$1"
  local status="$2"
  local invariant="$3"
  local detail="$4"
  local timestamp
  timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

  chaos_init_results
  printf '{"timestamp":"%s","environment":"%s","scenario":"%s","status":"%s","invariant":"%s","detail":"%s"}\n' \
    "$(json_escape "${timestamp}")" \
    "$(json_escape "${CHAOS_ENVIRONMENT}")" \
    "$(json_escape "${scenario}")" \
    "$(json_escape "${status}")" \
    "$(json_escape "${invariant}")" \
    "$(json_escape "${detail}")" >> "${CHAOS_RESULTS_JSONL}"
  printf "| \`%s\` | \`%s\` | %s | %s |\n" \
    "${scenario}" \
    "${status}" \
    "$(json_escape "${invariant}")" \
    "$(json_escape "${detail}")" >> "${CHAOS_SUMMARY_MD}"
}

run_step() {
  local scenario="$1"
  local invariant="$2"
  shift 2

  if [[ "${CHAOS_DRY_RUN}" == "1" ]]; then
    record_result "${scenario}" "dry-run" "${invariant}" "Would run: $*"
    return 0
  fi

  local log_path="${CHAOS_RESULTS_DIR}/${scenario}.log"
  local status
  printf '::group::%s\n' "${scenario}"
  set +e
  "$@" 2>&1 | tee "${log_path}"
  status="${PIPESTATUS[0]}"
  set -e
  if [[ "${status}" == "0" ]]; then
    printf '::endgroup::\n'
    record_result "${scenario}" "pass" "${invariant}" "Command passed; log: ${scenario}.log"
    return 0
  fi

  printf '::endgroup::\n'
  record_result "${scenario}" "fail" "${invariant}" "Command failed (${status}); log: ${scenario}.log"
  return "${status}"
}

run_rust_checks() {
  local command="$1"
  CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}" \
    CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" \
    RUSTC_WRAPPER="${RUSTC_WRAPPER:-}" \
    bash -c "${command}"
}
