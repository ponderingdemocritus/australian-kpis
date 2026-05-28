#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/chaos/lib.sh"

# record_result is called through run_step.
run_step \
  "source-5xx-circuit-breaker" \
  "Inject random source 5xx responses and verify circuit breaker opens and recovers through retry policy." \
  run_rust_checks \
  "cargo test -p au-kpis-adapter --lib circuit_breaker && cargo test -p au-kpis-adapter --test registry_dispatch adapter_error_classification_matches_retry_policy -- --exact && cargo test -p au-kpis-queue --test postgres nack_retries_then_dead_letters_after_attempt_budget -- --exact"
