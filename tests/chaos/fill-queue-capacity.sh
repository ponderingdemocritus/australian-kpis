#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/chaos/lib.sh"

# record_result is called through run_step.
run_step \
  "fill-queue-capacity" \
  "Fill queue to capacity and verify backpressure propagates without OOM or dropped work." \
  run_rust_checks \
  "cargo test -p au-kpis-ingestion-core --lib tests::produced_handoff_waits_for_capacity_instead_of_dropping_item -- --exact && cargo test -p au-kpis-ingestion-core --lib tests::produced_handoff_does_not_drop_item_when_full_and_cancelled -- --exact"
