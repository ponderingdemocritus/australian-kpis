#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/chaos/lib.sh"

# record_result is called through run_step.
run_step \
  "vacuum-heavy-writes" \
  "Run compaction/vacuum-adjacent DB maintenance during heavy writes and verify no deadlocks." \
  run_rust_checks \
  "cargo test -p au-kpis-db --test migrations migration_creates_hypertable_and_compression_policy -- --exact && cargo test -p au-kpis-loader --test load_batch default_options_split_observations_at_one_thousand_rows -- --exact"
