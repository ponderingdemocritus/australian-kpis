#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/chaos/lib.sh"

# record_result is called through run_step.
run_step \
  "sever-db-connection" \
  "Sever DB connection and verify reconnection plus queued jobs resume through lease reclaim." \
  run_rust_checks \
  "cargo test -p au-kpis-queue --test postgres stale_running_jobs_are_reclaimed_after_lease_timeout -- --exact && cargo test -p au-kpis-queue --test postgres renew_extends_lease_and_invalidates_old_handle -- --exact"
