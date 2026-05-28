#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/chaos/lib.sh"

# record_result is called through run_step.
run_step \
  "kill-ingestion-mid-load" \
  "Kill ingestion worker mid-load and verify no duplicates/no gaps after produced work drains." \
  run_rust_checks \
  "cargo test -p au-kpis-ingestion-core --test pipeline_failures cancellation_drains_buffered_artifacts_that_are_already_fetched -- --exact && cargo test -p au-kpis-ingestion-core --test pipeline_failures duplicate_artifact_jobs_keep_pending_loads_separate -- --exact && cargo test -p au-kpis-ingestion --test cli run_mode_exits_within_configured_shutdown_grace_period -- --exact"
