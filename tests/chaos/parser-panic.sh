#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/chaos/lib.sh"

run_step \
  "parser-panic" \
  "A parser panic is audited, rejects only its artifact, and does not abort sibling artifacts." \
  run_rust_checks \
  "cargo test -p au-kpis-ingestion-core --test pipeline_failures parser_panic_after_row_is_audited_without_aborting_sibling_artifacts -- --exact && cargo test -p au-kpis-adapter-aemo --lib parse_worker_error_distinguishes_panic_and_cancellation -- --exact"
