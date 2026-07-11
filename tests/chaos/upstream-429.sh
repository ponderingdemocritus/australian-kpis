#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/chaos/lib.sh"

run_step \
  "upstream-429" \
  "Upstream throttling preserves Retry-After and enters bounded transient retry." \
  run_rust_checks \
  "cargo test -p au-kpis-adapter-abs --test fetch fetch_preserves_retry_after_on_upstream_throttle -- --exact && cargo test -p au-kpis-adapter-aemo --test fetch fetch_preserves_retry_after_on_nemweb_throttle -- --exact && cargo test -p au-kpis-adapter --test registry_dispatch adapter_error_classification_matches_retry_policy -- --exact"
