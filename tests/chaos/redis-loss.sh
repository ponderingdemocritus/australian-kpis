#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/chaos/lib.sh"

run_step \
  "redis-loss" \
  "Public GETs degrade open while protected writes fail closed with retry guidance." \
  run_rust_checks \
  "cargo test -p au-kpis-api-http --test rate_limit redis_failure_degrades_public_gets_and_rejects_writes -- --exact"
