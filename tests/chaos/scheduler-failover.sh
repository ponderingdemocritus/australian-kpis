#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/chaos/lib.sh"

run_step \
  "scheduler-failover" \
  "Exactly one scheduler leads and the standby takes over without duplicate occurrences." \
  run_rust_checks \
  "cargo test -p au-kpis-scheduler --test scheduler two_schedulers_singleton_and_failover_emits_discovery_jobs -- --exact"
