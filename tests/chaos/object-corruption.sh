#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/chaos/lib.sh"

run_step \
  "object-corruption" \
  "Same-size object corruption is detected by SHA-256 and repaired from verified staged bytes." \
  run_rust_checks \
  "cargo test -p au-kpis-storage --lib tests::commit_staged_artifact_repairs_canonical_hash_mismatch -- --exact && cargo test -p au-kpis-adapter-abs --test fetch fetch_repairs_rewritten_storage_key_when_durable_blob_hash_mismatches -- --exact"
