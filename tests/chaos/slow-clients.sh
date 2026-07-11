#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/chaos/lib.sh"

run_step \
  "slow-clients" \
  "A disconnected bulk client releases the stream writer promptly without buffering the export." \
  run_rust_checks \
  "cargo test -p au-kpis-api-http --lib observations::tests::parquet_writer_returns_promptly_when_response_receiver_closes_mid_stream -- --exact && cargo test -p au-kpis-api-http --lib observations::tests::parquet_writer_streams_more_than_json_page_cap -- --exact"
