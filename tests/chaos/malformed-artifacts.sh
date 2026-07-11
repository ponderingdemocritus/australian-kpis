#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/tests/chaos/lib.sh"

run_step \
  "malformed-artifacts" \
  "Malformed XLSX and source rows become format errors without unwinding the worker." \
  run_rust_checks \
  "cargo test -p au-kpis-adapter-apra --lib tests::malformed_xlsx_returns_format_error_instead_of_panicking -- --exact && cargo test -p au-kpis-adapter-rba --lib tests::malformed_xlsx_returns_format_error_instead_of_panicking -- --exact && cargo test -p au-kpis-adapter-abs --test discover parse_dataflow_listing_rejects_malformed_non_cpi_rows -- --exact"
