#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RULES_DIR="${ROOT}/infra/observability/prometheus/rules"
RESULTS_DIR="${1:-${ROOT}/target/release-chaos-report/alerts}"
FIXTURE_NAME=".all-page-alerts-drill.$$.test.yml"
FIXTURE="${RULES_DIR}/${FIXTURE_NAME}"
trap 'rm -f "${FIXTURE}"' EXIT

mkdir -p "${RESULTS_DIR}"
python3 "${ROOT}/tools/observability/generate-alert-drill-fixture.py" \
  --rules "${RULES_DIR}/slo-burn-rates.yml" \
  --output "${FIXTURE}" \
  --inventory "${RESULTS_DIR}/alert-inventory.json"
cp "${FIXTURE}" "${RESULTS_DIR}/all-page-alerts.test.yml"

if command -v promtool >/dev/null 2>&1; then
  (cd "${RULES_DIR}" && promtool test rules "${FIXTURE_NAME}") \
    2>&1 | tee "${RESULTS_DIR}/promtool.log"
else
  docker run --rm \
    --entrypoint /bin/promtool \
    -v "${ROOT}:/workspace:ro" \
    -w /workspace/infra/observability/prometheus/rules \
    prom/prometheus:v3.4.1 \
    test rules "${FIXTURE_NAME}" \
    2>&1 | tee "${RESULTS_DIR}/promtool.log"
fi

grep -q "SUCCESS" "${RESULTS_DIR}/promtool.log"
jq '.status = "passed"' "${RESULTS_DIR}/alert-inventory.json" \
  > "${RESULTS_DIR}/alert-inventory.tmp.json"
mv "${RESULTS_DIR}/alert-inventory.tmp.json" "${RESULTS_DIR}/alert-inventory.json"
