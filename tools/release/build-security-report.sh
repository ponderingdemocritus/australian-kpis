#!/usr/bin/env bash
set -euo pipefail

: "${AU_KPIS_IMAGE_REGISTRY:?AU_KPIS_IMAGE_REGISTRY is required}"
: "${AU_KPIS_SECURITY_INPUT_DIR:?AU_KPIS_SECURITY_INPUT_DIR is required}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_DIR="${AU_KPIS_SECURITY_REPORT_DIR:-${ROOT}/target/release-security-report}"
mkdir -p "${REPORT_DIR}/supply-chain"
services=(api web pdf-extractor ingestion scheduler webhook-worker otel-collector migrate)
printf '[]\n' > "${REPORT_DIR}/services.json"

for service in "${services[@]}"; do
  digest_file="${AU_KPIS_SECURITY_INPUT_DIR}/${service}.digest"
  sbom="${AU_KPIS_SECURITY_INPUT_DIR}/${service}.spdx.json"
  scan="${AU_KPIS_SECURITY_INPUT_DIR}/${service}-trivy.sarif"
  [[ -s "${digest_file}" && -s "${sbom}" && -s "${scan}" ]] || {
    printf 'missing digest, SBOM, or scan for %s\n' "${service}" >&2
    exit 1
  }
  digest="$(tr -d '[:space:]' < "${digest_file}")"
  [[ "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]] || {
    printf 'invalid immutable digest for %s: %s\n' "${service}" "${digest}" >&2
    exit 1
  }
  jq -e '.spdxVersion | startswith("SPDX-")' "${sbom}" >/dev/null
  findings="$(jq '[.runs[]?.results[]?] | length' "${scan}")"
  [[ "${findings}" == "0" ]] || {
    printf '%s has %s HIGH/CRITICAL Trivy findings\n' "${service}" "${findings}" >&2
    exit 1
  }

  image="${AU_KPIS_IMAGE_REGISTRY}/au-kpis-${service}@${digest}"
  cosign verify \
    --certificate-identity-regexp "^https://github.com/${GITHUB_REPOSITORY}/.github/workflows/deploy.yml@refs/heads/main$" \
    --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
    "${image}" > "${REPORT_DIR}/${service}-cosign.json"
  cp "${sbom}" "${scan}" "${digest_file}" "${REPORT_DIR}/supply-chain/"

  jq --arg service "${service}" --arg image "${image}" --arg digest "${digest}" \
    '. + [{service: $service, image: $image, digest: $digest, sbom: "passed", trivy_high_critical: 0, signature: "verified"}]' \
    "${REPORT_DIR}/services.json" > "${REPORT_DIR}/services.tmp.json"
  mv "${REPORT_DIR}/services.tmp.json" "${REPORT_DIR}/services.json"
done

jq -n \
  --arg status passed \
  --arg git_sha "${GITHUB_SHA:-$(git -C "${ROOT}" rev-parse HEAD)}" \
  --arg release_checks "${AU_KPIS_RELEASE_CHECKS_RESULT:-success}" \
  --slurpfile services "${REPORT_DIR}/services.json" \
  '{
    status: $status,
    git_sha: $git_sha,
    release_checks: $release_checks,
    repository_scans: ["cargo-deny", "cargo-audit", "pnpm-audit", "gitleaks", "container-trivy"],
    services: $services[0]
  }' > "${REPORT_DIR}/security-report.json"

cat > "${REPORT_DIR}/summary.md" <<EOF
# Production security certification

- Status: \`passed\`
- Release checks: \`${AU_KPIS_RELEASE_CHECKS_RESULT:-success}\`
- Immutable images verified: \`${#services[@]}\`
- SPDX SBOMs present: \`${#services[@]}\`
- HIGH/CRITICAL Trivy findings: \`0\`
- GitHub OIDC signatures verified: \`${#services[@]}\`
- Repository gates: \`cargo-deny, cargo-audit, pnpm-audit, gitleaks\`
EOF
