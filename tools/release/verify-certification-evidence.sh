#!/usr/bin/env bash
set -euo pipefail

: "${GH_TOKEN:?GH_TOKEN is required}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_DIR="${AU_KPIS_SOAK_REPORT_DIR:-${ROOT}/target/release-soak-report}"
DOWNLOAD_DIR="$(mktemp -d)"
trap 'rm -rf "${DOWNLOAD_DIR}"' EXIT
mkdir -p "${REPORT_DIR}"
printf '[]\n' > "${REPORT_DIR}/evidence-index.json"

contracts=(
  "release-scale-report|30|scale-report.json"
  "release-restore-report|30|restore-report.json"
  "release-chaos-report|8|alert-inventory.json"
  "release-security-report|30|security-report.json"
)

for contract in "${contracts[@]}"; do
  IFS='|' read -r artifact max_age_days report_name <<<"${contract}"
  metadata="$(gh api -X GET \
    "repos/${GITHUB_REPOSITORY}/actions/artifacts" \
    -f name="${artifact}" -f per_page=100)"
  selected="$(jq -c '[.artifacts[] | select(.expired == false)] | sort_by(.created_at) | reverse | first // empty' \
    <<<"${metadata}")"
  [[ -n "${selected}" ]] || {
    printf 'no unexpired %s artifact exists\n' "${artifact}" >&2
    exit 1
  }
  artifact_id="$(jq -r '.id' <<<"${selected}")"
  created_at="$(jq -r '.created_at' <<<"${selected}")"
  age_seconds="$(python3 - "${created_at}" <<'PY'
from datetime import datetime, timezone
import sys
created = datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00"))
print(int((datetime.now(timezone.utc) - created).total_seconds()))
PY
)"
  (( age_seconds >= 0 && age_seconds <= max_age_days * 86400 )) || {
    printf '%s artifact is %s seconds old; maximum is %s days\n' \
      "${artifact}" "${age_seconds}" "${max_age_days}" >&2
    exit 1
  }

  artifact_dir="${DOWNLOAD_DIR}/${artifact}"
  mkdir -p "${artifact_dir}"
  gh api "repos/${GITHUB_REPOSITORY}/actions/artifacts/${artifact_id}/zip" \
    > "${artifact_dir}/artifact.zip"
  unzip -q "${artifact_dir}/artifact.zip" -d "${artifact_dir}/content"
  report="$(find "${artifact_dir}/content" -type f -name "${report_name}" -print -quit)"
  [[ -n "${report}" ]] || {
    printf '%s does not contain %s\n' "${artifact}" "${report_name}" >&2
    exit 1
  }
  jq -e '.status == "passed"' "${report}" >/dev/null

  if [[ "${artifact}" == "release-chaos-report" ]]; then
    jq -e '.page_alert_count == 24' "${report}" >/dev/null
    results="$(find "${artifact_dir}/content" -type f -name results.jsonl -print -quit)"
    [[ -n "${results}" ]] || {
      printf 'chaos evidence does not contain results.jsonl\n' >&2
      exit 1
    }
    jq -s -e 'length == 9 and all(.[]; .status == "pass")' "${results}" >/dev/null
  fi

  workflow_run_id="$(jq -r '.workflow_run.id' <<<"${selected}")"
  run_url="https://github.com/${GITHUB_REPOSITORY}/actions/runs/${workflow_run_id}"
  jq --arg name "${artifact}" --argjson id "${artifact_id}" \
    --arg created_at "${created_at}" --arg run_url "${run_url}" \
    --argjson age_seconds "${age_seconds}" \
    '. + [{name: $name, artifact_id: $id, created_at: $created_at, age_seconds: $age_seconds, workflow_run_url: $run_url, status: "passed"}]' \
    "${REPORT_DIR}/evidence-index.json" > "${REPORT_DIR}/evidence-index.tmp.json"
  mv "${REPORT_DIR}/evidence-index.tmp.json" "${REPORT_DIR}/evidence-index.json"
done

{
  printf '\n## Release Evidence\n\n'
  printf '| Artifact | Created | Age (seconds) | Status |\n|---|---|---:|---:|\n'
  jq -r '.[] | "| `\(.name)` | \(.created_at) | \(.age_seconds) | `\(.status)` |"' \
    "${REPORT_DIR}/evidence-index.json"
} >> "${REPORT_DIR}/summary.md"
