#!/usr/bin/env bash
set -euo pipefail

: "${AU_KPIS_DATABASE_URL:?AU_KPIS_DATABASE_URL is required}"
: "${AU_KPIS_R2_ENDPOINT:?AU_KPIS_R2_ENDPOINT is required}"
: "${AU_KPIS_R2_BUCKET:?AU_KPIS_R2_BUCKET is required}"

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT
manifest="$work_dir/database-artifacts.tsv"

sha256_stream() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

psql "$AU_KPIS_DATABASE_URL" --no-psqlrc --tuples-only --no-align --field-separator=$'\t' \
  --command="SELECT encode(id, 'hex'), storage_key, size_bytes FROM artifacts ORDER BY storage_key" \
  >"$manifest"

db_count="$(wc -l <"$manifest" | tr -d ' ')"
db_bytes="$(awk -F $'\t' '{ total += $3 } END { printf "%.0f", total }' "$manifest")"
read -r r2_count r2_bytes < <(
  aws --endpoint-url "$AU_KPIS_R2_ENDPOINT" s3 ls \
    "s3://${AU_KPIS_R2_BUCKET}/artifacts/" --recursive \
    | awk '{ count += 1; bytes += $3 } END { printf "%d %.0f\n", count, bytes }'
)

if [[ "$db_count" != "$r2_count" || "$db_bytes" != "$r2_bytes" ]]; then
  printf 'artifact reconciliation mismatch: db=%s/%s r2=%s/%s\n' \
    "$db_count" "$db_bytes" "$r2_count" "$r2_bytes" >&2
  exit 1
fi

while IFS=$'\t' read -r digest key size; do
  [[ "$key" == *"$digest" ]] || {
    printf 'storage key does not end with artifact digest: %s\n' "$key" >&2
    exit 1
  }
  remote_size="$(aws --endpoint-url "$AU_KPIS_R2_ENDPOINT" s3api head-object \
    --bucket "$AU_KPIS_R2_BUCKET" --key "$key" --query ContentLength --output text)"
  [[ "$remote_size" == "$size" ]] || {
    printf 'artifact size mismatch for %s: db=%s r2=%s\n' "$key" "$size" "$remote_size" >&2
    exit 1
  }
  remote_digest="$(aws --endpoint-url "$AU_KPIS_R2_ENDPOINT" s3 cp \
    "s3://${AU_KPIS_R2_BUCKET}/${key}" - --no-progress | sha256_stream)"
  [[ "$remote_digest" == "$digest" ]] || {
    printf 'artifact digest mismatch for %s\n' "$key" >&2
    exit 1
  }
done <"$manifest"

printf 'reconciled %s immutable artifacts (%s bytes)\n' "$db_count" "$db_bytes"
