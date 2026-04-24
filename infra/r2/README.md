# R2 bucket lifecycle

`lifecycle.json` is the canonical source for the bucket lifecycle rules
that the ingestion pipeline depends on.

## Rules

| ID | Prefix | Effect |
|---|---|---|
| `expire-artifacts-staging` | `artifacts-staging/` | Objects expire after 7 days; incomplete multipart uploads aborted after 1 day. |

The staging rule backstops `au-kpis-storage`'s streaming write path
(`put_artifact_stream`): uploads land in `artifacts-staging/<uuid>` and
are server-side copied to the canonical `artifacts/<sha256>` key once
the hash is known. The crate retries the staging delete on the happy
path, but the lifecycle rule catches the rare case where all retries
fail, bounding the worst-case storage leak at 7 days.

## Applying

```bash
# R2 (wrangler >= 3)
wrangler r2 bucket lifecycle put au-kpis-prod --file infra/r2/lifecycle.json

# S3-compatible (AWS CLI)
aws s3api put-bucket-lifecycle-configuration \
  --bucket au-kpis-prod \
  --lifecycle-configuration file://infra/r2/lifecycle.json
```

Re-apply after any change to this file. CI does not yet enforce drift
detection (tracked for a later issue).
