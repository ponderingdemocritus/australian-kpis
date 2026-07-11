# R2 bucket lifecycle

`lifecycle.json` is the canonical source for the bucket lifecycle rules
that the ingestion pipeline depends on.

## Rules

| ID | Prefix | Effect |
|---|---|---|
| `expire-artifacts-staging` | `artifacts-staging/` | Objects expire after 7 days; incomplete multipart uploads aborted after 1 day. |
| `archive-immutable-artifacts` | `artifacts/` | Objects transition to R2 Infrequent Access after 365 days and are never automatically deleted. |

The staging rule completes `au-kpis-storage`'s streaming write path
(`put_artifact_stream`): uploads land in `artifacts-staging/<uuid>` and
are server-side copied to the canonical `artifacts/<sha256>` key once
the hash is known. Production sets `AU_KPIS_OBJECT_STORE__DELETE_ENABLED=false`,
so the runtime never sends a delete request and lifecycle expiry removes every
staging copy within 7 days. Local and test stores may delete staging eagerly.

## Applying

```bash
# R2 (wrangler >= 3)
wrangler r2 bucket lifecycle set au-kpis-prod --file infra/r2/lifecycle.json

# S3-compatible (AWS CLI)
aws s3api put-bucket-lifecycle-configuration \
  --bucket au-kpis-prod \
  --lifecycle-configuration file://infra/r2/lifecycle.json
```

Create `au-kpis-staging` and `au-kpis-production` separately with the `apac`
location hint. Location hints are best-effort and immutable after initial bucket
creation. Runtime credentials are separate per environment and may put, get,
head, and list the application prefixes; they must not have bucket-management
or object-delete authority. Lifecycle management uses a distinct operator token.

Re-apply after any change to this file and compare `wrangler r2 bucket lifecycle
list <bucket>` with the checked-in JSON during release certification.
