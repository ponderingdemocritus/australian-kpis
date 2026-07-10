# Deploy And Rollback

GitHub Actions `deploy.yml` is the only deployment authority. Railway repository
auto-deploy must remain disabled.

## Deploy

1. Confirm release checks and `release-security-report` are green.
2. Record current successful Railway deployment IDs. The deploy script writes
   `target/release-evidence/railway-rollback.json` before changing services.
3. Apply the expand migration with the DDL role. Runtime roles must not own or
   alter schema.
4. Deploy the exact signed image digests to staging, then run API/APS/BFF/
   webhook smoke, source readiness, data quality, R2 reconciliation, and the
   five-minute rollback monitor.
5. Use the protected production approval. Production receives the same digests
   and migration, followed by the same smoke and reconciliation gates.

## Automatic Rollback

Smoke failure, eligible 5xx above 1% for five minutes, route p95 above twice its
certified baseline for five minutes, no ready API replica for two minutes, or
any count/digest mismatch invokes:

```bash
tools/release/rollback-railway.sh
```

The script calls Railway's rollback API with each pre-deploy deployment ID and
waits for a healthy terminal state. For a bootstrap service with no predecessor
it removes the new deployment.

## Manual Rollback

Re-run the failed workflow rollback step with the original rollback manifest,
project token, project ID, and environment. Never retag or rebuild an old image.
Schema rollback is forward-only: retain additive migrations and ship a new
expand/fix migration.

Before managed-data writes begin, state-plane rollback may switch secrets to the
retained read-only legacy state. After writes begin, do not reverse-copy partial
state: use Timescale PITR and immutable R2 replay via the restoration runbook.

Verify exact image digests, two ready replicas per stateless/worker service,
health, eligible error rate, p95, queue age, APS snapshot identity, and R2
reconciliation. Keep old services and credentials until the seven-day soak and
restore certification pass.
