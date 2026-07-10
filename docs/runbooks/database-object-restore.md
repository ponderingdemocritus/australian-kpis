# Database And Object Restore

Production objectives are Timescale RPO at most five minutes and RTO at most 30
minutes. R2 artifacts are immutable and must reconcile by count, bytes, and
SHA-256 after restoration.

## Timescale PITR

1. Declare the incident, stop schedule admission, and record the UTC failure
   time. Keep API reads unavailable or explicitly degraded; do not expose a
   partially restored generation.
2. Choose a target no more than five minutes before failure. Record request and
   completion timestamps from the Timescale provider, not workstation time.
3. Restore into a new isolated service. Do not overwrite the damaged service.
4. Apply no migrations until `_sqlx_migrations`, Timescale version, catalog,
   generations, observations, snapshots, and artifact metadata are inspected.
5. Point a private verification deployment at the restored database and run
   health, API/APS smoke, data quality, source readiness, and generation
   atomicity checks.

## R2 Reconciliation

Use read/list/head/get credentials without delete permission:

```bash
AU_KPIS_DATABASE_URL=... \
AU_KPIS_R2_ENDPOINT=... \
AU_KPIS_R2_BUCKET=... \
tools/release/reconcile-r2.sh
```

Any count, byte, storage-key digest, or downloaded SHA-256 mismatch blocks
traffic. Recover missing objects from the retained immutable source bucket or
audited upstream replay; never manufacture artifact bytes or change an artifact
ID.

## Certification

After the provider restore, dispatch `restore-certification.yml` with the four
UTC timestamps and the restored database secret. It runs
`tools/release/verify-restore.sh`, enforces RPO/RTO, captures database state,
performs full R2 reconciliation, and retains `release-restore-report`.

Cut over secrets only after all checks pass. Re-enable schedules gradually and
verify one full active-dataflow cycle, queue drain, APS publication, synthetics,
and alerts. Preserve the failed service read-only until incident sign-off; never
reverse-copy partial state.
