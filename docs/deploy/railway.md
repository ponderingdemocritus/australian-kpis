# Production deployment

Production uses Railway only for stateless compute and disposable Redis. Durable
state is external: Timescale Cloud in `ap-southeast-1`, separate Cloudflare R2
buckets with the `apac` hint, and Grafana Cloud. GitHub Actions is the only
deployment authority.

## Bootstrap

1. Create Timescale Cloud staging and production services with PostgreSQL 16,
   TimescaleDB, HA, PITR, at least 4 vCPU, 16 GiB RAM, 500 GiB storage, and 100
   connections.
2. Run `infra/postgres/bootstrap-roles.sql` as the bootstrap administrator. Pass
   `database_name`, `ddl_password`, and `runtime_password` as `psql` variables.
3. Create `au-kpis-staging` and `au-kpis-production` R2 buckets with the `apac`
   hint. Apply `infra/r2/lifecycle.json` and issue separate runtime credentials.
4. Create the Grafana Cloud stack and an OTLP access policy for logs, metrics,
   traces, dashboards, alerts, and one-minute synthetic checks.
5. Link the Railway project, then preview and apply the source-less service graph:

   ```bash
   pnpm install
   railway login
   railway link
   pnpm railway:plan
   pnpm railway:apply
   ```

6. Set service variables listed below in both Railway environments. `preserve()`
   in `.railway/railway.ts` prevents IaC from reading or replacing their values.
7. Configure GitHub `staging` and `production` environments, required reviewers
   for production, and the secrets consumed by `.github/workflows/deploy.yml`.

Do not connect code services to GitHub in Railway. The IaC graph creates them
without a source. `deploy.yml` connects signed immutable GHCR digests after all
release gates pass, so repository auto-deploy remains disabled.

## Topology

All Railway services run in `asia-southeast1-eqsg3a` without application sleep.

| Service | Replicas | CPU / memory | Health |
| --- | ---: | --- | --- |
| API | 2 | 2 vCPU / 2 GiB | `/readyz` |
| Web/BFF | 2 | 1 vCPU / 1 GiB | `/` |
| PDF extractor | 2 | 2 vCPU / 4 GiB | `/health` |
| Ingestion | 2 | 2 vCPU / 4 GiB | `/metrics` |
| Scheduler | 2 active/passive | 0.5 vCPU / 512 MiB | `/metrics` |
| Webhook worker | 2 | 1 vCPU / 1 GiB | process/lease canary |
| OTEL collector | 2 | 1 vCPU / 1 GiB | `/` on port 13133 |
| Redis | 1 | Railway managed | disposable only |

The API and web domains are public only through Cloudflare. Railway origin URLs
remain technically reachable but reject requests without trusted-origin HMAC
headers. All worker services use private networking only.

## Variables

Set `AU_KPIS_DATABASE__URL` on API, ingestion, scheduler, and webhook worker to
the environment's **runtime** Timescale URL. It must not be the DDL URL.

Set these on API:

```text
AU_KPIS_CACHE__URL=${{Redis.REDIS_URL}}
AU_KPIS_ORIGIN_AUTH_REQUIRED=true
AU_KPIS_CLOUDFLARE_ORIGIN_ID=<cloudflare-origin-id>
AU_KPIS_CLOUDFLARE_ORIGIN_SECRET=<32+-byte secret>
AU_KPIS_BFF_ORIGIN_ID=<bff-origin-id>
AU_KPIS_BFF_ORIGIN_SECRET=<different 32+-byte secret>
AU_KPIS_CURSOR_SIGNING_KEY=<32+-byte active key>
AU_KPIS_CURSOR_PRIOR_SIGNING_KEY=<prior key during rotation>
AU_KPIS_CURSOR_PRIOR_VALID_UNTIL=<RFC3339, at most 24 hours>
AU_KPIS_WEBHOOK_ENCRYPTION_KEY=<base64url 32-byte key>
AU_KPIS_WEBHOOK_ENCRYPTION_KEY_VERSION=<positive integer>
AU_KPIS_HTTP__CORS_ALLOWED_ORIGINS=["https://<web-domain>"]
AU_KPIS_METRICS_BEARER_TOKEN=<different random 32+-byte token>
```

Set the matching BFF origin ID/secret on web. Set the matching webhook
encryption key/version on webhook worker. Rotation deploys the new encryption
key and retains the prior key/version until all persisted secrets have been
rewrapped.

Set R2 endpoint, bucket, region, access key, and secret on ingestion and PDF
extractor. Runtime access covers only required prefixes and excludes object
delete. Ingestion sets `AU_KPIS_OBJECT_STORE__DELETE_ENABLED=false`; the staging
prefix lifecycle removes transient copies. Set Grafana Cloud OTLP endpoint,
instance ID, API key, and the matching metrics bearer token on the OTEL collector.

Provision dashboards, alerts, and 60-second public checks from
`infra/observability/grafana-cloud`. Configure the protected GitHub environments
with `GRAFANA_CLOUD_PROMETHEUS_QUERY_URL`, `GRAFANA_CLOUD_INSTANCE_ID`,
`GRAFANA_CLOUD_API_KEY`, and `ROUTE_P95_BASELINES` for automatic rollback
monitoring.

## Migrations

Runtime processes never migrate. `au-kpis-migrate` is the only production
migration entrypoint and receives the DDL connection URL from the protected
GitHub environment. The URL sets `role=au_kpis_owner`, for example through the
PostgreSQL `options` query parameter, after authenticating as `au_kpis_ddl`.

The deploy workflow applies expand migrations to staging, promotes the exact
same image digests, runs smoke, monitors the five-minute rollback windows, and
reconciles object state. It then waits for protected production approval,
applies the same migration, and deploys the same digests with overlap. A failed
smoke, rollback monitor, or reconciliation uses Railway's deployment rollback
API with the pre-deploy manifest to restore every previous successful image and
variable set; additive schema is retained.

## External gates

Before production approval, attach provider evidence for Timescale HA/PITR and
restore timing, R2 count/byte/hash reconciliation, Grafana alert fire/clear
drills, Cloudflare WAF/origin configuration, and the seven-day two-replica soak.
These are release gates and cannot be satisfied by repository configuration
alone.
