# Railway deployment

This project deploys to Railway as a multi-service project. Keep the web app,
API, workers, databases, and object storage in the same Railway project and
environment so services can use private networking.

## New project bootstrap

Use Railway Infrastructure as Code for new projects:

```bash
pnpm install
railway login
railway --version  # must be 5.2.0 or newer for IaC commands
railway init        # only when creating a new Railway project
# or: railway link  # when attaching this checkout to an existing project
pnpm railway:plan
pnpm railway:apply
```

Do not run `railway config init` in this repo unless you intend to replace the
checked-in Railway definition. Railway's IaC convention is the existing
`/.railway/railway.ts` file.

The Railway TypeScript SDK in this repo requires Node.js 22 or newer for IaC
authoring. The wider application still supports the Node.js version used by CI.

The project-level definition lives in `/.railway/railway.ts`. It creates the
TimescaleDB image service with a persistent volume, Redis, the artifact bucket,
the PDF extractor, ingestion worker, scheduler, API, and web dashboard. It also
wires service variables with Railway reference variables so new environments do
not need the per-service settings recreated by hand.

Railway-generated public domains are not included in `.railway/railway.ts`.
After `pnpm railway:apply`, generate public domains for `web` and, if external
SDK/API users need it, `api`. The web service talks to the API over Railway
private networking, so only `web` needs a public domain for the dashboard.

Use `railway config plan` before applying changes to an existing project. The
IaC API is still beta, and destructive changes require explicit confirmation.

## Config layout

Railway has two separate code-based configuration systems:

- Infrastructure as Code is project/environment scoped and lives in
  `/.railway/railway.ts`.
- Config as Code is per-service deploy config and uses `railway.toml` or
  `railway.json`.

Do not manage the same Railway service with both systems. Railway's IaC planner
blocks services that are still managed by `railway.toml` or `railway.json`.
For IaC-managed projects, keep `/.railway/railway.ts` as the active source of
truth and clear any service-level custom config file paths in the Railway
dashboard.

The `infra/railway/*.toml` files are manual fallback references only. If an
operator opts out of project-level IaC for a service, set that service's custom
config file path explicitly to the absolute repository path listed below. The
config file setting does not follow Railway's Root Directory setting.

## Services

| Service | Manual config-as-code file | Public domain | Health check |
| --- | --- | --- | --- |
| Web dashboard | `/infra/railway/web.toml` | Yes | `/` |
| API | `/infra/railway/api.toml` | Yes | `/v1/health` |
| PDF extractor | `/infra/railway/pdf-extractor.toml` | No | `/health` |
| Ingestion worker | `/infra/railway/ingestion.toml` | No | `/metrics` |
| Scheduler | `/infra/railway/scheduler.toml` | No | `/metrics` |
| TimescaleDB | Railway TimescaleDB template or custom image | No | Railway-managed |
| Redis | Railway Redis database | No | Railway-managed |
| Object storage | Railway Storage Bucket | No | Railway-managed |

The `infra/railway/*.toml` files remain useful as per-service config-as-code and
as a manual fallback. For a new project, prefer `/.railway/railway.ts` so the
whole project graph is created consistently.

For manual Config-as-Code deployment, create each code service from this GitHub
repository and set the service's custom config file path to the file listed
above. Use the repository root as the build root because the Rust workspace,
pnpm workspace, and shared packages live at the root.

The web dashboard uses `infra/docker/au-kpis-web.Dockerfile` instead of Railpack
auto-detection. This mixed Rust and Node monorepo has a root `rust-toolchain.toml`,
and Railpack can otherwise produce a Rust-only build plan for the web service
without installing pnpm.

The code-service Dockerfiles avoid Railway-specific cache mount IDs. Rust
services use `cargo-chef` layers for dependency caching, and the web image uses
a lockfile-only `pnpm fetch` layer before the source copy so fresh Railway
projects can build from source without project-specific Dockerfile edits.

## Required variables

Use Railway reference variables where possible so credentials rotate with the
backing service.

### Web dashboard

```text
AU_KPIS_API_BASE_URL=https://<api-public-domain>
```

The production web build defaults browser traffic to `/api/au-kpis`, which is a
Next.js proxy route. Set `NEXT_PUBLIC_AU_KPIS_API_BASE_URL` only when the
browser should call the API directly instead of using that proxy.

### API

```text
AU_KPIS_DATABASE__URL=${{Timescale.DATABASE_URL}}
AU_KPIS_CACHE__URL=${{Redis.REDIS_URL}}
AU_KPIS_HTTP__CORS_ALLOWED_ORIGINS=["https://<web-public-domain>"]
AU_KPIS_TELEMETRY__SERVICE_NAME=au-kpis-api
AU_KPIS_TELEMETRY__LOG_FORMAT=json
AU_KPIS_TELEMETRY__LOG_LEVEL=info
```

Do not set `AU_KPIS_HTTP__BIND` unless you need to override Railway's injected
`PORT`. Without an explicit bind, the binaries listen on `0.0.0.0:$PORT`.

### PDF extractor

```text
PORT=8000
AU_KPIS_OBJECT_STORE__ENDPOINT=${{Bucket.ENDPOINT}}
AU_KPIS_OBJECT_STORE__BUCKET=${{Bucket.BUCKET}}
AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID=${{Bucket.ACCESS_KEY_ID}}
AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY=${{Bucket.SECRET_ACCESS_KEY}}
AU_KPIS_OBJECT_STORE__REGION=${{Bucket.REGION}}
```

### Ingestion worker

```text
AU_KPIS_DATABASE__URL=${{Timescale.DATABASE_URL}}
AU_KPIS_OBJECT_STORE__ENDPOINT=${{Bucket.ENDPOINT}}
AU_KPIS_OBJECT_STORE__BUCKET=${{Bucket.BUCKET}}
AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID=${{Bucket.ACCESS_KEY_ID}}
AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY=${{Bucket.SECRET_ACCESS_KEY}}
AU_KPIS_OBJECT_STORE__REGION=${{Bucket.REGION}}
AU_KPIS_PDF_BASE_URL=http://pdf-extractor.railway.internal:8000
AU_KPIS_TELEMETRY__SERVICE_NAME=au-kpis-ingestion
AU_KPIS_TELEMETRY__LOG_FORMAT=json
AU_KPIS_TELEMETRY__LOG_LEVEL=info
```

### Scheduler

```text
AU_KPIS_DATABASE__URL=${{Timescale.DATABASE_URL}}
AU_KPIS_TELEMETRY__SERVICE_NAME=au-kpis-scheduler
AU_KPIS_TELEMETRY__LOG_FORMAT=json
AU_KPIS_TELEMETRY__LOG_LEVEL=info
```

## Notes

- The database must support TimescaleDB. The initial migration creates the
  `timescaledb` extension, hypertables, compression policies, and continuous
  aggregates; plain PostgreSQL is not sufficient.
- Railway Storage Buckets replace local MinIO. Leave
  `AU_KPIS_OBJECT_STORE__ALLOW_HTTP` unset for Railway's HTTPS bucket endpoint.
- Keep `pdf-extractor`, `ingestion`, `scheduler`, Redis, TimescaleDB, and the
  bucket private. Only the web dashboard and API need public domains.
- Deploy TimescaleDB, Redis, and the bucket before deploying code services. Then
  deploy `pdf-extractor`, `scheduler` or `ingestion` to apply migrations,
  `api`, and finally `web`.
