# Phase 2 Demo: ABS CPI End to End

This demo proves the Phase 2 path from setup -> ingest -> query -> chart for
the first production-shaped dataflow, `abs.cpi`.

Spec anchors:

- `Spec.md` section `Phase 2 - One end-to-end source: ABS CPI`
- `Spec.md` section `Verification plan`, subsection `Phase 2 (ABS CPI end-to-end)`

Target runtime from a clean clone is under 15 minutes on a normal development
machine with Docker, Rust, Node, pnpm, `sqlx-cli`, and `jq` already available.
One-time installation of those prerequisites is outside the timed demo.

Recorded demo: [asciinema terminal recording](./phase-2.cast). Play it with
`asciinema play docs/demos/phase-2.cast`.

## Setup the local stack

Install TypeScript dependencies, start the local services, and apply database
migrations. The seed file inserts only reference metadata; observations still
come from the live ABS CPI ingestion step.

```bash
corepack enable
pnpm install

docker compose -f infra/compose/docker-compose.yml up -d --build --wait

DATABASE_URL=postgres://au_kpis:au_kpis@127.0.0.1:54320/au_kpis \
  sqlx migrate run --source infra/migrations

docker compose -f infra/compose/docker-compose.yml exec -T postgres \
  psql -U au_kpis -d au_kpis -v ON_ERROR_STOP=1 \
  < docs/demos/phase-2-seed.sql

curl -fsS http://127.0.0.1:3000/v1/health | jq .
```

Create the local MinIO bucket used by the ingestion artifact store:

```bash
docker run --rm --network australian-kpis_default --entrypoint /bin/sh minio/mc -c \
  'mc alias set local http://minio:9000 au_kpis_minio au_kpis_minio_dev_password \
    && mc mb --ignore-existing local/au-kpis-artifacts'
```

Expected checkpoint: the seed command exits without errors, the health endpoint
returns `{"status":"ok"}`, and the bucket command either creates
`local/au-kpis-artifacts` or reports that it already exists.

## Ingest ABS CPI

Run the ABS CPI ingestion worker once against the compose database and MinIO
artifact store:

```bash
AU_KPIS_DATABASE__URL=postgres://au_kpis:au_kpis@127.0.0.1:54320/au_kpis \
AU_KPIS_OBJECT_STORE__ENDPOINT=http://127.0.0.1:9000 \
AU_KPIS_OBJECT_STORE__BUCKET=au-kpis-artifacts \
AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID=au_kpis_minio \
AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY=au_kpis_minio_dev_password \
AU_KPIS_OBJECT_STORE__REGION=us-east-1 \
AU_KPIS_OBJECT_STORE__ALLOW_HTTP=true \
AU_KPIS_TELEMETRY__LOG_FORMAT=pretty \
  cargo run --bin au-kpis-ingestion -- --once --source abs --dataflow cpi
```

Expected checkpoint: the worker exits successfully after fetching the ABS CPI
SDMX artifact, parsing observations, and loading rows into `observations` and
`observations_latest`.

## Query the API and SDK

Query the running API directly:

```bash
curl -fsS \
  'http://127.0.0.1:3000/v1/observations?dataflow=abs.cpi&dimensions[measure]=1&dimensions[index]=10001&dimensions[tsest]=10&dimensions[region]=50&dimensions[freq]=Q&limit=5' \
  | jq '{metadata, first_observation: .observations[0], count: (.observations | length)}'
```

Then query through the TypeScript SDK:

```bash
pnpm turbo run build --filter=@au-kpis/sdk --cache-dir=.turbo

node --input-type=module <<'JS'
import { createClient } from '@au-kpis/sdk'

const client = createClient({ baseUrl: 'http://127.0.0.1:3000', validate: true })
const page = await client.observations.list({
  dataflow: 'abs.cpi',
  dimensions: { measure: '1', index: '10001', tsest: '10', region: '50', freq: 'Q' },
  limit: 5,
})

console.log({
  count: page.observations.length,
  firstObservation: page.observations[0],
  attribution: page.metadata.attribution,
})
JS
```

Expected checkpoint: both commands return ABS CPI observations and attribution
metadata from the same local API.

## Chart in the Explorer

Start the React Explorer against the compose API:

```bash
NEXT_PUBLIC_AU_KPIS_API_BASE_URL=http://127.0.0.1:3000 \
  pnpm --filter @au-kpis/web dev -- --port 4174
```

Open <http://127.0.0.1:4174/>. The Explorer should render the national CPI
line chart and the state comparison chart for `abs.cpi` using observations
loaded by the ingestion run.

Bench context for the Phase 2 parser, loader, and API paths is recorded in
[`benches/baselines/issue-37.md`](../../benches/baselines/issue-37.md).

## Reset

Stop the web process with `Ctrl-C`. Keep data between runs with:

```bash
docker compose -f infra/compose/docker-compose.yml down
```

Reset all local compose data with:

```bash
docker compose -f infra/compose/docker-compose.yml down -v
```

## Troubleshooting

- If the web app cannot reach the API, keep it on port `4174` or update
  `AU_KPIS_HTTP__CORS_ALLOWED_ORIGINS` in the compose API service.
- If the API returns no observations, rerun the ingestion command and check the
  worker log for upstream ABS or MinIO errors.
- If `sqlx migrate run` reports missing extensions, recreate the compose stack
  with `down -v` and run the setup steps again.
