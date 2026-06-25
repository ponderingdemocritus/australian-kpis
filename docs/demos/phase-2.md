# Phase 2 Demo

This clean-clone walkthrough proves the Phase 2 local loop: setup, ingest ABS
CPI, query the API and SDK, and chart in the Explorer in under 15 minutes.

## Setup the local stack

Start the compose stack and apply migrations:

```bash
docker compose -f infra/compose/docker-compose.yml up -d --build --wait

DATABASE_URL=postgres://au_kpis:au_kpis@127.0.0.1:54320/au_kpis \
  sqlx migrate run --source infra/migrations
```

Seed the reproducible reference data from `phase-2-seed.sql` when starting from
a clean database.

## Ingest ABS CPI

Run a one-shot CPI ingestion against the local stack:

```bash
AU_KPIS_DATABASE__URL=postgres://au_kpis:au_kpis@127.0.0.1:54320/au_kpis \
AU_KPIS_CACHE__URL=redis://127.0.0.1:63790 \
  au-kpis-ingestion -- --once --source abs --dataflow cpi
```

The matching terminal recording is `phase-2.cast`.

## Query the API and SDK

Check the health endpoint, then query observations through the API or generated
SDK:

```bash
curl http://127.0.0.1:3000/v1/health
curl "http://127.0.0.1:3000/v1/observations?dataflow_id=abs.cpi"
```

## Chart in the Explorer

Open the web client, choose the ABS CPI dataflow, and chart the seeded series in
the Explorer.
