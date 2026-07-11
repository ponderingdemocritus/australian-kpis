# australian-kpis

A unified API, SDK, and client for Australian economic data — aggregating ABS, RBA, APRA, Treasury, ASX, AEMO, and state government publications into a consistent SDMX-inspired data model.

## Status

**Production v1 implementation.** The repository contains the Rust workspace,
source adapters, public API, generated SDK, Next.js client, PDF sidecar, local
stack, and deployment configuration. The production-v1 release remains
gate-based; the normative launch contract is in [`Spec.md`](./Spec.md) and the
architecture decision is recorded in
[`docs/adr/0001-production-v1-architecture.md`](./docs/adr/0001-production-v1-architecture.md).

## Stack

- **API + ingestion**: Rust (axum, tokio, sqlx)
- **SDK + client**: TypeScript (Bun, Next.js, React)
- **PDF extraction**: Python (FastAPI, pdfplumber, camelot)
- **DB**: Postgres 16 + TimescaleDB (managed via Timescale Cloud)
- **Queue and orchestration**: durable Postgres schedules, jobs, and generations
- **Cache**: disposable Railway Redis with database fallback
- **Blob**: Cloudflare R2
- **Deploy**: Cloudflare edge to Railway Singapore stateless compute, Timescale
  Cloud, R2, and Grafana Cloud; immutable promotion through GitHub Actions

See [`Spec.md`](./Spec.md) for the full architecture, data model, testing strategy, and phased rollout.

## Getting started

This workspace targets Rust 2024 on the pinned Rust 1.85 toolchain in
[`rust-toolchain.toml`](./rust-toolchain.toml). Earlier compilers such as 1.83
are not supported.

Start the local development stack:

```bash
docker compose -f infra/compose/docker-compose.yml up -d --build --wait

DATABASE_URL=postgres://au_kpis:au_kpis@127.0.0.1:54320/au_kpis \
  sqlx migrate run --source infra/migrations

curl http://127.0.0.1:3000/livez
curl http://127.0.0.1:3000/readyz
curl http://127.0.0.1:3000/v1/openapi.json
```

The compose stack exposes:

| Service | URL |
|---|---|
| API | `http://127.0.0.1:3000` |
| Postgres/Timescale | `postgres://au_kpis:au_kpis@127.0.0.1:54320/au_kpis` |
| Redis | `redis://127.0.0.1:63790` |
| MinIO API | `http://127.0.0.1:9000` |
| MinIO console | `http://127.0.0.1:9001` |
| PDF extractor stub | `http://127.0.0.1:8010` |

Named Docker volumes persist Postgres, Redis, and MinIO data between runs.
Use `docker compose -f infra/compose/docker-compose.yml down -v` when you
need a clean local stack.

Run local binaries against the compose services:

```bash
AU_KPIS_DATABASE__URL=postgres://au_kpis:au_kpis@127.0.0.1:54320/au_kpis \
AU_KPIS_CACHE__URL=redis://127.0.0.1:63790 \
  cargo run --bin au-kpis-api
```

For the Phase 2 clean-clone walkthrough, see
[`docs/demos/phase-2.md`](./docs/demos/phase-2.md).

## Contributing

- [`CONTRIBUTING.md`](./CONTRIBUTING.md) — human-oriented workflow guide
- [`AGENTS.md`](./AGENTS.md) — operating manual for AI coding agents (PR rules, CI gates, pre-flight checks)

Pre-commit hooks via `lefthook`.

## License

Apache-2.0. Data licenses retained per-source — see `Spec.md § Data licensing and attribution`.
