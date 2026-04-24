# australian-kpis

A unified API, SDK, and client for Australian economic data — aggregating ABS, RBA, APRA, Treasury, ASX, AEMO, and state government publications into a consistent SDMX-inspired data model.

## Status

**Pre-implementation.** The design is frozen in [`Spec.md`](./Spec.md). Work is tracked as [GitHub issues](https://github.com/ponderingdemocritus/australian-kpis/issues) under milestones M1–M5.

## Stack

- **API + ingestion**: Rust (axum, tokio, sqlx)
- **SDK + client**: TypeScript (Bun, Vite, React)
- **PDF extraction**: Python (FastAPI, pdfplumber, camelot)
- **DB**: Postgres 16 + TimescaleDB (managed via Timescale Cloud)
- **Queue**: apalis (Postgres backend)
- **Cache**: Redis
- **Blob**: Cloudflare R2
- **Deploy**: Fly.io

See [`Spec.md`](./Spec.md) for the full architecture, data model, testing strategy, and phased rollout.

## Getting started

This workspace targets Rust 2024 on the pinned Rust 1.85 toolchain in
[`rust-toolchain.toml`](./rust-toolchain.toml). Earlier compilers such as 1.83
are not supported.

Once [scaffolding is in place](https://github.com/ponderingdemocritus/australian-kpis/issues?q=is%3Aissue+label%3Atype%3Ascaffold):

```bash
# Start infra
docker compose -f infra/compose/docker-compose.yml up -d

# Run API
cargo run --bin au-kpis-api

# Run ingestion worker
cargo run --bin au-kpis-ingestion

# Run reference client
pnpm --filter web dev
```

## Contributing

- [`CONTRIBUTING.md`](./CONTRIBUTING.md) — human-oriented workflow guide
- [`AGENTS.md`](./AGENTS.md) — operating manual for AI coding agents (PR rules, CI gates, pre-flight checks)

Pre-commit hooks via `lefthook`.

## License

Apache-2.0. Data licenses retained per-source — see `Spec.md § Data licensing and attribution`.
