# AGENTS.md

Concise operating guide for AI coding agents working on **australian-kpis**.

## Authority

- `Spec.md` is the source of truth for architecture, data model, APIs, CI, and
  testing. If this file conflicts with `Spec.md`, follow `Spec.md` and fix this
  file.
- `CONTRIBUTING.md` covers the human workflow. This file keeps the agent
  execution rules short.
- If the spec is silent on architecture or behavior, do not guess. Add or
  propose a `Spec.md` amendment with the implementation.

## Project Map

- `crates/` - Rust workspace: domain, db, queue, loader, API, ingestion, config.
- `crates/adapters/` - source adapters for ABS, RBA, APRA, ASX, AEMO, Treasury,
  state sources, and related inputs.
- `crates/bins/` - runnable Rust binaries: API, ingestion, scheduler, CLI.
- `apps/web/` - Next.js reference client.
- `apps/pdf-extractor/` - Python FastAPI PDF sidecar.
- `apps/bench/` - k6 load tests.
- `packages/sdk/` and `packages/sdk-generated/` - TypeScript SDK packages.
- `infra/` - compose stack, Dockerfiles, migrations, deploy config.
- `tests/`, `fuzz/`, `benches/` - cross-cutting validation.

## Before Coding

1. Read the relevant `Spec.md` anchors and the issue pass requirements.
2. Respect `Depends on:` in GitHub issues. Do not start blocked work.
3. Keep one issue per branch/PR unless the user explicitly approves grouping.
4. Prefer the smallest change that satisfies the contract. No drive-by refactors.
5. Preserve existing user changes. Never revert unrelated dirty files.

Branch convention:

```bash
git switch -c ponderingdemocritus/<issue-number>-<short-slug>
```

Use lowercase kebab-case and keep the branch short.

## Data Source Scope

- A source or dataflow is in scope only when it is named in `Spec.md`, the
  relevant issue pass requirements, or a versioned scorecard config such as
  `crates/au-kpis-scorecard/config/aps.v1.toml`.
- Do not add, infer, or silently expand source coverage from available web data,
  payload shape, mirrored filenames, or adjacent datasets. If the scope is not
  explicit, add or propose a `Spec.md` or config amendment before implementation.
- Every scoped dataflow must have explicit `source_id`/`dataflow_id` contracts,
  provenance, source URL, license, attribution, cadence, and validation rules.
- For APS and other derived scorecards, unresolved or unavailable inputs must be
  represented as coverage gaps, manual inputs, visible-unscored inputs, or
  expected-missing entries according to config. Do not hide missing sources or
  let them affect scored outputs unless the config marks them as scored and
  reviewed.

## Local Setup

```bash
rustup show
corepack enable
pnpm install

docker compose -f infra/compose/docker-compose.yml up -d --build --wait

DATABASE_URL=postgres://au_kpis:au_kpis@127.0.0.1:54320/au_kpis \
  sqlx migrate run --source infra/migrations
```

Run the API locally against compose services:

```bash
AU_KPIS_DATABASE__URL=postgres://au_kpis:au_kpis@127.0.0.1:54320/au_kpis \
AU_KPIS_CACHE__URL=redis://127.0.0.1:63790 \
  cargo run --bin au-kpis-api
```

Run the web app:

```bash
NEXT_PUBLIC_AU_KPIS_API_BASE_URL=http://127.0.0.1:3000 \
  pnpm --filter @au-kpis/web exec next dev --hostname 127.0.0.1 --port 4173
```

For ingestion one-shot runs, set durable object-store env vars for MinIO/R2.
One-shot mode intentionally requires a real object store.

## Verification

Run focused checks while developing, then the relevant pre-flight subset before
handoff. Do not claim completion without running the checks you cite.

Rust:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo sqlx prepare --workspace        # when SQL queries changed
```

TypeScript:

```bash
pnpm run lint
pnpm turbo run typecheck test
```

Supply chain and secrets:

```bash
cargo deny check
cargo audit
gitleaks protect --staged
```

OpenAPI, when handlers or schemas changed:

```bash
cargo run -p au-kpis-openapi > openapi.json
oasdiff breaking openapi-main.json openapi.json
```

If local Docker, network, registry, or platform limits block a check, document
the exact blocked command and why.

## Test Expectations

- Pure logic: unit tests.
- Parsers/adapters: golden fixtures, `insta` snapshots, malformed-input tests,
  and property tests where shape variation matters.
- SQL/loader/API behavior: integration tests against real Postgres/Timescale
  via compose or testcontainers.
- API endpoints: handler/integration coverage plus OpenAPI drift checks.
- SDK changes: generated package builds and runtime checks.
- UI changes: Playwright and accessibility checks.
- Performance-sensitive paths: criterion/k6 coverage and baseline updates.

Coverage target is 80% line and 70% branch across shipped crates. CI has a
zero-flake policy.

## Coding Rules

Rust:

- Use `thiserror` in libraries and `anyhow` in binaries.
- No `.unwrap()` in non-test code.
- Do not hold locks across `.await`.
- Prefer compile-checked `sqlx::query!` where practical.
- Use streams for large data; avoid collecting hot-path datasets into `Vec`.

TypeScript:

- Biome is authoritative.
- Avoid `any`; use `unknown` and narrow at boundaries.
- Runtime validation belongs at API/SDK/process boundaries.

Python:

- Use `ruff` and strict `mypy`.
- Prefer `httpx`; every network call needs an explicit timeout.

Migrations:

- Add numbered `.up.sql` and `.down.sql` files where possible.
- Test migrate, revert, migrate for schema-sensitive changes.
- No destructive migration without a deprecation plan and `Spec.md` note.

Comments:

- Add comments only for non-obvious why/invariants/workarounds.

## Data And Ingestion Invariants

- Preserve provenance through every stage: discovery job id, trace context,
  source id, dataflow id, artifact id, storage key, and parsed bytes.
- Parsers must reject ambiguous source/dataflow provenance. Do not infer a
  dataflow only from payload shape or mirrored filenames.
- If observations carry `source_artifact_id`, validate the artifact id, storage
  key, and bytes are consistent before emitting rows.
- Keep streaming fixes on the hot path; do not add full-artifact scans unless
  the issue explicitly accepts that cost.
- Cancellation stops admitting new work but must drain already produced
  artifacts, observations, and audit records until the shutdown grace expires.
- Make large/performance fixtures production-shaped with valid content-addressed
  artifact ids and storage keys.
- Preserve idempotency, ordering, backpressure, auditability, and retry semantics
  in loader, queue, parser, ingestion, and API changes.

## Generated Artifacts

- Handler/schema changes must regenerate `openapi.json` in the same change.
- SDK output changes must update generated files and run SDK checks.
- Snapshot changes must be intentional and reviewed.
- Do not commit secrets, `.env`, production data, or files over 5 MB. Large
  fixtures belong in object storage with an in-repo reference.

## PR Contract

- One issue per PR.
- Title: conventional commit style, preferably including the issue number.
- Body: `Closes #N`, pass-requirements checklist, test plan, and spec impact.
- Target small diffs, roughly <=400 changed lines. Split larger work.
- No new dependency without explaining why existing dependencies are inadequate.
- Signed commits are required on `main`.

CI gates include compile, lint, format, tests, coverage, snapshots, OpenAPI
breaking diff, contract fuzzing, supply-chain scans, container scan, secrets,
bench regression, k6 smoke, and accessibility. See `Spec.md` for details.

## When Stuck

1. Re-read the relevant `Spec.md` section.
2. Search existing issues, tests, and merged PRs for precedent.
3. Prefer a draft PR with a spec amendment over an architectural guess.
4. Leave incomplete pass requirements unticked and explain the blocker.
