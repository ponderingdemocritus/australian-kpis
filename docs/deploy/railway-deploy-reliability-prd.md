# PRD: Railway Deploy Reliability

- **Status:** Draft
- **Date:** 2026-07-03
- **Owner:** ponderingdemocritus
- **Related:** PR #219 (`fix: remove Railway Docker cache mounts`), `docs/deploy/railway.md`, `.context/railway-docs-research.md`

## 1. Problem

Railway deployments of the au-kpis services keep failing even though every
container build passes in GitHub Actions CI. PR #219 fixed the immediate
blocker (Railway Metal rejects BuildKit cache mounts with generic IDs), but a
review of the branch found four remaining defects that either break deploys
today or will break them on routine future changes:

1. The web service's API base URL is malformed at runtime because it
   references a Railway variable (`${{api.PORT}}`) that is never defined.
2. The web Dockerfile hard-codes the pnpm workspace manifest list, so adding
   any new workspace package breaks every subsequent web build with
   `ERR_PNPM_OUTDATED_LOCKFILE`.
3. Railway watch patterns are out of sync with the web image's build inputs
   (`apps/docs/package.json` is a build input but not watched), so Railway can
   serve images built against stale workspace metadata.
4. Cache-cold Rust builds on Railway compile `cargo-chef` from source and
   download a duplicate Rust toolchain twice per image, making builds slow
   enough to risk timeouts — and Railway's layer cache is per-service and
   best-effort, so cold builds are common.

CI cannot catch any of these: it builds with a GHA layer cache, never
exercises Railway variable resolution, and never runs the deployed containers
against Railway private networking.

## 2. Goals

- A fresh Railway project created from this repo (via `.railway/railway.ts`)
  builds and deploys all five services to a healthy state with no manual
  Dockerfile or variable edits.
- Web → API requests over Railway private networking succeed.
- Adding a new pnpm workspace package does not break the web image build.
- Any change to a web-image build input triggers a web rebuild on Railway.
- A cache-cold Rust service build on Railway completes without compiling
  build tooling from source or downloading redundant toolchains.

## 3. Non-goals

- Restoring BuildKit cache mounts (Railway Metal requires
  `s/<service-id>-` prefixed IDs, which would hard-code project-specific
  values; PR #219 deliberately removed them and this PRD keeps that decision).
- Migrating services between Railway IaC (`.railway/railway.ts`) and per-
  service Config-as-Code (`infra/railway/*.toml`). Per the research note, the
  TypeScript IaC file is the source of truth and the TOML files are manual
  fallbacks; both must simply stay in sync where they overlap.
- Changing the CI container-security matrix (already parallelized and green).
- Runtime application changes beyond Railway environment variables.

## 4. Current state (verified 2026-07-03)

- Branch `ponderingdemocritus/railway-remove-cache-mounts`, PR #219, all 24
  CI checks green.
- No `--mount=type=cache` remains in `infra/docker/*.Dockerfile`.
- `pnpm-lock.yaml` importers: `.`, `.railway`, `apps/docs`, `apps/web`,
  `packages/sdk`, `packages/sdk-generated` — exactly the set copied by
  `infra/docker/au-kpis-web.Dockerfile` today, so the web image builds *today*.
- The API binary defaults to `0.0.0.0:3000` and only honours `PORT` when the
  env var is set (`crates/au-kpis-config/src/lib.rs:257`).
- Railway builds the final Dockerfile stage; all Dockerfiles end with the
  `runtime` stage, so Railway and CI (`target: runtime`) build the same thing.
- Next.js needs no build-time env: `AU_KPIS_API_BASE_URL` is read at runtime
  by the proxy route (`apps/web/src/app/api/au-kpis/[...path]/route.ts`), and
  `NEXT_PUBLIC_AU_KPIS_API_BASE_URL` has a safe default.

## 5. Requirements

### R1 — Web must reach the API over Railway private networking (P0)

**Defect.** In `.railway/railway.ts` the web service sets
`AU_KPIS_API_BASE_URL: 'http://${{api.RAILWAY_PRIVATE_DOMAIN}}:${{api.PORT}}'`,
but the api service defines no `PORT` variable. Railway resolves the missing
reference to an empty string, producing `http://api.railway.internal:`. Every
request through the web proxy route fails, and if the web healthcheck path
(`/`) renders API-backed content, deploys fail their healthcheck repeatedly.

**Requirement.**
- The api service must define `PORT` explicitly (`PORT: '3000'`, matching the
  binary's default bind and the Dockerfile `EXPOSE 3000`).
- `${{api.PORT}}` must resolve to a non-empty value in the web service's
  environment.
- Audit the other cross-service references for the same pattern
  (`${{pdf-extractor.*}}` in ingestion uses a hard-coded `:8000`, which is
  consistent with pdf-extractor's `PORT: '8000'` — keep it that way or make
  it a reference; either is fine so long as it resolves).

**Acceptance.**
- `railway config plan` (or a fresh project provision) shows the web service
  with a fully resolved `AU_KPIS_API_BASE_URL` ending in `:3000`.
- After deploy, a request through the web app's `/api/au-kpis/...` proxy
  returns an API response, not a connection error.

### R2 — Web image build must survive workspace growth (P0)

**Defect.** `infra/docker/au-kpis-web.Dockerfile` copies six enumerated
`package.json` files and runs `pnpm install --frozen-lockfile`. pnpm 9's
frozen-lockfile check requires the on-disk importer set to match the lockfile
exactly. The first new package under `apps/*` or `packages/*` adds a lockfile
importer the Dockerfile doesn't copy, and every web build fails with
`ERR_PNPM_OUTDATED_LOCKFILE` until someone edits the Dockerfile.

**Requirement.** Replace manifest enumeration with the lockfile-only
`pnpm fetch` pattern:

```dockerfile
COPY .npmrc pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm fetch --filter @au-kpis/web...
COPY . .
RUN pnpm install --offline --frozen-lockfile --filter @au-kpis/web... \
    && pnpm --filter @au-kpis/web... build
```

- The dependency-download layer must depend only on `.npmrc`,
  `pnpm-lock.yaml`, and `pnpm-workspace.yaml` (verify whether
  `pnpm fetch --filter` needs the workspace manifests; if it does, drop the
  `--filter` from `fetch` and keep it on `install` — fetching the full store
  is still correct and cache-stable).
- No individual workspace `package.json` may appear in a `COPY` instruction.

**Acceptance.**
- `docker buildx build -f infra/docker/au-kpis-web.Dockerfile --target runtime .`
  succeeds from a clean context.
- Regression test: add a scratch workspace package (`apps/tmp-pkg`) with an
  updated lockfile in a throwaway branch; the web image still builds without
  touching the Dockerfile.
- Rebuilding after a source-only change (no lockfile change) reuses the
  dependency layer (observable as a cache hit on the `pnpm fetch` layer).

### R3 — Railway watch patterns must cover every web build input (P1)

**Defect.** The web image's install layer inputs include
`apps/docs/package.json`, but neither `.railway/railway.ts` nor
`infra/railway/web.toml` watches it. A docs manifest change that doesn't touch
the lockfile silently skips the web rebuild; the next unrelated deploy
inherits a stale or broken install layer. (Flagged independently by the Codex
review on PR #219.)

**Requirement.**
- The watch pattern set for the web service must be a superset of the files
  the web Dockerfile reads before its final `COPY . .`.
- Apply identically to `.railway/railway.ts` and `infra/railway/web.toml`
  (they are documented as mirror configurations).
- If R2 lands first, the input set shrinks to
  `/.npmrc`, `/pnpm-lock.yaml`, `/pnpm-workspace.yaml` plus the source globs —
  update the watch lists to match rather than adding
  `/apps/docs/package.json`.

**Acceptance.**
- A checklist comparison (Dockerfile `COPY`/`RUN` inputs vs. watch patterns)
  shows no build input outside the watch set, in both config files.

### R4 — Cache-cold Rust builds must not compile tooling or duplicate toolchains (P1)

**Defect.** Each Rust Dockerfile (`api`, `ingestion`, `scheduler`):
- runs `cargo install cargo-chef --locked --version 0.1.72` in the `chef`
  stage — a several-minute source compile on every cache-cold build, per
  service;
- copies `rust-toolchain.toml` (channel `"1.85"`, components rustfmt+clippy)
  into the `planner` and `builder` stages. rustup treats channel `1.85` as a
  distinct toolchain from the image's pinned `1.85.x`, so both stages download
  a full second toolchain plus components before doing any work.

On Railway, layer cache is per-service and best-effort, and the GHA cache used
in CI does not apply — so these costs recur and push builds toward timeouts.

**Requirement.**
- Base the `chef` stage on the prebuilt cargo-chef image
  (`lukemathwalker/cargo-chef`, tag pinned to cargo-chef 0.1.72 / Rust 1.85 /
  bookworm — confirm the exact published tag) instead of `cargo install`.
- Stop copying `rust-toolchain.toml` into build stages; the base image pins
  Rust 1.85 already. If toolchain pinning inside the image is deemed
  necessary, pin the exact version (`channel = "1.85.x"` matching the image)
  so rustup performs no download — but prefer removal.
- Keep `cargo chef cook` keyed only on `recipe.json` so dependency layers
  cache across source-only commits.

**Acceptance.**
- A no-cache local build log (`docker buildx build --no-cache --progress=plain`)
  for each Rust image shows no `cargo install cargo-chef` step and no
  `rustup`/toolchain download lines.
- Cache-cold build wall time per Rust image drops measurably (record
  before/after from Railway build logs).
- CI container-security matrix stays green (it builds the same Dockerfiles).

## 6. Out-of-band verification (one-time, manual)

The repo is not currently linked to a Railway project from this workspace, so
the original failure logs were never inspected. Before closing this work:

1. `railway link` the actual project and pull the most recent failed build and
   deploy logs.
2. Confirm the failure signature matches R1 (healthcheck/runtime) or R4
   (build timeout/slowness). If it matches neither, file the real signature
   as a new requirement before declaring done.

## 7. Success metrics

- Zero failed Railway deploys attributable to R1–R4 causes over the two weeks
  following rollout.
- Cache-cold Rust image build time on Railway reduced (target: ≥5 minutes
  saved per service; baseline from step 6 logs).
- No web build failures on workspace-package addition (verified by the R2
  regression test).

## 8. Rollout & risks

- **Order:** R1 (config-only, immediate deploy fix) → R2 (web Dockerfile) →
  R3 (watch patterns, aligned to R2's final input set) → R4 (Rust
  Dockerfiles). R1 and R4 are independent and can land in parallel.
- **Risk (R2):** `pnpm fetch` behaviour with `--filter` differs across pnpm
  versions; validate against the pinned `pnpm@9.12.0` before committing to
  the filtered form.
- **Risk (R4):** the prebuilt cargo-chef image tag must exist for the exact
  version pair; if no `0.1.72`/`1.85` tag is published, fall back to the
  nearest cargo-chef version whose MSRV allows Rust 1.85 and re-pin.
- **Risk (general):** `infra/railway/*.toml` and `.railway/railway.ts` drift.
  Every change here must touch both; consider a CI check that diffs the web
  watch lists as a follow-up.
