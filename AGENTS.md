# AGENTS.md

Operating manual for AI coding agents (and humans) contributing to **australian-kpis**.

This file complements:
- [`Spec.md`](./Spec.md) — architecture, data model, testing strategy (ground truth)
- [`CONTRIBUTING.md`](./CONTRIBUTING.md) — human-oriented workflow

If anything here conflicts with `Spec.md`, **`Spec.md` wins** — file a PR to fix this doc.

---

## 1. Orientation (read this first)

- **Project**: unified Rust API + TypeScript SDK + Python PDF sidecar for Australian economic data (ABS, RBA, APRA, Treasury, ASX, AEMO, state govts).
- **Status**: pre-implementation. Work is scoped into 65 GitHub issues across milestones **M1–M5**.
- **Ground truth**: [`Spec.md`](./Spec.md) (~1300 lines). Skim the TOC; jump to the section relevant to your task. Don't invent architecture — if the spec is silent, propose an amendment PR to `Spec.md` alongside the change.

## 2. Repo layout

See `Spec.md § Monorepo layout`. In short:

```
crates/            Rust workspace (domain, db, adapter, api-http, loader, ...)
crates/adapters/   One crate per source (ABS, RBA, APRA, ...)
crates/bins/       Binaries: au-kpis-api, au-kpis-ingestion, au-kpis-scheduler, au-kpis-cli
apps/web/          TypeScript reference client (Vite + React)
apps/pdf-extractor/  Python sidecar (FastAPI + pdfplumber + camelot)
apps/bench/        k6 load-test scripts
packages/sdk/      TypeScript SDK (@au-kpis/sdk)
packages/sdk-generated/  OpenAPI-generated client + types
tools/             Codegen + admin scripts
infra/             docker-compose, Dockerfiles, sqlx migrations
tests/             Cross-crate e2e, chaos, contract tests
```

## 3. Finding work

1. **Check the issue board first**: [milestones](https://github.com/ponderingdemocritus/australian-kpis/milestones). Pick an issue with no unmet dependencies.
2. **Respect `Depends on:` lines** in issue bodies. Do not start a blocked issue.
3. **Claim it** by assigning yourself (or linking the branch you create).
4. Each issue's **Pass requirements** checklist is the contract. Every box must be ticked before merge.

## 4. Setup (one-time)

```bash
# Rust toolchain is pinned in rust-toolchain.toml
rustup show      # installs the pinned version

# Tooling
cargo install sqlx-cli cargo-nextest cargo-deny cargo-audit cargo-llvm-cov critcmp

# TS
corepack enable && pnpm install

# Pre-commit hooks
brew install lefthook && lefthook install

# Local infra
docker compose -f infra/compose/docker-compose.yml up -d
sqlx migrate run
```

If any of these fail on first run — don't paper over it; fix the underlying setup issue and update this doc if needed.

## 5. Development loop

```
1. git switch -c <github-handle>/<issue-number>-<short-slug>
2. cargo check + cargo test (fast feedback in the affected crate)
3. Implement smallest change that satisfies the issue's Pass requirements
4. Run local pre-flight checks (see § 6)
5. Push and open a PR (see § 7)
6. Wait for CI; fix gates; get review
7. Merge via merge queue
```

### Branch naming

- `<handle>/<issue>-<slug>` (e.g., `ponderingdemocritus/27-loader-copy-upsert`)
- Lowercase, kebab-case, under 50 chars total
- One issue per branch

## 6. Pre-flight checks (before pushing)

**Run this block every time.** If any fail, fix before pushing — CI will reject them anyway.

```bash
# Rust
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo sqlx prepare --workspace        # if queries changed

# TypeScript
pnpm biome check .
pnpm turbo run typecheck test

# Supply chain (catches bad deps before CI)
cargo deny check
cargo audit
gitleaks protect --staged             # pre-commit runs this too

# OpenAPI drift (if you touched handlers or schemas)
cargo run -p au-kpis-openapi > openapi.json
oasdiff breaking openapi-main.json openapi.json    # must be empty
```

**Lefthook** runs `cargo fmt --check`, `biome format --check`, `gitleaks protect`, and a >5 MB file block on every commit. Don't disable it.

## 7. PR rules

### Required

| Rule | Detail |
|---|---|
| **One issue per PR** | Title prefixed with issue number: `fix(loader): revision chain tiebreak (#27)` |
| **Closes keyword** | Body contains `Closes #N` so the issue auto-closes on merge |
| **Pass requirements checklist** | Copy the issue's checklist into the PR body; tick what you did |
| **Test plan** | Concrete steps to verify; unit + integration |
| **Spec impact** | Confirm "No Spec changes required" OR link amendment PR |
| **Signed commits** | `git config commit.gpgsign true` — required on `main` |
| **Conventional commit title** | `type(scope): subject` where type ∈ `feat`, `fix`, `refactor`, `test`, `ci`, `docs`, `chore`, `security` |

### Size + scope

- **Target ≤400 lines changed.** Bigger → split.
- **No drive-by refactors.** Unrelated cleanup goes in its own PR.
- **No new dependencies** without a comment explaining why an existing dep doesn't work. `cargo deny` rejects GPL-incompatible licenses.

### What reviewers look for

- The diff delivers exactly the issue's pass requirements — no more, no less
- Tests added for new behaviour; property/snapshot tests where applicable
- Error paths handled with `thiserror` (libs) / `anyhow` (bins) — no `.unwrap()` in non-test code
- No lock held across `.await` (clippy catches it; don't silence the lint)
- No secrets or real production data in fixtures

### Automated review guidance

- Prioritise correctness, security, data integrity, CI regressions, and spec drift over style commentary
- Flag missing tests for behaviour changes, migrations, API changes, or workflow logic as at least P1
- Flag missing `Spec.md` updates when a PR changes the CI contract or other architecture-level behaviour
- Ignore formatting-only nits unless they block understanding of the diff

## 8. CI rules (what must pass)

Full details in `Spec.md § CI/CD pipeline`. Summary:

### PR flow (blocking — all 14 gates)

| # | Gate | Tool | Fail condition |
|---|---|---|---|
| 1 | Compile | `cargo check --workspace` | error |
| 2 | Lint | `cargo clippy -D warnings` + `biome check` | any warning |
| 3 | Format | `cargo fmt --check` + `biome format --check` | diff |
| 4 | Tests | `nextest` + `vitest` + Playwright | fail or flake (retry >0) |
| 5 | Coverage | `cargo-llvm-cov` | <80% line or <70% branch |
| 6 | Snapshots | `insta` | drift without review commit |
| 7 | OpenAPI diff | `oasdiff breaking` | any breaking change without `/v2` |
| 8 | Contract | `schemathesis` | violation |
| 9 | Supply chain | `cargo audit`, `cargo deny`, `pnpm audit` | critical CVE or banned license |
| 10 | Container | `trivy` | HIGH/CRITICAL |
| 11 | Secrets | `gitleaks` | any finding |
| 12 | Bench | `critcmp` | >5% regression |
| 13 | Smoke | `k6 smoke.js` | threshold miss |
| 14 | Accessibility | `axe-core` | any WCAG AA violation |

**Target wall time: <5 min on warm cache.** If your PR slows CI materially, surface it in the PR description.

### Merge queue (blocking, runs once per batch)

- Playwright full suite
- k6 `smoke.js` against staging
- `schemathesis` deep fuzz
- Bench regression (no longer advisory)

Merge queue **ejects the entire batch** on failure. Respect the queue — don't force-push to bypass.

### Scheduled (non-blocking, but watch for alerts)

- k6 `sustained.js` + `burst.js` nightly in staging
- `cargo fuzz run` 30 min/target nightly
- `cargo mutants` weekly
- Renovate weekly dependency PRs

## 9. Commit style

```
type(scope): short imperative subject (≤72 chars)

Optional body explaining *why*. Wrap at 72 cols.

Optional footer:
Closes #N
```

- Types: `feat`, `fix`, `refactor`, `test`, `ci`, `docs`, `chore`, `security`
- Scope: crate name or directory (`loader`, `adapter-abs`, `sdk`, `infra`)
- Never attribute to Claude / Copilot / any tool — commits are authored by the human or machine identity that runs them. See `.claude/rules/git-commits.md` if you use the repo's `/commit` skill.

## 10. Code style

### Rust

- `rustfmt` defaults + `clippy::pedantic` aspirations (not enforced today; `clippy::all -D warnings` is)
- `#[tracing::instrument]` on public async functions
- `async-trait` only for dyn-dispatched traits (adapters); otherwise native async-fn
- Errors: `thiserror` in libraries, `anyhow` in binaries. No `Box<dyn Error>`.
- **Never** hold a lock across `.await`. `clippy::await_holding_lock` is denied.
- Prefer `sqlx::query!` (compile-checked) over `query()` (runtime-checked).
- Streams over `Vec` for anything over ~1000 items.

### TypeScript

- Biome defaults
- `type` > `interface` for unions; `interface` for extensible object shapes
- No default exports from barrel files
- No `any`; `unknown` if truly unknown, then narrow
- Runtime validation only at process boundaries (SDK ↔ server); trust internal TS types

### Python (PDF sidecar)

- `ruff` + `mypy --strict`
- `httpx` over `requests`
- Explicit timeouts on every network call

### Migrations

- One SQL file per change, numbered (`0007_add_series_updated_at.sql`)
- **Reversible where possible** — `.up.sql` + `.down.sql` pair
- Test: `sqlx migrate run` → `sqlx migrate revert` → `sqlx migrate run` leaves schema identical
- No destructive changes without a deprecation window and explicit `Spec.md` note

## 11. Testing expectations (see `Spec.md § Testing strategy`)

Every PR must add/update tests at the appropriate layer:

| Change type | Required test |
|---|---|
| New pure fn | Unit test |
| New parser | Snapshot test + property test |
| New API endpoint | Integration test via testcontainers + schemathesis coverage |
| New SQL query | Integration test hitting real PG+Timescale |
| New SDK method | SDK integration test against compose stack |
| New adapter | Golden-file `insta` snapshot + `wiremock`-based integration |
| UI change | Playwright E2E + axe-core pass |
| Performance-sensitive path | Criterion bench + committed baseline |

**Coverage floor**: 80% line / 70% branch across shipped crates. PR bot comments with the diff.

**Zero-flake policy**: any CI retry auto-files a `flaky`-labelled issue. Fix within 48h or delete the test.

## 12. Agent-specific pitfalls (avoid these)

1. **Don't regenerate `openapi.json` without proposing the handler change.** The order is: change handler → regen → commit both. Drift between them fails CI.
2. **Don't mock what you can testcontainer.** We have working testcontainers for PG+Timescale, Redis, MinIO. Use them.
3. **Don't add comments that narrate what the code does.** Only write a comment if the *why* is non-obvious (workaround, hidden invariant, surprising behaviour).
4. **Don't introduce backward-compat shims.** We're pre-`v1`. Just change the code.
5. **Don't batch unrelated changes.** The loader upsert fix and the API pagination bug are two PRs.
6. **Don't claim completion without ticking every pass-requirement box.** If you can't finish a box, leave it unticked, add a note explaining why, and leave the PR in draft.
7. **Don't silence clippy/biome lints** with `#[allow(...)]` without a comment naming the reason. CI flags bare allows in review.
8. **Don't bypass pre-commit hooks** with `--no-verify`. If a hook fails, fix the underlying issue.
9. **Don't commit secrets, `.env`, production fixtures, or large binaries.** Fixtures >5 MB go to R2 with a reference in-repo.
10. **When unsure about architecture**, open a draft PR with a `Spec.md` amendment — don't guess.

## 13. When stuck

1. **Re-read the relevant `Spec.md` section** — it's usually already answered there
2. **Search closed issues + merged PRs** for similar precedent
3. **Open a draft PR** with your best attempt and `@`-mention CODEOWNERS
4. **Amend `Spec.md`** if the spec genuinely doesn't cover your case

## 14. Release process (M5+)

- **API**: additive changes auto-deploy on merge to `main`. Breaking changes require a new `/v2` path + 6-month `/v1` deprecation window with `Deprecation` + `Sunset` headers (RFC 8594).
- **SDK**: semver via `@changesets/cli`. Every PR touching `packages/sdk` requires a changeset file. Release workflow publishes on merge.
- **Monorepo tags**: `api-vX.Y.Z`, `sdk-vX.Y.Z`, `ingestion-vX.Y.Z` — tagged independently.

Rollback on prod smoke failure is **automatic**. If you need a manual rollback: one-click Slack action or `fly releases rollback <app>`.

---

**TL;DR for an agent starting fresh:**

1. Read the relevant `Spec.md` section for your task
2. Pick an unblocked GitHub issue; respect dependencies
3. Branch, implement the smallest change that ticks every pass-requirement box
4. Run the § 6 pre-flight block locally
5. PR with `Closes #N`, checklist ticked, test plan
6. Wait for 14 CI gates to go green; fix what's red
7. Merge via queue
