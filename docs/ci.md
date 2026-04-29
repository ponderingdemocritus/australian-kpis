# CI

`Spec.md` is the source of truth for required gates. The pull request workflow
groups those gates into parallel GitHub Actions jobs and aggregates them through
the single `CI OK` status check.

## Pull request workflow

`.github/workflows/pr.yml` currently runs:

- Rust compile with `cargo check --workspace --locked`
- TypeScript typecheck and package tests through Turborepo
- Rust, TypeScript, Markdown, and Biome lint/format checks
- Rust tests with `cargo nextest`
- Rust coverage with line and branch thresholds plus advisory Codecov upload
- Snapshot checks with `cargo insta`
- OpenAPI drift and `oasdiff breaking` checks against the base branch document
- Schemathesis contract checks against the local contract server
- Supply-chain and secret scans: `cargo deny`, `cargo audit`,
  `pnpm audit --audit-level critical`, and gitleaks over full history
- API container build plus Trivy HIGH/CRITICAL image scan
- Curl and SDK smoke checks against the local contract server
- Advisory Criterion bench comparison through `critcmp`
- Advisory Codex structured review when repository secrets allow it

Rust jobs install `sccache` and use the GitHub Actions backend. TypeScript jobs
restore the pnpm store and `.turbo` cache before running Turborepo tasks.

## Dependency and secret policy

`deny.toml` is the Rust dependency policy. It rejects yanked crates, vulnerable
RustSec advisories, unknown registries, unknown git sources, and
GPL-incompatible licenses. Multiple versions and unmaintained advisories are
surfaced as warnings so they can be scheduled without blocking unrelated work.

The pull request workflow fails on `cargo audit`, `cargo deny`, critical
`pnpm audit` findings, gitleaks findings, and Trivy HIGH/CRITICAL image
findings. Cargo audit ignores two vulnerable lockfile entries explicitly in CI:
an optional `sqlx-mysql` package that is not enabled by any workspace crate, and
a `testcontainers` archive advisory in local integration-test harness code where
the fixed crate currently requires a newer Rust toolchain than the repository
pin. `cargo deny` carries the testcontainers exception plus the transitive
`rustls-pemfile` unmaintained advisory, which currently has no safe direct
replacement in upstream network/client dependency chains. The gitleaks job
checks out full history with `fetch-depth: 0`, so the scan covers both the pull
request and the reachable repository history.

Renovate is configured in `renovate.json` for weekly dependency PRs and lockfile
maintenance.

## Runtime target

The target from `Spec.md` is under 5 minutes on a warm cache. Record the
wall-clock duration from the first successful workflow run in the PR body before
marking the checklist item complete.
