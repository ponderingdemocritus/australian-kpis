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
- Supply-chain and secret scans
- API container build plus Trivy HIGH/CRITICAL image scan
- Curl and SDK smoke checks against the local contract server
- Advisory Criterion bench comparison through `critcmp`
- Advisory Codex structured review when repository secrets allow it

Rust jobs install `sccache` and use the GitHub Actions backend. TypeScript jobs
restore the pnpm store and `.turbo` cache before running Turborepo tasks.

## Runtime target

The target from `Spec.md` is under 5 minutes on a warm cache. Record the
wall-clock duration from the first successful workflow run in the PR body before
marking the checklist item complete.
