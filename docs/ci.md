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
- Schemathesis contract checks against the docker-compose API stack
- Supply-chain and secret scans: `cargo deny`, `cargo audit`,
  `pnpm audit --audit-level critical`, and gitleaks over full history
- API container build plus Trivy HIGH/CRITICAL image scan
- k6 smoke checks against the docker-compose stack on pull requests and the
  configured staging API when called by merge queue
- Advisory Criterion bench comparison through `critcmp`
- Advisory Codex structured review when repository secrets allow it

Rust jobs install `sccache` and use the GitHub Actions backend. TypeScript jobs
restore the pnpm store and `.turbo` cache before running Turborepo tasks.

## Merge queue workflow

`.github/workflows/merge.yml` is triggered by GitHub's `merge_group` event. It
calls the full pull request gate set from `.github/workflows/pr.yml` with
merge-queue mode enabled, so k6 smoke runs against the configured staging API
instead of the local compose API. It also runs the deeper Schemathesis profile
against staging and aggregates both through `CI OK`.

The `main` branch must require merge queue and status checks as documented in
`docs/ops/merge-queue.md`. GitHub removes a queued batch when a required
`merge_group` check fails or times out, so the full group is rejected before any
member PR lands.

## k6 smoke gate

The smoke job starts the compose API stack and an InfluxDB v1 metrics store for
pull requests, applies migrations, seeds `apps/web/e2e/fixtures/explorer.sql`,
and runs `apps/bench/smoke.js`. Merge-queue runs use
`AU_KPIS_STAGING_BASE_URL` from repository variables as the API target and still
publish k6 samples through the configured InfluxDB output.

Set `K6_INFLUXDB_ADDR` as a repository variable when CI should post trend data
to a shared InfluxDB/Grafana environment. If it is not set, the job posts to the
local compose InfluxDB database at `http://127.0.0.1:8086/k6`. Set
`AU_KPIS_SMOKE_API_KEY` as a repository secret when staging should run the smoke
scenario with an API-key tier instead of anonymous quotas.

## Nightly k6 load tests

`.github/workflows/k6-nightly.yml` runs at `0 2 * * *` against staging. It
executes `apps/bench/sustained.js` and `apps/bench/burst.js`, requires
`AU_KPIS_STAGING_BASE_URL`, and writes k6 samples through `K6_OUT` to the shared
InfluxDB datasource configured by `K6_INFLUXDB_ADDR`. The sustained scenario
uses 100 virtual users for 10 minutes. The burst scenario ramps to 2000 virtual
users, holds, and ramps down while enforcing the 429 and 5xx budgets from the
benchmarking spec.

The workflow uploads `k6-load-summary` artifacts for historical comparison.
Adding the `perf:regression` label to a PR runs the same staging load suite,
downloads the latest successful scheduled summary when available, and posts a
`k6 load comparison` PR comment through `actions/github-script`.

## Weekly cargo-mutants

`.github/workflows/mutation-weekly.yml` runs at `0 6 * * 0` on `main`. The job
installs `cargo-mutants` and `cargo-nextest`, then runs `cargo mutants
--workspace` with `--test-workspace true` so each mutant is checked against the
full workspace test suite. The mutation score threshold is 70%.

The workflow always writes a Markdown report, machine-readable JSON, and the raw
`mutants.out` directory to the retained `cargo-mutants-report` artifact. If any
mutants survive or time out, `tools/ci/mutation_report.py` generates a
follow-up body and the workflow creates an `add test` issue that lists the
surviving cargo-mutants locations and replacements.

## Weekly source-location audit

`.github/workflows/source-location-audit.yml` runs at `0 6 * * 1` on `main`.
It runs `au-kpis-scheduler source-location-audit` without requiring production
database access, uploads Markdown and JSON reports, and manages one deduplicated
`data: review source location drift` issue when configured data source
locations drift or need manual review.

Drift, bot-filtered, and manual-review findings do not fail the workflow. The
tracked GitHub issue is the operational todo. The companion
`.github/workflows/source-research-review.yml` workflow runs at `0 7 * * 1`,
reuses the latest scheduled `Source Location Audit` artifact from `main` within
the last eight days, reads the source register from the audited commit, runs the
current reviewed research tooling, writes schema-validated
`target/source-research/` packets for actionable findings, uploads them for 30
days, and appends a summary to the tracked issue only when the audited branch is
`main`. Mixed reports with aggregate `error` status still preserve packets for
non-error actionable findings before the workflow fails on the audit error.
Audit runs from revisions that predate the source register fail fast with a
compatibility error.

Pull requests also run the `Source Governance Contracts` job in
`.github/workflows/pr.yml`. It blocks on the source-register contract test,
source-location fixture tests, and source-research schema tests.

See `docs/source-location-audit.md` for the command, report schema, issue
lifecycle, source-register maintenance notes, and research artifact contract.

## Weekly chaos suite

`.github/workflows/chaos-weekly.yml` runs at `0 5 * * 0` with the GitHub
`staging` environment. It executes `tests/chaos/run.sh`, surfaces the Markdown
summary in the workflow run, and uploads retained `chaos-results` artifacts for
operator review.

The suite covers the five chaos scenarios from the spec: killing ingestion
mid-load, severing DB connectivity, filling queue capacity, source 5xx circuit
breaker recovery, and vacuum/compaction during heavy writes. See `docs/chaos.md`
for local execution and failure interpretation.

## Phase 5 full-load validation

`apps/bench/full-load.js` drives `/v1/observations` at 1000 rps for 10 minutes
with p99 <1s, error rate <0.1%, and no dropped k6 iterations. It is exposed as
the `full-load` manual-dispatch scenario in `.github/workflows/k6-nightly.yml`
so operators can rerun the Phase 5 validation against staging without making it
part of the nightly sustained/burst load run. The May 2026 validation report is
committed at `docs/perf/load-test-2026-05.md`.

## SDK publishing

`.github/workflows/sdk-publish.yml` runs on `main` after SDK package or
Changesets metadata changes. It builds `@au-kpis/sdk` and
`@au-kpis/sdk-generated`, runs the SDK test suite, then uses
`changesets/action` with `pnpm version-packages` and `pnpm release:sdk`.

When unreleased changeset files land on `main`, the workflow opens the
version-package PR with generated changelogs. When that version PR merges, the
same workflow publishes the public packages to npm with `NPM_TOKEN`.

## Nightly cargo-fuzz

`.github/workflows/fuzz-nightly.yml` runs at `0 3 * * *` on `main`. It installs
`cargo-fuzz`, seeds corpora through `tools/ci/seed_fuzz_corpora.py`, and runs
the SDMX-JSON, XLS, CSV, and PDF sidecar response targets for 30 minutes per target.

Any cargo-fuzz failure is treated as a release blocker. The workflow uploads
the retained `cargo-fuzz-artifacts` artifact, including the target corpora and
any crashing inputs written under `fuzz/artifacts/`. It also files a bug issue
for the failing scheduled run so maintainers can reproduce, minimize, and add
the crashing input to the committed corpus or a follow-up artifact.

## Contract fuzzing

The pull request `Contract (schemathesis)` job starts the docker-compose API
stack, applies migrations, seeds `apps/web/e2e/fixtures/explorer.sql`, validates
the live `/v1/openapi.json` document against the OpenAPI 3.1 schema, and runs
`tests/contract/schemathesis.toml` against every documented API operation. The
PR profile uses a small deterministic budget so this gate can stay blocking
without dominating CI time. The config uses `tests/contract/hooks.py` to
deserialize CSV and Parquet observation responses for response-schema checks,
and seeds known fixture IDs for resource-specific paths. It excludes Schemathesis'
`positive_data_acceptance` check because several parameters, such as pagination
cursors, are opaque tokens that cannot be generated as arbitrary valid strings;
status-code, content-type, response-schema, server-error, and negative-data
checks remain active.

Nightly schemathesis deep fuzzing lives in
`.github/workflows/contract-nightly.yml` and runs at `0 4 * * *` against the
configured staging API with `tests/contract/schemathesis.deep.toml`. Set
`AU_KPIS_STAGING_BASE_URL` as a repository variable. Set
`AU_KPIS_SMOKE_API_KEY` as a repository secret when staging should use an API-key
tier for the deeper request volume. If staging uses different fixture data from
the local smoke seed, set `AU_KPIS_CONTRACT_DATAFLOW`,
`AU_KPIS_CONTRACT_DIMENSION`, and `AU_KPIS_CONTRACT_SERIES_KEY` repository
variables so resource-specific fuzz cases reach existing records.

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
