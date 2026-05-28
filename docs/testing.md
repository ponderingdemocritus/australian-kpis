# Testing Infrastructure

This repository uses a layered test stack from `Spec.md § Testing strategy`.

## Local commands

```bash
# Fast Rust suite with repo-shared scheduling and timeouts
cargo nextest run --workspace

# CI-equivalent Rust suite: retries capped at 2 and any flaky retry fails the run
cargo nextest run --workspace --profile ci

# Coverage for Codecov / local inspection
cargo llvm-cov nextest --workspace --profile ci --lcov --output-path target/llvm-cov/lcov.info

# Snapshot verification
cargo insta test --workspace --check

# Weekly-style mutation score report, normally run by GitHub Actions
cargo mutants --workspace \
  --test-workspace true \
  --test-tool nextest \
  --minimum-test-timeout 120 \
  --output target/mutation \
  --no-times \
  --colors never \
  -- --profile ci
python3 tools/ci/mutation_report.py \
  --out-dir target/mutation/mutants.out \
  --min-score 70 \
  --markdown target/mutation/report.md \
  --json target/mutation/mutation-report.json \
  --issue-body target/mutation/add-test-issue.md

# Nightly parser fuzz targets, normally run by GitHub Actions for 30 minutes
# each. Use -runs=1 for a local smoke check before changing the target budget.
python3 tools/ci/seed_fuzz_corpora.py
cargo +nightly-2025-10-01 fuzz run sdmx_json -- -runs=1
cargo +nightly-2025-10-01 fuzz run xls -- -runs=1
cargo +nightly-2025-10-01 fuzz run csv -- -runs=1
cargo +nightly-2025-10-01 fuzz run pdf_response -- -runs=1

# Weekly chaos suite, normally run by GitHub Actions in the staging environment
tests/chaos/run.sh --results-dir target/chaos
tests/chaos/run.sh --dry-run --results-dir target/chaos

# Contract fuzzing against a running API
schemathesis --config-file tests/contract/schemathesis.toml run \
  --checks all \
  --exclude-checks positive_data_acceptance \
  --url http://127.0.0.1:3000 \
  http://127.0.0.1:3000/v1/openapi.json

# Nightly staging profile, matching the scheduled deep-fuzz workflow
export AU_KPIS_CONTRACT_DATAFLOW="${AU_KPIS_CONTRACT_DATAFLOW:-abs.cpi}"
export AU_KPIS_CONTRACT_DIMENSION="${AU_KPIS_CONTRACT_DIMENSION:-region}"
export AU_KPIS_CONTRACT_SERIES_KEY="${AU_KPIS_CONTRACT_SERIES_KEY:-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}"
schemathesis --config-file tests/contract/schemathesis.deep.toml run \
  --checks all \
  --exclude-checks positive_data_acceptance \
  --url "$AU_KPIS_STAGING_BASE_URL" \
  "$AU_KPIS_STAGING_BASE_URL/v1/openapi.json"
```

The Schemathesis configs load `tests/contract/hooks.py` so CSV and Parquet
observation responses are still deserialized for schema checks.

## Shared Docker harnesses

`crates/testing/` owns the shared `testcontainers` harnesses used by integration
tests. Today it provides:

- Redis via `au_kpis_testing::redis::start_redis`
- Timescale/Postgres via `au_kpis_testing::timescale::start_timescale`
- MinIO via `au_kpis_testing::minio::start_minio`

Use these helpers instead of duplicating image tags, startup waits, or URL
construction in individual test files.

## Snapshot policy

Repo-level Insta defaults live in `.config/insta.yaml`:

- snapshot updates are disabled by default in normal test runs
- diffs are shown when snapshots drift

Accept intentional snapshot changes explicitly with `cargo insta accept`.

## Flake policy

CI uses the `ci` nextest profile from `.config/nextest.toml`:

- retries are capped at 2 attempts after the initial failure
- the PR workflow fails if the nextest log reports any `FLAKY` retries
- JUnit output is written to `target/nextest/ci/junit.xml` for reporting

This matches the repository's zero-flake policy: fix flaky tests or delete them.

## Mutation testing

`.github/workflows/mutation-weekly.yml` runs `cargo mutants --workspace` every
Sunday at 06:00 UTC with `cargo-nextest` as the test runner. The scheduled gate
requires at least a 70% mutation score and uploads the retained
`cargo-mutants-report` artifact for maintainers to inspect.

Surviving or timed-out mutants are summarized by `tools/ci/mutation_report.py`.
When the scheduled run finds survivors, the workflow opens a follow-up issue
labeled `add test` so the missing coverage is tracked as normal backlog work.

## Parser fuzzing

`fuzz/` contains `cargo-fuzz` targets for the parser families identified in the
spec: ABS SDMX-JSON, RBA/APRA XLS, RBA CSV, and PDF sidecar response JSON.
Corpora are seeded from committed adapter fixtures and the curated PDF sidecar
response sample. The nightly workflow keeps newly discovered crashing inputs in
the `cargo-fuzz-artifacts` artifact so they can be minimized and promoted into
the committed corpus with the associated fix.

## Chaos testing

`tests/chaos/` contains the weekly chaos suite for the five resilience scenarios
listed in the spec. The workflow uploads a retained `chaos-results` artifact and
adds the Markdown summary to each scheduled run. See `docs/chaos.md` for the
scenario invariants and failure interpretation guide.
