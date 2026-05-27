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
