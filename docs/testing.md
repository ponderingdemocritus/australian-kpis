# Testing

This repository standardizes on `cargo nextest` for Rust test execution, `cargo-llvm-cov` for coverage, `insta` for snapshots, `proptest` for property checks, and `schemathesis` for OpenAPI contract fuzzing.

## Rust test runs

Use the shared workspace profile in [`.config/nextest.toml`](../.config/nextest.toml):

```bash
cargo nextest run --workspace
```

The default profile retries failures at most twice. CI captures the nextest log and fails the job if any test only passes on a retry, which keeps the zero-flake policy intact on the pinned Rust 1.85 toolchain.

## Coverage

Generate local line coverage and the LCOV artifact consumed by CI:

```bash
rustup component add llvm-tools-preview
mkdir -p target/llvm-cov
cargo llvm-cov --workspace --fail-under-lines 80 --lcov --output-path target/llvm-cov/lcov.info
```

## Snapshot tests

Snapshot tests live alongside the owning crate under `tests/snapshots/`.

```bash
INSTA_UPDATE=always cargo test -p au-kpis-domain --test observation_snapshot
```

Review the generated `.snap` diff before committing it.

## Property tests

Property tests should focus on semantic invariants rather than implementation details. The initial example lives in `crates/au-kpis-domain/tests/property_roundtrip.rs` and checks JSON round-tripping for `Observation`.

## Container-backed integration tests

Shared Docker helpers live in [`crates/testing`](../crates/testing/). Use them instead of open-coding container image setup when a test needs TimescaleDB, Redis, or MinIO.

Current integration suites require a working Docker daemon:

```bash
cargo nextest run --workspace
```

## Schemathesis

The baseline Schemathesis config lives at [`tests/contract/schemathesis.toml`](../tests/contract/schemathesis.toml).

```bash
export AU_KPIS_API_BASE_URL=http://127.0.0.1:3000
uvx schemathesis --config-file tests/contract/schemathesis.toml run http://127.0.0.1:3000/v1/openapi.json
```
