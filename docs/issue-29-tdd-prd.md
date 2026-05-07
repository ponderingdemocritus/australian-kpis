# Issue 29 TDD PRD: Ingestion Binary

## Context

Issue #29 turns the pre-existing ingestion core into an executable service. The
binary must support one-shot ABS CPI ingestion for demos and a long-running
worker loop for scheduled discovery/backfill work.

## Spec Anchors

- `Spec.md § Async architecture`
- `Spec.md § Ingestion pipeline`
- `Spec.md § Deployment`
- `Spec.md § Testing strategy`

## Behavior Contract

- `au-kpis-ingestion --once --source abs --dataflow cpi` runs one bounded
  discover -> fetch -> parse -> load pass through `au-kpis-ingestion-core`.
- `au-kpis-ingestion run` starts a queue-backed worker loop and remains alive
  until cancellation.
- `/metrics` exposes Prometheus text while the process is running.
- `AU_KPIS_HTTP__SHUTDOWN_GRACE_PERIOD_SECS` controls the drain window.
- The configured drain window is passed into `au-kpis-ingestion-core` so stage
  cancellation/drain behavior uses the same deadline as process shutdown.
- The binary loads shared config through `au-kpis-config`.
- A single `CancellationToken` is shared by the signal handler, metrics server,
  worker loop, and pipeline run.
- Any mode that persists observations or artifact provenance requires durable
  object-store config; there is no in-memory fallback for `--once`.
- `AU_KPIS_STARTUP_NOTIFY_FILE` is written only after adapter registration,
  object-store validation, and runtime construction succeed.

## Edge Cases Covered

- `--once` without `--source`/`--dataflow` fails before runtime startup.
- Unsupported dataflows fail before external I/O.
- `run` rejects source/dataflow filters.
- SIGTERM drains before process exit.
- The worker checks both discovery and backfill queues.
- `--once` and `run` both fail fast when durable object-store config is absent.
- Missing partial object-store configuration fails fast.
- Failed startup validation must not publish a readiness/startup file.

## Test Plan

- Unit tests cover CLI mode resolution, unsupported dataflow validation, and
  metrics rendering.
- Unit tests also cover the durable object-store requirement and shutdown-grace
  propagation into pipeline options.
- Integration tests cover ABS CPI one-shot ingestion against deterministic
  fixtures with explicit MinIO-backed object storage, metrics exposure, SIGTERM
  handling, configured drain windows, and startup-notify behavior on failed
  worker startup.

## TDD Notes

- RED: add a unit invariant proving `--once` no longer accepts missing
  object-store config, plus integration coverage that startup notification is
  not emitted before runtime validation succeeds.
- GREEN: remove the `Mode::Once` in-memory blob-store fallback, make positive
  process tests inject MinIO config explicitly, and move startup notification
  after runtime construction.
- REFACTOR: keep object-store env parsing isolated from process startup so the
  contract stays unit-testable without mutating global process env.

## Regression Checklist

- `--once` starts with only the config it actually uses: database plus optional
  telemetry settings. Missing Redis config must not block one-shot demos.
- `--once` and `run` must not start against ephemeral in-memory artifact
  storage when durable object-store config is missing.
- The configured shutdown grace must govern both the process timeout and the
  ingestion pipeline's internal cancellation drain path.
- Startup notification must not be emitted for a process that will still fail
  adapter or object-store initialization.
- Ingestion CLI tests must not inherit ambient `AU_KPIS_OBJECT_STORE__*`
  variables from the parent shell; success and failure paths set object-store
  config explicitly.
- A malformed or unsupported queued job is nacked and dead-lettered without
  terminating the long-running worker loop.
- The container image defaults to `run` without preventing callers from
  overriding the command to execute `--once`.
