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
- The binary loads shared config through `au-kpis-config`.
- A single `CancellationToken` is shared by the signal handler, metrics server,
  worker loop, and pipeline run.

## Edge Cases Covered

- `--once` without `--source`/`--dataflow` fails before runtime startup.
- Unsupported dataflows fail before external I/O.
- `run` rejects source/dataflow filters.
- SIGTERM drains before process exit.
- The worker checks both discovery and backfill queues.
- Missing partial object-store configuration fails fast.

## Test Plan

- Unit tests cover CLI mode resolution, unsupported dataflow validation, and
  metrics rendering.
- Integration tests cover ABS CPI one-shot ingestion against deterministic
  fixtures, metrics exposure, SIGTERM handling, and configured drain windows.
