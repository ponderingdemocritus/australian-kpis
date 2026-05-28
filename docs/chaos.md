# Chaos Testing

The chaos suite is a weekly staging-equivalent resilience check for the five
failure modes named in `Spec.md` testing strategy. It is deliberately scripted
under `tests/chaos/` so operators can run the same checks locally, in staging,
or from GitHub Actions.

## Run The Suite

Run every scenario and write review artifacts under `target/chaos`:

```bash
tests/chaos/run.sh --results-dir target/chaos
```

Run a single scenario:

```bash
tests/chaos/run.sh --scenario kill-ingestion-mid-load --results-dir target/chaos
```

Check wiring without touching infrastructure:

```bash
tests/chaos/run.sh --dry-run --results-dir target/chaos
```

The weekly workflow `.github/workflows/chaos-weekly.yml` runs at `0 5 * * 0`
with the GitHub `staging` environment. It appends `target/chaos/summary.md` to
the workflow summary and uploads `target/chaos/**` as the `chaos-results`
artifact for operator review after each scheduled run.

## Scenarios And Invariants

| Scenario | Failure mode | Expected invariant |
|---|---|---|
| `kill-ingestion-mid-load` | Kill ingestion worker mid-load | Produced work drains before shutdown; restart leaves no duplicates/no gaps. |
| `sever-db-connection` | Sever DB connection | Queue leases are reclaimed after reconnection and queued jobs resume. |
| `fill-queue-capacity` | Fill queue to capacity | Backpressure reaches producers; work is not dropped and the process does not OOM. |
| `source-5xx-circuit-breaker` | Random 5xx from source adapter | Transient failures are retried; the circuit breaker opens and recovers instead of hot-looping an unhealthy source. |
| `vacuum-heavy-writes` | Compaction/vacuum during heavy writes | Hypertable maintenance and write batches complete with no deadlocks. |

## Interpreting Failures

Each script writes one JSON line to `target/chaos/results.jsonl` and one row to
`target/chaos/summary.md`. A non-`pass` status means the corresponding invariant
did not hold or the scenario could not run to completion.

Use the failing row to pick the first system boundary to inspect:

- Ingestion shutdown failures usually point at cancellation, drain, or artifact
  handoff regressions.
- DB severing failures usually point at queue lease renewal, stale lease reclaim,
  or connection-pool recovery.
- Queue capacity failures usually point at bounded channel backpressure.
- Source 5xx failures usually point at transient error classification, retry
  backoff, or circuit breaker recovery.
- Vacuum/heavy-write failures usually point at migration, Timescale, or loader
  transaction behavior.

The workflow does not create follow-up issues automatically. Operators should
attach `summary.md`, the relevant `results.jsonl` line, and the failing job log
to the incident or bug they open.
