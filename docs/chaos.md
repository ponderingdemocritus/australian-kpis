# Production Chaos Certification

The weekly staging suite exercises every production-v1 failure class and
retains `release-chaos-report`. Run it with:

```bash
tests/chaos/run.sh --results-dir target/release-chaos-report
tools/observability/drill-all-alerts.sh target/release-chaos-report/alerts
```

Use `--scenario NAME` for one failure or `--dry-run` to inspect wiring without
touching dependencies. Each scenario writes a JSONL result, a Markdown row, and
its complete command log.

| Scenario | Failure | Required invariant |
|---|---|---|
| `redis-loss` | Redis unavailable | Public reads degrade open; protected writes fail closed. |
| `process-kill` | Ingestion killed mid-load | Admitted work drains; restart has no duplicates/no gaps. |
| `db-disconnect` | Database connection lost | Fenced leases reclaim after reconnection. |
| `upstream-429` | Source throttle | `Retry-After` survives and retry is bounded. |
| `parser-panic` | Parser worker panic | Failure is audited and sibling artifacts continue. |
| `malformed-artifacts` | Invalid XLSX/source row | Format error is isolated without process unwind. |
| `object-corruption` | Same-size R2 corruption | SHA-256 detects and verified staged bytes repair it. |
| `scheduler-failover` | Leader process exits | One standby takes over with no duplicate occurrence. |
| `slow-clients` | Bulk response client stalls/drops | Stream exits promptly and does not buffer the export. |

`drill-all-alerts.sh` parses the production rule file, generates a `promtool`
fixture for every `severity: page` alert, drives each alert to firing at 120
minutes, and supplies healthy signals until every alert is clear at 480
minutes. The generated inventory and fixture are retained with the chaos
evidence. This validates rule behavior; the Grafana notification-policy test is
still performed from the Grafana Cloud contact-point UI before launch.

A non-`pass` scenario or an alert fixture without `SUCCESS` blocks
certification. Inspect the scenario log first, then correlate its interval in
Grafana using the workflow run ID and `environment=staging` labels.
