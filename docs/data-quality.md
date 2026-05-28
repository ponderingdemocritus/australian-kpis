# Data Quality

The production data-quality job is a scheduled guardrail for silent corruption.
It runs hourly and writes a retained daily report artifact for operators.

## Per-dataflow rules

Rules live in `crates/bins/au-kpis-scheduler/src/data_quality.rs` and are keyed
by dataflow id. Each rule covers:

- plausible range: numeric observations must stay inside the configured bounds
- rolling z-score: values more than five standard deviations from the current
  dataflow mean are flagged
- cardinality: the latest period must contain the expected fraction of active
  series
- recency: the latest observation must match the expected dataflow cadence
- revision volume: revised observations ingested during the daily report window
  must stay below the configured threshold

The initial catalog covers the implemented dataflows: `abs.cpi`,
`rba.statistical_tables`, `apra.quarterly_statistics`,
`treasury.budget_papers`, and the NSW/VIC/QLD state budget dataflows.

## Running locally

```bash
cargo run -p au-kpis-scheduler -- data-quality \
  --report-path target/data-quality/data-quality-report.md
```

The command writes Markdown and JSON reports next to the requested report path.
Set `AU_KPIS_DATABASE__URL` to point at the target database.

## PagerDuty

When anomalies are detected and `AU_KPIS_PAGERDUTY_ROUTING_KEY` is set, the job
sends one PagerDuty Events v2 trigger with the anomaly summaries, affected
dataflows, and the report window. The existing Alertmanager route also sends
page-severity alerts to PagerDuty; this job uses PagerDuty directly so scheduled
checks page even if Prometheus scraping is delayed. If anomalies are detected
without a routing key, the command exits non-zero after writing the report.

## Scheduled workflow

`.github/workflows/data-quality.yml` runs at `0 * * * *` and can be started
manually. It uses `secrets.PROD_DATABASE_URL`, `secrets.PAGERDUTY_ROUTING_KEY`,
and uploads `data-quality-report` containing the daily report artifacts.

The job does not hand-edit endpoint or dataflow content. Drift is reviewable in
PRs because changes to the rule catalog, report shape, or schedule are ordinary
source diffs.
