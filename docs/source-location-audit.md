# Source Location Audit

The source-location audit is a scheduled guardrail for configured and scoped
upstream data locations. It checks official source pages, product pages,
directory listings, and API responses with deterministic HTTP rules. It does
not use browser automation, LLMs, or web search, and it does not change source
code automatically.

Run it locally with:

```bash
cargo run --locked -p au-kpis-scheduler -- source-location-audit \
  --report-path target/source-location-audit/source-location-audit.md
```

The scheduler dispatches this command before ingestion config loading,
telemetry setup, database connection, or migrations. It is external-web only and
does not require `PROD_DATABASE_URL`.

## Reports

The command writes two sibling files:

- `source-location-audit.md` for human review.
- `source-location-audit.json` for GitHub issue automation.

The command exits `0` when source drift or manual-review findings are present.
It exits non-zero only when the audit cannot run or write reports, such as an
invalid catalog rule, HTTP client construction failure, or artifact write
failure.

The JSON report has stable top-level fields consumed by GitHub Actions:

- `generated_at`: report timestamp.
- `status`: one of `ok`, `drift`, `manual_review`, or `error`.
- `checked_total`: number of rules evaluated.
- `findings_total`: number of findings.
- `results`: per-rule HTTP/result evidence.
- `findings`: operational findings with `source_id`, `dataflow_id`,
  `severity`, `current_url`, optional `latest_url`, `evidence`, and
  `recommendation`.

Status precedence is `error`, then `drift`, then `manual_review`, then `ok`.

## Scheduled Workflow

`.github/workflows/source-location-audit.yml` runs every Monday at `06:00 UTC`
and can also be started with `workflow_dispatch`.

The workflow:

1. Checks out the repository.
2. Sets up Rust.
3. Runs the scheduler `source-location-audit` command.
4. Uploads `target/source-location-audit/` as the `source-location-audit`
   artifact.
5. Opens, updates, or closes the tracked source drift todo issue.

Drift and manual-review findings do not fail the workflow. The GitHub issue is
the operational signal for source review. Report status `error` fails the
workflow after artifacts are uploaded because the audit could not reliably
verify sources.
Manual `workflow_dispatch` runs from non-`main` refs only produce retained
report artifacts; they do not create, update, or close the singleton tracked
issue.

## Issue Lifecycle

The workflow manages one deduplicated issue titled:

```text
data: review source location drift
```

The issue body contains this hidden marker:

```html
<!-- source-location-audit:tracked -->
```

When findings exist, the workflow creates or updates the issue and applies:

- `type:data`
- `area:ingestion`
- `source-drift`

The body includes summary counts, a findings table, a workflow artifact link,
and a checklist generated from finding recommendations.

When no findings exist and the tracked issue is open, the workflow comments
with the passing run URL and closes the issue.

## Rule Catalog

Rules live in
`crates/bins/au-kpis-scheduler/src/source_location_audit.rs`.

Add or update a rule when a source adapter, scoped source, or manual placeholder
changes. Prefer the narrowest deterministic rule that proves the source location
is still meaningful:

- Use `ContainsAny` for rolling latest-release or product pages where stable
  page text confirms the intended source.
- Use `BudgetYear` for official budget indexes where a newer year should flag
  older configured URLs.
- Use `DirectoryListing` for NEMWeb-style listings that must contain current
  report filename patterns.
- Use `LicensedProduct` for licensed feeds where public product pages are
  auditable but feed URLs may intentionally be empty.
- Use `WorldBankBreadyApi` for B-READY API availability checks. Null Australia
  values remain `manual_review`, not scored drift.
- Use `ManualPlaceholder` for placeholders such as
  `compute.au_datacentre_capacity_mw` until a reviewed source replaces
  `example.test`.

Tests for rule semantics belong in
`crates/bins/au-kpis-scheduler/tests/source_location_audit.rs` and should use
fixture HTML or JSON rather than live network calls.
