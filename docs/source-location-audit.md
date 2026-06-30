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
- `register_version`: checked-in source register version used by the audit.
- `status`: one of `ok`, `drift`, `bot_filtered`, `manual_review`, or
  `error`.
- `checked_total`: number of rules evaluated.
- `findings_total`: number of findings.
- `results`: per-rule HTTP/result evidence, including register-derived
  `source_status` and `audit_policy_kind` when available.
- `findings`: operational findings with `source_id`, `dataflow_id`,
  `severity`, `current_url`, optional `latest_url`, `evidence`, and
  `recommendation`.

Status precedence is `error`, then `drift`, then `bot_filtered`, then
`manual_review`, then `ok`.

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

Drift, bot-filtered, and manual-review findings do not fail the workflow. The
GitHub issue is the operational signal for source review. Report status `error`
fails the workflow after artifacts are uploaded because the audit could not
reliably verify sources.
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

## Source Register

The rule catalog is derived from the versioned source register:

```text
crates/au-kpis-source-register/config/source-register.v1.toml
```

Each scoped source/dataflow entry must declare source status, canonical URL,
license, attribution, cadence, provenance requirements, validation
requirements, and an audit policy. Add or update a register entry when a source
adapter, scoped source, APS scorecard input, licensed feed, or manual
placeholder changes.

Prefer the narrowest deterministic policy that proves the source location is
still meaningful:

- Use `contains_any` for rolling latest-release or product pages where stable
  page text confirms the intended source.
- Use `budget_year` for official budget indexes where a newer year should flag
  older configured URLs.
- Use `directory_listing` for NEMWeb-style listings that must contain current
  report filename patterns.
- Use `licensed_product` for licensed feeds where public product pages are
  auditable but feed URLs may intentionally be empty.
- Use `world_bank_bready_api` for B-READY API availability checks. Null Australia
  values remain `manual_review`, not scored drift.
- Use `manual_placeholder` for placeholders such as
  `compute.au_datacentre_capacity_mw` until a reviewed source replaces
  `example.test`.
- Use `manual_register_only` for curated or visible-unscored inputs with
  enforced review due dates but no reliable live URL audit.
- Use `bot_filtered` for official sources that block or challenge automated CI
  requests. Expected 403/429-style responses are classified as `bot_filtered`,
  not source drift.

Tests for rule semantics belong in
`crates/bins/au-kpis-scheduler/tests/source_location_audit.rs` and should use
fixture HTML or JSON rather than live network calls.

Contract tests in
`crates/bins/au-kpis-scheduler/tests/source_register_contract.rs` ensure APS
scorecard `source_dataflow_id` values and scheduler default rules stay
synchronized with the register.

## Source Research Review

`.github/workflows/source-research-review.yml` runs at `0 7 * * 1`, reuses the
latest scheduled source-location audit artifact from `main` in the last eight
days, reads the source register from the audited commit, runs the audited
source-register and scheduler/register Rust contracts, and uses the current
reviewed research tooling. Completed scheduled audit runs are reusable only when
their retained `source-location-audit` artifact is present; in-progress scheduled
runs do not block reuse of the latest completed artifact in the eight-day
window. Manual runs require an explicit source-location audit run id and can
target all findings or one `dataflow_id`.

The workflow generates bounded research packets under:

```text
target/source-research/
```

Each packet is schema-validated before issue comments are posted. Scheduled mode
creates evidence packets and recommendations only; it does not change Rust
adapters, scoring config, or source mappings. Issue comments are posted only for
audits from `main` when the workflow itself is running from `main`; branch-local
and manual non-main runs upload artifacts only. Replacement URLs still require a
reviewed register/config change.
If an audit report has aggregate `error` status, non-error actionable findings
still get retained research packets before the workflow fails on the audit error.
