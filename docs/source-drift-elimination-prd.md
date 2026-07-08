# Source Drift Elimination PRD

Status: Draft
Owner: Data platform / ingestion
Created: 2026-06-30
Related docs: `Spec.md`, `docs/source-location-audit.md`,
`docs/agentic-source-research-prd.md`, `docs/data-quality.md`,
`.context/data-source-review-2026-06-30.md`

## Summary

Australian KPI source reliability currently depends on several separate source
lists:

- adapter metadata in Rust crates,
- source-location audit rules,
- APS scorecard config,
- coverage-report expectations,
- documentation and CI workflow issue automation.

This works while maintainers remember to update all locations together. It does
not eliminate drift. The goal of this project is to make source drift hard to
introduce, fast to detect, and operationally obvious when it happens.

The product is a governed source-register system plus CI automation:

1. A versioned source register becomes the single reviewed catalog for scoped
   sources, audit rules, source freshness policy, license/attribution, manual
   review requirements, and coverage status.
2. CI contract tests fail when adapters, scorecards, source audits, docs, or
   coverage reports diverge from the register.
3. A scheduled source review workflow runs deterministic audits first, then
   launches agentic research only for findings requiring human-style discovery.
4. Research output is constrained to evidence packages, GitHub issue updates,
   and optional draft PRs. It never mutates ingestion behavior or scored source
   mappings without review.
5. Tests cover the register parser, rule evaluation, CI scripts, workflow issue
   automation, agentic research output validation, and real-world failure modes.

## Problem

Source drift appears in four forms:

- Location drift: a publisher moves an index, API, release page, directory, or
  artifact.
- Semantic drift: a source remains reachable but no longer exposes the intended
  dataset, measure, dimension, license, cadence, or release pattern.
- Coverage drift: APS or adapter scope changes without an audit policy,
  coverage status, or manual-review schedule.
- Operational drift: CI creates passive reports but no actionable issue, or
  agent research produces unreviewed recommendations that are not tied back to
  source contracts.

The June 2026 review showed concrete examples:

- RBA statistical tables and Victoria Planning pages are reachable only behind
  bot filtering from this environment.
- World Bank B-READY latest Australia values are null and must remain manual.
- NAIC AI Adoption Tracker exists, but plain HTTP fetching is brittle.
- Curated APS inputs have provenance but are not all covered by source-location
  audit rules.
- The audit rules are hand-maintained separately from adapter and scorecard
  scope.

## Goals

- Every scoped adapter dataflow and every APS source-dataflow has exactly one
  source-governance entry or an explicit reviewed exemption.
- Adding or changing a source requires updating one register and regenerating
  derived artifacts or satisfying contract tests.
- Weekly CI creates or updates one actionable drift issue with deterministic
  evidence plus agent-researched recommendations where useful.
- Agentic research can find current official source locations, publisher
  guidance, API alternatives, licenses, and cadence evidence, but it cannot
  silently expand scope or mark manual inputs as scored.
- Source drift tests are broad enough that rule changes, workflow changes, and
  source-register changes cannot regress unnoticed.

## Non-Goals

- Do not ingest new dataflows merely because agent research finds candidate
  public data.
- Do not let an agent auto-promote manual, low-confidence, or visible-unscored
  APS inputs to scored status.
- Do not use browser/LLM research in hot-path ingestion.
- Do not replace source-specific adapter validation with generic page scraping.
- Do not require production database access for source-location audit CI.

## Users

- Data maintainers need a single place to review scoped sources and their
  freshness requirements.
- Adapter implementers need clear acceptance criteria for adding a source.
- On-call operators need one tracked issue containing current drift evidence and
  recommended next actions.
- Reviewers need CI to prove that source metadata, audit rules, scorecard config,
  and docs stayed synchronized.

## Source Register

Add a versioned source register, initially:

```text
crates/au-kpis-source-register/config/source-register.v1.toml
```

The register is config, not code. It should be parsable by a small Rust crate
that exposes typed records to scheduler, ingestion, tests, docs generation, and
CI utilities.

Each record must include:

- `source_id`
- `dataflow_id`
- `status`: `active`, `manual_pending`, `visible_unscored`,
  `coverage_gap`, `licensed_feed`, `placeholder`, or `retired`
- `owner_area`: adapter, scorecard, curated, licensed, or experimental
- `canonical_url`
- `audit_policy`
- `license`
- `attribution`
- `cadence`
- `review_frequency`
- `source_scope`: spec anchor, issue number, or scorecard config reference
- `provenance_requirements`
- `validation_requirements`
- `expected_missing_reason`, when unresolved
- `manual_review_due_at`, when manual or low-cadence
- `replacement_candidate`, when placeholder or coverage gap

Audit policies:

- `contains_any`: stable page text confirms the intended source.
- `directory_listing`: current directory contains required filename patterns.
- `budget_year`: official budget index exposes the expected latest year.
- `api_json`: JSON path or semantic evaluator confirms current values.
- `licensed_product`: public product page is reachable for licensed feeds.
- `manual_placeholder`: intentionally unresolved source that must remain visible.
- `manual_register_only`: no reliable live URL, but a review due date is enforced.
- `bot_filtered`: source is official but CI cannot fetch it directly; requires a
  reviewed stable artifact, mirror, API, or manual research task.

Register invariants:

- `dataflow_id` is globally unique.
- `source_id` must match adapter metadata when an adapter exists.
- APS indicators must reference a register dataflow unless the indicator is
  explicitly derived from multiple register entries.
- A `manual_pending` or `visible_unscored` input must include `retrieved_at`,
  `reviewed_by`, `reviewed_at`, and `manual_review_due_at`.
- A `placeholder` must have a non-example replacement plan or a tracked issue.
- A `licensed_feed` may omit a feed URL only when the product page and license
  terms are audited.

## Product Requirements

### PR-1: Register-Driven Audit Rules

The scheduler source-location audit must load audit rules from the source
register or from generated Rust constants produced from the register.

Acceptance criteria:

- The existing 30 rules are represented in the register.
- `compute.au_datacentre_capacity_mw` remains a manual placeholder until
  replaced.
- Curated APS records for oversight strength, control/enable spend, and
  surveillance intensity have explicit audit or manual-register policies.
- The audit report includes the register version and per-record status.

### PR-2: Coverage And Scope Contract Tests

Add contract tests that compare:

- adapter manifest dataflow ids,
- adapter `dataflow_metadata()`,
- APS `source_dataflow_id` values,
- coverage report expected statuses,
- source-location audit policies,
- `Spec.md` APS source register entries where practical.

Acceptance criteria:

- A new adapter dataflow without a register entry fails CI.
- A new APS indicator referencing an unknown source-dataflow fails CI.
- A register entry without an adapter must be manual, placeholder, licensed, or
  coverage-gap.
- Generated docs and report schemas include all current register entries.

### PR-3: Deterministic Drift Detection

Strengthen the existing source-location audit:

- HTTP retries with bounded backoff.
- Optional HTTP/1.1 retry for hosts with HTTP/2 stream instability.
- Clear distinction between `drift`, `manual_review`, `bot_filtered`,
  `transport_error`, and `tooling_error`.
- Per-host override for expected access restrictions.
- Staleness checks for manual/curated `reviewed_at` and `manual_review_due_at`.

Acceptance criteria:

- NAIC transient HTTP/2 failure is classified as transport/tooling unless a
  retry confirms semantic absence.
- RBA and Victoria Planning 403/429 responses become `bot_filtered` findings with
  source-specific recommendations, not generic drift.
- World Bank null Australia values remain `manual_review` and cannot be scored.

### PR-4: Agentic Source Research Workflow

Add a scheduled and manually dispatchable CI workflow:

```text
.github/workflows/source-research-review.yml
```

The workflow runs after deterministic audit artifacts exist. It should only run
for findings with `manual_review`, `bot_filtered`, `drift`, or repeated
`transport_error` status.

Research task inputs:

- source/dataflow id,
- current URL,
- audit evidence,
- register record,
- allowed domains, if configured,
- required evidence checklist.

Research task outputs:

- `target/source-research/<artifact_id>.md`
- `target/source-research/<artifact_id>.json`
- `artifact_id` is unique per audit finding so multi-rule dataflows do not
  overwrite evidence.
- proposed classification: `same_source`, `moved`, `bot_filtered`,
  `source_retired`, `candidate_replacement`, `insufficient_evidence`
- source URLs with publisher names and retrieval timestamp,
- license/attribution evidence,
- cadence evidence,
- recommendation,
- risk notes.

Hard constraints:

- Research agents must prefer official publisher domains and primary sources.
- Research agents must not add new source scope unless the register marks a
  replacement candidate path.
- Research agents must not modify Rust adapters or scorecard status directly in
  scheduled mode.
- Any generated PR must be draft and limited to docs/register/audit-rule changes
  unless manually approved.

Acceptance criteria:

- The workflow appends research summaries to the singleton source-drift issue.
- Research artifacts are uploaded with 30-day retention.
- If a finding repeats for two scheduled runs, the issue checklist preserves
  history rather than overwriting context.
- Research JSON is schema-validated before issue comments are posted.

### PR-5: Auto-Review CI For Source PRs

Add CI checks for PRs touching source-related paths:

- `Spec.md`
- `docs/source-location-audit.md`
- `docs/source-candidate-decisions.md`
- `crates/adapters/**`
- `crates/au-kpis-scorecard/config/**`
- `crates/au-kpis-source-register/**`
- `crates/bins/au-kpis-scheduler/**`
- `.github/workflows/source-*.yml`

Required auto-review checks:

- source-register contract test,
- source-audit fixture tests,
- scorecard coverage consistency,
- generated docs freshness,
- workflow script lint/schema validation,
- focused agentic review comment for source-governance risks.

The agentic PR review should produce a markdown artifact and, where supported,
one PR comment. It should review for:

- unscoped source expansion,
- missing provenance/license/cadence,
- missing manual-review status,
- missing tests,
- unsafe scoring promotion,
- non-official replacement URLs,
- ingestion hot-path browser/LLM dependencies.

Acceptance criteria:

- A PR that changes APS source config but omits register updates fails.
- A PR that adds an audit policy without fixture tests fails.
- A PR that changes source-register generated docs without regeneration fails.
- Agentic review failure should not block if the model service is unavailable,
  but schema/lint/contract tests must block.

### PR-6: Full Test Coverage

Add tests at these layers:

Unit tests:

- register TOML parser,
- register validation,
- each audit policy evaluator,
- manual review due-date classification,
- report status aggregation,
- source research JSON schema validation,
- issue body rendering.

Fixture tests:

- RBA 403 / bot-filtered fixture,
- Victoria Planning Cloudflare challenge fixture,
- NAIC HTTP/2 retry fallback fixture,
- World Bank null latest values fixture,
- BudgetYear newer-year fixture,
- directory listing missing-pattern fixture,
- manual placeholder and overdue manual review fixture.

Contract tests:

- adapters vs register,
- APS config vs register,
- coverage report statuses vs register,
- source-location rules vs register,
- generated docs vs register,
- GitHub workflow expected artifact paths and JSON fields.

Workflow tests:

- use local fixture reports to test issue body generation,
- test no-findings closes issue logic,
- test findings update existing singleton issue,
- test `error` status fails after artifact upload,
- test research artifacts append to issue body/comment without dropping prior
  unresolved findings.

Integration tests:

- run scheduler audit against a local fixture server,
- run research summarizer against static mocked search results,
- run coverage report against seeded catalog and register records.

Optional live smoke:

- manually dispatched non-blocking source audit against current external sources.
- publish artifacts, never mutate source code.

Coverage targets:

- 90% line coverage for source-register and source-location audit crates/modules.
- 100% branch coverage for status aggregation and register validation invariants.
- Every new audit policy must include at least one passing and one failing
  fixture.

## CI Design

### PR CI

Add or extend jobs:

```text
source-register-check
source-audit-tests
scorecard-source-contract
source-docs-generated
source-agent-review
```

Blocking:

- register parser/validation,
- contract tests,
- fixture tests,
- generated docs freshness,
- workflow syntax/schema validation.

Non-blocking or soft-fail:

- agentic PR review when external model/research dependency is unavailable.

### Weekly CI

Existing:

- `.github/workflows/source-location-audit.yml`

Add:

- deterministic audit still runs first,
- issue automation remains the operational signal,
- `status: error` still fails after artifacts upload,
- source-research workflow runs for actionable findings,
- research artifacts are attached to the singleton issue.

### Manual Dispatch

Operators can run:

- source-location audit only,
- source research for all findings,
- source research for one dataflow id,
- dry-run issue rendering from a local report artifact.

## Data Model Sketch

```toml
version = "source-register.v1"

[[dataflows]]
source_id = "rba"
dataflow_id = "rba.statistical_tables"
status = "active"
owner_area = "adapter"
canonical_url = "https://www.rba.gov.au/statistics/tables/"
license = "RBA Copyright and Disclaimer Notice"
attribution = "Source: Reserve Bank of Australia"
cadence = "weekly"
review_frequency = "weekly"
source_scope = "Spec.md#source-adapters"
validation_requirements = [
  "table URL must be under /statistics/tables/csv/ or /statistics/tables/xls/",
  "table id must parse from filename",
]

[dataflows.audit_policy]
kind = "bot_filtered"
semantic_fallback = "direct_table_artifact_manifest"
expected_statuses = [403, 429]
recommendation = "Use reviewed direct CSV/XLS table artifacts if the index is bot-filtered."
```

## Rollout Plan

### Phase 1: Register And Contract Skeleton

- Add `au-kpis-source-register` crate.
- Encode current audit rules and APS manual inputs.
- Add parser/validation tests.
- Add contract tests for APS config and source-location rules.
- Keep existing scheduler behavior functionally unchanged.

Exit criteria:

- Current 30 audit rules are represented.
- Existing tests pass.
- Missing curated APS audit policies are explicit.

### Phase 2: Scheduler Integration

- Load or generate audit rules from the register.
- Extend report schema with register version and policy kind.
- Add manual due-date and bot-filtered classifications.
- Add fixture-driven audit tests.

Exit criteria:

- Existing source-location workflow continues to update the singleton issue.
- Live audit findings classify RBA and Victoria Planning as bot-filtered/manual
  instead of ambiguous drift.

### Phase 3: Agentic Research Workflow

- Add research artifact schema.
- Add workflow that runs only from audit findings.
- Add issue-comment/update logic.
- Add fixture tests for rendered issue content.

Exit criteria:

- Manual dispatch can research one finding.
- Scheduled research appends validated evidence without changing source code.

### Phase 4: PR Auto Review

- Add path-filtered CI job for source-governance changes.
- Add deterministic source contract checks.
- Add soft-fail agentic review artifact/comment.

Exit criteria:

- A deliberately incomplete source PR fails deterministic checks.
- Agent review output is useful but never the only blocking signal.

### Phase 5: Documentation And Operations

- Generate `docs/source-location-audit.md` rule catalog from the register.
- Add operator runbook for resolving findings.
- Add examples for moved source, bot-filtered source, retired source, and manual
  placeholder replacement.

Exit criteria:

- On-call can resolve a source-drift issue without reading scheduler source.
- Reviewers can see exactly why a source is in scope.

## Open Questions

- Should register config live in `crates/au-kpis-source-register/config/` or
  under top-level `config/` for non-Rust consumers?
- Which CI runner or service should perform agentic research, and what secret
  boundary should it use?
- Should research artifacts be allowed to open draft PRs, or should they only
  update issues until the workflow proves stable?
- How should manual-review due dates differ by cadence: annual budget inputs,
  quarterly APRA inputs, monthly AI tracker inputs, and one-off curated indices?
- Do bot-filtered sources need publisher outreach/allowlisting, or should every
  such source require a machine-readable official artifact alternative?

## Success Metrics

- Zero adapter or APS dataflows without a register entry.
- Zero source-location audit rules without fixture tests.
- Zero stale manual inputs past review due date without an open issue.
- Time from source drift detection to issue update under 15 minutes in weekly CI.
- Repeated drift issues include preserved history and current recommendation.
- Source-related PRs fail deterministically when provenance, cadence, license,
  coverage status, or audit policy is missing.

## Risks

- Agentic research can produce plausible but non-authoritative replacement
  sources. Mitigation: official-domain preference, JSON schema, evidence fields,
  and mandatory human review.
- Register generation can make small source changes feel heavier. Mitigation:
  clear templates and focused contract errors.
- Bot-filtered sources can keep failing in CI even when official pages are valid.
  Mitigation: explicit `bot_filtered` classification and reviewed machine-readable
  artifact fallbacks.
- Manual inputs can become bureaucratic placeholders. Mitigation: due dates,
  tracked issue checklist, and fail-on-overdue policy for high-weight inputs.

## Definition Of Done

- Source register exists and is validated in CI.
- Scheduler audit derives from or is contract-checked against the register.
- APS config, adapter metadata, coverage report expectations, and docs cannot
  diverge silently.
- Weekly CI produces deterministic audit artifacts, updates a singleton issue,
  fails on audit errors, and adds validated agentic research where configured.
- PR CI blocks missing source governance and missing tests.
- Test suite covers source-register validation, audit policies, workflow issue
  rendering, agentic research schema validation, and the current real failure
  classes from the June 2026 review.
