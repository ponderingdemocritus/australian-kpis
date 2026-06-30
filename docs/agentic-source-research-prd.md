# Agentic Source Research PRD

Status: Draft  
Owner: Data platform / ingestion  
Created: 2026-06-30  
Related docs: `docs/source-drift-elimination-prd.md`,
`docs/source-location-audit.md`, `docs/ci.md`,
`crates/au-kpis-source-register/config/source-register.v1.toml`

## Summary

The current source research workflow creates bounded, schema-validated research
packets from deterministic source-location audit findings. It does not yet ask
an external research agent to inspect publisher sites, reason about moved
sources, or propose evidence-backed next actions.

This PRD defines the next product layer: a guarded agentic research mode that
takes the data we already trust, gathers primary-source evidence, and returns a
reviewable artifact. The agent must improve the evidence package for humans. It
must not silently expand source scope, change ingestion behavior, promote manual
inputs to scored status, or mutate adapters in scheduled mode.

## Problem

The deterministic audit can identify that a source needs attention, but it
cannot reliably answer all follow-up questions:

- Did the source move to a new official URL?
- Is the current URL still official but bot-filtered from CI?
- Is there a machine-readable API or artifact behind the publisher page?
- Did the source retire, split, rename, or change cadence?
- Is the current license, attribution, or terms-of-use evidence still valid?
- Is a candidate replacement in scope, or would using it expand the product
  without review?

Without an agentic research layer, maintainers must manually reconstruct these
answers from audit findings and source-register records. With an unconstrained
agentic layer, the system risks plausible but non-authoritative source
recommendations. The product needs a middle path: evidence-driven research that
is useful, bounded, schema-validated, and review-only by default.

## Goals

- Convert each actionable audit finding into a stronger evidence package.
- Prefer official publisher domains and primary source material.
- Capture source location, license, attribution, cadence, scope, and validation
  evidence in a machine-validated JSON artifact.
- Preserve unresolved uncertainty explicitly as `insufficient_evidence`.
- Allow manual dispatch for one dataflow or all findings.
- Keep weekly scheduled mode deterministic until agentic mode has proven safe.
- Let future draft PR generation modify only docs, source-register config, and
  audit policy metadata unless a human explicitly approves broader changes.

## Non-Goals

- Do not ingest newly discovered data.
- Do not mutate Rust adapters, loader code, scorecard config, or source status
  from scheduled agentic runs.
- Do not make model availability a blocking PR or weekly CI dependency.
- Do not let an agent add source scope simply because a public dataset exists.
- Do not bypass source-register validation, APS coverage rules, or existing
  adapter semantics.
- Do not rely on non-primary sources when primary publisher evidence is
  available.

## Users

- Data maintainers need concise evidence to resolve source-drift issues.
- Adapter implementers need official URLs, license terms, cadence notes, and
  validation hints before changing source code.
- Reviewers need to see why a recommendation is in scope and what evidence
  supports it.
- Operators need issue comments that preserve history across repeated findings.

## Current Baseline

The current implementation already has:

- `source-register.v1.toml` as the reviewed catalog of scoped source/dataflow
  records.
- `au-kpis-scheduler source-location-audit` emitting deterministic findings.
- `SourceAuditStatus` and `SourceAuditSeverity` values including `drift`,
  `manual_review`, and `bot_filtered`.
- `tools/source_research.py generate` creating one JSON and one Markdown packet
  per actionable finding.
- `tools/source_research.py validate` enforcing the initial packet schema.
- `.github/workflows/source-research-review.yml` reusing retained deterministic
  audit artifacts, generating packets, uploading artifacts, and commenting
  summaries.

This PRD treats that as Phase 0. Agentic research must extend this baseline
without weakening its deterministic guarantees.

## Data We Have

### Source Register Record

Every research task must include the register record for the target dataflow:

- `source_id`
- `dataflow_id`
- `status`
- `owner_area`
- `canonical_url`
- `license`
- `attribution`
- `cadence`
- `review_frequency`
- `source_scope`
- `provenance_requirements`
- `validation_requirements`
- `expected_missing_reason`
- `retrieved_at`
- `reviewed_by`
- `reviewed_at`
- `manual_review_due_at`
- `replacement_candidate`
- `audit_policy`
- `additional_audit_policies`

The agent must treat `source_scope`, `status`, `expected_missing_reason`, and
`replacement_candidate` as governance constraints, not as optional context.

### Audit Finding

Every research task must include the deterministic finding:

- `source_id`
- `dataflow_id`
- `severity`
- `current_url`
- `latest_url`, when known
- `effective_url`, when available from the result table
- `http_status`
- `evidence`
- `recommendation`
- `generated_at`
- `register_version`

The agent must explain how its conclusion relates to the deterministic finding.
For example, a 403 on an official RBA page can support `bot_filtered`, while a
404 on a previously official page may support `moved` only if an official
replacement URL is found.

### Research Packet

The current packet has these fields:

- `schema_version`
- `artifact_id`
- `source_id`
- `dataflow_id`
- `current_url`
- `audit_evidence`
- `audit_severity`
- `register_status`
- `register_canonical_url`
- `allowed_domains`
- `required_evidence`
- `classification`
- `source_urls`
- `publisher_names`
- `retrieved_at`
- `license_evidence`
- `attribution_evidence`
- `cadence_evidence`
- `recommendation`
- `risk_notes`

Agentic mode must preserve these fields and may add fields only through a
schema-versioned change.

`artifact_id` must be unique and stable per audit finding. Canonical findings
may use the slugged `dataflow_id`; non-canonical or repeated findings must use a
stable suffix derived from the audited URL/rule identity rather than a run-local
ordinal so URL-specific evidence is not renumbered across weekly runs.

## Data We Need

Agentic research needs more structured evidence than the Phase 0 packet
currently captures.

### Source Identity Evidence

Required when classification is not `insufficient_evidence`:

- publisher name,
- official publisher domain,
- official page or API URL,
- page title or API descriptor,
- retrieved timestamp,
- whether the URL is the same as, redirects from, or replaces `current_url`,
- whether the URL matches the register `canonical_url` host or an allowed
  replacement host.

### Dataset Semantics Evidence

Required for `same_source`, `moved`, and `candidate_replacement`:

- dataset or publication name,
- measure, table, report, or artifact identifier,
- release period or latest period discovered,
- dimensions or filename patterns relevant to the adapter,
- whether the page/API exposes the same intended dataflow semantics,
- validation hints the adapter or scorecard can test.

### License And Attribution Evidence

Required for any source that may remain active, become active, or replace a
placeholder:

- license or terms URL,
- license name or publisher terms label,
- attribution text required by the publisher,
- restrictions on automated access, redistribution, commercial use, caching, or
  derived work,
- whether terms evidence came from the source page, a linked legal page, or a
  product page.

### Cadence And Freshness Evidence

Required when classification is `same_source`, `moved`, or
`candidate_replacement`:

- stated release cadence, if published,
- latest release date or latest period found,
- expected next release signal, if available,
- whether cadence matches the register,
- whether manual review due date should be moved earlier.

### Access And Bot-Filtering Evidence

Required when classification is `bot_filtered`:

- observed HTTP status or challenge pattern,
- whether browser access differs from plain HTTP access,
- official machine-readable fallback, if any,
- direct artifact URL pattern, if visible,
- whether allowlisting, API use, manual artifact capture, or publisher outreach
  is recommended.

### Scope And Replacement Evidence

Required when classification is `candidate_replacement`:

- which register field authorizes replacement, preferably `replacement_candidate`,
- why the candidate remains inside existing source scope,
- what source/dataflow would be retired or superseded,
- what config/doc changes would be needed,
- what must remain manual or visible-unscored until reviewed.

If the register does not contain a replacement path, the agent must classify the
finding as `insufficient_evidence` or `source_retired`; it must not invent new
scope.

## Classification Contract

Agentic research may return only these classifications:

- `same_source`: Current canonical URL still represents the intended source.
- `moved`: Official publisher evidence shows a new canonical URL for the same
  source/dataflow semantics.
- `bot_filtered`: Current source appears official but automation is challenged
  or blocked.
- `source_retired`: Official evidence says the source is discontinued, archived,
  superseded, or no longer updated.
- `candidate_replacement`: A scoped replacement candidate has enough primary
  evidence to consider a register/docs update.
- `insufficient_evidence`: The agent cannot prove one of the above.

The default classification must remain `insufficient_evidence` until the agent
proves a stronger classification with primary-source evidence.

## Required Output Schema

Agentic mode should produce `source-research.v2` JSON while continuing to render
Markdown for human review.

Required top-level fields:

```json
{
  "schema_version": "source-research.v2",
  "artifact_id": "rba.statistical_tables",
  "source_id": "rba",
  "dataflow_id": "rba.statistical_tables",
  "classification": "bot_filtered",
  "confidence": "medium",
  "current_url": "https://www.rba.gov.au/statistics/tables/",
  "register_canonical_url": "https://www.rba.gov.au/statistics/tables/",
  "retrieved_at": "2026-06-30T00:00:00Z",
  "evidence": [],
  "license_evidence": [],
  "cadence_evidence": [],
  "scope_assessment": {},
  "recommended_actions": [],
  "risk_notes": []
}
```

### Evidence Object

Each evidence item must include:

```json
{
  "url": "https://www.rba.gov.au/statistics/tables/",
  "publisher": "Reserve Bank of Australia",
  "retrieved_at": "2026-06-30T00:00:00Z",
  "evidence_type": "source_identity",
  "claim": "The RBA statistical tables index remains the official landing page.",
  "quoted_text": "Statistical Tables",
  "is_official": true,
  "domain_match": "allowed",
  "risk": "HTTP 403 from CI may require direct artifact fallback."
}
```

Allowed `evidence_type` values:

- `source_identity`
- `dataset_semantics`
- `license`
- `attribution`
- `cadence`
- `access`
- `replacement_scope`
- `retirement`

`quoted_text` must be short and source-specific. If quoting is unavailable, the
agent must provide a concise non-verbatim summary and mark the evidence as
weaker in `risk`.

### Scope Assessment Object

Required fields:

- `within_existing_scope`: boolean
- `scope_basis`: string
- `replacement_candidate_matched`: boolean
- `scoring_change_required`: boolean
- `adapter_change_required`: boolean
- `manual_review_required`: boolean

Any `scoring_change_required = true` result is review-only and must not produce
an automated config mutation.

### Confidence

Allowed confidence values:

- `high`: multiple primary-source evidence items support the classification.
- `medium`: one primary-source evidence item supports the classification, but
  cadence/license or access evidence is incomplete.
- `low`: evidence is plausible but incomplete; classification should usually be
  `insufficient_evidence`.

Validation must reject `high` confidence without at least two evidence items,
and must reject `candidate_replacement` without `scope_assessment.within_existing_scope = true`.

## Research Modes

### `packet_only`

Default for scheduled and manual runs.

- Reuses a retained deterministic audit artifact.
- Generates bounded research packets.
- Validates schema.
- Uploads artifacts.
- Comments a summary on the tracked issue.
- Does not call a model or browser.

### `agentic_manual`

Manual dispatch only.

- Requires an explicit `research_mode=agentic_manual` workflow input.
- Requires one `dataflow_id` or an explicit `all_findings=true` input.
- Requires a configured provider secret.
- Calls the agentic provider with a bounded prompt and tool policy.
- Writes `source-research.v2` JSON and Markdown.
- Validates outputs before posting issue comments.
- Never commits source changes.

### `agentic_scheduled_candidate`

Future mode, disabled until acceptance criteria are met.

- Runs only after repeated findings, for example two consecutive weekly runs.
- Uses strict time, cost, and dataflow-count limits.
- Posts issue comments only.
- Can be disabled globally by repository variable.

### `draft_pr`

Future manual mode.

- May open a draft PR only when requested manually.
- Allowed paths: `docs/**`, `crates/au-kpis-source-register/config/**`,
  source audit tests, and generated research fixtures.
- Disallowed paths without explicit human approval: adapters, scorecard config,
  loader code, migrations, and ingestion hot-path code.

## Agent Provider Interface

The provider should be an interchangeable command with a narrow contract:

```bash
tools/source_research_agent.py run \
  --packet target/source-research/rba.statistical_tables.json \
  --register crates/au-kpis-source-register/config/source-register.v1.toml \
  --out target/source-research/rba.statistical_tables.agent.json \
  --mode agentic_manual
```

Provider inputs:

- one packet JSON file,
- full register path,
- optional cached deterministic audit report,
- allowed domains,
- maximum URLs to inspect,
- maximum elapsed time,
- model/provider id,
- retrieval timestamp.

Provider outputs:

- one `source-research.v2` JSON file,
- one Markdown rendering,
- optional raw provider transcript retained as artifact only,
- no source-code edits.

Provider exit behavior:

- exit `0` only when JSON validates,
- exit non-zero on provider errors, malformed output, or guardrail violations,
- convert model unavailability into a non-blocking skipped result for manual
  issue comments, not a failing scheduled audit.

## Prompt Requirements

Every provider prompt must include:

- the exact source/dataflow id,
- register record,
- deterministic audit finding,
- current and canonical URLs,
- allowed domains,
- disallowed actions,
- classification enum,
- output JSON schema,
- evidence checklist,
- instruction to prefer official publisher sources,
- instruction to preserve uncertainty as `insufficient_evidence`,
- instruction not to recommend scope expansion unless authorized by
  `replacement_candidate`.

Prompts must not include secrets, production credentials, private source data,
or raw observation payloads.

## Guardrails

### Domain Guardrails

- Official publisher domains from `canonical_url` and
  `additional_audit_policies` are allowed by default.
- Replacement domains are allowed only when the register has
  `replacement_candidate` or a reviewed allowlist.
- Non-official domains may appear only in `risk_notes`; they cannot support
  `moved`, `same_source`, or `candidate_replacement`.

### Scope Guardrails

- The agent must not create a new `dataflow_id`.
- The agent must not mark `manual_pending`, `visible_unscored`, `coverage_gap`,
  or `placeholder` as scored.
- The agent must not recommend adapter ingestion changes unless the output says
  `manual_review_required = true`.
- If source semantics differ from the register, the output must be
  `candidate_replacement` or `insufficient_evidence`, not `same_source`.

### Mutation Guardrails

Scheduled and `agentic_manual` modes:

- may write artifacts under `target/source-research/`,
- may add an issue comment,
- must not commit or push.

`draft_pr` mode:

- must create a draft PR,
- must include a checklist of evidence,
- must keep changes within allowed paths,
- must leave scoring and adapter behavior unchanged unless explicitly approved.

### Cost And Reliability Guardrails

- Default maximum: 5 dataflows per manual run.
- Default maximum: 8 inspected URLs per dataflow.
- Default timeout: 5 minutes per dataflow.
- Provider failures should preserve Phase 0 packets.
- Repeated failures should add risk notes, not erase prior issue context.

## Workflow Requirements

### Manual Dispatch Inputs

Add workflow inputs:

- `research_mode`: `packet_only`, `agentic_manual`, or `draft_pr`
- `dataflow_id`: optional single dataflow id
- `all_findings`: boolean, default `false`
- `max_findings`: integer, default `5`
- `provider`: provider id, default repository-configured provider
- `dry_run_issue_comment`: boolean, default `false`

Validation:

- `agentic_manual` requires either `dataflow_id` or `all_findings=true`.
- `all_findings=true` requires `max_findings`.
- `draft_pr` requires `dataflow_id`.
- `packet_only` remains the only scheduled mode.

### Artifact Layout

For each dataflow:

```text
target/source-research/<dataflow_id>.json
target/source-research/<dataflow_id>.md
target/source-research/<dataflow_id>.agent.json
target/source-research/<dataflow_id>.agent.md
```

In the concrete file paths, `<dataflow_id>` means the unique `artifact_id`
derived from the dataflow id plus a suffix when multiple findings exist for the
same dataflow.

Summary files:

```text
target/source-research/summary.json
target/source-research/comment.md
target/source-research/agent-summary.json
target/source-research/agent-comment.md
```

Provider transcripts, if retained, must be stored under:

```text
target/source-research/transcripts/
```

They must not be committed.

### Issue Comment Behavior

Agentic comments must include:

- dataflow id,
- classification,
- confidence,
- recommended action,
- top evidence URLs,
- unresolved risk notes,
- artifact name and workflow run URL.

Comments must append to the tracked source-drift issue. They must not replace
the deterministic audit checklist or prior research history.

## Validation Rules

`tools/source_research.py validate` or a successor validator must enforce:

- allowed enum values,
- required non-empty strings,
- RFC 3339 timestamps,
- official-domain evidence for `same_source`, `moved`, and
  `candidate_replacement`,
- `candidate_replacement` requires register `replacement_candidate` or reviewed
  allowlist,
- `source_retired` requires official retirement/archive/supersession evidence,
- `bot_filtered` requires access evidence,
- high confidence requires at least two primary-source evidence items,
- license evidence is required for active or replacement recommendations,
- cadence evidence is required for active or replacement recommendations,
- no source-code mutation paths appear in scheduled artifacts,
- no scoring promotion is recommended without manual review.

Validation failures must block issue-comment posting for the agentic output, but
must still upload artifacts for debugging.

## Product Requirements

### PR-1: Agentic Research Schema V2

Define and validate `source-research.v2`.

Acceptance criteria:

- V1 packet generation remains backward compatible.
- V2 validates structured evidence, scope assessment, confidence, and
  recommendations.
- Invalid model output fails validation with field-specific errors.
- Fixture tests cover each classification.

### PR-2: Manual Agentic Provider Mode

Add manual-dispatch agentic mode.

Acceptance criteria:

- `packet_only` remains scheduled default.
- `agentic_manual` runs only from workflow dispatch.
- Missing provider secret skips cleanly with a clear artifact and issue note.
- Provider writes only research artifacts.
- Valid agentic output appends an issue comment.

### PR-3: Guardrail Enforcement

Enforce domain, scope, mutation, and scoring guardrails.

Acceptance criteria:

- Non-official source recommendations cannot validate as `moved`.
- Replacement recommendations without a register replacement path fail.
- Output that promotes manual/visible-unscored inputs to scored status fails.
- Output that proposes adapter or scorecard mutation in scheduled/manual
  research mode fails.

### PR-4: Research Quality Controls

Add quality checks for evidence usefulness.

Acceptance criteria:

- Every non-`insufficient_evidence` classification has at least one primary
  source evidence item.
- Every `same_source`, `moved`, or `candidate_replacement` result has license
  and cadence evidence or is downgraded to `insufficient_evidence`.
- Every result includes risk notes when evidence is incomplete.
- Repeated findings preserve history in issue comments.

### PR-5: Optional Draft PR Mode

Add a manually approved draft PR path after manual agentic mode is stable.

Acceptance criteria:

- Draft PRs are never created by scheduled runs.
- Draft PRs are limited to approved paths.
- PR bodies include evidence checklist and validation output.
- Any adapter or scorecard change requires separate human approval.

## Test Plan

Unit tests:

- V2 schema validator accepts one valid artifact per classification.
- Validator rejects missing evidence, invalid confidence, bad timestamps, and
  unknown classifications.
- Validator rejects non-official `moved` evidence.
- Validator rejects replacement without register authorization.
- Validator rejects scoring promotion or disallowed mutation recommendation.
- Prompt builder includes required fields and excludes secrets.

Fixture tests:

- RBA bot-filtered finding produces valid `bot_filtered` research.
- Victoria Planning access challenge produces `bot_filtered` or
  `insufficient_evidence` based on evidence.
- World Bank null latest values remains `manual_review` and cannot become
  `same_source` without value evidence.
- NAIC transient failure with reachable official page produces `same_source`
  only with source identity, license, and cadence evidence.
- Placeholder replacement without register authorization fails.

Workflow tests:

- `packet_only` scheduled run does not call provider.
- `agentic_manual` requires explicit inputs.
- Provider unavailable path uploads artifacts and does not fail deterministic
  audit.
- Valid agent output renders an issue comment.
- Invalid agent output blocks comment posting and uploads diagnostics.

Integration tests:

- Run provider interface against mocked search/browser results.
- Run validator against generated artifacts from static fixture packets.
- Run issue-comment renderer against prior-history fixture comments.

Live smoke:

- Manual, non-blocking run for one dataflow with retained artifacts.
- No source code mutation.

## Rollout Plan

### Phase 1: Schema And Validator

- Add `source-research.v2` schema.
- Add validator and fixture tests.
- Keep workflow in `packet_only`.

Exit criteria:

- V2 validation covers every classification and guardrail.
- Existing packet-only workflow is unchanged.

### Phase 2: Mock Provider

- Add provider interface with mocked static responses.
- Add prompt builder tests.
- Add workflow dry-run path that uses mock output.

Exit criteria:

- CI can prove provider orchestration without external services.

### Phase 3: Manual Real Provider

- Add provider secret boundary.
- Enable `agentic_manual` for one dataflow per dispatch by default.
- Append validated comments to tracked issue.

Exit criteria:

- Three manual runs produce useful evidence without guardrail violations.

### Phase 4: Limited Multi-Finding Manual Runs

- Allow `all_findings=true` with `max_findings`.
- Add cost and timeout reporting.
- Preserve per-dataflow failure isolation.

Exit criteria:

- One failed provider call does not prevent other valid artifacts from posting.

### Phase 5: Scheduled Candidate Or Draft PR Pilot

- Choose one: scheduled candidate comments or manual draft PR generation.
- Keep rollout behind repository variable.
- Require evidence quality and guardrail test coverage before enabling.

Exit criteria:

- Maintainers can resolve source-drift issues faster without unreviewed source
  behavior changes.

## Success Metrics

- At least 90% of actionable drift findings get a research packet.
- At least 70% of manual agentic runs produce a validated non-empty evidence
  artifact.
- Zero validated artifacts recommend out-of-scope source expansion.
- Zero scheduled runs mutate repository files.
- Median maintainer time to identify next action from a drift issue is under 15
  minutes.
- Repeated findings preserve prior research history and current evidence.

## Risks And Mitigations

- Risk: The agent recommends plausible but non-official sources.  
  Mitigation: official-domain validation and `insufficient_evidence` default.
- Risk: The agent expands product scope.  
  Mitigation: replacement authorization gate and scope assessment.
- Risk: Model availability creates CI flake.  
  Mitigation: scheduled mode stays packet-only; manual provider failures are
  non-blocking for deterministic audit artifacts.
- Risk: Evidence quotes exceed safe limits or include irrelevant content.  
  Mitigation: short source-specific snippets and artifact validation.
- Risk: Cost grows with repeated findings.  
  Mitigation: per-run dataflow, URL, timeout, and mode limits.
- Risk: Draft PR mode changes behavior prematurely.  
  Mitigation: manual-only, draft-only, path-limited, and no adapter/scorecard
  mutation without approval.

## Open Questions

- Which provider should implement the first real agentic mode?
- Should provider transcripts be retained for 30 days or shorter?
- Should allowed replacement domains be stored in the register or a separate
  policy file?
- Should high-confidence `moved` findings generate draft register changes
  automatically, or only issue comments?
- How should maintainer feedback on research quality be captured and used to
  tune prompts?

## Definition Of Done

- This PRD is linked from the broader source drift docs.
- V2 schema and validator are implemented with tests.
- Manual agentic workflow mode exists behind explicit dispatch inputs.
- Provider output is schema-validated before issue comments.
- Agentic mode cannot mutate adapters, scorecard config, or ingestion behavior
  in scheduled/manual research mode.
- Draft PR mode, if added, is manual, draft-only, and path-limited.
