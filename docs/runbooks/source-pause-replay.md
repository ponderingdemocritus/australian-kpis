# Source Pause And Replay

Pause a dataflow for schema drift, ambiguous provenance, repeated permanent
parse failure, digest mismatch, or upstream terms/location uncertainty. Do not
pause unrelated dataflows from the same source.

```bash
au-kpis-cli source pause \
  --dataflow treasury.budget_papers \
  --actor oncall@example.test \
  --reason "schema hash changed; incident INC-123"
```

Confirm its durable schedule is disabled, no new work is admitted, and already
leased work reaches a terminal audited state. Preserve raw R2 objects.

Inspect the rejected generation and artifact provenance:

```bash
au-kpis-cli generation inspect --id GENERATION_UUID
```

After reviewing the raw object, fixture, parser change, licence, and source
mapping, create a new audited parser generation. Never edit a published
generation or observation in place.

```bash
au-kpis-cli artifact reparse \
  --artifact-id SHA256_HEX \
  --dataflow treasury.budget_papers \
  --parser-version treasury-budget-v2 \
  --actor data-reviewer@example.test \
  --reason "reviewed schema update PR #123"
```

Retry only jobs whose failure is understood and corrected:

```bash
au-kpis-cli queue retry-dlq \
  --job-id 12345 \
  --actor oncall@example.test \
  --reason "upstream throttle ended"
```

Resume after the new generation publishes atomically, data quality passes, and
freshness/coverage recover:

```bash
au-kpis-cli source resume \
  --dataflow treasury.budget_papers \
  --actor data-reviewer@example.test \
  --reason "generation verified and quality checks passed"
```

Every command writes `operator_audit_log`. Attach command JSON, generation ID,
artifact digest, data-quality report, and alert-clear time to the incident.

## Reviewed Manual Inputs

Only the two governed launch blockers accept `manual-input load`. The input is
canonical JSON; dimensions are sorted during serialization and the exact bytes
become the content-addressed R2 artifact:

```json
{
  "measure_id": "manual.percent",
  "unit": "percent",
  "observations": [{
    "dimensions": {"category": "productive"},
    "time": "2026-06-30T00:00:00Z",
    "time_precision": "quarter",
    "value": 42.5,
    "status": "normal",
    "attributes": {"method": "reviewed mapping"}
  }]
}
```

Run the command with the governed licence, HTTPS evidence URL, retrieval date,
reviewer role/date, evidence notes, actor, and reason. It creates or validates
the governed manual catalog row, writes the immutable artifact, stages typed
rows under a new generation, records `manual_input_reviews`, and enqueues the
normal atomic Load stage. A successful command means `pending_load`, not
published; wait for the generation and launch-readiness gates.

```bash
au-kpis-cli manual-input load --file reviewed-input.json \
  --dataflow apra.super_asset_allocation \
  --source-url https://apra.gov.au/reviewed-release \
  --license "Creative Commons Attribution 3.0 Australia Licence" \
  --retrieved-at 2026-07-01 --reviewed-at 2026-07-02 \
  --reviewer-role product-methodology \
  --evidence-notes "Approved category mapping MR-12" \
  --actor reviewer@example.test --reason "load reviewed launch blocker"
```
