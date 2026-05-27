# Adapter Versioning

Source formats change independently of this service. Adapters must keep old
artifacts re-ingestable while detecting unexpected schema changes before rows
are silently loaded.

## Parser Versions

When a source changes shape, keep parser implementations side by side:

```rust
fn parse_v1(...) -> Result<Rows, AdapterError> { ... }
fn parse_v2(...) -> Result<Rows, AdapterError> { ... }
```

Use `ParserVersion` plus `select_parser_version` from `au-kpis-adapter` to
route by artifact date. Ranges are inclusive at the start and exclusive at the
end, so adjacent versions do not overlap:

```rust
let versions = [
    ParserVersion::new("parse_v1", ArtifactDateRange::before(v2_start)),
    ParserVersion::new("parse_v2", ArtifactDateRange::from(v2_start)),
];
let version = select_parser_version(&versions, artifact_date)?;
```

The selected version name must be attached to parser diagnostics and schema
hash checks. Do not delete an old parser while artifacts in object storage still
need it for deterministic re-ingest.

## Schema Hash Drift

Each parser version should define committed schema hashes for the table, sheet,
or structure it accepts. After computing the observed hash, call
`validate_schema_hash`. A mismatch returns a structured
`AdapterError::SchemaHashDrift`, logs the expected and observed hashes, and is
classified as permanent format drift.

Schema hash keys are source-specific, but should be stable and human-readable,
for example `bp4-agency-resourcing` or `apra-performance-sheet`.

## Alerting

The ingestion worker turns `AdapterError::SchemaHashDrift` into
`au_kpis_schema_hash_drifts_total{source, dataflow}`. Prometheus alert
`AuKpisSchemaHashDrift` pages immediately when the counter increases.

On alert:

1. Pause or leave paused the affected source/dataflow ingestion.
2. Review the raw artifact and parser diagnostics.
3. If the source changed intentionally, add or update the parser version and
   commit the new schema hash with fixture and `insta` snapshot coverage.
4. Re-ingest old and new fixture artifacts to prove both parser versions remain
   deterministic.

Model or PDF sidecar fallback may assist table extraction, but final economic
observations still go through the same Rust parser and schema validation path.
