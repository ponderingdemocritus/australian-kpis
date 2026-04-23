## Adapter Review Guide

Applies to:
- `crates/au-kpis-adapter/**`
- `crates/adapters/**`
- `crates/au-kpis-pdf-client/**`

Primary review goal:
- protect source-specific parser correctness without breaking abstraction boundaries

Focus on:
- parser correctness
- snapshot/property coverage
- adapter rate-limit and retry behavior
- adapter output staying persistence-agnostic
- PDF/network safety for difficult sources

Ask:
- does the adapter still emit domain parse results instead of touching storage concerns?
- is this parser change missing golden-file or property coverage?
- could this lose attribution, source artifact linkage, or format-drift observability?
- are Python/http timeout expectations still satisfied where relevant?
