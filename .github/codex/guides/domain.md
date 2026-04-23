## Domain Review Guide

Applies to:
- `crates/au-kpis-domain/**`
- shared domain types used across crates

Primary review goal:
- protect the canonical data model and type boundaries

Focus on:
- accidental weakening of typed IDs and `SeriesKey`
- mixing transport/storage concerns into pure domain types
- changes that blur `Series` vs `Observation`
- serialization/schema changes that ripple into OpenAPI or SDKs

Ask:
- does this preserve the `series` and `observations` split?
- does this keep `SeriesKey` as a type-safe boundary rather than a plain string?
- does this change require updated tests for serialization or schema derivation?
