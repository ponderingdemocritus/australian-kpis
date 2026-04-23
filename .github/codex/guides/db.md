## DB And Migration Review Guide

Applies to:
- `crates/au-kpis-db/**`
- `infra/migrations/**`

Primary review goal:
- protect correctness and performance of Timescale-backed storage

Focus on:
- hypertable correctness
- reversible migrations
- `observations_latest` correctness
- query safety and `sqlx::query!` usage
- revision history preservation

Ask:
- could this break revision semantics or latest-value reads?
- is there a destructive schema change without a spec/deprecation note?
- should this have a real integration test against Postgres/Timescale?
- does this create a likely performance regression on hot queries?
