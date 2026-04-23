## Ingestion And Loader Review Guide

Applies to:
- `crates/au-kpis-loader/**`
- `crates/au-kpis-ingestion-core/**`
- `crates/au-kpis-queue/**`
- `crates/bins/au-kpis-ingestion/**`
- `crates/bins/au-kpis-scheduler/**`

Primary review goal:
- protect idempotent, auditable, stream-oriented ingestion

Focus on:
- COPY-based batching
- series upsert vs observation insert boundaries
- retry behavior and backpressure
- revision ordering and idempotency
- queue abstraction boundaries

Ask:
- does this preserve raw artifact provenance and auditability?
- does this leak DB concerns into adapters or orchestration layers?
- does this materialize large datasets where streaming should be used?
- are integration or benchmark tests missing for hot-path behavior?
