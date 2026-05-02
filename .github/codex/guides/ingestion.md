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
- graceful shutdown semantics: cancellation stops admitting new work, but already-produced artifacts/observations/audit records must drain unless shutdown grace expires
- discovery/job correlation propagation: job id, trace parent, source id, dataflow id, and artifact id must survive every stage handoff and appear in logs/spans/audit rows where relevant
- revision ordering and idempotency
- queue abstraction boundaries

Ask:
- does this preserve raw artifact provenance and auditability?
- can cancellation under bounded-channel backpressure silently drop already-produced work?
- does each work item retain discovery correlation through fetch, parse, load, and parse-error recording?
- does this leak DB concerns into adapters or orchestration layers?
- does this materialize large datasets where streaming should be used?
- are integration or benchmark tests missing for hot-path behavior?
