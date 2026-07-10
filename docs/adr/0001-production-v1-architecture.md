# ADR 0001: Production v1 architecture

- Status: Accepted
- Date: 2026-07-10
- Owners: Platform, Data, API, Web, Product/Methodology
- Supersedes: Fly.io phase deployment and Railway-hosted state in `Spec.md`

## Context

The repository has working adapters, an observation API, APS calculation,
web UI, and an initial Railway project definition. The earlier architecture
still allowed process-local ingestion handoffs, startup migrations, dynamic APS
responses, plaintext webhook secrets, and stateful services provisioned inside
Railway. Those choices cannot meet the production-v1 recovery, auditability,
security, and immutable-promotion requirements.

Production v1 is gate-based. It must serve the public API and APS read model
while continuously ingesting 20 launch dataflows, survive process and cache
loss, restore managed data inside the declared RPO/RTO, and expose no partially
published generation.

## Decision

### State and compute

Railway runs stateless API, web/BFF, PDF extraction, ingestion, scheduler,
webhook, and OTEL services in Singapore. Durable state lives in Timescale Cloud
and Cloudflare R2 in the same broad geography. Railway Redis holds disposable
cache and rate-limit state only. Public traffic enters through Cloudflare and
must carry authenticated origin metadata.

This split makes Redis and process loss degradable while keeping database,
artifact, queue, schedule, generation, APS, and webhook state recoverable.

### Ingestion correctness

Postgres owns schedule occurrences and every stage transition. Discovery writes
canonical `discovered_work`; Fetch writes a content-addressed R2 artifact and
fetch record; Parse writes an auditable generation and typed unlogged staging;
Load publishes observations, audit records, and webhook outbox rows in one
transaction under a per-dataflow advisory lock.

No stage depends on an in-memory payload from its predecessor. A lost unlogged
stage is reconstructed from the immutable artifact. Generation IDs and stage
digests make replay, parser upgrades, reconciliation, and provenance explicit.

### Read models

`GET /v1/observations` remains the single observation query. Bounded first-page
JSON may be cached; cursors and streaming formats are not cached. Cursor
signatures bind the canonical query and dataflow generation watermark.

APS becomes an immutable persisted read model. The daily Sydney publication job
writes either a numeric published snapshot after coverage thresholds pass or an
insufficient-coverage snapshot with a null score. Corrections append revisions;
history can select original as-published or latest revisions.

### Subscription security

Subscription ownership and read/write scopes are enforced by API key. Endpoint
verification precedes activation. DNS and destination validation is repeated
and pinned on every request. Signing secrets are returned once and stored only
as AES-256-GCM ciphertext with versioned AAD. Webhook work uses fenced leases,
stable event IDs, bounded concurrency, defined retry classification, and an
automatic pause threshold.

### Delivery authority

GitHub Actions is the only deployment authority. It builds each image once,
produces and scans SBOMs, signs immutable GHCR digests, applies expand migrations
with DDL-only credentials, promotes the same digests through staging and
production, and rolls application digests back when automated gates fail.
Railway repository auto-deploy is disabled.

## Source ownership contract

The versioned source register is the launch manifest. Each active entry must
declare:

- Accountable `data` owner role.
- Five-field discovery cron and IANA timezone.
- Request timeout, steady-state rate, and burst.
- Soft-stale and hard-expired thresholds.
- Named adapter range rule and maximum generation series cardinality.
- `allow_partial_rows = false`.
- Repository-relative representative fixture or reviewed snapshot.

Register parsing fails closed. Scheduler, source APIs, launch-readiness checks,
and certification reports consume the same configuration.

## Consequences

- Three additive migration pairs are required before orchestration is enabled.
- Existing observations need an explicit legacy published generation before the
  generation foreign key becomes non-null.
- Existing dynamic APS endpoints and browser-side calculation are replaced by
  persisted snapshots and generated SDK validation.
- Existing webhook rows require forward migration to encrypted versioned
  secrets and pending verification.
- Railway project IaC must stop creating Postgres and artifact-bucket services.
- Deployment requires protected environments and external cloud credentials.
- Production rollback is application-first and schema-forward. After managed
  writes begin, state recovery uses Timescale PITR and R2 replay.

## Alternatives rejected

- Process-local handoffs: cannot resume safely after worker loss.
- Redis queues or schedules: Redis is intentionally disposable.
- One Railway stateful stack: does not satisfy managed HA/PITR and R2 recovery
  contracts.
- Dynamic APS history: cannot preserve original publication and corrections.
- Plaintext or one-way-hashed webhook secrets: hashing cannot sign deliveries;
  plaintext violates the one-time secret contract.
- Reverse migrations after production data acceptance: can destroy or detach
  durable audit state.

## Verification

Acceptance requires migration up/down/up tests, scheduler failover, stage replay,
atomic publication kill tests, APS idempotency/correction tests, subscription
ownership and SSRF tests, encrypted-secret checks, browser-to-BFF end-to-end
tests, one-million-row streaming memory checks, scale/chaos/security/restore
reports, alert fire-and-clear drills, and a seven-day two-replica staging soak.

The detailed thresholds, topology, rollout, rollback, and launch definition are
normative in `Spec.md` under "Production v1 release contract".
