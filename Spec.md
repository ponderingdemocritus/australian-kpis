# Australian Economic Data API — Technical Specification

| | |
|---|---|
| **Document** | `Spec.md` |
| **Version** | `v0.1.3` |
| **Status** | Approved |
| **Last updated** | 2026-05-01 |
| **Owner** | Platform team |
| **Audience** | Engineers, data partners, SDK consumers, operators |

> **Amendment process:** material changes flow through PRs against this file. Bump the version in the header and append a note to the `Changelog` section at the bottom. Work is tracked as GitHub issues under milestones M1–M5; see the repository's Issues tab for the live work breakdown.

---

## Table of contents

1. [Context](#context)
2. [Confirmed decisions](#confirmed-decisions)
3. [Stack (locked)](#stack-locked)
4. [Monorepo layout](#monorepo-layout)
5. [Async architecture (baked-in best practices)](#async-architecture-baked-in-best-practices)
6. [Data model (SDMX-inspired)](#data-model-sdmx-inspired)
7. [Source adapters](#source-adapters)
8. [Ingestion pipeline](#ingestion-pipeline)
9. [PDF extractor service (Python)](#pdf-extractor-service-python)
10. [API surface](#api-surface)
11. [TypeScript SDK](#typescript-sdk)
12. [Reference client (apps/web)](#reference-client-appsweb)
13. [Observability](#observability)
14. [Testing strategy](#testing-strategy)
15. [CI/CD pipeline](#cicd-pipeline)
16. [Benchmarking](#benchmarking)
17. [Security posture](#security-posture)
18. [Data licensing and attribution](#data-licensing-and-attribution)
19. [Deployment](#deployment)
20. [Phased rollout](#phased-rollout)
21. [Critical files/modules to create (Phase 1 + 2)](#critical-filesmodules-to-create-phase-1--2)
22. [Verification plan](#verification-plan)
23. [Review findings — best practices + abstraction checks](#review-findings--best-practices--abstraction-checks)
24. [Decisions log](#decisions-log)
25. [Glossary](#glossary)
26. [References](#references)
27. [Changelog](#changelog)

---

## Context

Build a data platform that aggregates Australian economic indicators from many public sources (ABS, RBA, APRA, Treasury, ASX, AEMO, state governments, ABR/ASIC) and exposes them through a consistent API, a typed TypeScript SDK, and a reference web client.

**Problem:** Australian economic data is fragmented across dozens of publishers using incompatible formats (SDMX-JSON, XLS, CSV, PDF with tables), inconsistent dimensional models, and unpredictable release cadences. Consumers (analysts, fintechs, researchers, dashboards) currently scrape or hand-roll integrations. There is no "one Bloomberg-style API" for Australian public data.

**Outcome:** A single SDK call

```typescript
await client.observations.list({
  dataflow: 'abs.cpi',
  dimensions: { region: 'AUS', measure: 'All groups CPI' },
  since: '2010-01-01',
})
```

returns clean, validated, SDMX-compliant time series regardless of upstream source format. Raw source artifacts are preserved in S3 for re-ingest and audit.

**Scale target (Year 1):**
- ~50M observations, ~100 dataflows, billions by Year 3
- Daily ingestion most series; high-frequency (AEMO) sub-hourly
- <200ms p95 API read latency for single-series queries
- Parquet/Arrow streaming for bulk data consumers
- 99.9% uptime, zero data loss

---

## Confirmed decisions

- Hosting: **Fly.io** phase 1-3; re-evaluate at scale.
- DB: **Timescale Cloud** (managed — backups, HA, compression policies).
- Licensing model: **Free tier** — generous default rate limits, API keys for abuse prevention + attribution.
- SDK: **TypeScript first**; Python/Go SDKs later via same OpenAPI codegen.
- First dataflow: **ABS CPI** (higher public demand than Labour Force; monthly + quarterly releases).
- Raw artifacts: **retained in S3/R2 indefinitely** — lifecycle policy moves >1yr objects to cold storage. Streaming uploads land first in `artifacts-staging/<uuid>` and are copied to `artifacts/<sha256>` once the hash is known; a second lifecycle rule expires `artifacts-staging/` after 7 days so any rare delete-failure leak from the storage crate is bounded. The declarative policy lives in `infra/r2/lifecycle.json`.
- Rust: **2024 edition, MSRV 1.85**.
- Migrations: **`sqlx migrate`** (sync-at-startup).
- PDF extraction: **deterministic first, model-assisted fallback**. Open-source
  document models are allowed, but only as pinned local sidecar backends whose
  output passes the same source-specific validation as non-model extraction.

## Stack (locked)

| Layer | Choice | Reason |
|---|---|---|
| **API server** | **Rust 2024 edition, latest stable axum + tokio 1** | Hot path, 24/7, cost + latency sensitive; streams Parquet natively |
| **Ingestion + workers** | **Rust (tokio, reqwest, calamine, polars)** | Flat memory at millions of rows; shared types with API; `polars`/`arrow-rs` advantage |
| **PDF extractor** | **Python FastAPI sidecar** with strategy backends (`pdfplumber` + `camelot` baseline; optional pinned local Docling/VLM fallback) | Deterministic extraction first; model assistance only when validation/format-drift requires it |
| **SDK** | **TypeScript**, generated from OpenAPI (`openapi-typescript` + orval + hand-written ergonomic wrapper) | Consumer language; typed; tree-shakeable; Bun/Node/browser/Deno compatible |
| **Client** | **TypeScript (Vite + React + TanStack Query + shadcn/ui)** | Reference app demonstrating SDK |
| **DB** | **Timescale Cloud (Postgres 16 + TimescaleDB)** | Hypertables, continuous aggregates, compression; managed ops |
| **Queue** | **Postgres-backed `Queue` trait implementation** | One fewer infra piece; transactional dequeue; retries + cron registration built in; trait lets us swap to `apalis` later |
| **Cache + rate limit** | **Redis (`fred`)** | Token bucket, ETags, hot-path caching |
| **Blob store** | **Cloudflare R2** via `object_store` crate | Egress-free; artifacts retained indefinitely; lifecycle → cold after 1y |
| **OpenAPI gen** | **`utoipa` + `utoipa-axum`** | Compile-time derivation, stays in sync with handlers |
| **Observability** | **`tracing` + OTLP → Grafana/Tempo/Loki/Prometheus** | OpenTelemetry-native |
| **Errors** | **`thiserror` in libs, `anyhow` in bins, `ApiError` + RFC 7807 in API** | Idiomatic split |
| **Config** | **`figment`** (layered: defaults → TOML → env) | Ergonomic, type-safe |
| **Testing** | **`cargo test`, `insta` (snapshot), `testcontainers` (integration), `criterion` (bench)** | |
| **Lint/format** | **`rustfmt` + `clippy` (deny warnings)** + **`cargo deny`** + **`cargo audit`** | |
| **Monorepo** | **Cargo workspace** for Rust, **pnpm workspaces + Turborepo** for TS | Polyglot root |
| **CI** | **GitHub Actions + `sccache`** | Cached Rust builds |
| **Migrations** | **`sqlx migrate`** — sync-at-startup, checked-in SQL files | Simple, transparent |

---

## Monorepo layout

```
australian-kpis/
├── Cargo.toml                              # Workspace root
├── rust-toolchain.toml                     # Pin Rust version
├── .cargo/config.toml                      # sccache, target flags
├── package.json                            # TS workspace root
├── pnpm-workspace.yaml
├── turbo.json
├── biome.json
├── crates/                                 # Rust workspace members
│   ├── au-kpis-domain/                     # Pure domain types (no async/tokio)
│   ├── au-kpis-error/                      # Shared error types (thiserror)
│   ├── au-kpis-config/                     # figment-based config loader
│   ├── au-kpis-telemetry/                  # tracing + OTel setup
│   ├── au-kpis-schema/                     # JSON Schema / OpenAPI derivation helpers
│   ├── au-kpis-db/                         # sqlx + Timescale queries
│   ├── au-kpis-storage/                    # object_store wrapper for S3/R2
│   ├── au-kpis-cache/                      # Redis cache abstraction
│   ├── au-kpis-queue/                      # Postgres queue: job types, scheduling
│   ├── au-kpis-auth/                       # API key validation, Redis rate limiter
│   ├── au-kpis-pdf-client/                 # HTTP client for Python sidecar
│   ├── au-kpis-adapter/                    # Adapter trait + base helpers (discover/fetch/parse)
│   ├── adapters/                           # Adapter implementation crates
│   │   ├── abs/                            # package `au-kpis-adapter-abs` (SDMX-JSON)
│   │   ├── rba/                            # package `au-kpis-adapter-rba` (XLS/CSV)
│   │   ├── apra/                           # package `au-kpis-adapter-apra` (quarterly XLS)
│   │   ├── asx/                            # package `au-kpis-adapter-asx` (announcements + EOD)
│   │   ├── treasury/                       # package `au-kpis-adapter-treasury` (PDF via sidecar)
│   │   ├── aemo/                           # package `au-kpis-adapter-aemo` (high-frequency CSV)
│   │   └── state-budgets/                  # package `au-kpis-adapter-state-budgets` (per-state PDF)
│   ├── au-kpis-loader/                     # Observation upsert, revision tracking
│   ├── au-kpis-ingestion-core/             # Orchestration: discover→fetch→parse→load
│   ├── au-kpis-api-http/                   # axum routes + handlers (library)
│   ├── au-kpis-openapi/                    # OpenAPI spec emitter (re-exports utoipa)
│   └── bins/
│       ├── au-kpis-api/                    # API server binary
│       ├── au-kpis-ingestion/              # Ingestion worker binary
│       ├── au-kpis-scheduler/              # Discovery cron binary
│       └── au-kpis-cli/                    # Admin CLI (migrations, backfills)
├── apps/                                   # TS apps
│   ├── pdf-extractor/                      # Python FastAPI (in apps/ for proximity)
│   └── web/                                # Vite + React reference client
├── packages/                               # TS packages
│   ├── sdk/                                # @au-kpis/sdk (published to npm)
│   ├── sdk-generated/                      # auto-generated from openapi.json
│   └── testing/                            # Shared TS test utilities
├── infra/
│   ├── compose/docker-compose.yml          # Local: Postgres+Timescale, Redis, Minio, PDF sidecar
│   ├── docker/                             # Dockerfiles per binary
│   ├── migrations/                         # sqlx migrations (Timescale-aware)
│   └── k8s/                                # Optional helm charts (phase 5+)
└── .github/workflows/                      # ci.yml, release.yml, sdk-publish.yml
```

### Dependency flow (Rust crates)

No cycles. Strict layering enforced via `cargo-deny`:

```
Layer 0 (pure):  domain, error
Layer 1:         config, schema, telemetry
Layer 2:         db, storage, cache, queue, auth, pdf-client
Layer 3:         adapter (trait)
Layer 4:         adapters/* (each)
Layer 5:         loader, ingestion-core, openapi, api-http
Layer 6 (bins):  au-kpis-api, au-kpis-ingestion, au-kpis-scheduler, au-kpis-cli
```

Each adapter is its own crate so you can add source 15 without touching sources 1–14. Adapters don't depend on `db` or `loader` — they only emit observations.

### Workspace Cargo.toml skeleton

```toml
[workspace]
resolver = "2"
members = ["crates/*", "crates/adapters/*", "crates/bins/*"]

[workspace.package]
edition = "2024"
rust-version = "1.85"
license = "Apache-2.0"

[workspace.dependencies]
# Internal crates (referenced by path; versions unnecessary inside workspace)
au-kpis-domain        = { path = "crates/au-kpis-domain" }
au-kpis-error         = { path = "crates/au-kpis-error" }
au-kpis-config        = { path = "crates/au-kpis-config" }
au-kpis-telemetry     = { path = "crates/au-kpis-telemetry" }
au-kpis-schema        = { path = "crates/au-kpis-schema" }
au-kpis-db            = { path = "crates/au-kpis-db" }
au-kpis-storage       = { path = "crates/au-kpis-storage" }
au-kpis-cache         = { path = "crates/au-kpis-cache" }
au-kpis-queue         = { path = "crates/au-kpis-queue" }
au-kpis-auth          = { path = "crates/au-kpis-auth" }
au-kpis-pdf-client    = { path = "crates/au-kpis-pdf-client" }
au-kpis-adapter       = { path = "crates/au-kpis-adapter" }
au-kpis-loader        = { path = "crates/au-kpis-loader" }
au-kpis-ingestion-core = { path = "crates/au-kpis-ingestion-core" }
au-kpis-api-http      = { path = "crates/au-kpis-api-http" }
au-kpis-openapi       = { path = "crates/au-kpis-openapi" }

# External
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal", "sync", "time", "tracing"] }
axum = { version = "0.7", features = ["macros"] }
tower = "0.5"
tower-http = { version = "0.6", features = ["trace", "cors", "compression-gzip", "timeout"] }
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "chrono", "uuid", "json", "macros"] }
reqwest = { version = "0.12", features = ["json", "stream", "gzip", "brotli", "rustls-tls"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
anyhow = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
tracing-opentelemetry = "0.29"
opentelemetry = "0.27"
opentelemetry-otlp = "0.27"
utoipa = { version = "5", features = ["axum_extras", "chrono", "uuid"] }
utoipa-axum = "0.1"
object_store = { version = "0.11", features = ["aws"] }
calamine = "0.26"
polars = { version = "0.44", features = ["lazy", "parquet", "csv", "json"] }
arrow = "53"
parquet = "53"
fred = { version = "9", features = ["enable-rustls"] }
figment = { version = "0.10", features = ["toml", "env"] }
async-trait = "0.1"
futures = "0.3"
tokio-util = "0.7"
tokio-stream = "0.1"
uuid = { version = "1", features = ["v4", "v7", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
bytes = "1"
insta = { version = "1", features = ["json", "yaml"] }
testcontainers = "0.23"
```

---

## Async architecture (baked-in best practices)

### Runtime

- **Multi-thread tokio** with worker count = `num_cpus::get()` by default, configurable via env.
- API binary: `#[tokio::main(flavor = "multi_thread")]` with explicit worker threads.
- Ingestion binary: same, plus **dedicated blocking pool** via `tokio::task::spawn_blocking` for CPU-heavy parse work (XLS, large JSON, Polars transforms).
- **Never** call `std::thread::sleep` or blocking I/O from async contexts — `clippy::await_holding_lock` + `tokio::task::block_in_place` rules enforced.

### Shared state (axum)

```rust
// crates/au-kpis-api-http/src/state.rs
#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,                  // sqlx pool — internally Arc
    pub cache: Arc<CacheClient>,     // fred Redis client
    pub config: Arc<AppConfig>,      // read-only after startup
    pub telemetry: Arc<Telemetry>,
    pub shutdown: CancellationToken, // propagated to all handlers + spawned tasks
}
```

- State is `Arc`-wrapped and `.clone()`-cheap.
- **No `Mutex<HashMap>`** patterns — use `dashmap` for concurrent maps, `RwLock` only if reads vastly dominate and value is small.
- **Never hold a lock across `.await`** — `clippy::await_holding_lock` denies this at CI.

### Graceful shutdown

```rust
// bins/au-kpis-api/src/main.rs
async fn shutdown_signal(token: CancellationToken) {
    let ctrl_c = async { signal::ctrl_c().await.ok(); };
    let terminate = async {
        signal::unix::signal(SignalKind::terminate())
            .ok()
            .and_then(|mut s| tokio::runtime::Handle::current().block_on(async { s.recv().await }));
    };
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    token.cancel();
}

// In main:
let token = CancellationToken::new();
let app = build_router(state.clone());
let listener = tokio::net::TcpListener::bind(&cfg.bind).await?;
tokio::spawn(shutdown_signal(token.clone()));
axum::serve(listener, app)
    .with_graceful_shutdown(token.cancelled_owned())
    .await?;
```

- **Single `CancellationToken`** plumbed everywhere.
- Ingestion workers poll `token.cancelled()` in `tokio::select!` loops; in-flight jobs checkpoint to DB and exit cleanly.
- Drain window: 30s default, configurable.

### Channels and backpressure

- **Pipeline stages** (discover → fetch → parse → load) connected via **bounded `mpsc` channels** — default capacity 64.
- Channel full = producer awaits = backpressure propagates upstream naturally.
- **`broadcast`** channel for fan-out events (e.g. "new dataflow registered").
- **`watch`** channel for config reloads (SIGHUP → new config broadcast to listeners).
- **`oneshot`** for request/response job results.

### Concurrency limits

- **`tokio::sync::Semaphore`** per source for rate limiting (belt-and-suspenders with `apalis`'s per-job backoff).
- **`futures::stream::iter(...).buffer_unordered(N)`** for parallel adapter fanout with bounded concurrency.
- `JoinSet` everywhere for spawning and reaping task groups — never bare `tokio::spawn` without a handle.

### Adapter trait (async)

```rust
// crates/au-kpis-adapter/src/lib.rs
use async_trait::async_trait;
use futures::stream::BoxStream;

#[async_trait]
pub trait SourceAdapter: Send + Sync + 'static {
    fn id(&self) -> &'static str;
    fn manifest(&self) -> &AdapterManifest;

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError>;

    #[tracing::instrument(skip(self, ctx), fields(source = self.id(), job_id = %job.id))]
    async fn fetch(&self, job: DiscoveredJob, ctx: &FetchCtx) -> Result<ArtifactRef, AdapterError>;

    /// Streaming parse — emits observations as they're produced. Allows the loader
    /// to upsert in batches without buffering an entire file in memory.
    #[tracing::instrument(skip(self, ctx), fields(source = self.id(), artifact = %artifact.id))]
    fn parse<'a>(
        &'a self,
        artifact: ArtifactRef,
        ctx: &'a ParseCtx,
    ) -> BoxStream<'a, Result<(SeriesDescriptor, Observation), AdapterError>>;
}
```

- **`async_trait` retained** — adapters are stored as `Arc<dyn SourceAdapter>` in the registry; native `async fn in trait` does not yet support object-safe dyn dispatch with lifetimes and `Send` bounds. `async-trait`'s heap allocation is negligible next to network + parse time.
- **Parse emits `(SeriesDescriptor, Observation)`** — the descriptor carries the dimensions map so the loader can upsert the `series` row on first encounter and then just reference `series_key` for subsequent observations.
- `parse` returns a `Stream` so parsers yield observations incrementally (critical for 500MB SDMX-JSON files or AEMO bulk dumps — never load full artifact into memory).
- **Adapter registry pattern**: `Adapters::builder().register(abs::new()).register(rba::new()).build()` in the ingestion binary. Adding a source = add one line.
- **`#[tracing::instrument]`** on every public async method; tracing context propagated through channels via `.instrument(Span::current())` wrappers in `au-kpis-queue`.

### Error handling across crates

- **Library crates** (`au-kpis-*` other than `bins/`): `thiserror`-derived enums with `#[from]` conversions. Never `anyhow::Error` in public library APIs.
- **Binaries**: `anyhow::Result` at the top-level with `.context(...)` to annotate.
- **API**: `ApiError` enum in `au-kpis-api-http` implements `IntoResponse`. Maps internal errors → HTTP status + JSON problem-details body (`application/problem+json`, RFC 7807).

```rust
#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("validation: {0}")]
    Validation(String),
    #[error("rate limited")]
    RateLimited { retry_after: Duration },
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Cache(#[from] CacheError),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for ApiError { /* maps to status + problem+json */ }
```

### Streaming responses

Observation queries can return millions of rows. We never buffer.

```rust
// Pseudocode
async fn observations_stream(
    State(state): State<AppState>,
    Query(q): Query<ObservationsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let stream = state.db.stream_observations(q.into_sql_params()).await?;
    let body = match q.format {
        Format::Json => StreamBody::new(json_lines_stream(stream)),
        Format::Csv => StreamBody::new(csv_stream(stream)),
        Format::Parquet => StreamBody::new(parquet_stream(stream)),  // arrow-rs → parquet
    };
    Ok((StatusCode::OK, headers_for_format(q.format), body))
}
```

- sqlx `fetch()` is natively a `Stream` — we wrap and transform to target format.
- Parquet path buffers into 1MB row groups then flushes — stays under 10MB peak memory per request regardless of total rows.
- Client-side aborts via `CancellationToken` tied to request span stop the DB cursor immediately.

### Tower middleware stack (ordered)

```
TraceLayer                  # request/response tracing
 └─ CorsLayer
    └─ CompressionLayer     # gzip/br for JSON/CSV; skip Parquet
       └─ TimeoutLayer (30s default, override per route)
          └─ RequestIdLayer # x-request-id propagation
             └─ AuthLayer   # API key validation
                └─ RateLimitLayer  # Redis token bucket
                   └─ MetricsLayer # per-route histograms
                      └─ Handler
```

- Ingestion binary uses `tower` too — `RateLimitLayer` on outbound HTTP to sources; `RetryLayer` with exponential backoff.

### Testing async code

- **Unit tests**: `#[tokio::test]`, in-process, use traits + mocks (mockall).
- **Integration tests**: `testcontainers` spins Postgres+Timescale+Redis+Minio per test. Real DB, real queries.
- **Snapshot tests** (`insta`): adapter-fixture → observations as YAML snapshots. Format drifts caught immediately.
- **Loom** for concurrent data structure tests (rare — maybe in queue crate).
- **Criterion** benches for hot paths: observation serialization, SDMX parse, Parquet generation.

---

## Data model (SDMX-inspired)

Defined in `au-kpis-domain`. Core decomposition: **`Series` (metadata + dimensions) is separate from `Observation` (time + value)**. This is the key abstraction that keeps the hot table narrow, makes dimensional queries fast, and avoids duplicating dimension maps across millions of rows.

```rust
// crates/au-kpis-domain/src/series.rs
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Series {
    pub series_key: SeriesKey,           // sha256 hash of (dataflow_id || sorted dimensions)
    pub dataflow_id: DataflowId,
    pub measure_id: MeasureId,
    pub dimensions: BTreeMap<DimensionId, CodeId>,  // JSONB in DB, GIN-indexed
    pub unit: String,
    pub first_observed: Option<DateTime<Utc>>,
    pub last_observed: Option<DateTime<Utc>>,
    pub active: bool,
}

// crates/au-kpis-domain/src/observation.rs
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Observation {
    pub series_key: SeriesKey,           // FK → series
    pub time: DateTime<Utc>,
    pub time_precision: TimePrecision,
    pub value: Option<f64>,
    pub status: ObservationStatus,
    pub revision_no: u32,                // 0 = original, N = Nth revision
    pub attributes: BTreeMap<String, String>,
    pub ingested_at: DateTime<Utc>,
    pub source_artifact_id: ArtifactId,
}
```

Core types: `Source`, `Dataflow`, `Dimension`, `Codelist`, `Code`, `Measure`, `Series`, `Observation`, `Artifact`. `ToSchema` derive (utoipa) makes them OpenAPI-visible. `SeriesKey` is a newtype wrapping a hash — not `String` — for type-safety at all boundaries, and series dimensions stay typed in-memory (`DimensionId`/`CodeId`) while serializing to ordinary JSON object keys/values.

### Database schema (key tables)

- **`series`** — vanilla Postgres, GIN index on `dimensions` JSONB for dimensional queries like "all VIC observations". ~100K rows, small.
- **`observations`** — **Timescale hypertable**, narrow table of `(series_key, time, revision_no, value, status, attributes, artifact_id)`. Primary key `(series_key, time, revision_no)`. Chunked on `time` (1-month), space-partitioned on `series_key` hash.
- **`observations_latest`** — materialized view / continuous aggregate: most-recent revision per `(series_key, time)`. This is the default query target; full `observations` for audit/revision history.
- **Compression** on chunks >7 days old (90%+ savings on numeric data).
- **Continuous aggregates** for weekly/monthly/quarterly rollups per series, refreshed by policy.
- `sources`, `dataflows`, `dimensions`, `codelists`, `codes`, `measures` — vanilla relational tables.
- `artifacts` — content-addressed (sha256) → S3 key plus captured HTTP response headers, including repeated values for the same header name; dedup on hash so refetching same file is a no-op.
- `parse_errors` — rows that failed validation, keyed to artifact for re-processing.
- `api_keys` — hashed (argon2id), scopes, rate-limit tier.

Typical query plan:
```
1. Resolve Dataflow + Dimensions → lookup Series rows (JSONB GIN) → Vec<SeriesKey>
2. SELECT * FROM observations_latest WHERE series_key IN (...) AND time BETWEEN ... ORDER BY time
   (hypertable chunk exclusion makes this fast even over years of data)
3. Stream results to client in JSON/CSV/Parquet
```

---

## Source adapters

Each source = its own crate implementing `SourceAdapter`. Adding source 15 never touches sources 1-14.

| Source | Crate / path | Discovery | Fetch | Parse |
|---|---|---|---|---|
| **ABS** | `crates/adapters/abs` (`au-kpis-adapter-abs`) | SDMX `/dataflow` diff | SDMX-JSON | Native stream (`serde_json` + `tokio-stream`) |
| **RBA** | `crates/adapters/rba` (`au-kpis-adapter-rba`) | Scrape stat-tables index weekly | XLS/CSV | `calamine` (XLS), `csv-async` (CSV) |
| **APRA** | `crates/adapters/apra` (`au-kpis-adapter-apra`) | Scrape release calendar | XLS | `calamine` |
| **ASX** | `crates/adapters/asx` (`au-kpis-adapter-asx`) | Announcements RSS + EOD | Mixed | `quick-xml` + `csv-async` |
| **Treasury** | `crates/adapters/treasury` (`au-kpis-adapter-treasury`) | Watch budget pages | PDF | `pdf-client` -> Python sidecar extraction strategy |
| **AEMO** | `crates/adapters/aemo` (`au-kpis-adapter-aemo`) | NEMWeb directory listings | CSV (frequent) | `csv-async` |
| **State budgets** | `crates/adapters/state-budgets` (`au-kpis-adapter-state-budgets`) | Hand-curated | PDF | Python sidecar extraction strategy |

### Guardrails

- Every adapter has a **golden-file test** (insta snapshot): fixture → expected observations.
- Adapters **never** touch DB. They emit `Observation` into a stream; `au-kpis-loader` handles persistence.
- **Rate limits** declared in `AdapterManifest`, enforced by `RateLimitLayer` on the shared HTTP client.
- **Versioned parsers**: `parse_v1`, `parse_v2` alongside; feature-flag by artifact date range. Re-ingest of old files stays correct.

---

## Ingestion pipeline

```
┌──────────────┐   ┌───────────┐   ┌────────┐   ┌─────────┐   ┌──────────┐
│ Scheduler    │──▶│ Discovery │──▶│ Fetch  │──▶│  Parse  │──▶│   Load   │
│ (apalis cron)│   │ (per src) │   │ S3 put │   │ stream  │   │ upsert   │
└──────────────┘   └───────────┘   └────────┘   └─────────┘   └──────────┘
   PG cron           adapter         rate-       Python            batched
                   .discover()      limited     (if PDF)           Timescale
                                     per src                       COPY
```

### Queue abstraction + semantics

`au-kpis-queue` exposes a `Queue` trait (push, pop, renew, ack, nack, schedule-cron) with a Postgres implementation using transactional leasing. Workers must periodically renew long-running leases; ack/nack operations are accepted only for the current monotonic lease token. The rest of the codebase depends on the trait — this implementation is swappable with `apalis` if we later want its worker runtime or scheduler model.

- **`discovery`** — cron-triggered per adapter schedule. Cheap.
- **`fetch`** — rate-limited per source (Tower layer on HTTP client + queue-level concurrency). Streams raw to S3 via `object_store`.
- **`parse`** — CPU-ish; Polars/calamine/XLS parsing in `spawn_blocking` pool. PDFs call the Python sidecar, which runs deterministic extraction first and can fall back to a pinned local model backend only after validation fails or drift is detected.
- **`load`** — DB upserts. Serialized per `(dataflow_id)` via job-group keys to avoid upsert conflicts on shared series.
- **`backfill`** — low priority queue for historical re-ingest; paused when live queues have backlog.

Postgres-backed jobs = transactional dequeue (`FOR UPDATE SKIP LOCKED`), no separate job store to operate. Retries + exponential backoff built in. Dead-letter queue per stage, retained independently of queue job retention. Running jobs carry a renewable lease timeout so crashed workers do not strand work; `load` job groups are enforced with database constraints so only one running job per `dataflow_id` exists at a time.

**Tracing context propagation**: every job carries a `trace_parent` field; worker restores span from it before processing, so traces span discovery → load as a single tree.

### Idempotency

- Fetch: S3 key = `sha256(content)` → content-addressed, write is idempotent.
- Parse: snapshot tested; same artifact → same observations.
- Load: `ON CONFLICT (dataflow_id, series_key, time, revision_no) DO UPDATE`; revisions tracked not overwritten.
- Jobs carry correlation ID from discovery to load for tracing.

### Failure handling

- **Transient** (5xx, timeout): retry via queue retry policy, max 5, exp backoff, DLQ.
- **Format error** (validation fails): pause adapter + Grafana alert + capture raw for review. **No retry** — format changed, human decides.
- **Partial parse**: validated observations load; failures → `parse_errors` with artifact pointer.
- **Circuit breaker**: per-source `tower` layer opens on >20%/50 fetch fail rate; pages on-call.

### Loader (batch upsert)

The loader consumes observation streams and writes in batches using Timescale-optimal patterns:

```rust
// crates/au-kpis-loader/src/lib.rs
async fn load_batch(
    pool: &PgPool,
    dataflow_id: &DataflowId,
    batch: Vec<Observation>,
) -> Result<LoadStats, LoadError> {
    // 1000-row batches → single COPY into staging table
    // Then INSERT ... ON CONFLICT from staging → hypertable
    // Staging table is TEMP, dropped automatically at tx end.
    let mut tx = pool.begin().await?;
    let mut copy = tx.copy_in_raw("COPY staging_obs (...) FROM STDIN BINARY").await?;
    for obs in &batch { copy.send(encode_row(obs)).await?; }
    copy.finish().await?;
    sqlx::query("INSERT INTO observations (...) SELECT ... FROM staging_obs ON CONFLICT ... DO UPDATE ...")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(stats)
}
```

- COPY is 10-50x faster than row-by-row INSERT for bulk loads.
- Batches bounded at 1000 rows or 10MB, whichever first.
- Each batch is its own transaction — partial failures don't kill a 1M-row load.

---

## PDF extractor service (Python)

The sidecar is an extraction orchestrator, not a trusted parser of final
economic observations. Rust adapters still own source-specific mapping and
validation.

- FastAPI service with pluggable extraction backends.
- Required baseline backend: deterministic `pdfplumber` + `camelot-py[cv]`
  for born-digital PDFs.
- Optional model backends: open-source document/table models such as Docling
  or Granite Docling, used only as a fallback or comparison path.
- `POST /extract { s3_key, source_id, artifact_date?, strategy? }` fetches
  directly from S3/R2 and streams structured table candidates back.
- Dockerized with Ghostscript, OpenCV, Tk, and optional model dependencies in
  a separate image/profile so the deterministic sidecar remains lightweight.
- Horizontally scalable; Rust adapter calls via `au-kpis-pdf-client` crate
  (reqwest wrapper with retries).

### Extraction strategy

Default order:

1. `deterministic`: run `pdfplumber`/`camelot`, preserving page number,
   bounding boxes, raw cell text, row/column spans where available, and parser
   diagnostics.
2. `validate`: source adapter checks table shape, period labels, numeric
   coercion, totals/cross-foot rules where present, plausible ranges, and
   expected row/column headers.
3. `model_fallback`: if deterministic extraction fails validation or schema
   hash drift is detected, run a pinned local model backend and re-run the
   same validation.
4. `manual_review`: if both paths fail, capture `parse_errors`, pause the
   adapter, and alert. Do not silently ingest model output.

Models may propose table structure and OCR text, but they must not directly
emit `Observation` rows. Every model result is normalized through the same
source-specific Rust parser and validation path as deterministic output.

### Model governance

- Model inference is local/self-hosted. Do not send government PDFs to
  hosted third-party inference APIs in production ingestion.
- Model name, version/revision, weights checksum, runtime, prompt/instruction,
  decoding parameters, page image hash, and output hash are persisted with the
  extraction metadata.
- Model backends are opt-in per source/date range through config. The default
  production path remains deterministic.
- Any model change that can affect parsed observations requires fixture
  snapshots, a before/after extraction report, and a changelog note.
- Generated table candidates must include confidence/diagnostic metadata where
  the backend exposes it. Low-confidence tables are treated as parse errors
  unless source-specific validation proves them safe.

### Sidecar response shape

The sidecar returns table candidates, not final observations:

```json
{
  "artifact_key": "artifacts/<sha256>",
  "backend": {
    "kind": "deterministic|model",
    "name": "camelot|pdfplumber|docling|granite-docling",
    "version": "...",
    "model_sha256": null
  },
  "tables": [
    {
      "page": 12,
      "bbox": [72.0, 120.0, 540.0, 720.0],
      "cells": [["Year", "Revenue"], ["2026-27", "123.4"]],
      "diagnostics": {"confidence": 0.98}
    }
  ]
}
```

The exact response type lives in `au-kpis-pdf-client`; this JSON is the
contract shape, not a hand-written OpenAPI replacement.

---

## API surface

Axum + utoipa. Versioned at `/v1`. OpenAPI served at `/v1/openapi.json`.

```
GET  /v1/sources
GET  /v1/sources/{id}
GET  /v1/dataflows
GET  /v1/dataflows/{id}
GET  /v1/dataflows/{id}/codelists/{dim}
GET  /v1/observations
       ?dataflow=abs.cpi
       &dimensions[region]=AUS
       &since=2010-01-01&until=2026-04-01
       &frequency=monthly
       &format=json|csv|parquet
       &cursor=...&limit=10000
GET  /v1/observations/latest
GET  /v1/series/{dataflow}/{series_key}
GET  /v1/search?q=unemployment
POST /v1/subscriptions                        # phase 5
GET  /v1/health
GET  /v1/openapi.json
```

### Non-functional

- **Auth**: `X-API-Key` header. Keys stored **argon2id-hashed** in DB; lookup cached in Redis with short TTL; constant-time compare. JWT for client app.
- **Rate limits** (free tier defaults): 60 rps / 1000 requests per hour per key. Burst allowance of 2x. Token bucket in Redis via `fred`. Returns `429` with `Retry-After` header and `X-RateLimit-*` headers on every response. Anonymous (no key) tier: 10 rps / 100 per hour per IP.
- **Caching**: Dataflow metadata long TTL. Observation responses ETag + `Cache-Control: public, max-age=60, stale-while-revalidate=300`. Cloudflare CDN in front.
- **Pagination**: cursor-based (opaque base64 of `(time, series_key)` pair). Max 10k rows per page.
- **Formats**: JSON default; CSV; **Parquet streamed via arrow-rs** — this is the killer feature for data-science consumers.
- **Versioning**: additive changes same version; breaking changes → `/v2`. OpenAPI diff enforced in CI via `oasdiff`.
- **Problem+JSON** error bodies (RFC 7807).

### OpenAPI generation (utoipa)

```rust
// crates/au-kpis-api-http/src/docs.rs
#[derive(OpenApi)]
#[openapi(
    paths(
        handlers::observations::list,
        handlers::dataflows::list,
        handlers::dataflows::get,
        ...
    ),
    components(schemas(
        domain::Observation,
        domain::Dataflow,
        domain::Source,
        api::ObservationsQuery,
        api::ObservationsResponse,
        api::ProblemDetails,
    )),
    tags((name = "observations", description = "Time-series observations"))
)]
pub struct ApiDoc;
```

- Each handler `#[utoipa::path(...)]` annotated.
- `cargo run -p au-kpis-openapi > openapi.json` emits the canonical spec from the
  shared `au-kpis-api-http::ApiDoc`.
- The PR contract job also starts the phase-1 API server and validates the live
  `GET /v1/openapi.json` response against the OpenAPI 3.1 schema before
  fuzzing covered routes with Schemathesis.
- CI compares against committed `openapi.json`; drift is a PR comment.

---

## TypeScript SDK

Published as `@au-kpis/sdk`. Generated from OpenAPI, wrapped ergonomically.

### Generation pipeline

```
utoipa macros (Rust)
    │
    ▼
openapi.json  (committed to repo)
    │
    ▼
pnpm sdk:generate
    ├─ openapi-typescript  → packages/sdk-generated/types.ts
    └─ orval               → packages/sdk-generated/client.ts
    │
    ▼
packages/sdk/src/client.ts  (hand-written ergonomic wrapper)
    │
    ▼
pnpm publish @au-kpis/sdk
```

### Public API

```typescript
import { createClient } from '@au-kpis/sdk'

const client = createClient({
  apiKey: process.env.AU_KPIS_KEY,
  baseUrl: 'https://api.au-kpis.example',  // optional
})

// Typed — IDE knows 'abs.cpi' dimensions
const { series } = await client.observations.list({
  dataflow: 'abs.cpi',
  dimensions: { region: 'AUS', measure: 'All groups CPI' },
  since: '2010-01-01',
})

// Stream large datasets (Parquet under the hood if requested)
for await (const obs of client.observations.stream({
  dataflow: 'aemo.dispatch',
  since: '2026-04-01',
})) {
  console.log(obs.time, obs.value)
}

// Catalog
const dataflows = await client.dataflows.list({ source: 'abs' })
const matches = await client.search('unemployment')
```

- **Runtime validation** (optional, off by default): Zod schemas generated from OpenAPI via `openapi-zod-client`; consumers pass `{ validate: true }` to enable.
- **Retries + Retry-After** awareness built in.
- **Bun + Node + browser + Deno** compatible — `fetch` only.
- **Tree-shakeable**: subpath imports supported.

---

## Reference client (apps/web)

Vite + React 18 + TanStack Query + Tailwind + shadcn/ui + Observable Plot.

Pages:
- **Explorer** — browse dataflows, pick dimensions, chart.
- **Search** — full-text across measures.
- **Compare** — multiple series on one chart.
- **Playground** — live query form → response viewer with curl / SDK snippet.

Demonstrates the SDK and doubles as internal data browser.

---

## Observability

- **`tracing` + OTLP exporter** in every binary.
- **`tracing-opentelemetry`** bridges spans to OTel.
- **`tower-http::TraceLayer`** for HTTP server + outbound `reqwest-tracing` for client spans.
- **Metrics** via `opentelemetry-prometheus`:
  - `au_kpis_ingestion_lag_seconds{dataflow}` — most important
  - `au_kpis_fetch_errors_total{source}`
  - `au_kpis_http_request_duration_seconds{route, status}`
  - `au_kpis_db_query_duration_seconds{query}`
  - `au_kpis_queue_depth{queue}`
- **Logs**: JSON via `tracing-subscriber`; `request_id` + `job_id` + `correlation_id` on every line.
- **Grafana dashboards**:
  - Ingestion freshness heatmap (dataflow × hours-since-load)
  - Per-source error rate
  - API latency p50/p95/p99 per route
  - Queue depth + worker saturation
- **Alerts**:
  - Dataflow no new observations >2× expected cadence
  - Adapter error rate >20%/1h
  - API p95 >500ms sustained 5m
  - DB replication lag / connection pool saturation
- **Data-quality checks** (scheduled job):
  - Value >5σ from rolling mean → flag observation suspicious
  - Series silent when it shouldn't be
  - Revision volume anomaly

---

## Testing strategy

Philosophy: data-intensive systems have most bugs at boundaries (source formats, DB, API contracts). We lean integration-heavy, but back it with property + fuzz + mutation testing so the test suite itself is trustworthy.

### Test layers

**Unit** (`cargo test`, `nextest`)
- Pure logic: parsers, normalizers, dimension-key hashing, error mappers.
- In-memory, no I/O.
- Rule: every public fn in every `au-kpis-*` crate has at least one test.
- Coverage floor: **80% line, 70% branch** (`cargo-llvm-cov`, uploaded to Codecov, PR comment diff).

**Property-based** (`proptest`)
- Parse/serialize round-trip: `parse(serialize(obs)) == obs`
- `SeriesKey` determinism: same dimensions → same hash; different dimensions → different hash (no collisions over 1M samples)
- Pagination invariants: `concat(pages) == full_result`
- Revision chain: latest revision always wins regardless of insertion order
- Chunk exclusion: queries with narrow time windows never scan chunks outside range

**Snapshot** (`insta`)
- Every adapter: fixture artifact → `Vec<Observation>` YAML snapshot. Format drift = PR with obvious diff.
- PDF sidecar fixtures: raw table candidates snapshot per backend. Adapter
  snapshots remain the merge contract for final observations.
- Golden API responses (JSON shape stability).
- OpenAPI spec snapshot — intentional changes get committed; drift needs review.

**Integration** (`testcontainers` — real PG+Timescale+Redis+Minio per test file)
- Loader: COPY batch → query back → revision upsert → `observations_latest` view correctness.
- Adapter: `wiremock` stubs upstream HTTP → full adapter run → DB rows match expected.
- PDF sidecar: deterministic backend, model fallback trigger, validation
  failure, and provenance metadata covered with committed fixtures.
- API: full request through middleware stack → DB → response validated against OpenAPI schema.
- Queue: enqueue → worker dequeue → retry on failure → DLQ on exhaustion.
- Migrations: apply forward; apply backward (where reversible); apply forward again — idempotent.

**Contract** (OpenAPI + `schemathesis`)
- Schemathesis reads our `openapi.json`, generates request variants, fuzzes against a running API.
- Catches: response-schema drift, missing error cases, nullable mismatches, status-code gaps.
- Runs on every OpenAPI change + nightly against staging.

**SDK integration**
- TS SDK test suite runs against real API via docker-compose.
- Verifies each SDK method: hits right endpoint, parses response, handles `4xx`/`5xx`, retries on `Retry-After`, streams correctly, types match runtime.

**End-to-end** (Playwright — `apps/web/e2e/`)
- Critical journeys: land → search "CPI" → pick series → view chart → download CSV → copy SDK snippet.
- Visual regression (screenshot diff vs committed baseline, per-breakpoint).
- Accessibility: `axe-core` on every key page; WCAG AA required.

**Smoke** (`apps/bench/smoke-post-deploy.sh`)
- Post-deploy in every environment. Pure bash + curl + SDK hello-world. Non-zero exit = auto-rollback.
- Covers: `/health`, `/openapi.json` validates, canary query returns real data, SDK constructs + calls succeed.

**Chaos** (scripted in `tests/chaos/`, runs weekly in staging)
- Kill ingestion worker mid-load → verify no duplicates/no gaps.
- Sever DB connection → verify reconnection, queued jobs resume.
- Fill queue to capacity → verify backpressure propagates, no OOM.
- Random 5xx from source adapter → verify circuit breaker opens and recovers.
- Compaction/vacuum during heavy writes → verify no deadlocks.

**Fuzzing** (`cargo-fuzz`)
- Parsers are the primary attack surface: SDMX-JSON, XLS (calamine), CSV, PDF
  sidecar responses, and model-produced table markup/JSON.
- Corpus: real production samples + mutation-generated inputs.
- Nightly runs for 30min each; any panic/OOM/infinite-loop blocks release + files bug.

**Mutation** (`cargo-mutants`, weekly)
- Mutates code, runs tests — surviving mutants indicate weak tests.
- Mutation score target: **≥70%**. Surviving mutants auto-filed as "add test" issues.

**Data-quality** (scheduled, runs against prod data)
- Per-dataflow rules: values within plausible ranges, cardinality stable, recency matches cadence.
- Alerts on violation; does not fail CI (production-only).

### Test organisation

Per-crate:
```
crates/<name>/
├── src/lib.rs              # #[cfg(test)] mod tests alongside code
├── tests/                  # Integration tests (cargo convention)
│   ├── integration_*.rs
│   └── fixtures/
├── benches/                # criterion
└── fuzz/                   # cargo-fuzz (parser crates only)
    └── fuzz_targets/
```

Root:
```
tests/
├── e2e/                    # docker-compose full-stack tests
├── chaos/                  # chaos scripts
├── contract/               # schemathesis configs
└── fixtures/               # shared real-world samples (gitignored large; pointer → S3)
apps/bench/                 # k6 scripts + post-deploy smoke
apps/web/e2e/               # Playwright
```

### Quality gates (PR-blocking)

| Gate | Tool | Threshold |
|---|---|---|
| Compile | `cargo check --workspace` | clean |
| Lint | `cargo clippy -- -D warnings`, `pnpm run lint` (`biome check` + `markdownlint-cli2`) | clean |
| Format | `cargo fmt --check`, `biome format --check` | clean |
| Tests | `cargo nextest run --workspace`, `vitest run`, Playwright | zero fail, zero flake |
| Coverage | `cargo-llvm-cov` | ≥80% line, ≥70% branch |
| Snapshot | `insta` | no unreviewed drift |
| OpenAPI | `oasdiff breaking` | no breaking without `/v2` |
| Contract | `schemathesis` | zero violations |
| Supply chain | `cargo audit`, `cargo deny`, `pnpm audit` | no critical CVEs, no banned licenses |
| Container | `trivy` | no HIGH/CRITICAL |
| Secrets | `gitleaks` | none |
| Bench | `critcmp main pr --threshold 5` | no >5% regression |
| Smoke | `k6 run smoke.js` against compose | all thresholds met |
| E2E | Playwright critical journeys | zero fail |
| A11y | axe-core via Playwright | zero violations on key pages |

### Flake policy

**Zero tolerance.** `nextest` records retries; any retry → auto-issue with `flaky` label + owner assigned from CODEOWNERS. CI retry ceiling: 2. Tests needing more either get fixed within 48h or deleted.

## CI/CD pipeline

Single orchestrator: **GitHub Actions** with merge queue. Four flows.

### 1. PR flow (`.github/workflows/pr.yml`)

Triggered on PR open/update. All jobs in parallel where possible; total target <5 min on warm cache.

```
parallel:
  - review          (Codex structured PR review → inline findings + summary; advisory, including missing output, unless repo vars enable blocking)
  - typecheck       (cargo check + tsc)
  - lint            (clippy, `pnpm run lint` = biome + markdownlint, gitleaks, cargo-deny)
  - build           (sccache cargo + pnpm build)
  - test            (nextest + vitest, testcontainers, source-specific streaming memory guardrails such as the ABS DHAT fetch profile)
  - coverage        (clean cargo-llvm-cov profile data with pinned nightly coverage toolchain → LCOV line/branch coverage → Codecov PR comment)
  - snapshot        (insta check)
  - openapi         (`cargo run -p au-kpis-openapi` export + oasdiff vs main)
  - contract        (live `/v1/openapi.json` schema validation + schemathesis)
  - security        (cargo audit, trivy on built images)
  - smoke           (spin compose, run curl + SDK smoke)
  - bench           (critcmp — advisory on PR, blocking in merge queue)
```

All gates from the table above run here. Blocking for merge.

The coverage job blocks on local `cargo-llvm-cov` line and branch thresholds.
The Codecov upload/reporting step is advisory so third-party ingest outages do
not fail an otherwise valid PR gate.

Source-specific memory guardrails may run as explicit PR blockers when they
protect a pass requirement that normal unit/integration tests cannot observe;
for example, the ABS fetch path runs its DHAT profile to enforce the streaming
heap budget.

Codex review is an additional review signal, not part of the 14 blocking gates by default. It runs only when `OPENAI_API_KEY` is configured for the repository and the PR originates from the same repository (not a fork). Default model: `gpt-5.5`; allow `CODEX_MODEL`, `CODEX_REVIEW_EFFORT`, and `CODEX_REVIEW_BLOCK_ON_INCORRECT` as repository variables for tuning.

### 2. Merge-queue flow (`.github/workflows/merge.yml`)

GitHub merge queue batches PRs; per batch:
- Full PR flow
- k6 `smoke.js` against compose
- `schemathesis` contract fuzz
- Bench regression now blocking
- Playwright full suite

Any failure ejects the whole batch; fixes re-queued individually.

### 3. Deploy flow (`.github/workflows/deploy.yml`)

Triggered on merge to `main`.

```
1. Build multi-arch release binaries (linux/amd64, arm64)
2. Build distroless Docker images; sign with cosign; SBOM via syft
3. Push to GHCR
4. Deploy to Fly staging (one region)
5. Run post-deploy smoke against staging — FAIL = auto-rollback
6. Run k6 sustained (abbreviated 2-min variant) against staging
7. Manual approval gate for production
8. Deploy to Fly prod (blue/green)
9. Post-deploy smoke against prod — FAIL = auto-rollback
10. Slack + PagerDuty deploy notification
```

**Rollback**: `fly releases rollback <app>` — automated on smoke failure, manual otherwise. One-click from Slack message.

### 4. Scheduled flows

| Cron | Job | Purpose |
|---|---|---|
| `*/5 * * * *` | External uptime probe (StatusCake) | Availability SLO |
| `*/15 * * * *` | Freshness canary (query known-fresh dataflow) | Data SLO |
| `0 * * * *` | Data-quality checks per dataflow | Silent corruption |
| `0 2 * * *` | Nightly k6 `sustained` + `burst` in staging | Perf regression |
| `0 3 * * *` | Nightly `cargo fuzz run` (30min/target) | Parser robustness |
| `0 4 * * *` | `schemathesis` against staging (deep fuzz) | Contract robustness |
| `0 6 * * 0` | `cargo mutants` weekly | Test quality |
| `0 7 * * 0` | Renovate dependency PRs | Hygiene |

### Release cadence + versioning

- **API** additive changes: auto-deploy on merge.
- **API** breaking changes: require `/v2` path; `/v1` deprecation 6-month window; `Deprecation` + `Sunset` headers per RFC 8594.
- **SDK** semver via `@changesets/cli` — every PR that touches `packages/sdk` requires a changeset file; release workflow publishes on merge.
- **Monorepo tags**: `api-vX.Y.Z`, `sdk-vX.Y.Z`, `ingestion-vX.Y.Z` (independent).

### Preflight: pre-commit hooks

`.git/hooks/pre-commit` (via `lefthook`, committed in-repo):
- `cargo fmt --check`, `biome format --check` (fast)
- `gitleaks protect --staged`
- Block commits that add files >5 MB (fixtures go to S3 pointers)

## Benchmarking

Benchmarks are **first-class** — they live in the repo, run in CI, enforce regressions, and drive capacity planning. Three layers:

### 1. Micro-benchmarks (`criterion`)

Per-crate `benches/` directories. CI runs on every PR; baseline committed to `benches/baselines/`. Regressions >5% block merge.

Priority bench targets:

| Crate | Bench | Budget |
|---|---|---|
| `au-kpis-domain` | Serialize 10k `Observation` to JSON | <50 ms |
| `au-kpis-domain` | Serialize 10k `Observation` to Parquet (arrow-rs) | <20 ms |
| `au-kpis-adapter` (helpers) | SDMX-JSON parse 100MB fixture → stream of observations | >500k obs/s |
| `crates/adapters/abs` (`au-kpis-adapter-abs`) | Full CPI dataflow parse (real fixture) | <2 s, <100 MB RSS |
| `crates/adapters/rba` (`au-kpis-adapter-rba`) | XLS parse (`calamine`) 10-sheet workbook | <500 ms |
| `au-kpis-loader` | Batch upsert 10k observations via COPY | <500 ms |
| `au-kpis-api-http` | Request handler end-to-end (in-process) | <5 ms overhead above DB |
| `au-kpis-auth` | API key verify (argon2id + Redis cache hit) | <1 ms p99 |
| `au-kpis-cache` | Redis token-bucket rate-limit check | <500 µs p99 |

Run locally: `cargo bench --workspace` — HTML reports under `target/criterion/`.

### 2. Load tests (`k6`, JavaScript scripts — fits TS monorepo naturally)

`apps/bench/` holds k6 scripts. Three scenarios:

- **`smoke.js`** — 1 VU for 30s, every endpoint. PR check.
- **`sustained.js`** — 100 VUs for 10 min, realistic query mix (70% single-series, 20% bulk, 10% catalog). Nightly in staging.
- **`burst.js`** — ramp 0 → 2000 VUs over 2 min, hold 2 min, ramp down. Weekly. Tests rate-limit behavior and autoscale.

Targets baked into k6 thresholds (failing = CI red):

| Scenario | Metric | Threshold |
|---|---|---|
| smoke | http_req_duration p95 | <200 ms |
| smoke | http_req_failed rate | <1% |
| sustained | http_req_duration p95 | <500 ms |
| sustained | http_req_duration p99 | <1500 ms |
| sustained | error rate | <0.1% |
| burst | 429 responses | present (rate limit working) but <30% of total |
| burst | 5xx responses | <0.5% |

Results POST to InfluxDB/Grafana for historical trending. A regression tag on a PR triggers side-by-side comparison in the PR comment.

### 3. Production SLOs (observed, not synthetic)

Grafana dashboards + alerts on real traffic. These are the numbers we **commit to externally**:

| SLO | Target | Error budget (30d) |
|---|---|---|
| API availability | 99.9% | 43 min |
| `GET /v1/observations` p95 | <200 ms (warm), <500 ms (cold) | - |
| `GET /v1/observations` p99 | <1 s | - |
| Data freshness (ABS daily series) | <4 h after source publish | - |
| Data freshness (ABS monthly series) | <24 h | - |
| Data freshness (AEMO 5-min series) | <15 min | - |
| Ingestion error rate per source | <1% | - |
| Parquet 1M-row download | <30 s, <100 MB server mem | - |

Burn-rate alerts (fast + slow window) per SLO — Google SRE standard pattern.

### 4. Profiling tooling

Not continuous but available on demand, documented in `docs/perf-playbook.md`:

- **CPU**: `samply` (cross-platform, user-mode) → Firefox profiler. `cargo flamegraph` alt.
- **Async runtime**: `tokio-console` (requires `RUSTFLAGS="--cfg tokio_unstable"` + `console-subscriber`). Shows task state, polling hot loops, stalled tasks.
- **Memory**: `dhat` (in-process), `heaptrack` (system-wide).
- **DB**: `EXPLAIN (ANALYZE, BUFFERS)` on every new hot-path query; `pg_stat_statements` in prod; slow-query log at >100ms.
- **Sqlx compile-check**: `sqlx prepare` generates offline query metadata — catches schema drift at build time, not runtime.

### 5. Capacity planning

Bench results feed a spreadsheet model:
- Observations per worker-hour (from loader bench) × worker count = ingestion headroom
- Rows-per-second per instance (from sustained load test) × instance count = serving headroom
- Projected Year-N growth → required capacity ratio

Reviewed quarterly. Triggers autoscaling threshold updates and capacity provisioning.

### CI integration

```yaml
# .github/workflows/bench.yml (summary)
on: [pull_request, push: { branches: [main] }]
jobs:
  criterion:
    steps:
      - run: cargo bench --workspace -- --save-baseline pr
      - run: cargo install critcmp && critcmp main pr --threshold 5
  k6-smoke:
    services: [timescale, redis, minio]
    steps:
      - run: cargo run --bin au-kpis-api &
      - run: k6 run apps/bench/smoke.js
```

Nightly staging load tests run on real cloud infra (Timescale Cloud staging branch).

## Security posture

- **TLS** terminated at Fly.io edge; internal service-to-service over private network (Fly 6PN).
- **Secrets**: Fly secrets at runtime; no `.env` files committed. Rotation playbook documented.
- **API key storage**: argon2id-hashed (never plaintext); keys shown once on creation. Prefix `auk_live_` for discoverability in leaks; GitHub secret scanning pattern registered.
- **SQL injection**: eliminated at compile time by `sqlx::query!` macros — all queries compile-checked against the real DB schema.
- **Dependency hygiene**: `cargo audit` + `cargo deny` in CI (reject yanked crates, GPL-incompatible licenses, unknown sources). Renovate for weekly updates.
- **Container hygiene**: distroless base images; non-root user; read-only filesystem; no shell.
- **Rate limiting + abuse**: per-key + per-IP buckets; slow-down on repeated 4xx from same key.
- **CORS**: explicit allowlist for browser clients; `*` allowed only on cacheable GET endpoints.
- **PII**: the platform aggregates public economic data — no PII by design. API keys are the only user data; audit log of key issuance/revocation retained 1 year.

## Data licensing and attribution

Most Australian sources publish under open licenses but require attribution. Each `Dataflow` row carries:
- `license` (e.g. `CC-BY-4.0`, `CC-BY-ND-4.0`, `public-domain`)
- `attribution` (required acknowledgement string)
- `source_url` (canonical citation target)

- SDK responses include `attribution` in metadata; client displays it below charts.
- Bulk download endpoints return a companion `LICENSE.txt` in the response envelope.
- Ingestion refuses to load a dataflow without a license field populated in the adapter manifest — forces explicit handling.

## Deployment

**Target:** Fly.io (phase 1-3); migrate to GCP/AWS when we outgrow.

- **api binary** — 2+ instances behind LB, autoscale on CPU. Health on `/v1/health`.
- **ingestion binary** — N workers, horizontal scale, pull from apalis queue.
- **scheduler binary** — singleton with leader-election (Postgres advisory lock).
- **pdf-extractor** — 2+ instances, internal-only.
- **postgres** — Timescale Cloud managed (backups, HA, compression policies).
- **redis** — Upstash or Fly Redis.
- **S3** — Cloudflare R2 (egress-free).
- **Single Rust binary per service** — Dockerfile uses `cargo chef` for layered caching. Images <100MB.

---

## Phased rollout

Each phase ends demo-able.

### Phase 1 — Skeleton (1.5 weeks)
- [ ] Cargo workspace + crate scaffolding (domain, error, config, telemetry)
- [ ] `rust-toolchain.toml`, `.cargo/config.toml` (sccache), clippy deny
- [ ] `au-kpis-api` hello-world with axum + utoipa
- [ ] `au-kpis-db` crate + sqlx + Timescale migration for observations hypertable
- [ ] `docker-compose.yml` (PG+Timescale, Redis, Minio, PDF sidecar stub)
- [ ] pnpm workspace + Turborepo for TS side
- [ ] CI: `cargo check`, `cargo clippy -D warnings`, `cargo test`, `cargo deny`, sccache
- [ ] OpenAPI export works; TS SDK regen pipeline stubbed
- [ ] `criterion` scaffold + one placeholder bench; `critcmp` wired to CI

### Phase 2 — One end-to-end source: ABS CPI (2.5 weeks)
- [ ] `au-kpis-adapter` trait crate + registry
- [ ] `crates/adapters/abs` (`au-kpis-adapter-abs`) — CPI dataflow (SDMX-JSON: `CPI` dataflow, monthly + quarterly)
- [ ] `au-kpis-queue` (`Queue` trait + Postgres impl)
- [ ] `au-kpis-ingestion-core` + worker binary with graceful shutdown
- [ ] `au-kpis-loader` with COPY-based batch upsert + series upsert-on-first-observation
- [ ] `/v1/observations` endpoint with JSON + CSV (Parquet in Phase 3)
- [ ] `/v1/dataflows` + `/v1/series/{dataflow}/{series_key}`
- [ ] SDK v0: `client.observations.list()`, `client.dataflows.list()`
- [ ] Web client: CPI chart (national + state comparison)
- [ ] Criterion benches: SDMX parse, loader COPY, observation JSON serialize
- [ ] k6 `smoke.js` in CI against `docker-compose` stack
- [ ] Demo: ingest ABS CPI, query via SDK, visualize in client, bench report in PR

### Phase 3 — Breadth + scale features (3-4 weeks)
- [ ] RBA + APRA adapters
- [ ] Catalog endpoints (sources, dataflows, codelists)
- [ ] Search (Postgres FTS to start, OpenSearch later)
- [ ] Parquet streaming response via arrow-rs
- [ ] Auth (API keys) + Redis rate limit
- [ ] Observability: full OTel pipeline + Grafana dashboards (incl. SLO dashboards)
- [ ] Reference client: Explorer + Compare
- [ ] k6 `sustained.js` + `burst.js` running nightly in staging
- [ ] Parquet bench: 1M rows streamed <30s, peak memory <100 MB, profiled with `dhat`

### Phase 4 — PDFs + difficult sources (3 weeks)
- [ ] Python PDF extractor service with deterministic baseline and optional pinned local model fallback
- [ ] Treasury adapter
- [ ] State budget adapters (NSW, VIC, QLD first)
- [ ] Versioned parsers + format-drift alerting

### Phase 5 — Production polish (ongoing)
- [ ] AEMO adapter (high-frequency)
- [ ] ASX adapter
- [ ] Continuous aggregates for common rollups
- [ ] Webhook subscriptions
- [ ] Public docs site (generated from OpenAPI via Scalar or Redoc)
- [ ] Data-quality scheduled checks
- [ ] SDK v1 published to npm
- [ ] Load test: 1000 rps sustained; Parquet streaming 10M rows <30s end-to-end

---

## Critical files/modules to create (Phase 1 + 2)

1. `Cargo.toml` (workspace)
2. `rust-toolchain.toml`, `.cargo/config.toml`
3. `crates/au-kpis-domain/src/{lib,source,dataflow,observation,artifact}.rs`
4. `crates/au-kpis-error/src/lib.rs`
5. `crates/au-kpis-config/src/lib.rs`
6. `crates/au-kpis-telemetry/src/lib.rs`
7. `crates/au-kpis-db/src/{lib,pool,queries}.rs`
8. `infra/migrations/0001_init.sql` — includes `CREATE EXTENSION timescaledb; SELECT create_hypertable(...)`
9. `crates/au-kpis-queue/src/lib.rs`
10. `crates/au-kpis-adapter/src/{lib,traits,ctx,error}.rs`
11. `crates/adapters/abs/src/{lib,discover,fetch,parse}.rs`
12. `crates/au-kpis-loader/src/lib.rs`
13. `crates/au-kpis-ingestion-core/src/lib.rs`
14. `crates/au-kpis-api-http/src/{lib,state,routes,docs,error}.rs`
15. `crates/bins/au-kpis-api/src/main.rs`
16. `crates/bins/au-kpis-ingestion/src/main.rs`
17. `packages/sdk/src/client.ts`
18. `apps/web/src/{App,pages/Explorer}.tsx`
19. `infra/compose/docker-compose.yml`
20. `.github/workflows/ci.yml`

---

## Verification plan

### Phase 1
- `cargo build --workspace` succeeds; `cargo clippy --workspace -- -D warnings` clean
- `cargo nextest run --workspace` passes (unit + initial integration via testcontainers)
- `cargo-llvm-cov` baseline established (will trend upward)
- `docker compose up` brings up all infra
- `curl localhost:3000/v1/health` → `200`
- `curl localhost:3000/v1/openapi.json` → valid OpenAPI 3.1 doc (validated via the OpenAPI 3.1 JSON schema)
- `SELECT * FROM timescaledb_information.hypertables` shows `observations` hypertable
- Migrations round-trip: up → down → up yields identical schema
- TS: `pnpm -w build` succeeds; SDK regen pipeline runs; `vitest run` passes
- `gitleaks`, `cargo-deny`, `cargo-audit`, `trivy` all clean
- PR CI flow executes <5 min on warm cache
- Post-deploy smoke script runs green against `docker-compose` stack

### Phase 2 (ABS CPI end-to-end)
- **Unit**: `au-kpis-adapter-abs::parse` insta snapshot matches CPI fixture
- **Property**: `proptest` round-trip on Observation serialization; `SeriesKey` determinism over 10k random dimension combos
- **Integration (testcontainers)**: real PG+Timescale spun up, ABS CPI fixture ingested, SQL returns expected rows including revisions; `observations_latest` view correct
- **Contract**: `schemathesis` runs against running API, zero violations against `openapi.json`
- **Live**: `cargo run --bin au-kpis-ingestion -- --once --source abs --dataflow cpi` loads observations end-to-end
- **API**: `GET /v1/observations?dataflow=abs.cpi&dimensions[region]=AUS` returns well-formed JSON with attribution metadata
- **SDK**: generated types compile in a consumer project; `client.observations.list({ dataflow: 'abs.cpi' })` returns data; SDK integration tests pass against compose stack
- **Client**: Explorer renders national CPI chart + state comparison; Playwright journey green; axe-core passes
- **Smoke**: post-deploy smoke script green against compose
- **Perf**: 10-year quarterly single-series query <200ms (warm cache), <500ms cold; k6 smoke thresholds met
- **Coverage**: ≥80% line across shipped crates

### Phase 3+
- Golden-file tests per adapter
- OpenAPI diff CI: breaking change = PR comment blocks merge
- **Criterion baseline enforcement**: `critcmp main pr --threshold 5` blocks PRs with >5% regression on any committed bench
- **k6 sustained load**: 1000 rps on `/v1/observations`, p99 <1s, error rate <0.1%
- **Parquet streaming bench**: 1M rows, peak memory <100 MB, completes <30s (measured with `dhat`)
- **Parquet scale validation**: 10M rows streamed end-to-end <30s in the Phase 5 load-test path
- **Ingestion throughput**: 10K observations/sec/worker via COPY path
- Synthetic freshness canary every 15min asserts recent observations exist
- SLO burn-rate alerts configured and exercised via chaos drill
- Chaos: kill ingestion mid-load, restart, verify no duplicates / no missing rows

### Ongoing CI
- Every PR: `cargo check/clippy/test`, `cargo deny`, `cargo audit`, `pnpm run lint`, TS typecheck/build, OpenAPI diff
- Staging deploy on merge to `main`
- Prod deploy behind manual approval
- Weekly dependency updates via Renovate

---

## Review findings — best practices + abstraction checks

Pass 1 over the plan. Fixes applied in-place:

1. **Series vs Observation split** — added explicit `series` table so dimensions aren't duplicated across millions of observation rows. Hot table (`observations`) stays narrow: `(series_key, time, revision_no, value, status, attributes, artifact_id)`. Dimensional queries ("all VIC CPI") go through `series` with GIN index, then join to hypertable.
2. **Revision handling** — `revision_no` as part of PK rather than `revision_of` pointer. Simpler. `observations_latest` view (continuous aggregate) is the default query target.
3. **`SeriesKey` as a newtype** — type-safe at every boundary; prevents string/id mix-ups.
4. **Queue trait abstraction** — `au-kpis-queue` exports a `Queue` trait backed by Postgres transactional leasing. Business logic depends on the trait, so the backend remains swappable later.
5. **`async-trait` justified** — adapters live behind `Arc<dyn SourceAdapter>` for the registry pattern; native async-fn-in-trait does not yet support this cleanly. Revisit when Rust stabilises dyn-compat for async.
6. **Tracing context propagation** — jobs carry `trace_parent`; workers reconstruct span. Traces span discovery→load as one tree, not one-per-worker-task.
7. **Adapter registry pattern** — `Adapters::builder().register(abs::new()).build()` in ingestion binary. Adding a source = one line. No central switch statement.
8. **Parse emits `(SeriesDescriptor, Observation)`** tuples — loader upserts series on first occurrence; subsequent observations just reference `series_key`. Keeps adapters free of DB knowledge.
9. **SDK runtime validation fixed** — uses `openapi-zod-client` (OpenAPI → Zod), not the reverse.
10. **Security posture** — argon2id key hashing, RFC 7807 errors, sqlx compile-time SQL check, `cargo deny` license/source allowlist, distroless containers, explicit CORS.
11. **Attribution built into domain** — license + attribution fields required on every Dataflow; ingestion refuses to load without them.
12. **Workspace internal deps** — path-based `[workspace.dependencies]` entries for all `au-kpis-*` crates so members reference `{ workspace = true }`.
13. **Version pinning** — pinned the crates that rarely break (tokio, axum); left fast-moving observability crates (`tracing-opentelemetry`, `opentelemetry-*`) as "latest compatible" since their minor versions break frequently.
14. **COPY-based bulk loader** — 10-50x faster than INSERT; bounded 1000 rows / 10MB per transaction so partial failures don't kill a 1M-row load.
15. **Streaming everywhere** — adapters emit streams; loader batches; API responses stream JSON/CSV/Parquet via `StreamBody`; sqlx `.fetch()` is native stream. Peak memory flat regardless of payload size.
16. **Clippy `await_holding_lock` denied** in CI — prevents the #1 async Rust footgun (deadlock via lock held across `.await`).
17. **Benchmarking is first-class** — dedicated `Benchmarking` section with three layers: `criterion` micro-benches (CI-enforced ≤5% regression), `k6` load tests (smoke on PR, sustained + burst nightly), and production SLOs with burn-rate alerts. Numeric budgets per hot path baked in.
18. **Testing is water-tight** — dedicated `Testing strategy` section with 11 layers: unit, property (`proptest`), snapshot (`insta`), integration (`testcontainers`), contract (`schemathesis` OpenAPI fuzz), SDK integration, E2E (Playwright + a11y + visual regression), smoke (post-deploy, auto-rollback), chaos (weekly), fuzz (`cargo-fuzz` nightly on parsers), mutation (`cargo-mutants` weekly ≥70%), data-quality (scheduled prod checks). Zero-flake policy.
19. **CI/CD is four-stream** — PR flow (<5 min, all gates), merge-queue batching (expensive gates), deploy flow (auto + manual gate + auto-rollback on smoke fail), scheduled (uptime/freshness/fuzz/mutation/renovate). Pre-commit hooks via `lefthook` for fast local feedback.
20. **Quality gates** — 14 PR-blocking gates table (compile, lint, format, tests, coverage 80/70, snapshot, OpenAPI breaking-diff, contract, supply-chain, container scan, secrets, bench, smoke, E2E, a11y). Any failure blocks merge.

## Decisions log

All confirmed 2026-04-23:
- Hosting: Fly.io → migrate later
- DB: Timescale Cloud
- License model: free tier, API keys
- SDK: TypeScript first; others via OpenAPI codegen
- First dataflow: ABS CPI
- Raw artifacts: retain indefinitely in R2, lifecycle-to-cold after 1 year
- Rust 2024 edition, MSRV 1.85
- `sqlx migrate` for migrations

---

## Glossary

| Term | Definition |
|---|---|
| **SDMX** | Statistical Data and Metadata eXchange — ISO standard for statistical data exchange. ABS publishes SDMX-JSON. |
| **Dataflow** | A conceptually coherent statistical dataset (e.g. "Consumer Price Index, Australia"). Has fixed dimensions + measures. |
| **Dimension** | A categorical axis of a dataflow (e.g. `region`, `measure`, `commodity`). |
| **Codelist** | A reusable vocabulary of codes for a dimension (e.g. Australian states). |
| **Measure** | What is being counted (unemployment rate, CPI index, GDP $). |
| **Observation** | A single data point — one row in the `observations` hypertable. |
| **Series** | A conceptual time-series identified by `(dataflow, dimension values)`. Stored in `series` table. |
| **Series key** | Deterministic hash of `dataflow_id + sorted dimensions` — primary identifier for a series. |
| **Revision** | A later-published correction to a previously-observed value. Tracked via `revision_no`; `observations_latest` view shows latest per `(series_key, time)`. |
| **Hypertable** | TimescaleDB-partitioned table. `observations` is partitioned by `time` (monthly chunks) + space-partitioned by `series_key` hash. |
| **Continuous aggregate** | TimescaleDB materialized view that incrementally maintains aggregates (e.g. monthly averages) as new data arrives. |
| **Adapter** | A crate implementing `SourceAdapter` for one upstream source (e.g. ABS). |
| **Artifact** | Raw upstream file (SDMX-JSON, XLS, PDF) persisted in R2, content-addressed by `sha256`, with fetch response headers retained for audit/re-ingest, including repeated values for the same header name. |
| **SLO** | Service Level Objective — committed performance/availability target; measured from real traffic. |
| **Burn rate** | Rate at which error budget is consumed; fast/slow window alerts are the Google SRE pattern. |
| **DLQ** | Dead-letter queue — jobs that exhausted retries; reviewed manually. |
| **ABS / RBA / APRA** | Australian Bureau of Statistics / Reserve Bank of Australia / Australian Prudential Regulation Authority. |
| **AEMO** | Australian Energy Market Operator. |
| **ASX** | Australian Securities Exchange. |

---

## References

### Statistical/source references
- **ABS SDMX API** — [`data.api.abs.gov.au`](https://data.api.abs.gov.au/)
- **ABS Consumer Price Index dataflow** — dataflow ID `CPI`, quarterly
- **RBA Statistical Tables** — [`www.rba.gov.au/statistics/tables/`](https://www.rba.gov.au/statistics/tables/)
- **APRA Statistics** — [`www.apra.gov.au/statistics`](https://www.apra.gov.au/statistics)
- **AEMO NEMWeb** — [`nemweb.com.au`](https://nemweb.com.au/)
- **Treasury Budget papers** — published annually May; MYEFO December

### Standards
- **SDMX 2.1** — [`sdmx.org/?page_id=5008`](https://sdmx.org/?page_id=5008)
- **OpenAPI 3.1** — [`spec.openapis.org/oas/v3.1.0`](https://spec.openapis.org/oas/v3.1.0)
- **RFC 7807** — Problem Details for HTTP APIs
- **RFC 8594** — The Sunset HTTP Header Field
- **Apache Arrow / Parquet** — columnar data format specs

### Implementation references
- **axum** — `docs.rs/axum`
- **tokio** — `tokio.rs/tokio/tutorial`
- **sqlx** — `docs.rs/sqlx`
- **TimescaleDB** — `docs.timescale.com`
- **utoipa** — `docs.rs/utoipa`
- **apalis** — `docs.rs/apalis`
- **Tokio Console** — `github.com/tokio-rs/console`
- **schemathesis** — `schemathesis.readthedocs.io`
- **Docling** — `docling.ai`
- **Granite Docling** — `huggingface.co/ibm-granite/granite-docling-258M`
- **Table Transformer** — `huggingface.co/microsoft/table-transformer-structure-recognition`

---

## Changelog

- **v0.1.3 (2026-05-01)** — Clarified repeated artifact response-header retention and source-specific streaming memory guardrails in PR CI.
- **v0.1.2 (2026-04-30)** — Clarified that artifact records retain upstream fetch response headers alongside the content-addressed storage key.
- **v0.1.1 (2026-04-28)** — Clarified PDF extraction architecture: deterministic `pdfplumber`/`camelot` remains the baseline, with optional pinned local document-model backends for fallback/comparison. Added validation, provenance, testing, and model-governance requirements.
- **v0.1.0 (2026-04-23)** — Initial spec approved. Rust API (Cargo workspace, ~20 crates), TypeScript SDK + client, Python PDF sidecar, Timescale Cloud, Fly.io. Full testing + CI + benchmarking baked in. 65 issues across 5 milestones tracked in GitHub.
