//! Adapter trait + base helpers (discover/fetch/parse).
//!
//! Source-specific crates implement [`SourceAdapter`] and register values in an
//! [`Adapters`] registry. The ingestion pipeline can then dispatch discovery,
//! fetch, and streaming parse work by source id without depending on any
//! concrete adapter crate.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{
    collections::BTreeMap,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use au_kpis_domain::{
    Artifact, DataflowId, Observation, SeriesDescriptor, SourceId, ids::ArtifactId,
};
use au_kpis_error::{Classify, CoreError, ErrorClass};
use au_kpis_storage::{BlobStore, StorageError};
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::Mutex, time::sleep};

/// Streaming observation payload emitted by adapters during parse.
pub type ObservationStream<'a> =
    BoxStream<'a, Result<(SeriesDescriptor, Observation), AdapterError>>;

/// Per-source HTTP rate-limit declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimit {
    /// Maximum requests allowed during [`Self::per`].
    pub max_requests: u32,
    /// Window over which [`Self::max_requests`] is measured.
    #[serde(with = "duration_millis")]
    pub per: Duration,
}

impl RateLimit {
    /// Construct a validated rate limit.
    pub fn new(max_requests: u32, per: Duration) -> Result<Self, AdapterError> {
        if max_requests == 0 {
            return Err(AdapterError::Validation(
                "rate-limit max_requests must be greater than zero".into(),
            ));
        }
        if per.is_zero() {
            return Err(AdapterError::Validation(
                "rate-limit window must be greater than zero".into(),
            ));
        }
        Ok(Self { max_requests, per })
    }

    fn spacing(self) -> Duration {
        let per_nanos = self.per.as_nanos();
        let spacing_nanos = (per_nanos / u128::from(self.max_requests)).max(1);
        let capped = spacing_nanos.min(u128::from(u64::MAX));
        Duration::from_nanos(capped as u64)
    }
}

/// Static metadata and operational policy for an adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterManifest {
    /// Stable source id, e.g. `abs`.
    pub source_id: SourceId,
    /// Human-readable source name.
    pub name: String,
    /// Adapter crate version or upstream parser version.
    pub version: String,
    /// Default source rate limit enforced by [`AdapterHttpClient`].
    pub rate_limit: RateLimit,
    /// Dataflows this adapter can emit.
    pub dataflows: Vec<DataflowId>,
}

/// Rate-limited HTTP client shared by adapter contexts.
#[derive(Clone)]
pub struct AdapterHttpClient {
    client: reqwest::Client,
    limiter: Arc<RateLimiter>,
}

impl fmt::Debug for AdapterHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdapterHttpClient")
            .field("rate_limit", &self.limiter.limit)
            .finish_non_exhaustive()
    }
}

impl AdapterHttpClient {
    /// Build a client with the source's declared rate limit.
    pub fn new(rate_limit: RateLimit) -> Self {
        Self::from_client(reqwest::Client::new(), rate_limit)
    }

    /// Wrap an existing `reqwest` client with the source's declared rate limit.
    pub fn from_client(client: reqwest::Client, rate_limit: RateLimit) -> Self {
        Self {
            client,
            limiter: Arc::new(RateLimiter::new(rate_limit)),
        }
    }

    /// Borrow the underlying client for request builders not covered by helpers.
    #[must_use]
    pub fn raw(&self) -> &reqwest::Client {
        &self.client
    }

    /// Send a request after waiting for a rate-limit permit.
    #[tracing::instrument(skip(self, request))]
    pub async fn execute(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, AdapterError> {
        self.limiter.wait_for_permit().await;
        Ok(request.send().await?)
    }

    /// Convenience `GET` helper using the shared rate limiter.
    #[tracing::instrument(skip(self), fields(url = %url))]
    pub async fn get(&self, url: &str) -> Result<reqwest::Response, AdapterError> {
        self.execute(self.client.get(url)).await
    }
}

#[derive(Debug)]
struct RateLimiter {
    limit: RateLimit,
    next_permit: Mutex<Instant>,
}

impl RateLimiter {
    fn new(limit: RateLimit) -> Self {
        Self {
            limit,
            next_permit: Mutex::new(Instant::now()),
        }
    }

    #[tracing::instrument(skip(self), fields(max_requests = self.limit.max_requests))]
    async fn wait_for_permit(&self) {
        loop {
            let now = Instant::now();
            let wait = {
                let mut next = self.next_permit.lock().await;
                if now >= *next {
                    *next = now + self.limit.spacing();
                    None
                } else {
                    Some(*next - now)
                }
            };

            match wait {
                Some(delay) => sleep(delay).await,
                None => return,
            }
        }
    }
}

/// Context supplied to adapter discovery.
#[derive(Debug, Clone)]
pub struct DiscoveryCtx {
    /// Rate-limited HTTP client for upstream metadata requests.
    pub http: AdapterHttpClient,
    /// Timestamp captured by the scheduler when discovery started.
    pub started_at: DateTime<Utc>,
    /// Stored upstream revisions for this discovery run, keyed by adapter-defined upstream identity.
    pub known_revisions: BTreeMap<String, UpstreamRevision>,
}

impl DiscoveryCtx {
    /// Construct a discovery context.
    #[must_use]
    pub fn new(http: AdapterHttpClient, started_at: DateTime<Utc>) -> Self {
        Self {
            http,
            started_at,
            known_revisions: BTreeMap::new(),
        }
    }

    /// Add one stored upstream revision to this discovery run.
    #[must_use]
    pub fn with_known_revision(
        mut self,
        key: impl Into<String>,
        revision: UpstreamRevision,
    ) -> Self {
        self.known_revisions.insert(key.into(), revision);
        self
    }

    /// Borrow the stored upstream revisions for this discovery run.
    #[must_use]
    pub const fn known_revisions(&self) -> &BTreeMap<String, UpstreamRevision> {
        &self.known_revisions
    }
}

/// Stored upstream revision metadata supplied to adapter discovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamRevision {
    version: String,
    last_updated: Option<String>,
}

impl UpstreamRevision {
    /// Construct a stored upstream revision.
    #[must_use]
    pub fn new(version: impl Into<String>, last_updated: Option<impl Into<String>>) -> Self {
        Self {
            version: version.into(),
            last_updated: last_updated.map(Into::into),
        }
    }

    /// Upstream version string.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Upstream update timestamp when exposed by the source.
    #[must_use]
    pub fn last_updated(&self) -> Option<&str> {
        self.last_updated.as_deref()
    }
}

/// Context supplied to adapter fetch jobs.
#[derive(Debug, Clone)]
pub struct FetchCtx {
    /// Rate-limited HTTP client for upstream artifact downloads.
    pub http: AdapterHttpClient,
    /// Content-addressed blob store for raw source artifacts.
    pub blob_store: BlobStore,
    /// Timestamp captured by the worker when fetch started.
    pub started_at: DateTime<Utc>,
}

impl FetchCtx {
    /// Construct a fetch context.
    #[must_use]
    pub fn new(http: AdapterHttpClient, blob_store: BlobStore, started_at: DateTime<Utc>) -> Self {
        Self {
            http,
            blob_store,
            started_at,
        }
    }
}

/// Context supplied to streaming parsers.
#[derive(Debug, Clone)]
pub struct ParseCtx {
    /// Rate-limited HTTP client for parser-side follow-up requests.
    pub http: AdapterHttpClient,
    /// Blob store used to read persisted artifacts.
    pub blob_store: BlobStore,
    /// Timestamp captured by the worker when parse started.
    pub started_at: DateTime<Utc>,
}

impl ParseCtx {
    /// Construct a parse context.
    #[must_use]
    pub fn new(http: AdapterHttpClient, blob_store: BlobStore, started_at: DateTime<Utc>) -> Self {
        Self {
            http,
            blob_store,
            started_at,
        }
    }
}

/// Unit of work emitted by discovery and consumed by fetch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredJob {
    /// Stable source-local job id.
    pub id: String,
    /// Source that emitted the job.
    pub source_id: SourceId,
    /// Dataflow expected from the fetched artifact.
    pub dataflow_id: DataflowId,
    /// Canonical upstream URL or locator.
    pub source_url: String,
    /// Adapter-specific metadata needed by fetch/parse.
    pub metadata: BTreeMap<String, String>,
}

/// Lightweight reference to a fetched artifact used by parse jobs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    /// Content-addressed artifact id.
    pub id: ArtifactId,
    /// Source that produced the artifact.
    pub source_id: SourceId,
    /// Canonical upstream URL.
    pub source_url: String,
    /// MIME-style content type.
    pub content_type: String,
    /// Persisted storage key.
    pub storage_key: String,
    /// On-wire size in bytes.
    pub size_bytes: u64,
    /// Fetch completion timestamp.
    pub fetched_at: DateTime<Utc>,
}

impl From<Artifact> for ArtifactRef {
    fn from(artifact: Artifact) -> Self {
        Self {
            id: artifact.id,
            source_id: artifact.source_id,
            source_url: artifact.source_url,
            content_type: artifact.content_type,
            size_bytes: artifact.size_bytes,
            storage_key: artifact.storage_key,
            fetched_at: artifact.fetched_at,
        }
    }
}

impl From<ArtifactRef> for Artifact {
    fn from(reference: ArtifactRef) -> Self {
        Self {
            id: reference.id,
            source_id: reference.source_id,
            source_url: reference.source_url,
            content_type: reference.content_type,
            size_bytes: reference.size_bytes,
            storage_key: reference.storage_key,
            fetched_at: reference.fetched_at,
        }
    }
}

/// Source adapter contract implemented by each source-specific crate.
#[async_trait]
pub trait SourceAdapter: fmt::Debug + Send + Sync + 'static {
    /// Stable source id, matching [`AdapterManifest::source_id`].
    fn id(&self) -> &'static str;

    /// Static adapter metadata and operational policy.
    fn manifest(&self) -> &AdapterManifest;

    /// Discover upstream work items that should be fetched.
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError>;

    /// Fetch and persist a discovered artifact.
    async fn fetch(&self, job: DiscoveredJob, ctx: &FetchCtx) -> Result<ArtifactRef, AdapterError>;

    /// Stream parsed observations without buffering a full artifact in memory.
    fn parse<'a>(&'a self, artifact: ArtifactRef, ctx: &'a ParseCtx) -> ObservationStream<'a>;
}

/// Immutable registry of source adapters.
#[derive(Clone)]
pub struct Adapters {
    by_id: Arc<BTreeMap<String, Arc<dyn SourceAdapter>>>,
}

impl fmt::Debug for Adapters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Adapters")
            .field("ids", &self.by_id.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Adapters {
    /// Start building a registry.
    #[must_use]
    pub fn builder() -> AdaptersBuilder {
        AdaptersBuilder::default()
    }

    /// Return an adapter by id.
    pub fn get(&self, id: &str) -> Result<Arc<dyn SourceAdapter>, AdapterError> {
        self.by_id
            .get(id)
            .cloned()
            .ok_or_else(|| AdapterError::UnknownAdapter(id.to_string()))
    }

    /// Dispatch discovery by source id.
    #[tracing::instrument(skip(self, ctx), fields(source = source_id))]
    pub async fn discover(
        &self,
        source_id: &str,
        ctx: &DiscoveryCtx,
    ) -> Result<Vec<DiscoveredJob>, AdapterError> {
        self.get(source_id)?.discover(ctx).await
    }

    /// Dispatch fetch by source id.
    #[tracing::instrument(skip(self, ctx), fields(source = source_id, job_id = %job.id))]
    pub async fn fetch(
        &self,
        source_id: &str,
        job: DiscoveredJob,
        ctx: &FetchCtx,
    ) -> Result<ArtifactRef, AdapterError> {
        self.get(source_id)?.fetch(job, ctx).await
    }

    /// Dispatch parse by source id.
    pub fn parse<'a>(
        &'a self,
        source_id: &str,
        artifact: ArtifactRef,
        ctx: &'a ParseCtx,
    ) -> Result<ObservationStream<'a>, AdapterError> {
        let adapter = self
            .by_id
            .get(source_id)
            .ok_or_else(|| AdapterError::UnknownAdapter(source_id.to_string()))?;
        Ok(adapter.parse(artifact, ctx))
    }

    /// Number of registered adapters.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// `true` when no adapters are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

/// Builder for [`Adapters`].
#[derive(Debug, Default)]
pub struct AdaptersBuilder {
    by_id: BTreeMap<String, Arc<dyn SourceAdapter>>,
}

impl AdaptersBuilder {
    /// Register a concrete adapter value.
    pub fn register<A>(&mut self, adapter: A) -> Result<&mut Self, AdapterError>
    where
        A: SourceAdapter,
    {
        self.register_arc(Arc::new(adapter))
    }

    /// Register an already shared adapter.
    pub fn register_arc(
        &mut self,
        adapter: Arc<dyn SourceAdapter>,
    ) -> Result<&mut Self, AdapterError> {
        let id = adapter.id();
        let manifest_id = adapter.manifest().source_id.as_str();
        if id != manifest_id {
            return Err(AdapterError::Validation(format!(
                "adapter id `{id}` does not match manifest source id `{manifest_id}`"
            )));
        }

        if self.by_id.contains_key(id) {
            return Err(AdapterError::DuplicateAdapter(id.to_string()));
        }

        self.by_id.insert(id.to_string(), adapter);
        Ok(self)
    }

    /// Build an immutable registry.
    #[must_use]
    pub fn build(self) -> Adapters {
        Adapters {
            by_id: Arc::new(self.by_id),
        }
    }
}

/// Errors returned by adapter discovery, fetch, parse, and registry dispatch.
#[derive(Debug, Error)]
pub enum AdapterError {
    /// Shared I/O, JSON, or validation failure.
    #[error(transparent)]
    Core(#[from] CoreError),

    /// HTTP client failure.
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),

    /// Object-storage failure.
    #[error(transparent)]
    Storage(#[from] StorageError),

    /// Source-specific adapter was not registered.
    #[error("unknown adapter: {0}")]
    UnknownAdapter(String),

    /// Registry contains more than one adapter for the same source.
    #[error("duplicate adapter: {0}")]
    DuplicateAdapter(String),

    /// Upstream source format changed or failed parser expectations.
    #[error("format drift: {0}")]
    FormatDrift(String),

    /// Caller-supplied or adapter-produced data violated a precondition.
    #[error("validation: {0}")]
    Validation(String),
}

impl Classify for AdapterError {
    fn class(&self) -> ErrorClass {
        match self {
            AdapterError::Core(err) => err.class(),
            AdapterError::Http(err) => {
                if err.is_timeout() || err.is_connect() {
                    ErrorClass::Transient
                } else if err.is_decode() {
                    ErrorClass::Permanent
                } else {
                    ErrorClass::Transient
                }
            }
            AdapterError::Storage(err) => err.class(),
            AdapterError::UnknownAdapter(_)
            | AdapterError::DuplicateAdapter(_)
            | AdapterError::FormatDrift(_) => ErrorClass::Permanent,
            AdapterError::Validation(_) => ErrorClass::Validation,
        }
    }
}

mod duration_millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(
            duration
                .as_millis()
                .try_into()
                .map_err(serde::ser::Error::custom)?,
        )
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}
