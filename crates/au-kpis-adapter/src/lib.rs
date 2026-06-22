//! Adapter trait + base helpers (discover/fetch/parse).
//!
//! Source-specific crates implement [`SourceAdapter`] and register values in an
//! [`Adapters`] registry. The ingestion pipeline can then dispatch discovery,
//! fetch, and streaming parse work by source id without depending on any
//! concrete adapter crate.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    io::{BufRead, BufReader, Cursor},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use async_trait::async_trait;
use au_kpis_domain::{
    Artifact, Dataflow, DataflowId, Observation, ResponseHeaders, SeriesDescriptor, Source,
    SourceId, ids::ArtifactId,
};
use au_kpis_error::{Classify, CoreError, ErrorClass};
use au_kpis_storage::{BlobStore, StorageError, StorageKey};
use chrono::{DateTime, NaiveDate, Utc};
use futures::stream::BoxStream;
use quick_xml::{Reader as XmlReader, events::Event};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{sync::Mutex, time::sleep};
use tokio_util::sync::CancellationToken;
use zip::ZipArchive;

const XLSX_MAX_COLUMN: u32 = 16_384;
const XLSX_MAX_ROW: u32 = 1_048_576;
const TRANSIENT_SEND_RETRIES: usize = 2;
const TRANSIENT_SEND_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);

/// Streaming observation payload emitted by adapters during parse.
pub type ObservationStream<'a> =
    BoxStream<'a, Result<(SeriesDescriptor, Observation), AdapterError>>;

/// Shared artifact provenance recorder used by fetch contexts.
pub type ArtifactRecorderRef = Arc<dyn ArtifactRecorder>;

/// Persists artifact provenance after a fetch stores raw bytes.
#[async_trait]
pub trait ArtifactRecorder: fmt::Debug + Send + Sync + 'static {
    /// Load durable blob metadata by content id, when one already exists.
    async fn get(&self, id: ArtifactId) -> Result<Option<Artifact>, AdapterError>;

    /// Persist one fetched artifact row.
    async fn record(&self, artifact: &Artifact) -> Result<Artifact, AdapterError>;

    /// Repair a durable row whose storage key no longer points at an object.
    async fn repair_storage_key(
        &self,
        artifact: &Artifact,
        observed_storage_key: &str,
    ) -> Result<Artifact, AdapterError>;
}

/// Capture an HTTP header map for artifact provenance without silently
/// dropping non-visible-ASCII values.
///
/// Values that cannot be represented as `HeaderValue::to_str()` are encoded as
/// lower-case hex with a `bytes:hex:` prefix so the original bytes remain
/// recoverable. Text values that would collide with that reserved prefix are
/// escaped with `text:`.
#[must_use]
pub fn capture_response_headers(headers: &reqwest::header::HeaderMap) -> ResponseHeaders {
    let mut captured = ResponseHeaders::new();
    for (name, value) in headers {
        captured
            .entry(name.as_str().to_string())
            .or_default()
            .push(header_value_for_audit(value));
    }
    captured
}

/// Parse `Retry-After` delta-seconds from captured response headers.
#[must_use]
pub fn retry_after_delta(headers: &ResponseHeaders) -> Option<Duration> {
    headers
        .get("retry-after")
        .and_then(|values| values.first())
        .and_then(|value| {
            value
                .parse::<u64>()
                .map(Duration::from_secs)
                .ok()
                .or_else(|| retry_after_http_date(value))
        })
}

/// Validate worksheet cell references before handing XLSX bytes to downstream
/// workbook parsers.
///
/// Malformed cell coordinates have triggered panics in third-party XLSX
/// readers. This helper treats invalid ZIP/XML/cell references as source
/// format drift and leaves non-XLSX byte streams untouched.
pub fn validate_xlsx_workbook_cell_refs(bytes: &[u8], source: &str) -> Result<(), AdapterError> {
    if !bytes.starts_with(b"PK") {
        return Ok(());
    }

    let mut archive = ZipArchive::new(Cursor::new(bytes)).map_err(|err| {
        AdapterError::FormatDrift(format!("{source} XLSX ZIP is unreadable: {err}"))
    })?;
    for index in 0..archive.len() {
        let entry = archive.by_index(index).map_err(|err| {
            AdapterError::FormatDrift(format!("{source} XLSX ZIP entry is unreadable: {err}"))
        })?;
        let name = entry.name().to_string();
        if name.starts_with("xl/worksheets/") && name.ends_with(".xml") {
            validate_xlsx_worksheet_cell_refs(source, &name, BufReader::new(entry))?;
        }
    }
    Ok(())
}

fn validate_xlsx_worksheet_cell_refs<R: BufRead>(
    source: &str,
    sheet_name: &str,
    xml: R,
) -> Result<(), AdapterError> {
    let mut reader = XmlReader::from_reader(xml);
    let mut buffer = Vec::new();
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element) | Event::Empty(element))
                if element.local_name().as_ref() == b"c" =>
            {
                for attribute in element.attributes().with_checks(false) {
                    let attribute = attribute.map_err(|err| {
                        AdapterError::FormatDrift(format!(
                            "{source} XLSX worksheet `{sheet_name}` has invalid XML attributes: {err}"
                        ))
                    })?;
                    if attribute.key.as_ref() == b"r" {
                        let reference =
                            attribute
                                .decode_and_unescape_value(reader.decoder())
                                .map_err(|err| {
                                    AdapterError::FormatDrift(format!(
                                        "{source} XLSX worksheet `{sheet_name}` has invalid cell reference: {err}"
                                    ))
                                })?;
                        validate_xlsx_cell_reference(source, sheet_name, &reference)?;
                    }
                }
            }
            Ok(Event::Eof) => return Ok(()),
            Ok(_) => {}
            Err(err) => {
                return Err(AdapterError::FormatDrift(format!(
                    "{source} XLSX worksheet `{sheet_name}` is malformed XML: {err}"
                )));
            }
        }
        buffer.clear();
    }
}

fn validate_xlsx_cell_reference(
    source: &str,
    sheet_name: &str,
    reference: &str,
) -> Result<(), AdapterError> {
    let mut bytes = reference.bytes().peekable();
    let _ = bytes.next_if_eq(&b'$');

    let mut column = 0_u32;
    let mut has_column = false;
    while let Some(byte) = bytes.next_if(|byte| byte.is_ascii_alphabetic()) {
        has_column = true;
        let value = u32::from(byte.to_ascii_uppercase() - b'A' + 1);
        column = column
            .checked_mul(26)
            .and_then(|current| current.checked_add(value))
            .ok_or_else(|| invalid_xlsx_cell_reference(source, sheet_name, reference))?;
        if column > XLSX_MAX_COLUMN {
            return Err(invalid_xlsx_cell_reference(source, sheet_name, reference));
        }
    }

    let _ = bytes.next_if_eq(&b'$');
    let mut row = 0_u32;
    let mut has_row = false;
    while let Some(byte) = bytes.next_if(|byte| byte.is_ascii_digit()) {
        has_row = true;
        row = row
            .checked_mul(10)
            .and_then(|current| current.checked_add(u32::from(byte - b'0')))
            .ok_or_else(|| invalid_xlsx_cell_reference(source, sheet_name, reference))?;
        if row > XLSX_MAX_ROW {
            return Err(invalid_xlsx_cell_reference(source, sheet_name, reference));
        }
    }

    if !has_column || !has_row || row == 0 || bytes.peek().is_some() {
        return Err(invalid_xlsx_cell_reference(source, sheet_name, reference));
    }

    Ok(())
}

fn invalid_xlsx_cell_reference(source: &str, sheet_name: &str, reference: &str) -> AdapterError {
    AdapterError::FormatDrift(format!(
        "invalid {source} XLSX worksheet cell reference `{reference}` in `{sheet_name}`"
    ))
}

fn retry_after_http_date(value: &str) -> Option<Duration> {
    let deadline = httpdate::parse_http_date(value).ok()?;
    Some(
        deadline
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO),
    )
}

fn header_value_for_audit(value: &reqwest::header::HeaderValue) -> String {
    value.to_str().map_or_else(
        |_| format!("bytes:hex:{}", hex_lower(value.as_bytes())),
        |text| {
            if text.starts_with("bytes:") || text.starts_with("text:") {
                format!("text:{text}")
            } else {
                text.to_string()
            }
        },
    )
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

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

/// Inclusive-start, exclusive-end artifact date range for parser version routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactDateRange {
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
}

impl ArtifactDateRange {
    /// Match artifacts before `end`.
    #[must_use]
    pub const fn before(end: NaiveDate) -> Self {
        Self {
            start: None,
            end: Some(end),
        }
    }

    /// Match artifacts from `start` onward.
    #[must_use]
    pub const fn from(start: NaiveDate) -> Self {
        Self {
            start: Some(start),
            end: None,
        }
    }

    /// Match artifacts from `start`, excluding `end`.
    #[must_use]
    pub const fn between(start: NaiveDate, end: NaiveDate) -> Self {
        Self {
            start: Some(start),
            end: Some(end),
        }
    }

    /// `true` when `artifact_date` is inside this range.
    #[must_use]
    pub fn contains(self, artifact_date: NaiveDate) -> bool {
        let after_start = match self.start {
            Some(start) => artifact_date >= start,
            None => true,
        };
        let before_end = match self.end {
            Some(end) => artifact_date < end,
            None => true,
        };
        after_start && before_end
    }
}

/// One named parser version and the artifact dates it owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserVersion {
    name: String,
    artifact_dates: ArtifactDateRange,
}

impl ParserVersion {
    /// Construct a parser version selector.
    #[must_use]
    pub fn new(name: impl Into<String>, artifact_dates: ArtifactDateRange) -> Self {
        Self {
            name: name.into(),
            artifact_dates,
        }
    }

    /// Stable parser version name, commonly `parse_v1` or `parse_v2`.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Date range owned by this parser version.
    #[must_use]
    pub const fn artifact_dates(&self) -> ArtifactDateRange {
        self.artifact_dates
    }
}

/// Select exactly one parser version for an artifact date.
pub fn select_parser_version(
    versions: &[ParserVersion],
    artifact_date: NaiveDate,
) -> Result<&ParserVersion, AdapterError> {
    if versions.is_empty() {
        return Err(AdapterError::Validation(
            "at least one parser version must be configured".into(),
        ));
    }
    for version in versions {
        if version.name.trim().is_empty() {
            return Err(AdapterError::Validation(
                "parser version name must not be empty".into(),
            ));
        }
    }

    let matching = versions
        .iter()
        .filter(|version| version.artifact_dates.contains(artifact_date))
        .collect::<Vec<_>>();
    match matching.as_slice() {
        [version] => Ok(*version),
        [] => Err(AdapterError::FormatDrift(format!(
            "no parser version covers artifact date `{artifact_date}`"
        ))),
        many => Err(AdapterError::Validation(format!(
            "parser version date ranges overlap for artifact date `{artifact_date}`: {}",
            many.iter()
                .map(|version| version.name())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

/// Expected schema hash for one source-specific parser/table shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedSchemaHash {
    source_id: SourceId,
    dataflow_id: DataflowId,
    parser_version: String,
    schema_key: String,
    expected_hash: String,
}

impl ExpectedSchemaHash {
    /// Construct and validate a schema-hash expectation.
    pub fn new(
        source_id: SourceId,
        dataflow_id: DataflowId,
        parser_version: impl Into<String>,
        schema_key: impl Into<String>,
        expected_hash: impl Into<String>,
    ) -> Result<Self, AdapterError> {
        let parser_version = parser_version.into();
        let schema_key = schema_key.into();
        let expected_hash = expected_hash.into();
        for (field, value) in [
            ("parser version", parser_version.as_str()),
            ("schema key", schema_key.as_str()),
            ("expected schema hash", expected_hash.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(AdapterError::Validation(format!(
                    "{field} must not be empty for schema drift detection"
                )));
            }
        }
        Ok(Self {
            source_id,
            dataflow_id,
            parser_version,
            schema_key,
            expected_hash,
        })
    }
}

/// Structured context for a schema-hash drift event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaHashDrift {
    /// Source whose parser observed drift.
    pub source_id: SourceId,
    /// Dataflow being parsed when drift was detected.
    pub dataflow_id: DataflowId,
    /// Parser version selected for the artifact date.
    pub parser_version: String,
    /// Source-specific table, sheet, or schema identifier.
    pub schema_key: String,
    /// Committed schema hash expected by the selected parser.
    pub expected_hash: String,
    /// Schema hash observed on the artifact.
    pub actual_hash: String,
}

impl fmt::Display for SchemaHashDrift {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "schema hash drift for source `{}` dataflow `{}` parser `{}` schema `{}`: expected `{}`, got `{}`",
            self.source_id.as_str(),
            self.dataflow_id.as_str(),
            self.parser_version,
            self.schema_key,
            self.expected_hash,
            self.actual_hash
        )
    }
}

/// Validate an observed schema hash against the selected parser's expectation.
pub fn validate_schema_hash(
    expected: &ExpectedSchemaHash,
    actual_hash: &str,
) -> Result<(), AdapterError> {
    if actual_hash.trim().is_empty() {
        return Err(AdapterError::Validation(
            "actual schema hash must not be empty for schema drift detection".into(),
        ));
    }
    if expected.expected_hash == actual_hash {
        return Ok(());
    }

    let drift = SchemaHashDrift {
        source_id: expected.source_id.clone(),
        dataflow_id: expected.dataflow_id.clone(),
        parser_version: expected.parser_version.clone(),
        schema_key: expected.schema_key.clone(),
        expected_hash: expected.expected_hash.clone(),
        actual_hash: actual_hash.to_string(),
    };
    tracing::error!(
        target: "au_kpis_adapter::schema_hash",
        source = drift.source_id.as_str(),
        dataflow = drift.dataflow_id.as_str(),
        parser_version = drift.parser_version.as_str(),
        schema_key = drift.schema_key.as_str(),
        expected_hash = drift.expected_hash.as_str(),
        actual_hash = drift.actual_hash.as_str(),
        "schema hash drift detected"
    );
    Err(AdapterError::SchemaHashDrift(Box::new(drift)))
}

/// Rate-limited HTTP client shared by adapter contexts.
#[derive(Clone)]
pub struct AdapterHttpClient {
    client: reqwest::Client,
    raw_artifact_client: reqwest::Client,
    limiter: Arc<RateLimiter>,
    circuit_breaker: Arc<CircuitBreaker>,
}

impl fmt::Debug for AdapterHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdapterHttpClient")
            .field("rate_limit", &self.limiter.limit)
            .field("circuit_breaker", &self.circuit_breaker)
            .finish_non_exhaustive()
    }
}

impl AdapterHttpClient {
    /// Build a client with the source's declared rate limit.
    ///
    /// The default client keeps reqwest's ordinary redirect and decompression
    /// behaviour for discovery and metadata requests.
    pub fn new(rate_limit: RateLimit) -> Self {
        let client = reqwest::Client::builder()
            .http1_only()
            .build()
            .expect("static reqwest client configuration is valid");
        let raw_artifact_client = reqwest::Client::builder()
            .http1_only()
            .redirect(reqwest::redirect::Policy::none())
            .no_gzip()
            .no_brotli()
            .build()
            .expect("static reqwest client configuration is valid");
        Self {
            client,
            raw_artifact_client,
            limiter: Arc::new(RateLimiter::new(rate_limit)),
            circuit_breaker: Arc::new(CircuitBreaker::new(CircuitBreakerConfig::default())),
        }
    }

    /// Borrow the underlying client for request builders not covered by helpers.
    #[must_use]
    pub fn raw(&self) -> &reqwest::Client {
        &self.client
    }

    /// Borrow the non-decompressing client for raw artifact persistence.
    #[must_use]
    pub fn raw_artifact(&self) -> &reqwest::Client {
        &self.raw_artifact_client
    }

    /// Send a request after waiting for a rate-limit permit.
    #[tracing::instrument(skip(self, request))]
    pub async fn execute(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, AdapterError> {
        let mut request = request;
        for attempt in 0..=TRANSIENT_SEND_RETRIES {
            let retry_request = if attempt < TRANSIENT_SEND_RETRIES {
                request.try_clone()
            } else {
                None
            };

            self.circuit_breaker.before_request().await?;
            self.limiter.wait_for_permit().await;
            match request.send().await {
                Ok(response) => {
                    if response.status().is_server_error() {
                        self.circuit_breaker.record_failure().await;
                    } else {
                        self.circuit_breaker.record_success().await;
                    }
                    return Ok(response);
                }
                Err(err) if retry_request.is_some() && is_transient_send_error(&err) => {
                    self.circuit_breaker.record_failure().await;
                    sleep(TRANSIENT_SEND_RETRY_BASE_DELAY * (attempt as u32 + 1)).await;
                    request = retry_request.expect("checked above");
                }
                Err(err) => {
                    self.circuit_breaker.record_failure().await;
                    return Err(err.into());
                }
            }
        }

        unreachable!("bounded retry loop returns on success or final error")
    }

    /// Convenience `GET` helper using the shared rate limiter.
    #[tracing::instrument(skip(self), fields(url = %url))]
    pub async fn get(&self, url: &str) -> Result<reqwest::Response, AdapterError> {
        self.execute(self.client.get(url)).await
    }
}

fn is_transient_send_error(err: &reqwest::Error) -> bool {
    !err.is_builder()
        && !err.is_redirect()
        && !err.is_status()
        && (err.is_request() || err.is_connect() || err.is_timeout() || err.is_body())
}

#[derive(Debug, Clone, Copy)]
struct CircuitBreakerConfig {
    min_samples: usize,
    failure_ratio: f64,
    open_for: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            min_samples: 50,
            failure_ratio: 0.20,
            open_for: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
struct CircuitBreaker {
    config: CircuitBreakerConfig,
    inner: Mutex<CircuitBreakerState>,
}

impl CircuitBreaker {
    fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(CircuitBreakerState {
                outcomes: VecDeque::with_capacity(config.min_samples),
                state: CircuitState::Closed,
            }),
        }
    }

    async fn before_request(&self) -> Result<(), AdapterError> {
        let now = Instant::now();
        let mut inner = self.inner.lock().await;
        match inner.state {
            CircuitState::Closed | CircuitState::HalfOpen => Ok(()),
            CircuitState::Open { until } if now >= until => {
                inner.state = CircuitState::HalfOpen;
                Ok(())
            }
            CircuitState::Open { until } => Err(AdapterError::CircuitOpen {
                retry_after: until.saturating_duration_since(now),
            }),
        }
    }

    async fn record_success(&self) {
        self.record(false).await;
    }

    async fn record_failure(&self) {
        self.record(true).await;
    }

    async fn record(&self, failed: bool) {
        let mut inner = self.inner.lock().await;
        match inner.state {
            CircuitState::HalfOpen if failed => {
                inner.outcomes.clear();
                inner.state = CircuitState::Open {
                    until: Instant::now() + self.config.open_for,
                };
            }
            CircuitState::HalfOpen => {
                inner.outcomes.clear();
                inner.state = CircuitState::Closed;
            }
            CircuitState::Open { .. } => {}
            CircuitState::Closed => {
                if inner.outcomes.len() == self.config.min_samples {
                    inner.outcomes.pop_front();
                }
                inner.outcomes.push_back(failed);
                if self.should_open(&inner.outcomes) {
                    inner.state = CircuitState::Open {
                        until: Instant::now() + self.config.open_for,
                    };
                }
            }
        }
    }

    fn should_open(&self, outcomes: &VecDeque<bool>) -> bool {
        if outcomes.len() < self.config.min_samples {
            return false;
        }
        let failures = outcomes.iter().filter(|failed| **failed).count();
        failures as f64 / outcomes.len() as f64 > self.config.failure_ratio
    }
}

#[derive(Debug, Clone, Copy)]
enum CircuitState {
    Closed,
    Open { until: Instant },
    HalfOpen,
}

#[derive(Debug)]
struct CircuitBreakerState {
    outcomes: VecDeque<bool>,
    state: CircuitState,
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
    /// W3C trace-parent tying discovery output to downstream fetch, parse, and load work.
    trace_parent: Option<String>,
    /// Optional dataflow scope when discovery should emit only one dataflow's jobs.
    requested_dataflow_id: Option<DataflowId>,
}

impl DiscoveryCtx {
    /// Construct a discovery context.
    #[must_use]
    pub fn new(http: AdapterHttpClient, started_at: DateTime<Utc>) -> Self {
        Self {
            http,
            started_at,
            known_revisions: BTreeMap::new(),
            trace_parent: None,
            requested_dataflow_id: None,
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

    /// Return a context annotated with the run-level trace parent.
    #[must_use]
    pub fn with_trace_parent(mut self, trace_parent: impl Into<String>) -> Self {
        self.trace_parent = Some(trace_parent.into());
        self
    }

    /// Return a context scoped to one requested dataflow.
    #[must_use]
    pub fn with_requested_dataflow_id(mut self, dataflow_id: DataflowId) -> Self {
        self.requested_dataflow_id = Some(dataflow_id);
        self
    }

    /// Borrow the stored upstream revisions for this discovery run.
    #[must_use]
    pub const fn known_revisions(&self) -> &BTreeMap<String, UpstreamRevision> {
        &self.known_revisions
    }

    /// W3C trace-parent carried by jobs emitted from this discovery run.
    #[must_use]
    pub fn trace_parent(&self) -> Option<&str> {
        self.trace_parent.as_deref()
    }

    /// Optional dataflow scope for this discovery run.
    #[must_use]
    pub fn requested_dataflow_id(&self) -> Option<&DataflowId> {
        self.requested_dataflow_id.as_ref()
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
    artifact_recorder: ArtifactRecorderRef,
    cancellation: CancellationToken,
}

impl FetchCtx {
    /// Construct a fetch context.
    #[must_use]
    pub fn new(
        http: AdapterHttpClient,
        blob_store: BlobStore,
        started_at: DateTime<Utc>,
        artifact_recorder: ArtifactRecorderRef,
    ) -> Self {
        Self {
            http,
            blob_store,
            started_at,
            artifact_recorder,
            cancellation: CancellationToken::new(),
        }
    }

    /// Return a context bound to the orchestrator's cancellation token so
    /// adapters can abort long-running fetch work during shutdown.
    #[must_use]
    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    /// Cancellation token shared with the orchestrator.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Convenience predicate equivalent to `self.cancellation().is_cancelled()`.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Persist fetched artifact provenance, then return the parse reference.
    pub async fn persist_artifact(&self, artifact: Artifact) -> Result<ArtifactRef, AdapterError> {
        Ok(self.artifact_recorder.record(&artifact).await?.into())
    }

    /// Load durable blob metadata for a content id, if present.
    pub async fn get_artifact(&self, id: ArtifactId) -> Result<Option<ArtifactRef>, AdapterError> {
        Ok(self.artifact_recorder.get(id).await?.map(Into::into))
    }

    /// Point durable provenance back at a known-good storage key.
    pub async fn repair_artifact_storage_key(
        &self,
        artifact: Artifact,
        observed_storage_key: &str,
    ) -> Result<ArtifactRef, AdapterError> {
        Ok(self
            .artifact_recorder
            .repair_storage_key(&artifact, observed_storage_key)
            .await?
            .into())
    }

    /// Delete a storage key that is known not to be the durable artifact row.
    pub async fn delete_artifact(&self, storage_key: &str) -> Result<(), AdapterError> {
        self.blob_store
            .delete(&StorageKey::from_persisted(storage_key))
            .await?;
        Ok(())
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
    expected_dataflow_id: Option<DataflowId>,
    job_id: Option<String>,
    trace_parent: Option<String>,
    metadata: BTreeMap<String, String>,
    cancellation: CancellationToken,
}

impl ParseCtx {
    /// Construct a parse context.
    #[must_use]
    pub fn new(http: AdapterHttpClient, blob_store: BlobStore, started_at: DateTime<Utc>) -> Self {
        Self {
            http,
            blob_store,
            started_at,
            expected_dataflow_id: None,
            job_id: None,
            trace_parent: None,
            metadata: BTreeMap::new(),
            cancellation: CancellationToken::new(),
        }
    }

    /// Return a context annotated with discovery-time dataflow provenance.
    #[must_use]
    pub fn with_expected_dataflow(
        mut self,
        dataflow_id: DataflowId,
        metadata: BTreeMap<String, String>,
    ) -> Self {
        self.expected_dataflow_id = Some(dataflow_id);
        self.metadata = metadata;
        self
    }

    /// Return a context annotated with discovery-time job correlation.
    #[must_use]
    pub fn with_job_correlation(
        mut self,
        job_id: impl Into<String>,
        trace_parent: Option<String>,
    ) -> Self {
        self.job_id = Some(job_id.into());
        self.trace_parent = trace_parent;
        self
    }

    /// Return a context bound to the orchestrator's cancellation token so the
    /// adapter parse stage can observe shutdown end-to-end.
    #[must_use]
    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    /// Expected dataflow carried from the discovered job, when available.
    #[must_use]
    pub fn expected_dataflow_id(&self) -> Option<&DataflowId> {
        self.expected_dataflow_id.as_ref()
    }

    /// Source-local discovered job id carried through fetch and parse.
    #[must_use]
    pub fn job_id(&self) -> Option<&str> {
        self.job_id.as_deref()
    }

    /// W3C trace-parent carried from discovery, when available.
    #[must_use]
    pub fn trace_parent(&self) -> Option<&str> {
        self.trace_parent.as_deref()
    }

    /// Adapter metadata carried from the discovered job.
    #[must_use]
    pub const fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    /// Cancellation token shared with the orchestrator. Adapters that perform
    /// long-running or CPU-heavy parse work should poll this token (or `select!`
    /// on its `cancelled()` future) so shutdown completes within the configured
    /// grace window.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Convenience predicate equivalent to `self.cancellation().is_cancelled()`.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
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
    /// W3C trace-parent tying discovery, fetch, parse, and load spans together.
    pub trace_parent: Option<String>,
    /// Adapter-specific metadata needed by fetch/parse.
    pub metadata: BTreeMap<String, String>,
}

/// Lightweight reference to a fetched artifact used by parse jobs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    /// Content-addressed artifact id.
    pub id: ArtifactId,
    /// Durable fetch provenance row for this parse reference, when persisted.
    pub fetch_id: Option<i64>,
    /// Source that produced the artifact.
    pub source_id: SourceId,
    /// Canonical upstream URL.
    pub source_url: String,
    /// MIME-style content type.
    pub content_type: String,
    /// HTTP response headers captured when the artifact was fetched, retaining
    /// repeated values for the same header name.
    pub response_headers: ResponseHeaders,
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
            fetch_id: artifact.fetch_id,
            source_id: artifact.source_id,
            source_url: artifact.source_url,
            content_type: artifact.content_type,
            response_headers: artifact.response_headers,
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
            fetch_id: reference.fetch_id,
            source_id: reference.source_id,
            source_url: reference.source_url,
            content_type: reference.content_type,
            response_headers: reference.response_headers,
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

    /// Static source metadata that orchestration can sync into the catalog.
    fn source_metadata(&self) -> Option<Source> {
        None
    }

    /// Static dataflow metadata that orchestration can sync into the catalog.
    fn dataflow_metadata(&self) -> Vec<Dataflow> {
        Vec::new()
    }

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

    /// Iterate over registered adapters.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn SourceAdapter>> {
        self.by_id.values()
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

    /// Upstream returned a non-success status and associated retry/provenance metadata.
    #[error("upstream status {status}")]
    UpstreamStatus {
        /// HTTP status code returned by the upstream source.
        status: reqwest::StatusCode,
        /// Parsed delta-seconds from `Retry-After`, when supplied in that form.
        retry_after: Option<Duration>,
        /// Response headers captured from the failed upstream response.
        response_headers: ResponseHeaders,
    },

    /// Source circuit breaker is open after recent transient upstream failures.
    #[error("source circuit breaker open")]
    CircuitOpen {
        /// Duration callers should wait before probing the source again.
        retry_after: Duration,
    },

    /// Persisting fetched artifact provenance failed.
    #[error("artifact provenance: {message}")]
    ArtifactRecord {
        /// Human-readable persistence failure.
        message: String,
        /// Retry classification supplied by the persistence layer.
        class: ErrorClass,
    },

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

    /// Observed schema hash changed for the selected source parser version.
    #[error("{0}")]
    SchemaHashDrift(Box<SchemaHashDrift>),

    /// Caller-supplied or adapter-produced data violated a precondition.
    #[error("validation: {0}")]
    Validation(String),
}

impl AdapterError {
    /// Construct an artifact provenance persistence error.
    #[must_use]
    pub fn artifact_record(message: impl Into<String>, class: ErrorClass) -> Self {
        Self::ArtifactRecord {
            message: message.into(),
            class,
        }
    }
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
            AdapterError::UpstreamStatus { status, .. } => {
                if matches!(status.as_u16(), 408 | 409 | 425 | 429) || status.is_server_error() {
                    ErrorClass::Transient
                } else {
                    ErrorClass::Permanent
                }
            }
            AdapterError::CircuitOpen { .. } => ErrorClass::Transient,
            AdapterError::ArtifactRecord { class, .. } => *class,
            AdapterError::Storage(err) => err.class(),
            AdapterError::UnknownAdapter(_)
            | AdapterError::DuplicateAdapter(_)
            | AdapterError::FormatDrift(_)
            | AdapterError::SchemaHashDrift(_) => ErrorClass::Permanent,
            AdapterError::Validation(_) => ErrorClass::Validation,
        }
    }

    fn retry_after(&self) -> Option<Duration> {
        match self {
            AdapterError::UpstreamStatus { retry_after, .. } => *retry_after,
            AdapterError::CircuitOpen { retry_after } => Some(*retry_after),
            _ => None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn circuit_breaker_opens_after_failure_ratio_exceeds_window_threshold() {
        let breaker = CircuitBreaker::new(CircuitBreakerConfig {
            min_samples: 50,
            failure_ratio: 0.20,
            open_for: Duration::from_millis(5),
        });

        for _ in 0..40 {
            breaker.record_success().await;
        }
        for _ in 0..10 {
            breaker.record_failure().await;
        }
        assert!(
            breaker.before_request().await.is_ok(),
            "exactly 20% failures must not open the circuit"
        );

        breaker.record_failure().await;
        let err = breaker
            .before_request()
            .await
            .expect_err("more than 20% failures in the window should open");
        assert!(
            matches!(err, AdapterError::CircuitOpen { retry_after } if retry_after > Duration::ZERO)
        );
    }

    #[tokio::test]
    async fn circuit_breaker_recovers_after_cooldown_and_half_open_success() {
        let breaker = CircuitBreaker::new(CircuitBreakerConfig {
            min_samples: 3,
            failure_ratio: 0.20,
            open_for: Duration::from_millis(5),
        });

        breaker.record_failure().await;
        breaker.record_failure().await;
        breaker.record_failure().await;
        assert!(matches!(
            breaker.before_request().await,
            Err(AdapterError::CircuitOpen { .. })
        ));

        sleep(Duration::from_millis(6)).await;
        breaker
            .before_request()
            .await
            .expect("cooldown should allow a half-open probe");
        breaker.record_success().await;
        breaker
            .before_request()
            .await
            .expect("successful half-open probe should close the circuit");
    }
}
