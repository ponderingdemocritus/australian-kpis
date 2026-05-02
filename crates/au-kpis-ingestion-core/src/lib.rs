//! Ingestion orchestration (discover -> fetch -> parse -> load).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::time::Duration;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterHttpClient, Adapters, ArtifactRecorder, ArtifactRecorderRef, DiscoveryCtx,
    FetchCtx, ParseCtx,
};
use au_kpis_db::PgPool;
use au_kpis_domain::{Artifact, ids::ArtifactId, ids::DataflowId, ids::SourceId};
use au_kpis_error::Classify;
use au_kpis_loader::{LoadItem, LoadItemAudit, LoadOptions, LoadStats};
use au_kpis_storage::BlobStore;
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::FuturesUnordered};
use opentelemetry::{
    Context as OtelContext, propagation::TextMapPropagator, trace::TraceContextExt,
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinSet, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, info_span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

const DEFAULT_CHANNEL_CAPACITY: usize = 64;
const DEFAULT_STAGE_CONCURRENCY: usize = 4;
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
static TRACE_PARENT_COUNTER: AtomicU64 = AtomicU64::new(1);

/// DB-backed artifact provenance recorder for fetch workers.
#[derive(Debug, Clone)]
pub struct DbArtifactRecorder {
    pool: PgPool,
}

impl DbArtifactRecorder {
    /// Construct a recorder that writes artifact rows through `au-kpis-db`.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Return this recorder behind the trait object expected by [`FetchCtx`].
    #[must_use]
    pub fn shared(self) -> ArtifactRecorderRef {
        Arc::new(self)
    }
}

#[async_trait]
impl ArtifactRecorder for DbArtifactRecorder {
    async fn get(
        &self,
        id: au_kpis_domain::ids::ArtifactId,
    ) -> Result<Option<Artifact>, AdapterError> {
        au_kpis_db::get_artifact(&self.pool, id)
            .await
            .map_err(|err| AdapterError::artifact_record(err.to_string(), err.class()))
    }

    async fn record(&self, artifact: &Artifact) -> Result<Artifact, AdapterError> {
        au_kpis_db::upsert_artifact_record(&self.pool, artifact)
            .await
            .map_err(|err| AdapterError::artifact_record(err.to_string(), err.class()))
    }

    async fn repair_storage_key(
        &self,
        artifact: &Artifact,
        observed_storage_key: &str,
    ) -> Result<Artifact, AdapterError> {
        au_kpis_db::repair_artifact_storage_key(&self.pool, artifact, observed_storage_key)
            .await
            .map_err(|err| AdapterError::artifact_record(err.to_string(), err.class()))
    }
}

/// Build the fetch context used by ingestion workers.
#[must_use]
pub fn fetch_ctx(
    http: AdapterHttpClient,
    blob_store: BlobStore,
    started_at: DateTime<Utc>,
    pool: PgPool,
) -> FetchCtx {
    FetchCtx::new(
        http,
        blob_store,
        started_at,
        DbArtifactRecorder::new(pool).shared(),
    )
}

/// Runtime options for a one-source ingestion pipeline run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineOptions {
    /// Capacity for each bounded stage channel.
    pub channel_capacity: usize,
    /// Maximum concurrent fetch jobs inside one source run.
    pub fetch_concurrency: usize,
    /// Maximum concurrent parse jobs inside one source run.
    pub parse_concurrency: usize,
    /// Maximum observations passed to the loader in one transaction.
    pub load_max_rows: usize,
    /// Maximum approximate COPY payload bytes passed to the loader.
    pub load_max_bytes: usize,
    /// Time allowed for stage tasks to drain after cancellation.
    pub shutdown_grace: Duration,
}

impl Default for PipelineOptions {
    fn default() -> Self {
        let load = LoadOptions::default();
        Self {
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            fetch_concurrency: DEFAULT_STAGE_CONCURRENCY,
            parse_concurrency: DEFAULT_STAGE_CONCURRENCY,
            load_max_rows: load.max_rows,
            load_max_bytes: load.max_bytes,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
        }
    }
}

/// Adapter contexts captured once and passed through each pipeline stage.
#[derive(Debug, Clone)]
pub struct PipelineContexts {
    /// Context used by adapter discovery.
    pub discovery: DiscoveryCtx,
    /// Context used by adapter fetch.
    pub fetch: FetchCtx,
    /// Context used by adapter parse.
    pub parse: ParseCtx,
}

/// Aggregate counters returned by an ingestion pipeline run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PipelineRunStats {
    /// Discovery jobs emitted by the adapter.
    pub discovered: u64,
    /// Artifacts fetched and persisted.
    pub fetched: u64,
    /// Parsed observations sent to the loader.
    pub parsed: u64,
    /// Loader result accumulated across all load batches.
    pub loaded: LoadStats,
}

impl PipelineRunStats {
    fn add(&mut self, other: Self) {
        self.discovered += other.discovered;
        self.fetched += other.fetched;
        self.parsed += other.parsed;
        self.loaded.observations_loaded += other.loaded.observations_loaded;
        self.loaded.series_upserted += other.loaded.series_upserted;
        self.loaded.parse_errors += other.loaded.parse_errors;
        self.loaded.batches += other.loaded.batches;
    }
}

/// Errors returned by ingestion orchestration.
#[derive(Debug, Error)]
pub enum IngestionError {
    /// Pipeline configuration was invalid.
    #[error("ingestion configuration: {0}")]
    Config(String),

    /// Adapter discovery, fetch, or parse failed.
    #[error(transparent)]
    Adapter(#[from] AdapterError),

    /// Loading parsed observations failed.
    #[error(transparent)]
    Load(#[from] au_kpis_loader::LoadError),

    /// A one-source run received work for a different source.
    #[error("source mismatch in {stage}: expected `{expected}`, got `{actual}`")]
    SourceMismatch {
        /// Pipeline stage that rejected the mismatched work item.
        stage: &'static str,
        /// Source id requested for this pipeline run.
        expected: String,
        /// Source id found on the work item.
        actual: String,
    },

    /// Parsed rows did not match the dataflow discovered for the artifact.
    #[error("dataflow mismatch: expected `{expected}`, got `{actual}`")]
    DataflowMismatch {
        /// Dataflow id from the discovered job.
        expected: String,
        /// Dataflow id emitted by the parser.
        actual: String,
    },

    /// Parsed observations pointed at a different artifact than the fetched input.
    #[error("artifact mismatch: expected `{expected}`, got `{actual}`")]
    ArtifactMismatch {
        /// Artifact id fetched for this parse job.
        expected: String,
        /// Artifact id emitted on the parsed observation.
        actual: String,
    },

    /// The caller cancelled the pipeline before it completed.
    #[error("ingestion cancelled")]
    Cancelled,

    /// A downstream stage exited before accepting a queued handoff.
    #[error("ingestion downstream channel closed")]
    DownstreamClosed,

    /// A stage task panicked or was aborted.
    #[error("ingestion task join: {0}")]
    Join(#[from] tokio::task::JoinError),

    /// Stage tasks did not finish inside the configured shutdown grace period.
    #[error("ingestion shutdown timed out after {0:?}")]
    ShutdownTimeout(Duration),
}

/// Bounded, cancellable discover -> fetch -> parse -> load orchestrator.
#[derive(Debug, Clone)]
pub struct IngestionPipeline {
    adapters: Adapters,
    pool: PgPool,
    options: PipelineOptions,
}

impl IngestionPipeline {
    /// Construct a pipeline with default bounded channel and loader options.
    #[must_use]
    pub fn new(adapters: Adapters, pool: PgPool) -> Self {
        Self {
            adapters,
            pool,
            options: PipelineOptions::default(),
        }
    }

    /// Return a copy using explicit runtime options.
    #[must_use]
    pub const fn with_options(mut self, options: PipelineOptions) -> Self {
        self.options = options;
        self
    }

    /// Run discovery, fetch, parse, and load for one source until completion or cancellation.
    #[tracing::instrument(skip(self, contexts, cancellation), fields(source = source_id.as_str()))]
    pub async fn run_source(
        &self,
        source_id: SourceId,
        mut contexts: PipelineContexts,
        cancellation: CancellationToken,
    ) -> Result<PipelineRunStats, IngestionError> {
        self.validate_options()?;

        if contexts.discovery.trace_parent().is_none() {
            let trace_parent = generated_trace_parent(&source_id, contexts.discovery.started_at);
            contexts.discovery = contexts.discovery.with_trace_parent(trace_parent);
        }

        let (discovered_tx, discovered_rx) = mpsc::channel(self.options.channel_capacity);
        let (artifact_tx, artifact_rx) = mpsc::channel(self.options.channel_capacity);
        let (load_tx, load_rx) = mpsc::channel(self.options.channel_capacity);

        let mut tasks = JoinSet::new();
        let source = source_id.as_str().to_string();
        let pipeline_token = cancellation.child_token();
        let trace_parent = contexts.discovery.trace_parent().map(str::to_owned);

        let discover_span = info_span!(
            "ingestion_discover",
            source = %source,
            trace_parent = trace_parent.as_deref().unwrap_or("")
        );
        restore_trace_parent(&discover_span, trace_parent.as_deref());
        tasks.spawn(
            discover_stage(
                self.adapters.clone(),
                source_id.clone(),
                contexts.discovery,
                discovered_tx,
                pipeline_token.clone(),
            )
            .instrument(discover_span),
        );
        let fetch_span = info_span!(
            "ingestion_fetch",
            source = %source,
            trace_parent = trace_parent.as_deref().unwrap_or("")
        );
        restore_trace_parent(&fetch_span, trace_parent.as_deref());
        tasks.spawn(
            fetch_stage(
                self.adapters.clone(),
                discovered_rx,
                contexts.fetch,
                artifact_tx,
                source_id.clone(),
                pipeline_token.clone(),
                self.options.fetch_concurrency,
            )
            .instrument(fetch_span),
        );
        let parse_span = info_span!(
            "ingestion_parse",
            source = %source,
            trace_parent = trace_parent.as_deref().unwrap_or("")
        );
        restore_trace_parent(&parse_span, trace_parent.as_deref());
        tasks.spawn(
            parse_stage(
                self.adapters.clone(),
                artifact_rx,
                contexts.parse,
                load_tx,
                source_id.clone(),
                pipeline_token.clone(),
                self.options.parse_concurrency,
            )
            .instrument(parse_span),
        );
        let load_span = info_span!(
            "ingestion_load",
            source = %source,
            trace_parent = trace_parent.as_deref().unwrap_or("")
        );
        restore_trace_parent(&load_span, trace_parent.as_deref());
        tasks.spawn(
            load_stage(
                self.pool.clone(),
                load_rx,
                LoadOptions {
                    max_rows: self.options.load_max_rows,
                    max_bytes: self.options.load_max_bytes,
                },
                pipeline_token.clone(),
            )
            .instrument(load_span),
        );

        let shutdown_grace = self.options.shutdown_grace;
        collect_stage_stats(tasks, cancellation, pipeline_token, shutdown_grace).await
    }

    fn validate_options(&self) -> Result<(), IngestionError> {
        if self.options.channel_capacity == 0 {
            return Err(IngestionError::Config(
                "channel_capacity must be greater than 0".into(),
            ));
        }
        if self.options.fetch_concurrency == 0 {
            return Err(IngestionError::Config(
                "fetch_concurrency must be greater than 0".into(),
            ));
        }
        if self.options.parse_concurrency == 0 {
            return Err(IngestionError::Config(
                "parse_concurrency must be greater than 0".into(),
            ));
        }
        if self.options.load_max_rows == 0 {
            return Err(IngestionError::Config(
                "load_max_rows must be greater than 0".into(),
            ));
        }
        if self.options.load_max_bytes == 0 {
            return Err(IngestionError::Config(
                "load_max_bytes must be greater than 0".into(),
            ));
        }
        if self.options.shutdown_grace.is_zero() {
            return Err(IngestionError::Config(
                "shutdown_grace must be greater than 0".into(),
            ));
        }
        Ok(())
    }
}

async fn collect_stage_stats(
    mut tasks: JoinSet<Result<PipelineRunStats, IngestionError>>,
    cancellation: CancellationToken,
    pipeline_token: CancellationToken,
    shutdown_grace: Duration,
) -> Result<PipelineRunStats, IngestionError> {
    let mut stats = PipelineRunStats::default();
    loop {
        tokio::select! {
            () = cancellation.cancelled() => {
                pipeline_token.cancel();
                let (drained_stats, errors) = timeout(shutdown_grace, drain_task_results(tasks))
                    .await
                    .map_err(|_| IngestionError::ShutdownTimeout(shutdown_grace))?;
                stats.add(drained_stats);
                if errors.is_empty() {
                    return Ok(stats);
                }
                return Err(preferred_error(IngestionError::Cancelled, errors));
            }
            result = tasks.join_next() => {
                let Some(result) = result else {
                    return Ok(stats);
                };
                match result? {
                    Ok(stage_stats) => stats.add(stage_stats),
                    Err(err) => {
                        pipeline_token.cancel();
                        let (_, errors) = timeout(shutdown_grace, drain_task_results(tasks))
                            .await
                            .map_err(|_| IngestionError::ShutdownTimeout(shutdown_grace))?;
                        return Err(preferred_error(err, errors));
                    }
                }
            }
        }
    }
}

async fn drain_task_results(
    mut tasks: JoinSet<Result<PipelineRunStats, IngestionError>>,
) -> (PipelineRunStats, Vec<IngestionError>) {
    let mut stats = PipelineRunStats::default();
    let mut errors = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(stage_stats)) => stats.add(stage_stats),
            Ok(Err(err)) => errors.push(err),
            Err(err) => errors.push(IngestionError::Join(err)),
        }
    }
    (stats, errors)
}

#[derive(Debug)]
struct FetchedArtifact {
    artifact: au_kpis_adapter::ArtifactRef,
    dataflow_id: DataflowId,
    correlation: JobCorrelation,
    metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct JobCorrelation {
    source_id: String,
    job_id: String,
    trace_parent: Option<String>,
}

impl JobCorrelation {
    fn row_context(&self) -> serde_json::Value {
        serde_json::json!({
            "source_id": self.source_id.as_str(),
            "job_id": self.job_id.as_str(),
            "trace_parent": self.trace_parent.as_deref(),
        })
    }
}

fn generated_trace_parent(source_id: &SourceId, started_at: DateTime<Utc>) -> String {
    let sequence = TRACE_PARENT_COUNTER.fetch_add(1, Ordering::Relaxed);
    let seed = format!(
        "ingestion:{}:{}:{}:{}",
        source_id.as_str(),
        started_at.to_rfc3339(),
        Utc::now().to_rfc3339(),
        sequence
    );
    let hex = ArtifactId::of_content(seed.as_bytes()).to_hex();
    format!("00-{}-{}-01", &hex[..32], &hex[32..48])
}

fn trace_context_from_trace_parent(trace_parent: &str) -> Option<OtelContext> {
    let carrier = HashMap::from([("traceparent".to_string(), trace_parent.to_string())]);
    let propagator = TraceContextPropagator::new();
    let context = propagator.extract(&carrier);
    context.span().span_context().is_valid().then_some(context)
}

fn restore_trace_parent(span: &Span, trace_parent: Option<&str>) {
    let Some(context) = trace_parent.and_then(trace_context_from_trace_parent) else {
        return;
    };
    span.set_parent(context);
}

#[derive(Debug)]
enum LoadStageItem {
    Observation {
        item: LoadItem,
        correlation: JobCorrelation,
    },
    ParseError(ParseErrorRecord),
}

#[derive(Debug)]
struct ParseErrorRecord {
    artifact_id: ArtifactId,
    error_kind: &'static str,
    error_message: String,
    row_context: Option<serde_json::Value>,
}

fn preferred_error(primary: IngestionError, errors: Vec<IngestionError>) -> IngestionError {
    if is_secondary_shutdown_error(&primary) {
        errors
            .into_iter()
            .find(|err| !is_secondary_shutdown_error(err))
            .unwrap_or(primary)
    } else {
        primary
    }
}

fn is_secondary_shutdown_error(err: &IngestionError) -> bool {
    matches!(
        err,
        IngestionError::Cancelled | IngestionError::DownstreamClosed
    )
}

async fn discover_stage(
    adapters: Adapters,
    source_id: SourceId,
    ctx: DiscoveryCtx,
    tx: mpsc::Sender<au_kpis_adapter::DiscoveredJob>,
    cancellation: CancellationToken,
) -> Result<PipelineRunStats, IngestionError> {
    if cancellation.is_cancelled() {
        return Err(IngestionError::Cancelled);
    }
    let jobs = tokio::select! {
        () = cancellation.cancelled() => return Err(IngestionError::Cancelled),
        jobs = adapters.discover(source_id.as_str(), &ctx) => jobs?,
    };
    let discovered = jobs.len() as u64;
    for job in jobs {
        if cancellation.is_cancelled() {
            return Err(IngestionError::Cancelled);
        }
        validate_source_id("discover", &source_id, &job.source_id)?;
        send_produced(&tx, job, &cancellation).await?;
    }
    Ok(PipelineRunStats {
        discovered,
        ..PipelineRunStats::default()
    })
}

async fn fetch_stage(
    adapters: Adapters,
    mut rx: mpsc::Receiver<au_kpis_adapter::DiscoveredJob>,
    ctx: FetchCtx,
    tx: mpsc::Sender<FetchedArtifact>,
    source_id: SourceId,
    cancellation: CancellationToken,
    concurrency: usize,
) -> Result<PipelineRunStats, IngestionError> {
    let mut fetched = 0;
    let mut input_closed = false;
    let mut cancelled = false;
    let mut in_flight = FuturesUnordered::new();

    loop {
        if cancellation.is_cancelled() {
            cancelled = true;
        }
        if input_closed && in_flight.is_empty() {
            break;
        }

        tokio::select! {
            () = cancellation.cancelled(), if !cancelled => {
                cancelled = true;
            }
            job = rx.recv(), if !input_closed && in_flight.len() < concurrency => {
                let Some(job) = job else {
                    input_closed = true;
                    continue;
                };
                validate_source_id("fetch", &source_id, &job.source_id)?;
                let fetch_cancellation = if cancelled || cancellation.is_cancelled() {
                    CancellationToken::new()
                } else {
                    cancellation.clone()
                };
                in_flight.push(fetch_one(
                    adapters.clone(),
                    source_id.clone(),
                    ctx.clone(),
                    job,
                    fetch_cancellation,
                ));
            }
            result = in_flight.next(), if !in_flight.is_empty() => {
                match result.expect("in_flight is not empty") {
                    Ok(fetched_artifact) => {
                        send_produced(&tx, fetched_artifact, &cancellation).await?;
                        fetched += 1;
                    }
                    Err(IngestionError::Cancelled) => cancelled = true,
                    Err(err) => return Err(err),
                }
            }
        }
    }

    if cancelled || cancellation.is_cancelled() {
        return Err(IngestionError::Cancelled);
    }

    Ok(PipelineRunStats {
        fetched,
        ..PipelineRunStats::default()
    })
}

async fn parse_stage(
    adapters: Adapters,
    mut rx: mpsc::Receiver<FetchedArtifact>,
    ctx: ParseCtx,
    tx: mpsc::Sender<LoadStageItem>,
    source_id: SourceId,
    cancellation: CancellationToken,
    concurrency: usize,
) -> Result<PipelineRunStats, IngestionError> {
    let mut parsed = 0;
    let mut input_closed = false;
    let mut cancelled = false;
    let mut in_flight = FuturesUnordered::new();

    loop {
        if cancellation.is_cancelled() {
            cancelled = true;
        }
        if input_closed && in_flight.is_empty() {
            break;
        }

        tokio::select! {
            () = cancellation.cancelled(), if !cancelled => {
                cancelled = true;
            }
            fetched = rx.recv(), if !input_closed && in_flight.len() < concurrency => {
                let Some(fetched) = fetched else {
                    input_closed = true;
                    continue;
                };
                validate_source_id("parse", &source_id, &fetched.artifact.source_id)?;
                let parse_cancellation = if cancelled || cancellation.is_cancelled() {
                    CancellationToken::new()
                } else {
                    cancellation.clone()
                };
                let parse_ctx = ctx.clone()
                    .with_expected_dataflow(fetched.dataflow_id.clone(), fetched.metadata.clone())
                    .with_job_correlation(
                        fetched.correlation.job_id.clone(),
                        fetched.correlation.trace_parent.clone(),
                    );
                in_flight.push(parse_one_artifact(
                    adapters.clone(),
                    source_id.clone(),
                    parse_ctx,
                    tx.clone(),
                    fetched,
                    parse_cancellation,
                ));
            }
            result = in_flight.next(), if !in_flight.is_empty() => {
                match result.expect("in_flight is not empty") {
                    Ok(count) => parsed += count,
                    Err(IngestionError::Cancelled) => cancelled = true,
                    Err(err) => return Err(err),
                }
            }
        }
    }

    if cancelled || cancellation.is_cancelled() {
        return Err(IngestionError::Cancelled);
    }

    Ok(PipelineRunStats {
        parsed,
        ..PipelineRunStats::default()
    })
}

async fn fetch_one(
    adapters: Adapters,
    source_id: SourceId,
    ctx: FetchCtx,
    job: au_kpis_adapter::DiscoveredJob,
    cancellation: CancellationToken,
) -> Result<FetchedArtifact, IngestionError> {
    let correlation = JobCorrelation {
        source_id: job.source_id.as_str().to_string(),
        job_id: job.id.clone(),
        trace_parent: job.trace_parent.clone(),
    };
    let dataflow_id = job.dataflow_id.clone();
    let metadata = job.metadata.clone();
    let artifact = tokio::select! {
        () = cancellation.cancelled() => return Err(IngestionError::Cancelled),
        artifact = adapters.fetch(source_id.as_str(), job, &ctx) => artifact?,
    };
    validate_source_id("fetch", &source_id, &artifact.source_id)?;
    Ok(FetchedArtifact {
        artifact,
        dataflow_id,
        correlation,
        metadata,
    })
}

async fn parse_one_artifact(
    adapters: Adapters,
    source_id: SourceId,
    parse_ctx: ParseCtx,
    tx: mpsc::Sender<LoadStageItem>,
    fetched: FetchedArtifact,
    cancellation: CancellationToken,
) -> Result<u64, IngestionError> {
    let artifact_id = fetched.artifact.id;
    let mut parsed = 0;
    let mut observations = adapters.parse(source_id.as_str(), fetched.artifact, &parse_ctx)?;

    loop {
        let row = tokio::select! {
            () = cancellation.cancelled() => return Err(IngestionError::Cancelled),
            row = observations.next() => row,
        };
        let Some(row) = row else {
            break;
        };

        let (series, observation) = match row {
            Ok(row) => row,
            Err(err) => {
                send_produced(
                    &tx,
                    LoadStageItem::ParseError(parse_error_record(
                        artifact_id,
                        &fetched.dataflow_id,
                        &source_id,
                        &fetched.correlation,
                        &err,
                        parsed == 0,
                    )),
                    &cancellation,
                )
                .await?;
                if parsed == 0 {
                    return Err(IngestionError::Adapter(err));
                }
                continue;
            }
        };
        if series.dataflow_id != fetched.dataflow_id {
            let expected = fetched.dataflow_id.to_string();
            let actual = series.dataflow_id.to_string();
            send_produced(
                &tx,
                LoadStageItem::ParseError(provenance_error_record(
                    artifact_id,
                    &fetched.dataflow_id,
                    &source_id,
                    &fetched.correlation,
                    "dataflow_mismatch",
                    &format!("dataflow mismatch: expected `{expected}`, got `{actual}`"),
                )),
                &cancellation,
            )
            .await?;
            return Err(IngestionError::DataflowMismatch { expected, actual });
        }
        if observation.source_artifact_id != artifact_id {
            let expected = artifact_id.to_string();
            let actual = observation.source_artifact_id.to_string();
            send_produced(
                &tx,
                LoadStageItem::ParseError(provenance_error_record(
                    artifact_id,
                    &fetched.dataflow_id,
                    &source_id,
                    &fetched.correlation,
                    "artifact_mismatch",
                    &format!("artifact mismatch: expected `{expected}`, got `{actual}`"),
                )),
                &cancellation,
            )
            .await?;
            return Err(IngestionError::ArtifactMismatch { expected, actual });
        }
        send_produced(
            &tx,
            LoadStageItem::Observation {
                item: LoadItem {
                    series,
                    observation,
                },
                correlation: fetched.correlation.clone(),
            },
            &cancellation,
        )
        .await?;
        parsed += 1;
    }

    Ok(parsed)
}

fn parse_error_record(
    artifact_id: ArtifactId,
    dataflow_id: &DataflowId,
    source_id: &SourceId,
    correlation: &JobCorrelation,
    err: &AdapterError,
    fatal: bool,
) -> ParseErrorRecord {
    ParseErrorRecord {
        artifact_id,
        error_kind: "adapter_parse",
        error_message: err.to_string(),
        row_context: Some(serde_json::json!({
            "dataflow_id": dataflow_id,
            "source_id": source_id,
            "artifact_id": artifact_id,
            "job_id": correlation.job_id.as_str(),
            "trace_parent": correlation.trace_parent.as_deref(),
            "error_class": format!("{:?}", err.class()),
            "fatal": fatal,
        })),
    }
}

fn provenance_error_record(
    artifact_id: ArtifactId,
    dataflow_id: &DataflowId,
    source_id: &SourceId,
    correlation: &JobCorrelation,
    error_kind: &'static str,
    error_message: &str,
) -> ParseErrorRecord {
    ParseErrorRecord {
        artifact_id,
        error_kind,
        error_message: error_message.to_string(),
        row_context: Some(serde_json::json!({
            "dataflow_id": dataflow_id,
            "source_id": source_id,
            "artifact_id": artifact_id,
            "job_id": correlation.job_id.as_str(),
            "trace_parent": correlation.trace_parent.as_deref(),
            "fatal": true,
        })),
    }
}

async fn load_stage(
    pool: PgPool,
    mut rx: mpsc::Receiver<LoadStageItem>,
    options: LoadOptions,
    cancellation: CancellationToken,
) -> Result<PipelineRunStats, IngestionError> {
    let mut loaded = LoadStats::default();
    let mut batch = Vec::with_capacity(options.max_rows.min(1024));
    let mut batch_correlations = Vec::with_capacity(options.max_rows.min(1024));
    let mut batch_bytes = 0usize;
    let mut draining = false;

    loop {
        let item = if draining {
            rx.recv().await
        } else {
            tokio::select! {
                () = cancellation.cancelled() => {
                    draining = true;
                    continue;
                }
                value = rx.recv() => value,
            }
        };

        let Some(item) = item else {
            break;
        };

        let (item, correlation) = match item {
            LoadStageItem::Observation { item, correlation } => (item, correlation),
            LoadStageItem::ParseError(record) => {
                au_kpis_loader::record_parse_error(
                    &pool,
                    record.artifact_id,
                    record.error_kind,
                    &record.error_message,
                    record.row_context,
                )
                .await?;
                loaded.parse_errors += 1;
                continue;
            }
        };

        let item_bytes = estimate_load_item_bytes(&item);
        if should_flush_load_batch(&batch, batch_bytes, item_bytes, options) {
            add_load_stats(
                &mut loaded,
                flush_load_batch(&pool, &mut batch, &mut batch_correlations, options).await?,
            );
            batch_bytes = 0;
        }

        batch_bytes += item_bytes;
        batch.push(item);
        batch_correlations.push(correlation);

        if batch.len() >= options.max_rows || batch_bytes >= options.max_bytes {
            add_load_stats(
                &mut loaded,
                flush_load_batch(&pool, &mut batch, &mut batch_correlations, options).await?,
            );
            batch_bytes = 0;
        }
    }
    if !batch.is_empty() {
        add_load_stats(
            &mut loaded,
            flush_load_batch(&pool, &mut batch, &mut batch_correlations, options).await?,
        );
    }
    Ok(PipelineRunStats {
        loaded,
        ..PipelineRunStats::default()
    })
}

async fn flush_load_batch(
    pool: &PgPool,
    batch: &mut Vec<LoadItem>,
    correlations: &mut Vec<JobCorrelation>,
    options: LoadOptions,
) -> Result<LoadStats, au_kpis_loader::LoadError> {
    let job_ids = joined_unique(
        correlations
            .iter()
            .map(|correlation| correlation.job_id.as_str()),
    );
    let trace_parents = joined_unique(
        correlations
            .iter()
            .filter_map(|correlation| correlation.trace_parent.as_deref()),
    );
    let items = std::mem::take(batch);
    let correlations = std::mem::take(correlations);
    if items.len() != correlations.len() {
        return Err(au_kpis_loader::LoadError::Validation(
            "load batch item/correlation count mismatch".into(),
        ));
    }
    let items = items
        .into_iter()
        .zip(correlations)
        .map(|(item, correlation)| LoadItemAudit {
            item,
            row_context: Some(correlation.row_context()),
        })
        .collect();
    au_kpis_loader::load_batch_with_options_and_audit_context(pool, items, options)
        .instrument(info_span!(
            "ingestion_load_batch",
            job_ids = %job_ids,
            trace_parents = %trace_parents,
        ))
        .await
}

fn joined_unique<'a>(values: impl Iterator<Item = &'a str>) -> String {
    values
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(",")
}

fn add_load_stats(total: &mut LoadStats, batch: LoadStats) {
    total.observations_loaded += batch.observations_loaded;
    total.series_upserted += batch.series_upserted;
    total.parse_errors += batch.parse_errors;
    total.batches += batch.batches;
}

fn validate_source_id(
    stage: &'static str,
    expected: &SourceId,
    actual: &SourceId,
) -> Result<(), IngestionError> {
    if expected == actual {
        Ok(())
    } else {
        Err(IngestionError::SourceMismatch {
            stage,
            expected: expected.as_str().to_string(),
            actual: actual.as_str().to_string(),
        })
    }
}

fn should_flush_load_batch(
    batch: &[LoadItem],
    batch_bytes: usize,
    next_item_bytes: usize,
    options: LoadOptions,
) -> bool {
    !batch.is_empty()
        && (batch.len() >= options.max_rows
            || batch_bytes.saturating_add(next_item_bytes) > options.max_bytes)
}

fn estimate_load_item_bytes(item: &LoadItem) -> usize {
    const BASE_ROW_BYTES: usize = 256;
    BASE_ROW_BYTES
        + item.series.dataflow_id.as_str().len()
        + item.series.measure_id.as_str().len()
        + item.series.unit.len()
        + item
            .series
            .dimensions
            .iter()
            .map(|(key, value)| key.as_str().len() + value.as_str().len())
            .sum::<usize>()
        + item
            .observation
            .attributes
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>()
}

async fn send_produced<T>(
    tx: &mpsc::Sender<T>,
    value: T,
    _cancellation: &CancellationToken,
) -> Result<(), IngestionError> {
    // Produced values are part of graceful drain; the run-level shutdown_grace
    // bounds this await instead of letting cancellation discard the value.
    let permit = tx
        .reserve()
        .await
        .map_err(|_| IngestionError::DownstreamClosed)?;
    permit.send(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use au_kpis_domain::{
        Observation, ObservationStatus, SeriesDescriptor, TimePrecision,
        ids::{ArtifactId, CodeId, DataflowId, DimensionId, MeasureId, SeriesKey},
    };
    use chrono::{TimeZone, Utc};
    use opentelemetry::trace::TraceContextExt;

    use super::*;

    const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn load_stage_flushes_before_next_item_exceeds_byte_cap() {
        let first = load_item_with_attribute_bytes(8);
        let second = load_item_with_attribute_bytes(8);
        let first_bytes = estimate_load_item_bytes(&first);
        let second_bytes = estimate_load_item_bytes(&second);
        let options = LoadOptions {
            max_rows: 64,
            max_bytes: first_bytes + second_bytes - 1,
        };

        assert!(!should_flush_load_batch(&[], 0, first_bytes, options));
        assert!(should_flush_load_batch(
            &[first],
            first_bytes,
            second_bytes,
            options
        ));
    }

    #[test]
    fn load_stage_does_not_flush_empty_batch_for_oversized_single_item() {
        let item = load_item_with_attribute_bytes(1024);
        let item_bytes = estimate_load_item_bytes(&item);
        let options = LoadOptions {
            max_rows: 64,
            max_bytes: item_bytes - 1,
        };

        assert!(!should_flush_load_batch(&[], 0, item_bytes, options));
    }

    #[tokio::test]
    async fn late_cancellation_after_stage_completion_keeps_success_stats() {
        let mut tasks = JoinSet::new();
        tasks.spawn(async {
            Ok(PipelineRunStats {
                discovered: 1,
                ..PipelineRunStats::default()
            })
        });
        tasks.spawn(async {
            Ok(PipelineRunStats {
                fetched: 1,
                ..PipelineRunStats::default()
            })
        });
        tokio::task::yield_now().await;

        let cancellation = CancellationToken::new();
        let pipeline_token = cancellation.child_token();
        cancellation.cancel();

        let stats = collect_stage_stats(
            tasks,
            cancellation,
            pipeline_token,
            Duration::from_millis(100),
        )
        .await
        .expect("completed stages should not be reclassified as cancelled");

        assert_eq!(stats.discovered, 1);
        assert_eq!(stats.fetched, 1);
    }

    #[test]
    fn trace_parent_context_extracts_w3c_parent_ids() {
        let context =
            trace_context_from_trace_parent(TRACE_PARENT).expect("trace parent should parse");
        let span_context = context.span().span_context().clone();

        assert!(span_context.is_valid());
        assert_eq!(
            span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(span_context.span_id().to_string(), "00f067aa0ba902b7");
    }

    #[tokio::test]
    async fn produced_handoff_waits_for_capacity_instead_of_dropping_item() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(1).await.expect("seed full channel");
        let cancellation = CancellationToken::new();

        let sender = tokio::spawn(async move { send_produced(&tx, 2, &cancellation).await });

        assert_eq!(rx.recv().await, Some(1));
        sender
            .await
            .expect("handoff task should not panic")
            .expect("handoff should complete once capacity is available");
        assert_eq!(rx.recv().await, Some(2));
    }

    #[tokio::test]
    async fn produced_handoff_does_not_drop_item_when_full_and_cancelled() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(1).await.expect("seed full channel");
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let mut sender = tokio::spawn(async move { send_produced(&tx, 2, &cancellation).await });

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut sender)
                .await
                .is_err(),
            "handoff must wait for downstream capacity instead of dropping the item"
        );
        assert_eq!(rx.recv().await, Some(1));
        sender
            .await
            .expect("handoff task should not panic")
            .expect("handoff should complete once capacity is available");
        assert_eq!(rx.recv().await, Some(2));
    }

    fn load_item_with_attribute_bytes(attribute_bytes: usize) -> LoadItem {
        let dataflow_id = DataflowId::new("stub.cpi").unwrap();
        let dimensions = BTreeMap::from([(
            DimensionId::new("region").unwrap(),
            CodeId::new("AUS").unwrap(),
        )]);
        let series_key = SeriesKey::derive(
            &dataflow_id,
            dimensions
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        let series = SeriesDescriptor {
            series_key,
            dataflow_id,
            measure_id: MeasureId::new("index").unwrap(),
            dimensions,
            unit: "index".into(),
        };
        let observation = Observation {
            series_key,
            time: Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap(),
            time_precision: TimePrecision::Quarter,
            value: Some(123.4),
            status: ObservationStatus::Normal,
            revision_no: 0,
            attributes: BTreeMap::from([("payload".into(), "x".repeat(attribute_bytes))]),
            ingested_at: Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
            source_artifact_id: ArtifactId::of_content(b"artifact"),
        };
        LoadItem {
            series,
            observation,
        }
    }
}
