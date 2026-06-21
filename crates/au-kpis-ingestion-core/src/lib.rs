//! Ingestion orchestration (discover -> fetch -> parse -> load).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::time::Duration;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterHttpClient, Adapters, ArtifactRecorder, ArtifactRecorderRef, DiscoveryCtx,
    FetchCtx, ObservationStream, ParseCtx,
};
use au_kpis_db::PgPool;
use au_kpis_domain::{
    Artifact, Observation, SeriesDescriptor, ids::ArtifactId, ids::DataflowId, ids::SourceId,
};
use au_kpis_error::Classify;
use au_kpis_loader::{LoadItem, LoadItemAudit, LoadOptions, LoadStats, StagedLoad};
use au_kpis_storage::BlobStore;
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::FuturesUnordered};
use opentelemetry::{
    Context as OtelContext, propagation::TextMapPropagator, trace::TraceContextExt,
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use thiserror::Error;
use tokio::{
    sync::mpsc,
    task::JoinSet,
    time::{Instant, timeout, timeout_at},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::{Instrument, Level, Span, info_span, instrument::WithSubscriber, trace_span};
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

    /// Database state lookup or write failed outside the loader.
    #[error(transparent)]
    Db(#[from] au_kpis_db::DbError),

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
            .instrument(discover_span)
            .with_current_subscriber(),
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
                StageRuntime {
                    concurrency: self.options.fetch_concurrency,
                    shutdown_grace: self.options.shutdown_grace,
                },
            )
            .instrument(fetch_span)
            .with_current_subscriber(),
        );
        let parse_span = info_span!(
            "ingestion_parse",
            source = %source,
            trace_parent = trace_parent.as_deref().unwrap_or("")
        );
        restore_trace_parent(&parse_span, trace_parent.as_deref());
        tasks.spawn(
            parse_stage(
                ParseStageContext {
                    adapters: self.adapters.clone(),
                    pool: self.pool.clone(),
                    ctx: contexts.parse,
                    source_id: source_id.clone(),
                    runtime: StageRuntime {
                        concurrency: self.options.parse_concurrency,
                        shutdown_grace: self.options.shutdown_grace,
                    },
                },
                artifact_rx,
                load_tx,
                pipeline_token.clone(),
            )
            .instrument(parse_span)
            .with_current_subscriber(),
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
            .instrument(load_span)
            .with_current_subscriber(),
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
            biased;
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

#[derive(Debug, Clone)]
struct ParseJobAuditOwned {
    artifact_id: ArtifactId,
    dataflow_id: DataflowId,
    source_id: SourceId,
    correlation: JobCorrelation,
}

#[derive(Debug, Clone, Copy)]
struct StageRuntime {
    concurrency: usize,
    shutdown_grace: Duration,
}

#[derive(Clone)]
struct ParseStageContext {
    adapters: Adapters,
    pool: PgPool,
    ctx: ParseCtx,
    source_id: SourceId,
    runtime: StageRuntime,
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

impl ParseJobAuditOwned {
    fn new(source_id: &SourceId, fetched: &FetchedArtifact) -> Self {
        Self {
            artifact_id: fetched.artifact.id,
            dataflow_id: fetched.dataflow_id.clone(),
            source_id: source_id.clone(),
            correlation: fetched.correlation.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingLoadKey {
    artifact_id: ArtifactId,
    correlation: JobCorrelation,
}

impl PendingLoadKey {
    fn new(artifact_id: ArtifactId, correlation: JobCorrelation) -> Self {
        Self {
            artifact_id,
            correlation,
        }
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

fn trace_parent_from_current_span() -> Option<String> {
    let context = Span::current().context();
    if !context.span().span_context().is_valid() {
        return None;
    }
    let mut carrier = HashMap::new();
    TraceContextPropagator::new().inject_context(&context, &mut carrier);
    carrier.remove("traceparent")
}

#[derive(Debug)]
enum LoadStageItem {
    Observation {
        item: LoadItem,
        correlation: JobCorrelation,
    },
    AcceptArtifact {
        artifact_id: ArtifactId,
        source_id: SourceId,
        dataflow_id: DataflowId,
        observations_parsed: u64,
        parse_errors: u64,
        correlation: JobCorrelation,
    },
    ParseError(ParseErrorRecord),
    RejectArtifact {
        artifact_id: ArtifactId,
        correlation: JobCorrelation,
    },
}

#[derive(Debug)]
struct ParseErrorRecord {
    artifact_id: ArtifactId,
    error_kind: &'static str,
    error_message: String,
    row_context: Option<serde_json::Value>,
}

#[derive(Debug)]
struct PendingArtifactCompletion {
    artifact_id: ArtifactId,
    source_id: SourceId,
    dataflow_id: DataflowId,
    observations_parsed: u64,
    observations_loaded: u64,
    correlation: JobCorrelation,
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
    mut ctx: DiscoveryCtx,
    tx: mpsc::Sender<au_kpis_adapter::DiscoveredJob>,
    cancellation: CancellationToken,
) -> Result<PipelineRunStats, IngestionError> {
    if cancellation.is_cancelled() {
        return Err(IngestionError::Cancelled);
    }
    if let Some(trace_parent) = trace_parent_from_current_span() {
        ctx = ctx.with_trace_parent(trace_parent);
    }
    let jobs = tokio::select! {
        () = cancellation.cancelled() => return Err(IngestionError::Cancelled),
        jobs = adapters.discover(source_id.as_str(), &ctx) => jobs?,
    };
    let jobs = filter_discovered_jobs(jobs, ctx.requested_dataflow_id());
    let discovered = jobs.len() as u64;
    for mut job in jobs {
        if cancellation.is_cancelled() {
            return Err(IngestionError::Cancelled);
        }
        validate_source_id("discover", &source_id, &job.source_id)?;
        if job.trace_parent.is_none() {
            job.trace_parent = ctx.trace_parent().map(str::to_owned);
        }
        send_produced(&tx, job, &cancellation).await?;
    }
    Ok(PipelineRunStats {
        discovered,
        ..PipelineRunStats::default()
    })
}

fn filter_discovered_jobs(
    jobs: Vec<au_kpis_adapter::DiscoveredJob>,
    requested_dataflow_id: Option<&DataflowId>,
) -> Vec<au_kpis_adapter::DiscoveredJob> {
    let Some(requested_dataflow_id) = requested_dataflow_id else {
        return jobs;
    };

    jobs.into_iter()
        .filter(|job| &job.dataflow_id == requested_dataflow_id)
        .collect()
}

async fn fetch_stage(
    adapters: Adapters,
    mut rx: mpsc::Receiver<au_kpis_adapter::DiscoveredJob>,
    ctx: FetchCtx,
    tx: mpsc::Sender<FetchedArtifact>,
    source_id: SourceId,
    cancellation: CancellationToken,
    runtime: StageRuntime,
) -> Result<PipelineRunStats, IngestionError> {
    let mut fetched = 0;
    let mut input_closed = false;
    let mut draining = false;
    let mut stopped_input = false;
    let mut cancelled_in_flight = false;
    let mut in_flight = FuturesUnordered::new();

    loop {
        if cancellation.is_cancelled() && !draining {
            draining = true;
            stopped_input = !(input_closed || (rx.is_closed() && rx.is_empty()));
        }
        if (input_closed || draining) && in_flight.is_empty() {
            break;
        }

        tokio::select! {
            () = cancellation.cancelled(), if !draining => {
                draining = true;
                stopped_input = !(input_closed || (rx.is_closed() && rx.is_empty()));
            }
            job = rx.recv(), if !input_closed && !draining && in_flight.len() < runtime.concurrency => {
                let Some(job) = job else {
                    input_closed = true;
                    continue;
                };
                if cancellation.is_cancelled() {
                    draining = true;
                    stopped_input = true;
                    continue;
                }
                validate_source_id("fetch", &source_id, &job.source_id)?;
                in_flight.push(fetch_one(
                    adapters.clone(),
                    source_id.clone(),
                    ctx.clone().with_cancellation(cancellation.clone()),
                    job,
                    cancellation.clone(),
                    runtime.shutdown_grace,
                ));
            }
            result = in_flight.next(), if !in_flight.is_empty() => {
                match result.expect("in_flight is not empty") {
                    Ok(fetched_artifact) => {
                        send_produced(&tx, fetched_artifact, &cancellation).await?;
                        fetched += 1;
                    }
                    Err(IngestionError::Cancelled) => {
                        draining = true;
                        cancelled_in_flight = true;
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    }

    if stopped_input || cancelled_in_flight {
        return Err(IngestionError::Cancelled);
    }

    Ok(PipelineRunStats {
        fetched,
        ..PipelineRunStats::default()
    })
}

async fn parse_stage(
    stage: ParseStageContext,
    mut rx: mpsc::Receiver<FetchedArtifact>,
    tx: mpsc::Sender<LoadStageItem>,
    cancellation: CancellationToken,
) -> Result<PipelineRunStats, IngestionError> {
    let ParseStageContext {
        adapters,
        pool,
        ctx,
        source_id,
        runtime,
    } = stage;
    let mut parsed = 0;
    let mut input_closed = false;
    let mut draining = false;
    let mut stopped_input = false;
    let mut cancelled_in_flight = false;
    let mut in_flight = FuturesUnordered::new();

    loop {
        if cancellation.is_cancelled() && !draining {
            draining = true;
            stopped_input = !(input_closed || (rx.is_closed() && rx.is_empty()));
        }
        if input_closed && in_flight.is_empty() {
            break;
        }

        tokio::select! {
            () = cancellation.cancelled(), if !draining => {
                draining = true;
                stopped_input = !(input_closed || (rx.is_closed() && rx.is_empty()));
            }
            fetched = rx.recv(), if !input_closed && in_flight.len() < runtime.concurrency => {
                let Some(fetched) = fetched else {
                    input_closed = true;
                    continue;
                };
                validate_source_id("parse", &source_id, &fetched.artifact.source_id)?;
                if artifact_load_completed_or_probe_unavailable(
                    &pool,
                    fetched.artifact.id,
                    &source_id,
                    &fetched.dataflow_id,
                )
                .await?
                {
                    tracing::info!(
                        artifact_id = %fetched.artifact.id,
                        dataflow_id = %fetched.dataflow_id,
                        "skipping previously completed artifact load"
                    );
                    continue;
                }
                let parse_ctx = ctx.clone()
                    .with_expected_dataflow(fetched.dataflow_id.clone(), fetched.metadata.clone())
                    .with_job_correlation(
                        fetched.correlation.job_id.clone(),
                        fetched.correlation.trace_parent.clone(),
                    )
                    .with_cancellation(cancellation.clone());
                let audit = ParseJobAuditOwned::new(&source_id, &fetched);
                let handle = AbortOnDropHandle::new(tokio::spawn(parse_one_artifact(
                    adapters.clone(),
                    source_id.clone(),
                    parse_ctx,
                    tx.clone(),
                    fetched,
                    cancellation.clone(),
                    runtime.shutdown_grace,
                ).with_current_subscriber()));
                in_flight.push(async move {
                    (audit, handle.await)
                });
            }
            result = in_flight.next(), if !in_flight.is_empty() => {
                let (audit, result) = result.expect("in_flight is not empty");
                match result {
                    Ok(Ok(count)) => parsed += count,
                    Ok(Err(IngestionError::Cancelled)) => {
                        draining = true;
                        cancelled_in_flight = true;
                    }
                    Ok(Err(err)) => return Err(err),
                    Err(err) if err.is_panic() => {
                        finish_panicked_parse(&tx, &audit, &cancellation, &err).await?;
                    }
                    Err(err) => return Err(IngestionError::Join(err)),
                }
            }
        }
    }

    if stopped_input || cancelled_in_flight {
        return Err(IngestionError::Cancelled);
    }

    Ok(PipelineRunStats {
        parsed,
        ..PipelineRunStats::default()
    })
}

async fn artifact_load_completed_or_probe_unavailable(
    pool: &PgPool,
    artifact_id: ArtifactId,
    source_id: &SourceId,
    dataflow_id: &DataflowId,
) -> Result<bool, IngestionError> {
    match au_kpis_db::artifact_load_completed(pool, artifact_id, source_id, dataflow_id).await {
        Ok(completed) => Ok(completed),
        Err(err) if artifact_load_probe_unavailable(&err) => {
            tracing::warn!(
                artifact_id = %artifact_id,
                source_id = %source_id,
                dataflow_id = %dataflow_id,
                error = %err,
                "artifact load completion probe unavailable; continuing with parse"
            );
            Ok(false)
        }
        Err(err) => Err(err.into()),
    }
}

fn artifact_load_probe_unavailable(err: &au_kpis_db::DbError) -> bool {
    err.is_pool_acquire_unavailable()
}

async fn fetch_one(
    adapters: Adapters,
    source_id: SourceId,
    ctx: FetchCtx,
    job: au_kpis_adapter::DiscoveredJob,
    cancellation: CancellationToken,
    shutdown_grace: Duration,
) -> Result<FetchedArtifact, IngestionError> {
    let trace_parent = job.trace_parent.clone();
    let span = info_span!(
        "ingestion_fetch_job",
        source = %source_id,
        job_id = %job.id,
        trace_parent = trace_parent.as_deref().unwrap_or("")
    );
    restore_trace_parent(&span, trace_parent.as_deref());
    let correlation = JobCorrelation {
        source_id: job.source_id.as_str().to_string(),
        job_id: job.id.clone(),
        trace_parent: job.trace_parent.clone(),
    };
    let dataflow_id = job.dataflow_id.clone();
    let metadata = job.metadata.clone();
    let mut fetch = Box::pin(adapters.fetch(source_id.as_str(), job, &ctx));
    let artifact = async {
        tokio::select! {
            biased;
            artifact = &mut fetch => artifact.map_err(IngestionError::Adapter),
            () = cancellation.cancelled() => timeout(shutdown_grace, &mut fetch)
                .await
                .map_err(|_| IngestionError::Cancelled)?
                .map_err(IngestionError::Adapter),
        }
    }
    .instrument(span)
    .await?;
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
    shutdown_grace: Duration,
) -> Result<u64, IngestionError> {
    let artifact_id = fetched.artifact.id;
    let trace_parent = fetched.correlation.trace_parent.clone();
    let span = info_span!(
        "ingestion_parse_job",
        source = %source_id,
        artifact_id = %artifact_id,
        job_id = %fetched.correlation.job_id,
        trace_parent = trace_parent.as_deref().unwrap_or("")
    );
    restore_trace_parent(&span, trace_parent.as_deref());

    async move {
        let mut parsed = 0;
        let mut parse_errors = 0;
        let mut drained_after_cancellation = false;
        let mut cancellation_deadline = None;
        let mut observations = adapters.parse(source_id.as_str(), fetched.artifact, &parse_ctx)?;
        let mut early_adapter_errors = Vec::new();
        let audit = ParseErrorAudit {
            artifact_id,
            dataflow_id: &fetched.dataflow_id,
            source_id: &source_id,
            correlation: &fetched.correlation,
            cancellation: &cancellation,
        };

        loop {
            let mut row_drained_after_cancellation = false;
            let row = if cancellation.is_cancelled() {
                let deadline =
                    *cancellation_deadline.get_or_insert_with(|| Instant::now() + shutdown_grace);
                match next_after_cancellation(&mut observations, deadline).await {
                    Ok(Some(row)) => {
                        row_drained_after_cancellation = true;
                        Some(row)
                    }
                    Ok(None) => None,
                    Err(err) => {
                        finish_cancelled_parse(&tx, &audit, &mut early_adapter_errors, parsed)
                            .await?;
                        return Err(err);
                    }
                }
            } else {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {
                        let deadline = *cancellation_deadline
                            .get_or_insert_with(|| Instant::now() + shutdown_grace);
                        match next_after_cancellation(&mut observations, deadline).await {
                            Ok(Some(row)) => {
                                row_drained_after_cancellation = true;
                                Some(row)
                            }
                            Ok(None) => None,
                            Err(err) => {
                                finish_cancelled_parse(
                                    &tx,
                                    &audit,
                                    &mut early_adapter_errors,
                                    parsed,
                                )
                                .await?;
                                return Err(err);
                            }
                        }
                    }
                    row = observations.next() => row,
                }
            };
            let Some(row) = row else {
                if cancellation.is_cancelled() && !drained_after_cancellation {
                    finish_cancelled_parse(&tx, &audit, &mut early_adapter_errors, parsed).await?;
                    return Err(IngestionError::Cancelled);
                }
                break;
            };
            if row_drained_after_cancellation || cancellation.is_cancelled() {
                drained_after_cancellation = true;
            }

            let (series, observation) = match row {
                Ok(row) => row,
                Err(err) => {
                    if matches!(err, AdapterError::SchemaHashDrift(_)) {
                        finish_schema_hash_drift_parse(
                            &tx,
                            &audit,
                            &mut early_adapter_errors,
                            &err,
                        )
                        .await?;
                        return Err(IngestionError::Adapter(err));
                    }
                    if parsed == 0 {
                        early_adapter_errors.push(err);
                    } else {
                        send_produced(
                            &tx,
                            LoadStageItem::ParseError(parse_error_record(
                                artifact_id,
                                &fetched.dataflow_id,
                                &source_id,
                                &fetched.correlation,
                                &err,
                                false,
                            )),
                            &cancellation,
                        )
                        .await?;
                        parse_errors += 1;
                    }
                    if cancellation.is_cancelled() {
                        finish_cancelled_parse(&tx, &audit, &mut early_adapter_errors, parsed)
                            .await?;
                        return Err(IngestionError::Cancelled);
                    }
                    continue;
                }
            };
            if series.dataflow_id != fetched.dataflow_id {
                let expected = fetched.dataflow_id.to_string();
                let actual = series.dataflow_id.to_string();
                send_adapter_parse_errors(&tx, &audit, &early_adapter_errors, false).await?;
                send_produced(
                    &tx,
                    LoadStageItem::RejectArtifact {
                        artifact_id,
                        correlation: fetched.correlation.clone(),
                    },
                    &cancellation,
                )
                .await?;
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
                send_adapter_parse_errors(&tx, &audit, &early_adapter_errors, false).await?;
                send_produced(
                    &tx,
                    LoadStageItem::RejectArtifact {
                        artifact_id,
                        correlation: fetched.correlation.clone(),
                    },
                    &cancellation,
                )
                .await?;
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
            if !early_adapter_errors.is_empty() {
                parse_errors += early_adapter_errors.len() as u64;
                send_adapter_parse_errors(&tx, &audit, &early_adapter_errors, false).await?;
                early_adapter_errors.clear();
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

        if parsed == 0 && !early_adapter_errors.is_empty() {
            let first_error = early_adapter_errors.remove(0);
            send_produced(
                &tx,
                LoadStageItem::ParseError(parse_error_record(
                    artifact_id,
                    &fetched.dataflow_id,
                    &source_id,
                    &fetched.correlation,
                    &first_error,
                    true,
                )),
                &cancellation,
            )
            .await?;
            send_adapter_parse_errors(&tx, &audit, &early_adapter_errors, true).await?;
            return Err(IngestionError::Adapter(first_error));
        }

        send_produced(
            &tx,
            LoadStageItem::AcceptArtifact {
                artifact_id,
                source_id: source_id.clone(),
                dataflow_id: fetched.dataflow_id.clone(),
                observations_parsed: parsed,
                parse_errors,
                correlation: fetched.correlation.clone(),
            },
            &cancellation,
        )
        .await?;

        Ok(parsed)
    }
    .instrument(span)
    .await
}

async fn finish_schema_hash_drift_parse(
    tx: &mpsc::Sender<LoadStageItem>,
    audit: &ParseErrorAudit<'_>,
    early_adapter_errors: &mut Vec<AdapterError>,
    err: &AdapterError,
) -> Result<(), IngestionError> {
    send_adapter_parse_errors(tx, audit, early_adapter_errors, false).await?;
    early_adapter_errors.clear();
    send_produced(
        tx,
        LoadStageItem::RejectArtifact {
            artifact_id: audit.artifact_id,
            correlation: audit.correlation.clone(),
        },
        audit.cancellation,
    )
    .await?;
    send_produced(
        tx,
        LoadStageItem::ParseError(parse_error_record(
            audit.artifact_id,
            audit.dataflow_id,
            audit.source_id,
            audit.correlation,
            err,
            true,
        )),
        audit.cancellation,
    )
    .await?;
    Ok(())
}

async fn next_after_cancellation(
    observations: &mut ObservationStream<'_>,
    deadline: Instant,
) -> Result<Option<Result<(SeriesDescriptor, Observation), AdapterError>>, IngestionError> {
    timeout_at(deadline, observations.next())
        .await
        .map_err(|_| IngestionError::Cancelled)
}

struct ParseErrorAudit<'a> {
    artifact_id: ArtifactId,
    dataflow_id: &'a DataflowId,
    source_id: &'a SourceId,
    correlation: &'a JobCorrelation,
    cancellation: &'a CancellationToken,
}

async fn send_adapter_parse_errors(
    tx: &mpsc::Sender<LoadStageItem>,
    audit: &ParseErrorAudit<'_>,
    errors: &[AdapterError],
    fatal: bool,
) -> Result<(), IngestionError> {
    for err in errors {
        send_produced(
            tx,
            LoadStageItem::ParseError(parse_error_record(
                audit.artifact_id,
                audit.dataflow_id,
                audit.source_id,
                audit.correlation,
                err,
                fatal,
            )),
            audit.cancellation,
        )
        .await?;
    }
    Ok(())
}

async fn finish_cancelled_parse(
    tx: &mpsc::Sender<LoadStageItem>,
    audit: &ParseErrorAudit<'_>,
    early_adapter_errors: &mut Vec<AdapterError>,
    parsed: u64,
) -> Result<(), IngestionError> {
    send_produced(
        tx,
        LoadStageItem::RejectArtifact {
            artifact_id: audit.artifact_id,
            correlation: audit.correlation.clone(),
        },
        audit.cancellation,
    )
    .await?;
    let had_early_adapter_errors = !early_adapter_errors.is_empty();
    if !early_adapter_errors.is_empty() {
        let fatal = parsed == 0;
        send_adapter_parse_errors(tx, audit, early_adapter_errors, fatal).await?;
        early_adapter_errors.clear();
    }
    if parsed > 0 || !had_early_adapter_errors {
        send_produced(
            tx,
            LoadStageItem::ParseError(parse_cancelled_error_record(audit, parsed)),
            audit.cancellation,
        )
        .await?;
    }
    Ok(())
}

async fn finish_panicked_parse(
    tx: &mpsc::Sender<LoadStageItem>,
    audit: &ParseJobAuditOwned,
    cancellation: &CancellationToken,
    err: &tokio::task::JoinError,
) -> Result<(), IngestionError> {
    send_produced(
        tx,
        LoadStageItem::RejectArtifact {
            artifact_id: audit.artifact_id,
            correlation: audit.correlation.clone(),
        },
        cancellation,
    )
    .await?;
    send_produced(
        tx,
        LoadStageItem::ParseError(parse_panic_error_record(audit, err)),
        cancellation,
    )
    .await?;
    Ok(())
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

fn parse_panic_error_record(
    audit: &ParseJobAuditOwned,
    err: &tokio::task::JoinError,
) -> ParseErrorRecord {
    ParseErrorRecord {
        artifact_id: audit.artifact_id,
        error_kind: "parser_panic",
        error_message: err.to_string(),
        row_context: Some(serde_json::json!({
            "dataflow_id": audit.dataflow_id,
            "source_id": audit.source_id,
            "artifact_id": audit.artifact_id,
            "job_id": audit.correlation.job_id.as_str(),
            "trace_parent": audit.correlation.trace_parent.as_deref(),
            "fatal": true,
        })),
    }
}

fn parse_cancelled_error_record(audit: &ParseErrorAudit<'_>, parsed: u64) -> ParseErrorRecord {
    ParseErrorRecord {
        artifact_id: audit.artifact_id,
        error_kind: "parse_cancelled",
        error_message: "parser cancelled before artifact stream was exhausted".to_string(),
        row_context: Some(serde_json::json!({
            "dataflow_id": audit.dataflow_id,
            "source_id": audit.source_id,
            "artifact_id": audit.artifact_id,
            "job_id": audit.correlation.job_id.as_str(),
            "trace_parent": audit.correlation.trace_parent.as_deref(),
            "rows_parsed": parsed,
            "fatal": true,
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
    let mut pending = BTreeMap::<PendingLoadKey, PendingArtifactLoad>::new();
    let mut accepted = AcceptedLoadBuffer::new(options);
    let mut accepted_completions = Vec::<PendingArtifactCompletion>::new();
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

        match item {
            LoadStageItem::Observation { item, correlation } => {
                let artifact_id = item.observation.source_artifact_id;
                let item_bytes = au_kpis_loader::estimate_load_item_bytes(&item)?;
                let key = PendingLoadKey::new(artifact_id, correlation.clone());
                let artifact = pending.entry(key).or_default();
                if artifact.will_stage(item_bytes, options) {
                    flush_accepted_if_needed(
                        &pool,
                        &mut accepted,
                        &mut accepted_completions,
                        options,
                        &mut loaded,
                    )
                    .await?;
                }
                artifact
                    .push(&pool, options, item, correlation, item_bytes)
                    .await?;
            }
            LoadStageItem::AcceptArtifact {
                artifact_id,
                source_id,
                dataflow_id,
                observations_parsed,
                parse_errors,
                correlation,
            } => {
                let key = PendingLoadKey::new(artifact_id, correlation);
                let mut artifact_stats = AcceptedArtifactStats::default();
                let mut accepted_artifact = false;
                if let Some(artifact) = pending.remove(&key) {
                    if artifact.is_staged() {
                        flush_accepted_if_needed(
                            &pool,
                            &mut accepted,
                            &mut accepted_completions,
                            options,
                            &mut loaded,
                        )
                        .await?;
                    }
                    artifact_stats = accept_artifact_load(
                        artifact,
                        &pool,
                        options,
                        &mut accepted,
                        &mut accepted_completions,
                        &mut loaded,
                    )
                    .await?;
                    accepted_artifact = true;
                }
                if accepted_artifact
                    && observations_parsed > 0
                    && parse_errors == 0
                    && artifact_stats.parse_errors == 0
                {
                    accepted_completions.push(PendingArtifactCompletion {
                        artifact_id,
                        source_id,
                        dataflow_id,
                        observations_parsed,
                        observations_loaded: artifact_stats.observations_loaded,
                        correlation: key.correlation,
                    });
                    if artifact_stats.committed || accepted.batch.is_empty() {
                        record_pending_artifact_completions(&pool, &mut accepted_completions)
                            .await?;
                    }
                }
            }
            LoadStageItem::RejectArtifact {
                artifact_id,
                correlation,
            } => {
                let key = PendingLoadKey::new(artifact_id, correlation);
                if let Some(artifact) = pending.remove(&key) {
                    flush_accepted_if_needed(
                        &pool,
                        &mut accepted,
                        &mut accepted_completions,
                        options,
                        &mut loaded,
                    )
                    .await?;
                    let staged_stats = artifact.rollback(&pool, options).await?;
                    add_load_stats(&mut loaded, staged_stats);
                }
            }
            LoadStageItem::ParseError(record) => {
                flush_accepted_if_needed(
                    &pool,
                    &mut accepted,
                    &mut accepted_completions,
                    options,
                    &mut loaded,
                )
                .await?;
                let trace_parent = parse_error_trace_parent(&record).map(str::to_owned);
                let span = info_span!(
                    "ingestion_load_parse_error",
                    trace_parent = trace_parent.as_deref().unwrap_or("")
                );
                restore_trace_parent(&span, trace_parent.as_deref());
                au_kpis_loader::record_parse_error(
                    &pool,
                    record.artifact_id,
                    record.error_kind,
                    &record.error_message,
                    record.row_context,
                )
                .instrument(span)
                .await?;
                loaded.parse_errors += 1;
            }
        }
    }
    flush_accepted_if_needed(
        &pool,
        &mut accepted,
        &mut accepted_completions,
        options,
        &mut loaded,
    )
    .await?;
    for (_, artifact) in pending {
        let staged_stats = artifact.rollback(&pool, options).await?;
        add_load_stats(&mut loaded, staged_stats);
    }
    Ok(PipelineRunStats {
        loaded,
        ..PipelineRunStats::default()
    })
}

async fn accept_artifact_load(
    artifact: PendingArtifactLoad,
    pool: &PgPool,
    options: LoadOptions,
    accepted: &mut AcceptedLoadBuffer,
    accepted_completions: &mut Vec<PendingArtifactCompletion>,
    loaded: &mut LoadStats,
) -> Result<AcceptedArtifactStats, IngestionError> {
    match artifact.accept(pool, options).await? {
        AcceptedArtifactLoad::Buffered {
            items,
            correlations,
        } => {
            append_accepted_load_items(
                pool,
                options,
                accepted,
                accepted_completions,
                loaded,
                items,
                correlations,
            )
            .await
        }
        AcceptedArtifactLoad::Committed(stats) => {
            add_load_stats(loaded, stats);
            Ok(AcceptedArtifactStats {
                observations_loaded: stats.observations_loaded,
                parse_errors: stats.parse_errors,
                committed: true,
            })
        }
    }
}

async fn append_accepted_load_items(
    pool: &PgPool,
    options: LoadOptions,
    accepted: &mut AcceptedLoadBuffer,
    accepted_completions: &mut Vec<PendingArtifactCompletion>,
    loaded: &mut LoadStats,
    items: Vec<LoadItem>,
    correlations: Vec<JobCorrelation>,
) -> Result<AcceptedArtifactStats, IngestionError> {
    if items.len() != correlations.len() {
        return Err(au_kpis_loader::LoadError::Validation(
            "accepted load item/correlation count mismatch".into(),
        )
        .into());
    }
    let audited_items = items
        .iter()
        .zip(correlations.iter())
        .map(|(item, correlation)| LoadItemAudit {
            item: item.clone(),
            row_context: Some(correlation.row_context()),
        })
        .collect::<Vec<_>>();
    let reference_validation =
        au_kpis_loader::validate_load_references(pool, &audited_items).await?;
    let mut artifact_stats = AcceptedArtifactStats {
        parse_errors: reference_validation.stats.parse_errors,
        ..AcceptedArtifactStats::default()
    };
    if reference_validation.stats.parse_errors > 0 {
        flush_accepted_if_needed(pool, accepted, accepted_completions, options, loaded).await?;
        add_load_stats(loaded, reference_validation.stats);
    }

    for ((item, correlation), valid) in items
        .into_iter()
        .zip(correlations)
        .zip(reference_validation.valid_rows)
    {
        if !valid {
            continue;
        }
        artifact_stats.observations_loaded += 1;
        let item_bytes = au_kpis_loader::estimate_load_item_bytes(&item)?;
        if au_kpis_loader::should_flush_load_batch(
            accepted.batch.len(),
            accepted.batch_bytes,
            item_bytes,
            options,
        ) {
            add_load_stats(
                loaded,
                flush_accepted_load_batch(pool, accepted, options).await?,
            );
            record_pending_artifact_completions(pool, accepted_completions).await?;
        }

        accepted.batch_bytes += item_bytes;
        accepted.batch.push(item);
        accepted.correlations.push(correlation);

        if au_kpis_loader::load_batch_boundary_reached(
            accepted.batch.len(),
            accepted.batch_bytes,
            options,
        ) {
            add_load_stats(
                loaded,
                flush_accepted_load_batch(pool, accepted, options).await?,
            );
            record_pending_artifact_completions(pool, accepted_completions).await?;
        }
    }
    Ok(artifact_stats)
}

async fn flush_accepted_if_needed(
    pool: &PgPool,
    accepted: &mut AcceptedLoadBuffer,
    accepted_completions: &mut Vec<PendingArtifactCompletion>,
    options: LoadOptions,
    loaded: &mut LoadStats,
) -> Result<(), IngestionError> {
    if accepted.batch.is_empty() {
        return Ok(());
    }
    add_load_stats(
        loaded,
        flush_accepted_load_batch(pool, accepted, options).await?,
    );
    record_pending_artifact_completions(pool, accepted_completions).await?;
    Ok(())
}

async fn record_pending_artifact_completions(
    pool: &PgPool,
    completions: &mut Vec<PendingArtifactCompletion>,
) -> Result<(), au_kpis_db::DbError> {
    for completion in completions.iter() {
        au_kpis_db::record_artifact_load_completion(
            pool,
            au_kpis_db::ArtifactLoadCompletion {
                artifact_id: completion.artifact_id,
                source_id: &completion.source_id,
                dataflow_id: &completion.dataflow_id,
                observations_parsed: completion.observations_parsed,
                observations_loaded: completion.observations_loaded,
                job_id: Some(completion.correlation.job_id.as_str()),
                trace_parent: completion.correlation.trace_parent.as_deref(),
            },
        )
        .await?;
    }
    completions.clear();
    Ok(())
}

#[derive(Debug)]
struct AcceptedLoadBuffer {
    batch: Vec<LoadItem>,
    correlations: Vec<JobCorrelation>,
    batch_bytes: usize,
}

impl AcceptedLoadBuffer {
    fn new(options: LoadOptions) -> Self {
        Self {
            batch: Vec::with_capacity(options.max_rows.min(1024)),
            correlations: Vec::with_capacity(options.max_rows.min(1024)),
            batch_bytes: 0,
        }
    }
}

#[derive(Debug, Default)]
struct PendingArtifactLoad {
    staged: Option<StagedLoad>,
    batch: Vec<LoadItem>,
    correlations: Vec<JobCorrelation>,
    batch_bytes: usize,
}

impl PendingArtifactLoad {
    fn is_staged(&self) -> bool {
        self.staged.is_some()
    }

    async fn push(
        &mut self,
        pool: &PgPool,
        options: LoadOptions,
        item: LoadItem,
        correlation: JobCorrelation,
        item_bytes: usize,
    ) -> Result<(), au_kpis_loader::LoadError> {
        if au_kpis_loader::should_flush_load_batch(
            self.batch.len(),
            self.batch_bytes,
            item_bytes,
            options,
        ) {
            self.stage(pool, options).await?;
        }

        self.batch_bytes += item_bytes;
        self.batch.push(item);
        self.correlations.push(correlation);

        if au_kpis_loader::load_batch_boundary_reached(self.batch.len(), self.batch_bytes, options)
        {
            self.stage(pool, options).await?;
        }

        Ok(())
    }

    fn will_stage(&self, next_item_bytes: usize, options: LoadOptions) -> bool {
        au_kpis_loader::should_flush_load_batch(
            self.batch.len(),
            self.batch_bytes,
            next_item_bytes,
            options,
        ) || self.batch.len() + 1 >= options.max_rows
            || self.batch_bytes.saturating_add(next_item_bytes) >= options.max_bytes
    }

    async fn accept(
        mut self,
        pool: &PgPool,
        options: LoadOptions,
    ) -> Result<AcceptedArtifactLoad, au_kpis_loader::LoadError> {
        if self.staged.is_some() {
            self.stage(pool, options).await?;
            let staged = self.staged.expect("staged load remains after stage");
            Ok(AcceptedArtifactLoad::Committed(staged.commit().await?))
        } else {
            Ok(AcceptedArtifactLoad::Buffered {
                items: self.batch,
                correlations: self.correlations,
            })
        }
    }

    async fn rollback(
        mut self,
        pool: &PgPool,
        options: LoadOptions,
    ) -> Result<LoadStats, au_kpis_loader::LoadError> {
        if !self.batch.is_empty() {
            self.stage(pool, options).await?;
        }
        if let Some(staged) = self.staged {
            return staged.rollback().await;
        }
        Ok(LoadStats::default())
    }

    async fn stage(
        &mut self,
        pool: &PgPool,
        options: LoadOptions,
    ) -> Result<(), au_kpis_loader::LoadError> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let items = match audited_load_items(&mut self.batch, &mut self.correlations) {
            Ok(items) => items,
            Err(err) => {
                if let Some(staged) = self.staged.take() {
                    return match staged.rollback().await {
                        Ok(_) => Err(err),
                        Err(cleanup_err) => Err(cleanup_err),
                    };
                }
                return Err(err);
            }
        };
        emit_load_correlation_spans(&items);
        let mut staged = match self.staged.take() {
            Some(staged) => staged,
            None => au_kpis_loader::begin_staged_load(pool, options).await?,
        };
        let span = info_span!("ingestion_load_stage_batch", rows = items.len(),);
        let result = staged
            .stage(
                items
                    .into_iter()
                    .map(|(item, correlation)| LoadItemAudit {
                        item,
                        row_context: Some(correlation.row_context()),
                    })
                    .collect(),
            )
            .instrument(span)
            .await;
        if let Err(err) = result {
            return match staged.rollback().await {
                Ok(_) => Err(err),
                Err(cleanup_err) => Err(cleanup_err),
            };
        }
        self.staged = Some(staged);
        self.batch_bytes = 0;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct AcceptedArtifactStats {
    observations_loaded: u64,
    parse_errors: u64,
    committed: bool,
}

#[derive(Debug)]
enum AcceptedArtifactLoad {
    Buffered {
        items: Vec<LoadItem>,
        correlations: Vec<JobCorrelation>,
    },
    Committed(LoadStats),
}

fn audited_load_items(
    batch: &mut Vec<LoadItem>,
    correlations: &mut Vec<JobCorrelation>,
) -> Result<Vec<(LoadItem, JobCorrelation)>, au_kpis_loader::LoadError> {
    let items = std::mem::take(batch);
    let correlations = std::mem::take(correlations);
    if items.len() != correlations.len() {
        return Err(au_kpis_loader::LoadError::Validation(
            "load batch item/correlation count mismatch".into(),
        ));
    }
    Ok(items.into_iter().zip(correlations).collect())
}

fn emit_load_correlation_spans(items: &[(LoadItem, JobCorrelation)]) {
    if !tracing::enabled!(Level::TRACE) {
        return;
    }

    let mut row_counts = BTreeMap::<(String, Option<String>), u64>::new();
    for (_, correlation) in items {
        *row_counts
            .entry((correlation.job_id.clone(), correlation.trace_parent.clone()))
            .or_default() += 1;
    }
    for ((job_id, trace_parent), rows) in row_counts {
        let span = trace_span!(
            "ingestion_load_batch",
            job_id = %job_id,
            trace_parent = trace_parent.as_deref().unwrap_or(""),
            rows
        );
        restore_trace_parent(&span, trace_parent.as_deref());
        let _entered = span.enter();
        tracing::info!("load batch correlation");
    }
}

async fn flush_accepted_load_batch(
    pool: &PgPool,
    accepted: &mut AcceptedLoadBuffer,
    options: LoadOptions,
) -> Result<LoadStats, au_kpis_loader::LoadError> {
    let items = audited_load_items(&mut accepted.batch, &mut accepted.correlations)?;
    accepted.batch_bytes = 0;
    emit_load_correlation_spans(&items);
    let span = info_span!("ingestion_load_commit_batch", rows = items.len(),);
    au_kpis_loader::load_batch_with_options_and_audit_context(
        pool,
        items
            .into_iter()
            .map(|(item, correlation)| LoadItemAudit {
                item,
                row_context: Some(correlation.row_context()),
            })
            .collect(),
        options,
    )
    .instrument(span)
    .await
}

fn parse_error_trace_parent(record: &ParseErrorRecord) -> Option<&str> {
    record
        .row_context
        .as_ref()
        .and_then(|context| context.get("trace_parent"))
        .and_then(serde_json::Value::as_str)
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

    use au_kpis_adapter::DiscoveredJob;
    use au_kpis_domain::{
        Observation, ObservationStatus, SeriesDescriptor, TimePrecision,
        ids::{ArtifactId, CodeId, DataflowId, DimensionId, MeasureId, SeriesKey},
    };
    use chrono::{TimeZone, Utc};
    use opentelemetry::trace::TraceContextExt;

    use super::*;

    const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn load_stage_uses_loader_owned_byte_cap_boundary() {
        let first = load_item_with_attribute_bytes(8);
        let second = load_item_with_attribute_bytes(8);
        let first_bytes = au_kpis_loader::estimate_load_item_bytes(&first).unwrap();
        let second_bytes = au_kpis_loader::estimate_load_item_bytes(&second).unwrap();
        let options = LoadOptions {
            max_rows: 64,
            max_bytes: first_bytes + second_bytes - 1,
        };

        assert!(!au_kpis_loader::should_flush_load_batch(
            0,
            0,
            first_bytes,
            options
        ));
        assert!(au_kpis_loader::should_flush_load_batch(
            1,
            first_bytes,
            second_bytes,
            options
        ));
    }

    #[test]
    fn load_stage_does_not_flush_empty_batch_for_oversized_single_item() {
        let item = load_item_with_attribute_bytes(1024);
        let item_bytes = au_kpis_loader::estimate_load_item_bytes(&item).unwrap();
        let options = LoadOptions {
            max_rows: 64,
            max_bytes: item_bytes - 1,
        };

        assert!(!au_kpis_loader::should_flush_load_batch(
            0, 0, item_bytes, options
        ));
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

    #[test]
    fn requested_dataflow_filter_keeps_only_matching_jobs() {
        let requested = DataflowId::new("abs.cpi").unwrap();
        let jobs = vec![
            discovered_job("job-cpi", "abs.cpi"),
            discovered_job("job-wpi", "abs.wpi"),
        ];

        let filtered = filter_discovered_jobs(jobs, Some(&requested));

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "job-cpi");
        assert_eq!(filtered[0].dataflow_id, requested);
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

    #[tokio::test]
    async fn post_cancel_parser_drain_uses_absolute_deadline() {
        let item = load_item_with_attribute_bytes(8);
        let row = (item.series, item.observation);
        let mut observations: ObservationStream<'static> =
            Box::pin(futures::stream::unfold(0_u8, move |state| {
                let row = row.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Some((Ok(row), state.saturating_add(1)))
                }
            }));
        let deadline = Instant::now() + Duration::from_millis(120);

        assert!(
            next_after_cancellation(&mut observations, deadline)
                .await
                .expect("first row before deadline")
                .is_some()
        );
        assert!(
            next_after_cancellation(&mut observations, deadline)
                .await
                .expect("second row before deadline")
                .is_some()
        );
        assert!(matches!(
            next_after_cancellation(&mut observations, deadline).await,
            Err(IngestionError::Cancelled)
        ));
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

    fn discovered_job(id: &str, dataflow_id: &str) -> DiscoveredJob {
        DiscoveredJob {
            id: id.to_string(),
            source_id: SourceId::new("abs").unwrap(),
            dataflow_id: DataflowId::new(dataflow_id).unwrap(),
            source_url: format!("https://example.test/{id}.json"),
            trace_parent: Some(TRACE_PARENT.into()),
            metadata: BTreeMap::new(),
        }
    }
}
