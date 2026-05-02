//! Ingestion orchestration (discover -> fetch -> parse -> load).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterHttpClient, Adapters, ArtifactRecorder, ArtifactRecorderRef, DiscoveryCtx,
    FetchCtx, ParseCtx,
};
use au_kpis_db::PgPool;
use au_kpis_domain::{Artifact, ids::ArtifactId, ids::DataflowId, ids::SourceId};
use au_kpis_error::Classify;
use au_kpis_loader::{LoadItem, LoadOptions, LoadStats};
use au_kpis_storage::BlobStore;
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::FuturesUnordered};
use thiserror::Error;
use tokio::{
    sync::{mpsc, mpsc::error::TryRecvError},
    task::JoinSet,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

const DEFAULT_CHANNEL_CAPACITY: usize = 64;
const DEFAULT_STAGE_CONCURRENCY: usize = 4;
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

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
        contexts: PipelineContexts,
        cancellation: CancellationToken,
    ) -> Result<PipelineRunStats, IngestionError> {
        self.validate_options()?;

        let (discovered_tx, discovered_rx) = mpsc::channel(self.options.channel_capacity);
        let (artifact_tx, artifact_rx) = mpsc::channel(self.options.channel_capacity);
        let (load_tx, load_rx) = mpsc::channel(self.options.channel_capacity);

        let mut tasks = JoinSet::new();
        let source = source_id.as_str().to_string();
        let pipeline_token = cancellation.child_token();

        tasks.spawn(
            discover_stage(
                self.adapters.clone(),
                source_id.clone(),
                contexts.discovery,
                discovered_tx,
                pipeline_token.clone(),
            )
            .instrument(info_span!("ingestion_discover", source = %source)),
        );
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
            .instrument(info_span!("ingestion_fetch", source = %source)),
        );
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
            .instrument(info_span!("ingestion_parse", source = %source)),
        );
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
            .instrument(info_span!("ingestion_load", source = %source)),
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
                let errors = timeout(shutdown_grace, drain_task_errors(tasks))
                    .await
                    .map_err(|_| IngestionError::ShutdownTimeout(shutdown_grace))?;
                return Err(preferred_error(IngestionError::Cancelled, errors));
            }
            result = tasks.join_next() => {
                let Some(result) = result else {
                    if cancellation.is_cancelled() {
                        return Err(IngestionError::Cancelled);
                    }
                    return Ok(stats);
                };
                match result? {
                    Ok(stage_stats) => stats.add(stage_stats),
                    Err(err) => {
                        pipeline_token.cancel();
                        let errors = timeout(shutdown_grace, drain_task_errors(tasks))
                            .await
                            .map_err(|_| IngestionError::ShutdownTimeout(shutdown_grace))?;
                        return Err(preferred_error(err, errors));
                    }
                }
            }
        }
    }
}

async fn drain_task_errors(
    mut tasks: JoinSet<Result<PipelineRunStats, IngestionError>>,
) -> Vec<IngestionError> {
    let mut errors = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => errors.push(err),
            Err(err) => errors.push(IngestionError::Join(err)),
        }
    }
    errors
}

#[derive(Debug)]
struct FetchedArtifact {
    artifact: au_kpis_adapter::ArtifactRef,
    dataflow_id: DataflowId,
    metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Debug)]
enum LoadStageItem {
    Observation(LoadItem),
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
        send_produced(&tx, job).await?;
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
    let mut draining = false;
    let mut input_closed = false;
    let mut in_flight = FuturesUnordered::new();

    loop {
        while !input_closed && in_flight.len() < concurrency {
            let Some(job) = recv_or_stop_on_cancel(&mut rx, &cancellation, &mut draining).await
            else {
                input_closed = true;
                break;
            };
            validate_source_id("fetch", &source_id, &job.source_id)?;
            in_flight.push(fetch_one(
                adapters.clone(),
                source_id.clone(),
                ctx.clone(),
                job,
            ));
        }

        if in_flight.is_empty() {
            break;
        }

        let fetched_artifact = in_flight.next().await.expect("in_flight is not empty")?;
        send_produced(&tx, fetched_artifact).await?;
        fetched += 1;
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
    let mut draining = false;
    let mut input_closed = false;
    let mut in_flight = FuturesUnordered::new();

    loop {
        while !input_closed && in_flight.len() < concurrency {
            let Some(fetched) = recv_or_stop_on_cancel(&mut rx, &cancellation, &mut draining).await
            else {
                input_closed = true;
                break;
            };
            validate_source_id("parse", &source_id, &fetched.artifact.source_id)?;
            let parse_ctx = ctx
                .clone()
                .with_expected_dataflow(fetched.dataflow_id.clone(), fetched.metadata.clone());
            in_flight.push(parse_one_artifact(
                adapters.clone(),
                source_id.clone(),
                parse_ctx,
                tx.clone(),
                fetched,
            ));
        }

        if in_flight.is_empty() {
            break;
        }

        parsed += in_flight.next().await.expect("in_flight is not empty")?;
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
) -> Result<FetchedArtifact, IngestionError> {
    let dataflow_id = job.dataflow_id.clone();
    let metadata = job.metadata.clone();
    let artifact = adapters.fetch(source_id.as_str(), job, &ctx).await?;
    validate_source_id("fetch", &source_id, &artifact.source_id)?;
    Ok(FetchedArtifact {
        artifact,
        dataflow_id,
        metadata,
    })
}

async fn parse_one_artifact(
    adapters: Adapters,
    source_id: SourceId,
    parse_ctx: ParseCtx,
    tx: mpsc::Sender<LoadStageItem>,
    fetched: FetchedArtifact,
) -> Result<u64, IngestionError> {
    let artifact_id = fetched.artifact.id;
    let mut parsed = 0;
    let mut observations = adapters.parse(source_id.as_str(), fetched.artifact, &parse_ctx)?;

    while let Some(row) = observations.next().await {
        let (series, observation) = match row {
            Ok(row) => row,
            Err(err) => {
                send_produced(
                    &tx,
                    LoadStageItem::ParseError(parse_error_record(
                        artifact_id,
                        &fetched.dataflow_id,
                        &source_id,
                        &err,
                        parsed == 0,
                    )),
                )
                .await?;
                if parsed == 0 {
                    return Err(IngestionError::Adapter(err));
                }
                continue;
            }
        };
        if series.dataflow_id != fetched.dataflow_id {
            return Err(IngestionError::DataflowMismatch {
                expected: fetched.dataflow_id.to_string(),
                actual: series.dataflow_id.to_string(),
            });
        }
        if observation.source_artifact_id != artifact_id {
            return Err(IngestionError::ArtifactMismatch {
                expected: artifact_id.to_string(),
                actual: observation.source_artifact_id.to_string(),
            });
        }
        send_produced(
            &tx,
            LoadStageItem::Observation(LoadItem {
                series,
                observation,
            }),
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
            "error_class": format!("{:?}", err.class()),
            "fatal": fatal,
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

        let item = match item {
            LoadStageItem::Observation(item) => item,
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
                au_kpis_loader::load_batch_with_options(&pool, std::mem::take(&mut batch), options)
                    .await?,
            );
            batch_bytes = 0;
        }

        batch_bytes += item_bytes;
        batch.push(item);

        if batch.len() >= options.max_rows || batch_bytes >= options.max_bytes {
            add_load_stats(
                &mut loaded,
                au_kpis_loader::load_batch_with_options(&pool, std::mem::take(&mut batch), options)
                    .await?,
            );
            batch_bytes = 0;
        }
    }
    if !batch.is_empty() {
        add_load_stats(
            &mut loaded,
            au_kpis_loader::load_batch_with_options(&pool, batch, options).await?,
        );
    }
    Ok(PipelineRunStats {
        loaded,
        ..PipelineRunStats::default()
    })
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

async fn send_produced<T>(tx: &mpsc::Sender<T>, value: T) -> Result<(), IngestionError> {
    tx.send(value)
        .await
        .map_err(|_| IngestionError::DownstreamClosed)
}

async fn recv_or_stop_on_cancel<T>(
    rx: &mut mpsc::Receiver<T>,
    cancellation: &CancellationToken,
    draining: &mut bool,
) -> Option<T> {
    loop {
        if *draining {
            return match rx.try_recv() {
                Ok(value) => Some(value),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => None,
            };
        }

        tokio::select! {
            () = cancellation.cancelled() => *draining = true,
            value = rx.recv() => return value,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use au_kpis_domain::{
        Observation, ObservationStatus, SeriesDescriptor, TimePrecision,
        ids::{ArtifactId, CodeId, DataflowId, DimensionId, MeasureId, SeriesKey},
    };
    use chrono::{TimeZone, Utc};

    use super::*;

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
    async fn produced_handoff_waits_for_capacity_instead_of_dropping_item() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(1).await.expect("seed full channel");

        let sender = tokio::spawn(async move { send_produced(&tx, 2).await });

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
