//! Ingestion worker binary.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::{
    env,
    ffi::OsString,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, bail};
use au_kpis_adapter::{AdapterHttpClient, Adapters, DiscoveryCtx, ParseCtx};
use au_kpis_adapter_abs::AbsAdapter;
use au_kpis_adapter_apra::ApraAdapter;
use au_kpis_adapter_rba::RbaAdapter;
use au_kpis_adapter_treasury::TreasuryAdapter;
use au_kpis_config::load_ingestion;
use au_kpis_db::{connect as connect_db, migrate};
use au_kpis_domain::ids::{DataflowId, SourceId};
use au_kpis_error::{Classify, ErrorClass};
use au_kpis_ingestion_core::{IngestionPipeline, PipelineContexts, PipelineOptions, fetch_ctx};
use au_kpis_queue::{ApalisPgQueue, JobKind, LeasedJob, Nack, Queue, QueueStage, WorkerId};
use au_kpis_storage::BlobStore;
use au_kpis_telemetry::{Telemetry, init as init_telemetry};
use axum::{Router, http::header, response::IntoResponse, routing::get};
use clap::{Parser, Subcommand};
use object_store::aws::AmazonS3Builder;
use tokio::{net::TcpListener, signal, time::Instant};
use tokio_util::sync::CancellationToken;

const ABS_CPI_DATAFLOW_SLUG: &str = "cpi";
const ABS_CPI_DATAFLOW_ID: &str = "abs.cpi";
const APRA_QUARTERLY_DATAFLOW_SLUG: &str = "quarterly-statistics";
const APRA_QUARTERLY_DATAFLOW_ID: &str = "apra.quarterly_statistics";
const RBA_STAT_TABLES_DATAFLOW_SLUG: &str = "statistical-tables";
const RBA_STAT_TABLES_DATAFLOW_ID: &str = "rba.statistical_tables";
const TREASURY_BUDGET_DATAFLOW_SLUG: &str = "budget-papers";
const TREASURY_BUDGET_DATAFLOW_ID: &str = "treasury.budget_papers";
const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;

/// Command-line arguments for `au-kpis-ingestion`.
#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    /// Run one end-to-end ingestion pass for a source/dataflow pair.
    #[arg(long)]
    once: bool,

    /// Source id for `--once`.
    #[arg(long)]
    source: Option<String>,

    /// Dataflow id for `--once`.
    #[arg(long)]
    dataflow: Option<String>,

    /// Worker id recorded on queue leases in `run` mode.
    #[arg(long, env = "AU_KPIS_WORKER_ID")]
    worker_id: Option<String>,

    /// Poll interval when no queue jobs are ready.
    #[arg(long, env = "AU_KPIS_QUEUE_POLL_INTERVAL_MS", default_value_t = DEFAULT_POLL_INTERVAL_MS)]
    poll_interval_ms: u64,

    #[command(subcommand)]
    command: Option<Command>,
}

/// Subcommands for long-running operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Subcommand)]
enum Command {
    /// Start the long-running worker loop.
    Run,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    Once { source: String, dataflow: String },
    Run,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunRequest {
    source_id: SourceId,
    dataflow_id: Option<DataflowId>,
    trace_parent: Option<String>,
}

#[derive(Debug, Default)]
struct WorkerMetrics {
    worker_loops_total: AtomicU64,
    jobs_completed_total: AtomicU64,
    jobs_failed_total: AtomicU64,
    once_runs_total: AtomicU64,
}

impl WorkerMetrics {
    fn render_prometheus(&self) -> String {
        format!(
            "# HELP au_kpis_ingestion_worker_loops_total Worker polling loop iterations.\n\
             # TYPE au_kpis_ingestion_worker_loops_total counter\n\
             au_kpis_ingestion_worker_loops_total {}\n\
             # HELP au_kpis_ingestion_jobs_completed_total Queue jobs completed by the worker.\n\
             # TYPE au_kpis_ingestion_jobs_completed_total counter\n\
             au_kpis_ingestion_jobs_completed_total {}\n\
             # HELP au_kpis_ingestion_jobs_failed_total Queue jobs failed by the worker.\n\
             # TYPE au_kpis_ingestion_jobs_failed_total counter\n\
             au_kpis_ingestion_jobs_failed_total {}\n\
             # HELP au_kpis_ingestion_once_runs_total One-shot ingestion runs completed.\n\
             # TYPE au_kpis_ingestion_once_runs_total counter\n\
             au_kpis_ingestion_once_runs_total {}\n",
            self.worker_loops_total.load(Ordering::Relaxed),
            self.jobs_completed_total.load(Ordering::Relaxed),
            self.jobs_failed_total.load(Ordering::Relaxed),
            self.once_runs_total.load(Ordering::Relaxed)
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ObjectStoreConfig {
    endpoint: Option<String>,
    bucket: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    region: Option<String>,
    allow_http: bool,
}

impl ObjectStoreConfig {
    fn from_env() -> Self {
        Self {
            endpoint: env::var("AU_KPIS_OBJECT_STORE__ENDPOINT").ok(),
            bucket: env::var("AU_KPIS_OBJECT_STORE__BUCKET").ok(),
            access_key_id: env::var("AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID").ok(),
            secret_access_key: env::var("AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY").ok(),
            region: env::var("AU_KPIS_OBJECT_STORE__REGION").ok(),
            allow_http: env::var("AU_KPIS_OBJECT_STORE__ALLOW_HTTP")
                .ok()
                .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes")),
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
#[cfg_attr(coverage_nightly, coverage(off))]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mode = resolve_mode(&cli)?;
    if let Mode::Once { source, dataflow } = &mode {
        validate_once_target(source, dataflow)?;
    }
    let config = Arc::new(load_ingestion(None).context("load config")?);
    let _telemetry = init_or_disabled(&config.telemetry)?;
    let db = connect_db(&config.database)
        .await
        .context("connect postgres database")?;
    migrate(&db).await.context("apply database migrations")?;

    let drain_window = Duration::from_secs(config.http.shutdown_grace_period_secs);
    let shutdown = CancellationToken::new();
    let metrics = Arc::new(WorkerMetrics::default());
    let runtime = Runtime {
        adapters: build_adapters()?,
        db,
        blob_store: build_blob_store(&mode, ObjectStoreConfig::from_env())?,
        metrics,
        pipeline_options: pipeline_options(drain_window),
        shutdown: shutdown.clone(),
        poll_interval: Duration::from_millis(cli.poll_interval_ms),
        worker_id: cli.worker_id.unwrap_or_else(default_worker_id),
    };
    let listener = TcpListener::bind(&config.http.bind)
        .await
        .with_context(|| format!("bind metrics listener on {}", config.http.bind))?;
    write_startup_notify(&listener)
        .await
        .context("write startup notification")?;
    let metrics_server = tokio::spawn(serve_metrics(
        listener,
        Arc::clone(&runtime.metrics),
        shutdown.clone(),
    ));
    let shutdown_listener = shutdown_signal(shutdown.clone());
    tokio::pin!(shutdown_listener);
    let work = run_mode(mode, runtime);
    tokio::pin!(work);

    tokio::select! {
        result = &mut work => {
            shutdown.cancel();
            result?;
        }
        result = &mut shutdown_listener => {
            result.context("listen for shutdown signal")?;
            match tokio::time::timeout(drain_window, &mut work).await {
                Ok(result) => result?,
                Err(_) => tracing::warn!(
                    drain_window_secs = drain_window.as_secs(),
                    "ingestion drain window elapsed; forcing worker exit"
                ),
            }
        }
    }

    match tokio::time::timeout(drain_window, metrics_server).await {
        Ok(result) => result.context("join metrics server")??,
        Err(_) => tracing::warn!(
            drain_window_secs = drain_window.as_secs(),
            "metrics server drain window elapsed; forcing exit"
        ),
    }

    Ok(())
}

#[derive(Debug)]
struct Runtime {
    adapters: Adapters,
    db: au_kpis_db::PgPool,
    blob_store: BlobStore,
    metrics: Arc<WorkerMetrics>,
    pipeline_options: PipelineOptions,
    shutdown: CancellationToken,
    poll_interval: Duration,
    worker_id: String,
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn run_mode(mode: Mode, runtime: Runtime) -> anyhow::Result<()> {
    match mode {
        Mode::Once { source, dataflow } => {
            let request = once_run_request(&source, &dataflow)?;
            let stats = run_source_once(&runtime, &request, runtime.shutdown.clone()).await?;
            runtime
                .metrics
                .once_runs_total
                .fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                source = request.source_id.as_str(),
                dataflow = request
                    .dataflow_id
                    .as_ref()
                    .map_or(dataflow.as_str(), DataflowId::as_str),
                discovered = stats.discovered,
                fetched = stats.fetched,
                parsed = stats.parsed,
                loaded = stats.loaded.observations_loaded,
                "one-shot ingestion completed"
            );
            Ok(())
        }
        Mode::Run => run_worker(runtime).await,
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn run_source_once(
    runtime: &Runtime,
    request: &RunRequest,
    cancellation: CancellationToken,
) -> Result<au_kpis_ingestion_core::PipelineRunStats, au_kpis_ingestion_core::IngestionError> {
    let adapter = runtime.adapters.get(request.source_id.as_str())?;
    let started_at = chrono::Utc::now();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let mut discovery = DiscoveryCtx::new(http.clone(), started_at);
    if let Some(trace_parent) = &request.trace_parent {
        discovery = discovery.with_trace_parent(trace_parent.clone());
    }
    if let Some(dataflow_id) = &request.dataflow_id {
        discovery = discovery.with_requested_dataflow_id(dataflow_id.clone());
    }
    let contexts = PipelineContexts {
        discovery,
        fetch: fetch_ctx(
            http.clone(),
            runtime.blob_store.clone(),
            started_at,
            runtime.db.clone(),
        ),
        parse: ParseCtx::new(http, runtime.blob_store.clone(), started_at),
    };
    IngestionPipeline::new(runtime.adapters.clone(), runtime.db.clone())
        .with_options(runtime.pipeline_options)
        .run_source(request.source_id.clone(), contexts, cancellation)
        .await
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn run_worker(runtime: Runtime) -> anyhow::Result<()> {
    let queue = ApalisPgQueue::new(runtime.db.clone());
    let worker_id = WorkerId::new(runtime.worker_id.clone()).context("build queue worker id")?;
    let shutdown = runtime.shutdown.clone();
    let metrics = Arc::clone(&runtime.metrics);

    run_worker_loop(shutdown, runtime.poll_interval, metrics, || {
        process_one_job(&runtime, &queue, worker_id.clone())
    })
    .await
}

async fn run_worker_loop<P, Fut>(
    shutdown: CancellationToken,
    poll_interval: Duration,
    metrics: Arc<WorkerMetrics>,
    mut process_next: P,
) -> anyhow::Result<()>
where
    P: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<WorkerStep>>,
{
    loop {
        metrics.worker_loops_total.fetch_add(1, Ordering::Relaxed);

        if shutdown.is_cancelled() {
            return Ok(());
        }

        match process_next().await? {
            WorkerStep::Processed => {}
            WorkerStep::Idle => {
                tokio::select! {
                    () = shutdown.cancelled() => return Ok(()),
                    () = tokio::time::sleep(poll_interval) => {}
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerStep {
    Processed,
    Idle,
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn process_one_job(
    runtime: &Runtime,
    queue: &ApalisPgQueue,
    worker_id: WorkerId,
) -> anyhow::Result<WorkerStep> {
    let mut leased = None;
    for stage in [QueueStage::Discover, QueueStage::Backfill] {
        if let Some(job) = queue
            .pop(stage, worker_id.clone())
            .await
            .with_context(|| format!("pop {stage} job"))?
        {
            leased = Some(job);
            break;
        }
    }
    let Some(job) = leased else {
        return Ok(WorkerStep::Idle);
    };

    let request = match job_run_request(job.job().kind(), job.trace_parent()) {
        Ok(request) => request,
        Err(err) => {
            queue
                .nack(&job, invalid_job_nack(err))
                .await
                .context("nack invalid queue job")?;
            runtime
                .metrics
                .jobs_failed_total
                .fetch_add(1, Ordering::Relaxed);
            return Ok(WorkerStep::Processed);
        }
    };
    let renewal_interval = lease_renewal_interval(queue.lease_timeout());
    let worker_shutdown = runtime.shutdown.child_token();
    let (job, result) = run_with_lease_renewal(queue, job, renewal_interval, async {
        run_source_once(runtime, &request, worker_shutdown).await
    })
    .await?;

    match result {
        Ok(stats) => {
            queue.ack(&job).await.context("ack queue job")?;
            runtime
                .metrics
                .jobs_completed_total
                .fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                job_id = %job.id(),
                discovered = stats.discovered,
                fetched = stats.fetched,
                parsed = stats.parsed,
                loaded = stats.loaded.observations_loaded,
                "queue ingestion job completed"
            );
        }
        Err(err) => {
            let class = ingestion_error_class(&err);
            queue
                .nack(&job, Nack::new(class, err.to_string()))
                .await
                .context("nack queue job")?;
            runtime
                .metrics
                .jobs_failed_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(WorkerStep::Processed)
}

trait LeaseClient<L> {
    async fn renew(&self, lease: &L) -> anyhow::Result<L>;
}

impl LeaseClient<LeasedJob> for ApalisPgQueue {
    async fn renew(&self, lease: &LeasedJob) -> anyhow::Result<LeasedJob> {
        Queue::renew(self, lease).await.context("renew queue lease")
    }
}

async fn run_with_lease_renewal<L, C, F>(
    client: &C,
    mut lease: L,
    interval: Duration,
    work: F,
) -> anyhow::Result<(L, F::Output)>
where
    L: Clone,
    C: LeaseClient<L> + Sync,
    F: Future,
{
    let work = work;
    tokio::pin!(work);
    let timer = tokio::time::sleep(interval);
    tokio::pin!(timer);

    loop {
        tokio::select! {
            result = &mut work => return Ok((lease, result)),
            () = &mut timer => {
                lease = client.renew(&lease).await?;
                timer.as_mut().reset(Instant::now() + interval);
            }
        }
    }
}

fn lease_renewal_interval(lease_timeout: Duration) -> Duration {
    std::cmp::max(lease_timeout / 2, Duration::from_secs(1))
}

fn invalid_job_nack(err: anyhow::Error) -> Nack {
    Nack::new(ErrorClass::Permanent, err.to_string())
}

fn ingestion_error_class(err: &au_kpis_ingestion_core::IngestionError) -> ErrorClass {
    match err {
        au_kpis_ingestion_core::IngestionError::Adapter(err) => err.class(),
        au_kpis_ingestion_core::IngestionError::Load(err) => load_error_class(err),
        au_kpis_ingestion_core::IngestionError::Config(_)
        | au_kpis_ingestion_core::IngestionError::SourceMismatch { .. }
        | au_kpis_ingestion_core::IngestionError::DataflowMismatch { .. }
        | au_kpis_ingestion_core::IngestionError::ArtifactMismatch { .. } => ErrorClass::Permanent,
        au_kpis_ingestion_core::IngestionError::Cancelled
        | au_kpis_ingestion_core::IngestionError::DownstreamClosed
        | au_kpis_ingestion_core::IngestionError::Join(_)
        | au_kpis_ingestion_core::IngestionError::ShutdownTimeout(_) => ErrorClass::Transient,
    }
}

fn load_error_class(err: &au_kpis_loader::LoadError) -> ErrorClass {
    match err {
        au_kpis_loader::LoadError::Validation(_) => ErrorClass::Validation,
        au_kpis_loader::LoadError::Json(_) => ErrorClass::Permanent,
        au_kpis_loader::LoadError::Db(_) => ErrorClass::Transient,
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn serve_metrics(
    listener: TcpListener,
    metrics: Arc<WorkerMetrics>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let app = Router::new().route(
        "/metrics",
        get({
            let metrics = Arc::clone(&metrics);
            move || async move {
                (
                    [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                    metrics.render_prometheus(),
                )
                    .into_response()
            }
        }),
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
        .context("serve ingestion metrics")
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn build_adapters() -> anyhow::Result<Adapters> {
    let mut builder = Adapters::builder();
    let abs = match env::var("AU_KPIS_ABS_BASE_URL") {
        Ok(base_url) => AbsAdapter::builder().base_url(base_url).build(),
        Err(_) => AbsAdapter::default(),
    };
    builder.register(abs).context("register ABS adapter")?;
    let apra = match env::var("AU_KPIS_APRA_RELEASE_URL") {
        Ok(release_url) => ApraAdapter::builder().release_url(release_url).build(),
        Err(_) => ApraAdapter::default(),
    };
    builder.register(apra).context("register APRA adapter")?;
    let rba = match env::var("AU_KPIS_RBA_INDEX_URL") {
        Ok(index_url) => RbaAdapter::builder().index_url(index_url).build(),
        Err(_) => RbaAdapter::default(),
    };
    builder.register(rba).context("register RBA adapter")?;
    let mut treasury = TreasuryAdapter::builder();
    if let Ok(budget_url) = env::var("AU_KPIS_TREASURY_BUDGET_URL") {
        treasury = treasury.budget_url(budget_url);
    }
    if let Ok(pdf_base_url) = env::var("AU_KPIS_PDF_BASE_URL") {
        treasury = treasury.pdf_base_url(pdf_base_url);
    }
    builder
        .register(treasury.try_build().context("build Treasury adapter")?)
        .context("register Treasury adapter")?;
    Ok(builder.build())
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn build_blob_store(mode: &Mode, config: ObjectStoreConfig) -> anyhow::Result<BlobStore> {
    match (
        config.endpoint,
        config.bucket,
        config.access_key_id,
        config.secret_access_key,
    ) {
        (Some(endpoint), Some(bucket), Some(access_key), Some(secret_key)) => {
            let store = AmazonS3Builder::new()
                .with_endpoint(endpoint)
                .with_region(config.region.unwrap_or_else(|| "us-east-1".to_string()))
                .with_bucket_name(bucket)
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key)
                .with_allow_http(config.allow_http)
                .with_virtual_hosted_style_request(false)
                .build()
                .context("build S3-compatible object store")?;
            Ok(BlobStore::new(store))
        }
        (None, None, None, None) => durable_object_store_required(mode),
        _ => bail!(
            "object store config requires endpoint, bucket, access key id, and secret access key"
        ),
    }
}

fn durable_object_store_required(mode: &Mode) -> anyhow::Result<BlobStore> {
    let mode_name = match mode {
        Mode::Once { .. } => "once mode",
        Mode::Run => "run mode",
    };
    bail!(
        "{mode_name} requires durable object store config: set AU_KPIS_OBJECT_STORE__ENDPOINT, AU_KPIS_OBJECT_STORE__BUCKET, AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID, and AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY"
    )
}

fn pipeline_options(shutdown_grace: Duration) -> PipelineOptions {
    PipelineOptions {
        shutdown_grace,
        ..PipelineOptions::default()
    }
}

fn resolve_mode(cli: &Cli) -> anyhow::Result<Mode> {
    match (cli.once, cli.command) {
        (true, None) => {
            let source = cli
                .source
                .clone()
                .context("`--once` requires `--source <id>`")?;
            let dataflow = cli
                .dataflow
                .clone()
                .context("`--once` requires `--dataflow <id>`")?;
            Ok(Mode::Once { source, dataflow })
        }
        (false, Some(Command::Run)) => {
            if cli.source.is_some() || cli.dataflow.is_some() {
                bail!("`run` does not accept `--source` or `--dataflow`");
            }
            Ok(Mode::Run)
        }
        (false, None) => bail!("choose either `--once --source <id> --dataflow <id>` or `run`"),
        (true, Some(Command::Run)) => bail!("`--once` cannot be combined with `run`"),
    }
}

fn validate_once_target(source: &str, dataflow: &str) -> anyhow::Result<()> {
    validate_supported_source(source)?;
    match source {
        "abs" if dataflow == ABS_CPI_DATAFLOW_SLUG => Ok(()),
        "apra" if dataflow == APRA_QUARTERLY_DATAFLOW_SLUG => Ok(()),
        "rba" if dataflow == RBA_STAT_TABLES_DATAFLOW_SLUG => Ok(()),
        "treasury" if dataflow == TREASURY_BUDGET_DATAFLOW_SLUG => Ok(()),
        "abs" => bail!(
            "unsupported dataflow `{dataflow}` for source `abs`; supported dataflow: {ABS_CPI_DATAFLOW_SLUG}"
        ),
        "apra" => bail!(
            "unsupported dataflow `{dataflow}` for source `apra`; supported dataflow: {APRA_QUARTERLY_DATAFLOW_SLUG}"
        ),
        "rba" => bail!(
            "unsupported dataflow `{dataflow}` for source `rba`; supported dataflow: {RBA_STAT_TABLES_DATAFLOW_SLUG}"
        ),
        "treasury" => bail!(
            "unsupported dataflow `{dataflow}` for source `treasury`; supported dataflow: {TREASURY_BUDGET_DATAFLOW_SLUG}"
        ),
        _ => unreachable!("source was validated above"),
    }
}

fn once_run_request(source: &str, dataflow: &str) -> anyhow::Result<RunRequest> {
    validate_once_target(source, dataflow)?;
    let dataflow_id = match source {
        "abs" => ABS_CPI_DATAFLOW_ID,
        "apra" => APRA_QUARTERLY_DATAFLOW_ID,
        "rba" => RBA_STAT_TABLES_DATAFLOW_ID,
        "treasury" => TREASURY_BUDGET_DATAFLOW_ID,
        _ => unreachable!("source was validated above"),
    };
    Ok(RunRequest {
        source_id: SourceId::new(source)
            .map_err(|err| au_kpis_adapter::AdapterError::Validation(err.to_string()))?,
        dataflow_id: Some(
            DataflowId::new(dataflow_id)
                .map_err(|err| au_kpis_adapter::AdapterError::Validation(err.to_string()))?,
        ),
        trace_parent: None,
    })
}

fn job_run_request(kind: &JobKind, trace_parent: Option<&str>) -> anyhow::Result<RunRequest> {
    match kind {
        JobKind::Discover { source_id } => {
            validate_supported_source(source_id.as_str())?;
            Ok(RunRequest {
                source_id: source_id.clone(),
                dataflow_id: None,
                trace_parent: trace_parent.map(str::to_owned),
            })
        }
        JobKind::Backfill {
            source_id,
            dataflow_id,
        } => {
            validate_supported_source(source_id.as_str())?;
            if let Some(dataflow_id) = dataflow_id {
                validate_supported_dataflow_id(source_id.as_str(), dataflow_id.as_str())?;
            }
            Ok(RunRequest {
                source_id: source_id.clone(),
                dataflow_id: dataflow_id.clone(),
                trace_parent: trace_parent.map(str::to_owned),
            })
        }
        other => bail!("unexpected job kind on discovery queue: {other:?}"),
    }
}

fn validate_supported_source(source: &str) -> anyhow::Result<()> {
    if !matches!(source, "abs" | "apra" | "rba" | "treasury") {
        bail!("unsupported source `{source}`; supported sources: abs, apra, rba, treasury");
    }
    Ok(())
}

fn validate_supported_dataflow_id(source: &str, dataflow_id: &str) -> anyhow::Result<()> {
    if source == "abs" && dataflow_id == ABS_CPI_DATAFLOW_ID {
        return Ok(());
    }
    if source == "apra" && dataflow_id == APRA_QUARTERLY_DATAFLOW_ID {
        return Ok(());
    }
    if source == "rba" && dataflow_id == RBA_STAT_TABLES_DATAFLOW_ID {
        return Ok(());
    }
    if source == "treasury" && dataflow_id == TREASURY_BUDGET_DATAFLOW_ID {
        return Ok(());
    }
    bail!(
        "unsupported dataflow `{dataflow_id}` for source `{source}`; supported dataflows: {ABS_CPI_DATAFLOW_ID}, {APRA_QUARTERLY_DATAFLOW_ID}, {RBA_STAT_TABLES_DATAFLOW_ID}, {TREASURY_BUDGET_DATAFLOW_ID}"
    );
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn init_or_disabled(config: &au_kpis_config::TelemetryConfig) -> anyhow::Result<Telemetry> {
    match init_telemetry(config) {
        Ok(telemetry) => Ok(telemetry),
        Err(err) if err.to_string() == "global telemetry subscriber already installed" => {
            Ok(Telemetry::disabled())
        }
        Err(err) => Err(err).context("initialize telemetry"),
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn shutdown_signal(token: CancellationToken) -> anyhow::Result<()> {
    let ctrl_c = async { signal::ctrl_c().await.context("install Ctrl-C handler") };

    #[cfg(unix)]
    let terminate = async {
        let mut stream = signal::unix::signal(signal::unix::SignalKind::terminate())
            .context("install SIGTERM handler")?;
        stream.recv().await.context("SIGTERM stream closed")
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<anyhow::Result<()>>();

    tokio::select! {
        result = ctrl_c => result?,
        result = terminate => result?,
    }

    token.cancel();
    Ok(())
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn write_startup_notify(listener: &TcpListener) -> anyhow::Result<()> {
    write_startup_notify_path(listener, env::var_os("AU_KPIS_STARTUP_NOTIFY_FILE")).await
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn write_startup_notify_path(
    listener: &TcpListener,
    path: Option<OsString>,
) -> anyhow::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };

    let addr = listener
        .local_addr()
        .context("read bound listener address")?;
    tokio::fs::write(path, addr.to_string())
        .await
        .context("persist bound listener address")?;
    Ok(())
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn default_worker_id() -> String {
    format!("au-kpis-ingestion-{}", uuid::Uuid::new_v4())
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    };

    use au_kpis_domain::ids::DataflowId;
    use au_kpis_queue::JobKind;
    use tokio::time::sleep;

    use super::*;

    fn cli(args: &[&str]) -> Cli {
        Cli::parse_from(std::iter::once("au-kpis-ingestion").chain(args.iter().copied()))
    }

    #[test]
    fn once_mode_requires_source_and_dataflow() {
        let err = resolve_mode(&cli(&["--once"]))
            .expect_err("once without source/dataflow should fail")
            .to_string();
        assert!(err.contains("--source"));
    }

    #[test]
    fn once_mode_resolves_source_and_dataflow() {
        assert_eq!(
            resolve_mode(&cli(&["--once", "--source", "abs", "--dataflow", "cpi"]))
                .expect("resolve once mode"),
            Mode::Once {
                source: "abs".to_string(),
                dataflow: "cpi".to_string()
            }
        );
    }

    #[test]
    fn run_mode_rejects_source_filter() {
        let err = resolve_mode(&cli(&["--source", "abs", "run"]))
            .expect_err("run source filter should fail")
            .to_string();
        assert!(err.contains("does not accept"));
    }

    #[test]
    fn metrics_render_prometheus_counters() {
        let metrics = WorkerMetrics::default();
        metrics.worker_loops_total.store(7, Ordering::Relaxed);

        let body = metrics.render_prometheus();

        assert!(body.contains("# TYPE au_kpis_ingestion_worker_loops_total counter"));
        assert!(body.contains("au_kpis_ingestion_worker_loops_total 7"));
    }

    #[test]
    fn once_mode_requires_durable_object_store_config() {
        let err = build_blob_store(
            &Mode::Once {
                source: "abs".to_string(),
                dataflow: "cpi".to_string(),
            },
            ObjectStoreConfig::default(),
        )
        .expect_err("once mode should reject missing durable object store config")
        .to_string();

        assert!(err.contains("durable object store config"));
    }

    #[test]
    fn configured_shutdown_grace_propagates_to_pipeline_options() {
        let options = pipeline_options(Duration::from_secs(7));

        assert_eq!(options.shutdown_grace, Duration::from_secs(7));
    }

    #[test]
    fn unsupported_dataflow_reports_specific_error() {
        let err = validate_once_target("abs", "wpi")
            .expect_err("unsupported dataflow should fail")
            .to_string();
        assert!(err.contains("unsupported dataflow"));
        assert!(err.contains("cpi"));
    }

    #[test]
    fn unsupported_source_reports_specific_error() {
        let err = validate_once_target("aemo", "cpi")
            .expect_err("unsupported source should fail")
            .to_string();
        assert!(err.contains("unsupported source"));
        assert!(err.contains("abs, apra, rba, treasury"));
    }

    #[test]
    fn rba_once_mode_resolves_statistical_tables_dataflow() {
        let request = once_run_request("rba", "statistical-tables")
            .expect("RBA statistical tables are supported");

        assert_eq!(request.source_id.as_str(), "rba");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("rba.statistical_tables").unwrap())
        );
    }

    #[test]
    fn apra_once_mode_resolves_quarterly_statistics_dataflow() {
        let request = once_run_request("apra", "quarterly-statistics")
            .expect("APRA quarterly statistics are supported");

        assert_eq!(request.source_id.as_str(), "apra");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("apra.quarterly_statistics").unwrap())
        );
    }

    #[test]
    fn treasury_once_mode_resolves_budget_papers_dataflow() {
        let request = once_run_request("treasury", "budget-papers")
            .expect("Treasury budget papers are supported");

        assert_eq!(request.source_id.as_str(), "treasury");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("treasury.budget_papers").unwrap())
        );
    }

    #[test]
    fn backfill_jobs_preserve_optional_dataflow_scope_and_trace_parent() {
        let trace_parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string();
        let request = job_run_request(
            &JobKind::Backfill {
                source_id: SourceId::new("abs").unwrap(),
                dataflow_id: Some(DataflowId::new("abs.cpi").unwrap()),
            },
            Some(trace_parent.as_str()),
        )
        .expect("build backfill request");

        assert_eq!(request.source_id.as_str(), "abs");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("abs.cpi").unwrap())
        );
        assert_eq!(request.trace_parent.as_deref(), Some(trace_parent.as_str()));
    }

    #[tokio::test]
    async fn lease_renewer_keeps_latest_handle_while_work_runs() {
        let renewals = Arc::new(AtomicUsize::new(0));
        let lease_client = TestLeaseClient {
            renewals: Arc::clone(&renewals),
        };

        let (lease, result) = run_with_lease_renewal(
            &lease_client,
            TestLease { version: 1 },
            Duration::from_millis(10),
            async {
                sleep(Duration::from_millis(35)).await;
                7_usize
            },
        )
        .await
        .expect("run with lease renewal");

        assert_eq!(result, 7);
        assert!(lease.version > 1, "lease version should be renewed");
        assert!(
            renewals.load(AtomicOrdering::Relaxed) > 0,
            "lease renewer should call renew at least once"
        );
    }

    #[tokio::test]
    async fn worker_loop_drains_in_flight_step_after_shutdown() {
        let shutdown = CancellationToken::new();
        let metrics = Arc::new(WorkerMetrics::default());
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));

        let worker = tokio::spawn({
            let shutdown = shutdown.clone();
            let metrics = Arc::clone(&metrics);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let completed = Arc::clone(&completed);
            let calls = Arc::clone(&calls);
            async move {
                run_worker_loop(shutdown, Duration::from_millis(5), metrics, move || {
                    let started = Arc::clone(&started);
                    let release = Arc::clone(&release);
                    let completed = Arc::clone(&completed);
                    let calls = Arc::clone(&calls);
                    async move {
                        calls.fetch_add(1, AtomicOrdering::Relaxed);
                        started.notify_one();
                        release.notified().await;
                        completed.fetch_add(1, AtomicOrdering::Relaxed);
                        Ok(WorkerStep::Processed)
                    }
                })
                .await
            }
        });

        started.notified().await;
        shutdown.cancel();
        sleep(Duration::from_millis(20)).await;

        assert!(
            !worker.is_finished(),
            "worker loop should not drop in-flight work on shutdown"
        );
        assert_eq!(
            completed.load(AtomicOrdering::Relaxed),
            0,
            "test work should still be waiting for its drain handoff"
        );

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("worker loop should exit after in-flight work drains")
            .expect("worker task should not panic")
            .expect("worker loop should succeed");

        assert_eq!(completed.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(
            calls.load(AtomicOrdering::Relaxed),
            1,
            "worker loop should not admit a second job after shutdown"
        );
    }

    #[test]
    fn load_error_classification_matches_retry_policy() {
        assert_eq!(
            load_error_class(&au_kpis_loader::LoadError::Validation("bad row".into())),
            ErrorClass::Validation
        );
        assert_eq!(
            load_error_class(&au_kpis_loader::LoadError::Json(
                serde_json::from_str::<serde_json::Value>("{").expect_err("invalid JSON")
            )),
            ErrorClass::Permanent
        );
        assert_eq!(
            load_error_class(&au_kpis_loader::LoadError::Db(sqlx::Error::PoolClosed)),
            ErrorClass::Transient
        );
    }

    #[test]
    fn ingestion_error_classification_preserves_permanent_invariants() {
        assert_eq!(
            ingestion_error_class(&au_kpis_ingestion_core::IngestionError::Config(
                "bad config".into()
            )),
            ErrorClass::Permanent
        );
        assert_eq!(
            ingestion_error_class(&au_kpis_ingestion_core::IngestionError::Cancelled),
            ErrorClass::Transient
        );
        assert_eq!(
            ingestion_error_class(&au_kpis_ingestion_core::IngestionError::DataflowMismatch {
                expected: "abs.cpi".into(),
                actual: "abs.wpi".into(),
            },),
            ErrorClass::Permanent
        );
    }

    #[test]
    fn invalid_job_nacks_are_permanent() {
        let nack = invalid_job_nack(anyhow::anyhow!("unsupported source `rba`"));
        let debug = format!("{nack:?}");

        assert!(debug.contains("Permanent"));
        assert!(debug.contains("unsupported source `rba`"));
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestLease {
        version: usize,
    }

    #[derive(Debug)]
    struct TestLeaseClient {
        renewals: Arc<AtomicUsize>,
    }

    impl LeaseClient<TestLease> for TestLeaseClient {
        async fn renew(&self, lease: &TestLease) -> anyhow::Result<TestLease> {
            self.renewals.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(TestLease {
                version: lease.version + 1,
            })
        }
    }
}
