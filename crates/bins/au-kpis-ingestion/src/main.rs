//! Ingestion worker binary.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::{
    env,
    ffi::OsString,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, bail};
use au_kpis_adapter::{AdapterHttpClient, Adapters, DiscoveryCtx, ParseCtx};
use au_kpis_adapter_abs::AbsAdapter;
use au_kpis_config::load;
use au_kpis_db::{connect as connect_db, migrate};
use au_kpis_domain::ids::SourceId;
use au_kpis_error::{Classify, ErrorClass};
use au_kpis_ingestion_core::{IngestionPipeline, PipelineContexts, PipelineOptions, fetch_ctx};
use au_kpis_queue::{ApalisPgQueue, JobKind, Nack, Queue, QueueStage, WorkerId};
use au_kpis_storage::BlobStore;
use au_kpis_telemetry::{Telemetry, init as init_telemetry};
use axum::{Router, http::header, response::IntoResponse, routing::get};
use clap::{Parser, Subcommand};
use object_store::{aws::AmazonS3Builder, memory::InMemory};
use tokio::{net::TcpListener, signal};
use tokio_util::sync::CancellationToken;

const ABS_CPI_DATAFLOW: &str = "cpi";
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

#[tokio::main(flavor = "multi_thread")]
#[cfg_attr(coverage_nightly, coverage(off))]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mode = resolve_mode(&cli)?;
    if let Mode::Once { source, dataflow } = &mode {
        validate_once_target(source, dataflow)?;
    }
    let config = Arc::new(load(None).context("load config")?);
    let _telemetry = init_or_disabled(&config.telemetry)?;
    let db = connect_db(&config.database)
        .await
        .context("connect postgres database")?;
    migrate(&db).await.context("apply database migrations")?;

    let shutdown = CancellationToken::new();
    let metrics = Arc::new(WorkerMetrics::default());
    let listener = TcpListener::bind(&config.http.bind)
        .await
        .with_context(|| format!("bind metrics listener on {}", config.http.bind))?;
    write_startup_notify(&listener)
        .await
        .context("write startup notification")?;
    let metrics_server = tokio::spawn(serve_metrics(
        listener,
        Arc::clone(&metrics),
        shutdown.clone(),
    ));
    let shutdown_listener = shutdown_signal(shutdown.clone());
    tokio::pin!(shutdown_listener);

    let runtime = Runtime {
        adapters: build_adapters()?,
        db,
        blob_store: build_blob_store()?,
        metrics,
        shutdown: shutdown.clone(),
        poll_interval: Duration::from_millis(cli.poll_interval_ms),
        worker_id: cli.worker_id.unwrap_or_else(default_worker_id),
    };

    let drain_window = Duration::from_secs(config.http.shutdown_grace_period_secs);
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
    shutdown: CancellationToken,
    poll_interval: Duration,
    worker_id: String,
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn run_mode(mode: Mode, runtime: Runtime) -> anyhow::Result<()> {
    match mode {
        Mode::Once { source, dataflow } => {
            validate_once_target(&source, &dataflow)?;
            let stats = run_source_once(&runtime, &source).await?;
            runtime
                .metrics
                .once_runs_total
                .fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                source,
                dataflow,
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
    source: &str,
) -> Result<au_kpis_ingestion_core::PipelineRunStats, au_kpis_ingestion_core::IngestionError> {
    let adapter = runtime.adapters.get(source)?;
    let started_at = chrono::Utc::now();
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let contexts = PipelineContexts {
        discovery: DiscoveryCtx::new(http.clone(), started_at),
        fetch: fetch_ctx(
            http.clone(),
            runtime.blob_store.clone(),
            started_at,
            runtime.db.clone(),
        ),
        parse: ParseCtx::new(http, runtime.blob_store.clone(), started_at),
    };
    IngestionPipeline::new(runtime.adapters.clone(), runtime.db.clone())
        .with_options(PipelineOptions::default())
        .run_source(
            SourceId::new(source)
                .map_err(|err| au_kpis_adapter::AdapterError::Validation(err.to_string()))?,
            contexts,
            runtime.shutdown.clone(),
        )
        .await
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn run_worker(runtime: Runtime) -> anyhow::Result<()> {
    let queue = ApalisPgQueue::new(runtime.db.clone());
    let worker_id = WorkerId::new(runtime.worker_id.clone()).context("build queue worker id")?;

    loop {
        runtime
            .metrics
            .worker_loops_total
            .fetch_add(1, Ordering::Relaxed);
        tokio::select! {
            () = runtime.shutdown.cancelled() => return Ok(()),
            result = process_one_job(&runtime, &queue, worker_id.clone()) => {
                match result? {
                    WorkerStep::Processed => {}
                    WorkerStep::Idle => {
                        tokio::select! {
                            () = runtime.shutdown.cancelled() => return Ok(()),
                            () = tokio::time::sleep(runtime.poll_interval) => {}
                        }
                    }
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

    let result = match job.job().kind() {
        JobKind::Discover { source_id } | JobKind::Backfill { source_id, .. } => {
            run_source_once(runtime, source_id.as_str()).await
        }
        other => {
            bail!("unexpected job kind on discovery queue: {other:?}");
        }
    };

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
    Ok(builder.build())
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn build_blob_store() -> anyhow::Result<BlobStore> {
    let endpoint = env::var("AU_KPIS_OBJECT_STORE__ENDPOINT").ok();
    let bucket = env::var("AU_KPIS_OBJECT_STORE__BUCKET").ok();
    let access_key = env::var("AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID").ok();
    let secret_key = env::var("AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY").ok();

    match (endpoint, bucket, access_key, secret_key) {
        (Some(endpoint), Some(bucket), Some(access_key), Some(secret_key)) => {
            let region = env::var("AU_KPIS_OBJECT_STORE__REGION")
                .unwrap_or_else(|_| "us-east-1".to_string());
            let allow_http = env::var("AU_KPIS_OBJECT_STORE__ALLOW_HTTP")
                .ok()
                .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes"));
            let store = AmazonS3Builder::new()
                .with_endpoint(endpoint)
                .with_region(region)
                .with_bucket_name(bucket)
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key)
                .with_allow_http(allow_http)
                .with_virtual_hosted_style_request(false)
                .build()
                .context("build S3-compatible object store")?;
            Ok(BlobStore::new(store))
        }
        (None, None, None, None) => Ok(BlobStore::new(InMemory::new())),
        _ => bail!(
            "object store config requires endpoint, bucket, access key id, and secret access key"
        ),
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
    if source != "abs" {
        bail!("unsupported source `{source}`; supported source: abs");
    }
    if dataflow != ABS_CPI_DATAFLOW {
        bail!("unsupported dataflow `{dataflow}` for source `abs`; supported dataflow: cpi");
    }
    Ok(())
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
    fn unsupported_dataflow_reports_specific_error() {
        let err = validate_once_target("abs", "wpi")
            .expect_err("unsupported dataflow should fail")
            .to_string();
        assert!(err.contains("unsupported dataflow"));
        assert!(err.contains("cpi"));
    }

    #[test]
    fn unsupported_source_reports_specific_error() {
        let err = validate_once_target("rba", "cpi")
            .expect_err("unsupported source should fail")
            .to_string();
        assert!(err.contains("unsupported source"));
        assert!(err.contains("abs"));
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
}
