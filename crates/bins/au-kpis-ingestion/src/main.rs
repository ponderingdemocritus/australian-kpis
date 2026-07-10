//! Ingestion worker binary.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    future::Future,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, bail};
use au_kpis_adapter::{AdapterError, AdapterHttpClient, Adapters, DiscoveryCtx, ParseCtx};
use au_kpis_adapter_abs::AbsAdapter;
use au_kpis_adapter_aemo::AemoAdapter;
use au_kpis_adapter_ai_readiness::AiReadinessAdapter;
use au_kpis_adapter_apra::ApraAdapter;
use au_kpis_adapter_asx::AsxAdapter;
use au_kpis_adapter_nhsac::NhsacAdapter;
use au_kpis_adapter_pc::PcAdapter;
use au_kpis_adapter_rba::RbaAdapter;
use au_kpis_adapter_state_budgets::StateBudgetsAdapter;
use au_kpis_adapter_state_capital::StateCapitalAdapter;
use au_kpis_adapter_state_planning::StatePlanningAdapter;
use au_kpis_adapter_treasury::TreasuryAdapter;
use au_kpis_adapter_worldbank::WorldbankAdapter;
use au_kpis_config::load_ingestion;
use au_kpis_db::{connect as connect_db, migrate};
use au_kpis_domain::{
    Dataflow, Frequency, License, Source,
    ids::{DataflowId, SourceId},
};
#[cfg(test)]
use au_kpis_error::{Classify, ErrorClass};
use au_kpis_ingestion_core::{IngestionPipeline, PipelineContexts, PipelineOptions, fetch_ctx};
use au_kpis_pdf_client::PdfClient;
#[cfg(test)]
use au_kpis_queue::JobKind;
use au_kpis_queue::{ApalisPgQueue, LeasedJob, Nack, Queue, QueueStage, WorkerId};
use au_kpis_scorecard::{CoverageStatus as ScorecardCoverageStatus, load_aps_v1_config};
use au_kpis_storage::BlobStore;
use au_kpis_telemetry::{Telemetry, init as init_telemetry};
use axum::{Router, http::header, response::IntoResponse, routing::get};
use clap::{Parser, Subcommand};
use object_store::aws::AmazonS3Builder;
use tokio::{net::TcpListener, signal, time::Instant};
use tokio_util::sync::CancellationToken;

mod coverage_report;
mod durable_worker;

const ABS_CPI_DATAFLOW_SLUG: &str = "cpi";
const ABS_CPI_DATAFLOW_ID: &str = "abs.cpi";
const ABS_BUILDING_APPROVALS_DATAFLOW_SLUG: &str = "building-approvals";
const ABS_BUILDING_APPROVALS_DATAFLOW_ID: &str = "abs.building_approvals";
const ABS_BUILDING_ACTIVITY_DATAFLOW_SLUG: &str = "building-activity";
const ABS_BUILDING_ACTIVITY_DATAFLOW_ID: &str = "abs.building_activity";
const ABS_DWELLING_COMPLETION_TIMES_DATAFLOW_SLUG: &str = "dwelling-completion-times";
const ABS_DWELLING_COMPLETION_TIMES_DATAFLOW_ID: &str = "abs.dwelling_completion_times";
const APRA_QUARTERLY_DATAFLOW_SLUG: &str = "quarterly-statistics";
const APRA_QUARTERLY_DATAFLOW_ID: &str = "apra.quarterly_statistics";
const APRA_SUPER_ASSET_ALLOCATION_DATAFLOW_SLUG: &str = "super-asset-allocation";
const APRA_SUPER_ASSET_ALLOCATION_DATAFLOW_ID: &str = "apra.super_asset_allocation";
const AEMO_DISPATCH_DATAFLOW_SLUG: &str = "dispatch";
const AEMO_DISPATCH_DATAFLOW_ID: &str = "aemo.dispatch";
const AEMO_GENERATION_MIX_DATAFLOW_SLUG: &str = "generation-mix";
const AEMO_GENERATION_MIX_DATAFLOW_ID: &str = "aemo.generation_mix";
const AEMO_DISPATCHABILITY_CAPACITY_DATAFLOW_SLUG: &str = "dispatchability-capacity";
const AEMO_DISPATCHABILITY_CAPACITY_DATAFLOW_ID: &str = "aemo.dispatchability_capacity";
const AI_READINESS_OXFORD_GARI_DATAFLOW_SLUG: &str = "oxford-gari";
const AI_READINESS_OXFORD_GARI_DATAFLOW_ID: &str = "oxford.gari";
const AI_READINESS_NAIC_ADOPTION_DATAFLOW_SLUG: &str = "naic-ai-adoption-tracker";
const AI_READINESS_NAIC_ADOPTION_DATAFLOW_ID: &str = "naic.ai_adoption_tracker";
const AI_READINESS_ABS_AI_RD_DATAFLOW_SLUG: &str = "abs-ai-rd";
const AI_READINESS_ABS_AI_RD_DATAFLOW_ID: &str = "abs.ai_rd";
const AI_READINESS_HOME_AFFAIRS_TALENT_DATAFLOW_SLUG: &str =
    "home-affairs-skillselect-talent-proxy";
const AI_READINESS_HOME_AFFAIRS_TALENT_DATAFLOW_ID: &str = "home_affairs.skillselect_talent_proxy";
const ASX_MARKET_STATISTICS_DATAFLOW_SLUG: &str = "market-statistics";
const ASX_MARKET_STATISTICS_DATAFLOW_ID: &str = "asx.market_statistics";
const ASX_ANNOUNCEMENTS_DATAFLOW_SLUG: &str = "announcements";
const ASX_ANNOUNCEMENTS_DATAFLOW_ID: &str = "asx.announcements";
const ASX_EOD_DATAFLOW_SLUG: &str = "eod";
const ASX_EOD_DATAFLOW_ID: &str = "asx.eod";
const NHSAC_HOUSING_ACCORD_DATAFLOW_SLUG: &str = "housing-accord-progress";
const NHSAC_HOUSING_ACCORD_DATAFLOW_ID: &str = "nhsac.housing_accord_progress";
const PC_PRODUCTIVITY_BULLETIN_DATAFLOW_SLUG: &str = "productivity-bulletin";
const PC_PRODUCTIVITY_BULLETIN_DATAFLOW_ID: &str = "pc.productivity_bulletin";
const WORLDBANK_BREADY_DATAFLOW_SLUG: &str = "bready";
const WORLDBANK_BREADY_DATAFLOW_ID: &str = "worldbank.bready";
const RBA_STAT_TABLES_DATAFLOW_SLUG: &str = "statistical-tables";
const RBA_STAT_TABLES_DATAFLOW_ID: &str = "rba.statistical_tables";
const STATE_BUDGETS_NSW_DATAFLOW_SLUG: &str = "nsw-budget";
const STATE_BUDGETS_NSW_DATAFLOW_ID: &str = "state_budgets.nsw_budget";
const STATE_BUDGETS_VIC_DATAFLOW_SLUG: &str = "vic-budget";
const STATE_BUDGETS_VIC_DATAFLOW_ID: &str = "state_budgets.vic_budget";
const STATE_BUDGETS_QLD_DATAFLOW_SLUG: &str = "qld-budget";
const STATE_BUDGETS_QLD_DATAFLOW_ID: &str = "state_budgets.qld_budget";
const STATE_CAPITAL_VIC_MAJOR_PROJECTS_DATAFLOW_SLUG: &str = "vic-major-projects";
const STATE_CAPITAL_VIC_MAJOR_PROJECTS_DATAFLOW_ID: &str = "state_capital.vic_major_projects";
const STATE_CAPITAL_BUDGET_CAPITAL_PAPERS_DATAFLOW_SLUG: &str = "budget-capital-papers";
const STATE_CAPITAL_BUDGET_CAPITAL_PAPERS_DATAFLOW_ID: &str = "state_capital.budget_capital_papers";
const STATE_PLANNING_NSW_DA_PROCESSING_DATAFLOW_SLUG: &str = "nsw-da-processing";
const STATE_PLANNING_NSW_DA_PROCESSING_DATAFLOW_ID: &str = "state_planning.nsw_da_processing";
const STATE_PLANNING_VIC_PERMIT_ACTIVITY_DATAFLOW_SLUG: &str = "vic-permit-activity";
const STATE_PLANNING_VIC_PERMIT_ACTIVITY_DATAFLOW_ID: &str = "state_planning.vic_permit_activity";
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

    /// Allow a one-shot run to complete when discovery or load produces zero rows.
    #[arg(long)]
    allow_zero_jobs: bool,

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
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
enum Command {
    /// Start the long-running worker loop.
    Run,

    /// Write a dataflow coverage report from catalog and load audit tables.
    CoverageReport {
        /// JSON report path. If omitted, JSON is written to stdout.
        #[arg(long)]
        output: Option<PathBuf>,

        /// Markdown report path.
        #[arg(long)]
        markdown: Option<PathBuf>,

        /// Exit non-zero when any dataflow is not fully loaded.
        #[arg(long)]
        fail_on_gaps: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    Once {
        source: String,
        dataflow: String,
        allow_zero_jobs: bool,
    },
    Run,
    CoverageReport {
        output: Option<PathBuf>,
        markdown: Option<PathBuf>,
        fail_on_gaps: bool,
    },
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
    staging_recoveries_total: AtomicU64,
    once_runs_total: AtomicU64,
    schema_hash_drifts_total: Mutex<BTreeMap<SchemaHashDriftMetricKey, u64>>,
}

impl WorkerMetrics {
    fn render_prometheus(&self) -> String {
        let mut body = format!(
            "# HELP au_kpis_ingestion_worker_loops_total Worker polling loop iterations.\n\
             # TYPE au_kpis_ingestion_worker_loops_total counter\n\
             au_kpis_ingestion_worker_loops_total {}\n\
             # HELP au_kpis_ingestion_jobs_completed_total Queue jobs completed by the worker.\n\
             # TYPE au_kpis_ingestion_jobs_completed_total counter\n\
             au_kpis_ingestion_jobs_completed_total {}\n\
             # HELP au_kpis_ingestion_jobs_failed_total Queue jobs failed by the worker.\n\
             # TYPE au_kpis_ingestion_jobs_failed_total counter\n\
             au_kpis_ingestion_jobs_failed_total {}\n\
             # HELP au_kpis_ingestion_staging_recoveries_total Generations reset after unlogged staging loss.\n\
             # TYPE au_kpis_ingestion_staging_recoveries_total counter\n\
             au_kpis_ingestion_staging_recoveries_total {}\n\
             # HELP au_kpis_ingestion_once_runs_total One-shot ingestion runs completed.\n\
             # TYPE au_kpis_ingestion_once_runs_total counter\n\
             au_kpis_ingestion_once_runs_total {}\n",
            self.worker_loops_total.load(Ordering::Relaxed),
            self.jobs_completed_total.load(Ordering::Relaxed),
            self.jobs_failed_total.load(Ordering::Relaxed),
            self.staging_recoveries_total.load(Ordering::Relaxed),
            self.once_runs_total.load(Ordering::Relaxed)
        );
        body.push_str(
            "# HELP au_kpis_schema_hash_drifts_total Schema hash drift events detected by source parsers.\n\
             # TYPE au_kpis_schema_hash_drifts_total counter\n",
        );
        for (key, count) in self
            .schema_hash_drifts_total
            .lock()
            .expect("schema drift metrics mutex should not be poisoned")
            .iter()
        {
            body.push_str(&format!(
                "au_kpis_schema_hash_drifts_total{{source=\"{}\",dataflow=\"{}\"}} {}\n",
                prometheus_label_value(&key.source),
                prometheus_label_value(&key.dataflow),
                count
            ));
        }
        body
    }

    fn record_ingestion_error(&self, err: &au_kpis_ingestion_core::IngestionError) {
        if let au_kpis_ingestion_core::IngestionError::Adapter(err) = err {
            self.record_adapter_error(err);
        }
    }

    fn record_adapter_error(&self, err: &AdapterError) {
        if let AdapterError::SchemaHashDrift(drift) = err {
            let key = SchemaHashDriftMetricKey {
                source: drift.source_id.as_str().to_string(),
                dataflow: drift.dataflow_id.as_str().to_string(),
            };
            let mut counts = self
                .schema_hash_drifts_total
                .lock()
                .expect("schema drift metrics mutex should not be poisoned");
            *counts.entry(key).or_insert(0) += 1;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SchemaHashDriftMetricKey {
    source: String,
    dataflow: String,
}

fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('"', r#"\""#)
        .replace('\n', r"\n")
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
    if let Mode::Once {
        source, dataflow, ..
    } = &mode
    {
        validate_once_target(source, dataflow)?;
    }
    let config = Arc::new(load_ingestion(None).context("load config")?);
    let _telemetry = init_or_disabled(&config.telemetry)?;
    let db = connect_db(&config.database)
        .await
        .context("connect postgres database")?;
    migrate(&db).await.context("apply database migrations")?;
    let adapters = build_adapters()?;
    sync_adapter_catalog(&db, &adapters)
        .await
        .context("sync adapter catalog metadata")?;
    if let Mode::CoverageReport {
        output,
        markdown,
        fail_on_gaps,
    } = mode
    {
        write_coverage_report(&db, output.as_ref(), markdown.as_ref(), fail_on_gaps).await?;
        return Ok(());
    }

    let drain_window = Duration::from_secs(config.http.shutdown_grace_period_secs);
    let shutdown = CancellationToken::new();
    let metrics = Arc::new(WorkerMetrics::default());
    let runtime = Runtime {
        adapters,
        db,
        blob_store: build_blob_store(&mode, ObjectStoreConfig::from_env())?,
        metrics,
        pipeline_options: pipeline_options(drain_window)?,
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
        Mode::Once {
            source,
            dataflow,
            allow_zero_jobs,
        } => {
            let request = once_run_request(&source, &dataflow)?;
            let stats = match run_source_once(&runtime, &request, runtime.shutdown.clone()).await {
                Ok(stats) => stats,
                Err(err) => {
                    runtime.metrics.record_ingestion_error(&err);
                    return Err(err.into());
                }
            };
            validate_once_run_stats(&request, &stats, allow_zero_jobs)?;
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
        Mode::CoverageReport { .. } => unreachable!("coverage-report exits before runtime setup"),
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn write_coverage_report(
    pool: &au_kpis_db::PgPool,
    output: Option<&PathBuf>,
    markdown: Option<&PathBuf>,
    fail_on_gaps: bool,
) -> anyhow::Result<()> {
    let report = load_coverage_report(pool).await?;
    let json = serde_json::to_string_pretty(&report).context("serialize coverage report")?;
    if let Some(path) = output {
        tokio::fs::write(path, json)
            .await
            .with_context(|| format!("write coverage report JSON to {}", path.display()))?;
    } else {
        println!("{json}");
    }
    if let Some(path) = markdown {
        tokio::fs::write(path, coverage_report::render_markdown(&report))
            .await
            .with_context(|| format!("write coverage report Markdown to {}", path.display()))?;
    }
    if fail_on_gaps {
        let gap_count = report
            .dataflows
            .iter()
            .filter(|dataflow| dataflow.status != coverage_report::CoverageStatus::Loaded)
            .count();
        if gap_count > 0 {
            bail!("{gap_count} dataflows are not fully loaded; see coverage report");
        }
    }
    Ok(())
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn load_coverage_report(
    pool: &au_kpis_db::PgPool,
) -> anyhow::Result<coverage_report::CoverageReport> {
    let expected_statuses = expected_coverage_statuses_by_dataflow()?;
    let rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            String,
            String,
            i64,
            i64,
            i64,
            i64,
            Option<String>,
        ),
    >(
        r#"
        WITH series_counts AS (
            SELECT dataflow_id, count(*)::BIGINT AS series_count
            FROM series
            GROUP BY dataflow_id
        ),
        latest_success AS (
            SELECT dataflow_id, max(completed_at) AS completed_at
            FROM artifact_loads
            GROUP BY dataflow_id
        ),
        load_counts AS (
            SELECT
                dataflow_id,
                count(*)::BIGINT AS loaded_artifacts,
                coalesce(sum(observations_loaded), 0)::BIGINT AS observations_loaded,
                to_char(
                    max(completed_at) AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                ) AS latest_load
            FROM artifact_loads
            GROUP BY dataflow_id
        ),
        parse_counts AS (
            SELECT
                row_context->>'dataflow_id' AS dataflow_id,
                count(*)::BIGINT AS parse_errors,
                count(DISTINCT artifact_id)::BIGINT AS failed_artifacts
            FROM parse_errors
            LEFT JOIN latest_success ls
                ON ls.dataflow_id = row_context->>'dataflow_id'
            WHERE row_context ? 'dataflow_id'
                AND NOT EXISTS (
                    SELECT 1
                    FROM artifact_loads al
                    WHERE al.dataflow_id = row_context->>'dataflow_id'
                        AND al.artifact_id = parse_errors.artifact_id
                        AND al.completed_at >= parse_errors.created_at
                )
                AND (ls.completed_at IS NULL OR parse_errors.created_at > ls.completed_at)
            GROUP BY row_context->>'dataflow_id'
        )
        SELECT
            d.source_id,
            d.id AS dataflow_id,
            d.name,
            d.source_url,
            coalesce(sc.series_count, 0)::BIGINT AS series_count,
            (coalesce(lc.loaded_artifacts, 0) + coalesce(pc.failed_artifacts, 0))::BIGINT
                AS artifact_count,
            coalesce(lc.observations_loaded, 0)::BIGINT AS observations_loaded,
            coalesce(pc.parse_errors, 0)::BIGINT AS parse_errors,
            lc.latest_load
        FROM dataflows d
        LEFT JOIN series_counts sc ON sc.dataflow_id = d.id
        LEFT JOIN load_counts lc ON lc.dataflow_id = d.id
        LEFT JOIN parse_counts pc ON pc.dataflow_id = d.id
        ORDER BY d.source_id, d.id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("load dataflow coverage counters")?
    .into_iter()
    .map(
        |(
            source_id,
            dataflow_id,
            name,
            source_url,
            series_count,
            artifact_count,
            observations_loaded,
            parse_errors,
            latest_load,
        )| {
            let expected_status = expected_statuses.get(&dataflow_id).copied();
            coverage_report::RawCoverageRow {
                source_id,
                dataflow_id,
                name,
                source_url,
                series_count,
                artifact_count,
                observations_loaded,
                parse_errors,
                latest_load,
                expected_status,
            }
        },
    )
    .collect();

    Ok(coverage_report::build_report(rows))
}

#[cfg_attr(coverage_nightly, coverage(off))]
fn expected_coverage_statuses_by_dataflow()
-> anyhow::Result<BTreeMap<String, coverage_report::CoverageStatus>> {
    let config = load_aps_v1_config().context("load APS scorecard config for coverage report")?;
    let mut statuses = BTreeMap::new();
    for indicator in config.indicators {
        let Some(status) = coverage_report_status_for_scorecard(indicator.coverage_status) else {
            continue;
        };
        insert_expected_coverage_status(&mut statuses, indicator.source_dataflow_id, status);
    }
    for (dataflow_id, status) in catalog_coverage_statuses() {
        insert_expected_coverage_status(&mut statuses, dataflow_id, status);
    }
    Ok(statuses)
}

fn catalog_coverage_statuses() -> [(&'static str, coverage_report::CoverageStatus); 3] {
    [
        (
            ASX_ANNOUNCEMENTS_DATAFLOW_ID,
            coverage_report::CoverageStatus::CoverageGap,
        ),
        (
            ASX_EOD_DATAFLOW_ID,
            coverage_report::CoverageStatus::CoverageGap,
        ),
        (
            STATE_CAPITAL_BUDGET_CAPITAL_PAPERS_DATAFLOW_ID,
            coverage_report::CoverageStatus::CoverageGap,
        ),
    ]
}

fn insert_expected_coverage_status(
    statuses: &mut BTreeMap<String, coverage_report::CoverageStatus>,
    dataflow_id: impl Into<String>,
    status: coverage_report::CoverageStatus,
) {
    statuses
        .entry(dataflow_id.into())
        .and_modify(|existing| {
            if coverage_report_status_priority(status) > coverage_report_status_priority(*existing)
            {
                *existing = status;
            }
        })
        .or_insert(status);
}

fn coverage_report_status_for_scorecard(
    status: ScorecardCoverageStatus,
) -> Option<coverage_report::CoverageStatus> {
    match status {
        ScorecardCoverageStatus::MissingExpected => {
            Some(coverage_report::CoverageStatus::MissingExpected)
        }
        ScorecardCoverageStatus::CoverageGap => Some(coverage_report::CoverageStatus::CoverageGap),
        ScorecardCoverageStatus::ManualPending => {
            Some(coverage_report::CoverageStatus::ManualPending)
        }
        ScorecardCoverageStatus::VisibleUnscored => {
            Some(coverage_report::CoverageStatus::VisibleUnscored)
        }
        ScorecardCoverageStatus::Resolved | ScorecardCoverageStatus::Stale => None,
    }
}

fn coverage_report_status_priority(status: coverage_report::CoverageStatus) -> u8 {
    match status {
        coverage_report::CoverageStatus::MissingExpected => 3,
        coverage_report::CoverageStatus::ManualPending => 2,
        coverage_report::CoverageStatus::CoverageGap => 1,
        coverage_report::CoverageStatus::VisibleUnscored => 0,
        coverage_report::CoverageStatus::Loaded
        | coverage_report::CoverageStatus::Partial
        | coverage_report::CoverageStatus::Failed
        | coverage_report::CoverageStatus::ZeroRows => 4,
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
async fn sync_adapter_catalog(
    pool: &au_kpis_db::PgPool,
    adapters: &Adapters,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await.context("begin adapter catalog sync")?;
    for adapter in adapters.iter() {
        if let Some(source) = adapter.source_metadata() {
            upsert_source(&mut tx, &source).await?;
        }
        for dataflow in adapter.dataflow_metadata() {
            upsert_dataflow(&mut tx, &dataflow).await?;
        }
    }
    tx.commit().await.context("commit adapter catalog sync")
}

async fn upsert_source(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source: &Source,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage, description)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO UPDATE
         SET name = EXCLUDED.name,
             homepage = EXCLUDED.homepage,
             description = EXCLUDED.description",
    )
    .bind(source.id.as_str())
    .bind(&source.name)
    .bind(&source.homepage)
    .bind(source.description.as_deref())
    .execute(&mut **tx)
    .await
    .with_context(|| format!("upsert source `{}`", source.id.as_str()))?;
    Ok(())
}

async fn upsert_dataflow(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dataflow: &Dataflow,
) -> anyhow::Result<()> {
    for measure in &dataflow.measures {
        let measure_id = measure.as_str();
        sqlx::query(
            "INSERT INTO measures (id, name, description, unit, scale)
             VALUES ($1, $2, NULL, $3, NULL)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(measure_id)
        .bind(catalog_label(measure_id))
        .bind(measure_id)
        .execute(&mut **tx)
        .await
        .with_context(|| format!("upsert measure `{measure_id}`"))?;
    }

    let dimension_ids = dataflow
        .dimensions
        .iter()
        .map(|dimension| dimension.as_str().to_string())
        .collect::<Vec<_>>();
    let measure_ids = dataflow
        .measures
        .iter()
        .map(|measure| measure.as_str().to_string())
        .collect::<Vec<_>>();

    sqlx::query(
        "INSERT INTO dataflows (
             id, source_id, name, description, dimensions, measures,
             frequency, license, attribution, source_url
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (id) DO UPDATE
         SET source_id = EXCLUDED.source_id,
             name = EXCLUDED.name,
             description = EXCLUDED.description,
             dimensions = EXCLUDED.dimensions,
             measures = EXCLUDED.measures,
             frequency = EXCLUDED.frequency,
             license = EXCLUDED.license,
             attribution = EXCLUDED.attribution,
             source_url = EXCLUDED.source_url",
    )
    .bind(dataflow.id.as_str())
    .bind(dataflow.source_id.as_str())
    .bind(&dataflow.name)
    .bind(dataflow.description.as_deref())
    .bind(&dimension_ids)
    .bind(&measure_ids)
    .bind(frequency_label(dataflow.frequency))
    .bind(license_label(&dataflow.license))
    .bind(&dataflow.attribution)
    .bind(&dataflow.source_url)
    .execute(&mut **tx)
    .await
    .with_context(|| format!("upsert dataflow `{}`", dataflow.id.as_str()))?;

    sqlx::query("DELETE FROM dimensions WHERE dataflow_id = $1")
        .bind(dataflow.id.as_str())
        .execute(&mut **tx)
        .await
        .with_context(|| format!("clear dimensions for `{}`", dataflow.id.as_str()))?;

    for (position, dimension) in dataflow.dimensions.iter().enumerate() {
        let dimension_id = dimension.as_str();
        let codelist_id = format!("{}.{}", dataflow.id.as_str(), dimension_id);
        sqlx::query(
            "INSERT INTO codelists (id, name, description)
             VALUES ($1, $2, NULL)
             ON CONFLICT (id) DO UPDATE
             SET name = EXCLUDED.name,
                 description = EXCLUDED.description",
        )
        .bind(&codelist_id)
        .bind(format!("{} {}", dataflow.name, catalog_label(dimension_id)))
        .execute(&mut **tx)
        .await
        .with_context(|| format!("upsert codelist `{codelist_id}`"))?;

        sqlx::query(
            "INSERT INTO dimensions (dataflow_id, id, name, description, codelist_id, position)
             VALUES ($1, $2, $3, NULL, $4, $5)",
        )
        .bind(dataflow.id.as_str())
        .bind(dimension_id)
        .bind(catalog_label(dimension_id))
        .bind(&codelist_id)
        .bind(i16::try_from(position).context("dimension position exceeds SMALLINT")?)
        .execute(&mut **tx)
        .await
        .with_context(|| {
            format!(
                "insert dimension `{}` for `{}`",
                dimension_id,
                dataflow.id.as_str()
            )
        })?;
    }

    Ok(())
}

fn frequency_label(frequency: Frequency) -> &'static str {
    match frequency {
        Frequency::Daily => "daily",
        Frequency::Weekly => "weekly",
        Frequency::Monthly => "monthly",
        Frequency::Quarterly => "quarterly",
        Frequency::Annual => "annual",
        Frequency::Irregular => "irregular",
    }
}

fn license_label(license: &License) -> String {
    match license {
        License::CcBy40 => "CC-BY-4.0".into(),
        License::CcByNd40 => "CC-BY-ND-4.0".into(),
        License::CcBySa40 => "CC-BY-SA-4.0".into(),
        License::PublicDomain => "public-domain".into(),
        License::Other(value) => value.clone(),
    }
}

fn catalog_label(id: &str) -> String {
    let mut label = String::with_capacity(id.len());
    let mut capitalize_next = true;
    for ch in id.chars() {
        if matches!(ch, '_' | '-' | '.') {
            label.push(' ');
            capitalize_next = true;
        } else if capitalize_next {
            label.extend(ch.to_uppercase());
            capitalize_next = false;
        } else {
            label.push(ch);
        }
    }
    label
}

#[cfg_attr(coverage_nightly, coverage(off))]
async fn run_worker(runtime: Runtime) -> anyhow::Result<()> {
    let recovered = au_kpis_db::recover_lost_observation_stages(&runtime.db)
        .await
        .context("recover lost unlogged observation staging")?;
    runtime
        .metrics
        .staging_recoveries_total
        .fetch_add(recovered, Ordering::Relaxed);
    if recovered > 0 {
        tracing::warn!(recovered, "reset generations after unlogged staging loss");
    }
    let queue = ApalisPgQueue::new(runtime.db.clone());
    let reconciled = durable_worker::reconcile_durable_jobs(&runtime, &queue)
        .await
        .context("reconcile durable ingestion stage jobs")?;
    if reconciled > 0 {
        tracing::info!(reconciled, "recreated durable ingestion stage jobs");
    }
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
    for stage in [
        QueueStage::Load,
        QueueStage::Parse,
        QueueStage::Fetch,
        QueueStage::Discover,
        QueueStage::Backfill,
    ] {
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

    let renewal_interval = lease_renewal_interval(queue.lease_timeout());
    let work_job = job.clone();
    let (job, result) = run_with_lease_renewal(queue, job, renewal_interval, async {
        durable_worker::process_job(runtime, queue, &work_job).await
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
                loaded = stats.loaded,
                "durable queue stage completed"
            );
        }
        Err(err) => {
            let mut nack = Nack::new(err.class(), err.to_string());
            if let Some(retry_after) = err.retry_after() {
                nack = nack.with_retry_after(retry_after);
            }
            queue.nack(&job, nack).await.context("nack queue job")?;
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

#[cfg(test)]
fn invalid_job_nack(err: anyhow::Error) -> Nack {
    Nack::new(ErrorClass::Permanent, err.to_string())
}

#[cfg(test)]
fn ingestion_error_class(err: &au_kpis_ingestion_core::IngestionError) -> ErrorClass {
    match err {
        au_kpis_ingestion_core::IngestionError::Adapter(err) => err.class(),
        au_kpis_ingestion_core::IngestionError::Db(err) => err.class(),
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

#[cfg(test)]
fn load_error_class(err: &au_kpis_loader::LoadError) -> ErrorClass {
    match err {
        au_kpis_loader::LoadError::Validation(_) => ErrorClass::Validation,
        au_kpis_loader::LoadError::Json(_) => ErrorClass::Permanent,
        au_kpis_loader::LoadError::Db(_) => ErrorClass::Transient,
        au_kpis_loader::LoadError::Durable(err) => err.class(),
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
    let mut abs = AbsAdapter::builder();
    if let Ok(base_url) = env::var("AU_KPIS_ABS_BASE_URL") {
        abs = abs.base_url(base_url);
    }
    if let Ok(release_url) = env::var("AU_KPIS_ABS_BUILDING_APPROVALS_RELEASE_URL") {
        abs = abs.building_approvals_release_url(release_url);
    }
    if let Ok(release_url) = env::var("AU_KPIS_ABS_BUILDING_ACTIVITY_RELEASE_URL") {
        abs = abs.building_activity_release_url(release_url);
    }
    if let Ok(article_url) = env::var("AU_KPIS_ABS_DWELLING_COMPLETION_TIMES_URL") {
        abs = abs.dwelling_completion_times_url(article_url);
    }
    let abs = abs.build();
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
    let mut aemo = AemoAdapter::builder();
    if let Ok(dispatch_listing_url) = env::var("AU_KPIS_AEMO_DISPATCH_LISTING_URL") {
        aemo = aemo.dispatch_listing_url(dispatch_listing_url);
    }
    if let Ok(generation_mix_listing_url) = env::var("AU_KPIS_AEMO_GENERATION_MIX_LISTING_URL") {
        aemo = aemo.generation_mix_listing_url(generation_mix_listing_url);
    }
    if let Ok(dispatchability_capacity_listing_url) =
        env::var("AU_KPIS_AEMO_DISPATCHABILITY_CAPACITY_LISTING_URL")
    {
        aemo = aemo.dispatchability_capacity_listing_url(dispatchability_capacity_listing_url);
    }
    let aemo = aemo.build();
    builder.register(aemo).context("register AEMO adapter")?;
    let ai_readiness = match env::var("AU_KPIS_AI_READINESS_INDEX_URL") {
        Ok(index_url) => AiReadinessAdapter::builder().index_url(index_url).build(),
        Err(_) => AiReadinessAdapter::default(),
    };
    builder
        .register(ai_readiness)
        .context("register AI readiness adapter")?;
    let mut asx = AsxAdapter::builder();
    if let Ok(market_statistics_url) = env::var("AU_KPIS_ASX_MARKET_STATISTICS_URL") {
        asx = asx.market_statistics_url(market_statistics_url);
    }
    if let Ok(announcements_rss_url) = env::var("AU_KPIS_ASX_ANNOUNCEMENTS_RSS_URL") {
        asx = asx.announcements_rss_url(announcements_rss_url);
    }
    if let Ok(eod_csv_url) = env::var("AU_KPIS_ASX_EOD_CSV_URL") {
        asx = asx.eod_csv_url(eod_csv_url);
    }
    let asx = asx.build();
    builder.register(asx).context("register ASX adapter")?;
    let nhsac = match env::var("AU_KPIS_NHSAC_INDEX_URL") {
        Ok(index_url) => NhsacAdapter::builder().index_url(index_url).build(),
        Err(_) => NhsacAdapter::default(),
    };
    builder.register(nhsac).context("register NHSAC adapter")?;
    let pc = match env::var("AU_KPIS_PC_INDEX_URL") {
        Ok(index_url) => PcAdapter::builder().index_url(index_url).build(),
        Err(_) => PcAdapter::default(),
    };
    builder.register(pc).context("register PC adapter")?;
    let worldbank = match env::var("AU_KPIS_WORLDBANK_INDEX_URL") {
        Ok(index_url) => WorldbankAdapter::builder().index_url(index_url).build(),
        Err(_) => WorldbankAdapter::default(),
    };
    builder
        .register(worldbank)
        .context("register World Bank adapter")?;
    let pdf_base_url = env::var("AU_KPIS_PDF_BASE_URL").ok();
    let pdf_request_timeout = pdf_request_timeout_from_env()?;
    let pdf_client = pdf_base_url
        .as_deref()
        .map(|base_url| {
            let mut client = PdfClient::builder().base_url(base_url);
            if let Some(timeout) = pdf_request_timeout {
                client = client.timeout(timeout);
            }
            client.build()
        })
        .transpose()
        .context("build PDF sidecar client")?;
    let mut treasury = TreasuryAdapter::builder();
    if let Ok(budget_url) = env::var("AU_KPIS_TREASURY_BUDGET_URL") {
        treasury = treasury.budget_url(budget_url);
    }
    if let Some(pdf_client) = pdf_client.clone() {
        treasury = treasury.pdf_client(pdf_client);
    } else if let Some(pdf_base_url) = &pdf_base_url {
        treasury = treasury.pdf_base_url(pdf_base_url.clone());
    }
    builder
        .register(treasury.try_build().context("build Treasury adapter")?)
        .context("register Treasury adapter")?;
    let mut state_budgets = StateBudgetsAdapter::builder();
    if let Some(pdf_client) = pdf_client {
        state_budgets = state_budgets.pdf_client(pdf_client);
    } else if let Some(pdf_base_url) = pdf_base_url {
        state_budgets = state_budgets.pdf_base_url(pdf_base_url);
    }
    builder
        .register(
            state_budgets
                .try_build()
                .context("build state budgets adapter")?,
        )
        .context("register state budgets adapter")?;
    let state_capital = match env::var("AU_KPIS_STATE_CAPITAL_INDEX_URL") {
        Ok(index_url) => StateCapitalAdapter::builder().index_url(index_url).build(),
        Err(_) => StateCapitalAdapter::default(),
    };
    builder
        .register(state_capital)
        .context("register state capital adapter")?;
    let state_planning = match env::var("AU_KPIS_STATE_PLANNING_INDEX_URL") {
        Ok(index_url) => StatePlanningAdapter::builder().index_url(index_url).build(),
        Err(_) => StatePlanningAdapter::default(),
    };
    builder
        .register(state_planning)
        .context("register state planning adapter")?;
    Ok(builder.build())
}

fn pdf_request_timeout_from_env() -> anyhow::Result<Option<Duration>> {
    let raw = match env::var("AU_KPIS_PDF_REQUEST_TIMEOUT_SECS") {
        Ok(raw) => raw,
        Err(env::VarError::NotPresent) => return Ok(None),
        Err(err) => return Err(err).context("read AU_KPIS_PDF_REQUEST_TIMEOUT_SECS"),
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let seconds = trimmed
        .parse::<u64>()
        .context("AU_KPIS_PDF_REQUEST_TIMEOUT_SECS must be a positive integer")?;
    if seconds == 0 {
        bail!("AU_KPIS_PDF_REQUEST_TIMEOUT_SECS must be a positive integer");
    }
    Ok(Some(Duration::from_secs(seconds)))
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
        Mode::CoverageReport { .. } => "coverage-report mode",
    };
    bail!(
        "{mode_name} requires durable object store config: set AU_KPIS_OBJECT_STORE__ENDPOINT, AU_KPIS_OBJECT_STORE__BUCKET, AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID, and AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY"
    )
}

fn pipeline_options(shutdown_grace: Duration) -> anyhow::Result<PipelineOptions> {
    pipeline_options_with_env(shutdown_grace, |name| env::var(name).ok())
}

fn pipeline_options_with_env<F>(
    shutdown_grace: Duration,
    get_env: F,
) -> anyhow::Result<PipelineOptions>
where
    F: Fn(&str) -> Option<String>,
{
    let mut options = PipelineOptions {
        shutdown_grace,
        ..PipelineOptions::default()
    };
    apply_positive_usize_override(
        &mut options.load_max_rows,
        "AU_KPIS_PIPELINE__LOAD_MAX_ROWS",
        &get_env,
    )?;
    apply_positive_usize_override(
        &mut options.load_max_bytes,
        "AU_KPIS_PIPELINE__LOAD_MAX_BYTES",
        &get_env,
    )?;
    apply_positive_usize_override(
        &mut options.channel_capacity,
        "AU_KPIS_PIPELINE__CHANNEL_CAPACITY",
        &get_env,
    )?;
    apply_positive_usize_override(
        &mut options.fetch_concurrency,
        "AU_KPIS_PIPELINE__FETCH_CONCURRENCY",
        &get_env,
    )?;
    apply_positive_usize_override(
        &mut options.parse_concurrency,
        "AU_KPIS_PIPELINE__PARSE_CONCURRENCY",
        &get_env,
    )?;
    Ok(options)
}

fn apply_positive_usize_override<F>(
    target: &mut usize,
    name: &str,
    get_env: &F,
) -> anyhow::Result<()>
where
    F: Fn(&str) -> Option<String>,
{
    let Some(raw) = get_env(name) else {
        return Ok(());
    };
    let value = raw
        .parse::<usize>()
        .with_context(|| format!("{name} must be a positive integer"))?;
    if value == 0 {
        bail!("{name} must be a positive integer");
    }
    *target = value;
    Ok(())
}

fn resolve_mode(cli: &Cli) -> anyhow::Result<Mode> {
    match (cli.once, cli.command.clone()) {
        (true, None) => {
            let source = cli
                .source
                .clone()
                .context("`--once` requires `--source <id>`")?;
            let dataflow = cli
                .dataflow
                .clone()
                .context("`--once` requires `--dataflow <id>`")?;
            Ok(Mode::Once {
                source,
                dataflow,
                allow_zero_jobs: cli.allow_zero_jobs,
            })
        }
        (false, Some(Command::Run)) => {
            if cli.source.is_some() || cli.dataflow.is_some() || cli.allow_zero_jobs {
                bail!("`run` does not accept `--source`, `--dataflow`, or `--allow-zero-jobs`");
            }
            Ok(Mode::Run)
        }
        (
            false,
            Some(Command::CoverageReport {
                output,
                markdown,
                fail_on_gaps,
            }),
        ) => {
            if cli.source.is_some() || cli.dataflow.is_some() || cli.allow_zero_jobs {
                bail!(
                    "`coverage-report` does not accept `--source`, `--dataflow`, or `--allow-zero-jobs`"
                );
            }
            Ok(Mode::CoverageReport {
                output,
                markdown,
                fail_on_gaps,
            })
        }
        (false, None) => {
            if cli.allow_zero_jobs {
                bail!("`--allow-zero-jobs` requires `--once`");
            }
            bail!(
                "choose either `--once --source <id> --dataflow <id>`, `coverage-report`, or `run`"
            )
        }
        (true, Some(Command::Run)) => bail!("`--once` cannot be combined with `run`"),
        (true, Some(Command::CoverageReport { .. })) => {
            bail!("`--once` cannot be combined with `coverage-report`")
        }
    }
}

fn validate_once_run_stats(
    request: &RunRequest,
    stats: &au_kpis_ingestion_core::PipelineRunStats,
    allow_zero_jobs: bool,
) -> anyhow::Result<()> {
    if allow_zero_jobs {
        return Ok(());
    }

    let dataflow = request
        .dataflow_id
        .as_ref()
        .map_or("<all>", DataflowId::as_str);
    if stats.discovered == 0 {
        bail!(
            "one-shot ingestion discovered zero jobs for source `{}` dataflow `{}`; pass `--allow-zero-jobs` only for reviewed upstream gaps",
            request.source_id.as_str(),
            dataflow
        );
    }
    if stats.loaded.observations_loaded == 0 {
        bail!(
            "one-shot ingestion loaded zero observations for source `{}` dataflow `{}` after discovering {} job(s); pass `--allow-zero-jobs` only for reviewed metadata-only or upstream-gap runs",
            request.source_id.as_str(),
            dataflow,
            stats.discovered
        );
    }

    Ok(())
}

fn validate_once_target(source: &str, dataflow: &str) -> anyhow::Result<()> {
    validate_supported_source(source)?;
    match source {
        "abs"
            if matches!(
                dataflow,
                ABS_CPI_DATAFLOW_SLUG
                    | ABS_BUILDING_APPROVALS_DATAFLOW_SLUG
                    | ABS_BUILDING_ACTIVITY_DATAFLOW_SLUG
                    | ABS_DWELLING_COMPLETION_TIMES_DATAFLOW_SLUG
            ) =>
        {
            Ok(())
        }
        "apra"
            if matches!(
                dataflow,
                APRA_QUARTERLY_DATAFLOW_SLUG | APRA_SUPER_ASSET_ALLOCATION_DATAFLOW_SLUG
            ) =>
        {
            Ok(())
        }
        "aemo"
            if matches!(
                dataflow,
                AEMO_DISPATCH_DATAFLOW_SLUG
                    | AEMO_GENERATION_MIX_DATAFLOW_SLUG
                    | AEMO_DISPATCHABILITY_CAPACITY_DATAFLOW_SLUG
            ) =>
        {
            Ok(())
        }
        "ai-readiness"
            if matches!(
                dataflow,
                AI_READINESS_OXFORD_GARI_DATAFLOW_SLUG
                    | AI_READINESS_NAIC_ADOPTION_DATAFLOW_SLUG
                    | AI_READINESS_ABS_AI_RD_DATAFLOW_SLUG
                    | AI_READINESS_HOME_AFFAIRS_TALENT_DATAFLOW_SLUG
            ) =>
        {
            Ok(())
        }
        "asx"
            if matches!(
                dataflow,
                ASX_MARKET_STATISTICS_DATAFLOW_SLUG
                    | ASX_ANNOUNCEMENTS_DATAFLOW_SLUG
                    | ASX_EOD_DATAFLOW_SLUG
            ) =>
        {
            Ok(())
        }
        "nhsac" if dataflow == NHSAC_HOUSING_ACCORD_DATAFLOW_SLUG => Ok(()),
        "pc" if dataflow == PC_PRODUCTIVITY_BULLETIN_DATAFLOW_SLUG => Ok(()),
        "worldbank" if dataflow == WORLDBANK_BREADY_DATAFLOW_SLUG => Ok(()),
        "rba" if dataflow == RBA_STAT_TABLES_DATAFLOW_SLUG => Ok(()),
        "state-budgets"
            if matches!(
                dataflow,
                STATE_BUDGETS_NSW_DATAFLOW_SLUG
                    | STATE_BUDGETS_VIC_DATAFLOW_SLUG
                    | STATE_BUDGETS_QLD_DATAFLOW_SLUG
            ) =>
        {
            Ok(())
        }
        "state_capital"
            if matches!(
                dataflow,
                STATE_CAPITAL_VIC_MAJOR_PROJECTS_DATAFLOW_SLUG
                    | STATE_CAPITAL_BUDGET_CAPITAL_PAPERS_DATAFLOW_SLUG
            ) =>
        {
            Ok(())
        }
        "state-planning"
            if matches!(
                dataflow,
                STATE_PLANNING_NSW_DA_PROCESSING_DATAFLOW_SLUG
                    | STATE_PLANNING_VIC_PERMIT_ACTIVITY_DATAFLOW_SLUG
            ) =>
        {
            Ok(())
        }
        "treasury" if dataflow == TREASURY_BUDGET_DATAFLOW_SLUG => Ok(()),
        "abs" => bail!(
            "unsupported dataflow `{dataflow}` for source `abs`; supported dataflows: {ABS_CPI_DATAFLOW_SLUG}, {ABS_BUILDING_APPROVALS_DATAFLOW_SLUG}, {ABS_BUILDING_ACTIVITY_DATAFLOW_SLUG}, {ABS_DWELLING_COMPLETION_TIMES_DATAFLOW_SLUG}"
        ),
        "apra" => bail!(
            "unsupported dataflow `{dataflow}` for source `apra`; supported dataflows: {APRA_QUARTERLY_DATAFLOW_SLUG}, {APRA_SUPER_ASSET_ALLOCATION_DATAFLOW_SLUG}"
        ),
        "aemo" => bail!(
            "unsupported dataflow `{dataflow}` for source `aemo`; supported dataflows: {AEMO_DISPATCH_DATAFLOW_SLUG}, {AEMO_GENERATION_MIX_DATAFLOW_SLUG}, {AEMO_DISPATCHABILITY_CAPACITY_DATAFLOW_SLUG}"
        ),
        "ai-readiness" => bail!(
            "unsupported dataflow `{dataflow}` for source `ai-readiness`; supported dataflows: {AI_READINESS_OXFORD_GARI_DATAFLOW_SLUG}, {AI_READINESS_NAIC_ADOPTION_DATAFLOW_SLUG}, {AI_READINESS_ABS_AI_RD_DATAFLOW_SLUG}, {AI_READINESS_HOME_AFFAIRS_TALENT_DATAFLOW_SLUG}"
        ),
        "asx" => bail!(
            "unsupported dataflow `{dataflow}` for source `asx`; supported dataflows: {ASX_MARKET_STATISTICS_DATAFLOW_SLUG}, {ASX_ANNOUNCEMENTS_DATAFLOW_SLUG}, {ASX_EOD_DATAFLOW_SLUG}"
        ),
        "nhsac" => bail!(
            "unsupported dataflow `{dataflow}` for source `nhsac`; supported dataflow: {NHSAC_HOUSING_ACCORD_DATAFLOW_SLUG}"
        ),
        "pc" => bail!(
            "unsupported dataflow `{dataflow}` for source `pc`; supported dataflow: {PC_PRODUCTIVITY_BULLETIN_DATAFLOW_SLUG}"
        ),
        "worldbank" => bail!(
            "unsupported dataflow `{dataflow}` for source `worldbank`; supported dataflow: {WORLDBANK_BREADY_DATAFLOW_SLUG}"
        ),
        "rba" => bail!(
            "unsupported dataflow `{dataflow}` for source `rba`; supported dataflow: {RBA_STAT_TABLES_DATAFLOW_SLUG}"
        ),
        "state-budgets" => bail!(
            "unsupported dataflow `{dataflow}` for source `state-budgets`; supported dataflows: {STATE_BUDGETS_NSW_DATAFLOW_SLUG}, {STATE_BUDGETS_VIC_DATAFLOW_SLUG}, {STATE_BUDGETS_QLD_DATAFLOW_SLUG}"
        ),
        "state_capital" => bail!(
            "unsupported dataflow `{dataflow}` for source `state_capital`; supported dataflows: {STATE_CAPITAL_VIC_MAJOR_PROJECTS_DATAFLOW_SLUG}, {STATE_CAPITAL_BUDGET_CAPITAL_PAPERS_DATAFLOW_SLUG}"
        ),
        "state-planning" => bail!(
            "unsupported dataflow `{dataflow}` for source `state-planning`; supported dataflows: {STATE_PLANNING_NSW_DA_PROCESSING_DATAFLOW_SLUG}, {STATE_PLANNING_VIC_PERMIT_ACTIVITY_DATAFLOW_SLUG}"
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
        "abs" if dataflow == ABS_CPI_DATAFLOW_SLUG => ABS_CPI_DATAFLOW_ID,
        "abs" if dataflow == ABS_BUILDING_APPROVALS_DATAFLOW_SLUG => {
            ABS_BUILDING_APPROVALS_DATAFLOW_ID
        }
        "abs" if dataflow == ABS_BUILDING_ACTIVITY_DATAFLOW_SLUG => {
            ABS_BUILDING_ACTIVITY_DATAFLOW_ID
        }
        "abs" if dataflow == ABS_DWELLING_COMPLETION_TIMES_DATAFLOW_SLUG => {
            ABS_DWELLING_COMPLETION_TIMES_DATAFLOW_ID
        }
        "apra" if dataflow == APRA_QUARTERLY_DATAFLOW_SLUG => APRA_QUARTERLY_DATAFLOW_ID,
        "apra" if dataflow == APRA_SUPER_ASSET_ALLOCATION_DATAFLOW_SLUG => {
            APRA_SUPER_ASSET_ALLOCATION_DATAFLOW_ID
        }
        "aemo" if dataflow == AEMO_DISPATCH_DATAFLOW_SLUG => AEMO_DISPATCH_DATAFLOW_ID,
        "aemo" if dataflow == AEMO_GENERATION_MIX_DATAFLOW_SLUG => AEMO_GENERATION_MIX_DATAFLOW_ID,
        "aemo" if dataflow == AEMO_DISPATCHABILITY_CAPACITY_DATAFLOW_SLUG => {
            AEMO_DISPATCHABILITY_CAPACITY_DATAFLOW_ID
        }
        "ai-readiness" if dataflow == AI_READINESS_OXFORD_GARI_DATAFLOW_SLUG => {
            AI_READINESS_OXFORD_GARI_DATAFLOW_ID
        }
        "ai-readiness" if dataflow == AI_READINESS_NAIC_ADOPTION_DATAFLOW_SLUG => {
            AI_READINESS_NAIC_ADOPTION_DATAFLOW_ID
        }
        "ai-readiness" if dataflow == AI_READINESS_ABS_AI_RD_DATAFLOW_SLUG => {
            AI_READINESS_ABS_AI_RD_DATAFLOW_ID
        }
        "ai-readiness" if dataflow == AI_READINESS_HOME_AFFAIRS_TALENT_DATAFLOW_SLUG => {
            AI_READINESS_HOME_AFFAIRS_TALENT_DATAFLOW_ID
        }
        "asx" if dataflow == ASX_MARKET_STATISTICS_DATAFLOW_SLUG => {
            ASX_MARKET_STATISTICS_DATAFLOW_ID
        }
        "asx" if dataflow == ASX_ANNOUNCEMENTS_DATAFLOW_SLUG => ASX_ANNOUNCEMENTS_DATAFLOW_ID,
        "asx" if dataflow == ASX_EOD_DATAFLOW_SLUG => ASX_EOD_DATAFLOW_ID,
        "nhsac" => NHSAC_HOUSING_ACCORD_DATAFLOW_ID,
        "pc" => PC_PRODUCTIVITY_BULLETIN_DATAFLOW_ID,
        "worldbank" => WORLDBANK_BREADY_DATAFLOW_ID,
        "rba" => RBA_STAT_TABLES_DATAFLOW_ID,
        "state-budgets" if dataflow == STATE_BUDGETS_NSW_DATAFLOW_SLUG => {
            STATE_BUDGETS_NSW_DATAFLOW_ID
        }
        "state-budgets" if dataflow == STATE_BUDGETS_VIC_DATAFLOW_SLUG => {
            STATE_BUDGETS_VIC_DATAFLOW_ID
        }
        "state-budgets" if dataflow == STATE_BUDGETS_QLD_DATAFLOW_SLUG => {
            STATE_BUDGETS_QLD_DATAFLOW_ID
        }
        "state_capital" if dataflow == STATE_CAPITAL_VIC_MAJOR_PROJECTS_DATAFLOW_SLUG => {
            STATE_CAPITAL_VIC_MAJOR_PROJECTS_DATAFLOW_ID
        }
        "state_capital" if dataflow == STATE_CAPITAL_BUDGET_CAPITAL_PAPERS_DATAFLOW_SLUG => {
            STATE_CAPITAL_BUDGET_CAPITAL_PAPERS_DATAFLOW_ID
        }
        "state-planning" if dataflow == STATE_PLANNING_NSW_DA_PROCESSING_DATAFLOW_SLUG => {
            STATE_PLANNING_NSW_DA_PROCESSING_DATAFLOW_ID
        }
        "state-planning" if dataflow == STATE_PLANNING_VIC_PERMIT_ACTIVITY_DATAFLOW_SLUG => {
            STATE_PLANNING_VIC_PERMIT_ACTIVITY_DATAFLOW_ID
        }
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

#[cfg(test)]
fn job_run_request(kind: &JobKind, trace_parent: Option<&str>) -> anyhow::Result<RunRequest> {
    match kind {
        JobKind::Discover {
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
    if !matches!(
        source,
        "abs"
            | "aemo"
            | "ai-readiness"
            | "apra"
            | "asx"
            | "nhsac"
            | "pc"
            | "worldbank"
            | "rba"
            | "state-budgets"
            | "state_capital"
            | "state-planning"
            | "treasury"
    ) {
        bail!(
            "unsupported source `{source}`; supported sources: abs, aemo, ai-readiness, apra, asx, nhsac, pc, worldbank, rba, state-budgets, state_capital, state-planning, treasury"
        );
    }
    Ok(())
}

#[cfg(test)]
fn validate_supported_dataflow_id(source: &str, dataflow_id: &str) -> anyhow::Result<()> {
    if source == "abs" && dataflow_id == ABS_CPI_DATAFLOW_ID {
        return Ok(());
    }
    if source == "abs" && dataflow_id == ABS_BUILDING_APPROVALS_DATAFLOW_ID {
        return Ok(());
    }
    if source == "abs" && dataflow_id == ABS_BUILDING_ACTIVITY_DATAFLOW_ID {
        return Ok(());
    }
    if source == "abs" && dataflow_id == ABS_DWELLING_COMPLETION_TIMES_DATAFLOW_ID {
        return Ok(());
    }
    if source == "apra" && dataflow_id == APRA_QUARTERLY_DATAFLOW_ID {
        return Ok(());
    }
    if source == "apra" && dataflow_id == APRA_SUPER_ASSET_ALLOCATION_DATAFLOW_ID {
        return Ok(());
    }
    if source == "aemo" && dataflow_id == AEMO_DISPATCH_DATAFLOW_ID {
        return Ok(());
    }
    if source == "aemo" && dataflow_id == AEMO_GENERATION_MIX_DATAFLOW_ID {
        return Ok(());
    }
    if source == "aemo" && dataflow_id == AEMO_DISPATCHABILITY_CAPACITY_DATAFLOW_ID {
        return Ok(());
    }
    if source == "ai-readiness" && dataflow_id == AI_READINESS_OXFORD_GARI_DATAFLOW_ID {
        return Ok(());
    }
    if source == "ai-readiness" && dataflow_id == AI_READINESS_NAIC_ADOPTION_DATAFLOW_ID {
        return Ok(());
    }
    if source == "ai-readiness" && dataflow_id == AI_READINESS_ABS_AI_RD_DATAFLOW_ID {
        return Ok(());
    }
    if source == "ai-readiness" && dataflow_id == AI_READINESS_HOME_AFFAIRS_TALENT_DATAFLOW_ID {
        return Ok(());
    }
    if source == "asx" && dataflow_id == ASX_MARKET_STATISTICS_DATAFLOW_ID {
        return Ok(());
    }
    if source == "asx" && dataflow_id == ASX_ANNOUNCEMENTS_DATAFLOW_ID {
        return Ok(());
    }
    if source == "asx" && dataflow_id == ASX_EOD_DATAFLOW_ID {
        return Ok(());
    }
    if source == "nhsac" && dataflow_id == NHSAC_HOUSING_ACCORD_DATAFLOW_ID {
        return Ok(());
    }
    if source == "pc" && dataflow_id == PC_PRODUCTIVITY_BULLETIN_DATAFLOW_ID {
        return Ok(());
    }
    if source == "worldbank" && dataflow_id == WORLDBANK_BREADY_DATAFLOW_ID {
        return Ok(());
    }
    if source == "rba" && dataflow_id == RBA_STAT_TABLES_DATAFLOW_ID {
        return Ok(());
    }
    if source == "state-budgets" && dataflow_id == STATE_BUDGETS_NSW_DATAFLOW_ID {
        return Ok(());
    }
    if source == "state-budgets" && dataflow_id == STATE_BUDGETS_VIC_DATAFLOW_ID {
        return Ok(());
    }
    if source == "state-budgets" && dataflow_id == STATE_BUDGETS_QLD_DATAFLOW_ID {
        return Ok(());
    }
    if source == "state_capital" && dataflow_id == STATE_CAPITAL_VIC_MAJOR_PROJECTS_DATAFLOW_ID {
        return Ok(());
    }
    if source == "state_capital" && dataflow_id == STATE_CAPITAL_BUDGET_CAPITAL_PAPERS_DATAFLOW_ID {
        return Ok(());
    }
    if source == "state-planning" && dataflow_id == STATE_PLANNING_NSW_DA_PROCESSING_DATAFLOW_ID {
        return Ok(());
    }
    if source == "state-planning" && dataflow_id == STATE_PLANNING_VIC_PERMIT_ACTIVITY_DATAFLOW_ID {
        return Ok(());
    }
    if source == "treasury" && dataflow_id == TREASURY_BUDGET_DATAFLOW_ID {
        return Ok(());
    }
    bail!(
        "unsupported dataflow `{dataflow_id}` for source `{source}`; supported dataflows: {ABS_CPI_DATAFLOW_ID}, {ABS_BUILDING_APPROVALS_DATAFLOW_ID}, {ABS_BUILDING_ACTIVITY_DATAFLOW_ID}, {ABS_DWELLING_COMPLETION_TIMES_DATAFLOW_ID}, {AEMO_DISPATCH_DATAFLOW_ID}, {AEMO_GENERATION_MIX_DATAFLOW_ID}, {AEMO_DISPATCHABILITY_CAPACITY_DATAFLOW_ID}, {AI_READINESS_OXFORD_GARI_DATAFLOW_ID}, {AI_READINESS_NAIC_ADOPTION_DATAFLOW_ID}, {AI_READINESS_ABS_AI_RD_DATAFLOW_ID}, {AI_READINESS_HOME_AFFAIRS_TALENT_DATAFLOW_ID}, {APRA_QUARTERLY_DATAFLOW_ID}, {APRA_SUPER_ASSET_ALLOCATION_DATAFLOW_ID}, {ASX_MARKET_STATISTICS_DATAFLOW_ID}, {ASX_ANNOUNCEMENTS_DATAFLOW_ID}, {ASX_EOD_DATAFLOW_ID}, {NHSAC_HOUSING_ACCORD_DATAFLOW_ID}, {PC_PRODUCTIVITY_BULLETIN_DATAFLOW_ID}, {WORLDBANK_BREADY_DATAFLOW_ID}, {RBA_STAT_TABLES_DATAFLOW_ID}, {STATE_BUDGETS_NSW_DATAFLOW_ID}, {STATE_BUDGETS_VIC_DATAFLOW_ID}, {STATE_BUDGETS_QLD_DATAFLOW_ID}, {STATE_CAPITAL_VIC_MAJOR_PROJECTS_DATAFLOW_ID}, {STATE_CAPITAL_BUDGET_CAPITAL_PAPERS_DATAFLOW_ID}, {STATE_PLANNING_NSW_DA_PROCESSING_DATAFLOW_ID}, {STATE_PLANNING_VIC_PERMIT_ACTIVITY_DATAFLOW_ID}, {TREASURY_BUDGET_DATAFLOW_ID}"
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
    use std::path::PathBuf;
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
                dataflow: "cpi".to_string(),
                allow_zero_jobs: false,
            }
        );
    }

    #[test]
    fn once_mode_resolves_allow_zero_jobs() {
        assert_eq!(
            resolve_mode(&cli(&[
                "--once",
                "--source",
                "ai-readiness",
                "--dataflow",
                "oxford-gari",
                "--allow-zero-jobs"
            ]))
            .expect("resolve once mode with zero override"),
            Mode::Once {
                source: "ai-readiness".to_string(),
                dataflow: "oxford-gari".to_string(),
                allow_zero_jobs: true,
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
    fn run_mode_rejects_allow_zero_jobs() {
        let err = resolve_mode(&cli(&["--allow-zero-jobs", "run"]))
            .expect_err("run zero-job override should fail")
            .to_string();
        assert!(err.contains("does not accept"));
        assert!(err.contains("--allow-zero-jobs"));
    }

    #[test]
    fn coverage_report_mode_resolves_output_paths() {
        assert_eq!(
            resolve_mode(&cli(&[
                "coverage-report",
                "--output",
                "coverage.json",
                "--markdown",
                "coverage.md"
            ]))
            .expect("resolve coverage report mode"),
            Mode::CoverageReport {
                output: Some(PathBuf::from("coverage.json")),
                markdown: Some(PathBuf::from("coverage.md")),
                fail_on_gaps: false,
            }
        );
    }

    #[test]
    fn coverage_report_mode_rejects_source_filter() {
        let err = resolve_mode(&cli(&["--source", "abs", "coverage-report"]))
            .expect_err("coverage report source filter should fail")
            .to_string();
        assert!(err.contains("does not accept"));
    }

    #[test]
    fn coverage_report_marks_catalog_only_source_gaps() {
        let statuses =
            expected_coverage_statuses_by_dataflow().expect("load expected coverage statuses");

        assert_eq!(
            statuses.get(ASX_ANNOUNCEMENTS_DATAFLOW_ID),
            Some(&coverage_report::CoverageStatus::CoverageGap)
        );
        assert_eq!(
            statuses.get(ASX_EOD_DATAFLOW_ID),
            Some(&coverage_report::CoverageStatus::CoverageGap)
        );
        assert_eq!(
            statuses.get(STATE_CAPITAL_BUDGET_CAPITAL_PAPERS_DATAFLOW_ID),
            Some(&coverage_report::CoverageStatus::CoverageGap)
        );
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
    fn metrics_render_schema_hash_drift_by_source_and_dataflow() {
        let metrics = WorkerMetrics::default();
        let err = au_kpis_ingestion_core::IngestionError::Adapter(
            au_kpis_adapter::AdapterError::SchemaHashDrift(Box::new(
                au_kpis_adapter::SchemaHashDrift {
                    source_id: SourceId::new("treasury").unwrap(),
                    dataflow_id: DataflowId::new("treasury.budget_papers").unwrap(),
                    parser_version: "parse_v2".to_string(),
                    schema_key: "bp4-agency-resourcing".to_string(),
                    expected_hash: "abc123".to_string(),
                    actual_hash: "def456".to_string(),
                },
            )),
        );

        metrics.record_ingestion_error(&err);
        let body = metrics.render_prometheus();

        assert!(body.contains("# TYPE au_kpis_schema_hash_drifts_total counter"));
        assert!(body.contains(
            "au_kpis_schema_hash_drifts_total{source=\"treasury\",dataflow=\"treasury.budget_papers\"} 1"
        ));
    }

    #[test]
    fn once_mode_requires_durable_object_store_config() {
        let err = build_blob_store(
            &Mode::Once {
                source: "abs".to_string(),
                dataflow: "cpi".to_string(),
                allow_zero_jobs: false,
            },
            ObjectStoreConfig::default(),
        )
        .expect_err("once mode should reject missing durable object store config")
        .to_string();

        assert!(err.contains("durable object store config"));
    }

    #[test]
    fn once_run_stats_reject_zero_discovery_without_override() {
        let request = once_run_request("ai-readiness", "oxford-gari").unwrap();
        let stats = au_kpis_ingestion_core::PipelineRunStats::default();

        let err = validate_once_run_stats(&request, &stats, false)
            .expect_err("zero discovered jobs should fail")
            .to_string();

        assert!(err.contains("discovered zero jobs"));
        assert!(err.contains("ai-readiness"));
        assert!(err.contains("oxford.gari"));
    }

    #[test]
    fn once_run_stats_allow_zero_discovery_with_override() {
        let request = once_run_request("ai-readiness", "oxford-gari").unwrap();
        let stats = au_kpis_ingestion_core::PipelineRunStats::default();

        validate_once_run_stats(&request, &stats, true).expect("zero override should pass");
    }

    #[test]
    fn once_run_stats_reject_zero_loaded_observations_without_override() {
        let request = once_run_request("aemo", "dispatch").unwrap();
        let stats = au_kpis_ingestion_core::PipelineRunStats {
            discovered: 1,
            fetched: 1,
            parsed: 0,
            loaded: Default::default(),
        };

        let err = validate_once_run_stats(&request, &stats, false)
            .expect_err("zero loaded observations should fail")
            .to_string();

        assert!(err.contains("loaded zero observations"));
        assert!(err.contains("aemo"));
        assert!(err.contains("aemo.dispatch"));
    }

    #[test]
    fn once_run_stats_accept_loaded_observations() {
        let request = once_run_request("aemo", "dispatch").unwrap();
        let mut stats = au_kpis_ingestion_core::PipelineRunStats {
            discovered: 1,
            fetched: 1,
            parsed: 1,
            loaded: Default::default(),
        };
        stats.loaded.observations_loaded = 1;

        validate_once_run_stats(&request, &stats, false).expect("loaded rows should pass");
    }

    #[test]
    fn configured_shutdown_grace_propagates_to_pipeline_options() {
        let options = pipeline_options_with_env(Duration::from_secs(7), |_| None)
            .expect("default pipeline options");

        assert_eq!(options.shutdown_grace, Duration::from_secs(7));
    }

    #[test]
    fn pipeline_env_overrides_load_batch_limits() {
        let options = pipeline_options_with_env(Duration::from_secs(7), |name| match name {
            "AU_KPIS_PIPELINE__LOAD_MAX_ROWS" => Some("125".to_string()),
            "AU_KPIS_PIPELINE__LOAD_MAX_BYTES" => Some("65536".to_string()),
            _ => None,
        })
        .expect("pipeline options with load overrides");

        assert_eq!(options.load_max_rows, 125);
        assert_eq!(options.load_max_bytes, 65_536);
        assert_eq!(options.shutdown_grace, Duration::from_secs(7));
    }

    #[test]
    fn pipeline_env_rejects_zero_load_batch_limit() {
        let err = pipeline_options_with_env(Duration::from_secs(7), |name| match name {
            "AU_KPIS_PIPELINE__LOAD_MAX_ROWS" => Some("0".to_string()),
            _ => None,
        })
        .expect_err("zero load row limit should fail")
        .to_string();

        assert!(err.contains("AU_KPIS_PIPELINE__LOAD_MAX_ROWS"));
        assert!(err.contains("positive integer"));
    }

    #[test]
    fn unsupported_dataflow_reports_specific_error() {
        let err = validate_once_target("abs", "wpi")
            .expect_err("unsupported dataflow should fail")
            .to_string();
        assert!(err.contains("unsupported dataflow"));
        assert!(err.contains("cpi"));
        assert!(err.contains("building-approvals"));
        assert!(err.contains("building-activity"));
        assert!(err.contains("dwelling-completion-times"));
    }

    #[test]
    fn unsupported_source_reports_specific_error() {
        let err = validate_once_target("unknown", "cpi")
            .expect_err("unsupported source should fail")
            .to_string();
        assert!(err.contains("unsupported source"));
        assert!(
            err.contains(
                "abs, aemo, ai-readiness, apra, asx, nhsac, pc, worldbank, rba, state-budgets, state_capital, state-planning, treasury"
            )
        );
    }

    #[test]
    fn aemo_once_mode_resolves_dispatch_dataflow() {
        let request = once_run_request("aemo", "dispatch").expect("AEMO dispatch is supported");

        assert_eq!(request.source_id.as_str(), "aemo");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("aemo.dispatch").unwrap())
        );
    }

    #[test]
    fn aemo_once_mode_resolves_generation_mix_dataflow() {
        let request =
            once_run_request("aemo", "generation-mix").expect("AEMO generation mix is supported");

        assert_eq!(request.source_id.as_str(), "aemo");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("aemo.generation_mix").unwrap())
        );
    }

    #[test]
    fn aemo_once_mode_resolves_dispatchability_capacity_dataflow() {
        let request = once_run_request("aemo", "dispatchability-capacity")
            .expect("AEMO dispatchability capacity is supported");

        assert_eq!(request.source_id.as_str(), "aemo");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("aemo.dispatchability_capacity").unwrap())
        );
    }

    #[test]
    fn abs_once_mode_resolves_building_approvals_dataflow() {
        let request = once_run_request("abs", "building-approvals")
            .expect("ABS building approvals are supported");

        assert_eq!(request.source_id.as_str(), "abs");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("abs.building_approvals").unwrap())
        );
    }

    #[test]
    fn abs_once_mode_resolves_building_activity_dataflow() {
        let request = once_run_request("abs", "building-activity")
            .expect("ABS building activity is supported");

        assert_eq!(request.source_id.as_str(), "abs");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("abs.building_activity").unwrap())
        );
    }

    #[test]
    fn abs_once_mode_resolves_dwelling_completion_times_dataflow() {
        let request = once_run_request("abs", "dwelling-completion-times")
            .expect("ABS dwelling completion times are supported");

        assert_eq!(request.source_id.as_str(), "abs");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("abs.dwelling_completion_times").unwrap())
        );
    }

    #[test]
    fn asx_once_mode_resolves_market_statistics_dataflow() {
        let request = once_run_request("asx", "market-statistics")
            .expect("ASX market statistics are supported");

        assert_eq!(request.source_id.as_str(), "asx");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("asx.market_statistics").unwrap())
        );
    }

    #[test]
    fn asx_once_mode_resolves_announcements_dataflow() {
        let request =
            once_run_request("asx", "announcements").expect("ASX announcements are supported");

        assert_eq!(request.source_id.as_str(), "asx");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("asx.announcements").unwrap())
        );
    }

    #[test]
    fn asx_once_mode_resolves_eod_dataflow() {
        let request = once_run_request("asx", "eod").expect("ASX EOD is supported");

        assert_eq!(request.source_id.as_str(), "asx");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("asx.eod").unwrap())
        );
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
    fn pc_once_mode_resolves_productivity_bulletin_dataflow() {
        let request = once_run_request("pc", "productivity-bulletin")
            .expect("PC productivity bulletin is supported");

        assert_eq!(request.source_id.as_str(), "pc");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("pc.productivity_bulletin").unwrap())
        );
    }

    #[test]
    fn nhsac_once_mode_resolves_housing_accord_progress_dataflow() {
        let request = once_run_request("nhsac", "housing-accord-progress")
            .expect("NHSAC Housing Accord progress is supported");

        assert_eq!(request.source_id.as_str(), "nhsac");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("nhsac.housing_accord_progress").unwrap())
        );
    }

    #[test]
    fn worldbank_once_mode_resolves_bready_dataflow() {
        let request =
            once_run_request("worldbank", "bready").expect("World Bank B-READY is supported");

        assert_eq!(request.source_id.as_str(), "worldbank");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("worldbank.bready").unwrap())
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
    fn apra_once_mode_resolves_super_asset_allocation_dataflow() {
        let request = once_run_request("apra", "super-asset-allocation")
            .expect("APRA super asset allocation is supported");

        assert_eq!(request.source_id.as_str(), "apra");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("apra.super_asset_allocation").unwrap())
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
    fn state_budgets_once_mode_resolves_nsw_budget_dataflow() {
        let request =
            once_run_request("state-budgets", "nsw-budget").expect("NSW state budget is supported");

        assert_eq!(request.source_id.as_str(), "state-budgets");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("state_budgets.nsw_budget").unwrap())
        );
    }

    #[test]
    fn state_budgets_once_mode_resolves_vic_budget_dataflow() {
        let request =
            once_run_request("state-budgets", "vic-budget").expect("VIC state budget is supported");

        assert_eq!(request.source_id.as_str(), "state-budgets");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("state_budgets.vic_budget").unwrap())
        );
    }

    #[test]
    fn state_budgets_once_mode_resolves_qld_budget_dataflow() {
        let request =
            once_run_request("state-budgets", "qld-budget").expect("QLD state budget is supported");

        assert_eq!(request.source_id.as_str(), "state-budgets");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("state_budgets.qld_budget").unwrap())
        );
    }

    #[test]
    fn state_planning_once_mode_resolves_nsw_da_processing_dataflow() {
        let request = once_run_request("state-planning", "nsw-da-processing")
            .expect("NSW planning throughput is supported");

        assert_eq!(request.source_id.as_str(), "state-planning");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("state_planning.nsw_da_processing").unwrap())
        );
    }

    #[test]
    fn state_planning_once_mode_resolves_vic_permit_activity_dataflow() {
        let request = once_run_request("state-planning", "vic-permit-activity")
            .expect("VIC planning throughput is supported");

        assert_eq!(request.source_id.as_str(), "state-planning");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("state_planning.vic_permit_activity").unwrap())
        );
    }

    #[test]
    fn ai_readiness_once_mode_resolves_oxford_gari_dataflow() {
        let request =
            once_run_request("ai-readiness", "oxford-gari").expect("Oxford GARI is supported");

        assert_eq!(request.source_id.as_str(), "ai-readiness");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("oxford.gari").unwrap())
        );
    }

    #[test]
    fn ai_readiness_once_mode_resolves_naic_adoption_dataflow() {
        let request = once_run_request("ai-readiness", "naic-ai-adoption-tracker")
            .expect("NAIC adoption tracker is supported");

        assert_eq!(request.source_id.as_str(), "ai-readiness");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("naic.ai_adoption_tracker").unwrap())
        );
    }

    #[test]
    fn ai_readiness_once_mode_resolves_abs_ai_rd_dataflow() {
        let request =
            once_run_request("ai-readiness", "abs-ai-rd").expect("ABS AI R&D is supported");

        assert_eq!(request.source_id.as_str(), "ai-readiness");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("abs.ai_rd").unwrap())
        );
    }

    #[test]
    fn ai_readiness_once_mode_resolves_home_affairs_talent_dataflow() {
        let request = once_run_request("ai-readiness", "home-affairs-skillselect-talent-proxy")
            .expect("Home Affairs talent proxy is supported");

        assert_eq!(request.source_id.as_str(), "ai-readiness");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("home_affairs.skillselect_talent_proxy").unwrap())
        );
    }

    #[test]
    fn state_capital_once_mode_resolves_vic_major_projects_dataflow() {
        let request = once_run_request("state_capital", "vic-major-projects")
            .expect("VIC major projects are supported");

        assert_eq!(request.source_id.as_str(), "state_capital");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("state_capital.vic_major_projects").unwrap())
        );
    }

    #[test]
    fn state_capital_once_mode_resolves_budget_capital_papers_dataflow() {
        let request = once_run_request("state_capital", "budget-capital-papers")
            .expect("state budget capital papers are supported");

        assert_eq!(request.source_id.as_str(), "state_capital");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("state_capital.budget_capital_papers").unwrap())
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

    #[test]
    fn backfill_jobs_accept_abs_building_approvals_scope() {
        let request = job_run_request(
            &JobKind::Backfill {
                source_id: SourceId::new("abs").unwrap(),
                dataflow_id: Some(DataflowId::new("abs.building_approvals").unwrap()),
            },
            None,
        )
        .expect("build ABS building approvals backfill request");

        assert_eq!(request.source_id.as_str(), "abs");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("abs.building_approvals").unwrap())
        );
    }

    #[test]
    fn backfill_jobs_accept_abs_building_activity_scope() {
        let request = job_run_request(
            &JobKind::Backfill {
                source_id: SourceId::new("abs").unwrap(),
                dataflow_id: Some(DataflowId::new("abs.building_activity").unwrap()),
            },
            None,
        )
        .expect("build ABS building activity backfill request");

        assert_eq!(request.source_id.as_str(), "abs");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("abs.building_activity").unwrap())
        );
    }

    #[test]
    fn backfill_jobs_accept_abs_dwelling_completion_times_scope() {
        let request = job_run_request(
            &JobKind::Backfill {
                source_id: SourceId::new("abs").unwrap(),
                dataflow_id: Some(DataflowId::new("abs.dwelling_completion_times").unwrap()),
            },
            None,
        )
        .expect("build ABS dwelling completion times backfill request");

        assert_eq!(request.source_id.as_str(), "abs");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("abs.dwelling_completion_times").unwrap())
        );
    }

    #[test]
    fn backfill_jobs_accept_pc_productivity_bulletin_scope() {
        let request = job_run_request(
            &JobKind::Backfill {
                source_id: SourceId::new("pc").unwrap(),
                dataflow_id: Some(DataflowId::new("pc.productivity_bulletin").unwrap()),
            },
            None,
        )
        .expect("build PC backfill request");

        assert_eq!(request.source_id.as_str(), "pc");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("pc.productivity_bulletin").unwrap())
        );
    }

    #[test]
    fn backfill_jobs_accept_nhsac_housing_accord_scope() {
        let request = job_run_request(
            &JobKind::Backfill {
                source_id: SourceId::new("nhsac").unwrap(),
                dataflow_id: Some(DataflowId::new("nhsac.housing_accord_progress").unwrap()),
            },
            None,
        )
        .expect("build NHSAC backfill request");

        assert_eq!(request.source_id.as_str(), "nhsac");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("nhsac.housing_accord_progress").unwrap())
        );
    }

    #[test]
    fn backfill_jobs_accept_worldbank_bready_scope() {
        let request = job_run_request(
            &JobKind::Backfill {
                source_id: SourceId::new("worldbank").unwrap(),
                dataflow_id: Some(DataflowId::new("worldbank.bready").unwrap()),
            },
            None,
        )
        .expect("build World Bank backfill request");

        assert_eq!(request.source_id.as_str(), "worldbank");
        assert_eq!(
            request.dataflow_id.as_ref(),
            Some(&DataflowId::new("worldbank.bready").unwrap())
        );
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
