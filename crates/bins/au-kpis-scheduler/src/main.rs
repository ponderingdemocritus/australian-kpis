//! Discovery cron binary.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{
    env,
    ffi::OsString,
    future::IntoFuture,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use au_kpis_config::load_ingestion;
use au_kpis_db::{PgPool, connect as connect_db, migrate};
use au_kpis_domain::{DataflowId, SourceId};
use au_kpis_queue::{ApalisPgQueue, CronSchedule, Job, Queue};
use au_kpis_scheduler::data_quality::{
    PagerDutyConfig, PagerDutyOutcome, default_data_quality_rules, notify_pagerduty,
    run_data_quality_checks,
};
use au_kpis_scheduler::source_location_audit::{
    default_source_location_rules, run_source_location_audit,
};
use au_kpis_source_register::{SourceStatus, load_source_register};
use au_kpis_telemetry::{Telemetry, init as init_telemetry};
use axum::{Router, http::header, response::IntoResponse, routing::get};
use chrono::Utc;
use chrono::{DateTime, NaiveDate, NaiveTime};
use chrono_tz::Australia::Sydney;
use clap::{Parser, Subcommand};
use sqlx::{Pool, Postgres, pool::PoolConnection};
use tokio::{net::TcpListener, signal};
use tokio_util::sync::CancellationToken;

const SCHEDULER_LEADER_LOCK_ID: i64 = 30_000_030;
const DEFAULT_TICK_MS: u64 = 1_000;
const DEFAULT_DATA_QUALITY_REPORT_PATH: &str = "target/data-quality/data-quality-report.md";
const DEFAULT_SOURCE_LOCATION_AUDIT_REPORT_PATH: &str =
    "target/source-location-audit/source-location-audit.md";
const DEFAULT_PAGERDUTY_EVENTS_URL: &str = "https://events.pagerduty.com/v2/enqueue";

/// Command-line arguments for `au-kpis-scheduler`.
#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
    /// Scheduler id recorded in logs and metrics.
    #[arg(long, env = "AU_KPIS_SCHEDULER_ID")]
    worker_id: Option<String>,

    /// Leader-election retry and schedule scan interval.
    #[arg(long, env = "AU_KPIS_SCHEDULER_TICK_MS", default_value_t = DEFAULT_TICK_MS)]
    tick_ms: u64,

    #[command(subcommand)]
    command: Option<Command>,
}

/// Scheduler subcommands.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
enum Command {
    /// Start the long-running scheduler loop.
    Run,
    /// Run data-quality checks once and write the daily report artifacts.
    DataQuality {
        /// Markdown report path for the generated daily report.
        #[arg(
            long,
            env = "AU_KPIS_DATA_QUALITY_REPORT_PATH",
            default_value = DEFAULT_DATA_QUALITY_REPORT_PATH
        )]
        report_path: PathBuf,

        /// PagerDuty Events v2 routing key used when anomalies are detected.
        #[arg(long, env = "AU_KPIS_PAGERDUTY_ROUTING_KEY")]
        pagerduty_routing_key: Option<String>,

        /// PagerDuty Events v2 endpoint.
        #[arg(
            long,
            env = "AU_KPIS_PAGERDUTY_EVENTS_URL",
            default_value = DEFAULT_PAGERDUTY_EVENTS_URL
        )]
        pagerduty_events_url: String,
    },
    /// Run source-location checks once and write the weekly report artifacts.
    SourceLocationAudit {
        /// Markdown report path for the generated source-location audit.
        #[arg(
            long,
            env = "AU_KPIS_SOURCE_LOCATION_AUDIT_REPORT_PATH",
            default_value = DEFAULT_SOURCE_LOCATION_AUDIT_REPORT_PATH
        )]
        report_path: PathBuf,
    },
}

#[derive(Debug, Clone)]
struct DiscoverySchedule {
    id: String,
    cron_expression: String,
    timezone: String,
    job: Job,
}

#[derive(Debug, Default)]
struct SchedulerMetrics {
    leader_active: AtomicU64,
    leader_acquired_total: AtomicU64,
    discovery_jobs_emitted_total: AtomicU64,
    scheduler_ticks_total: AtomicU64,
}

impl SchedulerMetrics {
    fn render_prometheus(&self) -> String {
        format!(
            "# HELP au_kpis_scheduler_leader_active Whether this scheduler process currently owns the leader lock.\n\
             # TYPE au_kpis_scheduler_leader_active gauge\n\
             au_kpis_scheduler_leader_active {}\n\
             # HELP au_kpis_scheduler_leader_acquired_total Leader lock acquisitions by this process.\n\
             # TYPE au_kpis_scheduler_leader_acquired_total counter\n\
             au_kpis_scheduler_leader_acquired_total {}\n\
             # HELP au_kpis_scheduler_discovery_jobs_emitted_total Discovery jobs emitted by this process.\n\
             # TYPE au_kpis_scheduler_discovery_jobs_emitted_total counter\n\
             au_kpis_scheduler_discovery_jobs_emitted_total {}\n\
             # HELP au_kpis_scheduler_ticks_total Scheduler loop ticks.\n\
             # TYPE au_kpis_scheduler_ticks_total counter\n\
             au_kpis_scheduler_ticks_total {}\n",
            self.leader_active.load(Ordering::Relaxed),
            self.leader_acquired_total.load(Ordering::Relaxed),
            self.discovery_jobs_emitted_total.load(Ordering::Relaxed),
            self.scheduler_ticks_total.load(Ordering::Relaxed)
        )
    }
}

#[derive(Debug)]
struct Runtime {
    db: PgPool,
    metrics: Arc<SchedulerMetrics>,
    shutdown: CancellationToken,
    tick: Duration,
    schedules: Vec<DiscoverySchedule>,
    worker_id: String,
}

#[derive(Debug)]
struct LeaderGuard {
    conn: PoolConnection<Postgres>,
    metrics: Arc<SchedulerMetrics>,
}

impl LeaderGuard {
    async fn try_acquire(
        pool: &Pool<Postgres>,
        metrics: Arc<SchedulerMetrics>,
    ) -> anyhow::Result<Option<Self>> {
        let mut conn = pool.acquire().await.context("acquire leader connection")?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(SCHEDULER_LEADER_LOCK_ID)
            .fetch_one(&mut *conn)
            .await
            .context("try scheduler leader lock")?;

        if !acquired {
            return Ok(None);
        }

        conn.close_on_drop();
        metrics.leader_active.store(1, Ordering::Relaxed);
        metrics
            .leader_acquired_total
            .fetch_add(1, Ordering::Relaxed);
        Ok(Some(Self { conn, metrics }))
    }

    async fn release(mut self) -> anyhow::Result<()> {
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(SCHEDULER_LEADER_LOCK_ID)
            .execute(&mut *self.conn)
            .await
            .context("unlock scheduler leader lock")?;
        self.metrics.leader_active.store(0, Ordering::Relaxed);
        Ok(())
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        self.metrics.leader_active.store(0, Ordering::Relaxed);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let command = resolve_command(&cli)?;

    if let Command::SourceLocationAudit { report_path } = command {
        return run_source_location_audit_command(&report_path).await;
    }

    let config = Arc::new(load_ingestion(None).context("load config")?);
    let _telemetry = init_or_disabled(&config.telemetry)?;
    let db = connect_db(&config.database)
        .await
        .context("connect postgres database")?;
    migrate(&db).await.context("apply database migrations")?;

    if let Command::DataQuality {
        report_path,
        pagerduty_routing_key,
        pagerduty_events_url,
    } = command
    {
        return run_data_quality_command(
            &db,
            &report_path,
            pagerduty_routing_key,
            pagerduty_events_url,
        )
        .await;
    }

    let shutdown = CancellationToken::new();
    let metrics = Arc::new(SchedulerMetrics::default());
    let runtime = Runtime {
        db,
        metrics,
        shutdown: shutdown.clone(),
        tick: Duration::from_millis(cli.tick_ms),
        schedules: default_discovery_schedules()?,
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
    let scheduler = run_scheduler(runtime);
    tokio::pin!(scheduler);

    let drain_window = Duration::from_secs(config.http.shutdown_grace_period_secs);
    tokio::select! {
        result = &mut scheduler => {
            shutdown.cancel();
            result?;
        }
        result = &mut shutdown_listener => {
            result.context("listen for shutdown signal")?;
            match tokio::time::timeout(drain_window, &mut scheduler).await {
                Ok(result) => result?,
                Err(_) => tracing::warn!(
                    drain_window_secs = drain_window.as_secs(),
                    "scheduler drain window elapsed; forcing exit"
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

async fn run_data_quality_command(
    db: &PgPool,
    report_path: &Path,
    pagerduty_routing_key: Option<String>,
    pagerduty_events_url: String,
) -> anyhow::Result<()> {
    let report = run_data_quality_checks(db, default_data_quality_rules(), Utc::now())
        .await
        .context("run data-quality checks")?;
    write_data_quality_reports(report_path, &report)
        .await
        .with_context(|| format!("write data-quality report to {}", report_path.display()))?;

    match notify_pagerduty(
        &report,
        &PagerDutyConfig {
            routing_key: pagerduty_routing_key,
            events_url: pagerduty_events_url,
        },
    )
    .await
    .context("notify PagerDuty for data-quality anomalies")?
    {
        PagerDutyOutcome::NoAnomalies => {
            tracing::info!("data-quality checks passed without anomalies");
        }
        PagerDutyOutcome::MissingRoutingKey => {
            tracing::warn!(
                anomalies = report.anomalies_total(),
                "data-quality anomalies detected, but AU_KPIS_PAGERDUTY_ROUTING_KEY is unset"
            );
            anyhow::bail!(
                "data-quality anomalies detected but AU_KPIS_PAGERDUTY_ROUTING_KEY is unset"
            );
        }
        PagerDutyOutcome::Sent => {
            tracing::warn!(
                anomalies = report.anomalies_total(),
                "data-quality anomalies sent to PagerDuty"
            );
        }
    }

    Ok(())
}

async fn run_source_location_audit_command(report_path: &Path) -> anyhow::Result<()> {
    let rules = default_source_location_rules().context("load source-location audit rules")?;
    let report = run_source_location_audit(&rules, Utc::now())
        .await
        .context("run source-location audit")?;
    write_source_location_audit_reports(report_path, &report)
        .await
        .with_context(|| {
            format!(
                "write source-location audit report to {}",
                report_path.display()
            )
        })?;

    if report.has_findings() {
        tracing::warn!(
            findings = report.findings_total,
            status = ?report.status,
            "source-location audit produced findings"
        );
    } else {
        tracing::info!("source-location audit passed without findings");
    }

    Ok(())
}

async fn write_data_quality_reports(
    report_path: &Path,
    report: &au_kpis_scheduler::data_quality::DataQualityReport,
) -> anyhow::Result<()> {
    if let Some(parent) = report_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
    }

    tokio::fs::write(report_path, report.render_markdown())
        .await
        .with_context(|| format!("write {}", report_path.display()))?;
    let json_path = report_path.with_extension("json");
    let json = serde_json::to_vec_pretty(report).context("serialize data-quality report JSON")?;
    tokio::fs::write(&json_path, json)
        .await
        .with_context(|| format!("write {}", json_path.display()))?;

    Ok(())
}

async fn write_source_location_audit_reports(
    report_path: &Path,
    report: &au_kpis_scheduler::source_location_audit::SourceLocationAuditReport,
) -> anyhow::Result<()> {
    if let Some(parent) = report_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
    }

    tokio::fs::write(report_path, report.render_markdown())
        .await
        .with_context(|| format!("write {}", report_path.display()))?;
    let json_path = report_path.with_extension("json");
    let json =
        serde_json::to_vec_pretty(report).context("serialize source-location audit report JSON")?;
    tokio::fs::write(&json_path, json)
        .await
        .with_context(|| format!("write {}", json_path.display()))?;

    Ok(())
}

async fn run_scheduler(runtime: Runtime) -> anyhow::Result<()> {
    let queue = ApalisPgQueue::new(runtime.db.clone());
    register_schedules(&queue, &runtime.schedules).await?;

    loop {
        runtime
            .metrics
            .scheduler_ticks_total
            .fetch_add(1, Ordering::Relaxed);
        if runtime.shutdown.is_cancelled() {
            return Ok(());
        }

        if let Some(leader) =
            LeaderGuard::try_acquire(&runtime.db, Arc::clone(&runtime.metrics)).await?
        {
            tracing::info!(scheduler = runtime.worker_id, "scheduler leader acquired");
            run_leader_loop(&runtime, &queue).await?;
            leader.release().await?;
            return Ok(());
        }

        tokio::select! {
            () = runtime.shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(runtime.tick) => {}
        }
    }
}

async fn run_leader_loop(runtime: &Runtime, queue: &ApalisPgQueue) -> anyhow::Result<()> {
    let mut materialized_through = None;
    loop {
        runtime
            .metrics
            .scheduler_ticks_total
            .fetch_add(1, Ordering::Relaxed);
        let emitted = queue
            .enqueue_due_schedules(Utc::now(), 100)
            .await
            .context("enqueue due schedule occurrences")?;
        runtime
            .metrics
            .discovery_jobs_emitted_total
            .fetch_add(emitted.len() as u64, Ordering::Relaxed);
        let due_snapshot_date = due_aps_snapshot_date(Utc::now());
        if materialized_through != Some(due_snapshot_date) {
            let snapshot =
                au_kpis_scorecard::materialize_aps_snapshot(&runtime.db, due_snapshot_date, None)
                    .await
                    .context("materialize daily APS snapshot")?;
            tracing::info!(
                snapshot_id = %snapshot.id,
                snapshot_date = %snapshot.snapshot_date,
                revision = snapshot.revision,
                publication_state = ?snapshot.publication_state,
                "daily APS snapshot materialized"
            );
            materialized_through = Some(due_snapshot_date);
        }

        tokio::select! {
            () = runtime.shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(runtime.tick) => {}
        }
    }
}

fn due_aps_snapshot_date(now: DateTime<Utc>) -> NaiveDate {
    let local = now.with_timezone(&Sydney);
    let days_back = if local.time() >= NaiveTime::from_hms_opt(0, 15, 0).expect("valid APS time") {
        1
    } else {
        2
    };
    local
        .date_naive()
        .checked_sub_days(chrono::Days::new(days_back))
        .expect("current date supports APS lookback")
}

async fn register_schedules(
    queue: &ApalisPgQueue,
    schedules: &[DiscoverySchedule],
) -> anyhow::Result<()> {
    for schedule in schedules {
        queue
            .schedule(
                CronSchedule::new(
                    &schedule.id,
                    &schedule.cron_expression,
                    schedule.job.clone(),
                )?
                .with_timezone(&schedule.timezone)?,
            )
            .await
            .with_context(|| format!("register {} cron schedule", schedule.id))?;
    }
    Ok(())
}

fn default_discovery_schedules() -> anyhow::Result<Vec<DiscoverySchedule>> {
    let register = load_source_register().context("load source register schedules")?;
    register
        .dataflows
        .into_iter()
        .filter(|dataflow| dataflow.status == SourceStatus::Active)
        .map(|dataflow| {
            let schedule = dataflow.schedule.ok_or_else(|| {
                anyhow::anyhow!(
                    "active dataflow `{}` has no discovery schedule",
                    dataflow.dataflow_id
                )
            })?;
            let source_id = SourceId::new(dataflow.source_id)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            let dataflow_id = DataflowId::new(dataflow.dataflow_id)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            let id = format!("discovery:{dataflow_id}");
            let job = Job::discover_dataflow(source_id, dataflow_id);
            CronSchedule::new(&id, &schedule.cron, job.clone())?
                .with_timezone(&schedule.timezone)?;
            Ok(DiscoverySchedule {
                id,
                cron_expression: schedule.cron,
                timezone: schedule.timezone,
                job,
            })
        })
        .collect()
}

async fn serve_metrics(
    listener: TcpListener,
    metrics: Arc<SchedulerMetrics>,
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
        .into_future()
        .await
        .context("serve scheduler metrics")
}

fn resolve_command(cli: &Cli) -> anyhow::Result<Command> {
    match &cli.command {
        Some(command) => Ok(command.clone()),
        None => anyhow::bail!("choose `run` to start the scheduler"),
    }
}

fn init_or_disabled(config: &au_kpis_config::TelemetryConfig) -> anyhow::Result<Telemetry> {
    match init_telemetry(config) {
        Ok(telemetry) => Ok(telemetry),
        Err(err) if err.to_string() == "global telemetry subscriber already installed" => {
            Ok(Telemetry::disabled())
        }
        Err(err) => Err(err).context("initialize telemetry"),
    }
}

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

async fn write_startup_notify(listener: &TcpListener) -> anyhow::Result<()> {
    write_startup_notify_path(listener, env::var_os("AU_KPIS_STARTUP_NOTIFY_FILE")).await
}

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

fn default_worker_id() -> String {
    format!("au-kpis-scheduler-{}", std::process::id())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use au_kpis_queue::JobKind;
    use au_kpis_scheduler::data_quality::DataQualityReport;
    use au_kpis_scheduler::source_location_audit::{
        SourceAuditFinding, SourceAuditSeverity, SourceAuditStatus, SourceLocationAuditReport,
    };
    use chrono::{TimeZone, Utc};

    use super::*;

    #[test]
    fn default_schedule_registers_adapter_discovery_cadences() {
        let schedules = default_discovery_schedules().expect("load schedules");

        assert_eq!(schedules.len(), 20);
        let abs_cpi = schedules
            .iter()
            .find(|schedule| schedule.id == "discovery:abs.cpi")
            .expect("ABS CPI schedule");
        assert_eq!(abs_cpi.cron_expression, "15 1 * * *");
        assert_eq!(abs_cpi.timezone, "Australia/Sydney");
        assert!(matches!(
            abs_cpi.job.kind(),
            JobKind::Discover {
                source_id,
                dataflow_id: Some(dataflow_id)
            } if source_id.as_str() == "abs" && dataflow_id.as_str() == "abs.cpi"
        ));
    }

    #[test]
    fn aps_daily_cutoff_uses_sydney_calendar_across_dst() {
        let before_cutoff = Utc.with_ymd_and_hms(2026, 10, 4, 13, 14, 59).unwrap();
        let after_cutoff = Utc.with_ymd_and_hms(2026, 10, 4, 13, 15, 0).unwrap();
        assert_eq!(
            due_aps_snapshot_date(before_cutoff),
            NaiveDate::from_ymd_opt(2026, 10, 3).unwrap()
        );
        assert_eq!(
            due_aps_snapshot_date(after_cutoff),
            NaiveDate::from_ymd_opt(2026, 10, 4).unwrap()
        );
    }

    #[test]
    fn metrics_render_prometheus_leader_and_emission_state() {
        let metrics = SchedulerMetrics::default();
        metrics.leader_active.store(1, Ordering::Relaxed);
        metrics
            .discovery_jobs_emitted_total
            .store(3, Ordering::Relaxed);

        let body = metrics.render_prometheus();

        assert!(body.contains("# TYPE au_kpis_scheduler_leader_active gauge"));
        assert!(body.contains("au_kpis_scheduler_leader_active 1"));
        assert!(body.contains("au_kpis_scheduler_discovery_jobs_emitted_total 3"));
    }

    #[tokio::test]
    async fn write_data_quality_reports_writes_markdown_and_json() {
        let now = Utc::now();
        let report = DataQualityReport {
            generated_at: now,
            window_start: now - chrono::Duration::days(1),
            window_end: now,
            results: Vec::new(),
        };
        let mut path = std::env::temp_dir();
        path.push(format!(
            "au-kpis-data-quality-report-{}-{}.md",
            std::process::id(),
            now.timestamp_nanos_opt().expect("timestamp nanos")
        ));

        write_data_quality_reports(&path, &report)
            .await
            .expect("write reports");

        let markdown = std::fs::read_to_string(&path).expect("read markdown report");
        let json = std::fs::read_to_string(path.with_extension("json")).expect("read json report");
        assert!(markdown.contains("# Data Quality Report"));
        assert!(json.contains("\"results\""));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("json"));
    }

    #[tokio::test]
    async fn write_source_location_audit_reports_writes_markdown_and_json() {
        let now = Utc::now();
        let report = SourceLocationAuditReport {
            generated_at: now,
            register_version: "source-register.v1".into(),
            status: SourceAuditStatus::ManualReview,
            checked_total: 1,
            findings_total: 1,
            results: Vec::new(),
            findings: vec![SourceAuditFinding {
                source_id: "compute".into(),
                dataflow_id: "compute.au_datacentre_capacity_mw".into(),
                severity: SourceAuditSeverity::ManualReview,
                current_url: "https://example.test/compute-capacity".into(),
                latest_url: None,
                evidence: "example.test is a placeholder source.".into(),
                recommendation: "Replace placeholder source.".into(),
            }],
        };
        let mut path = std::env::temp_dir();
        path.push(format!(
            "au-kpis-source-location-audit-{}-{}.md",
            std::process::id(),
            now.timestamp_nanos_opt().expect("timestamp nanos")
        ));

        write_source_location_audit_reports(&path, &report)
            .await
            .expect("write reports");

        let markdown = std::fs::read_to_string(&path).expect("read markdown report");
        let json = std::fs::read_to_string(path.with_extension("json")).expect("read json report");
        assert!(markdown.contains("# Source Location Audit Report"));
        assert!(json.contains("\"findings_total\": 1"));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("json"));
    }
}
