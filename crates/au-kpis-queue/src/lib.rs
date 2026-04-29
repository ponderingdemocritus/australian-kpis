//! Postgres-backed queue abstraction for ingestion jobs.
//!
//! This crate owns the queue-facing contract used by schedulers and
//! ingestion workers. The implementation is intentionally small and
//! SQL-visible: jobs are leased transactionally with `FOR UPDATE SKIP
//! LOCKED`, retries are persisted, and terminal failures are copied to
//! a dead-letter table for operator review.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{fmt, str::FromStr, time::Duration};

use async_trait::async_trait;
use au_kpis_domain::{DataflowId, SourceId};
use au_kpis_error::{Classify, CoreError, ErrorClass};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use thiserror::Error;

/// Result alias for queue operations.
pub type QueueResult<T> = Result<T, QueueError>;

/// Durable queue stage. Each variant maps to one logical ingestion queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueStage {
    /// Adapter discovery jobs.
    Discover,
    /// Artifact fetch jobs.
    Fetch,
    /// Artifact parse jobs.
    Parse,
    /// Observation load jobs.
    Load,
    /// Historical backfill jobs.
    Backfill,
}

impl QueueStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Discover => "discover",
            Self::Fetch => "fetch",
            Self::Parse => "parse",
            Self::Load => "load",
            Self::Backfill => "backfill",
        }
    }
}

impl fmt::Display for QueueStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for QueueStage {
    type Err = QueueError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "discover" => Ok(Self::Discover),
            "fetch" => Ok(Self::Fetch),
            "parse" => Ok(Self::Parse),
            "load" => Ok(Self::Load),
            "backfill" => Ok(Self::Backfill),
            other => Err(QueueError::Validation(format!(
                "unknown queue stage `{other}`"
            ))),
        }
    }
}

/// Identifier for a persisted queue job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JobId(i64);

impl JobId {
    /// Construct from the database identity value.
    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Return the database identity value.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Worker identifier recorded on leases.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerId(String);

impl WorkerId {
    /// Construct a worker id.
    pub fn new(value: impl Into<String>) -> QueueResult<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(QueueError::Validation(
                "worker id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }

    /// Borrow the worker id string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Queue job payload variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JobKind {
    /// Ask an adapter to discover upstream work.
    Discover {
        /// Source to discover.
        source_id: SourceId,
    },
    /// Fetch a source-local discovered job.
    Fetch {
        /// Source-local discovered job id.
        job_id: String,
        /// Source that emitted the job.
        source_id: SourceId,
    },
    /// Parse a stored artifact.
    Parse {
        /// Source that produced the artifact.
        source_id: SourceId,
        /// Content-addressed artifact id or hex digest.
        artifact_id: String,
        /// Object-storage key for the artifact.
        storage_key: String,
    },
    /// Load parsed observations for a dataflow/artifact pair.
    Load {
        /// Dataflow being loaded.
        dataflow_id: DataflowId,
        /// Artifact being loaded.
        artifact_id: String,
    },
    /// Historical backfill trigger.
    Backfill {
        /// Source to backfill.
        source_id: SourceId,
        /// Optional dataflow scope.
        dataflow_id: Option<DataflowId>,
    },
}

impl JobKind {
    /// Stage that owns this job.
    #[must_use]
    pub const fn stage(&self) -> QueueStage {
        match self {
            Self::Discover { .. } => QueueStage::Discover,
            Self::Fetch { .. } => QueueStage::Fetch,
            Self::Parse { .. } => QueueStage::Parse,
            Self::Load { .. } => QueueStage::Load,
            Self::Backfill { .. } => QueueStage::Backfill,
        }
    }
}

/// Persisted queue job with operational metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    kind: JobKind,
    trace_parent: Option<String>,
    priority: i32,
    max_attempts: i32,
}

impl Job {
    /// Construct a discovery job.
    #[must_use]
    pub fn discover(source_id: SourceId) -> Self {
        Self::new(JobKind::Discover { source_id })
    }

    /// Construct a fetch job.
    #[must_use]
    pub fn fetch(job_id: impl Into<String>, source_id: SourceId) -> Self {
        Self::new(JobKind::Fetch {
            job_id: job_id.into(),
            source_id,
        })
    }

    /// Construct a parse job.
    #[must_use]
    pub fn parse(
        source_id: SourceId,
        artifact_id: impl Into<String>,
        storage_key: impl Into<String>,
    ) -> Self {
        Self::new(JobKind::Parse {
            source_id,
            artifact_id: artifact_id.into(),
            storage_key: storage_key.into(),
        })
    }

    /// Construct a load job.
    #[must_use]
    pub fn load(dataflow_id: DataflowId, artifact_id: impl Into<String>) -> Self {
        Self::new(JobKind::Load {
            dataflow_id,
            artifact_id: artifact_id.into(),
        })
    }

    /// Construct a backfill job.
    #[must_use]
    pub fn backfill(source_id: SourceId, dataflow_id: Option<DataflowId>) -> Self {
        Self::new(JobKind::Backfill {
            source_id,
            dataflow_id,
        })
    }

    /// Construct from a concrete kind.
    #[must_use]
    pub fn new(kind: JobKind) -> Self {
        Self {
            kind,
            trace_parent: None,
            priority: 0,
            max_attempts: 5,
        }
    }

    /// Borrow the job kind.
    #[must_use]
    pub const fn kind(&self) -> &JobKind {
        &self.kind
    }

    /// Owning stage.
    #[must_use]
    pub const fn stage(&self) -> QueueStage {
        self.kind.stage()
    }

    /// Optional W3C trace context propagated across queue hops.
    #[must_use]
    pub fn trace_parent(&self) -> Option<&str> {
        self.trace_parent.as_deref()
    }

    /// Queue priority. Higher values are leased first.
    #[must_use]
    pub const fn priority(&self) -> i32 {
        self.priority
    }

    /// Maximum attempts before dead-lettering.
    #[must_use]
    pub const fn max_attempts(&self) -> i32 {
        self.max_attempts
    }

    /// Add a W3C trace context to the job.
    #[must_use]
    pub fn with_trace_parent(mut self, trace_parent: impl Into<String>) -> Self {
        self.trace_parent = Some(trace_parent.into());
        self
    }

    /// Set queue priority.
    #[must_use]
    pub const fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Set maximum attempts. Values less than one are clamped to one.
    #[must_use]
    pub fn with_max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    fn job_group_key(&self) -> Option<String> {
        match &self.kind {
            JobKind::Load { dataflow_id, .. } => Some(format!("load:{dataflow_id}")),
            _ => None,
        }
    }
}

/// A leased job returned by [`Queue::pop`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeasedJob {
    id: JobId,
    job: Job,
    worker_id: WorkerId,
    attempts: i32,
    leased_at: DateTime<Utc>,
}

impl LeasedJob {
    /// Persisted job id.
    #[must_use]
    pub const fn id(&self) -> JobId {
        self.id
    }

    /// Borrow the payload and metadata.
    #[must_use]
    pub const fn job(&self) -> &Job {
        &self.job
    }

    /// Worker that owns this lease.
    #[must_use]
    pub const fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    /// Attempt number for this lease.
    #[must_use]
    pub const fn attempts(&self) -> i32 {
        self.attempts
    }

    /// Lease timestamp.
    #[must_use]
    pub const fn leased_at(&self) -> DateTime<Utc> {
        self.leased_at
    }

    /// Trace context copied from the job.
    #[must_use]
    pub fn trace_parent(&self) -> Option<&str> {
        self.job.trace_parent()
    }
}

/// Nack policy supplied when a worker cannot complete a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nack {
    error_class: ErrorClass,
    message: String,
    retry_after: Option<Duration>,
}

impl Nack {
    /// Construct a nack reason.
    #[must_use]
    pub fn new(error_class: ErrorClass, message: impl Into<String>) -> Self {
        Self {
            error_class,
            message: message.into(),
            retry_after: None,
        }
    }

    /// Override the default exponential backoff delay.
    #[must_use]
    pub const fn with_retry_after(mut self, retry_after: Duration) -> Self {
        self.retry_after = Some(retry_after);
        self
    }
}

/// Dead-lettered job snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetteredJob {
    id: JobId,
    job: Job,
    attempts: i32,
    error_class: String,
    error_message: String,
    failed_at: DateTime<Utc>,
}

impl DeadLetteredJob {
    /// Original job id.
    #[must_use]
    pub const fn id(&self) -> JobId {
        self.id
    }

    /// Original job payload.
    #[must_use]
    pub const fn job(&self) -> &Job {
        &self.job
    }

    /// Attempts consumed before dead-lettering.
    #[must_use]
    pub const fn attempts(&self) -> i32 {
        self.attempts
    }

    /// Error class string persisted for review.
    #[must_use]
    pub fn error_class(&self) -> &str {
        &self.error_class
    }

    /// Terminal error message.
    #[must_use]
    pub fn error_message(&self) -> &str {
        &self.error_message
    }

    /// Dead-letter timestamp.
    #[must_use]
    pub const fn failed_at(&self) -> DateTime<Utc> {
        self.failed_at
    }
}

/// Cron registration persisted for schedulers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronSchedule {
    id: String,
    cron_expression: String,
    job: Job,
    enabled: bool,
}

impl CronSchedule {
    /// Construct a schedule registration.
    pub fn new(
        id: impl Into<String>,
        cron_expression: impl Into<String>,
        job: Job,
    ) -> QueueResult<Self> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(QueueError::Validation(
                "schedule id must not be empty".to_string(),
            ));
        }
        let cron_expression = cron_expression.into();
        if cron_expression.trim().is_empty() {
            return Err(QueueError::Validation(
                "cron expression must not be empty".to_string(),
            ));
        }
        Ok(Self {
            id,
            cron_expression,
            job,
            enabled: true,
        })
    }

    /// Schedule id.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Cron expression string.
    #[must_use]
    pub fn cron_expression(&self) -> &str {
        &self.cron_expression
    }

    /// Job emitted by this schedule.
    #[must_use]
    pub const fn job(&self) -> &Job {
        &self.job
    }

    /// Whether the schedule is enabled.
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    /// Return a copy with a new cron expression.
    pub fn with_cron_expression(mut self, cron_expression: impl Into<String>) -> QueueResult<Self> {
        let cron_expression = cron_expression.into();
        if cron_expression.trim().is_empty() {
            return Err(QueueError::Validation(
                "cron expression must not be empty".to_string(),
            ));
        }
        self.cron_expression = cron_expression;
        Ok(self)
    }
}

/// Queue operations used by scheduler and ingestion workers.
#[async_trait]
pub trait Queue: fmt::Debug + Send + Sync {
    /// Push a job into its owning stage.
    async fn push(&self, job: Job) -> QueueResult<JobId>;

    /// Lease the next ready job for a stage.
    async fn pop(&self, stage: QueueStage, worker_id: WorkerId) -> QueueResult<Option<LeasedJob>>;

    /// Mark a leased job complete.
    async fn ack(&self, job: &LeasedJob) -> QueueResult<()>;

    /// Release, retry, or dead-letter a leased job.
    async fn nack(&self, job: &LeasedJob, nack: Nack) -> QueueResult<()>;

    /// Register or update a cron schedule.
    async fn schedule(&self, schedule: CronSchedule) -> QueueResult<()>;
}

/// Postgres-backed queue implementation.
#[derive(Debug, Clone)]
pub struct ApalisPgQueue {
    pool: PgPool,
}

impl ApalisPgQueue {
    /// Construct a queue from a configured Postgres pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Borrow the underlying pool.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Fetch a dead-lettered job by original job id.
    #[tracing::instrument(skip(self))]
    pub async fn dead_lettered(&self, id: JobId) -> QueueResult<DeadLetteredJob> {
        let row = sqlx::query(
            "SELECT job_id, payload, attempts, error_class, error_message, failed_at
             FROM queue_dead_letters
             WHERE job_id = $1",
        )
        .bind(id.get())
        .fetch_one(&self.pool)
        .await
        .map_err(QueueError::Db)?;

        Ok(DeadLetteredJob {
            id,
            job: serde_json::from_value(row.get("payload"))?,
            attempts: row.get("attempts"),
            error_class: row.get("error_class"),
            error_message: row.get("error_message"),
            failed_at: row.get("failed_at"),
        })
    }

    /// Fetch a schedule by id.
    #[tracing::instrument(skip(self))]
    pub async fn schedule_by_id(&self, id: &str) -> QueueResult<Option<CronSchedule>> {
        let row = sqlx::query(
            "SELECT id, cron_expression, payload, enabled
             FROM queue_cron_schedules
             WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(QueueError::Db)?;

        row.map(schedule_from_row).transpose()
    }
}

#[async_trait]
impl Queue for ApalisPgQueue {
    #[tracing::instrument(skip(self, job), fields(stage = %job.stage()))]
    async fn push(&self, job: Job) -> QueueResult<JobId> {
        let payload = serde_json::to_value(&job)?;
        let row = sqlx::query(
            "INSERT INTO queue_jobs
                (stage, payload, priority, max_attempts, trace_parent, job_group_key)
             VALUES ($1, $2, $3, $4, $5, $6)
             RETURNING id",
        )
        .bind(job.stage().as_str())
        .bind(payload)
        .bind(job.priority())
        .bind(job.max_attempts())
        .bind(job.trace_parent())
        .bind(job.job_group_key())
        .fetch_one(&self.pool)
        .await
        .map_err(QueueError::Db)?;

        Ok(JobId::new(row.get("id")))
    }

    #[tracing::instrument(skip(self, worker_id), fields(stage = %stage, worker = worker_id.as_str()))]
    async fn pop(&self, stage: QueueStage, worker_id: WorkerId) -> QueueResult<Option<LeasedJob>> {
        let row = sqlx::query(
            "WITH candidate AS (
                 SELECT q.id
                 FROM queue_jobs q
                 WHERE q.stage = $1
                   AND q.status = 'pending'
                   AND q.run_at <= now()
                   AND (
                       q.job_group_key IS NULL
                       OR NOT EXISTS (
                           SELECT 1
                           FROM queue_jobs running
                           WHERE running.status = 'running'
                             AND running.job_group_key = q.job_group_key
                       )
                   )
                 ORDER BY q.priority DESC, q.run_at ASC, q.id ASC
                 LIMIT 1
                 FOR UPDATE SKIP LOCKED
             )
             UPDATE queue_jobs q
             SET status = 'running',
                 locked_by = $2,
                 locked_at = now(),
                 attempts = attempts + 1,
                 updated_at = now()
             FROM candidate
             WHERE q.id = candidate.id
             RETURNING q.id, q.payload, q.attempts, q.locked_at",
        )
        .bind(stage.as_str())
        .bind(worker_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(QueueError::Db)?;

        row.map(|row| leased_from_row(row, worker_id)).transpose()
    }

    #[tracing::instrument(skip(self, job), fields(job_id = %job.id()))]
    async fn ack(&self, job: &LeasedJob) -> QueueResult<()> {
        let result = sqlx::query(
            "UPDATE queue_jobs
             SET status = 'completed',
                 locked_by = NULL,
                 locked_at = NULL,
                 updated_at = now()
             WHERE id = $1
               AND status = 'running'
               AND locked_by = $2",
        )
        .bind(job.id().get())
        .bind(job.worker_id().as_str())
        .execute(&self.pool)
        .await
        .map_err(QueueError::Db)?;

        if result.rows_affected() == 0 {
            return Err(QueueError::LeaseLost(job.id()));
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, job, nack), fields(job_id = %job.id()))]
    async fn nack(&self, job: &LeasedJob, nack: Nack) -> QueueResult<()> {
        if !nack.error_class.is_retryable() || job.attempts() >= job.job().max_attempts() {
            dead_letter(&self.pool, job, &nack).await
        } else {
            retry_job(&self.pool, job, &nack).await
        }
    }

    #[tracing::instrument(skip(self, schedule), fields(schedule = schedule.id()))]
    async fn schedule(&self, schedule: CronSchedule) -> QueueResult<()> {
        let payload = serde_json::to_value(schedule.job())?;
        sqlx::query(
            "INSERT INTO queue_cron_schedules
                (id, stage, cron_expression, payload, trace_parent, enabled)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (id) DO UPDATE
             SET stage = EXCLUDED.stage,
                 cron_expression = EXCLUDED.cron_expression,
                 payload = EXCLUDED.payload,
                 trace_parent = EXCLUDED.trace_parent,
                 enabled = EXCLUDED.enabled,
                 updated_at = now()",
        )
        .bind(schedule.id())
        .bind(schedule.job().stage().as_str())
        .bind(schedule.cron_expression())
        .bind(payload)
        .bind(schedule.job().trace_parent())
        .bind(schedule.enabled())
        .execute(&self.pool)
        .await
        .map_err(QueueError::Db)?;
        Ok(())
    }
}

async fn retry_job(pool: &PgPool, job: &LeasedJob, nack: &Nack) -> QueueResult<()> {
    let retry_after_ms = nack
        .retry_after
        .unwrap_or_else(|| default_backoff(job.attempts()))
        .as_millis()
        .min(i64::MAX as u128) as i64;

    let result = sqlx::query(
        "UPDATE queue_jobs
         SET status = 'pending',
             locked_by = NULL,
             locked_at = NULL,
             run_at = now() + ($3 * INTERVAL '1 millisecond'),
             last_error = $4,
             updated_at = now()
         WHERE id = $1
           AND status = 'running'
           AND locked_by = $2",
    )
    .bind(job.id().get())
    .bind(job.worker_id().as_str())
    .bind(retry_after_ms)
    .bind(&nack.message)
    .execute(pool)
    .await
    .map_err(QueueError::Db)?;

    if result.rows_affected() == 0 {
        return Err(QueueError::LeaseLost(job.id()));
    }
    Ok(())
}

async fn dead_letter(pool: &PgPool, job: &LeasedJob, nack: &Nack) -> QueueResult<()> {
    let mut tx = pool.begin().await.map_err(QueueError::Db)?;
    let result = sqlx::query(
        "UPDATE queue_jobs
         SET status = 'dead',
             locked_by = NULL,
             locked_at = NULL,
             last_error = $3,
             updated_at = now()
         WHERE id = $1
           AND status = 'running'
           AND locked_by = $2",
    )
    .bind(job.id().get())
    .bind(job.worker_id().as_str())
    .bind(&nack.message)
    .execute(&mut *tx)
    .await
    .map_err(QueueError::Db)?;

    if result.rows_affected() == 0 {
        return Err(QueueError::LeaseLost(job.id()));
    }

    sqlx::query(
        "INSERT INTO queue_dead_letters
            (job_id, stage, payload, attempts, error_class, error_message, trace_parent)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (job_id) DO UPDATE
         SET attempts = EXCLUDED.attempts,
             error_class = EXCLUDED.error_class,
             error_message = EXCLUDED.error_message,
             trace_parent = EXCLUDED.trace_parent,
             failed_at = now()",
    )
    .bind(job.id().get())
    .bind(job.job().stage().as_str())
    .bind(serde_json::to_value(job.job())?)
    .bind(job.attempts())
    .bind(format!("{:?}", nack.error_class))
    .bind(&nack.message)
    .bind(job.trace_parent())
    .execute(&mut *tx)
    .await
    .map_err(QueueError::Db)?;

    tx.commit().await.map_err(QueueError::Db)?;
    Ok(())
}

fn default_backoff(attempts: i32) -> Duration {
    let shift = attempts.saturating_sub(1).clamp(0, 6) as u32;
    Duration::from_secs(1_u64 << shift)
}

fn leased_from_row(row: sqlx::postgres::PgRow, worker_id: WorkerId) -> QueueResult<LeasedJob> {
    Ok(LeasedJob {
        id: JobId::new(row.get("id")),
        job: serde_json::from_value(row.get("payload"))?,
        worker_id,
        attempts: row.get("attempts"),
        leased_at: row.get("locked_at"),
    })
}

fn schedule_from_row(row: sqlx::postgres::PgRow) -> QueueResult<CronSchedule> {
    let mut schedule = CronSchedule::new(
        row.get::<String, _>("id"),
        row.get::<String, _>("cron_expression"),
        serde_json::from_value(row.get("payload"))?,
    )?;
    schedule.enabled = row.get("enabled");
    Ok(schedule)
}

/// Errors returned by queue operations.
#[derive(Debug, Error)]
pub enum QueueError {
    /// Shared validation, JSON, or I/O failure.
    #[error(transparent)]
    Core(#[from] CoreError),

    /// JSON serialisation/deserialisation failure.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    /// Database operation failure.
    #[error("db: {0}")]
    Db(#[source] sqlx::Error),

    /// Caller supplied invalid data.
    #[error("validation: {0}")]
    Validation(String),

    /// The lease was no longer owned by the supplied worker.
    #[error("queue lease lost for job {0}")]
    LeaseLost(JobId),
}

impl Classify for QueueError {
    fn class(&self) -> ErrorClass {
        match self {
            Self::Core(err) => err.class(),
            Self::Json(_) | Self::Validation(_) | Self::LeaseLost(_) => ErrorClass::Validation,
            Self::Db(_) => ErrorClass::Transient,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_jobs_have_per_dataflow_group_key() {
        let job = Job::load(DataflowId::new("abs.cpi").unwrap(), "artifact-a");
        assert_eq!(job.job_group_key().as_deref(), Some("load:abs.cpi"));
    }

    #[test]
    fn non_load_jobs_do_not_have_group_keys() {
        let job = Job::discover(SourceId::new("abs").unwrap());
        assert!(job.job_group_key().is_none());
    }

    #[test]
    fn worker_id_rejects_blank_values() {
        assert!(WorkerId::new(" ").is_err());
    }

    #[test]
    fn queue_stage_parses_known_values_and_rejects_unknown_values() {
        let cases = [
            ("discover", QueueStage::Discover),
            ("fetch", QueueStage::Fetch),
            ("parse", QueueStage::Parse),
            ("load", QueueStage::Load),
            ("backfill", QueueStage::Backfill),
        ];

        for (raw, expected) in cases {
            let parsed = raw.parse::<QueueStage>().expect("stage should parse");
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), raw);
        }

        let err = "other".parse::<QueueStage>().expect_err("unknown stage");
        assert_eq!(err.class(), ErrorClass::Validation);
    }

    #[test]
    fn job_builders_assign_expected_stage_and_defaults() {
        let source_id = SourceId::new("abs").unwrap();
        let dataflow_id = DataflowId::new("abs.cpi").unwrap();
        let cases = [
            Job::discover(source_id.clone()),
            Job::fetch("job-a", source_id.clone()),
            Job::parse(source_id.clone(), "artifact-a", "raw/abs/a.pdf"),
            Job::load(dataflow_id, "artifact-a"),
            Job::backfill(source_id, None),
        ];

        for job in cases {
            assert_eq!(job.stage(), job.kind().stage());
            assert_eq!(job.priority(), 0);
            assert_eq!(job.max_attempts(), 5);
            assert!(job.trace_parent().is_none());
        }
    }

    #[test]
    fn job_metadata_builders_store_values_and_clamp_attempts() {
        let job = Job::discover(SourceId::new("abs").unwrap())
            .with_priority(10)
            .with_max_attempts(0)
            .with_trace_parent("trace-a");

        assert_eq!(job.priority(), 10);
        assert_eq!(job.max_attempts(), 1);
        assert_eq!(job.trace_parent(), Some("trace-a"));
    }

    #[test]
    fn cron_schedule_validation_covers_empty_fields_and_updates() {
        let job = Job::discover(SourceId::new("abs").unwrap());

        assert!(CronSchedule::new(" ", "0 8 * * *", job.clone()).is_err());
        assert!(CronSchedule::new("abs-cpi", " ", job.clone()).is_err());

        let schedule =
            CronSchedule::new("abs-cpi", "0 8 * * *", job).expect("schedule should be valid");
        assert_eq!(schedule.id(), "abs-cpi");
        assert_eq!(schedule.cron_expression(), "0 8 * * *");
        assert!(schedule.enabled());

        assert!(schedule.clone().with_cron_expression(" ").is_err());
        let updated = schedule
            .with_cron_expression("0 9 * * *")
            .expect("cron expression should update");
        assert_eq!(updated.cron_expression(), "0 9 * * *");
    }

    #[test]
    fn default_backoff_doubles_until_capped() {
        assert_eq!(default_backoff(0), Duration::from_secs(1));
        assert_eq!(default_backoff(1), Duration::from_secs(1));
        assert_eq!(default_backoff(2), Duration::from_secs(2));
        assert_eq!(default_backoff(7), Duration::from_secs(64));
        assert_eq!(default_backoff(99), Duration::from_secs(64));
    }

    #[test]
    fn queue_error_classifies_variants() {
        assert_eq!(
            QueueError::Validation("bad".to_string()).class(),
            ErrorClass::Validation
        );
        assert_eq!(
            QueueError::LeaseLost(JobId::new(42)).class(),
            ErrorClass::Validation
        );
        assert_eq!(
            QueueError::Db(sqlx::Error::RowNotFound).class(),
            ErrorClass::Transient
        );

        let json_err: QueueError = serde_json::from_str::<Job>("not json")
            .expect_err("invalid json")
            .into();
        assert_eq!(json_err.class(), ErrorClass::Validation);
    }
}
