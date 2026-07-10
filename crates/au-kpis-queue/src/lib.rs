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
use chrono_tz::Tz;
use croner::Cron;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use thiserror::Error;
use uuid::Uuid;

/// Result alias for queue operations.
pub type QueueResult<T> = Result<T, QueueError>;

/// Default lease timeout before a running job can be reclaimed by another worker.
pub const DEFAULT_LEASE_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const POP_CONFLICT_RETRIES: usize = 8;
const RETRY_BASE: Duration = Duration::from_secs(30);
const RETRY_CAP: Duration = Duration::from_secs(6 * 60 * 60);
const FETCH_RETRY_AFTER_CAP: Duration = Duration::from_secs(24 * 60 * 60);

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
        /// Optional dataflow scope for register-derived schedules.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dataflow_id: Option<DataflowId>,
    },
    /// Fetch a source-local discovered job.
    Fetch {
        /// Durable discovered-work identity.
        discovered_work_id: Uuid,
    },
    /// Parse a stored artifact.
    Parse {
        /// Durable parser generation identity.
        generation_id: Uuid,
    },
    /// Load one fully staged parser generation.
    Load {
        /// Dataflow used to serialize revision assignment.
        dataflow_id: DataflowId,
        /// Durable parser generation identity.
        generation_id: Uuid,
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
        Self::new(JobKind::Discover {
            source_id,
            dataflow_id: None,
        })
    }

    /// Construct a discovery job scoped to one registered dataflow.
    #[must_use]
    pub fn discover_dataflow(source_id: SourceId, dataflow_id: DataflowId) -> Self {
        Self::new(JobKind::Discover {
            source_id,
            dataflow_id: Some(dataflow_id),
        })
    }

    /// Construct a fetch job.
    #[must_use]
    pub fn fetch(discovered_work_id: Uuid) -> Self {
        Self::new(JobKind::Fetch { discovered_work_id })
    }

    /// Construct a parse job.
    #[must_use]
    pub fn parse(generation_id: Uuid) -> Self {
        Self::new(JobKind::Parse { generation_id })
    }

    /// Construct a load job.
    #[must_use]
    pub fn load(dataflow_id: DataflowId, generation_id: Uuid) -> Self {
        Self::new(JobKind::Load {
            dataflow_id,
            generation_id,
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

    fn dedupe_key(&self) -> Option<String> {
        match &self.kind {
            JobKind::Fetch { discovered_work_id } => Some(format!("fetch:{discovered_work_id}")),
            JobKind::Parse { generation_id } => Some(format!("parse:{generation_id}")),
            JobKind::Load { generation_id, .. } => Some(format!("load:{generation_id}")),
            JobKind::Discover { .. } | JobKind::Backfill { .. } => None,
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
    lease_version: i64,
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

    /// Monotonic token identifying the current lease ownership.
    #[must_use]
    pub const fn lease_version(&self) -> i64 {
        self.lease_version
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
    timezone: String,
    job: Job,
    enabled: bool,
    next_run_at: Option<DateTime<Utc>>,
    last_enqueued_at: Option<DateTime<Utc>>,
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
        validate_cron_expression(&cron_expression)?;
        Ok(Self {
            id,
            cron_expression,
            timezone: "UTC".to_string(),
            job,
            enabled: true,
            next_run_at: None,
            last_enqueued_at: None,
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

    /// IANA timezone used to evaluate the cron expression.
    #[must_use]
    pub fn timezone(&self) -> &str {
        &self.timezone
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

    /// Next persisted occurrence time.
    #[must_use]
    pub const fn next_run_at(&self) -> Option<DateTime<Utc>> {
        self.next_run_at
    }

    /// Most recent persisted occurrence time.
    #[must_use]
    pub const fn last_enqueued_at(&self) -> Option<DateTime<Utc>> {
        self.last_enqueued_at
    }

    /// Return a copy with a new cron expression.
    pub fn with_cron_expression(mut self, cron_expression: impl Into<String>) -> QueueResult<Self> {
        let cron_expression = cron_expression.into();
        validate_cron_expression(&cron_expression)?;
        self.cron_expression = cron_expression;
        Ok(self)
    }

    /// Return a copy evaluated in the supplied IANA timezone.
    pub fn with_timezone(mut self, timezone: impl Into<String>) -> QueueResult<Self> {
        let timezone = timezone.into();
        validate_timezone(&timezone)?;
        self.timezone = timezone;
        Ok(self)
    }
}

/// One schedule occurrence atomically paired with its emitted queue job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleOccurrence {
    id: Uuid,
    schedule_id: String,
    scheduled_for: DateTime<Utc>,
    job_id: JobId,
    created_at: DateTime<Utc>,
}

impl ScheduleOccurrence {
    /// Occurrence id.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Owning schedule id.
    #[must_use]
    pub fn schedule_id(&self) -> &str {
        &self.schedule_id
    }

    /// Logical cron instant represented by this occurrence.
    #[must_use]
    pub const fn scheduled_for(&self) -> DateTime<Utc> {
        self.scheduled_for
    }

    /// Queue job created in the same transaction.
    #[must_use]
    pub const fn job_id(&self) -> JobId {
        self.job_id
    }

    /// Database creation timestamp.
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

/// Calculate the next cron occurrence in an IANA timezone and return it in UTC.
pub fn next_schedule_occurrence(
    cron_expression: &str,
    timezone: &str,
    after: DateTime<Utc>,
) -> QueueResult<DateTime<Utc>> {
    let cron = parse_cron(cron_expression)?;
    let timezone = parse_timezone(timezone)?;
    cron.find_next_occurrence(&after.with_timezone(&timezone), false)
        .map(|next| next.with_timezone(&Utc))
        .map_err(|error| QueueError::Validation(format!("cron occurrence: {error}")))
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

    /// Extend a running lease and return the refreshed lease handle.
    async fn renew(&self, job: &LeasedJob) -> QueueResult<LeasedJob>;

    /// Release, retry, or dead-letter a leased job.
    async fn nack(&self, job: &LeasedJob, nack: Nack) -> QueueResult<()>;

    /// Register or update a cron schedule.
    async fn schedule(&self, schedule: CronSchedule) -> QueueResult<()>;

    /// Atomically enqueue currently due schedule occurrences.
    async fn enqueue_due_schedules(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> QueueResult<Vec<ScheduleOccurrence>>;
}

/// Postgres-backed queue implementation.
#[derive(Debug, Clone)]
pub struct ApalisPgQueue {
    pool: PgPool,
    lease_timeout: Duration,
}

impl ApalisPgQueue {
    /// Construct a queue from a configured Postgres pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self {
            pool,
            lease_timeout: DEFAULT_LEASE_TIMEOUT,
        }
    }

    /// Override the lease timeout used to reclaim jobs from crashed workers.
    #[must_use]
    pub const fn with_lease_timeout(mut self, lease_timeout: Duration) -> Self {
        self.lease_timeout = lease_timeout;
        self
    }

    /// Borrow the underlying pool.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Lease timeout used to reclaim jobs from crashed workers.
    #[must_use]
    pub const fn lease_timeout(&self) -> Duration {
        self.lease_timeout
    }

    /// Fetch a dead-lettered job by original job id.
    #[tracing::instrument(skip(self))]
    pub async fn dead_lettered(&self, id: JobId) -> QueueResult<DeadLetteredJob> {
        let row = sqlx::query!(
            r#"SELECT job_id, payload AS "payload!: serde_json::Value", attempts,
                      error_class, error_message, failed_at
             FROM queue_dead_letters
             WHERE job_id = $1"#,
            id.get()
        )
        .fetch_one(&self.pool)
        .await
        .map_err(QueueError::Db)?;

        Ok(DeadLetteredJob {
            id: JobId::new(row.job_id),
            job: serde_json::from_value(row.payload)?,
            attempts: row.attempts,
            error_class: row.error_class,
            error_message: row.error_message,
            failed_at: row.failed_at,
        })
    }

    /// Fetch a schedule by id.
    #[tracing::instrument(skip(self))]
    pub async fn schedule_by_id(&self, id: &str) -> QueueResult<Option<CronSchedule>> {
        let row = sqlx::query(
            r#"SELECT id, cron_expression, timezone, payload, enabled,
                      next_run_at, last_enqueued_at
               FROM queue_cron_schedules
               WHERE id = $1"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(QueueError::Db)?;

        row.map(|row| {
            let mut schedule = CronSchedule::new(
                row.try_get::<String, _>("id").map_err(QueueError::Db)?,
                row.try_get::<String, _>("cron_expression")
                    .map_err(QueueError::Db)?,
                serde_json::from_value(
                    row.try_get::<serde_json::Value, _>("payload")
                        .map_err(QueueError::Db)?,
                )?,
            )?
            .with_timezone(
                row.try_get::<String, _>("timezone")
                    .map_err(QueueError::Db)?,
            )?;
            schedule.enabled = row.try_get("enabled").map_err(QueueError::Db)?;
            schedule.next_run_at = Some(row.try_get("next_run_at").map_err(QueueError::Db)?);
            schedule.last_enqueued_at = row.try_get("last_enqueued_at").map_err(QueueError::Db)?;
            Ok(schedule)
        })
        .transpose()
    }
}

#[async_trait]
impl Queue for ApalisPgQueue {
    #[tracing::instrument(skip(self, job), fields(stage = %job.stage()))]
    async fn push(&self, job: Job) -> QueueResult<JobId> {
        let payload = serde_json::to_value(&job)?;
        let row = sqlx::query(
            r#"INSERT INTO queue_jobs
                (stage, payload, priority, max_attempts, trace_parent, job_group_key, dedupe_key)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (dedupe_key)
                 WHERE dedupe_key IS NOT NULL AND status IN ('pending', 'running')
             DO UPDATE SET updated_at = queue_jobs.updated_at
             RETURNING id"#,
        )
        .bind(job.stage().as_str())
        .bind(payload)
        .bind(job.priority())
        .bind(job.max_attempts())
        .bind(job.trace_parent())
        .bind(job.job_group_key())
        .bind(job.dedupe_key())
        .fetch_one(&self.pool)
        .await
        .map_err(QueueError::Db)?;

        Ok(JobId::new(row.try_get("id").map_err(QueueError::Db)?))
    }

    #[tracing::instrument(skip(self, worker_id), fields(stage = %stage, worker = worker_id.as_str()))]
    async fn pop(&self, stage: QueueStage, worker_id: WorkerId) -> QueueResult<Option<LeasedJob>> {
        for attempt in 0..POP_CONFLICT_RETRIES {
            let row = match sqlx::query!(
                r#"WITH candidate AS (
                     SELECT q.id
                     FROM queue_jobs q
                     WHERE q.stage = $1
                       AND (
                           (q.status = 'pending' AND q.run_at <= now())
                           OR (
                               q.status = 'running'
                               AND q.locked_at <= now() - ($3::BIGINT * INTERVAL '1 millisecond')
                           )
                       )
                       AND (
                           q.job_group_key IS NULL
                           OR pg_try_advisory_xact_lock(hashtext(q.job_group_key))
                       )
                       AND (
                           q.job_group_key IS NULL
                           OR q.status = 'running'
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
                     lease_version = lease_version + 1,
                     attempts = attempts + 1,
                     updated_at = now()
                 FROM candidate
                 WHERE q.id = candidate.id
                 RETURNING q.id,
                           q.payload AS "payload!: serde_json::Value",
                           q.attempts,
                           q.locked_at AS "locked_at!: DateTime<Utc>",
                           q.lease_version"#,
                stage.as_str(),
                worker_id.as_str(),
                duration_millis_i64(self.lease_timeout)
            )
            .fetch_optional(&self.pool)
            .await
            {
                Ok(row) => row,
                Err(err) if is_unique_violation(&err) && attempt + 1 < POP_CONFLICT_RETRIES => {
                    continue;
                }
                Err(err) => return Err(QueueError::Db(err)),
            };

            return row
                .map(|row| {
                    leased_from_parts(
                        row.id,
                        row.payload,
                        row.attempts,
                        row.locked_at,
                        row.lease_version,
                        worker_id,
                    )
                })
                .transpose();
        }

        Ok(None)
    }

    #[tracing::instrument(skip(self, job), fields(job_id = %job.id()))]
    async fn ack(&self, job: &LeasedJob) -> QueueResult<()> {
        let mut tx = self.pool.begin().await.map_err(QueueError::Db)?;
        let result = sqlx::query(
            r#"UPDATE queue_jobs
             SET status = 'completed',
                 locked_by = NULL,
                 locked_at = NULL,
                 updated_at = now()
             WHERE id = $1
               AND status = 'running'
               AND locked_by = $2
               AND lease_version = $3"#,
        )
        .bind(job.id().get())
        .bind(job.worker_id().as_str())
        .bind(job.lease_version())
        .execute(&mut *tx)
        .await
        .map_err(QueueError::Db)?;

        if result.rows_affected() == 0 {
            return Err(QueueError::LeaseLost(job.id()));
        }
        sqlx::query(
            "UPDATE queue_schedule_occurrences
             SET status = 'completed'
             WHERE job_id = $1",
        )
        .bind(job.id().get())
        .execute(&mut *tx)
        .await
        .map_err(QueueError::Db)?;
        tx.commit().await.map_err(QueueError::Db)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, job), fields(job_id = %job.id()))]
    async fn renew(&self, job: &LeasedJob) -> QueueResult<LeasedJob> {
        let row = sqlx::query!(
            r#"UPDATE queue_jobs
             SET locked_at = now(),
                 lease_version = lease_version + 1,
                 updated_at = now()
             WHERE id = $1
               AND status = 'running'
               AND locked_by = $2
               AND lease_version = $3
             RETURNING id,
                       payload AS "payload!: serde_json::Value",
                       attempts,
                       locked_at AS "locked_at!: DateTime<Utc>",
                       lease_version"#,
            job.id().get(),
            job.worker_id().as_str(),
            job.lease_version()
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(QueueError::Db)?;

        row.map(|row| {
            leased_from_parts(
                row.id,
                row.payload,
                row.attempts,
                row.locked_at,
                row.lease_version,
                job.worker_id().clone(),
            )
        })
        .transpose()?
        .ok_or(QueueError::LeaseLost(job.id()))
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
        let next_run_at =
            next_schedule_occurrence(schedule.cron_expression(), schedule.timezone(), Utc::now())?;
        sqlx::query(
            r#"INSERT INTO queue_cron_schedules
                (id, stage, cron_expression, timezone, payload, trace_parent,
                 enabled, next_run_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (id) DO UPDATE
             SET stage = EXCLUDED.stage,
                 cron_expression = EXCLUDED.cron_expression,
                 timezone = EXCLUDED.timezone,
                 payload = EXCLUDED.payload,
                 trace_parent = EXCLUDED.trace_parent,
                 enabled = EXCLUDED.enabled,
                 next_run_at = CASE
                     WHEN queue_cron_schedules.cron_expression <> EXCLUDED.cron_expression
                       OR queue_cron_schedules.timezone <> EXCLUDED.timezone
                       OR queue_cron_schedules.enabled <> EXCLUDED.enabled
                     THEN EXCLUDED.next_run_at
                     ELSE queue_cron_schedules.next_run_at
                 END,
                 updated_at = now()"#,
        )
        .bind(schedule.id())
        .bind(schedule.job().stage().as_str())
        .bind(schedule.cron_expression())
        .bind(schedule.timezone())
        .bind(payload)
        .bind(schedule.job().trace_parent())
        .bind(schedule.enabled())
        .bind(next_run_at)
        .execute(&self.pool)
        .await
        .map_err(QueueError::Db)?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn enqueue_due_schedules(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> QueueResult<Vec<ScheduleOccurrence>> {
        if limit == 0 {
            return Err(QueueError::Validation(
                "due schedule limit must be positive".to_string(),
            ));
        }
        let limit = i64::from(limit);
        let mut tx = self.pool.begin().await.map_err(QueueError::Db)?;
        let rows = sqlx::query(
            r#"SELECT id, cron_expression, timezone, payload, next_run_at
               FROM queue_cron_schedules
               WHERE enabled
                 AND next_run_at <= $1
               ORDER BY next_run_at, id
               LIMIT $2
               FOR UPDATE SKIP LOCKED"#,
        )
        .bind(now)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await
        .map_err(QueueError::Db)?;

        let mut occurrences = Vec::with_capacity(rows.len());
        for row in rows {
            let schedule_id: String = row.try_get("id").map_err(QueueError::Db)?;
            let cron_expression: String = row.try_get("cron_expression").map_err(QueueError::Db)?;
            let timezone: String = row.try_get("timezone").map_err(QueueError::Db)?;
            let payload: serde_json::Value = row.try_get("payload").map_err(QueueError::Db)?;
            let scheduled_for: DateTime<Utc> =
                row.try_get("next_run_at").map_err(QueueError::Db)?;
            let job: Job = serde_json::from_value(payload.clone())?;
            let next_run_at = next_schedule_occurrence(&cron_expression, &timezone, scheduled_for)?;

            let job_id: i64 = sqlx::query_scalar(
                r#"INSERT INTO queue_jobs
                    (stage, payload, priority, max_attempts, trace_parent, job_group_key)
                   VALUES ($1, $2, $3, $4, $5, $6)
                   RETURNING id"#,
            )
            .bind(job.stage().as_str())
            .bind(payload)
            .bind(job.priority())
            .bind(job.max_attempts())
            .bind(job.trace_parent())
            .bind(job.job_group_key())
            .fetch_one(&mut *tx)
            .await
            .map_err(QueueError::Db)?;

            let occurrence = sqlx::query(
                r#"INSERT INTO queue_schedule_occurrences
                    (schedule_id, scheduled_for, job_id)
                   VALUES ($1, $2, $3)
                   RETURNING id, created_at"#,
            )
            .bind(&schedule_id)
            .bind(scheduled_for)
            .bind(job_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(QueueError::Db)?;

            let updated = sqlx::query(
                r#"UPDATE queue_cron_schedules
                   SET last_enqueued_at = $2,
                       next_run_at = $3,
                       updated_at = now()
                   WHERE id = $1
                     AND next_run_at = $2"#,
            )
            .bind(&schedule_id)
            .bind(scheduled_for)
            .bind(next_run_at)
            .execute(&mut *tx)
            .await
            .map_err(QueueError::Db)?;
            if updated.rows_affected() != 1 {
                return Err(QueueError::Validation(format!(
                    "schedule `{schedule_id}` lost its due-time fence"
                )));
            }

            occurrences.push(ScheduleOccurrence {
                id: occurrence.try_get("id").map_err(QueueError::Db)?,
                schedule_id,
                scheduled_for,
                job_id: JobId::new(job_id),
                created_at: occurrence.try_get("created_at").map_err(QueueError::Db)?,
            });
        }

        tx.commit().await.map_err(QueueError::Db)?;
        Ok(occurrences)
    }
}

fn validate_cron_expression(value: &str) -> QueueResult<()> {
    parse_cron(value).map(|_| ())
}

fn parse_cron(value: &str) -> QueueResult<Cron> {
    if value.split_ascii_whitespace().count() != 5 {
        return Err(QueueError::Validation(
            "cron expression must contain five fields".to_string(),
        ));
    }
    Cron::from_str(value)
        .map_err(|error| QueueError::Validation(format!("invalid cron expression: {error}")))
}

fn validate_timezone(value: &str) -> QueueResult<()> {
    parse_timezone(value).map(|_| ())
}

fn parse_timezone(value: &str) -> QueueResult<Tz> {
    value
        .parse::<Tz>()
        .map_err(|error| QueueError::Validation(format!("invalid IANA timezone: {error}")))
}

async fn retry_job(pool: &PgPool, job: &LeasedJob, nack: &Nack) -> QueueResult<()> {
    let retry_after_ms = nack
        .retry_after
        .map(|delay| {
            delay.min(if matches!(job.job().kind(), JobKind::Fetch { .. }) {
                FETCH_RETRY_AFTER_CAP
            } else {
                RETRY_CAP
            })
        })
        .unwrap_or_else(|| default_backoff(job.attempts()))
        .as_millis()
        .min(i64::MAX as u128) as i64;

    let result = sqlx::query!(
        r#"UPDATE queue_jobs
         SET status = 'pending',
             locked_by = NULL,
             locked_at = NULL,
             run_at = now() + ($3::BIGINT * INTERVAL '1 millisecond'),
             last_error = $4,
             updated_at = now()
         WHERE id = $1
           AND status = 'running'
           AND locked_by = $2
           AND lease_version = $5"#,
        job.id().get(),
        job.worker_id().as_str(),
        retry_after_ms,
        &nack.message,
        job.lease_version()
    )
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
    let result = sqlx::query!(
        r#"UPDATE queue_jobs
         SET status = 'dead',
             locked_by = NULL,
             locked_at = NULL,
             last_error = $3,
             updated_at = now()
         WHERE id = $1
           AND status = 'running'
           AND locked_by = $2
           AND lease_version = $4"#,
        job.id().get(),
        job.worker_id().as_str(),
        &nack.message,
        job.lease_version()
    )
    .execute(&mut *tx)
    .await
    .map_err(QueueError::Db)?;

    if result.rows_affected() == 0 {
        return Err(QueueError::LeaseLost(job.id()));
    }

    sqlx::query!(
        r#"INSERT INTO queue_dead_letters
            (job_id, stage, payload, attempts, error_class, error_message, trace_parent)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (job_id) DO UPDATE
         SET attempts = EXCLUDED.attempts,
             error_class = EXCLUDED.error_class,
             error_message = EXCLUDED.error_message,
             trace_parent = EXCLUDED.trace_parent,
             failed_at = now()"#,
        job.id().get(),
        job.job().stage().as_str(),
        serde_json::to_value(job.job())?,
        job.attempts(),
        format!("{:?}", nack.error_class),
        &nack.message,
        job.trace_parent()
    )
    .execute(&mut *tx)
    .await
    .map_err(QueueError::Db)?;

    sqlx::query(
        "UPDATE queue_schedule_occurrences
         SET status = 'failed'
         WHERE job_id = $1",
    )
    .bind(job.id().get())
    .execute(&mut *tx)
    .await
    .map_err(QueueError::Db)?;

    tx.commit().await.map_err(QueueError::Db)?;
    Ok(())
}

fn default_backoff(attempts: i32) -> Duration {
    let cap = retry_backoff_cap(attempts);
    let entropy = Uuid::new_v4().as_u128() as u64;
    full_jitter(cap, entropy)
}

fn retry_backoff_cap(attempts: i32) -> Duration {
    let shift = attempts.saturating_sub(1).clamp(0, 31) as u32;
    let seconds = RETRY_BASE
        .as_secs()
        .saturating_mul(1_u64.checked_shl(shift).unwrap_or(u64::MAX));
    Duration::from_secs(seconds.min(RETRY_CAP.as_secs()))
}

fn full_jitter(cap: Duration, entropy: u64) -> Duration {
    let cap_millis = cap.as_millis().min(u128::from(u64::MAX)) as u64;
    Duration::from_millis(entropy % cap_millis.saturating_add(1))
}

fn duration_millis_i64(duration: Duration) -> i64 {
    duration.as_millis().min(i64::MAX as u128) as i64
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .and_then(|db_err| db_err.code())
        .as_deref()
        == Some("23505")
}

fn leased_from_parts(
    id: i64,
    payload: serde_json::Value,
    attempts: i32,
    locked_at: DateTime<Utc>,
    lease_version: i64,
    worker_id: WorkerId,
) -> QueueResult<LeasedJob> {
    Ok(LeasedJob {
        id: JobId::new(id),
        job: serde_json::from_value(payload)?,
        worker_id,
        attempts,
        leased_at: locked_at,
        lease_version,
    })
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
    use chrono::{Datelike, TimeZone, Timelike};

    use super::*;

    #[test]
    fn load_jobs_have_per_dataflow_group_key() {
        let job = Job::load(DataflowId::new("abs.cpi").unwrap(), Uuid::from_u128(1));
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
            Job::fetch(Uuid::from_u128(1)),
            Job::parse(Uuid::from_u128(2)),
            Job::load(dataflow_id, Uuid::from_u128(3)),
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
    fn cron_schedule_rejects_invalid_cron_and_timezone() {
        let job = Job::discover(SourceId::new("abs").unwrap());
        assert!(CronSchedule::new("abs-cpi", "not cron", job.clone()).is_err());
        assert!(
            CronSchedule::new("abs-cpi", "15 0 * * *", job)
                .unwrap()
                .with_timezone("Australia/NotAZone")
                .is_err()
        );
    }

    #[test]
    fn sydney_daily_schedule_stays_at_local_time_across_dst_boundaries() {
        let before_spring = Utc.with_ymd_and_hms(2026, 10, 3, 14, 30, 0).unwrap();
        let spring = next_schedule_occurrence("15 0 * * *", "Australia/Sydney", before_spring)
            .unwrap()
            .with_timezone(&chrono_tz::Australia::Sydney);
        assert_eq!((spring.year(), spring.month(), spring.day()), (2026, 10, 5));
        assert_eq!((spring.hour(), spring.minute()), (0, 15));

        let before_autumn = Utc.with_ymd_and_hms(2026, 4, 4, 14, 30, 0).unwrap();
        let autumn = next_schedule_occurrence("15 0 * * *", "Australia/Sydney", before_autumn)
            .unwrap()
            .with_timezone(&chrono_tz::Australia::Sydney);
        assert_eq!((autumn.year(), autumn.month(), autumn.day()), (2026, 4, 6));
        assert_eq!((autumn.hour(), autumn.minute()), (0, 15));
    }

    #[test]
    fn retry_backoff_uses_full_jitter_with_six_hour_cap() {
        assert_eq!(retry_backoff_cap(0), Duration::from_secs(30));
        assert_eq!(retry_backoff_cap(1), Duration::from_secs(30));
        assert_eq!(retry_backoff_cap(2), Duration::from_secs(60));
        assert_eq!(retry_backoff_cap(99), RETRY_CAP);
        assert_eq!(full_jitter(Duration::from_secs(30), 0), Duration::ZERO);
        assert!(full_jitter(Duration::from_secs(30), u64::MAX) <= Duration::from_secs(30));
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
