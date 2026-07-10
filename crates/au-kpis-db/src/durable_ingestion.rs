//! Durable discovered-work and ingestion-generation persistence.

use std::{fmt, str::FromStr};

use au_kpis_domain::{
    Artifact, ArtifactId, DataflowId, Observation, ObservationStatus, SeriesDescriptor,
    Sha256Digest, SourceId, TimePrecision,
};
use au_kpis_error::CoreError;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Postgres, QueryBuilder, Row};
use url::Url;
use uuid::Uuid;

use crate::{DbError, PgPool};

/// Inputs that identify one upstream discovery result.
#[derive(Debug, Clone)]
pub struct DiscoveredWorkInput<'a> {
    /// Schedule occurrence that produced the work, when scheduled.
    pub occurrence_id: Option<Uuid>,
    /// Owning source.
    pub source_id: &'a SourceId,
    /// Owning dataflow.
    pub dataflow_id: &'a DataflowId,
    /// Upstream artifact or release URL.
    pub source_url: &'a str,
    /// Stable upstream release/revision identity.
    pub upstream_revision: &'a str,
    /// Adapter discovery metadata.
    pub discovery_metadata: Value,
}

/// Persisted discovered-work identity and state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredWorkRecord {
    /// Durable work id.
    pub id: Uuid,
    /// Schedule occurrence that produced the work.
    pub occurrence_id: Option<Uuid>,
    /// Owning source.
    pub source_id: SourceId,
    /// Owning dataflow.
    pub dataflow_id: DataflowId,
    /// Canonical normalized source URL.
    pub source_url: String,
    /// Stable upstream revision.
    pub upstream_revision: String,
    /// SHA-256 work identity.
    pub identity_key: [u8; 32],
    /// Persisted work status.
    pub status: String,
    /// Serialized adapter job metadata required to resume Fetch and Parse.
    pub discovery_metadata: Value,
}

/// Complete durable context required to resume Parse or Load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationStageContext {
    /// Generation identity.
    pub generation_id: Uuid,
    /// Discovered work that produced this generation.
    pub discovered_work_id: Uuid,
    /// Exact persisted fetch reference used by this generation.
    pub artifact: Artifact,
    /// Owning source.
    pub source_id: SourceId,
    /// Owning dataflow.
    pub dataflow_id: DataflowId,
    /// Parser implementation version.
    pub parser_version: String,
    /// Transform implementation version.
    pub transform_version: String,
    /// Current generation state.
    pub status: GenerationStatus,
    /// Adapter discovery payload needed by Parse.
    pub discovery_metadata: Value,
    /// Stable upstream revision identity.
    pub upstream_revision: String,
    /// Queue job that created the generation.
    pub job_id: Option<i64>,
    /// W3C trace context propagated from discovery.
    pub trace_parent: Option<String>,
}

/// Inputs for an artifact/dataflow parser generation.
#[derive(Debug, Clone)]
pub struct GenerationInput<'a> {
    /// Discovered work being processed.
    pub discovered_work_id: Uuid,
    /// Concrete artifact fetch provenance row.
    pub artifact_fetch_id: i64,
    /// Owning source.
    pub source_id: &'a SourceId,
    /// Owning dataflow.
    pub dataflow_id: &'a DataflowId,
    /// Parser implementation version.
    pub parser_version: &'a str,
    /// Transform implementation version.
    pub transform_version: &'a str,
    /// Queue job id, when queue-driven.
    pub job_id: Option<i64>,
    /// W3C trace context.
    pub trace_parent: Option<&'a str>,
    /// Actor initiating this generation.
    pub actor: &'a str,
    /// Audited reason for operator-triggered work.
    pub reason: Option<&'a str>,
}

/// Durable ingestion generation state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationStatus {
    /// Waiting for parsing.
    PendingParse,
    /// Parser owns the generation.
    Parsing,
    /// Parsing completed without row errors.
    ParsedClean,
    /// Parsing completed with accepted partial rows.
    ParsedPartial,
    /// Artifact or provenance was permanently rejected.
    Rejected,
    /// Parsed staging is waiting for Load.
    PendingLoad,
    /// Loader owns the generation.
    Loading,
    /// Observations and publication audit committed atomically.
    Published,
    /// Load or stage processing failed.
    Failed,
}

impl GenerationStatus {
    /// Whether this status may transition directly to `next`.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::PendingParse, Self::Parsing | Self::Rejected)
                | (
                    Self::Parsing,
                    Self::ParsedClean | Self::ParsedPartial | Self::Rejected | Self::Failed
                )
                | (Self::ParsedClean | Self::ParsedPartial, Self::PendingLoad)
                | (Self::PendingLoad, Self::Loading)
                | (Self::Loading, Self::Published | Self::Failed)
                | (Self::Failed, Self::PendingParse | Self::PendingLoad)
        )
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::PendingParse => "pending_parse",
            Self::Parsing => "parsing",
            Self::ParsedClean => "parsed_clean",
            Self::ParsedPartial => "parsed_partial",
            Self::Rejected => "rejected",
            Self::PendingLoad => "pending_load",
            Self::Loading => "loading",
            Self::Published => "published",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for GenerationStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for GenerationStatus {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending_parse" => Ok(Self::PendingParse),
            "parsing" => Ok(Self::Parsing),
            "parsed_clean" => Ok(Self::ParsedClean),
            "parsed_partial" => Ok(Self::ParsedPartial),
            "rejected" => Ok(Self::Rejected),
            "pending_load" => Ok(Self::PendingLoad),
            "loading" => Ok(Self::Loading),
            "published" => Ok(Self::Published),
            "failed" => Ok(Self::Failed),
            other => Err(CoreError::Validation(format!(
                "unknown ingestion generation status `{other}`"
            ))),
        }
    }
}

/// Persisted ingestion generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationRecord {
    /// Generation id.
    pub id: Uuid,
    /// Discovered-work id.
    pub discovered_work_id: Uuid,
    /// Artifact fetch id.
    pub artifact_fetch_id: i64,
    /// Owning source.
    pub source_id: SourceId,
    /// Owning dataflow.
    pub dataflow_id: DataflowId,
    /// Parser version.
    pub parser_version: String,
    /// Transform version.
    pub transform_version: String,
    /// Current state.
    pub status: GenerationStatus,
    /// Parsed row count.
    pub parsed_count: u64,
    /// Published row count.
    pub loaded_count: u64,
    /// Rejected row count.
    pub error_count: u64,
    /// Stage digest when parsing completed.
    pub stage_digest: Option<[u8; 32]>,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Publication timestamp.
    pub published_at: Option<DateTime<Utc>>,
}

/// One typed observation-stage row supplied by a streaming parser.
#[derive(Debug, Clone, Copy)]
pub struct ObservationStageRow<'a> {
    /// Stable zero-based row number within the generation.
    pub row_no: u64,
    /// Complete series descriptor.
    pub series: &'a SeriesDescriptor,
    /// Complete observation.
    pub observation: &'a Observation,
}

/// Incremental digest over canonical typed observation-stage rows.
#[derive(Debug, Clone)]
pub struct StageDigest(Sha256);

impl StageDigest {
    /// Start an empty stage digest.
    #[must_use]
    pub fn new() -> Self {
        Self(Sha256::new())
    }

    /// Add one row using length-prefixed canonical JSON.
    pub fn update(&mut self, row: ObservationStageRow<'_>) -> Result<(), DbError> {
        validate_stage_row_shape(row)?;
        let canonical = serde_json::to_vec(&(row.row_no, row.series, row.observation))
            .map_err(CoreError::from)?;
        self.0.update((canonical.len() as u64).to_be_bytes());
        self.0.update(canonical);
        Ok(())
    }

    /// Finish and return the SHA-256 digest.
    #[must_use]
    pub fn finalize(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

impl Default for StageDigest {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute canonical SHA-256 identity for discovered upstream work.
pub fn work_identity_key(
    source_id: &SourceId,
    dataflow_id: &DataflowId,
    source_url: &str,
    upstream_revision: &str,
) -> Result<[u8; 32], DbError> {
    let normalized_url = normalize_source_url(source_url)?;
    if upstream_revision.trim().is_empty() {
        return Err(DbError::Core(CoreError::Validation(
            "upstream revision must not be empty".to_string(),
        )));
    }
    let canonical = serde_json::to_vec(&(
        source_id.as_str(),
        dataflow_id.as_str(),
        normalized_url.as_str(),
        upstream_revision,
    ))
    .map_err(CoreError::from)?;
    Ok(Sha256::digest(canonical).into())
}

/// Insert discovered work idempotently by its canonical identity.
pub async fn persist_discovered_work(
    pool: &PgPool,
    input: DiscoveredWorkInput<'_>,
) -> Result<DiscoveredWorkRecord, DbError> {
    let source_url = normalize_source_url(input.source_url)?;
    let identity_key = work_identity_key(
        input.source_id,
        input.dataflow_id,
        &source_url,
        input.upstream_revision,
    )?;
    let row = sqlx::query(
        r#"INSERT INTO discovered_work (
               occurrence_id, source_id, dataflow_id, source_url,
               upstream_revision, identity_key, discovery_metadata
           )
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (identity_key) DO UPDATE
           SET discovery_metadata = discovered_work.discovery_metadata || EXCLUDED.discovery_metadata,
               updated_at = now()
           RETURNING id, occurrence_id, source_id, dataflow_id, source_url,
                     upstream_revision, identity_key, status, discovery_metadata"#,
    )
    .bind(input.occurrence_id)
    .bind(input.source_id.as_str())
    .bind(input.dataflow_id.as_str())
    .bind(source_url)
    .bind(input.upstream_revision)
    .bind(identity_key.as_slice())
    .bind(input.discovery_metadata)
    .fetch_one(pool)
    .await
    .map_err(DbError::Query)?;

    discovered_work_from_row(row)
}

/// Load one durable discovered-work record by identity.
pub async fn get_discovered_work(
    pool: &PgPool,
    id: Uuid,
) -> Result<Option<DiscoveredWorkRecord>, DbError> {
    sqlx::query(
        r#"SELECT id, occurrence_id, source_id, dataflow_id, source_url,
                  upstream_revision, identity_key, status, discovery_metadata
           FROM discovered_work
           WHERE id = $1"#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(DbError::Query)?
    .map(discovered_work_from_row)
    .transpose()
}

/// Fence discovered work for Fetch. Returns `false` after it has already fetched.
pub async fn begin_discovered_work_fetch(pool: &PgPool, id: Uuid) -> Result<bool, DbError> {
    let result = sqlx::query(
        r#"UPDATE discovered_work
           SET status = 'fetching', updated_at = now()
           WHERE id = $1 AND status IN ('pending_fetch', 'fetching')"#,
    )
    .bind(id)
    .execute(pool)
    .await
    .map_err(DbError::Query)?;
    if result.rows_affected() == 1 {
        return Ok(true);
    }
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM discovered_work WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await
            .map_err(DbError::Query)?;
    match status.as_deref() {
        Some("fetched" | "handled") => Ok(false),
        Some(status) => Err(DbError::Core(CoreError::Validation(format!(
            "discovered work `{id}` cannot enter fetching from `{status}`"
        )))),
        None => Err(DbError::Core(CoreError::NotFound(format!(
            "discovered work `{id}`"
        )))),
    }
}

/// Mark a fenced discovered-work item fetched after its generation is durable.
pub async fn complete_discovered_work_fetch(pool: &PgPool, id: Uuid) -> Result<(), DbError> {
    let result = sqlx::query(
        r#"UPDATE discovered_work
           SET status = 'fetched', fetched_at = COALESCE(fetched_at, now()), updated_at = now()
           WHERE id = $1 AND status = 'fetching'"#,
    )
    .bind(id)
    .execute(pool)
    .await
    .map_err(DbError::Query)?;
    if result.rows_affected() == 1 {
        return Ok(());
    }
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM discovered_work WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await
            .map_err(DbError::Query)?;
    match status.as_deref() {
        Some("fetched" | "handled") => Ok(()),
        Some(status) => Err(DbError::Core(CoreError::Validation(format!(
            "discovered work `{id}` cannot complete Fetch from `{status}`"
        )))),
        None => Err(DbError::Core(CoreError::NotFound(format!(
            "discovered work `{id}`"
        )))),
    }
}

/// Permanently reject discovered work that cannot produce a valid generation.
pub async fn reject_discovered_work(pool: &PgPool, id: Uuid, reason: &str) -> Result<(), DbError> {
    let result = sqlx::query(
        r#"UPDATE discovered_work
           SET status = 'rejected',
               discovery_metadata = discovery_metadata || jsonb_build_object('rejection_reason', $2),
               updated_at = now()
           WHERE id = $1 AND status IN ('pending_fetch', 'fetching')"#,
    )
    .bind(id)
    .bind(reason)
    .execute(pool)
    .await
    .map_err(DbError::Query)?;
    if result.rows_affected() != 1 {
        return Err(DbError::Core(CoreError::Validation(format!(
            "discovered work `{id}` is not rejectable"
        ))));
    }
    Ok(())
}

/// Create or return the idempotent parser/transform generation.
pub async fn create_ingestion_generation(
    pool: &PgPool,
    input: GenerationInput<'_>,
) -> Result<GenerationRecord, DbError> {
    for (field, value) in [
        ("parser_version", input.parser_version),
        ("transform_version", input.transform_version),
        ("actor", input.actor),
    ] {
        if value.trim().is_empty() {
            return Err(DbError::Core(CoreError::Validation(format!(
                "{field} must not be empty"
            ))));
        }
    }

    let row = sqlx::query(
        r#"INSERT INTO ingestion_generations (
               discovered_work_id, artifact_fetch_id, source_id, dataflow_id,
               parser_version, transform_version, job_id, trace_parent, actor, reason
           )
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
           ON CONFLICT (artifact_fetch_id, dataflow_id, parser_version, transform_version)
           DO UPDATE SET updated_at = ingestion_generations.updated_at
           RETURNING id, discovered_work_id, artifact_fetch_id, source_id,
                     dataflow_id, parser_version, transform_version, status,
                     parsed_count, loaded_count, error_count, stage_digest,
                     created_at, published_at"#,
    )
    .bind(input.discovered_work_id)
    .bind(input.artifact_fetch_id)
    .bind(input.source_id.as_str())
    .bind(input.dataflow_id.as_str())
    .bind(input.parser_version)
    .bind(input.transform_version)
    .bind(input.job_id)
    .bind(input.trace_parent)
    .bind(input.actor)
    .bind(input.reason)
    .fetch_one(pool)
    .await
    .map_err(DbError::Query)?;

    generation_from_row(row)
}

/// Reconstruct the exact artifact and adapter context for a durable generation.
pub async fn get_ingestion_generation_context(
    pool: &PgPool,
    id: Uuid,
) -> Result<Option<GenerationStageContext>, DbError> {
    let row = sqlx::query(
        r#"SELECT generation.id, generation.discovered_work_id,
                  generation.source_id, generation.dataflow_id,
                  generation.parser_version, generation.transform_version,
                  generation.status, generation.job_id, generation.trace_parent,
                  work.discovery_metadata, work.upstream_revision,
                  artifact_fetch.id AS artifact_fetch_id, artifact_fetch.artifact_id,
                  artifact_fetch.source_url, artifact_fetch.content_type,
                  artifact_fetch.response_headers, artifact_fetch.size_bytes,
                  artifact_fetch.storage_key, artifact_fetch.fetched_at
           FROM ingestion_generations AS generation
           JOIN discovered_work AS work ON work.id = generation.discovered_work_id
           JOIN artifact_fetches AS artifact_fetch
             ON artifact_fetch.id = generation.artifact_fetch_id
           WHERE generation.id = $1"#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(DbError::Query)?;
    row.map(generation_context_from_row).transpose()
}

/// Mark a transient Parse or Load attempt failed so the same generation can retry.
pub async fn fail_ingestion_generation(
    pool: &PgPool,
    id: Uuid,
    reason: &str,
) -> Result<(), DbError> {
    let result = sqlx::query(
        r#"UPDATE ingestion_generations
           SET status = 'failed', reason = $2, failed_at = now(), updated_at = now()
           WHERE id = $1 AND status IN ('parsing', 'loading')"#,
    )
    .bind(id)
    .bind(reason)
    .execute(pool)
    .await
    .map_err(DbError::Query)?;
    if result.rows_affected() != 1 {
        return Err(DbError::Core(CoreError::Validation(format!(
            "generation `{id}` is not owned by an active stage"
        ))));
    }
    Ok(())
}

/// Permanently reject a pending or active parser generation and clear staging.
pub async fn reject_ingestion_generation(
    pool: &PgPool,
    id: Uuid,
    reason: &str,
) -> Result<(), DbError> {
    let mut tx = pool.begin().await.map_err(DbError::Query)?;
    let result = sqlx::query(
        r#"UPDATE ingestion_generations
           SET status = 'rejected', reason = $2, failed_at = now(), updated_at = now()
           WHERE id = $1 AND status IN ('pending_parse', 'parsing')"#,
    )
    .bind(id)
    .bind(reason)
    .execute(&mut *tx)
    .await
    .map_err(DbError::Query)?;
    if result.rows_affected() != 1 {
        return Err(DbError::Core(CoreError::Validation(format!(
            "generation `{id}` is not rejectable"
        ))));
    }
    sqlx::query("DELETE FROM observation_stage WHERE generation_id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;
    tx.commit().await.map_err(DbError::Query)?;
    Ok(())
}

/// Move a generation through one valid, fenced state transition.
pub async fn transition_ingestion_generation(
    pool: &PgPool,
    id: Uuid,
    current: GenerationStatus,
    next: GenerationStatus,
) -> Result<(), DbError> {
    if !current.can_transition_to(next) {
        return Err(DbError::Core(CoreError::Validation(format!(
            "invalid generation transition {current} -> {next}"
        ))));
    }
    let result = sqlx::query(
        r#"UPDATE ingestion_generations
           SET status = $3,
               parsing_started_at = CASE WHEN $3 = 'parsing' THEN now() ELSE parsing_started_at END,
               parsed_at = CASE WHEN $3 IN ('parsed_clean', 'parsed_partial') THEN now() ELSE parsed_at END,
               loading_started_at = CASE WHEN $3 = 'loading' THEN now() ELSE loading_started_at END,
               published_at = CASE WHEN $3 = 'published' THEN now() ELSE published_at END,
               failed_at = CASE WHEN $3 = 'failed' THEN now() ELSE failed_at END,
               updated_at = now()
           WHERE id = $1
             AND status = $2"#,
    )
    .bind(id)
    .bind(current.as_str())
    .bind(next.as_str())
    .execute(pool)
    .await
    .map_err(DbError::Query)?;
    if result.rows_affected() != 1 {
        return Err(DbError::Core(CoreError::Validation(format!(
            "generation `{id}` is not in expected state `{current}`"
        ))));
    }
    Ok(())
}

/// Fence a pending generation for parsing and clear remnants of an earlier attempt.
pub async fn begin_ingestion_parse(pool: &PgPool, id: Uuid) -> Result<(), DbError> {
    let mut tx = pool.begin().await.map_err(DbError::Query)?;
    let result = sqlx::query(
        r#"UPDATE ingestion_generations
           SET status = 'parsing',
               parsed_count = 0,
               loaded_count = 0,
               error_count = 0,
               stage_digest = NULL,
               parsing_started_at = now(),
               parsed_at = NULL,
               loading_started_at = NULL,
               failed_at = NULL,
               updated_at = now()
           WHERE id = $1
             AND status IN ('pending_parse', 'failed')"#,
    )
    .bind(id)
    .execute(&mut *tx)
    .await
    .map_err(DbError::Query)?;
    if result.rows_affected() != 1 {
        return Err(DbError::Core(CoreError::Validation(format!(
            "generation `{id}` is not available for parsing"
        ))));
    }
    sqlx::query("DELETE FROM observation_stage WHERE generation_id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;
    tx.commit().await.map_err(DbError::Query)?;
    Ok(())
}

/// Append one bounded parser batch to durable typed staging.
pub async fn append_observation_stage(
    pool: &PgPool,
    generation_id: Uuid,
    rows: &[ObservationStageRow<'_>],
) -> Result<(), DbError> {
    const MAX_STAGE_BATCH_ROWS: usize = 1_000;
    if rows.is_empty() {
        return Ok(());
    }
    if rows.len() > MAX_STAGE_BATCH_ROWS {
        return Err(DbError::Core(CoreError::Validation(format!(
            "observation stage batch has {} rows, maximum is {MAX_STAGE_BATCH_ROWS}",
            rows.len()
        ))));
    }
    for window in rows.windows(2) {
        if window[0].row_no >= window[1].row_no {
            return Err(DbError::Core(CoreError::Validation(
                "observation stage row numbers must be strictly increasing".to_string(),
            )));
        }
    }
    for row in rows {
        validate_stage_row_shape(*row)?;
    }

    let mut tx = pool.begin().await.map_err(DbError::Query)?;
    let generation = sqlx::query(
        r#"SELECT generation.status, generation.dataflow_id,
                  artifact_fetch.artifact_id
           FROM ingestion_generations AS generation
           JOIN artifact_fetches AS artifact_fetch
             ON artifact_fetch.id = generation.artifact_fetch_id
           WHERE generation.id = $1
           FOR UPDATE OF generation"#,
    )
    .bind(generation_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(DbError::Query)?
    .ok_or_else(|| DbError::Core(CoreError::NotFound(format!("generation `{generation_id}`"))))?;
    let status: String = generation.try_get("status").map_err(DbError::Query)?;
    if status != "parsing" {
        return Err(DbError::Core(CoreError::Validation(format!(
            "generation `{generation_id}` is `{status}`, expected `parsing`"
        ))));
    }
    let dataflow_id: String = generation.try_get("dataflow_id").map_err(DbError::Query)?;
    let artifact_id: Vec<u8> = generation.try_get("artifact_id").map_err(DbError::Query)?;

    for row in rows {
        if row.series.dataflow_id.as_str() != dataflow_id
            || row
                .observation
                .source_artifact_id
                .digest()
                .as_bytes()
                .as_slice()
                != artifact_id.as_slice()
        {
            return Err(DbError::Core(CoreError::Validation(format!(
                "staged row {} provenance does not match generation `{generation_id}`",
                row.row_no
            ))));
        }
    }

    let prepared = rows
        .iter()
        .map(|row| {
            Ok((
                *row,
                i64::try_from(row.row_no).map_err(|_| {
                    DbError::Core(CoreError::Validation(
                        "observation stage row number exceeds BIGINT".to_string(),
                    ))
                })?,
                serde_json::to_value(&row.series.dimensions).map_err(CoreError::from)?,
                serde_json::to_value(&row.observation.attributes).map_err(CoreError::from)?,
            ))
        })
        .collect::<Result<Vec<_>, DbError>>()?;

    let mut query = QueryBuilder::<Postgres>::new(
        "INSERT INTO observation_stage (generation_id, row_no, series_key, dataflow_id, \
         measure_id, dimensions, unit, time, time_precision, value, status, attributes, \
         revision_no, ingested_at, source_artifact_id) ",
    );
    query.push_values(
        prepared,
        |mut values, (row, row_no, dimensions, attributes)| {
            values
                .push_bind(generation_id)
                .push_bind(row_no)
                .push_bind(row.series.series_key.digest().as_bytes().as_slice())
                .push_bind(row.series.dataflow_id.as_str())
                .push_bind(row.series.measure_id.as_str())
                .push_bind(dimensions)
                .push_bind(&row.series.unit)
                .push_bind(row.observation.time)
                .push_bind(time_precision_db(row.observation.time_precision))
                .push_bind(row.observation.value)
                .push_bind(observation_status_db(row.observation.status))
                .push_bind(attributes)
                .push_bind(i64::from(row.observation.revision_no))
                .push_bind(row.observation.ingested_at)
                .push_bind(
                    row.observation
                        .source_artifact_id
                        .digest()
                        .as_bytes()
                        .as_slice(),
                );
        },
    );
    query
        .build()
        .execute(&mut *tx)
        .await
        .map_err(DbError::Query)?;
    tx.commit().await.map_err(DbError::Query)?;
    Ok(())
}

/// Verify staged count and finish Parse with a caller-maintained stage digest.
pub async fn complete_ingestion_parse(
    pool: &PgPool,
    generation_id: Uuid,
    parsed_count: u64,
    error_count: u64,
    stage_digest: [u8; 32],
    allow_partial_rows: bool,
) -> Result<GenerationStatus, DbError> {
    if error_count > 0 && !allow_partial_rows {
        return Err(DbError::Core(CoreError::Validation(
            "partial parser rows are disabled for this dataflow".to_string(),
        )));
    }
    let parsed_count = i64::try_from(parsed_count).map_err(|_| {
        DbError::Core(CoreError::Validation(
            "parsed count exceeds BIGINT".to_string(),
        ))
    })?;
    let error_count = i64::try_from(error_count).map_err(|_| {
        DbError::Core(CoreError::Validation(
            "error count exceeds BIGINT".to_string(),
        ))
    })?;
    let status = if error_count == 0 {
        GenerationStatus::ParsedClean
    } else {
        GenerationStatus::ParsedPartial
    };
    let result = sqlx::query(
        r#"UPDATE ingestion_generations AS generation
           SET status = $3,
               parsed_count = $2,
               error_count = $4,
               stage_digest = $5,
               parsed_at = now(),
               updated_at = now()
           WHERE generation.id = $1
             AND generation.status = 'parsing'
             AND (
                 SELECT count(*)
                 FROM observation_stage AS stage
                 WHERE stage.generation_id = generation.id
             ) = $2"#,
    )
    .bind(generation_id)
    .bind(parsed_count)
    .bind(status.as_str())
    .bind(error_count)
    .bind(stage_digest.as_slice())
    .execute(pool)
    .await
    .map_err(DbError::Query)?;
    if result.rows_affected() != 1 {
        return Err(DbError::Core(CoreError::Validation(format!(
            "generation `{generation_id}` staged row count/state mismatch"
        ))));
    }
    Ok(status)
}

/// Reset non-terminal generations whose unlogged stage rows were lost.
pub async fn recover_lost_observation_stages(pool: &PgPool) -> Result<u64, DbError> {
    let result = sqlx::query(
        r#"UPDATE ingestion_generations AS generation
           SET status = 'pending_parse',
               parsed_count = 0,
               loaded_count = 0,
               error_count = 0,
               stage_digest = NULL,
               parsing_started_at = NULL,
               parsed_at = NULL,
               loading_started_at = NULL,
               failed_at = NULL,
               updated_at = now(),
               reason = COALESCE(reason, 'lost unlogged staging recovered')
           WHERE generation.status IN (
               'parsing', 'parsed_clean', 'parsed_partial', 'pending_load',
               'loading', 'failed'
           )
             AND NOT EXISTS (
                 SELECT 1
                 FROM observation_stage AS stage
                 WHERE stage.generation_id = generation.id
             )"#,
    )
    .execute(pool)
    .await
    .map_err(DbError::Query)?;
    Ok(result.rows_affected())
}

fn normalize_source_url(value: &str) -> Result<String, DbError> {
    let mut url = Url::parse(value).map_err(|error| {
        DbError::Core(CoreError::Validation(format!(
            "invalid discovered source URL: {error}"
        )))
    })?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(DbError::Core(CoreError::Validation(
            "discovered source URL must be absolute HTTP(S)".to_string(),
        )));
    }
    url.set_fragment(None);
    let mut query = url.query_pairs().into_owned().collect::<Vec<_>>();
    query.sort();
    url.set_query(None);
    if !query.is_empty() {
        url.query_pairs_mut().extend_pairs(query);
    }
    Ok(url.into())
}

fn validate_stage_row_shape(row: ObservationStageRow<'_>) -> Result<(), DbError> {
    if row.row_no > i64::MAX as u64 {
        return Err(DbError::Core(CoreError::Validation(
            "observation stage row number exceeds BIGINT".to_string(),
        )));
    }
    if row.series.series_key != row.observation.series_key
        || row.series.compute_series_key() != row.series.series_key
    {
        return Err(DbError::Core(CoreError::Validation(format!(
            "observation stage row {} has inconsistent series identity",
            row.row_no
        ))));
    }
    Ok(())
}

const fn time_precision_db(value: TimePrecision) -> &'static str {
    match value {
        TimePrecision::Minute => "minute",
        TimePrecision::Day => "day",
        TimePrecision::Week => "week",
        TimePrecision::Month => "month",
        TimePrecision::Quarter => "quarter",
        TimePrecision::Year => "year",
    }
}

const fn observation_status_db(value: ObservationStatus) -> &'static str {
    match value {
        ObservationStatus::Normal => "normal",
        ObservationStatus::Estimated => "estimated",
        ObservationStatus::Forecast => "forecast",
        ObservationStatus::Imputed => "imputed",
        ObservationStatus::Missing => "missing",
        ObservationStatus::Provisional => "provisional",
        ObservationStatus::Revised => "revised",
        ObservationStatus::Break => "break",
    }
}

fn discovered_work_from_row(row: sqlx::postgres::PgRow) -> Result<DiscoveredWorkRecord, DbError> {
    Ok(DiscoveredWorkRecord {
        id: row.try_get("id").map_err(DbError::Query)?,
        occurrence_id: row.try_get("occurrence_id").map_err(DbError::Query)?,
        source_id: parse_source_id(row.try_get("source_id").map_err(DbError::Query)?)?,
        dataflow_id: parse_dataflow_id(row.try_get("dataflow_id").map_err(DbError::Query)?)?,
        source_url: row.try_get("source_url").map_err(DbError::Query)?,
        upstream_revision: row.try_get("upstream_revision").map_err(DbError::Query)?,
        identity_key: digest_bytes(row.try_get("identity_key").map_err(DbError::Query)?)?,
        status: row.try_get("status").map_err(DbError::Query)?,
        discovery_metadata: row.try_get("discovery_metadata").map_err(DbError::Query)?,
    })
}

fn generation_context_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<GenerationStageContext, DbError> {
    let source_id = parse_source_id(row.try_get("source_id").map_err(DbError::Query)?)?;
    let size_bytes: i64 = row.try_get("size_bytes").map_err(DbError::Query)?;
    let size_bytes = u64::try_from(size_bytes).map_err(|_| {
        DbError::Core(CoreError::Validation(
            "artifact fetch size is negative".to_string(),
        ))
    })?;
    let artifact_id = ArtifactId::from_digest(Sha256Digest::from_bytes(digest_bytes(
        row.try_get("artifact_id").map_err(DbError::Query)?,
    )?));
    Ok(GenerationStageContext {
        generation_id: row.try_get("id").map_err(DbError::Query)?,
        discovered_work_id: row.try_get("discovered_work_id").map_err(DbError::Query)?,
        artifact: Artifact {
            id: artifact_id,
            fetch_id: Some(row.try_get("artifact_fetch_id").map_err(DbError::Query)?),
            source_id: source_id.clone(),
            source_url: row.try_get("source_url").map_err(DbError::Query)?,
            content_type: row.try_get("content_type").map_err(DbError::Query)?,
            response_headers: serde_json::from_value(
                row.try_get("response_headers").map_err(DbError::Query)?,
            )
            .map_err(CoreError::from)?,
            size_bytes,
            storage_key: row.try_get("storage_key").map_err(DbError::Query)?,
            fetched_at: row.try_get("fetched_at").map_err(DbError::Query)?,
        },
        source_id,
        dataflow_id: parse_dataflow_id(row.try_get("dataflow_id").map_err(DbError::Query)?)?,
        parser_version: row.try_get("parser_version").map_err(DbError::Query)?,
        transform_version: row.try_get("transform_version").map_err(DbError::Query)?,
        status: row
            .try_get::<String, _>("status")
            .map_err(DbError::Query)?
            .parse()
            .map_err(DbError::Core)?,
        discovery_metadata: row.try_get("discovery_metadata").map_err(DbError::Query)?,
        upstream_revision: row.try_get("upstream_revision").map_err(DbError::Query)?,
        job_id: row.try_get("job_id").map_err(DbError::Query)?,
        trace_parent: row.try_get("trace_parent").map_err(DbError::Query)?,
    })
}

fn generation_from_row(row: sqlx::postgres::PgRow) -> Result<GenerationRecord, DbError> {
    let parsed_count: i64 = row.try_get("parsed_count").map_err(DbError::Query)?;
    let loaded_count: i64 = row.try_get("loaded_count").map_err(DbError::Query)?;
    let error_count: i64 = row.try_get("error_count").map_err(DbError::Query)?;
    let stage_digest: Option<Vec<u8>> = row.try_get("stage_digest").map_err(DbError::Query)?;
    Ok(GenerationRecord {
        id: row.try_get("id").map_err(DbError::Query)?,
        discovered_work_id: row.try_get("discovered_work_id").map_err(DbError::Query)?,
        artifact_fetch_id: row.try_get("artifact_fetch_id").map_err(DbError::Query)?,
        source_id: parse_source_id(row.try_get("source_id").map_err(DbError::Query)?)?,
        dataflow_id: parse_dataflow_id(row.try_get("dataflow_id").map_err(DbError::Query)?)?,
        parser_version: row.try_get("parser_version").map_err(DbError::Query)?,
        transform_version: row.try_get("transform_version").map_err(DbError::Query)?,
        status: row
            .try_get::<String, _>("status")
            .map_err(DbError::Query)?
            .parse()
            .map_err(DbError::Core)?,
        parsed_count: nonnegative_count("parsed_count", parsed_count)?,
        loaded_count: nonnegative_count("loaded_count", loaded_count)?,
        error_count: nonnegative_count("error_count", error_count)?,
        stage_digest: stage_digest.map(digest_bytes).transpose()?,
        created_at: row.try_get("created_at").map_err(DbError::Query)?,
        published_at: row.try_get("published_at").map_err(DbError::Query)?,
    })
}

fn parse_source_id(value: String) -> Result<SourceId, DbError> {
    SourceId::new(value).map_err(|error| DbError::Core(CoreError::Validation(error.to_string())))
}

fn parse_dataflow_id(value: String) -> Result<DataflowId, DbError> {
    DataflowId::new(value).map_err(|error| DbError::Core(CoreError::Validation(error.to_string())))
}

fn digest_bytes(bytes: Vec<u8>) -> Result<[u8; 32], DbError> {
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        DbError::Core(CoreError::Validation(format!(
            "database digest has {} bytes, expected 32",
            bytes.len()
        )))
    })
}

fn nonnegative_count(name: &str, value: i64) -> Result<u64, DbError> {
    u64::try_from(value).map_err(|_| {
        DbError::Core(CoreError::Validation(format!(
            "database {name} is negative: {value}"
        )))
    })
}

#[cfg(test)]
mod tests {
    use au_kpis_domain::{DataflowId, SourceId};

    use super::{GenerationStatus, work_identity_key};

    #[test]
    fn work_identity_normalizes_url_query_order_and_fragment() {
        let source = SourceId::new("abs").unwrap();
        let dataflow = DataflowId::new("abs.cpi").unwrap();
        let first = work_identity_key(
            &source,
            &dataflow,
            "https://EXAMPLE.test/releases?b=2&a=1#section",
            "2026-Q2",
        )
        .unwrap();
        let second = work_identity_key(
            &source,
            &dataflow,
            "https://example.test/releases?a=1&b=2",
            "2026-Q2",
        )
        .unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn work_identity_binds_dataflow_and_revision() {
        let source = SourceId::new("abs").unwrap();
        let cpi = DataflowId::new("abs.cpi").unwrap();
        let building = DataflowId::new("abs.building_activity").unwrap();
        let first = work_identity_key(&source, &cpi, "https://example.test/a", "one").unwrap();
        let other_flow =
            work_identity_key(&source, &building, "https://example.test/a", "one").unwrap();
        let other_revision =
            work_identity_key(&source, &cpi, "https://example.test/a", "two").unwrap();
        assert_ne!(first, other_flow);
        assert_ne!(first, other_revision);
    }

    #[test]
    fn generation_state_machine_rejects_skips_and_terminal_changes() {
        assert!(GenerationStatus::PendingParse.can_transition_to(GenerationStatus::Parsing));
        assert!(GenerationStatus::Loading.can_transition_to(GenerationStatus::Published));
        assert!(!GenerationStatus::PendingParse.can_transition_to(GenerationStatus::Published));
        assert!(!GenerationStatus::Published.can_transition_to(GenerationStatus::PendingLoad));
        assert!(!GenerationStatus::Rejected.can_transition_to(GenerationStatus::PendingParse));
    }
}
