//! Immutable APS snapshot persistence and materialization.

use au_kpis_db::PgPool;
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::Australia::Sydney;
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Row, Transaction};
use thiserror::Error;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    Axis, Confidence, ConfidenceBand, CoverageStatus, IndicatorConfig, IndicatorContribution,
    IndicatorObservation, ScoreZone, ScorecardConfig, ScorecardError, SubIndexScore, Trend,
    load_aps_v1_config, score_aps_snapshot,
};

/// Snapshot publication result after applying coverage gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicationState {
    /// All overall and per-axis coverage gates passed.
    Published,
    /// Snapshot is retained for audit but has no public numeric score.
    InsufficientCoverage,
}

impl PublicationState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Published => "published",
            Self::InsufficientCoverage => "insufficient_coverage",
        }
    }
}

/// Revision view used by APS history reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistoryView {
    /// Original daily publication, excluding later corrections.
    #[default]
    AsPublished,
    /// Most recent correction for each snapshot date.
    Latest,
}

/// Audited correction request for a previously published snapshot date.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApsCorrection {
    /// Snapshot revision being superseded.
    pub supersedes_snapshot_id: Uuid,
    /// Required operator audit reason.
    pub reason: String,
}

/// Immutable APS API snapshot persisted as one publication revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PublishedApsSnapshot {
    /// Stable snapshot revision identity.
    pub id: Uuid,
    /// Scorecard id.
    pub scorecard_id: String,
    /// Immutable config version.
    pub config_version: String,
    /// SHA-256 digest of the canonical config JSON.
    pub config_digest: String,
    /// Sydney calendar date represented by this publication.
    pub snapshot_date: NaiveDate,
    /// Zero-based correction revision.
    pub revision: u32,
    /// Prior snapshot revision superseded by this correction.
    pub supersedes_snapshot_id: Option<Uuid>,
    /// End of the represented Sydney calendar day, expressed in UTC.
    pub as_of: DateTime<Utc>,
    /// Immutable database publication time.
    pub published_at: DateTime<Utc>,
    /// Coverage-gated publication state.
    pub publication_state: PublicationState,
    /// Numeric APS value only when coverage gates pass.
    pub score: Option<f64>,
    /// Score zone only when coverage gates pass.
    pub zone: Option<ScoreZone>,
    /// Movement against the nearest comparable numeric snapshot.
    pub trend: Trend,
    /// Missing-input lower and upper APS bounds.
    pub confidence_band: ConfidenceBand,
    /// Overall confidence label.
    pub confidence: Confidence,
    /// Overall usable scored weight percentage.
    pub coverage_pct: f64,
    /// Axis-level values and coverage.
    pub sub_indexes: Vec<SubIndexScore>,
    /// Full contribution and provenance detail.
    pub contributions: Vec<IndicatorContribution>,
}

/// Compact APS point returned by history endpoints.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ApsSnapshotSummary {
    /// Stable snapshot revision identity.
    pub id: Uuid,
    /// Scorecard id.
    pub scorecard_id: String,
    /// Immutable config version.
    pub config_version: String,
    /// SHA-256 digest of the canonical config JSON.
    pub config_digest: String,
    /// Sydney calendar date represented by this publication.
    pub snapshot_date: NaiveDate,
    /// Zero-based correction revision.
    pub revision: u32,
    /// Prior snapshot revision superseded by this correction.
    pub supersedes_snapshot_id: Option<Uuid>,
    /// End of the represented Sydney calendar day, expressed in UTC.
    pub as_of: DateTime<Utc>,
    /// Immutable database publication time.
    pub published_at: DateTime<Utc>,
    /// Coverage-gated publication state.
    pub publication_state: PublicationState,
    /// Numeric APS value only when coverage gates pass.
    pub score: Option<f64>,
    /// Score zone only when coverage gates pass.
    pub zone: Option<ScoreZone>,
    /// Movement against the nearest comparable numeric snapshot.
    pub trend: Trend,
    /// Missing-input lower and upper APS bounds.
    pub confidence_band: ConfidenceBand,
    /// Overall confidence label.
    pub confidence: Confidence,
    /// Overall usable scored weight percentage.
    pub coverage_pct: f64,
    /// Axis-level values and coverage.
    pub sub_indexes: Vec<SubIndexScore>,
}

impl From<&PublishedApsSnapshot> for ApsSnapshotSummary {
    fn from(snapshot: &PublishedApsSnapshot) -> Self {
        Self {
            id: snapshot.id,
            scorecard_id: snapshot.scorecard_id.clone(),
            config_version: snapshot.config_version.clone(),
            config_digest: snapshot.config_digest.clone(),
            snapshot_date: snapshot.snapshot_date,
            revision: snapshot.revision,
            supersedes_snapshot_id: snapshot.supersedes_snapshot_id,
            as_of: snapshot.as_of,
            published_at: snapshot.published_at,
            publication_state: snapshot.publication_state,
            score: snapshot.score,
            zone: snapshot.zone,
            trend: snapshot.trend,
            confidence_band: snapshot.confidence_band,
            confidence: snapshot.confidence,
            coverage_pct: snapshot.coverage_pct,
            sub_indexes: snapshot.sub_indexes.clone(),
        }
    }
}

/// APS persistence/materialization error.
#[derive(Debug, Error)]
pub enum ScorecardStoreError {
    /// Scorecard config or formula rejected input.
    #[error(transparent)]
    Scorecard(#[from] ScorecardError),
    /// Database operation failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// JSON persistence failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Persisted state violated an invariant.
    #[error("scorecard store validation: {0}")]
    Validation(String),
}

#[derive(Debug)]
struct ResolvedInput {
    observation: IndicatorObservation,
}

/// Idempotently materialize one Sydney daily APS snapshot or append a correction.
pub async fn materialize_aps_snapshot(
    pool: &PgPool,
    snapshot_date: NaiveDate,
    correction: Option<ApsCorrection>,
) -> Result<PublishedApsSnapshot, ScorecardStoreError> {
    let config = load_aps_v1_config()?;
    let config_json = serde_json::to_value(&config)?;
    let config_digest: [u8; 32] = hex::decode(&config.digest)
        .map_err(|error| ScorecardStoreError::Validation(error.to_string()))?
        .try_into()
        .map_err(|_| ScorecardStoreError::Validation("invalid config digest length".into()))?;
    let as_of = snapshot_as_of(snapshot_date)?;
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("aps:{}:{snapshot_date}", config.version))
        .execute(&mut *tx)
        .await?;
    persist_config(&mut tx, &config, &config_json, config_digest).await?;

    if correction.is_none() {
        if let Some(existing) = load_existing_revision(&mut tx, snapshot_date, 0).await? {
            tx.commit().await?;
            return Ok(existing);
        }
    }
    let (revision, supersedes_snapshot_id, correction_reason) =
        correction_revision(&mut tx, &config, snapshot_date, correction).await?;
    let inputs = resolve_inputs(&mut tx, &config, as_of).await?;
    let previous_score = load_previous_score(&mut tx, &config, snapshot_date).await?;
    let calculation = score_aps_snapshot(
        &config,
        &inputs
            .iter()
            .map(|input| input.observation.clone())
            .collect::<Vec<_>>(),
        snapshot_date.to_string(),
        previous_score,
    )?;
    let throughput_coverage = axis_coverage(&calculation.sub_indexes, Axis::Throughput);
    let orientation_coverage = axis_coverage(&calculation.sub_indexes, Axis::Orientation);
    let publication_state = if calculation.coverage_pct >= config.coverage_thresholds.overall_pct
        && throughput_coverage >= config.coverage_thresholds.axis_pct
        && orientation_coverage >= config.coverage_thresholds.axis_pct
    {
        PublicationState::Published
    } else {
        PublicationState::InsufficientCoverage
    };
    let published_at = Utc::now();
    let id = Uuid::new_v4();
    let published = PublishedApsSnapshot {
        id,
        scorecard_id: config.id.clone(),
        config_version: config.version.clone(),
        config_digest: hex::encode(config_digest),
        snapshot_date,
        revision,
        supersedes_snapshot_id,
        as_of,
        published_at,
        publication_state,
        score: (publication_state == PublicationState::Published).then_some(calculation.score),
        zone: (publication_state == PublicationState::Published).then_some(calculation.zone),
        trend: calculation.trend,
        confidence_band: calculation.confidence_band,
        confidence: calculation.confidence,
        coverage_pct: calculation.coverage_pct,
        sub_indexes: calculation.sub_indexes,
        contributions: calculation.contributions,
    };
    persist_snapshot(
        &mut tx,
        &published,
        config_digest,
        throughput_coverage,
        orientation_coverage,
        correction_reason.as_deref(),
    )
    .await?;
    tx.commit().await?;
    Ok(published)
}

/// Load the newest APS date/revision.
pub async fn load_latest_aps_snapshot(
    pool: &PgPool,
    view: HistoryView,
) -> Result<Option<PublishedApsSnapshot>, ScorecardStoreError> {
    let relation = history_relation(view);
    let query = format!(
        "SELECT snapshot_payload FROM {relation}
         WHERE scorecard_id = 'aps'
         ORDER BY snapshot_date DESC, revision DESC LIMIT 1"
    );
    load_payload_optional(pool, &query, None).await
}

/// Load one APS snapshot revision by UUID.
pub async fn load_aps_snapshot(
    pool: &PgPool,
    id: Uuid,
) -> Result<Option<PublishedApsSnapshot>, ScorecardStoreError> {
    load_payload_optional(
        pool,
        "SELECT snapshot_payload FROM scorecard_snapshots WHERE id = $1",
        Some(id),
    )
    .await
}

/// Load a bounded APS history page without contribution recomputation.
pub async fn load_aps_history(
    pool: &PgPool,
    view: HistoryView,
    since: NaiveDate,
    until: NaiveDate,
    limit: u32,
) -> Result<Vec<ApsSnapshotSummary>, ScorecardStoreError> {
    if since > until || limit == 0 || limit > 1_000 {
        return Err(ScorecardStoreError::Validation(
            "invalid APS history bounds or limit".to_string(),
        ));
    }
    let relation = history_relation(view);
    let query = format!(
        "SELECT summary_payload FROM {relation}
         WHERE scorecard_id = 'aps' AND snapshot_date BETWEEN $1 AND $2
         ORDER BY snapshot_date ASC LIMIT $3"
    );
    let rows = sqlx::query(&query)
        .bind(since)
        .bind(until)
        .bind(i64::from(limit))
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| serde_json::from_value(row.try_get("summary_payload")?).map_err(Into::into))
        .collect()
}

async fn persist_config(
    tx: &mut Transaction<'_, Postgres>,
    config: &ScorecardConfig,
    config_json: &serde_json::Value,
    digest: [u8; 32],
) -> Result<(), ScorecardStoreError> {
    sqlx::query(
        "INSERT INTO scorecard_configs (scorecard_id, version, digest, config)
         VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
    )
    .bind(&config.id)
    .bind(&config.version)
    .bind(digest.as_slice())
    .bind(config_json)
    .execute(&mut **tx)
    .await?;
    let stored: Vec<u8> = sqlx::query_scalar(
        "SELECT digest FROM scorecard_configs WHERE scorecard_id = $1 AND version = $2",
    )
    .bind(&config.id)
    .bind(&config.version)
    .fetch_one(&mut **tx)
    .await?;
    if stored.as_slice() != digest.as_slice() {
        return Err(ScorecardStoreError::Validation(format!(
            "config version `{}` already has a different digest",
            config.version
        )));
    }
    Ok(())
}

async fn correction_revision(
    tx: &mut Transaction<'_, Postgres>,
    config: &ScorecardConfig,
    snapshot_date: NaiveDate,
    correction: Option<ApsCorrection>,
) -> Result<(u32, Option<Uuid>, Option<String>), ScorecardStoreError> {
    let Some(correction) = correction else {
        return Ok((0, None, None));
    };
    if correction.reason.trim().is_empty() {
        return Err(ScorecardStoreError::Validation(
            "correction reason is required".to_string(),
        ));
    }
    let row = sqlx::query(
        "SELECT snapshot_date, config_version, revision
         FROM scorecard_snapshots WHERE id = $1 AND scorecard_id = $2",
    )
    .bind(correction.supersedes_snapshot_id)
    .bind(&config.id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| ScorecardStoreError::Validation("superseded snapshot not found".into()))?;
    let predecessor_date: NaiveDate = row.try_get("snapshot_date")?;
    let predecessor_config: String = row.try_get("config_version")?;
    let predecessor_revision: i32 = row.try_get("revision")?;
    if predecessor_date != snapshot_date || predecessor_config != config.version {
        return Err(ScorecardStoreError::Validation(
            "correction predecessor date/config mismatch".to_string(),
        ));
    }
    let latest_revision: Option<i32> = sqlx::query_scalar(
        "SELECT max(revision) FROM scorecard_snapshots
         WHERE scorecard_id = $1 AND config_version = $2 AND snapshot_date = $3",
    )
    .bind(&config.id)
    .bind(&config.version)
    .bind(snapshot_date)
    .fetch_one(&mut **tx)
    .await?;
    let latest_revision = latest_revision.ok_or_else(|| {
        ScorecardStoreError::Validation("correction predecessor disappeared".to_string())
    })?;
    if predecessor_revision != latest_revision {
        return Err(ScorecardStoreError::Validation(
            "correction must supersede the latest revision".to_string(),
        ));
    }
    let revision = u32::try_from(latest_revision + 1)
        .map_err(|_| ScorecardStoreError::Validation("snapshot revision overflow".into()))?;
    Ok((
        revision,
        Some(correction.supersedes_snapshot_id),
        Some(correction.reason),
    ))
}

async fn resolve_inputs(
    tx: &mut Transaction<'_, Postgres>,
    config: &ScorecardConfig,
    as_of: DateTime<Utc>,
) -> Result<Vec<ResolvedInput>, ScorecardStoreError> {
    let mut inputs = Vec::with_capacity(config.indicators.len());
    for indicator in &config.indicators {
        let dimensions = serde_json::to_value(&indicator.dimension_selector)?;
        let row = sqlx::query(
            "SELECT observation.value, observation.time, observation.series_key,
                    observation.source_artifact_id,
                    observation.ingestion_generation_id
             FROM series
             JOIN LATERAL (
                 SELECT value, time, series_key, source_artifact_id,
                        ingestion_generation_id, revision_no
                 FROM observations
                 WHERE observations.series_key = series.series_key AND time <= $4
                 ORDER BY time DESC, revision_no DESC LIMIT 1
             ) AS observation ON TRUE
             WHERE series.dataflow_id = $1 AND series.measure_id = $2
               AND series.dimensions = $3::JSONB AND series.active
             ORDER BY observation.time DESC, observation.series_key ASC LIMIT 1",
        )
        .bind(&indicator.source_dataflow_id)
        .bind(&indicator.measure_id)
        .bind(dimensions)
        .bind(as_of)
        .fetch_optional(&mut **tx)
        .await?;
        let observation = if let Some(row) = row {
            let time: DateTime<Utc> = row.try_get("time")?;
            let value: Option<f64> = row.try_get("value")?;
            let status = value.map_or(indicator.coverage_status, |_| {
                freshness_status(indicator, time, as_of)
            });
            IndicatorObservation {
                indicator_id: indicator.indicator_id.clone(),
                raw_value: value,
                coverage_status: status,
                latest_period: Some(time.date_naive().to_string()),
                series_key: Some(hex::encode(row.try_get::<Vec<u8>, _>("series_key")?)),
                source_artifact_id: Some(hex::encode(
                    row.try_get::<Vec<u8>, _>("source_artifact_id")?,
                )),
                ingestion_generation_id: row
                    .try_get::<Option<Uuid>, _>("ingestion_generation_id")?
                    .map(|id| id.to_string()),
                notes: None,
            }
        } else {
            IndicatorObservation::missing(indicator.indicator_id.clone(), indicator.coverage_status)
        };
        inputs.push(ResolvedInput { observation });
    }
    Ok(inputs)
}

fn freshness_status(
    indicator: &IndicatorConfig,
    observed_at: DateTime<Utc>,
    as_of: DateTime<Utc>,
) -> CoverageStatus {
    let age = as_of.signed_duration_since(observed_at);
    if age <= Duration::seconds(indicator.soft_after_seconds as i64) {
        CoverageStatus::Resolved
    } else if age <= Duration::seconds(indicator.hard_after_seconds as i64) {
        CoverageStatus::Stale
    } else {
        indicator.coverage_status
    }
}

async fn load_previous_score(
    tx: &mut Transaction<'_, Postgres>,
    config: &ScorecardConfig,
    snapshot_date: NaiveDate,
) -> Result<Option<f64>, ScorecardStoreError> {
    Ok(sqlx::query_scalar(
        "SELECT score FROM scorecard_snapshots_latest
         WHERE scorecard_id = $1 AND config_version = $2
           AND snapshot_date < $3 AND publication_state = 'published'
         ORDER BY snapshot_date DESC LIMIT 1",
    )
    .bind(&config.id)
    .bind(&config.version)
    .bind(snapshot_date)
    .fetch_optional(&mut **tx)
    .await?
    .flatten())
}

async fn persist_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    snapshot: &PublishedApsSnapshot,
    config_digest: [u8; 32],
    throughput_coverage: f64,
    orientation_coverage: f64,
    correction_reason: Option<&str>,
) -> Result<(), ScorecardStoreError> {
    let payload = serde_json::to_value(snapshot)?;
    let summary = serde_json::to_value(ApsSnapshotSummary::from(snapshot))?;
    sqlx::query(
        "INSERT INTO scorecard_snapshots (
             id, scorecard_id, config_version, config_digest, snapshot_date,
             revision, supersedes_snapshot_id, correction_reason, as_of,
             published_at, publication_state, score, zone,
             overall_coverage_pct, throughput_coverage_pct,
             orientation_coverage_pct, summary_payload, snapshot_payload
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)",
    )
    .bind(snapshot.id)
    .bind(&snapshot.scorecard_id)
    .bind(&snapshot.config_version)
    .bind(config_digest.as_slice())
    .bind(snapshot.snapshot_date)
    .bind(
        i32::try_from(snapshot.revision).map_err(|_| {
            ScorecardStoreError::Validation("snapshot revision exceeds INTEGER".into())
        })?,
    )
    .bind(snapshot.supersedes_snapshot_id)
    .bind(correction_reason)
    .bind(snapshot.as_of)
    .bind(snapshot.published_at)
    .bind(snapshot.publication_state.as_str())
    .bind(snapshot.score)
    .bind(snapshot.zone.map(zone_str))
    .bind(snapshot.coverage_pct)
    .bind(throughput_coverage)
    .bind(orientation_coverage)
    .bind(summary)
    .bind(payload)
    .execute(&mut **tx)
    .await?;
    for contribution in &snapshot.contributions {
        let series_key = decode_optional_digest(contribution.series_key.as_deref())?;
        let artifact_id = decode_optional_digest(contribution.source_artifact_id.as_deref())?;
        let generation_id = contribution
            .ingestion_generation_id
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|error| ScorecardStoreError::Validation(error.to_string()))?;
        sqlx::query(
            "INSERT INTO scorecard_snapshot_contributions (
                 snapshot_id, indicator_id, series_key, source_artifact_id,
                 ingestion_generation_id, contribution
             ) VALUES ($1,$2,$3,$4,$5,$6)",
        )
        .bind(snapshot.id)
        .bind(&contribution.indicator_id)
        .bind(series_key)
        .bind(artifact_id)
        .bind(generation_id)
        .bind(serde_json::to_value(contribution)?)
        .execute(&mut **tx)
        .await?;
        if let Some(generation_id) = generation_id {
            sqlx::query(
                "INSERT INTO scorecard_snapshot_generations (snapshot_id, generation_id)
                 VALUES ($1,$2) ON CONFLICT DO NOTHING",
            )
            .bind(snapshot.id)
            .bind(generation_id)
            .execute(&mut **tx)
            .await?;
        }
    }
    Ok(())
}

async fn load_existing_revision(
    tx: &mut Transaction<'_, Postgres>,
    snapshot_date: NaiveDate,
    revision: i32,
) -> Result<Option<PublishedApsSnapshot>, ScorecardStoreError> {
    let payload: Option<serde_json::Value> = sqlx::query_scalar(
        "SELECT snapshot_payload FROM scorecard_snapshots
         WHERE scorecard_id = 'aps' AND snapshot_date = $1 AND revision = $2",
    )
    .bind(snapshot_date)
    .bind(revision)
    .fetch_optional(&mut **tx)
    .await?;
    payload
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}

async fn load_payload_optional(
    pool: &PgPool,
    query: &str,
    id: Option<Uuid>,
) -> Result<Option<PublishedApsSnapshot>, ScorecardStoreError> {
    let mut query = sqlx::query_scalar::<_, serde_json::Value>(query);
    if let Some(id) = id {
        query = query.bind(id);
    }
    query
        .fetch_optional(pool)
        .await?
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}

fn history_relation(view: HistoryView) -> &'static str {
    match view {
        HistoryView::AsPublished => "scorecard_snapshots_as_published",
        HistoryView::Latest => "scorecard_snapshots_latest",
    }
}

fn snapshot_as_of(date: NaiveDate) -> Result<DateTime<Utc>, ScorecardStoreError> {
    let next_midnight = date
        .succ_opt()
        .and_then(|next| next.and_hms_opt(0, 0, 0))
        .ok_or_else(|| ScorecardStoreError::Validation("invalid snapshot date".into()))?;
    Sydney
        .from_local_datetime(&next_midnight)
        .single()
        .map(|value| value.with_timezone(&Utc) - Duration::milliseconds(1))
        .ok_or_else(|| ScorecardStoreError::Validation("ambiguous Sydney snapshot date".into()))
}

fn axis_coverage(sub_indexes: &[SubIndexScore], axis: Axis) -> f64 {
    sub_indexes
        .iter()
        .find(|sub_index| sub_index.axis == axis)
        .map_or(0.0, |sub_index| sub_index.coverage_pct)
}

fn zone_str(zone: ScoreZone) -> &'static str {
    match zone {
        ScoreZone::Red => "scarcity",
        ScoreZone::Yellow => "mixed",
        ScoreZone::Green => "abundance",
    }
}

fn decode_optional_digest(value: Option<&str>) -> Result<Option<Vec<u8>>, ScorecardStoreError> {
    value
        .map(|value| {
            let bytes = hex::decode(value)
                .map_err(|error| ScorecardStoreError::Validation(error.to_string()))?;
            if bytes.len() != 32 {
                return Err(ScorecardStoreError::Validation(
                    "scorecard provenance digest is not 32 bytes".into(),
                ));
            }
            Ok(bytes)
        })
        .transpose()
}
