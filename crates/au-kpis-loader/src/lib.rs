//! Observation upsert, revision tracking.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use au_kpis_domain::{
    Observation, ObservationStatus, SeriesDescriptor, TimePrecision,
    ids::{ArtifactId, SeriesKey},
};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use thiserror::Error;
use tracing::instrument;

const DEFAULT_MAX_ROWS: usize = 1_000;
const DEFAULT_MAX_BYTES: usize = 10 * 1024 * 1024;

/// A parsed observation paired with the series metadata needed by the loader.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadItem {
    /// Series metadata emitted by the source adapter.
    pub series: SeriesDescriptor,
    /// Observation for the series.
    pub observation: Observation,
}

/// Aggregate result for a loader run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LoadStats {
    /// Valid observations written through the staging table.
    pub observations_loaded: u64,
    /// Distinct valid series descriptors upserted.
    pub series_upserted: u64,
    /// Invalid rows recorded in `parse_errors`.
    pub parse_errors: u64,
    /// Number of database transactions used for valid batches.
    pub batches: u64,
}

/// Loader configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadOptions {
    /// Maximum observations per transaction.
    pub max_rows: usize,
    /// Maximum approximate COPY payload bytes per transaction.
    pub max_bytes: usize,
}

impl Default for LoadOptions {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_MAX_ROWS,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

/// Errors returned by the loader.
#[derive(Debug, Error)]
pub enum LoadError {
    /// Loader input was internally inconsistent.
    #[error("loader validation: {0}")]
    Validation(String),

    /// JSON conversion for staged dimensions or attributes failed.
    #[error("loader json: {0}")]
    Json(#[from] serde_json::Error),

    /// A database operation failed.
    #[error("loader db: {0}")]
    Db(#[from] sqlx::Error),
}

/// Load observations using the spec default 1000-row / 10 MB batch limits.
#[instrument(skip(pool, items))]
pub async fn load_batch(pool: &PgPool, items: Vec<LoadItem>) -> Result<LoadStats, LoadError> {
    load_batch_with_options(pool, items, LoadOptions::default()).await
}

/// Load observations using explicit batch limits.
#[instrument(skip(pool, items))]
pub async fn load_batch_with_options(
    pool: &PgPool,
    items: Vec<LoadItem>,
    options: LoadOptions,
) -> Result<LoadStats, LoadError> {
    if options.max_rows == 0 {
        return Err(LoadError::Validation(
            "max_rows must be greater than 0".into(),
        ));
    }
    if options.max_bytes == 0 {
        return Err(LoadError::Validation(
            "max_bytes must be greater than 0".into(),
        ));
    }

    let mut stats = LoadStats::default();
    let mut valid_items = Vec::with_capacity(items.len());

    for item in items {
        match validate_item(&item) {
            Ok(()) => {
                valid_items.push(item);
            }
            Err(message) => {
                record_loader_validation_error(
                    pool,
                    item.observation.source_artifact_id,
                    &message,
                    &item,
                )
                .await?;
                stats.parse_errors += 1;
            }
        }
    }

    if valid_items.is_empty() {
        return Ok(stats);
    }

    upsert_series_batch(pool, &valid_items, &mut stats).await?;

    let mut valid_batch = Vec::new();
    let mut valid_batch_bytes = 0usize;
    for item in valid_items {
        let estimated_bytes = estimate_item_bytes(&item)?;
        if !valid_batch.is_empty()
            && (valid_batch.len() >= options.max_rows
                || valid_batch_bytes + estimated_bytes > options.max_bytes)
        {
            load_observation_batch(pool, &valid_batch, &mut stats).await?;
            valid_batch.clear();
            valid_batch_bytes = 0;
        }
        valid_batch_bytes += estimated_bytes;
        valid_batch.push(item);
    }

    if !valid_batch.is_empty() {
        load_observation_batch(pool, &valid_batch, &mut stats).await?;
    }
    Ok(stats)
}

fn validate_item(item: &LoadItem) -> Result<(), String> {
    let computed = item.series.compute_series_key();
    if item.series.series_key != computed {
        return Err(format!(
            "series descriptor key {} does not match computed key {}",
            item.series.series_key, computed
        ));
    }
    if item.observation.series_key != item.series.series_key {
        return Err(format!(
            "observation series key {} does not match descriptor key {}",
            item.observation.series_key, item.series.series_key
        ));
    }
    Ok(())
}

fn estimate_item_bytes(item: &LoadItem) -> Result<usize, LoadError> {
    let dimensions = descriptor_dimensions_json(&item.series)?;
    let attributes = serde_json::to_string(&item.observation.attributes)?;
    Ok(256 + dimensions.len() + attributes.len())
}

async fn upsert_series_batch(
    pool: &PgPool,
    batch: &[LoadItem],
    stats: &mut LoadStats,
) -> Result<(), LoadError> {
    let mut tx = pool.begin().await?;
    create_series_staging_table(&mut tx).await?;
    copy_series(&mut tx, batch).await?;
    let series_upserted = upsert_series(&mut tx).await?;
    tx.commit().await?;

    stats.series_upserted += series_upserted;
    Ok(())
}

async fn load_observation_batch(
    pool: &PgPool,
    batch: &[LoadItem],
    stats: &mut LoadStats,
) -> Result<(), LoadError> {
    let mut tx = pool.begin().await?;
    create_observation_staging_table(&mut tx).await?;
    copy_observations(&mut tx, batch).await?;
    let observations_loaded = upsert_observations(&mut tx).await?;
    tx.commit().await?;

    stats.observations_loaded += observations_loaded;
    stats.batches += 1;
    Ok(())
}

async fn create_series_staging_table(tx: &mut Transaction<'_, Postgres>) -> Result<(), LoadError> {
    sqlx::query(
        "CREATE TEMP TABLE staging_series (
             series_key_hex TEXT NOT NULL,
             dataflow_id TEXT NOT NULL,
             measure_id TEXT NOT NULL,
             dimensions JSONB NOT NULL,
             unit TEXT NOT NULL,
             first_observed TIMESTAMPTZ NOT NULL,
             last_observed TIMESTAMPTZ NOT NULL
         ) ON COMMIT DROP",
    )
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn create_observation_staging_table(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), LoadError> {
    sqlx::query(
        "CREATE TEMP TABLE staging_observations (
             series_key_hex TEXT NOT NULL,
             time TIMESTAMPTZ NOT NULL,
             revision_no INTEGER NOT NULL,
             time_precision TEXT NOT NULL,
             value DOUBLE PRECISION,
             status TEXT NOT NULL,
             attributes JSONB NOT NULL,
             ingested_at TIMESTAMPTZ NOT NULL,
             source_artifact_hex TEXT NOT NULL
         ) ON COMMIT DROP",
    )
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn copy_series(
    tx: &mut Transaction<'_, Postgres>,
    batch: &[LoadItem],
) -> Result<(), LoadError> {
    let mut rows = BTreeMap::<SeriesKey, SeriesStageRow>::new();
    for item in batch {
        let dimensions_json = descriptor_dimensions_json(&item.series)?;
        match rows.entry(item.series.series_key) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let row = entry.get_mut();
                row.first_observed = row.first_observed.min(item.observation.time);
                row.last_observed = row.last_observed.max(item.observation.time);
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(SeriesStageRow {
                    descriptor: item.series.clone(),
                    dimensions_json,
                    first_observed: item.observation.time,
                    last_observed: item.observation.time,
                });
            }
        }
    }

    let payload = rows.values().try_fold(String::new(), |mut payload, row| {
        push_copy_fields(
            &mut payload,
            [
                row.descriptor.series_key.to_hex(),
                row.descriptor.dataflow_id.to_string(),
                row.descriptor.measure_id.to_string(),
                row.dimensions_json.clone(),
                row.descriptor.unit.clone(),
                row.first_observed.to_rfc3339(),
                row.last_observed.to_rfc3339(),
            ],
        );
        Ok::<_, LoadError>(payload)
    })?;

    let mut copy = tx
        .as_mut()
        .copy_in_raw(
            "COPY staging_series (
                 series_key_hex, dataflow_id, measure_id, dimensions, unit,
                 first_observed, last_observed
             ) FROM STDIN",
        )
        .await?;
    copy.send(payload.as_bytes()).await?;
    copy.finish().await?;

    Ok(())
}

async fn upsert_series(tx: &mut Transaction<'_, Postgres>) -> Result<u64, LoadError> {
    let result = sqlx::query(
        "INSERT INTO series (
             series_key, dataflow_id, measure_id, dimensions, unit,
             first_observed, last_observed, active
         )
         SELECT decode(series_key_hex, 'hex'), dataflow_id, measure_id, dimensions,
                unit, min(first_observed), max(last_observed), TRUE
         FROM staging_series
         GROUP BY series_key_hex, dataflow_id, measure_id, dimensions, unit
         ON CONFLICT (series_key) DO UPDATE
         SET first_observed = LEAST(series.first_observed, EXCLUDED.first_observed),
             last_observed = GREATEST(series.last_observed, EXCLUDED.last_observed),
             updated_at = now()",
    )
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected())
}

async fn copy_observations(
    tx: &mut Transaction<'_, Postgres>,
    batch: &[LoadItem],
) -> Result<(), LoadError> {
    let mut payload = String::new();
    for item in batch {
        let obs = &item.observation;
        push_copy_fields(
            &mut payload,
            [
                obs.series_key.to_hex(),
                obs.time.to_rfc3339(),
                obs.revision_no.to_string(),
                time_precision_db(obs.time_precision).to_string(),
                obs.value
                    .map_or_else(|| "\\N".to_string(), |value| value.to_string()),
                observation_status_db(obs.status).to_string(),
                serde_json::to_string(&obs.attributes)?,
                obs.ingested_at.to_rfc3339(),
                obs.source_artifact_id.to_hex(),
            ],
        );
    }

    let mut copy = tx
        .as_mut()
        .copy_in_raw(
            "COPY staging_observations (
                 series_key_hex, time, revision_no, time_precision, value,
                 status, attributes, ingested_at, source_artifact_hex
             ) FROM STDIN",
        )
        .await?;
    copy.send(payload.as_bytes()).await?;
    copy.finish().await?;

    Ok(())
}

async fn upsert_observations(tx: &mut Transaction<'_, Postgres>) -> Result<u64, LoadError> {
    let result = sqlx::query(
        "INSERT INTO observations (
             series_key, time, revision_no, time_precision, value, status,
             attributes, ingested_at, source_artifact_id
         )
         SELECT decode(series_key_hex, 'hex'), time, revision_no, time_precision,
                value, status, attributes, ingested_at,
                decode(source_artifact_hex, 'hex')
         FROM staging_observations
         ON CONFLICT (series_key, time, revision_no) DO UPDATE
         SET time_precision = EXCLUDED.time_precision,
             value = EXCLUDED.value,
             status = EXCLUDED.status,
             attributes = EXCLUDED.attributes,
             ingested_at = EXCLUDED.ingested_at,
             source_artifact_id = EXCLUDED.source_artifact_id",
    )
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected())
}

/// Record a parser failure against the source artifact for audit and reprocessing.
pub async fn record_parse_error(
    pool: &PgPool,
    artifact_id: ArtifactId,
    error_kind: &str,
    error_message: &str,
    row_context: Option<Value>,
) -> Result<(), LoadError> {
    sqlx::query(
        "INSERT INTO parse_errors (artifact_id, error_kind, error_message, row_context)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(artifact_id.digest().as_bytes().as_slice())
    .bind(error_kind)
    .bind(error_message)
    .bind(row_context)
    .execute(pool)
    .await?;

    Ok(())
}

async fn record_loader_validation_error(
    pool: &PgPool,
    artifact_id: ArtifactId,
    message: &str,
    item: &LoadItem,
) -> Result<(), LoadError> {
    let row_context = serde_json::json!({
        "dataflow_id": item.series.dataflow_id,
        "series_key": item.series.series_key,
        "observation_time": item.observation.time,
        "revision_no": item.observation.revision_no,
    });

    record_parse_error(
        pool,
        artifact_id,
        "loader_validation",
        message,
        Some(row_context),
    )
    .await
}

#[derive(Debug)]
struct SeriesStageRow {
    descriptor: SeriesDescriptor,
    dimensions_json: String,
    first_observed: chrono::DateTime<chrono::Utc>,
    last_observed: chrono::DateTime<chrono::Utc>,
}

fn descriptor_dimensions_json(descriptor: &SeriesDescriptor) -> Result<String, serde_json::Error> {
    let dimensions: BTreeMap<&str, &str> = descriptor
        .dimensions
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    serde_json::to_string(&dimensions)
}

fn push_copy_fields<const N: usize>(payload: &mut String, fields: [String; N]) {
    for (index, field) in fields.into_iter().enumerate() {
        if index > 0 {
            payload.push('\t');
        }
        if field == "\\N" {
            payload.push_str("\\N");
        } else {
            escape_copy_field(payload, &field);
        }
    }
    payload.push('\n');
}

fn escape_copy_field(payload: &mut String, field: &str) {
    for ch in field.chars() {
        match ch {
            '\\' => payload.push_str("\\\\"),
            '\t' => payload.push_str("\\t"),
            '\n' => payload.push_str("\\n"),
            '\r' => payload.push_str("\\r"),
            _ => payload.push(ch),
        }
    }
}

fn time_precision_db(value: TimePrecision) -> &'static str {
    match value {
        TimePrecision::Day => "day",
        TimePrecision::Week => "week",
        TimePrecision::Month => "month",
        TimePrecision::Quarter => "quarter",
        TimePrecision::Year => "year",
    }
}

fn observation_status_db(value: ObservationStatus) -> &'static str {
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
