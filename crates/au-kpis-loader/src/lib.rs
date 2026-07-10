//! Observation upsert, revision tracking.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashSet};

use au_kpis_domain::{
    Observation, ObservationStatus, SeriesDescriptor, TimePrecision,
    ids::{ArtifactId, SeriesKey},
};
use serde_json::Value;
use sqlx::{Acquire, PgPool, Postgres, Transaction, pool::PoolConnection};
use thiserror::Error;
use tracing::instrument;

mod generation;

pub use generation::{GenerationPublication, publish_ingestion_generation};

const DEFAULT_MAX_ROWS: usize = 1_000;
const DEFAULT_MAX_BYTES: usize = 10 * 1024 * 1024;
const WEBHOOK_EVENT_DATA_UPDATED: &str = "data.updated";
const WEBHOOK_DELIVERY_MAX_ATTEMPTS: i32 = 5;

/// A parsed observation paired with the series metadata needed by the loader.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadItem {
    /// Series metadata emitted by the source adapter.
    pub series: SeriesDescriptor,
    /// Observation for the series.
    pub observation: Observation,
}

/// Loader item plus optional audit fields to merge into validation errors.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadItemAudit {
    /// Parsed row to validate and load.
    pub item: LoadItem,
    /// Extra row-level audit context for `parse_errors.row_context`.
    pub row_context: Option<Value>,
}

/// Session-scoped loader staging state for one source artifact.
///
/// Parsed rows can be staged incrementally to keep the hot path bounded, then
/// promoted only after the parser accepts the full artifact. Each staged COPY
/// chunk uses its own transaction; the temporary staging tables live on a
/// dedicated connection that is cleaned and returned to the pool when the
/// artifact is accepted or rejected.
#[derive(Debug)]
pub struct StagedLoad {
    conn: PoolConnection<Postgres>,
    stats: LoadStats,
    promotion_max_rows: usize,
    cleaned: bool,
}

impl From<LoadItem> for LoadItemAudit {
    fn from(item: LoadItem) -> Self {
        Self {
            item,
            row_context: None,
        }
    }
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
    /// Number of valid observation COPY batches.
    pub batches: u64,
}

/// Result of validating load-item database references before COPY.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadReferenceValidation {
    /// Rows that passed reference validation and may continue to COPY.
    pub valid_rows: Vec<bool>,
    /// Invalid references recorded in `parse_errors`.
    pub stats: LoadStats,
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

    /// Durable ingestion state was invalid or unavailable.
    #[error("loader durable state: {0}")]
    Durable(#[from] au_kpis_db::DbError),
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
    load_batch_with_options_and_audit_context(
        pool,
        items.into_iter().map(Into::into).collect(),
        options,
    )
    .await
}

/// Load observations using explicit batch limits and audit context for rejected rows.
#[instrument(skip(pool, items))]
pub async fn load_batch_with_options_and_audit_context(
    pool: &PgPool,
    items: Vec<LoadItemAudit>,
    options: LoadOptions,
) -> Result<LoadStats, LoadError> {
    validate_options(options)?;

    let mut stats = LoadStats::default();
    let mut valid_items = Vec::with_capacity(items.len());

    for audited in items {
        match validate_item(&audited.item) {
            Ok(()) => {
                valid_items.push(audited.item);
            }
            Err(message) => {
                record_loader_validation_error(
                    pool,
                    audited.item.observation.source_artifact_id,
                    &message,
                    &audited.item,
                    audited.row_context,
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
        let estimated_bytes = estimate_load_item_bytes(&item)?;
        if should_flush_load_batch(
            valid_batch.len(),
            valid_batch_bytes,
            estimated_bytes,
            options,
        ) {
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

/// Validate database references that would otherwise fail after batching.
#[instrument(skip(pool, items))]
pub async fn validate_load_references(
    pool: &PgPool,
    items: &[LoadItemAudit],
) -> Result<LoadReferenceValidation, LoadError> {
    let rows = load_reference_rows(pool, items).await?;
    let mut validation = empty_reference_validation(items.len());
    for (idx, missing) in missing_references(items, rows) {
        let audited = &items[idx];
        record_loader_validation_error(
            pool,
            audited.item.observation.source_artifact_id,
            &format!("missing loader reference: {}", missing.join(", ")),
            &audited.item,
            audited.row_context.clone(),
        )
        .await?;
        validation.valid_rows[idx] = false;
        validation.stats.parse_errors += 1;
    }

    Ok(validation)
}

async fn validate_load_references_on_connection(
    conn: &mut PoolConnection<Postgres>,
    items: Vec<LoadItemAudit>,
) -> Result<(Vec<LoadItem>, LoadStats), LoadError> {
    let rows = load_reference_rows_on_connection(conn, &items).await?;
    let mut validation = empty_reference_validation(items.len());
    for (idx, missing) in missing_references(&items, rows) {
        let audited = &items[idx];
        record_loader_validation_error_on_connection(
            conn,
            audited.item.observation.source_artifact_id,
            &format!("missing loader reference: {}", missing.join(", ")),
            &audited.item,
            audited.row_context.clone(),
        )
        .await?;
        validation.valid_rows[idx] = false;
        validation.stats.parse_errors += 1;
    }

    let valid_items = items
        .into_iter()
        .zip(validation.valid_rows.iter())
        .filter_map(|(audited, valid)| valid.then_some(audited.item))
        .collect();

    Ok((valid_items, validation.stats))
}

type ReferenceRow = (i64, bool, bool, bool);

async fn load_reference_rows(
    pool: &PgPool,
    items: &[LoadItemAudit],
) -> Result<Vec<ReferenceRow>, LoadError> {
    if items.is_empty() {
        return Ok(Vec::new());
    }

    let (dataflow_ids, measure_ids, artifact_ids) = reference_validation_inputs(items);
    sqlx::query_as(
        "WITH input AS (
             SELECT *
             FROM UNNEST($1::text[], $2::text[], $3::bytea[])
                  WITH ORDINALITY AS item(dataflow_id, measure_id, artifact_id, ord)
         )
         SELECT
             input.ord::BIGINT,
             dataflows.id IS NOT NULL,
             measures.id IS NOT NULL,
             artifacts.id IS NOT NULL
         FROM input
         LEFT JOIN dataflows ON dataflows.id = input.dataflow_id
         LEFT JOIN measures ON measures.id = input.measure_id
         LEFT JOIN artifacts ON artifacts.id = input.artifact_id
         ORDER BY input.ord",
    )
    .bind(dataflow_ids)
    .bind(measure_ids)
    .bind(artifact_ids)
    .fetch_all(pool)
    .await
    .map_err(Into::into)
}

async fn load_reference_rows_on_connection(
    conn: &mut PoolConnection<Postgres>,
    items: &[LoadItemAudit],
) -> Result<Vec<ReferenceRow>, LoadError> {
    if items.is_empty() {
        return Ok(Vec::new());
    }

    let (dataflow_ids, measure_ids, artifact_ids) = reference_validation_inputs(items);
    sqlx::query_as(
        "WITH input AS (
             SELECT *
             FROM UNNEST($1::text[], $2::text[], $3::bytea[])
                  WITH ORDINALITY AS item(dataflow_id, measure_id, artifact_id, ord)
         )
         SELECT
             input.ord::BIGINT,
             dataflows.id IS NOT NULL,
             measures.id IS NOT NULL,
             artifacts.id IS NOT NULL
         FROM input
         LEFT JOIN dataflows ON dataflows.id = input.dataflow_id
         LEFT JOIN measures ON measures.id = input.measure_id
         LEFT JOIN artifacts ON artifacts.id = input.artifact_id
         ORDER BY input.ord",
    )
    .bind(dataflow_ids)
    .bind(measure_ids)
    .bind(artifact_ids)
    .fetch_all(&mut **conn)
    .await
    .map_err(Into::into)
}

fn reference_validation_inputs(
    items: &[LoadItemAudit],
) -> (Vec<String>, Vec<String>, Vec<Vec<u8>>) {
    let dataflow_ids = items
        .iter()
        .map(|item| item.item.series.dataflow_id.to_string())
        .collect();
    let measure_ids = items
        .iter()
        .map(|item| item.item.series.measure_id.to_string())
        .collect();
    let artifact_ids = items
        .iter()
        .map(|item| {
            item.item
                .observation
                .source_artifact_id
                .digest()
                .as_bytes()
                .to_vec()
        })
        .collect();
    (dataflow_ids, measure_ids, artifact_ids)
}

fn empty_reference_validation(row_count: usize) -> LoadReferenceValidation {
    LoadReferenceValidation {
        valid_rows: vec![true; row_count],
        stats: LoadStats::default(),
    }
}

fn missing_references(
    items: &[LoadItemAudit],
    rows: Vec<ReferenceRow>,
) -> impl Iterator<Item = (usize, Vec<String>)> + '_ {
    rows.into_iter()
        .filter_map(|(ord, dataflow, measure, artifact)| {
            let idx = usize::try_from(ord).ok()?.checked_sub(1)?;
            let item = items.get(idx)?;
            let mut missing = Vec::new();
            if !dataflow {
                missing.push(format!("dataflow `{}`", item.item.series.dataflow_id));
            }
            if !measure {
                missing.push(format!("measure `{}`", item.item.series.measure_id));
            }
            if !artifact {
                missing.push(format!(
                    "artifact `{}`",
                    item.item.observation.source_artifact_id
                ));
            }
            (!missing.is_empty()).then_some((idx, missing))
        })
}

/// Start a staged load session for one artifact.
#[instrument(skip(pool))]
pub async fn begin_staged_load(
    pool: &PgPool,
    options: LoadOptions,
) -> Result<StagedLoad, LoadError> {
    validate_options(options)?;

    let mut conn = pool.acquire().await?;
    drop_staging_tables(&mut conn).await?;
    let mut tx = (&mut conn).begin().await?;
    create_series_staging_table_with_on_commit(&mut tx, "PRESERVE ROWS").await?;
    create_observation_staging_table_with_on_commit(&mut tx, "PRESERVE ROWS", true).await?;
    tx.commit().await?;

    Ok(StagedLoad {
        conn,
        stats: LoadStats::default(),
        promotion_max_rows: options.max_rows,
        cleaned: false,
    })
}

impl StagedLoad {
    /// Append a validated COPY chunk to this artifact's staging tables.
    pub async fn stage(&mut self, items: Vec<LoadItemAudit>) -> Result<(), LoadError> {
        let mut valid_items = Vec::with_capacity(items.len());

        for audited in items {
            match validate_item(&audited.item) {
                Ok(()) => {
                    valid_items.push(audited);
                }
                Err(message) => {
                    record_loader_validation_error_on_connection(
                        &mut self.conn,
                        audited.item.observation.source_artifact_id,
                        &message,
                        &audited.item,
                        audited.row_context,
                    )
                    .await?;
                    self.stats.parse_errors += 1;
                }
            }
        }

        let (valid_items, reference_stats) =
            validate_load_references_on_connection(&mut self.conn, valid_items).await?;
        self.stats.parse_errors += reference_stats.parse_errors;

        if valid_items.is_empty() {
            return Ok(());
        }

        let result = async {
            let mut tx = (&mut self.conn).begin().await?;
            copy_series(&mut tx, &valid_items).await?;
            copy_observations(&mut tx, &valid_items).await?;
            tx.commit().await?;
            Ok(())
        }
        .await;
        if let Err(err) = result {
            return match drop_staging_tables(&mut self.conn).await {
                Ok(()) => Err(err),
                Err(cleanup_err) => Err(cleanup_err),
            };
        }
        self.stats.batches += 1;
        Ok(())
    }

    /// Promote all staged rows into durable tables.
    pub async fn commit(mut self) -> Result<LoadStats, LoadError> {
        let result = async {
            let mut tx = (&mut self.conn).begin().await?;
            self.stats.series_upserted += upsert_series(&mut tx).await?;
            tx.commit().await?;
            self.stats.observations_loaded +=
                upsert_observations_in_chunk_transactions(&mut self.conn, self.promotion_max_rows)
                    .await?;
            Ok(self.stats)
        }
        .await;
        let cleanup = drop_staging_tables(&mut self.conn).await;
        if cleanup.is_ok() {
            self.cleaned = true;
        }
        match (result, cleanup) {
            (Ok(stats), Ok(())) => Ok(stats),
            (Err(err), _) | (Ok(_), Err(err)) => Err(err),
        }
    }

    /// Drop staged rows for a rejected artifact and surface any loader
    /// validation errors that were already recorded against `parse_errors` so
    /// the caller can fold them into pipeline stats.
    pub async fn rollback(mut self) -> Result<LoadStats, LoadError> {
        let stats = self.stats;
        drop_staging_tables(&mut self.conn).await?;
        self.cleaned = true;
        Ok(stats)
    }
}

impl Drop for StagedLoad {
    fn drop(&mut self) {
        if !self.cleaned {
            self.conn.close_on_drop();
        }
    }
}

fn validate_options(options: LoadOptions) -> Result<(), LoadError> {
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
    Ok(())
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

/// Estimate the COPY payload size contribution for one load item.
///
/// The ingestion orchestrator uses this loader-owned estimator only to decide
/// when an in-memory artifact buffer should be handed to the loader; the loader
/// remains the source of truth for COPY batch boundaries.
pub fn estimate_load_item_bytes(item: &LoadItem) -> Result<usize, LoadError> {
    let dimensions = descriptor_dimensions_json(&item.series)?;
    let attributes = serde_json::to_string(&item.observation.attributes)?;
    Ok(256 + dimensions.len() + attributes.len())
}

/// Return whether adding the next item should flush the current COPY batch.
#[must_use]
pub fn should_flush_load_batch(
    batch_len: usize,
    batch_bytes: usize,
    next_item_bytes: usize,
    options: LoadOptions,
) -> bool {
    batch_len != 0
        && (batch_len >= options.max_rows
            || batch_bytes.saturating_add(next_item_bytes) > options.max_bytes)
}

/// Return whether the current batch has reached a configured COPY boundary.
#[must_use]
pub fn load_batch_boundary_reached(
    batch_len: usize,
    batch_bytes: usize,
    options: LoadOptions,
) -> bool {
    batch_len >= options.max_rows || batch_bytes >= options.max_bytes
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
    create_series_staging_table(&mut tx).await?;
    create_observation_staging_table(&mut tx).await?;
    copy_series(&mut tx, batch).await?;
    let staged_rows = copy_direct_observations(&mut tx, batch).await?;
    let observations_loaded = upsert_observations(&mut tx, staged_rows).await?;
    tx.commit().await?;

    stats.observations_loaded += observations_loaded;
    stats.batches += 1;
    Ok(())
}

async fn create_series_staging_table(tx: &mut Transaction<'_, Postgres>) -> Result<(), LoadError> {
    create_series_staging_table_with_on_commit(tx, "DROP").await
}

async fn drop_staging_tables(conn: &mut PoolConnection<Postgres>) -> Result<(), LoadError> {
    sqlx::query("DROP TABLE IF EXISTS staging_observations, staging_series")
        .execute(&mut **conn)
        .await?;
    Ok(())
}

async fn create_series_staging_table_with_on_commit(
    tx: &mut Transaction<'_, Postgres>,
    on_commit: &str,
) -> Result<(), LoadError> {
    let query = format!(
        "CREATE TEMP TABLE staging_series (
             series_key_hex TEXT NOT NULL,
             dataflow_id TEXT NOT NULL,
             measure_id TEXT NOT NULL,
             dimensions JSONB NOT NULL,
             unit TEXT NOT NULL,
             first_observed TIMESTAMPTZ NOT NULL,
             last_observed TIMESTAMPTZ NOT NULL
         ) ON COMMIT {on_commit}",
    );
    sqlx::query(&query).execute(&mut **tx).await?;

    Ok(())
}

async fn create_observation_staging_table(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), LoadError> {
    create_observation_staging_table_with_on_commit(tx, "DROP", false).await
}

async fn create_observation_staging_table_with_on_commit(
    tx: &mut Transaction<'_, Postgres>,
    on_commit: &str,
    include_stage_row_id: bool,
) -> Result<(), LoadError> {
    let stage_row_id = if include_stage_row_id {
        "stage_row_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
    } else {
        ""
    };
    let query = format!(
        "CREATE TEMP TABLE staging_observations (
             {stage_row_id}
             series_key_hex TEXT NOT NULL,
             time TIMESTAMPTZ NOT NULL,
             revision_no INTEGER NOT NULL,
             time_precision TEXT NOT NULL,
             value DOUBLE PRECISION,
             status TEXT NOT NULL,
             attributes JSONB NOT NULL,
             ingested_at TIMESTAMPTZ NOT NULL,
             source_artifact_hex TEXT NOT NULL
         ) ON COMMIT {on_commit}",
    );
    sqlx::query(&query).execute(&mut **tx).await?;

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
    let payload = observation_copy_payload(batch.iter())?;
    copy_observation_payload(tx, payload).await
}

async fn copy_observations_deduped(
    tx: &mut Transaction<'_, Postgres>,
    batch: &[LoadItem],
) -> Result<usize, LoadError> {
    let rows = dedupe_observations_by_series_time(batch);
    let row_count = rows.len();
    let payload = observation_copy_payload(rows.into_iter())?;
    copy_observation_payload(tx, payload).await?;
    Ok(row_count)
}

async fn copy_direct_observations(
    tx: &mut Transaction<'_, Postgres>,
    batch: &[LoadItem],
) -> Result<usize, LoadError> {
    if has_duplicate_observation_times(batch) {
        copy_observations_deduped(tx, batch).await
    } else {
        copy_observations(tx, batch).await?;
        Ok(batch.len())
    }
}

fn has_duplicate_observation_times(batch: &[LoadItem]) -> bool {
    let mut seen = HashSet::with_capacity(batch.len());
    batch
        .iter()
        .any(|item| !seen.insert((item.observation.series_key, item.observation.time)))
}

fn dedupe_observations_by_series_time(batch: &[LoadItem]) -> Vec<&LoadItem> {
    let mut rows: BTreeMap<(SeriesKey, chrono::DateTime<chrono::Utc>), (usize, &LoadItem)> =
        BTreeMap::new();
    for (index, item) in batch.iter().enumerate() {
        let key = (item.observation.series_key, item.observation.time);
        match rows.entry(key) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let (_, previous) = entry.get();
                if item.observation.revision_no >= previous.observation.revision_no {
                    entry.insert((index, item));
                }
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((index, item));
            }
        }
    }
    rows.into_values().map(|(_, item)| item).collect()
}

fn observation_copy_payload<'a>(
    rows: impl IntoIterator<Item = &'a LoadItem>,
) -> Result<String, LoadError> {
    let mut payload = String::new();
    for item in rows {
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
    Ok(payload)
}

async fn copy_observation_payload(
    tx: &mut Transaction<'_, Postgres>,
    payload: String,
) -> Result<(), LoadError> {
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

async fn upsert_observations(
    tx: &mut Transaction<'_, Postgres>,
    staged_row_count: usize,
) -> Result<u64, LoadError> {
    let staged_row_count = u64::try_from(staged_row_count)
        .map_err(|_| LoadError::Validation("staged observation count exceeded u64 range".into()))?;
    let inserted = insert_new_observations(tx).await?;
    let observations_loaded = if inserted == staged_row_count {
        inserted
    } else {
        inserted + upsert_observation_revisions(tx).await?
    };
    enqueue_webhook_deliveries_for_observations(tx, observations_loaded).await?;
    Ok(observations_loaded)
}

async fn insert_new_observations(tx: &mut Transaction<'_, Postgres>) -> Result<u64, LoadError> {
    let result = sqlx::query(
        "INSERT INTO observations (
             series_key, time, revision_no, time_precision, value, status,
             attributes, ingested_at, source_artifact_id
         )
         SELECT decode(series_key_hex, 'hex'), time, 0, time_precision,
                value, status, attributes, ingested_at,
                decode(source_artifact_hex, 'hex')
         FROM staging_observations
         ON CONFLICT (series_key, time, revision_no) DO NOTHING",
    )
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected())
}

async fn upsert_observation_revisions(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<u64, LoadError> {
    // Adapter revision numbers are provisional. Persist only changed staged rows
    // and derive the effective revision from the existing chain.
    let result = sqlx::query(
        "WITH staged_rows AS MATERIALIZED (
             SELECT decode(series_key_hex, 'hex') AS series_key,
                    time,
                    time_precision,
                    value,
                    status,
                    attributes,
                    ingested_at,
                    decode(source_artifact_hex, 'hex') AS source_artifact_id
             FROM staging_observations
         ),
         changed_rows AS MATERIALIZED (
             SELECT staged.*
             FROM staged_rows staged
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM observations existing
                 WHERE existing.series_key = staged.series_key
                   AND existing.time = staged.time
                   AND existing.time_precision = staged.time_precision
                   AND existing.value IS NOT DISTINCT FROM staged.value
                   AND existing.status = staged.status
                   AND existing.attributes = staged.attributes
                   AND existing.source_artifact_id = staged.source_artifact_id
             )
         ),
         assigned_rows AS MATERIALIZED (
             SELECT staged.series_key,
                    staged.time,
                    (COALESCE(max(existing.revision_no), -1) + 1)::INTEGER AS revision_no,
                    staged.time_precision,
                    staged.value,
                    staged.status,
                    staged.attributes,
                    staged.ingested_at,
                    staged.source_artifact_id
             FROM changed_rows staged
             LEFT JOIN observations existing
               ON existing.series_key = staged.series_key
              AND existing.time = staged.time
             GROUP BY staged.series_key, staged.time, staged.time_precision,
                      staged.value, staged.status, staged.attributes,
                      staged.ingested_at, staged.source_artifact_id
         )
         INSERT INTO observations (
             series_key, time, revision_no, time_precision, value, status,
             attributes, ingested_at, source_artifact_id
         )
         SELECT series_key, time, revision_no, time_precision, value, status,
                attributes, ingested_at, source_artifact_id
         FROM assigned_rows
         ON CONFLICT (series_key, time, revision_no) DO NOTHING",
    )
    .execute(&mut **tx)
    .await?;

    Ok(result.rows_affected())
}

async fn upsert_observations_in_chunk_transactions(
    conn: &mut PoolConnection<Postgres>,
    max_rows: usize,
) -> Result<u64, LoadError> {
    let chunk_limit = chunk_limit(max_rows)?;
    let final_stage_row_id = max_staged_observation_row_id(conn).await?.unwrap_or(0);
    if final_stage_row_id == 0 {
        return Ok(0);
    }

    let mut last_stage_row_id = 0_i64;
    let mut observations_loaded = 0_u64;
    loop {
        let mut tx = (&mut *conn).begin().await?;
        let (max_stage_row_id, chunk_rows_loaded, had_rows) =
            upsert_next_observation_chunk(&mut tx, last_stage_row_id, chunk_limit).await?;

        if !had_rows {
            tx.commit().await?;
            break;
        }

        observations_loaded += chunk_rows_loaded;
        last_stage_row_id = max_stage_row_id;

        if last_stage_row_id >= final_stage_row_id {
            enqueue_webhook_deliveries_for_observations(&mut tx, observations_loaded).await?;
            tx.commit().await?;
            break;
        }

        tx.commit().await?;
    }

    Ok(observations_loaded)
}

async fn max_staged_observation_row_id(
    conn: &mut PoolConnection<Postgres>,
) -> Result<Option<i64>, LoadError> {
    sqlx::query_scalar("SELECT max(stage_row_id) FROM staging_observations")
        .fetch_one(&mut **conn)
        .await
        .map_err(LoadError::from)
}

fn chunk_limit(max_rows: usize) -> Result<i64, LoadError> {
    let chunk_limit = i64::try_from(max_rows)
        .map_err(|_| LoadError::Validation("max_rows exceeds the database integer range".into()))?;
    if chunk_limit <= 0 {
        return Err(LoadError::Validation(
            "max_rows must be greater than 0".into(),
        ));
    }
    Ok(chunk_limit)
}

async fn upsert_next_observation_chunk(
    tx: &mut Transaction<'_, Postgres>,
    last_stage_row_id: i64,
    chunk_limit: i64,
) -> Result<(i64, u64, bool), LoadError> {
    // Keep staged promotion semantics aligned with direct batch loads: staged
    // rows are deduped per series/time, exact replays are skipped, and changed
    // rows append to the existing revision chain.
    let (max_stage_row_id, chunk_rows_loaded, had_rows): (i64, i64, bool) = sqlx::query_as(
        "WITH next_rows AS MATERIALIZED (
             SELECT stage_row_id, series_key_hex, time, revision_no, time_precision,
                    value, status, attributes, ingested_at, source_artifact_hex
             FROM staging_observations
             WHERE stage_row_id > $1
             ORDER BY stage_row_id
             LIMIT $2
         ),
         deduped_rows AS MATERIALIZED (
             SELECT DISTINCT ON (series_key_hex, time)
                    stage_row_id,
                    decode(series_key_hex, 'hex') AS series_key,
                    time,
                    time_precision,
                    value,
                    status,
                    attributes,
                    ingested_at,
                    decode(source_artifact_hex, 'hex') AS source_artifact_id
             FROM next_rows
             ORDER BY series_key_hex, time, stage_row_id DESC
         ),
         changed_rows AS MATERIALIZED (
             SELECT staged.*
             FROM deduped_rows staged
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM observations existing
                 WHERE existing.series_key = staged.series_key
                   AND existing.time = staged.time
                   AND existing.time_precision = staged.time_precision
                   AND existing.value IS NOT DISTINCT FROM staged.value
                   AND existing.status = staged.status
                   AND existing.attributes = staged.attributes
                   AND existing.source_artifact_id = staged.source_artifact_id
             )
         ),
         assigned_rows AS MATERIALIZED (
             SELECT staged.series_key,
                    staged.time,
                    (COALESCE(max(existing.revision_no), -1) + 1)::INTEGER AS revision_no,
                    staged.time_precision,
                    staged.value,
                    staged.status,
                    staged.attributes,
                    staged.ingested_at,
                    staged.source_artifact_id
             FROM changed_rows staged
             LEFT JOIN observations existing
               ON existing.series_key = staged.series_key
              AND existing.time = staged.time
             GROUP BY staged.series_key, staged.time, staged.time_precision,
                      staged.value, staged.status, staged.attributes,
                      staged.ingested_at, staged.source_artifact_id
         ),
         inserted AS (
             INSERT INTO observations (
                 series_key, time, revision_no, time_precision, value, status,
                 attributes, ingested_at, source_artifact_id
             )
             SELECT series_key, time, revision_no, time_precision, value, status,
                    attributes, ingested_at, source_artifact_id
             FROM assigned_rows
             ON CONFLICT (series_key, time, revision_no) DO NOTHING
             RETURNING 1
         )
         SELECT COALESCE((SELECT max(stage_row_id) FROM next_rows), $1)::BIGINT,
                (SELECT count(*) FROM inserted)::BIGINT,
                EXISTS(SELECT 1 FROM next_rows)",
    )
    .bind(last_stage_row_id)
    .bind(chunk_limit)
    .fetch_one(&mut **tx)
    .await?;

    let chunk_rows_loaded = u64::try_from(chunk_rows_loaded)
        .map_err(|_| LoadError::Validation("loaded observation count exceeded u64 range".into()))?;

    Ok((max_stage_row_id, chunk_rows_loaded, had_rows))
}

async fn enqueue_webhook_deliveries_for_observations(
    tx: &mut Transaction<'_, Postgres>,
    observations_loaded: u64,
) -> Result<(), LoadError> {
    let deliveries_enqueued = enqueue_webhook_deliveries_for_staging(tx).await?;
    if deliveries_enqueued > 0 {
        tracing::info!(
            observations_loaded,
            deliveries_enqueued,
            "webhook deliveries enqueued for loaded observations"
        );
    }

    Ok(())
}

async fn enqueue_webhook_deliveries_for_staging(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<u64, LoadError> {
    let rows = sqlx::query(
        "WITH loaded_events AS (
             SELECT series.dataflow_id,
                    decode(observations.source_artifact_hex, 'hex') AS artifact_id,
                    count(*)::BIGINT AS observations_loaded,
                    max(observations.ingested_at) AS occurred_at
             FROM staging_observations observations
             JOIN staging_series series
               ON series.series_key_hex = observations.series_key_hex
             GROUP BY series.dataflow_id, observations.source_artifact_hex
         ),
         event_payloads AS (
             SELECT dataflow_id,
                    artifact_id,
                    jsonb_build_object(
                        'event', $1,
                        'dataflow_id', dataflow_id,
                        'artifact_id', encode(artifact_id, 'hex'),
                        'observations_loaded', observations_loaded,
                        'occurred_at', occurred_at
                    ) AS payload
             FROM loaded_events
         )
         INSERT INTO webhook_deliveries (
             subscription_id, event_type, dataflow_id, artifact_id, payload,
             status, attempts, max_attempts, next_attempt_at
         )
         SELECT subscriptions.id, $1, events.dataflow_id, events.artifact_id,
                events.payload, 'pending', 0, $2, now()
         FROM event_payloads events
         JOIN webhook_subscriptions subscriptions
           ON subscriptions.status = 'active'
          AND (
              cardinality(subscriptions.dataflow_ids) = 0
              OR events.dataflow_id = ANY(subscriptions.dataflow_ids)
          )",
    )
    .bind(WEBHOOK_EVENT_DATA_UPDATED)
    .bind(WEBHOOK_DELIVERY_MAX_ATTEMPTS)
    .execute(&mut **tx)
    .await?;

    Ok(rows.rows_affected())
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
    audit_context: Option<Value>,
) -> Result<(), LoadError> {
    let row_context = loader_validation_row_context(item, audit_context);

    record_parse_error(
        pool,
        artifact_id,
        "loader_validation",
        message,
        Some(row_context),
    )
    .await
}

async fn record_loader_validation_error_on_connection(
    conn: &mut PoolConnection<Postgres>,
    artifact_id: ArtifactId,
    message: &str,
    item: &LoadItem,
    audit_context: Option<Value>,
) -> Result<(), LoadError> {
    let row_context = loader_validation_row_context(item, audit_context);

    record_parse_error_on_connection(
        conn,
        artifact_id,
        "loader_validation",
        message,
        Some(row_context),
    )
    .await
}

fn loader_validation_row_context(item: &LoadItem, audit_context: Option<Value>) -> Value {
    let base_context = serde_json::json!({
        "dataflow_id": item.series.dataflow_id,
        "series_key": item.series.series_key,
        "observation_time": item.observation.time,
        "revision_no": item.observation.revision_no,
    });
    merge_row_context(base_context, audit_context)
}

async fn record_parse_error_on_connection(
    conn: &mut PoolConnection<Postgres>,
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
    .execute(&mut **conn)
    .await?;

    Ok(())
}

fn merge_row_context(mut base: Value, extra: Option<Value>) -> Value {
    if let Some(extra) = extra {
        match (&mut base, extra) {
            (Value::Object(base), Value::Object(extra)) => {
                for (key, value) in extra {
                    base.insert(key, value);
                }
            }
            (Value::Object(base), extra) => {
                base.insert("audit_context".to_string(), extra);
            }
            (_, extra) => {
                base = extra;
            }
        }
    }
    base
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
        TimePrecision::Minute => "minute",
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

#[cfg(test)]
mod tests {
    use super::*;
    use au_kpis_domain::ids::{CodeId, DataflowId, DimensionId, MeasureId, Sha256Digest};
    use chrono::TimeZone;

    #[test]
    fn validate_item_rejects_legacy_measureless_series_key() {
        let dataflow_id = DataflowId::new("abs.cpi").unwrap();
        let measure_id = MeasureId::new("index").unwrap();
        let dimensions: BTreeMap<DimensionId, CodeId> = [(
            DimensionId::new("region").unwrap(),
            CodeId::new("AUS").unwrap(),
        )]
        .into_iter()
        .collect();
        let series_key = legacy_series_key_without_measure(&dataflow_id, &dimensions);
        let artifact_id = ArtifactId::of_content(b"legacy key fixture");
        let observed_at = chrono::Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap();
        let ingested_at = chrono::Utc.with_ymd_and_hms(2024, 4, 24, 0, 0, 0).unwrap();

        let item = LoadItem {
            series: SeriesDescriptor {
                series_key,
                dataflow_id,
                measure_id,
                dimensions,
                unit: "index".into(),
            },
            observation: Observation {
                series_key,
                time: observed_at,
                time_precision: TimePrecision::Quarter,
                value: Some(134.2),
                status: ObservationStatus::Normal,
                revision_no: 0,
                attributes: BTreeMap::new(),
                ingested_at,
                source_artifact_id: artifact_id,
            },
        };

        let err = validate_item(&item).expect_err("legacy series key should be rejected");
        assert!(err.contains("does not match computed key"), "{err}");
    }

    #[test]
    fn direct_batch_dedup_keeps_highest_revision_then_latest_tie() {
        let descriptor = test_descriptor();
        let artifact_id = ArtifactId::of_content(b"direct dedupe fixture");
        let time = chrono::Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap();

        let revisions = vec![
            test_item(&descriptor, artifact_id, time, 0, 100.0),
            test_item(&descriptor, artifact_id, time, 2, 102.0),
            test_item(&descriptor, artifact_id, time, 1, 101.0),
        ];
        let deduped = dedupe_observations_by_series_time(&revisions);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].observation.revision_no, 2);
        assert_eq!(deduped[0].observation.value, Some(102.0));

        let ties = vec![
            test_item(&descriptor, artifact_id, time, 0, 104.0),
            test_item(&descriptor, artifact_id, time, 0, 105.0),
        ];
        let deduped = dedupe_observations_by_series_time(&ties);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].observation.value, Some(105.0));
    }

    #[test]
    fn missing_reference_formatter_reports_all_missing_dependencies() {
        let descriptor = test_descriptor();
        let artifact_id = ArtifactId::of_content(b"missing reference fixture");
        let item = test_item(
            &descriptor,
            artifact_id,
            chrono::Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap(),
            0,
            100.0,
        );
        let audited = vec![LoadItemAudit::from(item)];

        let missing: Vec<_> = missing_references(
            &audited,
            vec![
                (0, false, false, false),
                (2, false, false, false),
                (1, true, true, true),
                (1, false, false, false),
            ],
        )
        .collect();

        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].0, 0);
        assert_eq!(
            missing[0].1,
            vec![
                "dataflow `abs.cpi`".to_string(),
                "measure `index`".to_string(),
                format!("artifact `{artifact_id}`"),
            ]
        );
    }

    #[tokio::test]
    async fn empty_reference_load_skips_database_queries() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused/unused")
            .expect("lazy pool");

        let rows = load_reference_rows(&pool, &[])
            .await
            .expect("empty reference check");

        assert!(rows.is_empty());
    }

    #[test]
    fn validation_helpers_reject_zero_limits_and_key_mismatch() {
        assert_eq!(
            validate_options(LoadOptions {
                max_rows: 0,
                max_bytes: 1,
            })
            .unwrap_err()
            .to_string(),
            "loader validation: max_rows must be greater than 0"
        );
        assert_eq!(
            validate_options(LoadOptions {
                max_rows: 1,
                max_bytes: 0,
            })
            .unwrap_err()
            .to_string(),
            "loader validation: max_bytes must be greater than 0"
        );
        assert!(
            validate_options(LoadOptions {
                max_rows: 1,
                max_bytes: 1,
            })
            .is_ok()
        );

        let descriptor = test_descriptor();
        let artifact_id = ArtifactId::of_content(b"observation key mismatch");
        let mut item = test_item(
            &descriptor,
            artifact_id,
            chrono::Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap(),
            0,
            100.0,
        );
        item.observation.series_key =
            legacy_series_key_without_measure(&descriptor.dataflow_id, &descriptor.dimensions);

        let err = validate_item(&item).expect_err("observation key mismatch should fail");
        assert!(err.contains("observation series key"), "{err}");

        let valid = test_item(
            &descriptor,
            artifact_id,
            chrono::Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap(),
            0,
            101.0,
        );
        assert!(validate_item(&valid).is_ok());
    }

    #[test]
    fn batch_helpers_cover_empty_rows_and_byte_limits() {
        let options = LoadOptions {
            max_rows: 2,
            max_bytes: 10,
        };

        assert!(!should_flush_load_batch(0, 11, 1, options));
        assert!(!should_flush_load_batch(1, 4, 5, options));
        assert!(should_flush_load_batch(2, 1, 1, options));
        assert!(should_flush_load_batch(1, 10, 1, options));
        assert!(!load_batch_boundary_reached(1, 9, options));
        assert!(load_batch_boundary_reached(2, 1, options));
        assert!(load_batch_boundary_reached(1, 10, options));
    }

    #[test]
    fn duplicate_detection_and_dedup_keep_non_duplicate_rows() {
        let descriptor = test_descriptor();
        let artifact_id = ArtifactId::of_content(b"dedupe branch fixture");
        let first_time = chrono::Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap();
        let second_time = chrono::Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
        let distinct = vec![
            test_item(&descriptor, artifact_id, first_time, 1, 101.0),
            test_item(&descriptor, artifact_id, second_time, 0, 100.0),
        ];
        assert!(!has_duplicate_observation_times(&distinct));
        assert_eq!(dedupe_observations_by_series_time(&distinct).len(), 2);

        let duplicates = vec![
            test_item(&descriptor, artifact_id, first_time, 2, 102.0),
            test_item(&descriptor, artifact_id, first_time, 1, 101.0),
        ];
        assert!(has_duplicate_observation_times(&duplicates));
        let deduped = dedupe_observations_by_series_time(&duplicates);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].observation.value, Some(102.0));
    }

    #[test]
    fn row_context_merge_handles_absent_objects_and_scalars() {
        let base = serde_json::json!({"artifact": "a1"});
        assert_eq!(merge_row_context(base.clone(), None), base);

        assert_eq!(
            merge_row_context(base.clone(), Some(serde_json::json!({"row": 7}))),
            serde_json::json!({"artifact": "a1", "row": 7})
        );
        assert_eq!(
            merge_row_context(base, Some(serde_json::json!("line 7"))),
            serde_json::json!({"artifact": "a1", "audit_context": "line 7"})
        );
        assert_eq!(
            merge_row_context(
                serde_json::json!("base"),
                Some(serde_json::json!({"row": 7}))
            ),
            serde_json::json!({"row": 7})
        );
    }

    #[test]
    fn copy_field_helpers_escape_delimiters_and_preserve_nulls() {
        let mut payload = String::new();
        push_copy_fields(&mut payload, ["a\tb".into(), "\\N".into(), "c\\d\n".into()]);

        assert_eq!(payload, "a\\tb\t\\N\tc\\\\d\\n\n");
    }

    fn test_descriptor() -> SeriesDescriptor {
        let dataflow_id = DataflowId::new("abs.cpi").unwrap();
        let measure_id = MeasureId::new("index").unwrap();
        let dimensions: BTreeMap<DimensionId, CodeId> = [(
            DimensionId::new("region").unwrap(),
            CodeId::new("AUS").unwrap(),
        )]
        .into_iter()
        .collect();
        let series_key = SeriesKey::derive(
            &dataflow_id,
            &measure_id,
            dimensions
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        SeriesDescriptor {
            series_key,
            dataflow_id,
            measure_id,
            dimensions,
            unit: "index".into(),
        }
    }

    fn test_item(
        descriptor: &SeriesDescriptor,
        artifact_id: ArtifactId,
        time: chrono::DateTime<chrono::Utc>,
        revision_no: u32,
        value: f64,
    ) -> LoadItem {
        LoadItem {
            series: descriptor.clone(),
            observation: Observation {
                series_key: descriptor.series_key,
                time,
                time_precision: TimePrecision::Quarter,
                value: Some(value),
                status: ObservationStatus::Normal,
                revision_no,
                attributes: BTreeMap::new(),
                ingested_at: chrono::Utc.with_ymd_and_hms(2024, 4, 24, 0, 0, 0).unwrap(),
                source_artifact_id: artifact_id,
            },
        }
    }

    fn legacy_series_key_without_measure(
        dataflow_id: &DataflowId,
        dimensions: &BTreeMap<DimensionId, CodeId>,
    ) -> SeriesKey {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(dataflow_id.as_str().as_bytes());
        for (key, value) in dimensions {
            bytes.push(0);
            bytes.extend_from_slice(key.as_str().as_bytes());
            bytes.push(b'=');
            bytes.extend_from_slice(value.as_str().as_bytes());
        }
        SeriesKey::from_digest(Sha256Digest::hash(&bytes))
    }
}
