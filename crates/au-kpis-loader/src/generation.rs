use std::collections::BTreeMap;

use au_kpis_db::{ObservationStageRow, StageDigest};
use au_kpis_domain::{
    ArtifactId, CodeId, DataflowId, DimensionId, MeasureId, Observation, ObservationStatus,
    SeriesDescriptor, SeriesKey, Sha256Digest, TimePrecision,
};
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::LoadError;

/// Result of one atomic generation publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerationPublication {
    /// Generation that was published.
    pub generation_id: Uuid,
    /// Series rows inserted or refreshed.
    pub series_upserted: u64,
    /// New observation revisions inserted.
    pub observations_loaded: u64,
}

#[derive(Debug)]
struct GenerationMeta {
    id: Uuid,
    discovered_work_id: Uuid,
    artifact_fetch_id: i64,
    artifact_id: Vec<u8>,
    source_id: String,
    dataflow_id: String,
    status: String,
    parsed_count: i64,
    loaded_count: i64,
    stage_digest: Option<Vec<u8>>,
    job_id: Option<i64>,
    trace_parent: Option<String>,
}

/// Verify and publish one staged generation in a single transaction.
pub async fn publish_ingestion_generation(
    pool: &PgPool,
    generation_id: Uuid,
) -> Result<GenerationPublication, LoadError> {
    let mut tx = pool.begin().await?;
    let meta = load_generation_for_update(&mut tx, generation_id).await?;
    if meta.status == "published" {
        return Ok(GenerationPublication {
            generation_id,
            series_upserted: 0,
            observations_loaded: nonnegative_u64("loaded_count", meta.loaded_count)?,
        });
    }
    if meta.status != "pending_load" {
        return Err(LoadError::Validation(format!(
            "generation `{generation_id}` is `{}`, expected `pending_load`",
            meta.status
        )));
    }
    let expected_digest = meta.stage_digest.as_deref().ok_or_else(|| {
        LoadError::Validation(format!("generation `{generation_id}` has no stage digest"))
    })?;
    if expected_digest.len() != 32 {
        return Err(LoadError::Validation(format!(
            "generation `{generation_id}` stage digest is not 32 bytes"
        )));
    }

    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(&meta.dataflow_id)
        .execute(&mut *tx)
        .await?;
    verify_stage(&mut tx, &meta, expected_digest).await?;

    sqlx::query(
        "UPDATE ingestion_generations
         SET status = 'loading', loading_started_at = now(), updated_at = now()
         WHERE id = $1 AND status = 'pending_load'",
    )
    .bind(generation_id)
    .execute(&mut *tx)
    .await?;

    validate_existing_series(&mut tx, generation_id).await?;
    let series_upserted = upsert_generation_series(&mut tx, generation_id).await?;
    let observations_loaded = insert_generation_observations(&mut tx, generation_id).await?;
    write_publication_audit(&mut tx, &meta, observations_loaded).await?;
    enqueue_generation_webhooks(&mut tx, &meta, observations_loaded).await?;

    sqlx::query(
        "UPDATE discovered_work
         SET status = 'handled', handled_at = now(), updated_at = now()
         WHERE id = $1",
    )
    .bind(meta.discovered_work_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE ingestion_generations
         SET status = 'published', loaded_count = $2,
             published_at = now(), updated_at = now()
         WHERE id = $1 AND status = 'loading'",
    )
    .bind(generation_id)
    .bind(
        i64::try_from(observations_loaded)
            .map_err(|_| LoadError::Validation("observations loaded exceeds BIGINT".to_string()))?,
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM observation_stage WHERE generation_id = $1")
        .bind(generation_id)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(GenerationPublication {
        generation_id,
        series_upserted,
        observations_loaded,
    })
}

async fn enqueue_generation_webhooks(
    tx: &mut Transaction<'_, Postgres>,
    meta: &GenerationMeta,
    observations_loaded: u64,
) -> Result<(), LoadError> {
    if observations_loaded == 0 {
        return Ok(());
    }
    let observations_loaded = i64::try_from(observations_loaded)
        .map_err(|_| LoadError::Validation("observations loaded exceeds BIGINT".to_string()))?;
    let rows = sqlx::query(
        r#"INSERT INTO webhook_deliveries (
               subscription_id, event_id, generation_id, event_type, dataflow_id,
               artifact_id, payload, status, attempts, max_attempts,
               next_attempt_at, expires_at
           )
           SELECT subscription.id, $1, $1, 'data.updated', $2, $3,
                  jsonb_build_object(
                      'id', $1,
                      'schema_version', '1',
                      'type', 'data.updated',
                      'occurred_at', now(),
                      'generation_id', $1,
                      'dataflow_id', $2,
                      'artifact_id', encode($3, 'hex'),
                      'observations_loaded', $4
                  ),
                  'pending', 0, 12, now(), now() + INTERVAL '24 hours'
           FROM webhook_subscriptions AS subscription
           WHERE subscription.status = 'active'
             AND (
                 cardinality(subscription.dataflow_ids) = 0
                 OR $2 = ANY(subscription.dataflow_ids)
             )
           ON CONFLICT (event_id, subscription_id) DO NOTHING"#,
    )
    .bind(meta.id)
    .bind(&meta.dataflow_id)
    .bind(&meta.artifact_id)
    .bind(observations_loaded)
    .execute(&mut **tx)
    .await?;
    if rows.rows_affected() > 0 {
        tracing::info!(
            generation_id = %meta.id,
            dataflow_id = %meta.dataflow_id,
            deliveries_enqueued = rows.rows_affected(),
            "durable webhook outbox rows enqueued"
        );
    }
    Ok(())
}

async fn load_generation_for_update(
    tx: &mut Transaction<'_, Postgres>,
    generation_id: Uuid,
) -> Result<GenerationMeta, LoadError> {
    let row = sqlx::query(
        r#"SELECT generation.id, generation.discovered_work_id,
                  generation.artifact_fetch_id, artifact_fetch.artifact_id,
                  generation.source_id, generation.dataflow_id, generation.status,
                  generation.parsed_count, generation.loaded_count,
                  generation.stage_digest, generation.job_id, generation.trace_parent
           FROM ingestion_generations AS generation
           JOIN artifact_fetches AS artifact_fetch
             ON artifact_fetch.id = generation.artifact_fetch_id
           WHERE generation.id = $1
           FOR UPDATE OF generation"#,
    )
    .bind(generation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| LoadError::Validation(format!("generation `{generation_id}` not found")))?;
    Ok(GenerationMeta {
        id: row.try_get("id")?,
        discovered_work_id: row.try_get("discovered_work_id")?,
        artifact_fetch_id: row.try_get("artifact_fetch_id")?,
        artifact_id: row.try_get("artifact_id")?,
        source_id: row.try_get("source_id")?,
        dataflow_id: row.try_get("dataflow_id")?,
        status: row.try_get("status")?,
        parsed_count: row.try_get("parsed_count")?,
        loaded_count: row.try_get("loaded_count")?,
        stage_digest: row.try_get("stage_digest")?,
        job_id: row.try_get("job_id")?,
        trace_parent: row.try_get("trace_parent")?,
    })
}

async fn verify_stage(
    tx: &mut Transaction<'_, Postgres>,
    meta: &GenerationMeta,
    expected_digest: &[u8],
) -> Result<(), LoadError> {
    let mut digest = StageDigest::new();
    let mut count = 0_u64;
    let mut rows = sqlx::query(
        r#"SELECT row_no, series_key, dataflow_id, measure_id, dimensions,
                  unit, time, time_precision, revision_no, value, status,
                  attributes, ingested_at, source_artifact_id
           FROM observation_stage
           WHERE generation_id = $1
           ORDER BY row_no"#,
    )
    .bind(meta.id)
    .fetch(&mut **tx);
    while let Some(row) = rows.try_next().await? {
        let (series, observation, row_no) = stage_row_from_db(row)?;
        digest.update(ObservationStageRow {
            row_no,
            series: &series,
            observation: &observation,
        })?;
        count += 1;
    }
    drop(rows);

    if count != nonnegative_u64("parsed_count", meta.parsed_count)? {
        return Err(LoadError::Validation(format!(
            "generation `{}` staged count changed before Load",
            meta.id
        )));
    }
    if digest.finalize().as_slice() != expected_digest {
        return Err(LoadError::Validation(format!(
            "generation `{}` staged digest changed before Load",
            meta.id
        )));
    }
    Ok(())
}

fn stage_row_from_db(
    row: sqlx::postgres::PgRow,
) -> Result<(SeriesDescriptor, Observation, u64), LoadError> {
    let series_key = series_key(row.try_get("series_key")?)?;
    let dataflow_id = DataflowId::new(row.try_get::<String, _>("dataflow_id")?)
        .map_err(|error| LoadError::Validation(error.to_string()))?;
    let measure_id = MeasureId::new(row.try_get::<String, _>("measure_id")?)
        .map_err(|error| LoadError::Validation(error.to_string()))?;
    let dimensions: BTreeMap<DimensionId, CodeId> =
        serde_json::from_value(row.try_get::<Value, _>("dimensions")?)?;
    let artifact_id = artifact_id(row.try_get("source_artifact_id")?)?;
    let revision_no = u32::try_from(row.try_get::<i32, _>("revision_no")?)
        .map_err(|_| LoadError::Validation("negative staged revision".to_string()))?;
    let row_no = u64::try_from(row.try_get::<i64, _>("row_no")?)
        .map_err(|_| LoadError::Validation("negative staged row number".to_string()))?;
    let series = SeriesDescriptor {
        series_key,
        dataflow_id,
        measure_id,
        dimensions,
        unit: row.try_get("unit")?,
    };
    let observation = Observation {
        series_key,
        time: row.try_get::<DateTime<Utc>, _>("time")?,
        time_precision: parse_time_precision(&row.try_get::<String, _>("time_precision")?)?,
        value: row.try_get("value")?,
        status: parse_observation_status(&row.try_get::<String, _>("status")?)?,
        revision_no,
        attributes: serde_json::from_value(row.try_get::<Value, _>("attributes")?)?,
        ingested_at: row.try_get("ingested_at")?,
        source_artifact_id: artifact_id,
    };
    Ok((series, observation, row_no))
}

async fn validate_existing_series(
    tx: &mut Transaction<'_, Postgres>,
    generation_id: Uuid,
) -> Result<(), LoadError> {
    let staged_conflict: bool = sqlx::query_scalar(
        r#"SELECT EXISTS (
               SELECT 1
               FROM observation_stage
               WHERE generation_id = $1
               GROUP BY series_key
               HAVING count(DISTINCT ROW(dataflow_id, measure_id, dimensions, unit)) > 1
           )"#,
    )
    .bind(generation_id)
    .fetch_one(&mut **tx)
    .await?;
    if staged_conflict {
        return Err(LoadError::Validation(format!(
            "generation `{generation_id}` contains conflicting series descriptors"
        )));
    }

    let conflict: bool = sqlx::query_scalar(
        r#"SELECT EXISTS (
               SELECT 1
               FROM observation_stage AS stage
               JOIN series AS existing ON existing.series_key = stage.series_key
               WHERE stage.generation_id = $1
                 AND (
                     existing.dataflow_id <> stage.dataflow_id
                     OR existing.measure_id <> stage.measure_id
                     OR existing.dimensions <> stage.dimensions
                     OR existing.unit <> stage.unit
                 )
           )"#,
    )
    .bind(generation_id)
    .fetch_one(&mut **tx)
    .await?;
    if conflict {
        return Err(LoadError::Validation(format!(
            "generation `{generation_id}` conflicts with an existing series descriptor"
        )));
    }
    Ok(())
}

async fn upsert_generation_series(
    tx: &mut Transaction<'_, Postgres>,
    generation_id: Uuid,
) -> Result<u64, LoadError> {
    let result = sqlx::query(
        r#"INSERT INTO series (
               series_key, dataflow_id, measure_id, dimensions, unit,
               first_observed, last_observed, active
           )
           SELECT series_key, dataflow_id, measure_id, dimensions, unit,
                  min(time), max(time), TRUE
           FROM observation_stage
           WHERE generation_id = $1
           GROUP BY series_key, dataflow_id, measure_id, dimensions, unit
           ON CONFLICT (series_key) DO UPDATE
           SET first_observed = LEAST(series.first_observed, EXCLUDED.first_observed),
               last_observed = GREATEST(series.last_observed, EXCLUDED.last_observed),
               active = TRUE,
               updated_at = now()"#,
    )
    .bind(generation_id)
    .execute(&mut **tx)
    .await?;
    Ok(result.rows_affected())
}

async fn insert_generation_observations(
    tx: &mut Transaction<'_, Postgres>,
    generation_id: Uuid,
) -> Result<u64, LoadError> {
    let result = sqlx::query(
        r#"WITH deduped AS MATERIALIZED (
               SELECT DISTINCT ON (series_key, time)
                      series_key, time, time_precision, value, status,
                      attributes, ingested_at, source_artifact_id
               FROM observation_stage
               WHERE generation_id = $1
               ORDER BY series_key, time, row_no DESC
           ),
           changed AS MATERIALIZED (
               SELECT staged.*
               FROM deduped AS staged
               WHERE NOT EXISTS (
                   SELECT 1
                   FROM observations AS existing
                   WHERE existing.series_key = staged.series_key
                     AND existing.time = staged.time
                     AND existing.time_precision = staged.time_precision
                     AND existing.value IS NOT DISTINCT FROM staged.value
                     AND existing.status = staged.status
                     AND existing.attributes = staged.attributes
                     AND existing.source_artifact_id = staged.source_artifact_id
               )
           ),
           assigned AS MATERIALIZED (
               SELECT changed.*,
                      (
                          SELECT COALESCE(max(existing.revision_no), -1) + 1
                          FROM observations AS existing
                          WHERE existing.series_key = changed.series_key
                            AND existing.time = changed.time
                      )::INTEGER AS revision_no
               FROM changed
           )
           INSERT INTO observations (
               series_key, time, revision_no, time_precision, value, status,
               attributes, ingested_at, source_artifact_id,
               ingestion_generation_id
           )
           SELECT series_key, time, revision_no, time_precision, value, status,
                  attributes, ingested_at, source_artifact_id, $1
           FROM assigned"#,
    )
    .bind(generation_id)
    .execute(&mut **tx)
    .await?;
    Ok(result.rows_affected())
}

async fn write_publication_audit(
    tx: &mut Transaction<'_, Postgres>,
    meta: &GenerationMeta,
    observations_loaded: u64,
) -> Result<(), LoadError> {
    let parsed_count = nonnegative_u64("parsed_count", meta.parsed_count)?;
    let job_id = meta.job_id.map(|value| value.to_string());
    sqlx::query(
        r#"INSERT INTO artifact_loads (
               artifact_id, artifact_fetch_id, source_id, dataflow_id,
               observations_parsed, observations_loaded, job_id, trace_parent,
               ingestion_generation_id
           )
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           ON CONFLICT (artifact_id, source_id, dataflow_id) DO UPDATE
           SET artifact_fetch_id = EXCLUDED.artifact_fetch_id,
               observations_parsed = EXCLUDED.observations_parsed,
               observations_loaded = EXCLUDED.observations_loaded,
               job_id = EXCLUDED.job_id,
               trace_parent = EXCLUDED.trace_parent,
               ingestion_generation_id = EXCLUDED.ingestion_generation_id,
               completed_at = now()"#,
    )
    .bind(&meta.artifact_id)
    .bind(meta.artifact_fetch_id)
    .bind(&meta.source_id)
    .bind(&meta.dataflow_id)
    .bind(
        i64::try_from(parsed_count)
            .map_err(|_| LoadError::Validation("parsed count exceeds BIGINT".to_string()))?,
    )
    .bind(
        i64::try_from(observations_loaded)
            .map_err(|_| LoadError::Validation("loaded count exceeds BIGINT".to_string()))?,
    )
    .bind(job_id)
    .bind(&meta.trace_parent)
    .bind(meta.id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn series_key(bytes: Vec<u8>) -> Result<SeriesKey, LoadError> {
    digest(bytes).map(|digest| SeriesKey::from_digest(Sha256Digest::from_bytes(digest)))
}

fn artifact_id(bytes: Vec<u8>) -> Result<ArtifactId, LoadError> {
    digest(bytes).map(|digest| ArtifactId::from_digest(Sha256Digest::from_bytes(digest)))
}

fn digest(bytes: Vec<u8>) -> Result<[u8; 32], LoadError> {
    bytes.try_into().map_err(|bytes: Vec<u8>| {
        LoadError::Validation(format!(
            "database digest has {} bytes, expected 32",
            bytes.len()
        ))
    })
}

fn nonnegative_u64(name: &str, value: i64) -> Result<u64, LoadError> {
    u64::try_from(value).map_err(|_| LoadError::Validation(format!("{name} is negative: {value}")))
}

fn parse_time_precision(value: &str) -> Result<TimePrecision, LoadError> {
    match value {
        "minute" => Ok(TimePrecision::Minute),
        "day" => Ok(TimePrecision::Day),
        "week" => Ok(TimePrecision::Week),
        "month" => Ok(TimePrecision::Month),
        "quarter" => Ok(TimePrecision::Quarter),
        "year" => Ok(TimePrecision::Year),
        other => Err(LoadError::Validation(format!(
            "unknown staged time precision `{other}`"
        ))),
    }
}

fn parse_observation_status(value: &str) -> Result<ObservationStatus, LoadError> {
    match value {
        "normal" => Ok(ObservationStatus::Normal),
        "estimated" => Ok(ObservationStatus::Estimated),
        "forecast" => Ok(ObservationStatus::Forecast),
        "imputed" => Ok(ObservationStatus::Imputed),
        "missing" => Ok(ObservationStatus::Missing),
        "provisional" => Ok(ObservationStatus::Provisional),
        "revised" => Ok(ObservationStatus::Revised),
        "break" => Ok(ObservationStatus::Break),
        other => Err(LoadError::Validation(format!(
            "unknown staged observation status `{other}`"
        ))),
    }
}
