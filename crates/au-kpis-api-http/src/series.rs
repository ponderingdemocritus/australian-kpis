//! `/v1/series/{dataflow}/{series_key}` lookup endpoint.

use std::{collections::BTreeMap, str::FromStr};

use au_kpis_domain::{
    Observation, ObservationStatus, Series, TimePrecision,
    ids::{
        ArtifactId, CodeId, DataflowId, DimensionId, MeasureId, SHA256_BYTES, SeriesKey,
        Sha256Digest,
    },
};
use axum::{
    Json,
    extract::{Path, State},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row, postgres::PgRow};
use utoipa::ToSchema;

use crate::{ApiError, AppState};

/// Revision details for the latest observation returned with a series lookup.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SeriesRevisionMetadata {
    /// Latest revision number for the observation timestamp.
    pub revision_no: u32,
    /// Whether this observation supersedes the original publication.
    pub is_revision: bool,
    /// When this revision was ingested.
    pub ingested_at: DateTime<Utc>,
    /// Source artifact that produced this revision.
    pub source_artifact_id: ArtifactId,
}

/// Response envelope for `GET /v1/series/{dataflow}/{series_key}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SeriesLookupResponse {
    /// Series metadata.
    pub series: Series,
    /// Latest observation by timestamp, using `observations_latest` revision selection.
    pub latest_observation: Option<Observation>,
    /// Revision metadata for `latest_observation`.
    pub revision: Option<SeriesRevisionMetadata>,
}

/// `GET /v1/series/{dataflow}/{series_key}`.
#[utoipa::path(
    get,
    path = "/v1/series/{dataflow}/{series_key}",
    operation_id = "getSeries",
    params(
        ("dataflow" = String, Path, min_length = 1, max_length = 128, description = "Dataflow id."),
        ("series_key" = String, Path, min_length = 64, max_length = 64, pattern = "^[0-9a-f]{64}$", description = "64-character series key hex digest.")
    ),
    responses(
        (status = 200, description = "Series metadata and latest observation.", body = SeriesLookupResponse, content_type = "application/json"),
        (status = 400, description = "Invalid path parameter.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 404, description = "Series not found.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 500, description = "Internal error.", body = crate::ProblemDetails, content_type = "application/problem+json")
    ),
    tag = "series"
)]
pub async fn get_series(
    State(state): State<AppState>,
    Path((dataflow, series_key)): Path<(String, String)>,
) -> Result<Json<SeriesLookupResponse>, ApiError> {
    let dataflow = DataflowId::new(dataflow)
        .map_err(|err| ApiError::Validation(format!("invalid dataflow: {err}")))?;
    let series_key = SeriesKey::from_str(&series_key)
        .map_err(|err| ApiError::Validation(format!("invalid series_key: {err}")))?;
    load_series_lookup(&state.db, &dataflow, &series_key)
        .await
        .map(Json)
}

async fn load_series_lookup(
    pool: &PgPool,
    dataflow: &DataflowId,
    series_key: &SeriesKey,
) -> Result<SeriesLookupResponse, ApiError> {
    let row = sqlx::query(
        "SELECT s.series_key,
                s.dataflow_id,
                s.measure_id,
                s.dimensions,
                s.unit,
                s.first_observed,
                s.last_observed,
                s.active,
                o.time AS observation_time,
                o.time_precision,
                o.value,
                o.status,
                o.revision_no,
                o.attributes,
                o.ingested_at,
                o.source_artifact_id
         FROM series s
         LEFT JOIN LATERAL (
             SELECT series_key, time, time_precision, value, status, revision_no,
                    attributes, ingested_at, source_artifact_id
             FROM observations_latest
             WHERE series_key = s.series_key
             ORDER BY time DESC
             LIMIT 1
         ) o ON TRUE
         WHERE s.dataflow_id = $1 AND s.series_key = $2",
    )
    .bind(dataflow.as_str())
    .bind(series_key.digest().as_bytes().to_vec())
    .fetch_optional(pool)
    .await?;

    let row = row.ok_or_else(|| ApiError::NotFound(format!("series `{series_key}`")))?;
    series_lookup_from_row(row)
}

fn series_lookup_from_row(row: PgRow) -> Result<SeriesLookupResponse, ApiError> {
    let series = series_from_row(&row)?;
    let latest_observation = observation_from_row(&row)?;
    let revision = latest_observation
        .as_ref()
        .map(|observation| SeriesRevisionMetadata {
            revision_no: observation.revision_no,
            is_revision: observation.revision_no > 0,
            ingested_at: observation.ingested_at,
            source_artifact_id: observation.source_artifact_id,
        });

    Ok(SeriesLookupResponse {
        series,
        latest_observation,
        revision,
    })
}

fn series_from_row(row: &PgRow) -> Result<Series, ApiError> {
    Ok(Series {
        series_key: series_key_from_bytes(row.try_get("series_key")?)?,
        dataflow_id: dataflow_id_from_str(row.try_get("dataflow_id")?)?,
        measure_id: measure_id_from_str(row.try_get("measure_id")?)?,
        dimensions: dimensions_from_json(row.try_get("dimensions")?)?,
        unit: row.try_get("unit")?,
        first_observed: row.try_get("first_observed")?,
        last_observed: row.try_get("last_observed")?,
        active: row.try_get("active")?,
    })
}

fn observation_from_row(row: &PgRow) -> Result<Option<Observation>, ApiError> {
    let Some(time) = row.try_get::<Option<DateTime<Utc>>, _>("observation_time")? else {
        return Ok(None);
    };
    let revision_no = row
        .try_get::<Option<i32>, _>("revision_no")?
        .ok_or(ApiError::Internal)?;
    let revision_no = u32::try_from(revision_no).map_err(|err| {
        tracing::error!(%err, revision_no, "database returned invalid revision_no");
        ApiError::Internal
    })?;
    let source_artifact_id = row
        .try_get::<Option<Vec<u8>>, _>("source_artifact_id")?
        .ok_or(ApiError::Internal)
        .and_then(artifact_id_from_bytes)?;

    Ok(Some(Observation {
        series_key: series_key_from_bytes(row.try_get("series_key")?)?,
        time,
        time_precision: parse_time_precision(
            row.try_get::<Option<String>, _>("time_precision")?
                .as_deref()
                .ok_or(ApiError::Internal)?,
        )?,
        value: row.try_get("value")?,
        status: parse_observation_status(
            row.try_get::<Option<String>, _>("status")?
                .as_deref()
                .ok_or(ApiError::Internal)?,
        )?,
        revision_no,
        attributes: json_map(row.try_get("attributes")?)?,
        ingested_at: row
            .try_get::<Option<DateTime<Utc>>, _>("ingested_at")?
            .ok_or(ApiError::Internal)?,
        source_artifact_id,
    }))
}

fn dimensions_from_json(
    value: serde_json::Value,
) -> Result<BTreeMap<DimensionId, CodeId>, ApiError> {
    serde_json::from_value(value).map_err(|err| {
        tracing::error!(error = %err, "database series dimensions were invalid");
        ApiError::Internal
    })
}

fn json_map(value: Option<serde_json::Value>) -> Result<BTreeMap<String, String>, ApiError> {
    serde_json::from_value(value.ok_or(ApiError::Internal)?).map_err(|err| {
        tracing::error!(error = %err, "database JSON map was not string-valued");
        ApiError::Internal
    })
}

fn parse_time_precision(value: &str) -> Result<TimePrecision, ApiError> {
    match value {
        "day" => Ok(TimePrecision::Day),
        "week" => Ok(TimePrecision::Week),
        "month" => Ok(TimePrecision::Month),
        "quarter" => Ok(TimePrecision::Quarter),
        "year" => Ok(TimePrecision::Year),
        _ => {
            tracing::error!(
                time_precision = value,
                "database returned invalid time precision"
            );
            Err(ApiError::Internal)
        }
    }
}

fn parse_observation_status(value: &str) -> Result<ObservationStatus, ApiError> {
    match value {
        "normal" => Ok(ObservationStatus::Normal),
        "estimated" => Ok(ObservationStatus::Estimated),
        "forecast" => Ok(ObservationStatus::Forecast),
        "imputed" => Ok(ObservationStatus::Imputed),
        "missing" => Ok(ObservationStatus::Missing),
        "provisional" => Ok(ObservationStatus::Provisional),
        "revised" => Ok(ObservationStatus::Revised),
        "break" => Ok(ObservationStatus::Break),
        _ => {
            tracing::error!(
                status = value,
                "database returned invalid observation status"
            );
            Err(ApiError::Internal)
        }
    }
}

fn series_key_from_bytes(bytes: Vec<u8>) -> Result<SeriesKey, ApiError> {
    digest_from_bytes(bytes).map(SeriesKey::from_digest)
}

fn artifact_id_from_bytes(bytes: Vec<u8>) -> Result<ArtifactId, ApiError> {
    digest_from_bytes(bytes).map(ArtifactId::from_digest)
}

fn digest_from_bytes(bytes: Vec<u8>) -> Result<Sha256Digest, ApiError> {
    let length = bytes.len();
    let bytes: [u8; SHA256_BYTES] = bytes.try_into().map_err(|_| {
        tracing::error!(length, "database returned invalid SHA-256 digest length");
        ApiError::Internal
    })?;
    Ok(Sha256Digest::from_bytes(bytes))
}

fn dataflow_id_from_str(value: String) -> Result<DataflowId, ApiError> {
    DataflowId::new(value).map_err(invalid_db_id)
}

fn measure_id_from_str(value: String) -> Result<MeasureId, ApiError> {
    MeasureId::new(value).map_err(invalid_db_id)
}

fn invalid_db_id(err: au_kpis_domain::ids::IdError) -> ApiError {
    tracing::error!(%err, "database returned invalid identifier");
    ApiError::Internal
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_series_key_is_a_validation_error() {
        let err = SeriesKey::from_str("not-a-key")
            .map_err(|err| ApiError::Validation(format!("invalid series_key: {err}")))
            .unwrap_err();

        assert!(err.to_string().contains("series_key"));
    }
}
