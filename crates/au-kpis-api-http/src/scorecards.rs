//! Derived scorecard API handlers.

use std::collections::BTreeSet;

use au_kpis_domain::ids::{ArtifactId, SeriesKey, Sha256Digest};
use au_kpis_scorecard::{
    CoverageStatus, IndicatorConfig, IndicatorObservation, ScorecardConfig, ScorecardError,
    ScorecardSnapshot, load_aps_v1_config, score_aps_snapshot,
};
use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row, postgres::PgRow};

use crate::{AppState, error::ApiError};

const APS_CONFIG_CACHE_CONTROL: &str = "public, max-age=3600, stale-while-revalidate=86400";
const APS_LATEST_CACHE_CONTROL: &str = "public, max-age=60, stale-while-revalidate=300";
const APS_HISTORY_CACHE_CONTROL: &str = "public, max-age=300, stale-while-revalidate=3600";

/// Query string for APS history snapshots.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
pub struct ScorecardHistoryQuery {
    /// Inclusive lower snapshot date in `YYYY-MM-DD` form.
    pub since: Option<String>,
    /// Inclusive upper snapshot date in `YYYY-MM-DD` form.
    pub until: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedScorecardInputs {
    observations: Vec<IndicatorObservation>,
    latest_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct ResolvedIndicatorRow {
    raw_value: Option<f64>,
    latest_time: DateTime<Utc>,
    series_key: SeriesKey,
    source_artifact_id: ArtifactId,
}

/// `GET /v1/scorecards/aps/config`.
#[utoipa::path(
    get,
    operation_id = "getApsScorecardConfig",
    path = "/v1/scorecards/aps/config",
    responses(
        (
            status = 200,
            description = "Versioned APS scorecard config.",
            body = ScorecardConfig,
            headers(
                ("ETag" = String, description = "Strong entity tag for the config JSON."),
                ("Cache-Control" = String, description = "Public CDN cache policy.")
            )
        ),
        (
            status = 304,
            description = "The client's cached config is still fresh.",
            headers(
                ("ETag" = String, description = "Strong entity tag for the config JSON."),
                ("Cache-Control" = String, description = "Public CDN cache policy.")
            )
        ),
        (
            status = 500,
            description = "Internal server error.",
            content_type = "application/problem+json",
            body = crate::error::ProblemDetails
        )
    ),
    tag = "scorecards"
)]
pub async fn aps_config(headers: HeaderMap) -> Result<Response, ApiError> {
    let config = load_config()?;
    json_cache_response(&headers, &config, APS_CONFIG_CACHE_CONTROL)
}

/// `GET /v1/scorecards/aps/latest`.
#[utoipa::path(
    get,
    operation_id = "getApsScorecardLatest",
    path = "/v1/scorecards/aps/latest",
    responses(
        (
            status = 200,
            description = "Latest APS scorecard snapshot.",
            body = ScorecardSnapshot,
            headers(
                ("ETag" = String, description = "Strong entity tag for the latest snapshot JSON."),
                ("Cache-Control" = String, description = "Public CDN cache policy.")
            )
        ),
        (
            status = 304,
            description = "The client's cached latest snapshot is still fresh.",
            headers(
                ("ETag" = String, description = "Strong entity tag for the latest snapshot JSON."),
                ("Cache-Control" = String, description = "Public CDN cache policy.")
            )
        ),
        (
            status = 500,
            description = "Internal server error.",
            content_type = "application/problem+json",
            body = crate::error::ProblemDetails
        )
    ),
    tag = "scorecards"
)]
pub async fn aps_latest(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let config = load_config()?;
    let resolved = resolve_scorecard_inputs(&state.db, &config, None).await?;
    let previous_score = previous_score(&state.db, &config, resolved.latest_time).await?;
    let as_of = resolved
        .latest_time
        .map(format_snapshot_date)
        .unwrap_or_else(|| Utc::now().date_naive().to_string());
    let snapshot = score_snapshot(&config, &resolved.observations, as_of, previous_score)?;
    json_cache_response(&headers, &snapshot, APS_LATEST_CACHE_CONTROL)
}

/// `GET /v1/scorecards/aps/history`.
#[utoipa::path(
    get,
    operation_id = "listApsScorecardHistory",
    path = "/v1/scorecards/aps/history",
    params(ScorecardHistoryQuery),
    responses(
        (
            status = 200,
            description = "Time-ordered APS scorecard snapshots.",
            body = Vec<ScorecardSnapshot>,
            headers(
                ("ETag" = String, description = "Strong entity tag for the history JSON."),
                ("Cache-Control" = String, description = "Public CDN cache policy.")
            )
        ),
        (
            status = 304,
            description = "The client's cached history response is still fresh.",
            headers(
                ("ETag" = String, description = "Strong entity tag for the history JSON."),
                ("Cache-Control" = String, description = "Public CDN cache policy.")
            )
        ),
        (
            status = 400,
            description = "Invalid query string.",
            content_type = "application/problem+json",
            body = crate::error::ProblemDetails
        ),
        (
            status = 500,
            description = "Internal server error.",
            content_type = "application/problem+json",
            body = crate::error::ProblemDetails
        )
    ),
    tag = "scorecards"
)]
pub async fn aps_history(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ScorecardHistoryQuery>,
) -> Result<Response, ApiError> {
    let config = load_config()?;
    let since = parse_history_date(query.since.as_deref(), "since")?;
    let until = parse_history_date(query.until.as_deref(), "until")?;
    if let (Some(since), Some(until)) = (since, until) {
        if since > until {
            return Err(ApiError::Validation(
                "`since` must be earlier than or equal to `until`".into(),
            ));
        }
    }

    let times = load_snapshot_times(&state.db, &config, since, until).await?;
    let mut snapshots = Vec::with_capacity(times.len());
    let mut previous_score = None;
    for time in times {
        let resolved = resolve_scorecard_inputs(&state.db, &config, Some(time)).await?;
        let snapshot = score_snapshot(
            &config,
            &resolved.observations,
            format_snapshot_date(time),
            previous_score,
        )?;
        previous_score = Some(snapshot.score);
        snapshots.push(snapshot);
    }

    json_cache_response(&headers, &snapshots, APS_HISTORY_CACHE_CONTROL)
}

fn load_config() -> Result<ScorecardConfig, ApiError> {
    load_aps_v1_config().map_err(scorecard_error)
}

fn score_snapshot(
    config: &ScorecardConfig,
    observations: &[IndicatorObservation],
    as_of: String,
    previous_score: Option<f64>,
) -> Result<ScorecardSnapshot, ApiError> {
    score_aps_snapshot(config, observations, as_of, previous_score).map_err(scorecard_error)
}

fn scorecard_error(err: ScorecardError) -> ApiError {
    tracing::error!(error = %err, "scorecard error");
    ApiError::Internal
}

async fn resolve_scorecard_inputs(
    pool: &PgPool,
    config: &ScorecardConfig,
    as_of: Option<DateTime<Utc>>,
) -> Result<ResolvedScorecardInputs, ApiError> {
    let mut observations = Vec::with_capacity(config.indicators.len());
    let mut latest_time = None;

    for indicator in &config.indicators {
        let row = load_indicator_observation(pool, indicator, as_of).await?;
        if let Some(row) = row {
            latest_time = latest_time.max(Some(row.latest_time));
            observations.push(resolved_indicator_observation(indicator, row));
        } else {
            observations.push(IndicatorObservation::missing(
                indicator.indicator_id.clone(),
                indicator.coverage_status,
            ));
        }
    }

    Ok(ResolvedScorecardInputs {
        observations,
        latest_time,
    })
}

fn resolved_indicator_observation(
    indicator: &IndicatorConfig,
    row: ResolvedIndicatorRow,
) -> IndicatorObservation {
    let status = if indicator.coverage_status == CoverageStatus::VisibleUnscored {
        CoverageStatus::VisibleUnscored
    } else if row.raw_value.is_some() {
        CoverageStatus::Resolved
    } else {
        indicator.coverage_status
    };

    IndicatorObservation {
        indicator_id: indicator.indicator_id.clone(),
        raw_value: row.raw_value,
        coverage_status: status,
        latest_period: Some(format_snapshot_date(row.latest_time)),
        series_key: Some(row.series_key.to_string()),
        source_artifact_id: Some(row.source_artifact_id.to_string()),
        notes: None,
    }
}

async fn load_indicator_observation(
    pool: &PgPool,
    indicator: &IndicatorConfig,
    as_of: Option<DateTime<Utc>>,
) -> Result<Option<ResolvedIndicatorRow>, ApiError> {
    let row = sqlx::query(
        "SELECT o.value,
                o.time,
                o.series_key,
                o.source_artifact_id
         FROM observations_latest o
         JOIN series s ON s.series_key = o.series_key
         WHERE s.dataflow_id = $1
           AND s.measure_id = $2
           AND s.dimensions = $3::jsonb
           AND ($4::timestamptz IS NULL OR o.time <= $4)
         ORDER BY o.time DESC, o.revision_no DESC, o.series_key ASC
         LIMIT 1",
    )
    .bind(&indicator.source_dataflow_id)
    .bind(&indicator.measure_id)
    .bind(serde_json::json!(indicator.dimension_selector))
    .bind(as_of)
    .fetch_optional(pool)
    .await?;

    row.map(row_to_resolved_indicator).transpose()
}

fn row_to_resolved_indicator(row: PgRow) -> Result<ResolvedIndicatorRow, ApiError> {
    Ok(ResolvedIndicatorRow {
        raw_value: row.try_get("value")?,
        latest_time: row.try_get("time")?,
        series_key: series_key_from_bytes(row.try_get("series_key")?)?,
        source_artifact_id: artifact_id_from_bytes(row.try_get("source_artifact_id")?)?,
    })
}

async fn previous_score(
    pool: &PgPool,
    config: &ScorecardConfig,
    latest_time: Option<DateTime<Utc>>,
) -> Result<Option<f64>, ApiError> {
    let Some(latest_time) = latest_time else {
        return Ok(None);
    };
    let Some(previous_time) = load_previous_snapshot_time(pool, config, latest_time).await? else {
        return Ok(None);
    };
    let previous = resolve_scorecard_inputs(pool, config, Some(previous_time)).await?;
    let snapshot = score_snapshot(
        config,
        &previous.observations,
        format_snapshot_date(previous_time),
        None,
    )?;
    Ok(Some(snapshot.score))
}

async fn load_previous_snapshot_time(
    pool: &PgPool,
    config: &ScorecardConfig,
    latest_time: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, ApiError> {
    let times = load_snapshot_times(pool, config, None, Some(latest_time)).await?;
    Ok(times.into_iter().rev().find(|time| *time < latest_time))
}

async fn load_snapshot_times(
    pool: &PgPool,
    config: &ScorecardConfig,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
) -> Result<Vec<DateTime<Utc>>, ApiError> {
    let mut times = BTreeSet::new();
    for indicator in &config.indicators {
        let rows = sqlx::query(
            "SELECT DISTINCT o.time
             FROM observations_latest o
             JOIN series s ON s.series_key = o.series_key
             WHERE s.dataflow_id = $1
               AND s.measure_id = $2
               AND s.dimensions = $3::jsonb
               AND ($4::timestamptz IS NULL OR o.time >= $4)
               AND ($5::timestamptz IS NULL OR o.time <= $5)
             ORDER BY o.time ASC",
        )
        .bind(&indicator.source_dataflow_id)
        .bind(&indicator.measure_id)
        .bind(serde_json::json!(indicator.dimension_selector))
        .bind(since)
        .bind(until)
        .fetch_all(pool)
        .await?;
        for row in rows {
            times.insert(row.try_get("time")?);
        }
    }
    Ok(times.into_iter().collect())
}

fn parse_history_date(value: Option<&str>, field: &str) -> Result<Option<DateTime<Utc>>, ApiError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| ApiError::Validation(format!("`{field}` must be a valid YYYY-MM-DD date")))?;
    Ok(Some(DateTime::from_naive_utc_and_offset(
        date.and_hms_opt(0, 0, 0)
            .expect("midnight should be representable"),
        Utc,
    )))
}

fn format_snapshot_date(time: DateTime<Utc>) -> String {
    time.date_naive().to_string()
}

fn json_cache_response<T: Serialize>(
    headers: &HeaderMap,
    value: &T,
    cache_control: &'static str,
) -> Result<Response, ApiError> {
    let body = serde_json::to_string(value).map_err(|err| {
        tracing::error!(error = %err, "failed to serialize scorecard response");
        ApiError::Internal
    })?;
    let etag = content_etag(body.as_bytes());
    let cache_control = HeaderValue::from_static(cache_control);
    let etag_header = HeaderValue::from_str(&etag).map_err(|err| {
        tracing::error!(error = %err, "generated invalid scorecard ETag");
        ApiError::Internal
    })?;

    if if_none_match_fresh(headers, &etag) {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, cache_control);
        response.headers_mut().insert(header::ETAG, etag_header);
        return Ok(response);
    }

    let mut response = Json(value).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, cache_control);
    response.headers_mut().insert(header::ETAG, etag_header);
    Ok(response)
}

fn content_etag(body: &[u8]) -> String {
    format!("\"{}\"", Sha256Digest::hash(body))
}

fn if_none_match_fresh(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|candidate| candidate == "*" || candidate == etag)
        })
}

fn series_key_from_bytes(bytes: Vec<u8>) -> Result<SeriesKey, ApiError> {
    digest_from_bytes(bytes).map(SeriesKey::from_digest)
}

fn artifact_id_from_bytes(bytes: Vec<u8>) -> Result<ArtifactId, ApiError> {
    digest_from_bytes(bytes).map(ArtifactId::from_digest)
}

fn digest_from_bytes(bytes: Vec<u8>) -> Result<Sha256Digest, ApiError> {
    let length = bytes.len();
    let bytes = bytes.try_into().map_err(|_| {
        tracing::error!(length, "database returned invalid SHA-256 digest length");
        ApiError::Internal
    })?;
    Ok(Sha256Digest::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header};

    use super::{if_none_match_fresh, parse_history_date};

    #[test]
    fn if_none_match_accepts_lists_and_wildcards() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_static("\"older\", \"current\""),
        );
        assert!(if_none_match_fresh(&headers, "\"current\""));

        headers.insert(header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        assert!(if_none_match_fresh(&headers, "\"anything\""));
    }

    #[test]
    fn history_dates_require_iso_day() {
        assert!(parse_history_date(Some("2024-01-31"), "since").is_ok());
        let err = parse_history_date(Some("31/01/2024"), "since").unwrap_err();
        assert!(
            err.to_string().contains("YYYY-MM-DD"),
            "unexpected error: {err}"
        );
    }
}
