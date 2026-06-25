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
use chrono::{DateTime, Duration, NaiveDate, Utc};
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

#[derive(Debug, Clone)]
struct IndicatorSeriesRef {
    series_key: Vec<u8>,
    last_observed: DateTime<Utc>,
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
    let as_of = resolved
        .latest_time
        .map(format_snapshot_date)
        .unwrap_or_else(|| Utc::now().date_naive().to_string());
    let snapshot = score_snapshot(&config, &resolved.observations, as_of, None)?;
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
    let mut rows = Vec::with_capacity(config.indicators.len());
    let mut latest_time = None;

    for indicator in &config.indicators {
        let row = load_indicator_observation(pool, indicator, as_of).await?;
        if let Some(row) = row {
            latest_time = latest_time.max(Some(row.latest_time));
            rows.push((indicator, Some(row)));
        } else {
            rows.push((indicator, None));
        }
    }
    let snapshot_time = as_of.or(latest_time).unwrap_or_else(Utc::now);
    let observations = rows
        .into_iter()
        .map(|(indicator, row)| {
            row.map_or_else(
                || {
                    IndicatorObservation::missing(
                        indicator.indicator_id.clone(),
                        indicator.coverage_status,
                    )
                },
                |row| resolved_indicator_observation(indicator, row, snapshot_time),
            )
        })
        .collect();

    Ok(ResolvedScorecardInputs {
        observations,
        latest_time,
    })
}

fn resolved_indicator_observation(
    indicator: &IndicatorConfig,
    row: ResolvedIndicatorRow,
    snapshot_time: DateTime<Utc>,
) -> IndicatorObservation {
    let status = resolved_indicator_status(indicator, &row, snapshot_time);

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

fn resolved_indicator_status(
    indicator: &IndicatorConfig,
    row: &ResolvedIndicatorRow,
    snapshot_time: DateTime<Utc>,
) -> CoverageStatus {
    if indicator.coverage_status == CoverageStatus::VisibleUnscored {
        return CoverageStatus::VisibleUnscored;
    }
    if row.raw_value.is_none() {
        return indicator.coverage_status;
    }
    if is_stale_for_cadence(&indicator.cadence, row.latest_time, snapshot_time) {
        CoverageStatus::Stale
    } else {
        CoverageStatus::Resolved
    }
}

fn is_stale_for_cadence(
    cadence: &str,
    latest_time: DateTime<Utc>,
    snapshot_time: DateTime<Utc>,
) -> bool {
    let Some(max_lag) = max_lag_for_cadence(cadence) else {
        return false;
    };
    snapshot_time.signed_duration_since(latest_time) > max_lag
}

fn max_lag_for_cadence(cadence: &str) -> Option<Duration> {
    match cadence {
        "daily" => Some(Duration::days(2)),
        "weekly" => Some(Duration::days(14)),
        "monthly" => Some(Duration::days(45)),
        "quarterly" => Some(Duration::days(120)),
        "annual" => Some(Duration::days(400)),
        _ => None,
    }
}

async fn load_indicator_observation(
    pool: &PgPool,
    indicator: &IndicatorConfig,
    as_of: Option<DateTime<Utc>>,
) -> Result<Option<ResolvedIndicatorRow>, ApiError> {
    if as_of.is_none() {
        return load_latest_indicator_observation(pool, indicator).await;
    }

    let series_keys = load_indicator_series_keys(pool, indicator).await?;
    if series_keys.is_empty() {
        return Ok(None);
    }

    let row = sqlx::query(
        "SELECT latest.value,
                latest.time,
                latest.series_key,
                latest.source_artifact_id
         FROM unnest($1::bytea[]) AS keys(series_key)
         CROSS JOIN LATERAL (
             SELECT o.value,
                    o.time,
                    o.series_key,
                    o.source_artifact_id,
                    o.revision_no
             FROM observations o
             WHERE o.series_key = keys.series_key
               AND ($2::timestamptz IS NULL OR o.time <= $2)
             ORDER BY o.time DESC, o.revision_no DESC
             LIMIT 1
         ) AS latest
         ORDER BY latest.time DESC, latest.revision_no DESC, latest.series_key ASC
         LIMIT 1",
    )
    .bind(series_keys)
    .bind(as_of)
    .fetch_optional(pool)
    .await?;

    row.map(row_to_resolved_indicator).transpose()
}

async fn load_latest_indicator_observation(
    pool: &PgPool,
    indicator: &IndicatorConfig,
) -> Result<Option<ResolvedIndicatorRow>, ApiError> {
    let series_refs = load_indicator_latest_series_refs(pool, indicator).await?;
    for series_ref in series_refs {
        let row = sqlx::query(
            "SELECT o.value,
                    o.time,
                    o.series_key,
                    o.source_artifact_id
             FROM observations o
             WHERE o.series_key = $1
               AND o.time = $2
             ORDER BY o.revision_no DESC
             LIMIT 1",
        )
        .bind(series_ref.series_key)
        .bind(series_ref.last_observed)
        .fetch_optional(pool)
        .await?;
        if let Some(row) = row {
            return row_to_resolved_indicator(row).map(Some);
        }
    }
    Ok(None)
}

async fn load_indicator_latest_series_refs(
    pool: &PgPool,
    indicator: &IndicatorConfig,
) -> Result<Vec<IndicatorSeriesRef>, ApiError> {
    let rows = sqlx::query(
        "SELECT series_key, last_observed
         FROM series
         WHERE dataflow_id = $1
           AND measure_id = $2
           AND dimensions = $3::jsonb
           AND active
           AND last_observed IS NOT NULL
         ORDER BY last_observed DESC, series_key ASC",
    )
    .bind(&indicator.source_dataflow_id)
    .bind(&indicator.measure_id)
    .bind(serde_json::json!(indicator.dimension_selector))
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(IndicatorSeriesRef {
                series_key: row.try_get("series_key")?,
                last_observed: row.try_get("last_observed")?,
            })
        })
        .collect()
}

async fn load_indicator_series_keys(
    pool: &PgPool,
    indicator: &IndicatorConfig,
) -> Result<Vec<Vec<u8>>, ApiError> {
    let rows = sqlx::query(
        "SELECT series_key
         FROM series
         WHERE dataflow_id = $1
           AND measure_id = $2
           AND dimensions = $3::jsonb
           AND active
         ORDER BY series_key ASC",
    )
    .bind(&indicator.source_dataflow_id)
    .bind(&indicator.measure_id)
    .bind(serde_json::json!(indicator.dimension_selector))
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| row.try_get("series_key").map_err(ApiError::from))
        .collect()
}

fn row_to_resolved_indicator(row: PgRow) -> Result<ResolvedIndicatorRow, ApiError> {
    Ok(ResolvedIndicatorRow {
        raw_value: row.try_get("value")?,
        latest_time: row.try_get("time")?,
        series_key: series_key_from_bytes(row.try_get("series_key")?)?,
        source_artifact_id: artifact_id_from_bytes(row.try_get("source_artifact_id")?)?,
    })
}

async fn load_snapshot_times(
    pool: &PgPool,
    config: &ScorecardConfig,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
) -> Result<Vec<DateTime<Utc>>, ApiError> {
    let mut times = BTreeSet::new();
    for indicator in &config.indicators {
        let series_keys = load_indicator_series_keys(pool, indicator).await?;
        if series_keys.is_empty() {
            continue;
        }

        let rows = sqlx::query(
            "SELECT DISTINCT o.time
             FROM unnest($1::bytea[]) AS keys(series_key)
             JOIN observations o ON o.series_key = keys.series_key
             WHERE ($2::timestamptz IS NULL OR o.time >= $2)
               AND ($3::timestamptz IS NULL OR o.time <= $3)
             ORDER BY o.time ASC",
        )
        .bind(series_keys)
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
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use au_kpis_cache::{
        CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig,
    };
    use au_kpis_config::{
        AppConfig, CacheConfig, DatabaseConfig, HttpConfig, RateLimitConfig, TelemetryConfig,
    };
    use au_kpis_domain::ids::{ArtifactId, Sha256Digest};
    use au_kpis_scorecard::{
        Axis, Confidence, CoverageStatus, Direction, IndicatorConfig, Normalization, Provenance,
    };
    use au_kpis_telemetry::Telemetry;
    use axum::{
        extract::{Query, State},
        http::{HeaderMap, HeaderValue, header},
    };
    use chrono::{TimeZone as _, Utc};
    use sqlx::postgres::PgPoolOptions;
    use tokio_util::sync::CancellationToken;

    use super::{
        ResolvedIndicatorRow, ScorecardHistoryQuery, aps_history, artifact_id_from_bytes,
        digest_from_bytes, format_snapshot_date, if_none_match_fresh, is_stale_for_cadence,
        json_cache_response, load_config, max_lag_for_cadence, parse_history_date, previous_score,
        resolved_indicator_status, series_key_from_bytes,
    };
    use crate::{AppState, error::ApiError};

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
    fn if_none_match_rejects_missing_invalid_and_stale_values() {
        let mut headers = HeaderMap::new();
        assert!(!if_none_match_fresh(&headers, "\"current\""));

        headers.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_bytes(&[0xff]).expect("non-UTF8 header"),
        );
        assert!(!if_none_match_fresh(&headers, "\"current\""));

        headers.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_static("\"older\", \"other\""),
        );
        assert!(!if_none_match_fresh(&headers, "\"current\""));
    }

    #[test]
    fn history_dates_require_iso_day() {
        assert_eq!(parse_history_date(None, "since").unwrap(), None);
        assert!(parse_history_date(Some("2024-01-31"), "since").is_ok());
        let err = parse_history_date(Some("31/01/2024"), "since").unwrap_err();
        assert!(
            err.to_string().contains("YYYY-MM-DD"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn history_bounds_validate_before_database_access() {
        let reversed = aps_history(
            State(test_state()),
            HeaderMap::new(),
            Query(ScorecardHistoryQuery {
                since: Some("2025-02-01".into()),
                until: Some("2025-01-01".into()),
            }),
        )
        .await
        .expect_err("reversed history bounds should be rejected");
        assert!(matches!(reversed, ApiError::Validation(_)));

        let ordered = aps_history(
            State(test_state()),
            HeaderMap::new(),
            Query(ScorecardHistoryQuery {
                since: Some("2025-01-01".into()),
                until: Some("2025-02-01".into()),
            }),
        )
        .await;
        assert!(ordered.is_err());

        let open_ended = aps_history(
            State(test_state()),
            HeaderMap::new(),
            Query(ScorecardHistoryQuery {
                since: Some("2025-01-01".into()),
                until: None,
            }),
        )
        .await;
        assert!(open_ended.is_err());
    }

    #[tokio::test]
    async fn previous_score_skips_database_when_latest_time_is_absent() {
        let pool = lazy_pool();
        let config = load_config().expect("scorecard config");

        assert_eq!(
            previous_score(&pool, &config, None)
                .await
                .expect("no latest snapshot skips lookup"),
            None
        );

        let with_latest = previous_score(
            &pool,
            &config,
            Some(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()),
        )
        .await;
        assert!(with_latest.is_err());
    }

    #[test]
    fn cadence_helpers_cover_fresh_stale_and_unknown_cadences() {
        let latest = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let snapshot = Utc.with_ymd_and_hms(2025, 2, 20, 0, 0, 0).unwrap();

        assert!(is_stale_for_cadence("monthly", latest, snapshot));
        assert!(!is_stale_for_cadence("quarterly", latest, snapshot));
        assert!(!is_stale_for_cadence("ad hoc", latest, snapshot));

        for cadence in ["daily", "weekly", "monthly", "quarterly", "annual"] {
            assert!(
                max_lag_for_cadence(cadence).is_some(),
                "{cadence} should have a max lag"
            );
        }
        assert_eq!(max_lag_for_cadence("irregular"), None);
    }

    #[test]
    fn resolved_indicator_status_preserves_visible_and_missing_cases() {
        let snapshot = Utc.with_ymd_and_hms(2025, 3, 1, 0, 0, 0).unwrap();
        let latest = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let mut indicator = indicator_config(CoverageStatus::MissingExpected, "monthly");
        let row = resolved_row(Some(42.0), latest);

        assert_eq!(
            resolved_indicator_status(&indicator, &resolved_row(None, latest), snapshot),
            CoverageStatus::MissingExpected
        );
        assert_eq!(
            resolved_indicator_status(&indicator, &row, snapshot),
            CoverageStatus::Stale
        );

        indicator.cadence = "quarterly".into();
        assert_eq!(
            resolved_indicator_status(&indicator, &row, snapshot),
            CoverageStatus::Resolved
        );

        indicator.coverage_status = CoverageStatus::VisibleUnscored;
        assert_eq!(
            resolved_indicator_status(&indicator, &row, snapshot),
            CoverageStatus::VisibleUnscored
        );
    }

    #[test]
    fn json_cache_response_returns_body_then_not_modified_for_fresh_etag() {
        let headers = HeaderMap::new();
        let response = json_cache_response(
            &headers,
            &serde_json::json!({"score": 1}),
            "public, max-age=1",
        )
        .expect("json response");
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let etag = response.headers().get(header::ETAG).expect("etag").clone();

        let mut fresh = HeaderMap::new();
        fresh.insert(header::IF_NONE_MATCH, etag);
        let response = json_cache_response(
            &fresh,
            &serde_json::json!({"score": 1}),
            "public, max-age=1",
        )
        .expect("not modified response");
        assert_eq!(response.status(), axum::http::StatusCode::NOT_MODIFIED);
    }

    #[test]
    fn digest_helpers_reject_invalid_database_byte_lengths() {
        let digest = Sha256Digest::hash(b"scorecard");
        assert_eq!(
            format_snapshot_date(Utc.with_ymd_and_hms(2025, 6, 30, 12, 0, 0).unwrap()),
            "2025-06-30"
        );
        series_key_from_bytes(digest.as_bytes().to_vec()).expect("series key");
        artifact_id_from_bytes(digest.as_bytes().to_vec()).expect("artifact id");
        assert!(digest_from_bytes(vec![0; 31]).is_err());
    }

    fn indicator_config(status: CoverageStatus, cadence: &str) -> IndicatorConfig {
        IndicatorConfig {
            indicator_id: "indicator".into(),
            source_dataflow_id: "source.flow".into(),
            measure_id: "measure".into(),
            dimension_selector: BTreeMap::new(),
            axis: Axis::Throughput,
            component: "component".into(),
            weight: 1.0,
            direction: Direction::HigherIsBetter,
            normalization: Normalization {
                worst: 0.0,
                best: 100.0,
            },
            display_label: "Indicator".into(),
            description: "Description".into(),
            unit: "index".into(),
            confidence: Confidence::High,
            coverage_status: status,
            cadence: cadence.into(),
            provenance: Provenance {
                source_url: "https://example.test".into(),
                license: "CC-BY-4.0".into(),
                attribution: "Example".into(),
                retrieved_at: None,
                reviewed_by: None,
                reviewed_at: None,
                notes: None,
            },
        }
    }

    fn resolved_row(
        raw_value: Option<f64>,
        latest_time: chrono::DateTime<Utc>,
    ) -> ResolvedIndicatorRow {
        let artifact_id = ArtifactId::of_content(b"artifact");
        ResolvedIndicatorRow {
            raw_value,
            latest_time,
            series_key: au_kpis_domain::ids::SeriesKey::from_digest(*artifact_id.digest()),
            source_artifact_id: artifact_id,
        }
    }

    fn test_state() -> AppState {
        AppState::new(
            lazy_pool(),
            Arc::new(CacheClient::from_backend(NoopBackend)),
            Arc::new(AppConfig {
                http: HttpConfig::default(),
                database: DatabaseConfig {
                    url: "postgres://postgres:postgres@localhost/unused".into(),
                },
                cache: CacheConfig {
                    url: "redis://localhost:6379".into(),
                },
                telemetry: TelemetryConfig::default(),
                rate_limits: RateLimitConfig::default(),
            }),
            Arc::new(Telemetry::disabled()),
            CancellationToken::new(),
        )
    }

    fn lazy_pool() -> sqlx::PgPool {
        PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/unused")
            .expect("lazy pool")
    }

    #[derive(Debug)]
    struct NoopBackend;

    #[async_trait::async_trait]
    impl CacheBackend for NoopBackend {
        async fn get(&self, _key: &str) -> Result<Option<String>, CacheError> {
            Ok(None)
        }

        async fn set(&self, _key: &str, _value: String, _ttl: Duration) -> Result<(), CacheError> {
            Ok(())
        }

        async fn delete(&self, _key: &str) -> Result<bool, CacheError> {
            Ok(false)
        }

        async fn take_token_bucket(
            &self,
            _key: &str,
            _config: TokenBucketConfig,
            _requested: u32,
            _now_ms: u64,
        ) -> Result<RateLimitDecision, CacheError> {
            Ok(RateLimitDecision {
                allowed: true,
                remaining: 1,
                retry_after: Duration::ZERO,
            })
        }
    }
}
