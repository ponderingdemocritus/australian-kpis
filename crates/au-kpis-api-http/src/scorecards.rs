//! Derived scorecard API handlers.

use au_kpis_domain::ids::Sha256Digest;
use au_kpis_scorecard::{
    ApsSnapshotSummary, HistoryView, PublishedApsSnapshot, ScorecardConfig, ScorecardError,
    load_aps_history, load_aps_snapshot, load_aps_v1_config, load_latest_aps_snapshot,
};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
    /// Revision view, defaulting to original as-published values.
    #[serde(default)]
    pub view: HistoryView,
    /// Maximum points returned, from 1 through 1,000.
    pub limit: Option<u32>,
}

/// Query string for the latest APS snapshot.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
pub struct ScorecardLatestQuery {
    /// Revision view, defaulting to original as-published values.
    #[serde(default)]
    pub view: HistoryView,
}

/// Query string selecting an immutable APS config version.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
pub struct ScorecardConfigQuery {
    /// Config version; omitted selects the current version.
    pub version: Option<String>,
}

/// `GET /v1/scorecards/aps/config`.
#[utoipa::path(
    get,
    operation_id = "getApsScorecardConfig",
    path = "/v1/scorecards/aps/config",
    params(ScorecardConfigQuery),
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
pub async fn aps_config(
    headers: HeaderMap,
    Query(query): Query<ScorecardConfigQuery>,
) -> Result<Response, ApiError> {
    let config = load_config()?;
    if query
        .version
        .as_deref()
        .is_some_and(|version| version != config.version)
    {
        return Err(ApiError::NotFound(format!(
            "APS config version `{}`",
            query.version.as_deref().unwrap_or_default()
        )));
    }
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
            body = PublishedApsSnapshot,
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
    Query(query): Query<ScorecardLatestQuery>,
) -> Result<Response, ApiError> {
    let snapshot = load_latest_aps_snapshot(&state.db, query.view)
        .await
        .map_err(scorecard_store_error)?
        .ok_or_else(|| ApiError::NotFound("official APS snapshot".to_string()))?;
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
            body = Vec<ApsSnapshotSummary>,
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
    let since = parse_history_date(query.since.as_deref(), "since")?;
    let until = parse_history_date(query.until.as_deref(), "until")?;
    if let (Some(since), Some(until)) = (since, until) {
        if since > until {
            return Err(ApiError::Validation(
                "`since` must be earlier than or equal to `until`".into(),
            ));
        }
    }

    let today = Utc::now().date_naive();
    let since_date = since
        .map(|value| value.date_naive())
        .unwrap_or_else(|| today - Duration::days(364));
    let until_date = until.map(|value| value.date_naive()).unwrap_or(today);
    if until_date.signed_duration_since(since_date) > Duration::days(3_653) {
        return Err(ApiError::Validation(
            "APS history range cannot exceed ten years".into(),
        ));
    }
    let limit = query.limit.unwrap_or(365);
    if limit == 0 || limit > 1_000 {
        return Err(ApiError::Validation(
            "`limit` must be between 1 and 1000".into(),
        ));
    }
    let snapshots = load_aps_history(&state.db, query.view, since_date, until_date, limit)
        .await
        .map_err(scorecard_store_error)?;
    json_cache_response(&headers, &snapshots, APS_HISTORY_CACHE_CONTROL)
}

/// `GET /v1/scorecards/aps/snapshots/{id}`.
#[utoipa::path(
    get,
    operation_id = "getApsScorecardSnapshot",
    path = "/v1/scorecards/aps/snapshots/{id}",
    params(("id" = Uuid, Path, description = "Snapshot revision UUID.")),
    responses(
        (status = 200, description = "Immutable APS snapshot revision.", body = PublishedApsSnapshot),
        (status = 404, description = "Snapshot not found.", body = crate::ProblemDetails, content_type = "application/problem+json")
    ),
    tag = "scorecards"
)]
pub async fn aps_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let snapshot = load_aps_snapshot(&state.db, id)
        .await
        .map_err(scorecard_store_error)?
        .ok_or_else(|| ApiError::NotFound(format!("APS snapshot `{id}`")))?;
    json_cache_response(&headers, &snapshot, APS_HISTORY_CACHE_CONTROL)
}

fn load_config() -> Result<ScorecardConfig, ApiError> {
    load_aps_v1_config().map_err(scorecard_error)
}

fn scorecard_error(err: ScorecardError) -> ApiError {
    tracing::error!(error = %err, "scorecard error");
    ApiError::Internal
}

fn scorecard_store_error(err: au_kpis_scorecard::ScorecardStoreError) -> ApiError {
    match err {
        au_kpis_scorecard::ScorecardStoreError::Db(err) => ApiError::Db(err),
        au_kpis_scorecard::ScorecardStoreError::Validation(message) => {
            ApiError::Validation(message)
        }
        au_kpis_scorecard::ScorecardStoreError::Scorecard(err) => scorecard_error(err),
        au_kpis_scorecard::ScorecardStoreError::Json(err) => {
            tracing::error!(error = %err, "persisted scorecard JSON error");
            ApiError::Internal
        }
    }
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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use au_kpis_cache::{
        CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig,
    };
    use au_kpis_config::{
        AppConfig, CacheConfig, DatabaseConfig, HttpConfig, RateLimitConfig, TelemetryConfig,
    };
    use au_kpis_scorecard::HistoryView;
    use au_kpis_telemetry::Telemetry;
    use axum::{
        extract::{Query, State},
        http::{HeaderMap, HeaderValue, header},
    };
    use sqlx::postgres::PgPoolOptions;
    use tokio_util::sync::CancellationToken;

    use super::{
        ScorecardHistoryQuery, aps_history, if_none_match_fresh, json_cache_response,
        parse_history_date,
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
                view: HistoryView::AsPublished,
                limit: None,
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
                view: HistoryView::AsPublished,
                limit: None,
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
                view: HistoryView::AsPublished,
                limit: None,
            }),
        )
        .await;
        assert!(open_ended.is_err());
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
