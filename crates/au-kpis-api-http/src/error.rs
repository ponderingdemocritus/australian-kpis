//! RFC 7807 API error responses.

use std::time::Duration;

use au_kpis_cache::CacheError;
use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

/// RFC 7807 problem details body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ProblemDetails {
    /// Problem type URI.
    #[serde(rename = "type")]
    pub r#type: String,
    /// Short, human-readable summary.
    pub title: String,
    /// HTTP status code for this problem.
    pub status: u16,
    /// Request-specific detail, if any.
    pub detail: Option<String>,
    /// Resource-specific identifier, if any.
    pub instance: Option<String>,
}

/// API-layer errors rendered as RFC 7807 responses.
#[derive(Debug, Error)]
pub enum ApiError {
    /// Client did not supply a valid API key.
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    /// Authenticated identity lacks a required scope or resource ownership.
    #[error("forbidden: {0}")]
    Forbidden(String),
    /// Requested resource was not found.
    #[error("not found: {0}")]
    NotFound(String),
    /// The client supplied invalid request data.
    #[error("validation: {0}")]
    Validation(String),
    /// Cursor watermark no longer matches the published dataflow generation.
    #[error("stale cursor")]
    StaleCursor,
    /// Per-replica short or bulk admission capacity is exhausted.
    #[error("request admission capacity exhausted")]
    AdmissionOverloaded {
        /// Bulk streams use 429; short requests use 503.
        bulk: bool,
    },
    /// A dependency required by this route class is unavailable.
    #[error("dependency unavailable: {0}")]
    DependencyUnavailable(&'static str),
    /// The client has been rate limited.
    #[error("rate limited")]
    RateLimited {
        /// Seconds until retry.
        retry_after: Duration,
        /// Total quota for the current rate-limit window.
        limit: u32,
        /// Remaining quota for the current rate-limit window.
        remaining: u32,
        /// Seconds until the current rate-limit window resets.
        reset_after: Duration,
    },
    /// The server exceeded the per-request timeout.
    #[error("request timed out")]
    RequestTimeout,
    /// Database access failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// Cache access failed.
    #[error(transparent)]
    Cache(#[from] CacheError),
    /// Unexpected internal failure.
    #[error("internal server error")]
    Internal,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, problem, rate_limit) = match self {
            ApiError::Unauthorized(detail) => (
                StatusCode::UNAUTHORIZED,
                ProblemDetails {
                    r#type: "about:blank".into(),
                    title: "Unauthorized".into(),
                    status: StatusCode::UNAUTHORIZED.as_u16(),
                    detail: Some(detail),
                    instance: None,
                },
                None,
            ),
            ApiError::Forbidden(detail) => (
                StatusCode::FORBIDDEN,
                ProblemDetails {
                    r#type: "about:blank".into(),
                    title: "Forbidden".into(),
                    status: StatusCode::FORBIDDEN.as_u16(),
                    detail: Some(detail),
                    instance: None,
                },
                None,
            ),
            ApiError::NotFound(detail) => (
                StatusCode::NOT_FOUND,
                ProblemDetails {
                    r#type: "about:blank".into(),
                    title: "Not Found".into(),
                    status: StatusCode::NOT_FOUND.as_u16(),
                    detail: Some(detail),
                    instance: None,
                },
                None,
            ),
            ApiError::Validation(detail) => (
                StatusCode::BAD_REQUEST,
                ProblemDetails {
                    r#type: "about:blank".into(),
                    title: "Bad Request".into(),
                    status: StatusCode::BAD_REQUEST.as_u16(),
                    detail: Some(detail),
                    instance: None,
                },
                None,
            ),
            ApiError::StaleCursor => (
                StatusCode::BAD_REQUEST,
                ProblemDetails {
                    r#type: "https://au-kpis.example/problems/stale-cursor".into(),
                    title: "Stale Cursor".into(),
                    status: StatusCode::BAD_REQUEST.as_u16(),
                    detail: Some(
                        "the dataflow changed after this cursor was issued; restart pagination"
                            .into(),
                    ),
                    instance: None,
                },
                None,
            ),
            ApiError::AdmissionOverloaded { bulk } => {
                let status = if bulk {
                    StatusCode::TOO_MANY_REQUESTS
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                };
                let retry_after = if bulk { 5 } else { 1 };
                (
                    status,
                    ProblemDetails {
                        r#type: "https://au-kpis.example/problems/admission-overloaded".into(),
                        title: "Request Capacity Exhausted".into(),
                        status: status.as_u16(),
                        detail: Some("retry after capacity becomes available".into()),
                        instance: None,
                    },
                    Some(RateLimitHeaders {
                        retry_after: Duration::from_secs(retry_after),
                        limit: 0,
                        remaining: 0,
                        reset_after: Duration::from_secs(retry_after),
                    }),
                )
            }
            ApiError::DependencyUnavailable(dependency) => (
                StatusCode::SERVICE_UNAVAILABLE,
                ProblemDetails {
                    r#type: "https://au-kpis.example/problems/dependency-unavailable".into(),
                    title: "Dependency Unavailable".into(),
                    status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                    detail: Some(format!("{dependency} is temporarily unavailable")),
                    instance: None,
                },
                Some(RateLimitHeaders {
                    retry_after: Duration::from_secs(1),
                    limit: 0,
                    remaining: 0,
                    reset_after: Duration::from_secs(1),
                }),
            ),
            ApiError::RateLimited {
                retry_after,
                limit,
                remaining,
                reset_after,
            } => (
                StatusCode::TOO_MANY_REQUESTS,
                ProblemDetails {
                    r#type: "about:blank".into(),
                    title: "Too Many Requests".into(),
                    status: StatusCode::TOO_MANY_REQUESTS.as_u16(),
                    detail: Some("rate limit exceeded".into()),
                    instance: None,
                },
                Some(RateLimitHeaders {
                    retry_after,
                    limit,
                    remaining,
                    reset_after,
                }),
            ),
            ApiError::RequestTimeout => (
                StatusCode::GATEWAY_TIMEOUT,
                ProblemDetails {
                    r#type: "about:blank".into(),
                    title: "Gateway Timeout".into(),
                    status: StatusCode::GATEWAY_TIMEOUT.as_u16(),
                    detail: Some("request timed out".into()),
                    instance: None,
                },
                None,
            ),
            ApiError::Db(err) => database_error(err),
            ApiError::Cache(err) => internal_server_error(&err),
            ApiError::Internal => internal_server_error(&"internal"),
        };

        let mut response = Json(problem).into_response();
        *response.status_mut() = status;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );

        if let Some(rate_limit) = rate_limit {
            crate::rate_limit::insert_rate_limit_error_headers(
                response.headers_mut(),
                rate_limit.retry_after,
                rate_limit.limit,
                rate_limit.remaining,
                rate_limit.reset_after,
            );
        }

        response
    }
}

fn database_error(err: sqlx::Error) -> (StatusCode, ProblemDetails, Option<RateLimitHeaders>) {
    if matches!(
        &err,
        sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
    ) {
        tracing::error!(error = %err, "database dependency unavailable");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            ProblemDetails {
                r#type: "https://au-kpis.example/problems/dependency-unavailable".into(),
                title: "Dependency Unavailable".into(),
                status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                detail: Some("database is temporarily unavailable".into()),
                instance: None,
            },
            None,
        );
    }
    internal_server_error(&err)
}

#[derive(Debug, Clone, Copy)]
struct RateLimitHeaders {
    retry_after: Duration,
    limit: u32,
    remaining: u32,
    reset_after: Duration,
}

fn internal_server_error(
    err: &impl std::fmt::Display,
) -> (StatusCode, ProblemDetails, Option<RateLimitHeaders>) {
    tracing::error!(error = %err, "internal API error");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        ProblemDetails {
            r#type: "about:blank".into(),
            title: "Internal Server Error".into(),
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
            detail: Some("internal server error".into()),
            instance: None,
        },
        None,
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{
        http::{HeaderMap, StatusCode, header},
        response::IntoResponse,
    };

    use super::ApiError;
    use crate::rate_limit::insert_rate_limit_error_headers;

    #[test]
    fn rate_limited_error_headers_round_up_retry_after() {
        let mut headers = HeaderMap::new();

        insert_rate_limit_error_headers(
            &mut headers,
            Duration::from_millis(250),
            10,
            0,
            Duration::from_secs(1),
        );

        assert_eq!(headers.get(header::RETRY_AFTER).unwrap(), "1");
        assert_eq!(headers.get("x-ratelimit-limit").unwrap(), "10");
        assert_eq!(headers.get("x-ratelimit-remaining").unwrap(), "0");
        assert_eq!(headers.get("x-ratelimit-reset").unwrap(), "1");
    }

    #[test]
    fn api_errors_emit_problem_details_with_optional_rate_limit_headers() {
        let validation = ApiError::Validation("bad query".into()).into_response();
        assert_eq!(validation.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            validation.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/problem+json"
        );
        assert!(!validation.headers().contains_key(header::RETRY_AFTER));

        let limited = ApiError::RateLimited {
            retry_after: Duration::from_millis(250),
            limit: 10,
            remaining: 0,
            reset_after: Duration::from_secs(1),
        }
        .into_response();
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(limited.headers().get(header::RETRY_AFTER).unwrap(), "1");
        assert_eq!(limited.headers().get("x-ratelimit-limit").unwrap(), "10");

        let short = ApiError::AdmissionOverloaded { bulk: false }.into_response();
        assert_eq!(short.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(short.headers().get(header::RETRY_AFTER).unwrap(), "1");
        let bulk = ApiError::AdmissionOverloaded { bulk: true }.into_response();
        assert_eq!(bulk.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(bulk.headers().get(header::RETRY_AFTER).unwrap(), "5");

        let timeout = ApiError::RequestTimeout.into_response();
        assert_eq!(timeout.status(), StatusCode::GATEWAY_TIMEOUT);
        let exhausted = ApiError::Db(sqlx::Error::PoolClosed).into_response();
        assert_eq!(exhausted.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
