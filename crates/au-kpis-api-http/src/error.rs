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
    /// Requested resource was not found.
    #[error("not found: {0}")]
    NotFound(String),
    /// The client supplied invalid request data.
    #[error("validation: {0}")]
    Validation(String),
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
                StatusCode::REQUEST_TIMEOUT,
                ProblemDetails {
                    r#type: "about:blank".into(),
                    title: "Request Timeout".into(),
                    status: StatusCode::REQUEST_TIMEOUT.as_u16(),
                    detail: Some("request timed out".into()),
                    instance: None,
                },
                None,
            ),
            ApiError::Db(err) => internal_server_error(&err),
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

    use axum::http::{HeaderMap, header};

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
}
