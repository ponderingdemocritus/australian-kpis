//! RFC 7807 API error responses.

use std::time::Duration;

use au_kpis_cache::CacheError;
use axum::{
    Json,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

const X_RATE_LIMIT_LIMIT: header::HeaderName = header::HeaderName::from_static("x-ratelimit-limit");
const X_RATE_LIMIT_REMAINING: header::HeaderName =
    header::HeaderName::from_static("x-ratelimit-remaining");
const X_RATE_LIMIT_RESET: header::HeaderName = header::HeaderName::from_static("x-ratelimit-reset");

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
            insert_header(
                response.headers_mut(),
                header::RETRY_AFTER,
                rate_limit.retry_after.as_secs(),
            );
            insert_header(response.headers_mut(), X_RATE_LIMIT_LIMIT, rate_limit.limit);
            insert_header(
                response.headers_mut(),
                X_RATE_LIMIT_REMAINING,
                rate_limit.remaining,
            );
            insert_header(
                response.headers_mut(),
                X_RATE_LIMIT_RESET,
                rate_limit.reset_after.as_secs(),
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

fn insert_header<T: std::fmt::Display>(
    headers: &mut HeaderMap,
    name: header::HeaderName,
    value: T,
) {
    if let Ok(value) = HeaderValue::from_str(&value.to_string()) {
        headers.insert(name, value);
    }
}

#[cfg(test)]
mod tests {
    use axum::http::header;

    use super::insert_header;

    struct InvalidHeaderValue;

    impl std::fmt::Display for InvalidHeaderValue {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("invalid\nheader")
        }
    }

    #[test]
    fn insert_header_ignores_invalid_display_values() {
        let mut headers = axum::http::HeaderMap::new();

        insert_header(&mut headers, header::RETRY_AFTER, InvalidHeaderValue);

        assert!(!headers.contains_key(header::RETRY_AFTER));
    }
}
