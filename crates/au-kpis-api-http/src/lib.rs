//! axum routes + handlers (library).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{sync::Arc, time::Duration};

use au_kpis_cache::{CacheClient, CacheError};
use au_kpis_config::AppConfig;
use au_kpis_telemetry::Telemetry;
use axum::{
    Json, Router,
    error_handling::HandleErrorLayer,
    http::{HeaderMap, HeaderValue, Method, StatusCode, header, header::InvalidHeaderValue},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tower::{BoxError, ServiceBuilder, timeout::TimeoutLayer};
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowOrigin, CorsLayer},
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};
use utoipa::{OpenApi, ToSchema};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const REQUEST_ID_HEADER: header::HeaderName = header::HeaderName::from_static("x-request-id");
const X_RATE_LIMIT_LIMIT: header::HeaderName = header::HeaderName::from_static("x-ratelimit-limit");
const X_RATE_LIMIT_REMAINING: header::HeaderName =
    header::HeaderName::from_static("x-ratelimit-remaining");
const X_RATE_LIMIT_RESET: header::HeaderName = header::HeaderName::from_static("x-ratelimit-reset");

/// Shared application state.
#[derive(Debug, Clone)]
pub struct AppState {
    /// Shared Postgres pool.
    pub db: PgPool,
    /// Shared cache client.
    pub cache: Arc<CacheClient>,
    /// Immutable runtime config.
    pub config: Arc<AppConfig>,
    /// Telemetry handle kept alive for process lifetime.
    pub telemetry: Arc<Telemetry>,
    /// Global shutdown token.
    pub shutdown: CancellationToken,
}

impl AppState {
    /// Construct a new shared application state bundle.
    pub fn new(
        db: PgPool,
        cache: Arc<CacheClient>,
        config: Arc<AppConfig>,
        telemetry: Arc<Telemetry>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            db,
            cache,
            config,
            telemetry,
            shutdown,
        }
    }
}

/// Health endpoint response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    /// Current service health.
    pub status: String,
}

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

/// Errors that can occur while assembling the HTTP router.
#[derive(Debug, Error)]
pub enum RouterBuildError {
    /// One of the configured CORS origins is not a valid HTTP header value.
    #[error("invalid CORS origin header value: {0}")]
    InvalidCorsOrigin(#[from] InvalidHeaderValue),
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

/// `GET /v1/health`.
#[utoipa::path(
    get,
    operation_id = "health",
    path = "/v1/health",
    responses(
        (
            status = 200,
            description = "API is healthy.",
            body = HealthResponse
        ),
        (
            status = 408,
            description = "Request timed out.",
            content_type = "application/problem+json",
            body = ProblemDetails
        )
    )
)]
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
    })
}

/// `GET /v1/openapi.json`.
#[utoipa::path(
    get,
    operation_id = "openapi",
    path = "/v1/openapi.json",
    responses(
        (
            status = 200,
            description = "Current OpenAPI document.",
            content_type = "application/json",
            body = Object
        ),
        (
            status = 408,
            description = "Request timed out.",
            content_type = "application/problem+json",
            body = ProblemDetails
        ),
        (
            status = 500,
            description = "OpenAPI generation failed.",
            content_type = "application/problem+json",
            body = ProblemDetails
        )
    )
)]
async fn openapi() -> Result<impl IntoResponse, ApiError> {
    let document = ApiDoc::openapi().to_pretty_json().map_err(|err| {
        tracing::error!(error = %err, "openapi serialization failed");
        ApiError::Internal
    })?;

    Ok(([(header::CONTENT_TYPE, "application/json")], document))
}

/// Root OpenAPI document for the API handlers in this crate.
#[derive(Debug, OpenApi)]
#[openapi(
    info(
        title = "Australian KPIs API",
        version = "0.1.0",
        description = "Unified API for Australian public economic data."
    ),
    paths(health, openapi),
    components(schemas(HealthResponse, ProblemDetails))
)]
pub struct ApiDoc;

/// Compose arbitrary routes with the standard API middleware stack.
pub fn router_with(routes: Router<AppState>, state: AppState) -> Result<Router, RouterBuildError> {
    let cors = cors_layer(&state.config)?;

    Ok(routes.with_state(state).layer(
        ServiceBuilder::new()
            .layer(TraceLayer::new_for_http())
            .layer(cors)
            .layer(CompressionLayer::new())
            .layer(HandleErrorLayer::new(handle_timeout_error))
            .layer(TimeoutLayer::new(REQUEST_TIMEOUT))
            .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
            .layer(PropagateRequestIdLayer::x_request_id()),
    ))
}

/// Minimal application router for the currently implemented handlers.
pub fn router(state: AppState) -> Result<Router, RouterBuildError> {
    router_with(
        Router::<AppState>::new()
            .route("/v1/health", get(health))
            .route("/v1/openapi.json", get(openapi)),
        state,
    )
}

async fn handle_timeout_error(err: BoxError) -> impl IntoResponse {
    if err.is::<tower::timeout::error::Elapsed>() {
        ApiError::RequestTimeout.into_response()
    } else {
        tracing::error!(error = %err, "timeout layer returned unexpected error");
        ApiError::Internal.into_response()
    }
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

#[derive(Debug, Clone, Copy)]
struct RateLimitHeaders {
    retry_after: Duration,
    limit: u32,
    remaining: u32,
    reset_after: Duration,
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

fn cors_layer(config: &AppConfig) -> Result<CorsLayer, RouterBuildError> {
    let mut layer = CorsLayer::new()
        .allow_methods([Method::GET])
        .allow_headers([
            header::ACCEPT,
            header::ACCEPT_ENCODING,
            header::CONTENT_TYPE,
            header::HeaderName::from_static("x-api-key"),
        ])
        .expose_headers([REQUEST_ID_HEADER]);

    if !config.http.cors_allowed_origins.is_empty() {
        let origins = config
            .http
            .cors_allowed_origins
            .iter()
            .map(|origin| HeaderValue::from_str(origin))
            .collect::<Result<Vec<_>, _>>()?;
        layer = layer.allow_origin(AllowOrigin::list(origins));
    }

    Ok(layer)
}
