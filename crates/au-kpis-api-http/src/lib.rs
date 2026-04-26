//! axum routes + handlers (library).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{future::pending, sync::Arc, time::Duration};

use anyhow::anyhow;
use au_kpis_cache::{CacheClient, CacheError};
use au_kpis_config::AppConfig;
use au_kpis_telemetry::Telemetry;
use axum::{
    Json, Router,
    error_handling::HandleErrorLayer,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use tokio::{net::TcpListener, signal};
use tokio_util::sync::CancellationToken;
use tower::{BoxError, ServiceBuilder, timeout::TimeoutLayer};
use tower_http::{
    compression::CompressionLayer,
    cors::CorsLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};
use utoipa::{OpenApi, ToSchema};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, problem, retry_after) = match self {
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
            ApiError::RateLimited { retry_after } => (
                StatusCode::TOO_MANY_REQUESTS,
                ProblemDetails {
                    r#type: "about:blank".into(),
                    title: "Too Many Requests".into(),
                    status: StatusCode::TOO_MANY_REQUESTS.as_u16(),
                    detail: Some("rate limit exceeded".into()),
                    instance: None,
                },
                Some(retry_after),
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
            ApiError::Internal(err) => internal_server_error(&err),
        };

        let mut response = Json(problem).into_response();
        *response.status_mut() = status;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );

        if let Some(retry_after) = retry_after {
            if let Ok(value) = HeaderValue::from_str(&retry_after.as_secs().to_string()) {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
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
        )
    )
)]
async fn openapi() -> Result<impl IntoResponse, ApiError> {
    Ok((
        [(header::CONTENT_TYPE, "application/json")],
        ApiDoc::openapi()
            .to_pretty_json()
            .map_err(|err| ApiError::Internal(anyhow!(err)))?,
    ))
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

/// Minimal application router for the currently implemented handlers.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/openapi.json", get(openapi))
        .with_state(state)
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive())
                .layer(CompressionLayer::new())
                .layer(HandleErrorLayer::new(handle_timeout_error))
                .layer(TimeoutLayer::new(REQUEST_TIMEOUT))
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                .layer(PropagateRequestIdLayer::x_request_id()),
        )
}

/// Serve the API until the state's shutdown token is cancelled.
pub async fn serve(listener: TcpListener, state: AppState) -> std::io::Result<()> {
    let shutdown = state.shutdown.clone();
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
}

/// Wait for Ctrl-C or SIGTERM, then cancel the provided shutdown token.
pub async fn shutdown_signal(token: CancellationToken) {
    let ctrl_c = async {
        let _ = signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                let _ = stream.recv().await;
            }
            Err(_) => pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }

    token.cancel();
}

async fn handle_timeout_error(err: BoxError) -> impl IntoResponse {
    if err.is::<tower::timeout::error::Elapsed>() {
        ApiError::RequestTimeout.into_response()
    } else {
        ApiError::Internal(anyhow!(err)).into_response()
    }
}

fn internal_server_error(
    err: &impl std::fmt::Display,
) -> (StatusCode, ProblemDetails, Option<Duration>) {
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
