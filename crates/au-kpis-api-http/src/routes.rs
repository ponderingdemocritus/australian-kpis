//! HTTP route handlers.

use axum::{
    Json,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use utoipa::{OpenApi, ToSchema};

use crate::AppState;
use crate::{docs::ApiDoc, error::ApiError};

/// Health endpoint response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    /// Current service health.
    pub status: String,
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
            status = 504,
            description = "Request timed out.",
            content_type = "application/problem+json",
            body = crate::error::ProblemDetails
        )
    )
)]
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
    })
}

/// One dependency status in the production readiness response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DependencyHealth {
    /// `up`, `down`, or `degraded`.
    pub status: String,
    /// Probe latency in whole milliseconds when a probe was attempted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
}

/// Dependency collection in the production readiness response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct HealthDependencies {
    /// Timescale/Postgres and schema state.
    pub database: DependencyHealth,
    /// Disposable Redis cache/rate-limit state.
    pub redis: DependencyHealth,
    /// OTLP exporter configuration state.
    pub telemetry: DependencyHealth,
}

/// Production liveness/readiness response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RuntimeHealthResponse {
    /// `live`, `ready`, `degraded`, or `not_ready`.
    pub status: String,
    /// Immutable build version or git SHA.
    pub version: String,
    /// Dependency details. Liveness omits dependency probes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<HealthDependencies>,
}

/// `GET /livez`.
#[utoipa::path(
    get,
    operation_id = "liveness",
    path = "/livez",
    responses((status = 200, description = "Process is live.", body = RuntimeHealthResponse))
)]
pub async fn livez() -> Json<RuntimeHealthResponse> {
    Json(RuntimeHealthResponse {
        status: "live".to_string(),
        version: build_version(),
        dependencies: None,
    })
}

/// `GET /readyz`.
#[utoipa::path(
    get,
    operation_id = "readiness",
    path = "/readyz",
    responses(
        (status = 200, description = "Process is ready or ready with disposable dependency degradation.", body = RuntimeHealthResponse),
        (status = 503, description = "Durable database/schema dependency is unavailable.", body = RuntimeHealthResponse)
    )
)]
pub async fn readyz(State(state): State<AppState>) -> Response {
    match tokio::time::timeout(Duration::from_secs(1), readiness(&state)).await {
        Ok((status, body)) => (status, Json(body)).into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(RuntimeHealthResponse {
                status: "not_ready".to_string(),
                version: build_version(),
                dependencies: Some(HealthDependencies {
                    database: dependency("down", None),
                    redis: dependency("degraded", None),
                    telemetry: telemetry_health(&state),
                }),
            }),
        )
            .into_response(),
    }
}

async fn readiness(state: &AppState) -> (StatusCode, RuntimeHealthResponse) {
    let database = probe_database(state).await;
    let redis = probe_redis(state).await;
    let telemetry = telemetry_health(state);
    let database_up = database.status == "up";
    let degraded = redis.status != "up" || telemetry.status != "up";
    let status = if !database_up {
        "not_ready"
    } else if degraded {
        "degraded"
    } else {
        "ready"
    };
    let http_status = if database_up {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        http_status,
        RuntimeHealthResponse {
            status: status.to_string(),
            version: build_version(),
            dependencies: Some(HealthDependencies {
                database,
                redis,
                telemetry,
            }),
        },
    )
}

async fn probe_database(state: &AppState) -> DependencyHealth {
    let started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        sqlx::query_scalar::<_, i64>(
            "SELECT version FROM _sqlx_migrations ORDER BY version DESC LIMIT 1",
        )
        .fetch_optional(&state.db),
    )
    .await;
    let status = if matches!(result, Ok(Ok(Some(_)))) {
        "up"
    } else {
        "down"
    };
    dependency(status, Some(elapsed_millis(started)))
}

async fn probe_redis(state: &AppState) -> DependencyHealth {
    let started = Instant::now();
    let result = tokio::time::timeout(Duration::from_millis(100), state.cache.health_check()).await;
    let status = if matches!(result, Ok(Ok(()))) {
        "up"
    } else {
        "degraded"
    };
    dependency(status, Some(elapsed_millis(started)))
}

fn telemetry_health(state: &AppState) -> DependencyHealth {
    dependency(
        if state.config.telemetry.otlp_endpoint.is_some() {
            "up"
        } else {
            "degraded"
        },
        None,
    )
}

fn dependency(status: &str, latency_ms: Option<u64>) -> DependencyHealth {
    DependencyHealth {
        status: status.to_string(),
        latency_ms,
    }
}

fn elapsed_millis(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn build_version() -> String {
    option_env!("GIT_SHA")
        .unwrap_or(env!("CARGO_PKG_VERSION"))
        .to_string()
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
            status = 504,
            description = "Request timed out.",
            content_type = "application/problem+json",
            body = crate::error::ProblemDetails
        ),
        (
            status = 500,
            description = "OpenAPI generation failed.",
            content_type = "application/problem+json",
            body = crate::error::ProblemDetails
        )
    )
)]
pub async fn openapi() -> Result<Response, ApiError> {
    let document = ApiDoc::openapi().to_pretty_json().map_err(|err| {
        tracing::error!(error = %err, "openapi serialization failed");
        ApiError::Internal
    })?;

    Ok(([(header::CONTENT_TYPE, "application/json")], document).into_response())
}
