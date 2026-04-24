//! axum routes + handlers (library).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use axum::{Json, Router, routing::get};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

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
        )
    )
)]
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
    })
}

/// Root OpenAPI document for the API handlers in this crate.
#[derive(Debug, OpenApi)]
#[openapi(
    info(
        title = "Australian KPIs API",
        version = "0.1.0",
        description = "Unified API for Australian public economic data."
    ),
    paths(health),
    components(schemas(HealthResponse))
)]
pub struct ApiDoc;

/// Minimal application router for the currently implemented handlers.
pub fn router() -> Router {
    Router::new().route("/v1/health", get(health))
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    use super::router;

    #[tokio::test]
    async fn health_route_returns_ok() {
        let response = router()
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }
}
