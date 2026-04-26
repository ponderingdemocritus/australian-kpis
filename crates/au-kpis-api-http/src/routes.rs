//! HTTP route handlers.

use axum::{
    Json,
    http::header,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

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
            status = 408,
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
