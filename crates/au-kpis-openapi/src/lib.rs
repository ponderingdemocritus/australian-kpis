//! OpenAPI spec emitter.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

/// Minimal health response exposed by the API skeleton.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    /// Service status.
    pub status: String,
}

/// Documented health handler signature used by the generated spec.
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
pub fn health() -> HealthResponse {
    HealthResponse {
        status: "ok".into(),
    }
}

/// Root OpenAPI document for the API.
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

/// Emit the current OpenAPI document as OpenAPI 3.1 JSON.
pub fn emit() -> String {
    ApiDoc::openapi()
        .to_pretty_json()
        .expect("OpenAPI document should serialize to JSON")
}
