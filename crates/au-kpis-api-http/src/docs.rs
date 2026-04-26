//! OpenAPI document assembly.

use utoipa::OpenApi;

use crate::{
    error::ProblemDetails,
    routes::{__path_health, __path_openapi, HealthResponse},
};

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
