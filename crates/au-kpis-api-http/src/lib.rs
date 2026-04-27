//! axum routes + handlers (library).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::time::Duration;

use au_kpis_config::AppConfig;
use axum::{
    Router,
    error_handling::HandleErrorLayer,
    http::{HeaderValue, Method, header, header::InvalidHeaderValue},
    response::IntoResponse,
    routing::get,
};
use thiserror::Error;
use tower::{BoxError, ServiceBuilder, timeout::TimeoutLayer};
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowOrigin, CorsLayer},
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};

pub mod docs;
pub mod error;
pub mod routes;
pub mod state;

pub use docs::ApiDoc;
pub use error::{ApiError, ProblemDetails};
pub use routes::{HealthResponse, health, openapi};
pub use state::AppState;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const REQUEST_ID_HEADER: header::HeaderName = header::HeaderName::from_static("x-request-id");

/// Errors that can occur while assembling the HTTP router.
#[derive(Debug, Error)]
pub enum RouterBuildError {
    /// One of the configured CORS origins is not a valid HTTP header value.
    #[error("invalid CORS origin header value: {0}")]
    InvalidCorsOrigin(#[from] InvalidHeaderValue),
}

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

#[cfg(test)]
mod tests {
    use std::io;

    use axum::{http::StatusCode, response::IntoResponse};
    use tower::BoxError;

    use super::handle_timeout_error;

    #[tokio::test]
    async fn unexpected_timeout_layer_errors_map_to_internal_problem() {
        let err: BoxError = Box::new(io::Error::other("unexpected middleware failure"));
        let response = handle_timeout_error(err).await.into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
