//! axum routes + handlers (library).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::time::Duration;

use au_kpis_config::AppConfig;
use axum::{
    Router,
    error_handling::HandleErrorLayer,
    http::{HeaderValue, Method, header, header::InvalidHeaderValue},
    middleware,
    response::IntoResponse,
    routing::{get, post},
};
use thiserror::Error;
use tower::{BoxError, ServiceBuilder, timeout::TimeoutLayer};
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowOrigin, CorsLayer},
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};

pub mod auth;
pub mod dataflows;
pub mod docs;
pub mod error;
pub mod observations;
pub mod origin_auth;
pub mod rate_limit;
pub mod routes;
pub mod scorecards;
pub mod search;
pub mod series;
pub mod sources;
pub mod state;
pub mod subscriptions;

pub use auth::{RequiredApiKey, require_api_key, verify_api_key_header};
pub use dataflows::{
    DataflowCodelistResponse, DataflowDetailResponse, DataflowsQuery, DataflowsResponse,
    get_dataflow, get_dataflow_codelist, list_dataflows,
};
pub use docs::ApiDoc;
pub use error::{ApiError, ProblemDetails};
pub use observations::{
    ObservationsMetadata, ObservationsResponse, ObservationsRow, PaginationMetadata,
    list_observations,
};
pub use rate_limit::rate_limit;
pub use routes::{
    DependencyHealth, HealthDependencies, HealthResponse, RuntimeHealthResponse, health, livez,
    openapi, readyz,
};
pub use scorecards::{ScorecardHistoryQuery, aps_config, aps_history, aps_latest};
pub use search::{SearchQuery, SearchResponse, SearchResult, SearchResultKind, search_catalog};
pub use series::{SeriesLookupResponse, SeriesRevisionMetadata, get_series};
pub use sources::{
    SourceCatalogDataflow, SourceCatalogEntry, SourceFreshnessPolicy, SourceRequestPolicy,
    SourceSchedule, SourceValidationPolicy, SourcesResponse, get_source, list_sources,
};
pub use state::AppState;
pub use subscriptions::{
    CreateSubscriptionRequest, CreateSubscriptionResponse, DeliveryOptions, DeliveryRunOutcome,
    ListSubscriptionsResponse, RotateSubscriptionSecretResponse, SubscriptionDetails,
    SubscriptionError, WebhookDeliveryEvent, create_subscription, deliver_due_webhooks,
    enqueue_data_update_event, get_subscription, list_subscriptions, revoke_subscription,
    rotate_subscription_secret, run_webhook_delivery_worker, spawn_webhook_delivery_worker,
    verify_subscription,
};

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
    let rate_limit = middleware::from_fn_with_state(state.clone(), rate_limit::rate_limit);
    let origin_auth =
        middleware::from_fn_with_state(state.clone(), origin_auth::require_trusted_origin);

    Ok(routes.with_state(state).layer(
        ServiceBuilder::new()
            .layer(TraceLayer::new_for_http())
            .layer(origin_auth)
            .layer(rate_limit)
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
            .route("/livez", get(livez))
            .route("/readyz", get(readyz))
            .route("/v1/health", get(health))
            .route("/v1/sources", get(sources::list_sources))
            .route("/v1/sources/:source_id", get(sources::get_source))
            .route("/v1/dataflows", get(dataflows::list_dataflows))
            .route("/v1/dataflows/:id", get(dataflows::get_dataflow))
            .route(
                "/v1/dataflows/:id/codelists/:dim",
                get(dataflows::get_dataflow_codelist),
            )
            .route("/v1/observations", get(observations::list_observations))
            .route("/v1/scorecards/aps/config", get(scorecards::aps_config))
            .route("/v1/scorecards/aps/latest", get(scorecards::aps_latest))
            .route("/v1/scorecards/aps/history", get(scorecards::aps_history))
            .route(
                "/v1/scorecards/aps/snapshots/:id",
                get(scorecards::aps_snapshot),
            )
            .route("/v1/series/:dataflow/:series_key", get(series::get_series))
            .route("/v1/search", get(search::search_catalog))
            .route(
                "/v1/subscriptions",
                get(subscriptions::list_subscriptions).post(subscriptions::create_subscription),
            )
            .route(
                "/v1/subscriptions/:id",
                get(subscriptions::get_subscription).delete(subscriptions::revoke_subscription),
            )
            .route(
                "/v1/subscriptions/:id/verify",
                post(subscriptions::verify_subscription),
            )
            .route(
                "/v1/subscriptions/:id/rotate-secret",
                post(subscriptions::rotate_subscription_secret),
            )
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
        .allow_methods([Method::GET, Method::POST, Method::DELETE])
        .allow_headers([
            header::ACCEPT,
            header::ACCEPT_ENCODING,
            header::CONTENT_TYPE,
            header::HeaderName::from_static("x-api-key"),
        ])
        .allow_private_network(true)
        .expose_headers([
            REQUEST_ID_HEADER,
            header::RETRY_AFTER,
            header::HeaderName::from_static("x-ratelimit-limit"),
            header::HeaderName::from_static("x-ratelimit-remaining"),
            header::HeaderName::from_static("x-ratelimit-reset"),
        ]);

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
