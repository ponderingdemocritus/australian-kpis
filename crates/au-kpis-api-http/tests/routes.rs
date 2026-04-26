use std::{sync::Arc, time::Duration};

use au_kpis_api_http::{ApiError, AppState, ProblemDetails, router, router_with};
use au_kpis_cache::{CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig};
use au_kpis_config::{AppConfig, DatabaseConfig, HttpConfig, LogFormat, TelemetryConfig};
use au_kpis_telemetry::Telemetry;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use jsonschema::draft202012;
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

#[derive(Debug, Default)]
struct NoopCacheBackend;

#[async_trait::async_trait]
impl CacheBackend for NoopCacheBackend {
    async fn get(&self, _key: &str) -> Result<Option<String>, CacheError> {
        Ok(None)
    }

    async fn set(&self, _key: &str, _value: String, _ttl: Duration) -> Result<(), CacheError> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<bool, CacheError> {
        Ok(false)
    }

    async fn take_token_bucket(
        &self,
        _key: &str,
        _config: TokenBucketConfig,
        _requested: u32,
        _now_ms: u64,
    ) -> Result<RateLimitDecision, CacheError> {
        Ok(RateLimitDecision {
            allowed: true,
            remaining: 0,
            retry_after: Duration::ZERO,
        })
    }
}

fn test_state() -> AppState {
    test_state_with_origins(Vec::new())
}

fn test_state_with_origins(cors_allowed_origins: Vec<String>) -> AppState {
    let db = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://postgres:postgres@localhost/au_kpis")
        .expect("lazy postgres pool");

    let config = AppConfig {
        http: HttpConfig {
            bind: "127.0.0.1:0".into(),
            cors_allowed_origins,
            shutdown_grace_period_secs: 30,
        },
        database: DatabaseConfig {
            url: "postgres://postgres:postgres@localhost/au_kpis".into(),
        },
        cache: au_kpis_config::CacheConfig {
            url: "redis://127.0.0.1:6379".into(),
        },
        telemetry: TelemetryConfig {
            service_name: "au-kpis-test".into(),
            log_format: LogFormat::Json,
            log_level: "info".into(),
            otlp_endpoint: None,
        },
    };

    AppState::new(
        db,
        Arc::new(CacheClient::from_backend(NoopCacheBackend)),
        Arc::new(config),
        Arc::new(Telemetry::disabled()),
        CancellationToken::new(),
    )
}

#[tokio::test]
async fn health_route_returns_ok_json_and_request_id() {
    let response = router(test_state())
        .expect("router")
        .oneshot(
            Request::builder()
                .uri("/v1/health")
                .header(header::ACCEPT_ENCODING, "gzip")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    assert!(
        response.headers().contains_key("x-request-id"),
        "request-id middleware should stamp the response"
    );

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(parsed, serde_json::json!({ "status": "ok" }));
}

#[tokio::test]
async fn openapi_route_serves_generated_spec() {
    let response = router(test_state())
        .expect("router")
        .oneshot(
            Request::builder()
                .uri("/v1/openapi.json")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(parsed["openapi"], "3.1.0");
    assert_eq!(
        parsed["paths"]["/v1/health"]["get"]["operationId"],
        "health"
    );
    assert_eq!(
        parsed["paths"]["/v1/openapi.json"]["get"]["operationId"],
        "openapi"
    );
    assert_eq!(
        parsed["paths"]["/v1/health"]["get"]["responses"]["408"]["content"]["application/problem+json"]
            ["schema"]["$ref"],
        "#/components/schemas/ProblemDetails"
    );
    assert_eq!(
        parsed["paths"]["/v1/openapi.json"]["get"]["responses"]["500"]["content"]["application/problem+json"]
            ["schema"]["$ref"],
        "#/components/schemas/ProblemDetails"
    );

    let schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../au-kpis-openapi/tests/fixtures/openapi-3.1-schema.json"
    ))
    .expect("schema fixture should parse");
    let validator = draft202012::new(&schema).expect("compile schema");
    let output = validator.validate(&parsed);
    assert!(
        output.is_ok(),
        "expected live /v1/openapi.json payload to validate, got {output:?}"
    );
}

#[tokio::test]
async fn validation_errors_render_problem_json() {
    let response = ApiError::Validation("bad query".into()).into_response();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let parsed: ProblemDetails = serde_json::from_slice(&body).expect("problem details json");
    assert_eq!(parsed.title, "Bad Request");
    assert_eq!(parsed.status, 400);
    assert_eq!(parsed.detail.as_deref(), Some("bad query"));
}

#[tokio::test]
async fn internal_errors_do_not_leak_server_details() {
    let response = ApiError::Internal.into_response();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let parsed: ProblemDetails = serde_json::from_slice(&body).expect("problem details json");
    assert_eq!(parsed.title, "Internal Server Error");
    assert_eq!(parsed.detail.as_deref(), Some("internal server error"));
}

#[tokio::test]
async fn cors_does_not_allow_arbitrary_origins_by_default() {
    let response = router(test_state())
        .expect("router")
        .oneshot(
            Request::builder()
                .uri("/v1/health")
                .header(header::ORIGIN, "https://evil.example")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert!(
        !response
            .headers()
            .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN)
    );
}

#[tokio::test]
async fn cors_allows_configured_origin() {
    let response = router(test_state_with_origins(vec![
        "https://app.au-kpis.example".into(),
    ]))
    .expect("router")
    .oneshot(
        Request::builder()
            .uri("/v1/health")
            .header(header::ORIGIN, "https://app.au-kpis.example")
            .body(Body::empty())
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .unwrap(),
        "https://app.au-kpis.example"
    );
    assert!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_EXPOSE_HEADERS)
            .unwrap()
            .to_str()
            .expect("exposed headers string")
            .to_ascii_lowercase()
            .contains("x-request-id")
    );
}

#[tokio::test]
async fn cors_preflight_allows_x_api_key_header_for_configured_origin() {
    let response = router(test_state_with_origins(vec![
        "https://app.au-kpis.example".into(),
    ]))
    .expect("router")
    .oneshot(
        Request::builder()
            .method("OPTIONS")
            .uri("/v1/health")
            .header(header::ORIGIN, "https://app.au-kpis.example")
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
            .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "x-api-key")
            .body(Body::empty())
            .expect("request"),
    )
    .await
    .expect("response");

    assert!(response.status().is_success(), "preflight should succeed");
    assert_eq!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .unwrap(),
        "https://app.au-kpis.example"
    );
    assert!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
            .unwrap()
            .to_str()
            .expect("allow headers string")
            .to_ascii_lowercase()
            .contains("x-api-key")
    );
}

#[tokio::test]
async fn timeout_middleware_returns_problem_json_through_router_stack() {
    async fn slow() -> &'static str {
        tokio::time::sleep(Duration::from_secs(31)).await;
        "slow"
    }

    let response = router_with(
        Router::<AppState>::new().route("/v1/slow", get(slow)),
        test_state(),
    )
    .expect("router")
    .oneshot(
        Request::builder()
            .uri("/v1/slow")
            .body(Body::empty())
            .expect("request"),
    )
    .await
    .expect("response");

    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );
    assert!(
        response.headers().contains_key("x-request-id"),
        "timeout responses should preserve the request id"
    );

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let parsed: ProblemDetails = serde_json::from_slice(&body).expect("problem details json");
    assert_eq!(parsed.title, "Request Timeout");
    assert_eq!(parsed.status, 408);
}
