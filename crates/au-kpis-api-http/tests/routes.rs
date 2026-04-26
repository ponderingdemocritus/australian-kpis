use std::{sync::Arc, time::Duration};

use au_kpis_api_http::{ApiError, AppState, ProblemDetails, router};
use au_kpis_cache::{CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig};
use au_kpis_config::{AppConfig, DatabaseConfig, HttpConfig, LogFormat, TelemetryConfig};
use au_kpis_telemetry::Telemetry;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
    response::IntoResponse,
};
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
    let db = PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://postgres:postgres@localhost/au_kpis")
        .expect("lazy postgres pool");

    let config = AppConfig {
        http: HttpConfig {
            bind: "127.0.0.1:0".into(),
        },
        database: DatabaseConfig {
            url: "postgres://postgres:postgres@localhost/au_kpis".into(),
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
    let response = ApiError::Internal(anyhow::anyhow!("database url leaked")).into_response();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    let parsed: ProblemDetails = serde_json::from_slice(&body).expect("problem details json");
    assert_eq!(parsed.title, "Internal Server Error");
    assert_eq!(parsed.detail.as_deref(), Some("internal server error"));
}
