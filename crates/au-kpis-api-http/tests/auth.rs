use std::sync::Arc;

use au_kpis_api_http::{AppState, auth::require_api_key, router_with};
use au_kpis_auth::{ApiKeyManager, CreateApiKeyRequest, VerifiedApiKey};
use au_kpis_cache::CacheClient;
use au_kpis_config::{
    AppConfig, DatabaseConfig, HttpConfig, LogFormat, RateLimitConfig, TelemetryConfig,
};
use au_kpis_db::{connect, migrate};
use au_kpis_telemetry::Telemetry;
use au_kpis_testing::{
    redis::{RedisHarness, start_redis},
    timescale::{TimescaleHarness, start_timescale},
};
use axum::{
    Extension, Json, Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
    middleware,
    routing::get,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

struct TestContext {
    _postgres: TimescaleHarness,
    _redis: RedisHarness,
    pool: PgPool,
    cache: Arc<CacheClient>,
}

impl TestContext {
    async fn start(database: &str) -> Self {
        let postgres = start_timescale(database)
            .await
            .expect("start timescaledb container");
        let redis = start_redis().await.expect("start redis container");
        let pool = connect(&DatabaseConfig {
            url: postgres.url().to_string(),
        })
        .await
        .expect("connect postgres");
        migrate(&pool).await.expect("apply migrations");
        let cache = Arc::new(
            CacheClient::connect(redis.url())
                .await
                .expect("connect redis"),
        );

        Self {
            _postgres: postgres,
            _redis: redis,
            pool,
            cache,
        }
    }

    fn state(&self) -> AppState {
        test_state(self.pool.clone(), self.cache.clone())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protected_routes_validate_x_api_key_and_reject_missing_invalid_or_revoked_keys() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let ctx = TestContext::start("au_kpis_api_auth").await;
    let manager = ApiKeyManager::new(ctx.pool.clone(), ctx.cache.clone());
    let created = manager
        .create_key(CreateApiKeyRequest {
            name: "sdk client".into(),
            scopes: vec!["observations:read".into()],
            rate_limit_tier: "free".into(),
            actor: "platform-admin@example.com".into(),
        })
        .await
        .expect("create api key");

    let app = protected_router(ctx.state());

    let valid = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/protected")
                .header("x-api-key", &created.plaintext)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("valid response");
    assert_eq!(valid.status(), StatusCode::OK);
    let body = to_json(valid).await;
    assert_eq!(body["api_key_id"], created.id.to_string());
    assert_eq!(body["name"], "sdk client");

    let missing = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/protected")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("missing response");
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        missing.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );

    let invalid = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/protected")
                .header("x-api-key", "auk_live_invalid")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("invalid response");
    assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);

    manager
        .revoke_key(created.id, "security-admin@example.com")
        .await
        .expect("revoke key");
    let revoked = app
        .oneshot(
            Request::builder()
                .uri("/protected")
                .header("x-api-key", &created.plaintext)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("revoked response");
    assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);
}

fn protected_router(state: AppState) -> axum::Router {
    let protected = Router::new()
        .route("/protected", get(protected_handler))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_key,
        ));
    router_with(protected, state).expect("router")
}

async fn protected_handler(Extension(key): Extension<VerifiedApiKey>) -> Json<Value> {
    Json(json!({
        "api_key_id": key.id.to_string(),
        "name": key.name,
    }))
}

async fn to_json(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&body).expect("json body")
}

fn test_state(db: PgPool, cache: Arc<CacheClient>) -> AppState {
    let config = AppConfig {
        http: HttpConfig {
            bind: "127.0.0.1:0".into(),
            cors_allowed_origins: Vec::new(),
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
        rate_limits: RateLimitConfig::default(),
    };

    AppState::new(
        db,
        cache,
        Arc::new(config),
        Arc::new(Telemetry::disabled()),
        CancellationToken::new(),
    )
}

fn docker_available() -> bool {
    std::env::var_os("DOCKER_HOST").is_some()
        || std::path::Path::new("/var/run/docker.sock").exists()
}
