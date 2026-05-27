use std::{sync::Arc, time::Duration};

use au_kpis_api_http::{AppState, router_with};
use au_kpis_auth::{ApiKeyManager, CreateApiKeyRequest};
use au_kpis_cache::CacheClient;
use au_kpis_config::{
    AppConfig, CacheConfig, DatabaseConfig, HttpConfig, LogFormat, RateLimitConfig,
    RateLimitQuotaConfig, TelemetryConfig,
};
use au_kpis_db::{connect, migrate};
use au_kpis_telemetry::Telemetry;
use au_kpis_testing::{
    redis::{RedisHarness, start_redis},
    timescale::{TimescaleHarness, start_timescale},
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    routing::get,
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

struct RedisContext {
    _redis: RedisHarness,
    cache: Arc<CacheClient>,
}

impl RedisContext {
    async fn start() -> Self {
        let redis = start_redis().await.expect("start redis container");
        let cache = Arc::new(
            CacheClient::connect(redis.url())
                .await
                .expect("connect redis"),
        );

        Self {
            _redis: redis,
            cache,
        }
    }
}

struct AuthContext {
    _postgres: TimescaleHarness,
    _redis: RedisHarness,
    pool: PgPool,
    cache: Arc<CacheClient>,
}

impl AuthContext {
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn anonymous_requests_are_limited_by_ip_and_recover_after_refill() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let ctx = RedisContext::start().await;
    let app = public_router(test_state(
        lazy_pool(),
        ctx.cache,
        test_config_with_limits(RateLimitConfig {
            anonymous: RateLimitQuotaConfig {
                per_second: 1,
                per_hour: 100,
                burst_multiplier: 1,
            },
            ..RateLimitConfig::default()
        }),
    ));

    let first = app
        .clone()
        .oneshot(request("/public", None, "203.0.113.10"))
        .await
        .expect("first response");
    assert_eq!(first.status(), StatusCode::OK);
    assert_rate_limit_headers(first.headers(), "1");
    assert!(first.headers().get(header::RETRY_AFTER).is_none());

    let limited = app
        .clone()
        .oneshot(request("/public", None, "203.0.113.10"))
        .await
        .expect("limited response");
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_rate_limit_headers(limited.headers(), "1");
    assert!(
        limited.headers().get(header::RETRY_AFTER).is_some(),
        "429 responses must include Retry-After"
    );

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let recovered = app
        .oneshot(request("/public", None, "203.0.113.10"))
        .await
        .expect("recovered response");
    assert_eq!(recovered.status(), StatusCode::OK);
    assert_rate_limit_headers(recovered.headers(), "1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_requests_are_limited_by_key_even_across_ips() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let ctx = AuthContext::start("au_kpis_api_rate_limit").await;
    let manager = ApiKeyManager::new(ctx.pool.clone(), ctx.cache.clone());
    let created = manager
        .create_key(CreateApiKeyRequest {
            name: "load-test client".into(),
            scopes: vec!["observations:read".into()],
            rate_limit_tier: "free".into(),
            actor: "platform-admin@example.com".into(),
        })
        .await
        .expect("create api key");

    let mut config = RateLimitConfig::default();
    config.tiers.get_mut("free").expect("free tier").per_key = RateLimitQuotaConfig {
        per_second: 1,
        per_hour: 100,
        burst_multiplier: 1,
    };
    config.tiers.get_mut("free").expect("free tier").per_ip = RateLimitQuotaConfig {
        per_second: 100,
        per_hour: 1_000,
        burst_multiplier: 1,
    };

    let app = public_router(test_state(
        ctx.pool,
        ctx.cache,
        test_config_with_limits(config),
    ));

    let first = app
        .clone()
        .oneshot(request("/public", Some(&created.plaintext), "203.0.113.20"))
        .await
        .expect("first response");
    assert_eq!(first.status(), StatusCode::OK);
    assert_rate_limit_headers(first.headers(), "1");

    let limited = app
        .oneshot(request("/public", Some(&created.plaintext), "203.0.113.21"))
        .await
        .expect("limited response");
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_rate_limit_headers(limited.headers(), "1");
    assert!(
        limited.headers().get(header::RETRY_AFTER).is_some(),
        "key limit should return Retry-After"
    );
}

fn public_router(state: AppState) -> Router {
    router_with(
        Router::<AppState>::new().route("/public", get(|| async { "ok" })),
        state,
    )
    .expect("router")
}

fn request(uri: &str, api_key: Option<&str>, ip: &str) -> Request<Body> {
    let mut builder = Request::builder().uri(uri).header("x-forwarded-for", ip);
    if let Some(api_key) = api_key {
        builder = builder.header("x-api-key", api_key);
    }
    builder.body(Body::empty()).expect("request")
}

fn assert_rate_limit_headers(headers: &axum::http::HeaderMap, expected_limit: &str) {
    assert_eq!(
        headers.get("x-ratelimit-limit").expect("limit header"),
        expected_limit
    );
    assert!(
        headers.get("x-ratelimit-remaining").is_some(),
        "remaining header is required"
    );
    assert!(
        headers.get("x-ratelimit-reset").is_some(),
        "reset header is required"
    );
}

fn test_state(db: PgPool, cache: Arc<CacheClient>, config: AppConfig) -> AppState {
    AppState::new(
        db,
        cache,
        Arc::new(config),
        Arc::new(Telemetry::disabled()),
        CancellationToken::new(),
    )
}

fn test_config_with_limits(rate_limits: RateLimitConfig) -> AppConfig {
    AppConfig {
        http: HttpConfig {
            bind: "127.0.0.1:0".into(),
            cors_allowed_origins: Vec::new(),
            shutdown_grace_period_secs: 30,
        },
        database: DatabaseConfig {
            url: "postgres://postgres:postgres@localhost/au_kpis".into(),
        },
        cache: CacheConfig {
            url: "redis://127.0.0.1:6379".into(),
        },
        telemetry: TelemetryConfig {
            service_name: "au-kpis-test".into(),
            log_format: LogFormat::Json,
            log_level: "info".into(),
            otlp_endpoint: None,
        },
        rate_limits,
    }
}

fn lazy_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://postgres:postgres@127.0.0.1/au_kpis_unreachable")
        .expect("lazy postgres pool")
}

fn docker_available() -> bool {
    std::env::var_os("DOCKER_HOST").is_some()
        || std::path::Path::new("/var/run/docker.sock").exists()
}
