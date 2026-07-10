use std::{sync::Arc, time::Duration};

use au_kpis_api_http::{
    AppState, router,
    subscriptions::{
        DeliveryOptions, WebhookDeliveryEvent, deliver_due_webhooks, enqueue_data_update_event,
    },
};
use au_kpis_auth::{ApiKeyManager, CreateApiKeyRequest, CreatedApiKey};
use au_kpis_cache::CacheClient;
use au_kpis_config::{
    AppConfig, DatabaseConfig, HttpConfig, LogFormat, RateLimitConfig, TelemetryConfig,
};
use au_kpis_db::{connect, migrate};
use au_kpis_domain::{ArtifactId, DataflowId};
use au_kpis_telemetry::Telemetry;
use au_kpis_testing::{
    redis::{RedisHarness, start_redis},
    timescale::{TimescaleHarness, start_timescale},
};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;

struct TestContext {
    _postgres: TimescaleHarness,
    _redis: RedisHarness,
    pool: PgPool,
    cache: Arc<CacheClient>,
    manager: ApiKeyManager,
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
        let manager = ApiKeyManager::new(pool.clone(), cache.clone());
        seed_dataflow(&pool).await;
        Self {
            _postgres: postgres,
            _redis: redis,
            pool,
            cache,
            manager,
        }
    }

    fn state(&self) -> AppState {
        test_state(self.pool.clone(), self.cache.clone())
    }

    async fn key(&self, name: &str, scopes: &[&str]) -> CreatedApiKey {
        self.manager
            .create_key(CreateApiKeyRequest {
                name: name.into(),
                scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
                rate_limit_tier: "free".into(),
                actor: "platform-admin@example.com".into(),
            })
            .await
            .expect("create API key")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_subscription_returns_pending_record_and_one_time_encrypted_secret() {
    if !docker_available() {
        return;
    }
    let ctx = TestContext::start("au_kpis_subscription_create_v1").await;
    let key = ctx.key("webhook client", &["subscriptions:write"]).await;

    let response = subscription_request(
        &ctx,
        &key,
        "POST",
        "/v1/subscriptions",
        Some(json!({
            "url": "https://example.com/au-kpis-webhook",
            "dataflow_ids": ["abs.cpi"]
        })),
    )
    .await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = response_json(response).await;
    assert_eq!(body["subscription"]["status"], "pending_verification");
    assert_eq!(body["subscription"]["dataflow_ids"], json!(["abs.cpi"]));
    assert!(body["subscription"].get("signing_secret").is_none());
    let secret = body["signing_secret"].as_str().expect("one-time secret");
    let secret_bytes = URL_SAFE_NO_PAD.decode(secret).expect("base64url secret");
    assert_eq!(secret_bytes.len(), 32);

    let row = sqlx::query(
        "SELECT status, secret_ciphertext, secret_nonce, secret_key_version
         FROM webhook_subscriptions",
    )
    .fetch_one(&ctx.pool)
    .await
    .expect("persisted subscription");
    assert_eq!(row.get::<String, _>("status"), "pending_verification");
    assert_ne!(row.get::<Vec<u8>, _>("secret_ciphertext"), secret_bytes);
    assert_eq!(row.get::<Vec<u8>, _>("secret_nonce").len(), 12);
    assert_eq!(row.get::<i32, _>("secret_key_version"), 1);
    let plaintext_column_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM information_schema.columns
             WHERE table_name = 'webhook_subscriptions'
               AND column_name = 'signing_secret'
         )",
    )
    .fetch_one(&ctx.pool)
    .await
    .unwrap();
    assert!(!plaintext_column_exists);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_routes_enforce_scopes_ownership_rotation_and_audited_revocation() {
    if !docker_available() {
        return;
    }
    let ctx = TestContext::start("au_kpis_subscription_ownership_v1").await;
    let read_only = ctx.key("read only", &["subscriptions:read"]).await;
    let forbidden = subscription_request(
        &ctx,
        &read_only,
        "POST",
        "/v1/subscriptions",
        Some(json!({"url": "https://example.com/hook", "dataflow_ids": ["abs.cpi"]})),
    )
    .await;
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

    let owner = ctx
        .key("owner", &["subscriptions:read", "subscriptions:write"])
        .await;
    let created = subscription_request(
        &ctx,
        &owner,
        "POST",
        "/v1/subscriptions",
        Some(json!({"url": "https://example.com/hook", "dataflow_ids": ["abs.cpi"]})),
    )
    .await;
    assert_eq!(created.status(), StatusCode::ACCEPTED);
    let created = response_json(created).await;
    let id = created["subscription"]["id"].as_str().unwrap();
    let original_secret = created["signing_secret"].as_str().unwrap();

    let other = ctx.key("other", &["subscriptions:read"]).await;
    let hidden = subscription_request(
        &ctx,
        &other,
        "GET",
        &format!("/v1/subscriptions/{id}"),
        None,
    )
    .await;
    assert_eq!(hidden.status(), StatusCode::NOT_FOUND);

    let listed = subscription_request(&ctx, &owner, "GET", "/v1/subscriptions", None).await;
    assert_eq!(listed.status(), StatusCode::OK);
    let listed = response_json(listed).await;
    assert_eq!(listed["subscriptions"].as_array().unwrap().len(), 1);

    let rotated = subscription_request(
        &ctx,
        &owner,
        "POST",
        &format!("/v1/subscriptions/{id}/rotate-secret"),
        None,
    )
    .await;
    assert_eq!(rotated.status(), StatusCode::OK);
    let rotated = response_json(rotated).await;
    assert_ne!(rotated["signing_secret"].as_str().unwrap(), original_secret);
    let previous_expires: Option<chrono::DateTime<Utc>> = sqlx::query_scalar(
        "SELECT previous_secret_expires_at FROM webhook_subscriptions WHERE id = $1",
    )
    .bind(id.parse::<Uuid>().unwrap())
    .fetch_one(&ctx.pool)
    .await
    .unwrap();
    assert!(previous_expires.is_some_and(|value| value > Utc::now()));

    let revoked = subscription_request(
        &ctx,
        &owner,
        "DELETE",
        &format!("/v1/subscriptions/{id}"),
        None,
    )
    .await;
    assert_eq!(revoked.status(), StatusCode::NO_CONTENT);
    let row = sqlx::query(
        "SELECT status, secret_ciphertext, revoked_at
         FROM webhook_subscriptions WHERE id = $1",
    )
    .bind(id.parse::<Uuid>().unwrap())
    .fetch_one(&ctx.pool)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("status"), "revoked");
    assert!(row.get::<Option<Vec<u8>>, _>("secret_ciphertext").is_none());
    assert!(
        row.get::<Option<chrono::DateTime<Utc>>, _>("revoked_at")
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_delivery_lease_is_reclaimed_with_stable_event_and_fenced_attempt() {
    if !docker_available() {
        return;
    }
    let ctx = TestContext::start("au_kpis_subscription_lease_v1").await;
    let owner = ctx.key("delivery owner", &["subscriptions:write"]).await;
    let created = subscription_request(
        &ctx,
        &owner,
        "POST",
        "/v1/subscriptions",
        Some(json!({"url": "https://example.com/hook", "dataflow_ids": ["abs.cpi"]})),
    )
    .await;
    let created = response_json(created).await;
    let subscription_id: Uuid = created["subscription"]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    sqlx::query(
        "UPDATE webhook_subscriptions
         SET status = 'active', verified_at = now(),
             target_url = 'https://unresolvable.invalid/hook'
         WHERE id = $1",
    )
    .bind(subscription_id)
    .execute(&ctx.pool)
    .await
    .unwrap();

    let (generation_id, artifact_id) = seed_generation(&ctx.pool).await;
    let event_id = Uuid::new_v4();
    let event = WebhookDeliveryEvent {
        id: event_id,
        generation_id,
        dataflow_id: DataflowId::new("abs.cpi").unwrap(),
        artifact_id: Some(artifact_id),
        observations_loaded: 7,
        occurred_at: Utc::now(),
    };
    assert_eq!(
        enqueue_data_update_event(&ctx.pool, &event).await.unwrap(),
        1
    );
    let stale_worker = Uuid::new_v4();
    sqlx::query(
        "UPDATE webhook_deliveries
         SET status = 'delivering', lease_owner = $2, lease_version = 7,
             leased_until = $3
         WHERE subscription_id = $1",
    )
    .bind(subscription_id)
    .bind(stale_worker)
    .bind(Utc::now() - ChronoDuration::seconds(1))
    .execute(&ctx.pool)
    .await
    .unwrap();

    let outcome = deliver_due_webhooks(
        &ctx.pool,
        &reqwest::Client::new(),
        Utc::now(),
        DeliveryOptions {
            max_attempts: 2,
            base_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(2),
            batch_size: 32,
        },
    )
    .await
    .expect("reclaim delivery");
    assert_eq!(outcome.attempted, 1);
    assert_eq!(outcome.delivered, 0);
    assert_eq!(outcome.failed, 0);

    let row = sqlx::query(
        "SELECT event_id, generation_id, status, attempts, lease_version,
                lease_owner, leased_until
         FROM webhook_deliveries WHERE subscription_id = $1",
    )
    .bind(subscription_id)
    .fetch_one(&ctx.pool)
    .await
    .unwrap();
    assert_eq!(row.get::<Uuid, _>("event_id"), event_id);
    assert_eq!(row.get::<Uuid, _>("generation_id"), generation_id);
    assert_eq!(row.get::<String, _>("status"), "pending");
    assert_eq!(row.get::<i32, _>("attempts"), 1);
    assert_eq!(row.get::<i64, _>("lease_version"), 8);
    assert!(row.get::<Option<Uuid>, _>("lease_owner").is_none());
    assert!(
        row.get::<Option<chrono::DateTime<Utc>>, _>("leased_until")
            .is_none()
    );
    let attempt_lease: i64 = sqlx::query_scalar(
        "SELECT lease_version FROM webhook_delivery_attempts WHERE delivery_id = (
             SELECT id FROM webhook_deliveries WHERE subscription_id = $1
         )",
    )
    .bind(subscription_id)
    .fetch_one(&ctx.pool)
    .await
    .unwrap();
    assert_eq!(attempt_lease, 8);
}

async fn subscription_request(
    ctx: &TestContext,
    key: &CreatedApiKey,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> axum::response::Response {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("x-api-key", &key.plaintext);
    if body.is_some() {
        request = request.header(header::CONTENT_TYPE, "application/json");
    }
    router(ctx.state())
        .expect("router")
        .oneshot(
            request
                .body(body.map_or_else(Body::empty, |value| Body::from(value.to_string())))
                .expect("request"),
        )
        .await
        .expect("response")
}

async fn response_json(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&body).expect("json body")
}

async fn seed_dataflow(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage, description)
         VALUES ('abs', 'Australian Bureau of Statistics', 'https://www.abs.gov.au', 'ABS')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO dataflows (
             id, source_id, name, description, dimensions, measures, frequency,
             license, attribution, source_url
         ) VALUES (
             'abs.cpi', 'abs', 'Consumer Price Index', 'CPI', ARRAY['region'],
             ARRAY['cpi'], 'quarterly', 'CC BY 4.0', 'ABS', 'https://www.abs.gov.au/cpi'
         )",
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_generation(pool: &PgPool) -> (Uuid, ArtifactId) {
    let artifact = ArtifactId::of_content(b"webhook generation fixture");
    let storage_key = format!("artifacts/{artifact}");
    sqlx::query(
        "INSERT INTO artifacts (
             id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at
         ) VALUES ($1, 'abs', 'https://example.test/webhook.json',
                   'application/json', '{}'::JSONB, 1, $2, now())",
    )
    .bind(artifact.digest().as_bytes().as_slice())
    .bind(&storage_key)
    .execute(pool)
    .await
    .unwrap();
    let fetch_id: i64 = sqlx::query_scalar(
        "INSERT INTO artifact_fetches (
             artifact_id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at
         ) VALUES ($1, 'abs', 'https://example.test/webhook.json',
                   'application/json', '{}'::JSONB, 1, $2, now()) RETURNING id",
    )
    .bind(artifact.digest().as_bytes().as_slice())
    .bind(storage_key)
    .fetch_one(pool)
    .await
    .unwrap();
    let work_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO discovered_work (
             id, source_id, dataflow_id, source_url, upstream_revision,
             identity_key, status
         ) VALUES ($1, 'abs', 'abs.cpi', 'https://example.test/webhook.json',
                   'webhook-v1', $2, 'handled')",
    )
    .bind(work_id)
    .bind(vec![0x55_u8; 32])
    .execute(pool)
    .await
    .unwrap();
    let generation_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO ingestion_generations (
             id, discovered_work_id, artifact_fetch_id, source_id, dataflow_id,
             parser_version, transform_version, status, published_at
         ) VALUES ($1, $2, $3, 'abs', 'abs.cpi', 'webhook-test-v1',
                   'identity-v1', 'published', now())",
    )
    .bind(generation_id)
    .bind(work_id)
    .bind(fetch_id)
    .execute(pool)
    .await
    .unwrap();
    (generation_id, artifact)
}

fn test_state(db: PgPool, cache: Arc<CacheClient>) -> AppState {
    AppState::new(
        db,
        cache,
        Arc::new(AppConfig {
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
        }),
        Arc::new(Telemetry::disabled()),
        CancellationToken::new(),
    )
}

fn docker_available() -> bool {
    std::env::var_os("DOCKER_HOST").is_some()
        || std::path::Path::new("/var/run/docker.sock").exists()
}
