use std::{sync::Arc, time::Duration};

use au_kpis_api_http::{
    AppState, router,
    subscriptions::{
        DeliveryOptions, WebhookDeliveryEvent, deliver_due_webhooks, enqueue_data_update_event,
    },
};
use au_kpis_auth::{ApiKeyManager, CreateApiKeyRequest};
use au_kpis_cache::CacheClient;
use au_kpis_config::{
    AppConfig, DatabaseConfig, HttpConfig, LogFormat, RateLimitConfig, TelemetryConfig,
};
use au_kpis_db::{connect, migrate};
use au_kpis_domain::ids::DataflowId;
use au_kpis_telemetry::Telemetry;
use au_kpis_testing::{
    redis::{RedisHarness, start_redis},
    timescale::{TimescaleHarness, start_timescale},
};
use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::State,
    http::{HeaderMap, Request, StatusCode, header},
    response::IntoResponse,
    routing::post,
};
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use sqlx::PgPool;
use tokio::{net::TcpListener, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

type HmacSha256 = Hmac<Sha256>;

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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_subscriptions_creates_authenticated_subscription_and_returns_secret() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let ctx = TestContext::start("au_kpis_subscription_create").await;
    let key = ctx
        .manager
        .create_key(CreateApiKeyRequest {
            name: "webhook client".into(),
            scopes: vec!["subscriptions:write".into()],
            rate_limit_tier: "free".into(),
            actor: "platform-admin@example.com".into(),
        })
        .await
        .expect("create api key");

    let response = router(ctx.state())
        .expect("router")
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/subscriptions")
                .header("x-api-key", &key.plaintext)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "url": "https://example.com/au-kpis-webhook",
                        "dataflow_ids": ["abs.cpi"]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::CREATED);
    let body = response_json(response).await;
    assert_eq!(
        body["subscription"]["url"],
        "https://example.com/au-kpis-webhook"
    );
    assert_eq!(body["subscription"]["status"], "active");
    assert_eq!(body["subscription"]["dataflow_ids"], json!(["abs.cpi"]));
    assert!(
        body["subscription"]["signing_secret"]
            .as_str()
            .is_some_and(|secret| secret.len() >= 32),
        "creation response should expose the signing secret once"
    );

    let persisted: (uuid::Uuid,) = sqlx::query_as(
        "SELECT api_key_id
         FROM webhook_subscriptions
         WHERE target_url = $1",
    )
    .bind("https://example.com/au-kpis-webhook")
    .fetch_one(&ctx.pool)
    .await
    .expect("fetch persisted subscription");
    assert_eq!(persisted.0, key.id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn due_webhook_deliveries_are_hmac_signed_and_marked_delivered() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let ctx = TestContext::start("au_kpis_subscription_delivery").await;
    let receiver = WebhookReceiver::start(StatusCode::NO_CONTENT).await;
    let (subscription_id, signing_secret) = create_subscription(&ctx, &receiver.url()).await;
    let event = WebhookDeliveryEvent {
        dataflow_id: DataflowId::new("abs.cpi").expect("valid dataflow"),
        artifact_id: None,
        observations_loaded: 7,
        occurred_at: Utc.with_ymd_and_hms(2026, 5, 29, 10, 0, 0).unwrap(),
    };

    let enqueued = enqueue_data_update_event(&ctx.pool, &event)
        .await
        .expect("enqueue deliveries");
    assert_eq!(enqueued, 1);
    let due_at = delivery_state(&ctx.pool, subscription_id)
        .await
        .next_attempt_at
        .expect("initial delivery due timestamp");
    let now = due_at + ChronoDuration::seconds(1);

    let outcome = deliver_due_webhooks(
        &ctx.pool,
        &reqwest::Client::new(),
        now,
        DeliveryOptions {
            max_attempts: 3,
            base_backoff: Duration::from_secs(10),
            max_backoff: Duration::from_secs(60),
            batch_size: 10,
        },
    )
    .await
    .expect("deliver due webhooks");
    assert_eq!(outcome.attempted, 1);
    assert_eq!(outcome.delivered, 1);
    assert_eq!(outcome.failed, 0);

    let received = receiver.next().await;
    let timestamp = received
        .headers
        .get("x-au-kpis-webhook-timestamp")
        .expect("timestamp header")
        .to_str()
        .expect("timestamp header string");
    let signature = received
        .headers
        .get("x-au-kpis-webhook-signature")
        .expect("signature header")
        .to_str()
        .expect("signature header string");
    assert_eq!(
        signature,
        expected_signature(&signing_secret, timestamp, &received.body)
    );

    let payload: Value = serde_json::from_slice(&received.body).expect("json payload");
    assert_eq!(payload["event"], "data.updated");
    assert_eq!(payload["dataflow_id"], "abs.cpi");
    assert_eq!(payload["observations_loaded"], 7);

    let delivery: (String, i32, i64) = sqlx::query_as(
        "SELECT status, attempts, (
             SELECT count(*) FROM webhook_delivery_attempts
             WHERE webhook_delivery_attempts.delivery_id = webhook_deliveries.id
         )::BIGINT
         FROM webhook_deliveries
         WHERE subscription_id = $1",
    )
    .bind(subscription_id)
    .fetch_one(&ctx.pool)
    .await
    .expect("fetch delivery status");
    assert_eq!(delivery, ("delivered".into(), 1, 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_webhook_deliveries_retry_with_exponential_backoff_and_stop_after_bound() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let ctx = TestContext::start("au_kpis_subscription_retry").await;
    let receiver = WebhookReceiver::start(StatusCode::SERVICE_UNAVAILABLE).await;
    let (subscription_id, _) = create_subscription(&ctx, &receiver.url()).await;
    let event = WebhookDeliveryEvent {
        dataflow_id: DataflowId::new("abs.cpi").expect("valid dataflow"),
        artifact_id: None,
        observations_loaded: 1,
        occurred_at: Utc.with_ymd_and_hms(2026, 5, 29, 11, 0, 0).unwrap(),
    };
    enqueue_data_update_event(&ctx.pool, &event)
        .await
        .expect("enqueue delivery");

    let due_at = delivery_state(&ctx.pool, subscription_id)
        .await
        .next_attempt_at
        .expect("initial delivery due timestamp");
    let now = due_at + ChronoDuration::seconds(1);
    let options = DeliveryOptions {
        max_attempts: 2,
        base_backoff: Duration::from_secs(10),
        max_backoff: Duration::from_secs(60),
        batch_size: 10,
    };

    let first = deliver_due_webhooks(&ctx.pool, &reqwest::Client::new(), now, options)
        .await
        .expect("first attempt");
    assert_eq!(first.attempted, 1);
    assert_eq!(first.delivered, 0);
    assert_eq!(first.failed, 0);
    let first_failure = delivery_state(&ctx.pool, subscription_id).await;
    assert_eq!(first_failure.status, "pending");
    assert_eq!(first_failure.attempts, 1);
    assert!(first_failure.next_attempt_at.is_some_and(|due| due > now));

    let too_early = deliver_due_webhooks(
        &ctx.pool,
        &reqwest::Client::new(),
        now + ChronoDuration::seconds(9),
        options,
    )
    .await
    .expect("too early retry");
    assert_eq!(too_early.attempted, 0);

    let second = deliver_due_webhooks(
        &ctx.pool,
        &reqwest::Client::new(),
        now + ChronoDuration::seconds(10),
        options,
    )
    .await
    .expect("second attempt");
    assert_eq!(second.attempted, 1);
    assert_eq!(second.delivered, 0);
    assert_eq!(second.failed, 1);

    let exhausted = delivery_state(&ctx.pool, subscription_id).await;
    assert_eq!(exhausted.status, "failed");
    assert_eq!(exhausted.attempts, 2);
    assert!(exhausted.next_attempt_at.is_none());
    assert_eq!(exhausted.attempt_rows, 2);
}

async fn create_subscription(ctx: &TestContext, url: &str) -> (uuid::Uuid, String) {
    let key = ctx
        .manager
        .create_key(CreateApiKeyRequest {
            name: "delivery client".into(),
            scopes: vec!["subscriptions:write".into()],
            rate_limit_tier: "free".into(),
            actor: "platform-admin@example.com".into(),
        })
        .await
        .expect("create api key");
    let response = router(ctx.state())
        .expect("router")
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/subscriptions")
                .header("x-api-key", &key.plaintext)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "url": url,
                        "dataflow_ids": ["abs.cpi"]
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = response_json(response).await;
    (
        body["subscription"]["id"]
            .as_str()
            .expect("subscription id")
            .parse()
            .expect("uuid"),
        body["subscription"]["signing_secret"]
            .as_str()
            .expect("signing secret")
            .to_string(),
    )
}

#[derive(Debug)]
struct DeliveryState {
    status: String,
    attempts: i32,
    next_attempt_at: Option<chrono::DateTime<Utc>>,
    attempt_rows: i64,
}

async fn delivery_state(pool: &PgPool, subscription_id: uuid::Uuid) -> DeliveryState {
    let (status, attempts, next_attempt_at, attempt_rows) = sqlx::query_as(
        "SELECT status, attempts, next_attempt_at, (
             SELECT count(*) FROM webhook_delivery_attempts
             WHERE webhook_delivery_attempts.delivery_id = webhook_deliveries.id
         )::BIGINT
         FROM webhook_deliveries
         WHERE subscription_id = $1",
    )
    .bind(subscription_id)
    .fetch_one(pool)
    .await
    .expect("fetch delivery state");

    DeliveryState {
        status,
        attempts,
        next_attempt_at,
        attempt_rows,
    }
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
         VALUES ('abs', 'Australian Bureau of Statistics', 'https://www.abs.gov.au', 'ABS')
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(pool)
    .await
    .expect("seed source");

    sqlx::query(
        "INSERT INTO dataflows (
             id, source_id, name, description, dimensions, measures, frequency,
             license, attribution, source_url
         )
         VALUES (
             'abs.cpi', 'abs', 'Consumer Price Index', 'CPI',
             ARRAY['region'], ARRAY['cpi'], 'quarterly',
             'CC BY 4.0', 'ABS', 'https://www.abs.gov.au/cpi'
         )
         ON CONFLICT (id) DO NOTHING",
    )
    .execute(pool)
    .await
    .expect("seed dataflow");
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

#[derive(Debug)]
struct ReceivedWebhook {
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Debug)]
struct WebhookReceiver {
    url: String,
    received: mpsc::Receiver<ReceivedWebhook>,
}

impl WebhookReceiver {
    async fn start(status: StatusCode) -> Self {
        let (tx, rx) = mpsc::channel(8);
        let app = Router::new()
            .route("/hook", post(capture_webhook))
            .with_state(ReceiverState { status, tx });
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind receiver");
        let url = format!(
            "http://{}/hook",
            listener.local_addr().expect("receiver addr")
        );
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve receiver");
        });
        Self { url, received: rx }
    }

    fn url(&self) -> String {
        self.url.clone()
    }

    async fn next(mut self) -> ReceivedWebhook {
        self.received
            .recv()
            .await
            .expect("webhook receiver should capture request")
    }
}

#[derive(Debug, Clone)]
struct ReceiverState {
    status: StatusCode,
    tx: mpsc::Sender<ReceivedWebhook>,
}

async fn capture_webhook(
    State(state): State<ReceiverState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    state
        .tx
        .send(ReceivedWebhook { headers, body })
        .await
        .expect("send received webhook");
    state.status
}

fn expected_signature(secret: &str, timestamp: &str, body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac key");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

fn docker_available() -> bool {
    std::env::var_os("DOCKER_HOST").is_some()
        || std::path::Path::new("/var/run/docker.sock").exists()
}
