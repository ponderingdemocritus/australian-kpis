use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use au_kpis_api_http::{AppState, router};
use au_kpis_cache::{CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig};
use au_kpis_config::{
    AppConfig, DatabaseConfig, HttpConfig, LogFormat, RateLimitConfig, TelemetryConfig,
};
use au_kpis_telemetry::Telemetry;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const LONG_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Clone, Default)]
struct RecordingCacheBackend {
    inner: Arc<RecordingCacheState>,
}

#[derive(Debug, Default)]
struct RecordingCacheState {
    values: Mutex<BTreeMap<String, String>>,
    sets: Mutex<Vec<CacheSet>>,
}

#[derive(Debug, Clone)]
struct CacheSet {
    key: String,
    ttl: Duration,
}

#[derive(Debug, Clone, Default)]
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

#[async_trait::async_trait]
impl CacheBackend for RecordingCacheBackend {
    async fn get(&self, key: &str) -> Result<Option<String>, CacheError> {
        Ok(self
            .inner
            .values
            .lock()
            .expect("cache values lock")
            .get(key)
            .cloned())
    }

    async fn set(&self, key: &str, value: String, ttl: Duration) -> Result<(), CacheError> {
        self.inner
            .values
            .lock()
            .expect("cache values lock")
            .insert(key.to_string(), value);
        self.inner
            .sets
            .lock()
            .expect("cache sets lock")
            .push(CacheSet {
                key: key.to_string(),
                ttl,
            });
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, CacheError> {
        Ok(self
            .inner
            .values
            .lock()
            .expect("cache values lock")
            .remove(key)
            .is_some())
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_catalog_single_term_query_stays_under_p95_budget() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let db = TestDb::start("au_kpis_api_search_perf").await;
    seed_catalog(db.pool()).await;

    let app = router(test_state(
        db.pool().clone(),
        Arc::new(CacheClient::from_backend(NoopCacheBackend)),
    ))
    .expect("router");

    for _ in 0..3 {
        assert_search_ok(app.clone(), "/v1/search?q=index").await;
    }

    let mut samples = Vec::with_capacity(30);
    for _ in 0..30 {
        let started = Instant::now();
        assert_search_ok(app.clone(), "/v1/search?q=index").await;
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    let p95_index = (samples.len() * 95).div_ceil(100).saturating_sub(1);
    let p95 = samples[p95_index];

    assert!(
        p95 < Duration::from_millis(100),
        "single-term catalog search p95 should stay under 100 ms; p95={p95:?}, samples={samples:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_catalog_returns_ranked_dataflows_and_measures_with_cache() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let db = TestDb::start("au_kpis_api_search").await;
    seed_catalog(db.pool()).await;

    let cache = RecordingCacheBackend::default();
    let app = router(test_state(
        db.pool().clone(),
        Arc::new(CacheClient::from_backend(cache.clone())),
    ))
    .expect("router");

    let response = app
        .clone()
        .oneshot(request("/v1/search?q=price%20index"))
        .await
        .expect("search response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL).unwrap(),
        "public, max-age=3600, stale-while-revalidate=86400"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("search body");
    let parsed: Value = serde_json::from_slice(&body).expect("search json");

    assert_eq!(parsed["query"], "price index");
    let results = parsed["results"].as_array().expect("results array");
    assert!(
        results.len() >= 2,
        "expected dataflow and measure matches, got {parsed}"
    );
    assert_eq!(results[0]["kind"], "dataflow");
    assert_eq!(results[0]["id"], "abs.cpi");
    assert_eq!(results[0]["source_id"], "abs");
    assert_eq!(results[0]["dataflow_ids"][0], "abs.cpi");
    assert!(
        results[0]["score"].as_f64().expect("score") > 0.0,
        "ranked results should expose positive scores"
    );
    assert!(
        results
            .iter()
            .any(|result| result["kind"] == "measure" && result["id"] == "index"),
        "expected index measure result in {parsed}"
    );
    assert!(
        results.iter().all(|result| result["id"] != "rba.cash_rate"),
        "unrelated cash-rate dataflow should not match {parsed}"
    );

    let sets = cache.inner.sets.lock().expect("cache sets lock").clone();
    assert!(
        sets.iter()
            .any(|set| set.key == "api:search:q=price+index:limit=20" && set.ttl >= LONG_TTL),
        "search response should be cached with a catalog TTL, got {sets:?}"
    );

    sqlx::query("DELETE FROM dataflows WHERE id = 'abs.cpi'")
        .execute(db.pool())
        .await
        .expect("delete dataflow after cache warm");
    let cached = app
        .oneshot(request("/v1/search?q=price%20index"))
        .await
        .expect("cached search response");
    assert_eq!(cached.status(), StatusCode::OK);
    let body = to_bytes(cached.into_body(), usize::MAX)
        .await
        .expect("cached search body");
    let parsed: Value = serde_json::from_slice(&body).expect("cached search json");
    assert_eq!(parsed["results"][0]["id"], "abs.cpi");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_catalog_matches_dataflow_acronyms() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let db = TestDb::start("au_kpis_api_search_acronym").await;
    seed_catalog(db.pool()).await;

    let app = router(test_state(
        db.pool().clone(),
        Arc::new(CacheClient::from_backend(NoopCacheBackend)),
    ))
    .expect("router");

    let response = app
        .oneshot(request("/v1/search?q=CPI&limit=5"))
        .await
        .expect("search response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("search body");
    let parsed: Value = serde_json::from_slice(&body).expect("search json");
    let results = parsed["results"].as_array().expect("results array");

    assert!(
        results.iter().any(|result| {
            result["kind"] == "dataflow"
                && result["id"] == "abs.cpi"
                && result["name"] == "Consumer Price Index"
        }),
        "expected CPI acronym query to return Consumer Price Index dataflow: {parsed}"
    );
}

async fn assert_search_ok(app: axum::Router, uri: &str) {
    let response = app.oneshot(request(uri)).await.expect("search response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("search body");
    let parsed: Value = serde_json::from_slice(&body).expect("search json");
    assert!(
        !parsed["results"]
            .as_array()
            .expect("results array")
            .is_empty(),
        "expected at least one result for {uri}: {parsed}"
    );
}

#[tokio::test]
async fn search_catalog_rejects_blank_query_before_database_access() {
    let app = router(test_state(
        lazy_pool(),
        Arc::new(CacheClient::from_backend(RecordingCacheBackend::default())),
    ))
    .expect("router");

    let response = app
        .oneshot(request("/v1/search?q=%20%20"))
        .await
        .expect("search response");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );
}

fn request(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request")
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

fn lazy_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://postgres:postgres@127.0.0.1/au_kpis_unreachable")
        .expect("lazy postgres pool")
}

#[derive(Debug)]
struct TestDb {
    pool: PgPool,
    _timescale: au_kpis_testing::timescale::TimescaleHarness,
}

impl TestDb {
    async fn start(database: &str) -> Self {
        let timescale = au_kpis_testing::timescale::start_timescale(database)
            .await
            .expect("start timescale test container");
        let cfg = DatabaseConfig {
            url: timescale.url().to_string(),
        };

        let mut last_err = None;
        for _ in 0..10 {
            match au_kpis_db::connect(&cfg).await {
                Ok(pool) => {
                    au_kpis_db::migrate(&pool).await.expect("apply migrations");
                    return Self {
                        pool,
                        _timescale: timescale,
                    };
                }
                Err(err) => {
                    last_err = Some(err);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
        panic!("timescaledb did not accept connections: {last_err:?}");
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }
}

async fn seed_catalog(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage, description)
         VALUES
         ('abs', 'Australian Bureau of Statistics', 'https://www.abs.gov.au', NULL),
         ('rba', 'Reserve Bank of Australia', 'https://www.rba.gov.au', NULL)",
    )
    .execute(pool)
    .await
    .expect("insert sources");

    sqlx::query(
        "INSERT INTO measures (id, name, description, unit, scale)
         VALUES
         ('index', 'Index', 'Price index level', 'index', NULL),
         ('rate', 'Rate', 'Interest rate percent', 'percent', NULL)",
    )
    .execute(pool)
    .await
    .expect("insert measures");

    sqlx::query(
        "INSERT INTO dataflows (
             id, source_id, name, description, dimensions, measures,
             frequency, license, attribution, source_url
         )
         VALUES
         (
             'abs.cpi', 'abs', 'Consumer Price Index',
             'Quarterly consumer price index across Australian regions.',
             ARRAY['region', 'measure'], ARRAY['index'], 'quarterly', 'CC-BY-4.0',
             'Source: Australian Bureau of Statistics',
             'https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia'
         ),
         (
             'rba.cash_rate', 'rba', 'Cash Rate Target', NULL,
             ARRAY['measure'], ARRAY['rate'], 'daily', 'CC-BY-4.0',
             'Source: Reserve Bank of Australia',
             'https://www.rba.gov.au/statistics/cash-rate/'
         )",
    )
    .execute(pool)
    .await
    .expect("insert dataflows");
}

fn docker_available() -> bool {
    std::env::var_os("DOCKER_HOST").is_some()
        || std::path::Path::new("/var/run/docker.sock").exists()
}
