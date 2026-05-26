use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use au_kpis_api_http::{AppState, router};
use au_kpis_cache::{CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig};
use au_kpis_config::{AppConfig, DatabaseConfig, HttpConfig, LogFormat, TelemetryConfig};
use au_kpis_telemetry::Telemetry;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use serde_json::json;
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

#[tokio::test]
async fn dataflow_catalog_routes_serve_cached_payloads_without_database() {
    let cache = RecordingCacheBackend::default();
    cache
        .inner
        .values
        .lock()
        .expect("cache values lock")
        .extend([
            (
                "api:dataflows:list:source=abs:frequency=quarterly".to_string(),
                json!({
                    "dataflows": [cached_dataflow_json()]
                })
                .to_string(),
            ),
            (
                "api:dataflows:get:abs.cpi".to_string(),
                json!({
                    "dataflow": cached_dataflow_json(),
                    "dimensions": [{
                        "id": "region",
                        "name": "Region",
                        "description": null,
                        "codelist_id": "CL_REGION_AU",
                        "position": 0
                    }]
                })
                .to_string(),
            ),
            (
                "api:dataflows:codelist:abs.cpi:region".to_string(),
                json!({
                    "dataflow_id": "abs.cpi",
                    "dimension_id": "region",
                    "codelist": {
                        "id": "CL_REGION_AU",
                        "name": "Australian regions",
                        "description": null,
                        "codes": [{
                            "id": "AUS",
                            "codelist_id": "CL_REGION_AU",
                            "name": "Australia",
                            "description": null,
                            "parent_id": null
                        }]
                    }
                })
                .to_string(),
            ),
        ]);

    let app = router(test_state(
        lazy_pool(),
        Arc::new(CacheClient::from_backend(cache)),
    ))
    .expect("router");

    for uri in [
        "/v1/dataflows?source=abs&frequency=quarterly",
        "/v1/dataflows/abs.cpi",
        "/v1/dataflows/abs.cpi/codelists/region",
    ] {
        let response = app.clone().oneshot(request(uri)).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "public, max-age=3600, stale-while-revalidate=86400"
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert!(
            parsed.to_string().contains("abs.cpi"),
            "expected cached payload for {uri}, got {parsed}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataflow_catalog_endpoints_filter_render_codelists_and_use_long_ttl_cache() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let db = TestDb::start("au_kpis_api_dataflows").await;
    seed_catalog(db.pool()).await;

    let cache = RecordingCacheBackend::default();
    let app = router(test_state(
        db.pool().clone(),
        Arc::new(CacheClient::from_backend(cache.clone())),
    ))
    .expect("router");

    let list = app
        .clone()
        .oneshot(request("/v1/dataflows?source=abs&frequency=quarterly"))
        .await
        .expect("list response");
    assert_eq!(list.status(), StatusCode::OK);
    assert_eq!(
        list.headers().get(header::CACHE_CONTROL).unwrap(),
        "public, max-age=3600, stale-while-revalidate=86400"
    );
    let body = to_bytes(list.into_body(), usize::MAX)
        .await
        .expect("list body");
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("list json");
    assert_eq!(parsed["dataflows"].as_array().unwrap().len(), 1);
    assert_eq!(parsed["dataflows"][0]["id"], "abs.cpi");
    assert_eq!(parsed["dataflows"][0]["frequency"], "quarterly");

    let detail = app
        .clone()
        .oneshot(request("/v1/dataflows/abs.cpi"))
        .await
        .expect("detail response");
    assert_eq!(detail.status(), StatusCode::OK);
    let body = to_bytes(detail.into_body(), usize::MAX)
        .await
        .expect("detail body");
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("detail json");
    assert_eq!(parsed["dataflow"]["id"], "abs.cpi");
    assert_eq!(parsed["dataflow"]["license"], "CC-BY-4.0");
    assert_eq!(parsed["dimensions"][0]["id"], "region");
    assert_eq!(parsed["dimensions"][0]["codelist_id"], "CL_REGION_AU");

    let codelist = app
        .clone()
        .oneshot(request("/v1/dataflows/abs.cpi/codelists/region"))
        .await
        .expect("codelist response");
    assert_eq!(codelist.status(), StatusCode::OK);
    let body = to_bytes(codelist.into_body(), usize::MAX)
        .await
        .expect("codelist body");
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("codelist json");
    assert_eq!(parsed["dataflow_id"], "abs.cpi");
    assert_eq!(parsed["dimension_id"], "region");
    assert_eq!(parsed["codelist"]["id"], "CL_REGION_AU");
    assert_eq!(parsed["codelist"]["codes"][0]["id"], "AUS");
    assert_eq!(parsed["codelist"]["codes"][1]["id"], "NSW");

    let sets = cache.inner.sets.lock().expect("cache sets lock").clone();
    assert!(
        sets.iter()
            .any(|set| set.key.starts_with("api:dataflows:list:") && set.ttl >= LONG_TTL),
        "list response should be cached with a long TTL, got {sets:?}"
    );
    assert!(
        sets.iter()
            .any(|set| set.key == "api:dataflows:get:abs.cpi" && set.ttl >= LONG_TTL),
        "detail response should be cached with a long TTL, got {sets:?}"
    );
    assert!(
        sets.iter()
            .any(|set| set.key == "api:dataflows:codelist:abs.cpi:region" && set.ttl >= LONG_TTL),
        "codelist response should be cached with a long TTL, got {sets:?}"
    );

    sqlx::query("DELETE FROM dataflows WHERE id = 'abs.cpi'")
        .execute(db.pool())
        .await
        .expect("delete dataflow after cache warm");
    let cached = app
        .oneshot(request("/v1/dataflows?source=abs&frequency=quarterly"))
        .await
        .expect("cached list response");
    assert_eq!(cached.status(), StatusCode::OK);
    let body = to_bytes(cached.into_body(), usize::MAX)
        .await
        .expect("cached body");
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("cached json");
    assert_eq!(parsed["dataflows"][0]["id"], "abs.cpi");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataflow_catalog_endpoints_return_problem_json_for_missing_resources() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let db = TestDb::start("au_kpis_api_dataflows_missing").await;
    seed_catalog(db.pool()).await;
    let app = router(test_state(
        db.pool().clone(),
        Arc::new(CacheClient::from_backend(RecordingCacheBackend::default())),
    ))
    .expect("router");

    let missing_dataflow = app
        .clone()
        .oneshot(request("/v1/dataflows/missing.flow"))
        .await
        .expect("missing dataflow response");
    assert_eq!(missing_dataflow.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        missing_dataflow
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap(),
        "application/problem+json"
    );

    let missing_dimension = app
        .oneshot(request("/v1/dataflows/abs.cpi/codelists/missing"))
        .await
        .expect("missing dimension response");
    assert_eq!(missing_dimension.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        missing_dimension
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap(),
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

fn cached_dataflow_json() -> serde_json::Value {
    json!({
        "id": "abs.cpi",
        "source_id": "abs",
        "name": "Consumer Price Index",
        "description": null,
        "dimensions": ["region"],
        "measures": ["index"],
        "frequency": "quarterly",
        "license": "CC-BY-4.0",
        "attribution": "Source: Australian Bureau of Statistics",
        "source_url": "https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia"
    })
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
        "INSERT INTO codelists (id, name, description)
         VALUES
         ('CL_REGION_AU', 'Australian regions', 'ABS statistical regions'),
         ('CL_MEASURE_CPI', 'CPI measures', NULL)",
    )
    .execute(pool)
    .await
    .expect("insert codelists");

    sqlx::query(
        "INSERT INTO codes (codelist_id, id, name, description, parent_id)
         VALUES
         ('CL_REGION_AU', 'AUS', 'Australia', NULL, NULL),
         ('CL_REGION_AU', 'NSW', 'New South Wales', NULL, 'AUS'),
         ('CL_MEASURE_CPI', 'all_groups', 'All groups CPI', NULL, NULL)",
    )
    .execute(pool)
    .await
    .expect("insert codes");

    sqlx::query(
        "INSERT INTO dataflows (
             id, source_id, name, description, dimensions, measures,
             frequency, license, attribution, source_url
         )
         VALUES
         (
             'abs.cpi', 'abs', 'Consumer Price Index',
             'Quarterly CPI across Australian regions.',
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

    sqlx::query(
        "INSERT INTO dimensions (dataflow_id, id, name, description, codelist_id, position)
         VALUES
         ('abs.cpi', 'region', 'Region', 'Geographic region', 'CL_REGION_AU', 0),
         ('abs.cpi', 'measure', 'Measure', NULL, 'CL_MEASURE_CPI', 1),
         ('rba.cash_rate', 'measure', 'Measure', NULL, 'CL_MEASURE_CPI', 0)",
    )
    .execute(pool)
    .await
    .expect("insert dimensions");
}

fn docker_available() -> bool {
    std::env::var_os("DOCKER_HOST").is_some()
        || std::path::Path::new("/var/run/docker.sock").exists()
}
