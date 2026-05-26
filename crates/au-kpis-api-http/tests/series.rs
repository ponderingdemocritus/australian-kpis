use std::{collections::BTreeMap, sync::Arc, time::Duration};

use au_kpis_api_http::{AppState, router};
use au_kpis_cache::{CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig};
use au_kpis_config::{AppConfig, DatabaseConfig, HttpConfig, LogFormat, TelemetryConfig};
use au_kpis_domain::ids::{ArtifactId, DataflowId, SeriesKey};
use au_kpis_telemetry::Telemetry;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use chrono::{TimeZone, Utc};
use serde_json::json;
use sqlx::PgPool;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn series_endpoint_returns_metadata_latest_observation_and_revision_metadata() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let db = TestDb::start("au_kpis_api_series").await;
    let series_key = seed_series(db.pool()).await;
    let app = router(test_state(db.pool().clone())).expect("router");

    let response = app
        .clone()
        .oneshot(request(&format!("/v1/series/abs.cpi/{series_key}")))
        .await
        .expect("series response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("series body");
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("series json");
    assert_eq!(parsed["series"]["series_key"], series_key.to_string());
    assert_eq!(parsed["series"]["dataflow_id"], "abs.cpi");
    assert_eq!(parsed["series"]["dimensions"], json!({ "region": "AUS" }));
    assert_eq!(parsed["latest_observation"]["value"], 136.9);
    assert_eq!(parsed["latest_observation"]["revision_no"], 1);
    assert_eq!(parsed["revision"]["revision_no"], 1);
    assert_eq!(parsed["revision"]["is_revision"], true);
    assert_eq!(
        parsed["revision"]["source_artifact_id"],
        ArtifactId::of_content(b"api series fixture").to_string()
    );

    let wrong_dataflow = app
        .clone()
        .oneshot(request(&format!("/v1/series/rba.cash_rate/{series_key}")))
        .await
        .expect("wrong dataflow response");
    assert_eq!(wrong_dataflow.status(), StatusCode::NOT_FOUND);

    let missing_key = SeriesKey::derive(
        &DataflowId::new("abs.cpi").unwrap(),
        [("region", "MISSING")],
    );
    let missing = app
        .oneshot(request(&format!("/v1/series/abs.cpi/{missing_key}")))
        .await
        .expect("missing response");
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
}

fn request(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

fn test_state(db: PgPool) -> AppState {
    AppState::new(
        db,
        Arc::new(CacheClient::from_backend(NoopCacheBackend)),
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

async fn seed_series(pool: &PgPool) -> SeriesKey {
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
         VALUES ('index', 'CPI index', NULL, 'index', NULL)",
    )
    .execute(pool)
    .await
    .expect("insert measure");

    sqlx::query(
        "INSERT INTO dataflows (
             id, source_id, name, description, dimensions, measures,
             frequency, license, attribution, source_url
         )
         VALUES
         (
             'abs.cpi', 'abs', 'Consumer Price Index', NULL,
             ARRAY['region'], ARRAY['index'], 'quarterly', 'CC-BY-4.0',
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

    let dataflow = DataflowId::new("abs.cpi").unwrap();
    let dimensions: BTreeMap<String, String> = [("region".to_string(), "AUS".to_string())]
        .into_iter()
        .collect();
    let series_key = SeriesKey::derive(
        &dataflow,
        dimensions
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    sqlx::query(
        "INSERT INTO series (
             series_key, dataflow_id, measure_id, dimensions, unit,
             first_observed, last_observed, active
         )
         VALUES ($1, 'abs.cpi', 'index', $2, 'index', $3, $4, true)",
    )
    .bind(series_key.digest().as_bytes().as_slice())
    .bind(json!(dimensions))
    .bind(Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap())
    .bind(Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap())
    .execute(pool)
    .await
    .expect("insert series");

    let artifact = ArtifactId::of_content(b"api series fixture");
    sqlx::query(
        "INSERT INTO artifacts (
             id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at
         )
         VALUES ($1, 'abs', 'https://example.test/cpi.json', 'application/json',
                 '{}'::jsonb, 128, $2, $3)",
    )
    .bind(artifact.digest().as_bytes().as_slice())
    .bind(format!("artifacts/{artifact}"))
    .bind(Utc.with_ymd_and_hms(2024, 7, 24, 0, 0, 0).unwrap())
    .execute(pool)
    .await
    .expect("insert artifact");

    insert_observation(pool, series_key, artifact, (2024, 3, 1), 0, 135.0).await;
    insert_observation(pool, series_key, artifact, (2024, 6, 1), 0, 136.2).await;
    insert_observation(pool, series_key, artifact, (2024, 6, 1), 1, 136.9).await;

    series_key
}

async fn insert_observation(
    pool: &PgPool,
    series_key: SeriesKey,
    artifact: ArtifactId,
    date: (i32, u32, u32),
    revision_no: i32,
    value: f64,
) {
    sqlx::query(
        "INSERT INTO observations (
             series_key, time, revision_no, time_precision, value, status,
             attributes, ingested_at, source_artifact_id
         )
         VALUES ($1, $2, $3, 'quarter', $4, 'normal',
                 '{}'::jsonb, $5, $6)",
    )
    .bind(series_key.digest().as_bytes().as_slice())
    .bind(
        Utc.with_ymd_and_hms(date.0, date.1, date.2, 0, 0, 0)
            .unwrap(),
    )
    .bind(revision_no)
    .bind(value)
    .bind(Utc.with_ymd_and_hms(2024, 7, 24, 0, 0, 0).unwrap())
    .bind(artifact.digest().as_bytes().as_slice())
    .execute(pool)
    .await
    .expect("insert observation");
}

fn docker_available() -> bool {
    std::env::var_os("DOCKER_HOST").is_some()
        || std::path::Path::new("/var/run/docker.sock").exists()
}
