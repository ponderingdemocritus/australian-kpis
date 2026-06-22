use std::{collections::BTreeMap, sync::Arc, time::Duration};

use au_kpis_api_http::{AppState, router};
use au_kpis_cache::{CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig};
use au_kpis_config::{
    AppConfig, DatabaseConfig, HttpConfig, LogFormat, RateLimitConfig, TelemetryConfig,
};
use au_kpis_domain::ids::{ArtifactId, DataflowId, SeriesKey};
use au_kpis_telemetry::Telemetry;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use chrono::{TimeZone, Utc};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
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

#[tokio::test]
async fn aps_config_endpoint_is_cacheable_and_documented() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://postgres:postgres@localhost/au_kpis")
        .expect("lazy pool");
    let app = router(test_state(pool)).expect("router");

    let first = app
        .clone()
        .oneshot(request("/v1/scorecards/aps/config"))
        .await
        .expect("config response");

    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        first.headers().get(header::CACHE_CONTROL).unwrap(),
        "public, max-age=3600, stale-while-revalidate=86400"
    );
    let etag = first
        .headers()
        .get(header::ETAG)
        .expect("config etag")
        .clone();
    let body = to_bytes(first.into_body(), usize::MAX)
        .await
        .expect("config body");
    let config: Value = serde_json::from_slice(&body).expect("valid config json");
    assert_eq!(config["id"], "aps");
    assert_eq!(config["version"], "aps.v1");
    assert!(
        config["indicators"]
            .as_array()
            .expect("indicators")
            .iter()
            .any(|indicator| indicator["coverage_status"] == "visible_unscored")
    );

    let cached = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/scorecards/aps/config")
                .header(header::IF_NONE_MATCH, etag)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("cached config response");
    assert_eq!(cached.status(), StatusCode::NOT_MODIFIED);

    let openapi = app
        .oneshot(request("/v1/openapi.json"))
        .await
        .expect("openapi response");
    assert_eq!(openapi.status(), StatusCode::OK);
    let body = to_bytes(openapi.into_body(), usize::MAX)
        .await
        .expect("openapi body");
    let spec: Value = serde_json::from_slice(&body).expect("openapi json");
    assert!(spec["paths"]["/v1/scorecards/aps/config"].is_object());
    assert!(spec["paths"]["/v1/scorecards/aps/latest"].is_object());
    assert!(spec["paths"]["/v1/scorecards/aps/history"].is_object());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aps_latest_and_history_score_seeded_inputs_with_provenance() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let db = TestDb::start("au_kpis_api_scorecards").await;
    seed_scorecard_inputs(db.pool()).await;
    let app = router(test_state(db.pool().clone())).expect("router");

    let latest = app
        .clone()
        .oneshot(request("/v1/scorecards/aps/latest"))
        .await
        .expect("latest response");
    assert_eq!(latest.status(), StatusCode::OK);
    assert_eq!(
        latest.headers().get(header::CACHE_CONTROL).unwrap(),
        "public, max-age=60, stale-while-revalidate=300"
    );
    assert!(latest.headers().contains_key(header::ETAG));
    let body = to_bytes(latest.into_body(), usize::MAX)
        .await
        .expect("latest body");
    let snapshot: Value = serde_json::from_slice(&body).expect("latest json");

    assert_eq!(snapshot["scorecard_id"], "aps");
    assert_eq!(snapshot["config_version"], "aps.v1");
    assert_eq!(snapshot["zone"], "green");
    assert_eq!(snapshot["trend"], "up");
    assert_eq!(snapshot["score"], 100.0);
    assert!((snapshot["coverage_pct"].as_f64().unwrap() - 63.63636363636363).abs() < 1e-9);
    assert!(snapshot["confidence_band"]["low"].as_f64().unwrap() < 100.0);
    assert_eq!(snapshot["confidence_band"]["high"], 100.0);

    let contributions = snapshot["contributions"].as_array().expect("contributions");
    assert_eq!(contributions.len(), 8);
    let housing = contribution(contributions, "housing.approvals");
    assert_eq!(housing["raw_value"], 25000.0);
    assert_eq!(housing["normalized_value"], 1.0);
    assert_eq!(housing["coverage_status"], "resolved");
    assert!(
        housing["series_key"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert!(
        housing["source_artifact_id"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert_eq!(housing["latest_period"], "2024-02-01");
    assert_eq!(housing["license"], "CC-BY-4.0");
    assert!(
        housing["source_url"]
            .as_str()
            .unwrap()
            .starts_with("https://")
    );

    let accord = contribution(contributions, "housing.accord-progress");
    assert_eq!(accord["raw_value"], 100.0);
    assert_eq!(accord["normalized_value"], 1.0);
    assert_eq!(accord["coverage_status"], "resolved");
    assert_eq!(accord["latest_period"], "2024-02-01");
    assert_eq!(accord["license"], "NHSAC copyright");
    assert_eq!(accord["source_url"], "https://nhsac.gov.au/publications");

    let productivity = contribution(contributions, "productivity.market-sector");
    assert_eq!(productivity["raw_value"], 3.0);
    assert_eq!(productivity["normalized_value"], 1.0);
    assert_eq!(productivity["coverage_status"], "resolved");
    assert!(
        productivity["series_key"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert!(
        productivity["source_artifact_id"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert_eq!(productivity["latest_period"], "2024-02-01");
    assert_eq!(productivity["license"], "CC-BY-4.0");
    assert_eq!(
        productivity["source_url"],
        "https://www.pc.gov.au/ongoing/productivity-insights"
    );

    let visible = contribution(contributions, "control.enable-spend-ratio");
    assert_eq!(visible["coverage_status"], "visible_unscored");
    assert!(visible["normalized_value"].is_null());

    let history = app
        .oneshot(request(
            "/v1/scorecards/aps/history?since=2024-01-01&until=2024-12-31",
        ))
        .await
        .expect("history response");
    assert_eq!(history.status(), StatusCode::OK);
    assert_eq!(
        history.headers().get(header::CACHE_CONTROL).unwrap(),
        "public, max-age=300, stale-while-revalidate=3600"
    );
    let body = to_bytes(history.into_body(), usize::MAX)
        .await
        .expect("history body");
    let snapshots: Value = serde_json::from_slice(&body).expect("history json");
    let snapshots = snapshots.as_array().expect("snapshot array");
    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[0]["as_of"], "2024-01-01");
    assert_eq!(snapshots[0]["trend"], "unavailable");
    assert_eq!(snapshots[1]["as_of"], "2024-02-01");
    assert_eq!(snapshots[1]["trend"], "up");
    assert_eq!(snapshots[1]["score"], 100.0);
}

fn contribution<'a>(contributions: &'a [Value], indicator_id: &str) -> &'a Value {
    contributions
        .iter()
        .find(|contribution| contribution["indicator_id"] == indicator_id)
        .unwrap_or_else(|| panic!("missing contribution {indicator_id}"))
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
            rate_limits: RateLimitConfig::default(),
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

async fn seed_scorecard_inputs(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage, description)
         VALUES
            ('abs', 'Australian Bureau of Statistics', 'https://www.abs.gov.au', NULL),
            ('nhsac', 'National Housing Supply and Affordability Council', 'https://nhsac.gov.au', NULL),
            ('pc', 'Productivity Commission', 'https://www.pc.gov.au', NULL),
            ('worldbank', 'World Bank', 'https://www.worldbank.org', NULL)",
    )
    .execute(pool)
    .await
    .expect("insert sources");

    sqlx::query(
        "INSERT INTO measures (id, name, description, unit, scale)
         VALUES
            ('dwellings_approved', 'Dwellings approved', NULL, 'dwellings', NULL),
            ('progress_to_target_pct', 'Housing Accord progress to target', NULL, 'percent', NULL),
            ('market_sector_growth', 'Market-sector productivity growth', NULL, 'percent', NULL),
            ('business_entry_score', 'Business entry readiness', NULL, 'index', NULL)",
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
             'abs.building_approvals', 'abs', 'Building approvals', NULL,
             ARRAY['region', 'measure'], ARRAY['dwellings_approved'], 'monthly', 'CC-BY-4.0',
             'Source: Australian Bureau of Statistics',
             'https://www.abs.gov.au/statistics/industry/building-and-construction/building-approvals-australia'
         ),
         (
             'nhsac.housing_accord_progress', 'nhsac', 'Housing Accord progress', NULL,
             ARRAY['region', 'measure'], ARRAY['progress_to_target_pct'], 'annual', 'NHSAC copyright',
             'Source: National Housing Supply and Affordability Council',
             'https://nhsac.gov.au/publications'
         ),
         (
             'pc.productivity_bulletin', 'pc', 'Productivity bulletin', NULL,
             ARRAY['region', 'measure'], ARRAY['market_sector_growth'], 'annual', 'CC-BY-4.0',
             'Source: Productivity Commission',
             'https://www.pc.gov.au/ongoing/productivity-insights'
         ),
         (
             'worldbank.bready', 'worldbank', 'Business Ready', NULL,
             ARRAY['country', 'measure'], ARRAY['business_entry_score'], 'annual', 'World Bank terms',
             'Source: World Bank B-READY',
             'https://www.worldbank.org/en/businessready'
         )",
    )
    .execute(pool)
    .await
    .expect("insert dataflows");

    let housing_artifact = insert_artifact(pool, "abs", b"housing approvals").await;
    let accord_artifact = insert_artifact(pool, "nhsac", b"housing accord").await;
    let productivity_artifact = insert_artifact(pool, "pc", b"productivity bulletin").await;
    let bready_artifact = insert_artifact(pool, "worldbank", b"bready").await;

    let housing = insert_series(
        pool,
        "abs.building_approvals",
        "dwellings_approved",
        "dwellings",
        [("region", "AUS"), ("measure", "dwellings_approved")],
    )
    .await;
    let bready = insert_series(
        pool,
        "worldbank.bready",
        "business_entry_score",
        "index",
        [("country", "AUS"), ("measure", "business_entry_score")],
    )
    .await;
    let productivity = insert_series(
        pool,
        "pc.productivity_bulletin",
        "market_sector_growth",
        "percent",
        [("region", "AUS"), ("measure", "market_sector_growth")],
    )
    .await;
    let accord = insert_series(
        pool,
        "nhsac.housing_accord_progress",
        "progress_to_target_pct",
        "percent",
        [("region", "AUS"), ("measure", "progress_to_target_pct")],
    )
    .await;

    insert_observation(
        pool,
        housing,
        housing_artifact,
        (2024, 1, 1),
        10000.0,
        "month",
    )
    .await;
    insert_observation(pool, bready, bready_artifact, (2024, 1, 1), 50.0, "year").await;
    insert_observation(
        pool,
        productivity,
        productivity_artifact,
        (2024, 1, 1),
        -2.0,
        "year",
    )
    .await;
    insert_observation(pool, accord, accord_artifact, (2024, 1, 1), 0.0, "year").await;
    insert_observation(
        pool,
        housing,
        housing_artifact,
        (2024, 2, 1),
        25000.0,
        "month",
    )
    .await;
    insert_observation(pool, bready, bready_artifact, (2024, 2, 1), 100.0, "year").await;
    insert_observation(
        pool,
        productivity,
        productivity_artifact,
        (2024, 2, 1),
        3.0,
        "year",
    )
    .await;
    insert_observation(pool, accord, accord_artifact, (2024, 2, 1), 100.0, "year").await;
}

async fn insert_artifact(pool: &PgPool, source_id: &str, content: &[u8]) -> ArtifactId {
    let artifact = ArtifactId::of_content(content);
    sqlx::query(
        "INSERT INTO artifacts (
             id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at
         )
         VALUES ($1, $2, $3, 'application/json', '{}'::jsonb, $4, $5, $6)",
    )
    .bind(artifact.digest().as_bytes().as_slice())
    .bind(source_id)
    .bind(format!("https://example.test/{source_id}.json"))
    .bind(i64::try_from(content.len()).expect("artifact length"))
    .bind(format!("artifacts/{artifact}"))
    .bind(Utc.with_ymd_and_hms(2024, 4, 24, 0, 0, 0).unwrap())
    .execute(pool)
    .await
    .expect("insert artifact");
    artifact
}

async fn insert_series<const N: usize>(
    pool: &PgPool,
    dataflow_id: &str,
    measure_id: &str,
    unit: &str,
    dimensions: [(&str, &str); N],
) -> SeriesKey {
    let dataflow = DataflowId::new(dataflow_id).unwrap();
    let dimensions: BTreeMap<String, String> = dimensions
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    let key = SeriesKey::derive(
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
         VALUES ($1, $2, $3, $4, $5, $6, $7, true)",
    )
    .bind(key.digest().as_bytes().as_slice())
    .bind(dataflow_id)
    .bind(measure_id)
    .bind(json!(dimensions))
    .bind(unit)
    .bind(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap())
    .bind(Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap())
    .execute(pool)
    .await
    .expect("insert series");
    key
}

async fn insert_observation(
    pool: &PgPool,
    series_key: SeriesKey,
    artifact: ArtifactId,
    date: (i32, u32, u32),
    value: f64,
    time_precision: &str,
) {
    let observed_at = Utc
        .with_ymd_and_hms(date.0, date.1, date.2, 0, 0, 0)
        .unwrap();
    sqlx::query(
        "INSERT INTO observations (
             series_key, time, revision_no, time_precision, value, status,
             attributes, ingested_at, source_artifact_id
         )
         VALUES ($1, $2, 0, $3, $4, 'normal', '{}'::jsonb, $5, $6)",
    )
    .bind(series_key.digest().as_bytes().as_slice())
    .bind(observed_at)
    .bind(time_precision)
    .bind(value)
    .bind(Utc.with_ymd_and_hms(2024, 4, 24, 0, 0, 0).unwrap())
    .bind(artifact.digest().as_bytes().as_slice())
    .execute(pool)
    .await
    .expect("insert observation");
}

fn docker_available() -> bool {
    if std::env::var_os("DOCKER_HOST").is_some() {
        return true;
    }
    std::os::unix::net::UnixStream::connect("/var/run/docker.sock").is_ok()
}
