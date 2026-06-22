use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{Float64Array, StringArray, UInt32Array};
use au_kpis_api_http::{AppState, ObservationsResponse, router};
use au_kpis_cache::{CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig};
use au_kpis_config::{
    AppConfig, DatabaseConfig, HttpConfig, LogFormat, RateLimitConfig, TelemetryConfig,
};
use au_kpis_domain::ids::{ArtifactId, DataflowId, MeasureId, SeriesKey};
use au_kpis_telemetry::Telemetry;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use chrono::{TimeZone, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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
async fn observations_endpoint_streams_latest_json_csv_and_cache_headers() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let db = TestDb::start("au_kpis_api_observations").await;
    seed_observations(db.pool()).await;
    let app = router(test_state(db.pool().clone())).expect("router");

    let first = app
        .clone()
        .oneshot(request(
            "/v1/observations?dataflow=abs.cpi&dimensions[region]=AUS&since=2024-01-01&until=2024-12-31&limit=1",
        ))
        .await
        .expect("json response");

    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        first.headers().get(header::CACHE_CONTROL).unwrap(),
        "public, max-age=60, stale-while-revalidate=300"
    );
    let etag = first.headers().get(header::ETAG).unwrap().clone();
    let body = to_bytes(first.into_body(), usize::MAX)
        .await
        .expect("json body");
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
    assert_eq!(parsed["metadata"]["license"], "CC-BY-4.0");
    assert_eq!(
        parsed["metadata"]["attribution"],
        "Source: Australian Bureau of Statistics"
    );
    assert_eq!(parsed["observations"].as_array().unwrap().len(), 1);
    assert_eq!(parsed["observations"][0]["value"], 135.0);
    let cursor = parsed["pagination"]["next_cursor"]
        .as_str()
        .expect("next cursor")
        .to_string();

    let second = app
        .clone()
        .oneshot(request(&format!(
            "/v1/observations?dataflow=abs.cpi&dimensions[region]=AUS&cursor={cursor}&limit=10"
        )))
        .await
        .expect("second page response");
    let body = to_bytes(second.into_body(), usize::MAX)
        .await
        .expect("second body");
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("valid json");
    assert_eq!(parsed["observations"].as_array().unwrap().len(), 1);
    assert_eq!(parsed["observations"][0]["value"], 136.2);
    assert!(parsed["pagination"]["next_cursor"].is_null());

    let cached = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/observations?dataflow=abs.cpi&dimensions[region]=AUS&since=2024-01-01&until=2024-12-31&limit=1")
                .header(header::IF_NONE_MATCH, etag.clone())
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("cached response");
    assert_eq!(cached.status(), StatusCode::NOT_MODIFIED);

    sqlx::query(
        "UPDATE series SET updated_at = now() + interval '1 second' WHERE dataflow_id = 'abs.cpi'",
    )
    .execute(db.pool())
    .await
    .expect("touch series metadata");
    let changed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/observations?dataflow=abs.cpi&dimensions[region]=AUS&since=2024-01-01&until=2024-12-31&limit=1")
                .header(header::IF_NONE_MATCH, etag)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("changed etag response");
    assert_eq!(changed.status(), StatusCode::OK);

    let csv = app
        .oneshot(request(
            "/v1/observations?dataflow=abs.cpi&dimensions[region]=AUS&format=csv&limit=1",
        ))
        .await
        .expect("csv response");
    assert_eq!(csv.status(), StatusCode::OK);
    assert_eq!(
        csv.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/csv; charset=utf-8"
    );
    assert_eq!(
        csv.headers().get(header::CACHE_CONTROL).unwrap(),
        "public, max-age=60, stale-while-revalidate=300"
    );
    assert!(!csv.headers().contains_key(header::ETAG));
    let body = String::from_utf8(
        to_bytes(csv.into_body(), usize::MAX)
            .await
            .expect("csv body")
            .to_vec(),
    )
    .expect("csv utf-8");
    assert!(body.starts_with("# dataflow=abs.cpi,license=CC-BY-4.0"));
    assert!(body.contains("series_key,time,time_precision,value,status,revision_no"));
    assert!(body.contains(",135,normal,1,"));
    assert!(body.contains("# next_cursor="));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observations_endpoint_streams_decodable_parquet_with_headers() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let db = TestDb::start("au_kpis_api_observations_parquet").await;
    seed_observations(db.pool()).await;
    let app = router(test_state(db.pool().clone())).expect("router");

    let response = app
        .oneshot(request(
            "/v1/observations?dataflow=abs.cpi&dimensions[region]=AUS&format=parquet&limit=10",
        ))
        .await
        .expect("parquet response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/vnd.apache.parquet"
    );
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL).unwrap(),
        "public, max-age=60, stale-while-revalidate=300"
    );
    assert!(!response.headers().contains_key(header::ETAG));

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("parquet body");
    assert_eq!(&body[..4], b"PAR1");
    assert_eq!(&body[body.len() - 4..], b"PAR1");

    let reader = ParquetRecordBatchReaderBuilder::try_new(body)
        .expect("parquet reader")
        .build()
        .expect("record batch reader");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("parquet batches");
    let batch = batches.first().expect("at least one batch");
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec![
            "series_key",
            "time",
            "time_precision",
            "value",
            "status",
            "revision_no",
            "dimensions",
            "attributes",
            "ingested_at",
            "source_artifact_id",
            "measure_id",
            "unit",
        ]
    );
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );

    let value = batch
        .column_by_name("value")
        .expect("value column")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("value float64");
    let revision_no = batch
        .column_by_name("revision_no")
        .expect("revision column")
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("revision uint32");
    let dimensions = batch
        .column_by_name("dimensions")
        .expect("dimensions column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("dimensions string");

    assert_eq!(value.value(0), 135.0);
    assert_eq!(revision_no.value(0), 1);
    assert_eq!(dimensions.value(0), "{\"region\":\"AUS\"}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broad_high_cardinality_observations_return_validation_error() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let db = TestDb::start("au_kpis_api_observations_high_cardinality").await;
    seed_observations(db.pool()).await;
    for idx in 0..510 {
        let region = format!("R{idx:03}");
        insert_series(db.pool(), &region).await;
    }
    let app = router(test_state(db.pool().clone())).expect("router");

    let response = app
        .oneshot(request("/v1/observations?dataflow=abs.cpi&limit=3"))
        .await
        .expect("high-cardinality broad response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("problem body");
    let parsed: serde_json::Value =
        serde_json::from_slice(&body).expect("problem details response");
    assert_eq!(parsed["status"], StatusCode::BAD_REQUEST.as_u16());
    assert!(
        parsed["detail"]
            .as_str()
            .unwrap_or_default()
            .contains("add more dimensions[] filters"),
        "unexpected validation body: {parsed}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paginated_observations_concatenate_to_the_full_result() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let db = TestDb::start("au_kpis_api_observations_pagination").await;
    seed_observations(db.pool()).await;
    let artifact = ArtifactId::of_content(b"api observations fixture");
    seed_extra_pagination_observations(db.pool(), artifact).await;
    let app = router(test_state(db.pool().clone())).expect("router");

    let full = get_observations_json(app.clone(), "/v1/observations?dataflow=abs.cpi&limit=100")
        .await
        .observations;
    assert!(full.len() > 8, "fixture should exercise multiple pages");

    for limit in 1..=5 {
        let mut cursor = None;
        let mut paged = Vec::new();
        for _ in 0..20 {
            let uri = match &cursor {
                Some(cursor) => {
                    format!("/v1/observations?dataflow=abs.cpi&limit={limit}&cursor={cursor}")
                }
                None => format!("/v1/observations?dataflow=abs.cpi&limit={limit}"),
            };
            let page = get_observations_json(app.clone(), &uri).await;
            cursor = page.pagination.next_cursor;
            paged.extend(page.observations);
            if cursor.is_none() {
                break;
            }
        }

        assert_eq!(paged, full, "page size {limit} did not concatenate cleanly");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observations_endpoint_uses_monthly_rollup_when_frequency_matches_grain() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let db = TestDb::start("au_kpis_api_observations_rollup").await;
    disable_monthly_rollup_policy(db.pool()).await;
    seed_observations(db.pool()).await;
    seed_daily_rollup_observations(db.pool()).await;
    refresh_monthly_rollup(db.pool()).await;
    let app = router(test_state(db.pool().clone())).expect("router");

    let response = get_observations_json(
        app.clone(),
        "/v1/observations?dataflow=abs.daily&frequency=monthly&limit=10",
    )
    .await;

    assert_eq!(response.metadata.dataflow.as_str(), "abs.daily");
    assert_eq!(response.observations.len(), 2);
    assert_eq!(
        response.observations[0].time,
        Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()
    );
    assert_eq!(
        response.observations[0].time_precision,
        au_kpis_domain::TimePrecision::Month
    );
    assert_eq!(response.observations[0].value, Some(15.0));
    assert_eq!(
        response.observations[0]
            .attributes
            .get("aggregate")
            .map(String::as_str),
        Some("avg")
    );
    assert_eq!(
        response.observations[0]
            .attributes
            .get("observations_count")
            .map(String::as_str),
        Some("2")
    );
    assert_eq!(
        response.observations[1].time,
        Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap()
    );
    assert_eq!(response.observations[1].value, Some(40.0));

    for _ in 0..3 {
        let response = get_observations_json(
            app.clone(),
            "/v1/observations?dataflow=abs.daily&frequency=monthly&limit=10",
        )
        .await;
        assert_eq!(response.observations.len(), 2);
    }

    let mut samples = Vec::with_capacity(30);
    for _ in 0..30 {
        let started = Instant::now();
        let response = get_observations_json(
            app.clone(),
            "/v1/observations?dataflow=abs.daily&frequency=monthly&limit=10",
        )
        .await;
        assert_eq!(response.observations.len(), 2);
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    let p95_index = (samples.len() * 95).div_ceil(100).saturating_sub(1);
    let p95 = samples[p95_index];
    assert!(
        p95 < Duration::from_millis(50),
        "monthly rollup query p95 should stay under 50 ms; p95={p95:?}, samples={samples:?}"
    );
}

fn request(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

async fn get_observations_json(app: axum::Router, uri: &str) -> ObservationsResponse {
    let response = app.oneshot(request(uri)).await.expect("json response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("json body");
    serde_json::from_slice(&body).expect("observations response")
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

async fn seed_observations(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage, description)
         VALUES ('abs', 'Australian Bureau of Statistics', 'https://www.abs.gov.au', NULL)",
    )
    .execute(pool)
    .await
    .expect("insert source");

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
         VALUES (
             'abs.cpi', 'abs', 'Consumer Price Index', NULL,
             ARRAY['region'], ARRAY['index'], 'quarterly', 'CC-BY-4.0',
             'Source: Australian Bureau of Statistics',
             'https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia'
         )",
    )
    .execute(pool)
    .await
    .expect("insert dataflow");

    let artifact = ArtifactId::of_content(b"api observations fixture");
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
    .bind(Utc.with_ymd_and_hms(2024, 4, 24, 0, 0, 0).unwrap())
    .execute(pool)
    .await
    .expect("insert artifact");

    let aus_key = insert_series(pool, "AUS").await;
    let nsw_key = insert_series(pool, "NSW").await;
    insert_observation(
        pool,
        ObservationSeed::new(aus_key, artifact, (2024, 3, 1), 0, 134.2),
    )
    .await;
    insert_observation(
        pool,
        ObservationSeed::new(aus_key, artifact, (2024, 3, 1), 1, 135.0),
    )
    .await;
    insert_observation(
        pool,
        ObservationSeed::new(aus_key, artifact, (2024, 6, 1), 0, 136.2),
    )
    .await;
    insert_observation(
        pool,
        ObservationSeed::new(nsw_key, artifact, (2024, 3, 1), 0, 137.1),
    )
    .await;
}

async fn seed_extra_pagination_observations(pool: &PgPool, artifact: ArtifactId) {
    for (region, base) in [("VIC", 140.0), ("QLD", 150.0)] {
        let key = insert_series(pool, region).await;
        for (offset, date) in [
            (0.0, (2024, 3, 1)),
            (1.0, (2024, 6, 1)),
            (2.0, (2024, 9, 1)),
        ] {
            insert_observation(
                pool,
                ObservationSeed::new(key, artifact, date, 0, base + offset),
            )
            .await;
        }
    }

    let aus = SeriesKey::derive(
        &DataflowId::new("abs.cpi").unwrap(),
        &MeasureId::new("index").unwrap(),
        [("region", "AUS")],
    );
    for (offset, date) in [(0.0, (2024, 9, 1)), (1.0, (2024, 12, 1))] {
        insert_observation(
            pool,
            ObservationSeed::new(aus, artifact, date, 0, 138.0 + offset),
        )
        .await;
    }
}

async fn seed_daily_rollup_observations(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO dataflows (
             id, source_id, name, description, dimensions, measures,
             frequency, license, attribution, source_url
         )
         VALUES (
             'abs.daily', 'abs', 'Daily indicator', NULL,
             ARRAY['region'], ARRAY['index'], 'daily', 'CC-BY-4.0',
             'Source: Australian Bureau of Statistics',
             'https://www.abs.gov.au/'
         )",
    )
    .execute(pool)
    .await
    .expect("insert daily dataflow");

    let artifact = ArtifactId::of_content(b"api observations daily rollup fixture");
    sqlx::query(
        "INSERT INTO artifacts (
             id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at
         )
         VALUES ($1, 'abs', 'https://example.test/daily.json', 'application/json',
                 '{}'::jsonb, 128, $2, $3)",
    )
    .bind(artifact.digest().as_bytes().as_slice())
    .bind(format!("artifacts/{artifact}"))
    .bind(Utc.with_ymd_and_hms(2024, 2, 24, 0, 0, 0).unwrap())
    .execute(pool)
    .await
    .expect("insert daily artifact");

    let dataflow = DataflowId::new("abs.daily").unwrap();
    let measure = MeasureId::new("index").unwrap();
    let dimensions: BTreeMap<String, String> = [("region".to_string(), "AUS".to_string())]
        .into_iter()
        .collect();
    let key = SeriesKey::derive(
        &dataflow,
        &measure,
        dimensions
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );

    sqlx::query(
        "INSERT INTO series (
             series_key, dataflow_id, measure_id, dimensions, unit,
             first_observed, last_observed, active
         )
         VALUES ($1, 'abs.daily', 'index', $2, 'index', $3, $4, true)",
    )
    .bind(key.digest().as_bytes().as_slice())
    .bind(json!(dimensions))
    .bind(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap())
    .bind(Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap())
    .execute(pool)
    .await
    .expect("insert daily series");

    for (date, value) in [
        ((2024, 1, 1), 10.0),
        ((2024, 1, 15), 20.0),
        ((2024, 2, 1), 40.0),
    ] {
        insert_observation(
            pool,
            ObservationSeed::new(key, artifact, date, 0, value).with_time_precision("day"),
        )
        .await;
    }
}

async fn refresh_monthly_rollup(pool: &PgPool) {
    sqlx::query(
        "CALL refresh_continuous_aggregate(
            'observations_rollup_monthly',
            '2024-01-01 00:00:00+00'::timestamptz,
            '2024-03-01 00:00:00+00'::timestamptz
         )",
    )
    .execute(pool)
    .await
    .expect("refresh monthly continuous aggregate");
}

async fn disable_monthly_rollup_policy(pool: &PgPool) {
    sqlx::query("SELECT remove_continuous_aggregate_policy('observations_rollup_monthly', if_exists => TRUE)")
        .execute(pool)
        .await
        .expect("remove monthly rollup refresh policy");
}

async fn insert_series(pool: &PgPool, region: &str) -> SeriesKey {
    let dataflow = DataflowId::new("abs.cpi").unwrap();
    let measure = MeasureId::new("index").unwrap();
    let dimensions: BTreeMap<String, String> = [("region".to_string(), region.to_string())]
        .into_iter()
        .collect();
    let key = SeriesKey::derive(
        &dataflow,
        &measure,
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
    .bind(key.digest().as_bytes().as_slice())
    .bind(json!(dimensions))
    .bind(Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap())
    .bind(Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap())
    .execute(pool)
    .await
    .expect("insert series");
    key
}

#[derive(Debug, Clone, Copy)]
struct ObservationSeed {
    series_key: SeriesKey,
    artifact: ArtifactId,
    date: (i32, u32, u32),
    revision_no: i32,
    value: f64,
    time_precision: &'static str,
}

impl ObservationSeed {
    fn new(
        series_key: SeriesKey,
        artifact: ArtifactId,
        date: (i32, u32, u32),
        revision_no: i32,
        value: f64,
    ) -> Self {
        Self {
            series_key,
            artifact,
            date,
            revision_no,
            value,
            time_precision: "quarter",
        }
    }

    fn with_time_precision(mut self, time_precision: &'static str) -> Self {
        self.time_precision = time_precision;
        self
    }
}

async fn insert_observation(pool: &PgPool, seed: ObservationSeed) {
    let observed_at = Utc
        .with_ymd_and_hms(seed.date.0, seed.date.1, seed.date.2, 0, 0, 0)
        .unwrap();
    sqlx::query(
        "INSERT INTO observations (
             series_key, time, revision_no, time_precision, value, status,
             attributes, ingested_at, source_artifact_id
         )
         VALUES ($1, $2, $3, $4, $5, 'normal',
                 '{}'::jsonb, $6, $7)",
    )
    .bind(seed.series_key.digest().as_bytes().as_slice())
    .bind(observed_at)
    .bind(seed.revision_no)
    .bind(seed.time_precision)
    .bind(seed.value)
    .bind(Utc.with_ymd_and_hms(2024, 4, 24, 0, 0, 0).unwrap())
    .bind(seed.artifact.digest().as_bytes().as_slice())
    .execute(pool)
    .await
    .expect("insert observation");
    sqlx::query(
        "UPDATE series
         SET first_observed = LEAST(COALESCE(first_observed, $2), $2),
             last_observed = GREATEST(COALESCE(last_observed, $2), $2)
         WHERE series_key = $1",
    )
    .bind(seed.series_key.digest().as_bytes().as_slice())
    .bind(observed_at)
    .execute(pool)
    .await
    .expect("update series observation bounds");
}

fn docker_available() -> bool {
    if let Some(host) = std::env::var_os("DOCKER_HOST") {
        let host = host.to_string_lossy();
        if let Some(path) = host.strip_prefix("unix://") {
            return std::os::unix::net::UnixStream::connect(path).is_ok();
        }
        return true;
    }

    std::os::unix::net::UnixStream::connect("/var/run/docker.sock").is_ok()
}
