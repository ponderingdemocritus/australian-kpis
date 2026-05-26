use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use au_kpis_api_http::{AppState, router};
use au_kpis_cache::{CacheBackend, CacheClient, CacheError, RateLimitDecision, TokenBucketConfig};
use au_kpis_config::{AppConfig, DatabaseConfig, HttpConfig, LogFormat, TelemetryConfig};
use au_kpis_domain::ids::{ArtifactId, DataflowId, SeriesKey};
use au_kpis_telemetry::Telemetry;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use chrono::{DateTime, TimeZone, Utc};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use serde_json::json;
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const API_URI: &str = "/v1/observations?dataflow=abs.cpi&limit=25";
const ROW_COUNT: usize = 1_000;

#[derive(Debug, Default)]
struct NoopCacheBackend;

#[async_trait]
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

struct BenchDb {
    pool: PgPool,
    _timescale: au_kpis_testing::timescale::TimescaleHarness,
}

async fn bench_db() -> BenchDb {
    let timescale = au_kpis_testing::timescale::start_timescale("au_kpis_api_bench")
        .await
        .expect("start timescaledb benchmark container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };

    let mut last_err = None;
    for _ in 0..10 {
        match au_kpis_db::connect(&cfg).await {
            Ok(pool) => {
                au_kpis_db::migrate(&pool).await.expect("apply migrations");
                seed_observations(&pool).await;
                return BenchDb {
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
    panic!("timescaledb did not accept benchmark connections: {last_err:?}");
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

    let artifact = ArtifactId::of_content(b"api handler benchmark artifact");
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
    .bind(ts(2024, 4, 24))
    .execute(pool)
    .await
    .expect("insert artifact");

    for region in ["AUS", "NSW", "VIC", "QLD", "WA"] {
        let series_key = insert_series(pool, region).await;
        for idx in 0..(ROW_COUNT / 5) {
            insert_observation(pool, series_key, artifact, idx).await;
        }
    }
}

async fn insert_series(pool: &PgPool, region: &str) -> SeriesKey {
    let dataflow = DataflowId::new("abs.cpi").expect("static dataflow id is valid");
    let dimensions: BTreeMap<String, String> = [("region".to_string(), region.to_string())]
        .into_iter()
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
         VALUES ($1, 'abs.cpi', 'index', $2, 'index', $3, $4, true)",
    )
    .bind(key.digest().as_bytes().as_slice())
    .bind(json!(dimensions))
    .bind(ts(2020, 1, 1))
    .bind(ts(2020, 1, 1) + chrono::Duration::days((ROW_COUNT / 5) as i64))
    .execute(pool)
    .await
    .expect("insert series");
    key
}

async fn insert_observation(
    pool: &PgPool,
    series_key: SeriesKey,
    artifact: ArtifactId,
    idx: usize,
) {
    sqlx::query(
        "INSERT INTO observations (
             series_key, time, revision_no, time_precision, value, status,
             attributes, ingested_at, source_artifact_id
         )
         VALUES ($1, $2, 0, 'day', $3, 'normal',
                 '{}'::jsonb, $4, $5)",
    )
    .bind(series_key.digest().as_bytes().as_slice())
    .bind(ts(2020, 1, 1) + chrono::Duration::days(idx as i64))
    .bind(100.0 + f64::from(idx as u32) / 10.0)
    .bind(ts(2024, 4, 24))
    .bind(artifact.digest().as_bytes().as_slice())
    .execute(pool)
    .await
    .expect("insert observation");
}

fn ts(year: i32, month: u32, day: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, 0, 0, 0)
        .single()
        .expect("valid timestamp")
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
                service_name: "au-kpis-bench".into(),
                log_format: LogFormat::Json,
                log_level: "info".into(),
                otlp_endpoint: None,
            },
        }),
        Arc::new(Telemetry::disabled()),
        CancellationToken::new(),
    )
}

fn request(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

async fn direct_db_page(pool: &PgPool) -> usize {
    let metadata = sqlx::query(
        "SELECT license, attribution, source_url
         FROM dataflows
         WHERE id = 'abs.cpi'",
    )
    .fetch_one(pool)
    .await
    .expect("fetch metadata");
    black_box(metadata.get::<String, _>("license"));
    black_box(metadata.get::<String, _>("attribution"));
    black_box(metadata.get::<String, _>("source_url"));

    let fingerprint = sqlx::query(
        "SELECT count(*)::bigint AS row_count,
                max(o.ingested_at) AS max_ingested_at,
                max(o.time) AS max_time,
                max(o.revision_no) AS max_revision_no
         FROM observations_latest o
         JOIN series s ON s.series_key = o.series_key
         JOIN dataflows d ON d.id = s.dataflow_id
         WHERE s.dataflow_id = 'abs.cpi'",
    )
    .fetch_one(pool)
    .await
    .expect("fetch etag fingerprint");
    black_box(fingerprint.get::<i64, _>("row_count"));

    let rows = sqlx::query(
        "SELECT o.series_key,
                o.time,
                o.revision_no,
                o.time_precision,
                o.value,
                o.status,
                o.attributes,
                o.ingested_at,
                o.source_artifact_id,
                s.dimensions,
                s.measure_id,
                s.unit
         FROM observations_latest o
         JOIN series s ON s.series_key = o.series_key
         JOIN dataflows d ON d.id = s.dataflow_id
         WHERE s.dataflow_id = 'abs.cpi'
         ORDER BY o.time ASC, o.series_key ASC
         LIMIT 26",
    )
    .fetch_all(pool)
    .await
    .expect("fetch rows");
    black_box(rows.len())
}

async fn handler_page(app: Router) -> usize {
    let response = app
        .oneshot(request(API_URI))
        .await
        .expect("handler response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("handler body");
    black_box(body.len())
}

async fn estimate_handler_overhead(pool: &PgPool, app: Router) -> Duration {
    let mut db_elapsed = Duration::ZERO;
    let mut handler_elapsed = Duration::ZERO;
    let samples = 20;

    for _ in 0..samples {
        let started = Instant::now();
        direct_db_page(pool).await;
        db_elapsed += started.elapsed();

        let started = Instant::now();
        handler_page(app.clone()).await;
        handler_elapsed += started.elapsed();
    }

    (handler_elapsed / samples).saturating_sub(db_elapsed / samples)
}

fn bench_observations_handler(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let db = runtime.block_on(bench_db());
    let app = router(test_state(db.pool.clone())).expect("router");
    let overhead = runtime.block_on(estimate_handler_overhead(&db.pool, app.clone()));
    eprintln!("api handler overhead estimate above direct DB: {overhead:?}");

    let mut group = c.benchmark_group("api_observations");
    group.bench_function("direct_db_observations_page", |b| {
        b.iter(|| runtime.block_on(direct_db_page(&db.pool)));
    });
    group.bench_function("handler_observations_page_under_5ms_above_db", |b| {
        b.iter(|| runtime.block_on(handler_page(app.clone())));
    });
    group.finish();

    drop(app);
    runtime.block_on(async move {
        drop(db);
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(20);
    targets = bench_observations_handler
}
criterion_main!(benches);
