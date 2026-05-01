use std::{collections::BTreeMap, sync::LazyLock, time::Duration};

use au_kpis_config::DatabaseConfig;
use au_kpis_db::{connect, migrate};
use au_kpis_domain::{
    Observation, ObservationStatus, SeriesDescriptor, TimePrecision,
    ids::{ArtifactId, CodeId, DataflowId, DimensionId, MeasureId, SeriesKey},
};
use au_kpis_loader::{LoadItem, LoadOptions, load_batch, load_batch_with_options};
use au_kpis_testing::timescale::start_timescale;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use sqlx::{PgPool, Row};

static TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

#[derive(Debug)]
struct TestDb {
    pool: PgPool,
    _timescale: au_kpis_testing::timescale::TimescaleHarness,
}

async fn test_db() -> TestDb {
    let timescale = start_timescale("au_kpis_loader_test")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };

    let mut last_err = None;
    for _ in 0..10 {
        match connect(&cfg).await {
            Ok(pool) => {
                migrate(&pool).await.expect("apply migrations");
                return TestDb {
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

async fn seed_reference_data(pool: &PgPool, artifact_id: ArtifactId) {
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

    sqlx::query(
        "INSERT INTO artifacts (
             id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at
         )
         VALUES ($1, 'abs', 'https://example.test/cpi.json', 'application/json',
                 '{}'::jsonb, 128, $2, $3)",
    )
    .bind(artifact_id.digest().as_bytes().as_slice())
    .bind(format!("artifacts/{artifact_id}"))
    .bind(ts(2024, 4, 24))
    .execute(pool)
    .await
    .expect("insert artifact");
}

fn descriptor(region: &str) -> SeriesDescriptor {
    let dataflow_id = DataflowId::new("abs.cpi").unwrap();
    let dimensions: BTreeMap<DimensionId, CodeId> = [(
        DimensionId::new("region").unwrap(),
        CodeId::new(region).unwrap(),
    )]
    .into_iter()
    .collect();
    let series_key = SeriesKey::derive(
        &dataflow_id,
        dimensions
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );

    SeriesDescriptor {
        series_key,
        dataflow_id,
        measure_id: MeasureId::new("index").unwrap(),
        dimensions,
        unit: "index".to_string(),
    }
}

fn observation(
    descriptor: &SeriesDescriptor,
    artifact_id: ArtifactId,
    time: DateTime<Utc>,
    revision_no: u32,
    value: f64,
) -> Observation {
    Observation {
        series_key: descriptor.series_key,
        time,
        time_precision: TimePrecision::Quarter,
        value: Some(value),
        status: ObservationStatus::Normal,
        revision_no,
        attributes: BTreeMap::new(),
        ingested_at: ts(2024, 4, 24),
        source_artifact_id: artifact_id,
    }
}

fn item(
    descriptor: &SeriesDescriptor,
    artifact_id: ArtifactId,
    time: DateTime<Utc>,
    revision_no: u32,
    value: f64,
) -> LoadItem {
    LoadItem {
        series: descriptor.clone(),
        observation: observation(descriptor, artifact_id, time, revision_no, value),
    }
}

fn ts(year: i32, month: u32, day: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, 0, 0, 0)
        .single()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upserts_series_and_latest_revision() {
    let _guard = TEST_LOCK.lock().await;
    let db = test_db().await;
    let pool = &db.pool;
    let artifact_id = ArtifactId::of_content(b"loader revision fixture");
    seed_reference_data(pool, artifact_id).await;
    let aus = descriptor("AUS");
    let time = ts(2024, 3, 1);

    let stats = load_batch(
        pool,
        vec![
            item(&aus, artifact_id, time, 0, 134.2),
            item(&aus, artifact_id, time, 1, 135.0),
        ],
    )
    .await
    .expect("load observations");

    assert_eq!(stats.observations_loaded, 2);
    assert_eq!(stats.series_upserted, 1);
    assert_eq!(stats.parse_errors, 0);

    let row = sqlx::query(
        "SELECT s.dataflow_id, s.first_observed, s.last_observed,
                o.revision_no, o.value
         FROM observations_latest o
         JOIN series s USING (series_key)
         WHERE s.series_key = $1",
    )
    .bind(aus.series_key.digest().as_bytes().as_slice())
    .fetch_one(pool)
    .await
    .expect("fetch latest revision");

    assert_eq!(row.get::<String, _>("dataflow_id"), "abs.cpi");
    assert_eq!(row.get::<DateTime<Utc>, _>("first_observed"), time);
    assert_eq!(row.get::<DateTime<Utc>, _>("last_observed"), time);
    assert_eq!(row.get::<i32, _>("revision_no"), 1);
    assert_eq!(row.get::<Option<f64>, _>("value"), Some(135.0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validation_errors_are_recorded_without_failing_valid_rows() {
    let _guard = TEST_LOCK.lock().await;
    let db = test_db().await;
    let pool = &db.pool;
    let artifact_id = ArtifactId::of_content(b"loader partial failure fixture");
    seed_reference_data(pool, artifact_id).await;
    let aus = descriptor("AUS");
    let mut bad = descriptor("NSW");
    bad.series_key = aus.series_key;

    let stats = load_batch(
        pool,
        vec![
            item(&aus, artifact_id, ts(2024, 3, 1), 0, 134.2),
            item(&bad, artifact_id, ts(2024, 6, 1), 0, 136.1),
        ],
    )
    .await
    .expect("load valid rows and record parse errors");

    assert_eq!(stats.observations_loaded, 1);
    assert_eq!(stats.parse_errors, 1);

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(pool)
        .await
        .expect("count observations");
    let parse_error_count: i64 = sqlx::query_scalar("SELECT count(*) FROM parse_errors")
        .fetch_one(pool)
        .await
        .expect("count parse errors");

    assert_eq!(observation_count, 1);
    assert_eq!(parse_error_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loads_ten_thousand_observations_with_copy_batches() {
    let _guard = TEST_LOCK.lock().await;
    let db = test_db().await;
    let pool = &db.pool;
    let artifact_id = ArtifactId::of_content(b"loader performance fixture");
    seed_reference_data(pool, artifact_id).await;

    let descriptor = descriptor("AUS");
    load_batch(
        pool,
        vec![item(&descriptor, artifact_id, ts(2024, 3, 1), 0, 100.0)],
    )
    .await
    .expect("warm loader path");

    let mut best_elapsed = Duration::MAX;
    for attempt in 0_i64..3 {
        let mut rows = Vec::with_capacity(10_000);
        for index in 0_i64..10_000 {
            rows.push(item(
                &descriptor,
                artifact_id,
                ts(2024, 3, 1) + ChronoDuration::seconds(1 + attempt * 20_000 + index),
                0,
                index as f64,
            ));
        }

        let started = std::time::Instant::now();
        let stats = load_batch_with_options(
            pool,
            rows,
            LoadOptions {
                max_rows: 10_000,
                max_bytes: 10 * 1024 * 1024,
            },
        )
        .await
        .expect("load 10k observations");
        let elapsed = started.elapsed();

        assert_eq!(stats.observations_loaded, 10_000);
        assert_eq!(stats.parse_errors, 0);
        best_elapsed = best_elapsed.min(elapsed);
        if best_elapsed < Duration::from_millis(500) {
            break;
        }
    }

    assert!(
        best_elapsed < Duration::from_millis(500),
        "10k COPY load should finish under 500ms, best attempt took {best_elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_options_split_observations_at_one_thousand_rows() {
    let _guard = TEST_LOCK.lock().await;
    let db = test_db().await;
    let pool = &db.pool;
    let artifact_id = ArtifactId::of_content(b"loader batch split fixture");
    seed_reference_data(pool, artifact_id).await;

    let descriptor = descriptor("AUS");
    let rows: Vec<_> = (0_i64..1_001)
        .map(|index| {
            item(
                &descriptor,
                artifact_id,
                ts(2024, 3, 1) + ChronoDuration::seconds(index),
                0,
                index as f64,
            )
        })
        .collect();

    let stats = load_batch(pool, rows)
        .await
        .expect("load 1001 observations");

    assert_eq!(stats.observations_loaded, 1_001);
    assert_eq!(stats.batches, 2);
}
