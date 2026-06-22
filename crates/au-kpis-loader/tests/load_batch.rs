use std::{collections::BTreeMap, sync::LazyLock, time::Duration};

use au_kpis_config::DatabaseConfig;
use au_kpis_db::{connect, migrate};
use au_kpis_domain::{
    Observation, ObservationStatus, SeriesDescriptor, TimePrecision,
    ids::{ArtifactId, CodeId, DataflowId, DimensionId, MeasureId, SeriesKey},
};
use au_kpis_loader::{
    LoadItem, LoadItemAudit, LoadOptions, begin_staged_load, load_batch,
    load_batch_boundary_reached, load_batch_with_options, should_flush_load_batch,
};
use au_kpis_testing::timescale::start_timescale;
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};

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

async fn seed_artifact(pool: &PgPool, artifact_id: ArtifactId, source_url: &str) {
    sqlx::query(
        "INSERT INTO artifacts (
             id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at
         )
         VALUES ($1, 'abs', $2, 'application/json',
                 '{}'::jsonb, 128, $3, $4)",
    )
    .bind(artifact_id.digest().as_bytes().as_slice())
    .bind(source_url)
    .bind(format!("artifacts/{artifact_id}"))
    .bind(ts(2024, 4, 25))
    .execute(pool)
    .await
    .expect("insert additional artifact");
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

fn permutations<T: Clone>(values: &[T]) -> Vec<Vec<T>> {
    if values.is_empty() {
        return vec![Vec::new()];
    }

    let mut result = Vec::new();
    for index in 0..values.len() {
        let mut remaining = values.to_vec();
        let value = remaining.remove(index);
        for mut suffix in permutations(&remaining) {
            let mut order = Vec::with_capacity(values.len());
            order.push(value.clone());
            order.append(&mut suffix);
            result.push(order);
        }
    }
    result
}

#[test]
fn load_batch_boundary_helpers_cover_row_and_byte_edges() {
    let options = LoadOptions {
        max_rows: 3,
        max_bytes: 100,
    };

    assert!(!should_flush_load_batch(0, 99, 10, options));
    assert!(should_flush_load_batch(3, 10, 1, options));
    assert!(should_flush_load_batch(2, 95, 6, options));
    assert!(!should_flush_load_batch(2, 90, 10, options));

    assert!(load_batch_boundary_reached(3, 0, options));
    assert!(load_batch_boundary_reached(1, 100, options));
    assert!(!load_batch_boundary_reached(2, 99, options));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upserts_series_and_initial_revision() {
    let _guard = TEST_LOCK.lock().await;
    let db = test_db().await;
    let pool = &db.pool;
    let artifact_id = ArtifactId::of_content(b"loader revision fixture");
    seed_reference_data(pool, artifact_id).await;
    let aus = descriptor("AUS");
    let time = ts(2024, 3, 1);

    let stats = load_batch(pool, vec![item(&aus, artifact_id, time, 0, 134.2)])
        .await
        .expect("load observations");

    assert_eq!(stats.observations_loaded, 1);
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
    assert_eq!(row.get::<i32, _>("revision_no"), 0);
    assert_eq!(row.get::<Option<f64>, _>("value"), Some(134.2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_reingest_with_adapter_revision_zero_appends_revision() {
    let _guard = TEST_LOCK.lock().await;
    let db = test_db().await;
    let pool = &db.pool;
    let initial_artifact = ArtifactId::of_content(b"loader initial revision assignment fixture");
    let revised_artifact = ArtifactId::of_content(b"loader revised revision assignment fixture");
    seed_reference_data(pool, initial_artifact).await;
    seed_artifact(
        pool,
        revised_artifact,
        "https://example.test/cpi-revised.json",
    )
    .await;
    let aus = descriptor("AUS");
    let time = ts(2024, 3, 1);

    let initial = load_batch(pool, vec![item(&aus, initial_artifact, time, 0, 134.2)])
        .await
        .expect("load initial observation");
    assert_eq!(initial.observations_loaded, 1);

    let revised = load_batch(pool, vec![item(&aus, revised_artifact, time, 0, 135.0)])
        .await
        .expect("load revised observation");
    assert_eq!(revised.observations_loaded, 1);

    let rows = sqlx::query(
        "SELECT revision_no, value, encode(source_artifact_id, 'hex') AS artifact_id_hex
         FROM observations
         WHERE series_key = $1 AND time = $2
         ORDER BY revision_no",
    )
    .bind(aus.series_key.digest().as_bytes().as_slice())
    .bind(time)
    .fetch_all(pool)
    .await
    .expect("fetch revision chain");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i32, _>("revision_no"), 0);
    assert_eq!(rows[0].get::<Option<f64>, _>("value"), Some(134.2));
    assert_eq!(
        rows[0].get::<String, _>("artifact_id_hex"),
        initial_artifact.to_string()
    );
    assert_eq!(rows[1].get::<i32, _>("revision_no"), 1);
    assert_eq!(rows[1].get::<Option<f64>, _>("value"), Some(135.0));
    assert_eq!(
        rows[1].get::<String, _>("artifact_id_hex"),
        revised_artifact.to_string()
    );

    let latest = sqlx::query(
        "SELECT revision_no, value
         FROM observations_latest
         WHERE series_key = $1 AND time = $2",
    )
    .bind(aus.series_key.digest().as_bytes().as_slice())
    .bind(time)
    .fetch_one(pool)
    .await
    .expect("fetch latest revision");
    assert_eq!(latest.get::<i32, _>("revision_no"), 1);
    assert_eq!(latest.get::<Option<f64>, _>("value"), Some(135.0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_observations_in_same_batch_collapse_to_last_row() {
    let _guard = TEST_LOCK.lock().await;
    let db = test_db().await;
    let pool = &db.pool;
    let artifact_id = ArtifactId::of_content(b"loader duplicate observation fixture");
    seed_reference_data(pool, artifact_id).await;
    let aus = descriptor("AUS");
    let time = ts(2024, 3, 1);

    let stats = load_batch(
        pool,
        vec![
            item(&aus, artifact_id, time, 0, 134.2),
            item(&aus, artifact_id, time, 0, 135.0),
        ],
    )
    .await
    .expect("load duplicate observations");

    assert_eq!(stats.observations_loaded, 1);
    assert_eq!(stats.series_upserted, 1);
    assert_eq!(stats.parse_errors, 0);

    let row = sqlx::query(
        "SELECT count(*) AS row_count, max(value) AS value
         FROM observations
         WHERE series_key = $1
           AND time = $2
           AND revision_no = 0",
    )
    .bind(aus.series_key.digest().as_bytes().as_slice())
    .bind(time)
    .fetch_one(pool)
    .await
    .expect("fetch deduped observation");

    assert_eq!(row.get::<i64, _>("row_count"), 1);
    assert_eq!(row.get::<Option<f64>, _>("value"), Some(135.0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_observation_reload_does_not_rewrite_existing_row() {
    let _guard = TEST_LOCK.lock().await;
    let db = test_db().await;
    let pool = &db.pool;
    let artifact_id = ArtifactId::of_content(b"loader idempotent observation fixture");
    seed_reference_data(pool, artifact_id).await;
    seed_observation_update_counter(pool).await;
    let aus = descriptor("AUS");
    let row = item(&aus, artifact_id, ts(2024, 3, 1), 0, 134.2);

    let first = load_batch(pool, vec![row.clone()])
        .await
        .expect("initial observation load");
    assert_eq!(first.observations_loaded, 1);

    let mut reload = row;
    reload.observation.ingested_at = ts(2024, 4, 25);
    let second = load_batch(pool, vec![reload])
        .await
        .expect("idempotent observation reload");

    assert_eq!(second.observations_loaded, 0);
    let updates: i32 = sqlx::query_scalar("SELECT count FROM observation_update_counter")
        .fetch_one(pool)
        .await
        .expect("read update counter");
    assert_eq!(updates, 0, "identical reload should not update the row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn latest_revision_wins_for_every_insertion_order() {
    let _guard = TEST_LOCK.lock().await;
    let db = test_db().await;
    let pool = &db.pool;
    let artifact_a = ArtifactId::of_content(b"loader revision permutation fixture a");
    let artifact_b = ArtifactId::of_content(b"loader revision permutation fixture b");
    let artifact_c = ArtifactId::of_content(b"loader revision permutation fixture c");
    let artifact_d = ArtifactId::of_content(b"loader revision permutation fixture d");
    seed_reference_data(pool, artifact_a).await;
    seed_artifact(pool, artifact_b, "https://example.test/cpi-b.json").await;
    seed_artifact(pool, artifact_c, "https://example.test/cpi-c.json").await;
    seed_artifact(pool, artifact_d, "https://example.test/cpi-d.json").await;
    let aus = descriptor("AUS");
    let revisions = [
        (artifact_a, 134.2),
        (artifact_b, 135.0),
        (artifact_c, 133.9),
        (artifact_d, 136.4),
    ];
    let orders = permutations(&revisions);

    for (case, order) in orders.iter().enumerate() {
        let time = ts(2024, 3, 1) + ChronoDuration::days(case as i64);
        for (artifact_id, value) in order {
            let stats = load_batch(pool, vec![item(&aus, *artifact_id, time, 0, *value)])
                .await
                .expect("load revision permutation");

            assert_eq!(stats.observations_loaded, 1);
            assert_eq!(stats.parse_errors, 0);
        }
    }

    let rows = sqlx::query(
        "SELECT time, revision_no, value
         FROM observations_latest
         WHERE series_key = $1
         ORDER BY time",
    )
    .bind(aus.series_key.digest().as_bytes().as_slice())
    .fetch_all(pool)
    .await
    .expect("fetch latest revisions");

    assert_eq!(rows.len(), orders.len());
    for (row, order) in rows.iter().zip(orders) {
        let expected_latest = order.last().expect("order has revisions").1;
        assert_eq!(row.get::<i32, _>("revision_no"), 3);
        assert_eq!(row.get::<Option<f64>, _>("value"), Some(expected_latest));
    }
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
async fn load_batch_enqueues_webhook_deliveries_for_matching_subscriptions() {
    let _guard = TEST_LOCK.lock().await;
    let db = test_db().await;
    let pool = &db.pool;
    let artifact_id = ArtifactId::of_content(b"loader webhook delivery fixture");
    seed_reference_data(pool, artifact_id).await;
    seed_webhook_subscriptions(pool).await;
    let aus = descriptor("AUS");
    let nsw = descriptor("NSW");

    let stats = load_batch(
        pool,
        vec![
            item(&aus, artifact_id, ts(2024, 3, 1), 0, 134.2),
            item(&nsw, artifact_id, ts(2024, 6, 1), 0, 136.1),
        ],
    )
    .await
    .expect("load observations and enqueue webhook delivery");

    assert_eq!(stats.observations_loaded, 2);

    let rows = sqlx::query(
        "SELECT dataflow_id, encode(artifact_id, 'hex') AS artifact_id_hex,
                payload, status, attempts, max_attempts
         FROM webhook_deliveries
         ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("fetch webhook deliveries");

    assert_eq!(rows.len(), 2, "matching dataflow and wildcard subscribers");
    for row in rows {
        assert_eq!(row.get::<String, _>("dataflow_id"), "abs.cpi");
        assert_eq!(
            row.get::<String, _>("artifact_id_hex"),
            artifact_id.to_string()
        );
        assert_eq!(row.get::<String, _>("status"), "pending");
        assert_eq!(row.get::<i32, _>("attempts"), 0);
        assert_eq!(row.get::<i32, _>("max_attempts"), 5);
        let payload = row.get::<serde_json::Value, _>("payload");
        assert_eq!(payload["event"], "data.updated");
        assert_eq!(payload["dataflow_id"], "abs.cpi");
        assert_eq!(payload["artifact_id"], artifact_id.to_string());
        assert_eq!(payload["observations_loaded"], 2);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_load_cleanup_returns_live_connection_to_pool() {
    let _guard = TEST_LOCK.lock().await;
    let timescale = start_timescale("au_kpis_loader_staged_connection_reuse")
        .await
        .expect("start timescaledb container");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(timescale.url())
        .await
        .expect("connect to timescaledb");
    migrate(&pool).await.expect("apply migrations");

    let artifact_id = ArtifactId::of_content(b"loader staged connection reuse fixture");
    seed_reference_data(&pool, artifact_id).await;
    let initial_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .expect("read initial backend pid");

    let aus = descriptor("AUS");
    let mut staged = begin_staged_load(
        &pool,
        LoadOptions {
            max_rows: 1,
            max_bytes: 1024 * 1024,
        },
    )
    .await
    .expect("begin staged load");
    staged
        .stage(vec![LoadItemAudit {
            item: item(&aus, artifact_id, ts(2024, 3, 1), 0, 134.2),
            row_context: None,
        }])
        .await
        .expect("stage row");
    let stats = staged.commit().await.expect("commit staged load");

    assert_eq!(stats.observations_loaded, 1);
    let after_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .expect("read backend pid after staged cleanup");
    assert_eq!(after_pid, initial_pid);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_load_commit_promotes_observations_in_configured_chunks() {
    let _guard = TEST_LOCK.lock().await;
    let db = test_db().await;
    let pool = &db.pool;
    let artifact_id = ArtifactId::of_content(b"loader staged chunked commit fixture");
    seed_reference_data(pool, artifact_id).await;
    seed_webhook_subscriptions(pool).await;
    seed_observation_statement_row_limit(pool, 2).await;

    let aus = descriptor("AUS");
    let rows: Vec<_> = (0_i64..5)
        .map(|index| LoadItemAudit {
            item: item(
                &aus,
                artifact_id,
                ts(2024, 3, 1) + ChronoDuration::seconds(index),
                0,
                index as f64,
            ),
            row_context: None,
        })
        .collect();

    let mut staged = begin_staged_load(
        pool,
        LoadOptions {
            max_rows: 2,
            max_bytes: 1024 * 1024,
        },
    )
    .await
    .expect("begin staged load");
    staged.stage(rows).await.expect("stage rows");

    let stats = staged.commit().await.expect("commit staged rows in chunks");

    assert_eq!(stats.observations_loaded, 5);
    assert_eq!(stats.parse_errors, 0);
    let delivery_count: i64 = sqlx::query_scalar("SELECT count(*) FROM webhook_deliveries")
        .fetch_one(pool)
        .await
        .expect("count webhook deliveries for staged commit");
    assert_eq!(delivery_count, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_staged_load_closes_dirty_session_before_pool_reuse() {
    let _guard = TEST_LOCK.lock().await;
    let timescale = start_timescale("au_kpis_loader_staged_drop_closes_session")
        .await
        .expect("start timescaledb container");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(timescale.url())
        .await
        .expect("connect to timescaledb");
    migrate(&pool).await.expect("apply migrations");

    let artifact_id = ArtifactId::of_content(b"loader staged dirty drop fixture");
    seed_reference_data(&pool, artifact_id).await;
    let initial_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .expect("read initial backend pid");

    let aus = descriptor("AUS");
    let mut staged = begin_staged_load(
        &pool,
        LoadOptions {
            max_rows: 1,
            max_bytes: 1024 * 1024,
        },
    )
    .await
    .expect("begin staged load");
    staged
        .stage(vec![LoadItemAudit {
            item: item(&aus, artifact_id, ts(2024, 3, 1), 0, 134.2),
            row_context: None,
        }])
        .await
        .expect("stage row");
    drop(staged);

    let after_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .expect("read backend pid after dirty staged drop");
    let staging_exists: bool = sqlx::query_scalar(
        "SELECT to_regclass('pg_temp.staging_series') IS NOT NULL
         OR to_regclass('pg_temp.staging_observations') IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("check temp staging tables after dirty staged drop");

    assert_ne!(after_pid, initial_pid);
    assert!(!staging_exists);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_load_rollback_returns_recorded_parse_error_stats() {
    let _guard = TEST_LOCK.lock().await;
    let timescale = start_timescale("au_kpis_loader_staged_rollback_parse_errors")
        .await
        .expect("start timescaledb container");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(timescale.url())
        .await
        .expect("connect to timescaledb");
    migrate(&pool).await.expect("apply migrations");

    let artifact_id = ArtifactId::of_content(b"loader staged rollback parse errors fixture");
    seed_reference_data(&pool, artifact_id).await;

    let aus = descriptor("AUS");
    let mut bad = descriptor("NSW");
    bad.series_key = aus.series_key;

    let mut staged = begin_staged_load(
        &pool,
        LoadOptions {
            max_rows: 8,
            max_bytes: 1024 * 1024,
        },
    )
    .await
    .expect("begin staged load");
    staged
        .stage(vec![LoadItemAudit {
            item: item(&bad, artifact_id, ts(2024, 3, 1), 0, 134.2),
            row_context: None,
        }])
        .await
        .expect("stage records loader validation error without failing");

    let stats = staged.rollback().await.expect("rollback returns stats");
    assert_eq!(stats.parse_errors, 1);
    assert_eq!(stats.observations_loaded, 0);

    let parse_error_count: i64 = sqlx::query_scalar("SELECT count(*) FROM parse_errors")
        .fetch_one(&pool)
        .await
        .expect("count parse_errors after rollback");
    assert_eq!(parse_error_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_load_stage_error_drops_temp_tables_before_pool_reuse() {
    let _guard = TEST_LOCK.lock().await;
    let timescale = start_timescale("au_kpis_loader_staged_stage_error_cleanup")
        .await
        .expect("start timescaledb container");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(timescale.url())
        .await
        .expect("connect to timescaledb");
    migrate(&pool).await.expect("apply migrations");

    let artifact_id = ArtifactId::of_content(b"loader staged error cleanup fixture");
    seed_reference_data(&pool, artifact_id).await;

    let aus = descriptor("AUS");
    let mut staged = begin_staged_load(
        &pool,
        LoadOptions {
            max_rows: 1,
            max_bytes: 1024 * 1024,
        },
    )
    .await
    .expect("begin staged load");
    staged
        .stage(vec![LoadItemAudit {
            item: item(&aus, artifact_id, ts(2024, 3, 1), 0, 134.2),
            row_context: None,
        }])
        .await
        .expect("stage initial row");

    let mut bad_item = item(&aus, artifact_id, ts(2024, 6, 1), 0, 135.0);
    bad_item.series.unit.push('\0');
    staged
        .stage(vec![LoadItemAudit {
            item: bad_item,
            row_context: None,
        }])
        .await
        .expect_err("nul byte in copy payload should fail staging");
    drop(staged);

    let staging_exists: bool = sqlx::query_scalar(
        "SELECT to_regclass('pg_temp.staging_series') IS NOT NULL
         OR to_regclass('pg_temp.staging_observations') IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("check temp staging tables after failed stage");
    assert!(!staging_exists);
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

    if std::env::var_os("CI").is_some() {
        let budget = Duration::from_millis(500);
        assert!(
            best_elapsed < budget,
            "10k COPY load should finish under {budget:?}, best attempt took {best_elapsed:?}"
        );
    }
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

async fn seed_webhook_subscriptions(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO api_keys (id, key_hash, name, scopes, rate_limit_tier)
         VALUES
             ('11111111-1111-1111-1111-111111111111', 'hash-a', 'webhooks a',
              ARRAY['subscriptions:write'], 'free'),
             ('22222222-2222-2222-2222-222222222222', 'hash-b', 'webhooks b',
              ARRAY['subscriptions:write'], 'free'),
             ('33333333-3333-3333-3333-333333333333', 'hash-c', 'webhooks c',
              ARRAY['subscriptions:write'], 'free')",
    )
    .execute(pool)
    .await
    .expect("insert api keys");

    sqlx::query(
        "INSERT INTO webhook_subscriptions (
             id, api_key_id, target_url, dataflow_ids, signing_secret
         )
         VALUES
             ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
              '11111111-1111-1111-1111-111111111111',
              'https://example.test/cpi', ARRAY['abs.cpi'], 'secret-a-secret-a-secret-a-secret-a'),
             ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
              '22222222-2222-2222-2222-222222222222',
              'https://example.test/all', ARRAY[]::TEXT[], 'secret-b-secret-b-secret-b-secret-b'),
             ('cccccccc-cccc-cccc-cccc-cccccccccccc',
              '33333333-3333-3333-3333-333333333333',
              'https://example.test/wpi', ARRAY['abs.wpi'], 'secret-c-secret-c-secret-c-secret-c')",
    )
    .execute(pool)
    .await
    .expect("insert webhook subscriptions");
}

async fn seed_observation_statement_row_limit(pool: &PgPool, max_rows: i32) {
    sqlx::query(
        "CREATE TABLE statement_row_counter (
             count INTEGER NOT NULL
         )",
    )
    .execute(pool)
    .await
    .expect("create statement counter");
    sqlx::query("INSERT INTO statement_row_counter (count) VALUES (0)")
        .execute(pool)
        .await
        .expect("seed statement counter");
    sqlx::query(
        "CREATE OR REPLACE FUNCTION test_limit_observation_statement_rows()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         DECLARE
             next_count INTEGER;
         BEGIN
             UPDATE statement_row_counter
             SET count = count + 1
             RETURNING count INTO next_count;
             IF next_count > TG_ARGV[0]::INTEGER THEN
                 RAISE EXCEPTION 'observation statement exceeded test row limit';
             END IF;
             RETURN NEW;
         END;
         $$",
    )
    .execute(pool)
    .await
    .expect("create row limit trigger function");
    sqlx::query(
        "CREATE OR REPLACE FUNCTION test_reset_observation_statement_rows()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             UPDATE statement_row_counter SET count = 0;
             RETURN NULL;
         END;
         $$",
    )
    .execute(pool)
    .await
    .expect("create row limit reset function");
    let create_limit_trigger = format!(
        "CREATE TRIGGER test_limit_observation_statement_rows
         BEFORE INSERT OR UPDATE ON observations
         FOR EACH ROW
         EXECUTE FUNCTION test_limit_observation_statement_rows({max_rows})",
    );
    sqlx::query(&create_limit_trigger)
        .execute(pool)
        .await
        .expect("create row limit trigger");
    sqlx::query(
        "CREATE TRIGGER test_reset_observation_statement_rows
         AFTER INSERT OR UPDATE ON observations
         FOR EACH STATEMENT
         EXECUTE FUNCTION test_reset_observation_statement_rows()",
    )
    .execute(pool)
    .await
    .expect("create row limit reset trigger");
}

async fn seed_observation_update_counter(pool: &PgPool) {
    sqlx::query(
        "CREATE TABLE observation_update_counter (
             count INTEGER NOT NULL
         )",
    )
    .execute(pool)
    .await
    .expect("create observation update counter");
    sqlx::query("INSERT INTO observation_update_counter (count) VALUES (0)")
        .execute(pool)
        .await
        .expect("seed observation update counter");
    sqlx::query(
        "CREATE OR REPLACE FUNCTION test_count_observation_updates()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             UPDATE observation_update_counter SET count = count + 1;
             RETURN NEW;
         END;
         $$",
    )
    .execute(pool)
    .await
    .expect("create observation update counter function");
    sqlx::query(
        "CREATE TRIGGER test_count_observation_updates
         BEFORE UPDATE ON observations
         FOR EACH ROW
         EXECUTE FUNCTION test_count_observation_updates()",
    )
    .execute(pool)
    .await
    .expect("create observation update counter trigger");
}
