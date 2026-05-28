use chrono::{Duration, Utc};

use au_kpis_scheduler::data_quality::{DataQualityRule, run_data_quality_checks};
use au_kpis_testing::timescale::start_timescale;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_quality_checks_report_range_cardinality_recency_and_revision_anomalies() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }

    let timescale = start_timescale("au_kpis_data_quality")
        .await
        .expect("start timescale");
    let pool = connect_with_retry(timescale.url()).await;
    let now = Utc::now();
    seed_anomalous_dataflow(&pool, now).await;

    let rule = DataQualityRule {
        dataflow_id: "abs.cpi",
        min_value: 0.0,
        max_value: 200.0,
        min_active_series: 2,
        latest_period_cardinality_floor: 1.0,
        max_recency_lag_days: 90,
        max_daily_revisions: 0,
        z_score_sigma: 5.0,
    };

    let report = run_data_quality_checks(&pool, &[rule], now)
        .await
        .expect("run data-quality checks");

    let result = report.results.first().expect("one dataflow result");
    let rules = result
        .anomalies
        .iter()
        .map(|anomaly| anomaly.rule.as_str())
        .collect::<Vec<_>>();
    assert!(rules.contains(&"plausible_range"), "{rules:?}");
    assert!(rules.contains(&"cardinality"), "{rules:?}");
    assert!(rules.contains(&"recency"), "{rules:?}");
    assert!(rules.contains(&"revision_volume"), "{rules:?}");
    assert!(report.render_markdown().contains("Data Quality Report"));
}

async fn connect_with_retry(database_url: &str) -> sqlx::PgPool {
    let cfg = au_kpis_config::DatabaseConfig {
        url: database_url.to_string(),
    };
    let mut last_err = None;
    for _ in 0..10 {
        match au_kpis_db::connect(&cfg).await {
            Ok(pool) => {
                au_kpis_db::migrate(&pool).await.expect("apply migrations");
                return pool;
            }
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }
    panic!("timescaledb did not accept connections: {last_err:?}");
}

async fn seed_anomalous_dataflow(pool: &sqlx::PgPool, now: chrono::DateTime<Utc>) {
    let latest = now - Duration::days(120);
    let revision_time = latest;
    let series_a = [1_u8; 32];
    let series_b = [2_u8; 32];
    let artifact = [3_u8; 32];

    sqlx::query(
        r#"INSERT INTO sources (id, name, homepage)
           VALUES ('abs', 'Australian Bureau of Statistics', 'https://www.abs.gov.au')"#,
    )
    .execute(pool)
    .await
    .expect("insert source");

    sqlx::query(
        r#"INSERT INTO measures (id, name, unit)
           VALUES ('index', 'Index', 'index')"#,
    )
    .execute(pool)
    .await
    .expect("insert measure");

    sqlx::query(
        r#"INSERT INTO dataflows (
               id, source_id, name, dimensions, measures, frequency, license,
               attribution, source_url
           )
           VALUES (
               'abs.cpi', 'abs', 'Consumer Price Index', ARRAY['region'],
               ARRAY['index'], 'quarterly', 'CC BY 4.0', 'ABS',
               'https://data.api.abs.gov.au/rest/data/ABS,CPI,2.0.0/all'
           )"#,
    )
    .execute(pool)
    .await
    .expect("insert dataflow");

    sqlx::query(
        r#"INSERT INTO artifacts (
               id, source_id, source_url, content_type, response_headers,
               size_bytes, storage_key, fetched_at
           )
           VALUES (
               $1, 'abs',
               'https://data.api.abs.gov.au/rest/data/ABS,CPI,2.0.0/all',
               'application/json', '{}'::jsonb, 2, 'raw/abs/cpi.json', $2
           )"#,
    )
    .bind(artifact.as_slice())
    .bind(now)
    .execute(pool)
    .await
    .expect("insert artifact");

    for (series_key, region) in [(series_a, "au"), (series_b, "nsw")] {
        sqlx::query(
            r#"INSERT INTO series (
                   series_key, dataflow_id, measure_id, dimensions, unit,
                   first_observed, last_observed, active
               )
               VALUES ($1, 'abs.cpi', 'index', $2, 'index', $3, $3, true)"#,
        )
        .bind(series_key.as_slice())
        .bind(serde_json::json!({ "region": region }))
        .bind(latest)
        .execute(pool)
        .await
        .expect("insert series");
    }

    sqlx::query(
        r#"INSERT INTO observations (
               series_key, time, revision_no, time_precision, value, status,
               ingested_at, source_artifact_id
           )
           VALUES ($1, $2, 0, 'quarter', 300.0, 'normal', $3, $4)"#,
    )
    .bind(series_a.as_slice())
    .bind(latest)
    .bind(now)
    .bind(artifact.as_slice())
    .execute(pool)
    .await
    .expect("insert latest out-of-range observation");

    sqlx::query(
        r#"INSERT INTO observations (
               series_key, time, revision_no, time_precision, value, status,
               ingested_at, source_artifact_id
           )
           VALUES ($1, $2, 1, 'quarter', 301.0, 'revised', $3, $4)"#,
    )
    .bind(series_a.as_slice())
    .bind(revision_time)
    .bind(now)
    .bind(artifact.as_slice())
    .execute(pool)
    .await
    .expect("insert revision observation");
}

fn docker_available() -> bool {
    std::env::var_os("DOCKER_HOST").is_some()
        || std::path::Path::new("/var/run/docker.sock").exists()
}
