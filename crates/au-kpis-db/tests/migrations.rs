//! Integration tests for `au-kpis-db` migrations.
//!
//! Uses [`testcontainers`] to spin up a real TimescaleDB instance,
//! apply the bundled migrations, and verify:
//!
//! * the target tables + the `observations` hypertable are present,
//! * a compression policy exists on `observations`,
//! * `revert → run` yields the same schema as a fresh `run`
//!   (the idempotency contract in issue #7).
//!
//! Requires a working Docker daemon. In CI the job runs against the
//! default socket; locally `docker compose -f infra/compose/docker-compose.yml`
//! or `colima start` is enough.

use std::time::Duration;

use au_kpis_config::DatabaseConfig;
use au_kpis_db::{connect, ensure_timescale, migrate, revert_latest, timescale_version};
use au_kpis_testing::timescale::start_timescale;
use sqlx::{PgPool, Row};

async fn connect_with_retry(cfg: &DatabaseConfig) -> PgPool {
    let mut last_err = None;
    for _ in 0..10 {
        match connect(cfg).await {
            Ok(pool) => return pool,
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    panic!("timescaledb did not accept connections: {last_err:?}");
}

async fn hypertable_exists(pool: &PgPool, name: &str) -> bool {
    let row: (bool,) = sqlx::query_as(
        "SELECT EXISTS (
            SELECT 1
            FROM   timescaledb_information.hypertables
            WHERE  hypertable_name = $1
        )",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("query hypertable existence");
    row.0
}

async fn has_compression_policy(pool: &PgPool, name: &str) -> bool {
    let row: (bool,) = sqlx::query_as(
        "SELECT EXISTS (
            SELECT 1
            FROM   timescaledb_information.jobs
            WHERE  proc_name = 'policy_compression'
            AND    hypertable_name = $1
        )",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("query compression policy existence");
    row.0
}

async fn continuous_aggregate_exists(pool: &PgPool, name: &str) -> bool {
    let row: (bool,) = sqlx::query_as(
        "SELECT EXISTS (
            SELECT 1
            FROM   timescaledb_information.continuous_aggregates
            WHERE  view_name = $1
        )",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("query continuous aggregate existence");
    row.0
}

async fn has_continuous_aggregate_policy(pool: &PgPool, name: &str) -> bool {
    let row: (bool,) = sqlx::query_as(
        "SELECT EXISTS (
            SELECT 1
            FROM timescaledb_information.jobs AS jobs
            JOIN timescaledb_information.continuous_aggregates AS aggregates
              ON aggregates.materialization_hypertable_schema = jobs.hypertable_schema
             AND aggregates.materialization_hypertable_name = jobs.hypertable_name
            WHERE jobs.proc_name = 'policy_refresh_continuous_aggregate'
              AND aggregates.view_schema = 'public'
              AND aggregates.view_name = $1
        )",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("query continuous aggregate policy existence");
    row.0
}

async fn index_exists(pool: &PgPool, name: &str) -> bool {
    let row: (bool,) = sqlx::query_as(
        "SELECT EXISTS (
            SELECT 1
            FROM   pg_indexes
            WHERE  schemaname = 'public'
            AND    indexname = $1
        )",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("query index existence");
    row.0
}

async fn column_default(pool: &PgPool, table: &str, column: &str) -> Option<String> {
    sqlx::query_scalar(
        "SELECT column_default
         FROM   information_schema.columns
         WHERE  table_schema = 'public'
         AND    table_name = $1
         AND    column_name = $2",
    )
    .bind(table)
    .bind(column)
    .fetch_one(pool)
    .await
    .expect("query column default")
}

/// Collect a stable `(table, column)` list for every user table in the
/// `public` schema. Excludes sqlx's bookkeeping table so fingerprints
/// compare equal regardless of migration-tracking state.
async fn schema_fingerprint(pool: &PgPool) -> Vec<(String, String)> {
    let rows = sqlx::query(
        "SELECT table_name, column_name
         FROM   information_schema.columns
         WHERE  table_schema = 'public'
         AND    table_name <> '_sqlx_migrations'
         ORDER  BY table_name, ordinal_position",
    )
    .fetch_all(pool)
    .await
    .expect("fetch schema fingerprint");

    rows.into_iter()
        .map(|row| {
            (
                row.get::<String, _>("table_name"),
                row.get::<String, _>("column_name"),
            )
        })
        .collect()
}

async fn applied_migration_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
        .fetch_one(pool)
        .await
        .expect("count applied migrations")
}

async fn seed_minute_precision_fixture(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage)
         VALUES ('aemo', 'AEMO', 'https://aemo.com.au')",
    )
    .execute(pool)
    .await
    .expect("seed source");

    sqlx::query(
        "INSERT INTO measures (id, name, unit)
         VALUES ('value', 'Value', 'MW')",
    )
    .execute(pool)
    .await
    .expect("seed measure");

    sqlx::query(
        "INSERT INTO dataflows
            (id, source_id, name, dimensions, measures, frequency, license, attribution, source_url)
         VALUES
            ('aemo.dispatch', 'aemo', 'AEMO dispatch', ARRAY['region'], ARRAY['value'],
             'irregular', 'AEMO Copyright and Disclaimer Notice',
             'Source: Australian Energy Market Operator', 'https://www.nemweb.com.au')",
    )
    .execute(pool)
    .await
    .expect("seed dataflow");

    sqlx::query(
        "INSERT INTO series (series_key, dataflow_id, measure_id, dimensions, unit)
         VALUES
            (decode(repeat('11', 32), 'hex'), 'aemo.dispatch', 'value',
             jsonb_build_object('region', 'NSW1'), 'MW')",
    )
    .execute(pool)
    .await
    .expect("seed series");

    sqlx::query(
        "INSERT INTO artifacts
            (id, source_id, source_url, content_type, size_bytes, storage_key, fetched_at,
             response_headers)
         VALUES
            (decode(repeat('22', 32), 'hex'), 'aemo', 'https://www.nemweb.com.au/example.zip',
             'application/zip', 10, 'artifacts/test', now(), '{}'::jsonb)",
    )
    .execute(pool)
    .await
    .expect("seed artifact");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn migration_creates_hypertable_and_compression_policy() {
    let timescale = start_timescale("au_kpis_test")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;

    // `connect` already enables the extension; prove that the
    // compile-checked query resolves the version.
    let version = timescale_version(&pool)
        .await
        .expect("timescale version query")
        .expect("timescale extension should be installed after connect");
    assert!(
        !version.is_empty(),
        "timescale extension version should be non-empty"
    );

    migrate(&pool).await.expect("apply migrations");

    assert!(
        hypertable_exists(&pool, "observations").await,
        "observations should be registered as a hypertable"
    );
    assert!(
        has_compression_policy(&pool, "observations").await,
        "observations should have a compression policy installed"
    );
    assert_eq!(
        column_default(&pool, "artifacts", "response_headers").await,
        None,
        "artifact response_headers must be explicitly supplied"
    );

    seed_minute_precision_fixture(&pool).await;
    sqlx::query(
        "INSERT INTO observations
            (series_key, time, revision_no, time_precision, value, status, source_artifact_id)
         VALUES
            (decode(repeat('11', 32), 'hex'), '2026-06-19T17:05:00Z', 0,
             'minute', 1224.55, 'normal', decode(repeat('22', 32), 'hex'))",
    )
    .execute(&pool)
    .await
    .expect("minute precision should be accepted");

    let invalid_precision = sqlx::query(
        "INSERT INTO observations
            (series_key, time, revision_no, time_precision, value, status, source_artifact_id)
         VALUES
            (decode(repeat('11', 32), 'hex'), '2026-06-19T17:10:00Z', 0,
             'hour', 1225.00, 'normal', decode(repeat('22', 32), 'hex'))",
    )
    .execute(&pool)
    .await;
    assert!(
        invalid_precision.is_err(),
        "unsupported time precision should still be rejected"
    );

    // Sanity-check one representative table + the latest-revision view.
    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'public' ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .expect("list tables");
    let table_names: Vec<&str> = tables.iter().map(|t| t.0.as_str()).collect();
    for expected in [
        "api_key_audit_log",
        "api_keys",
        "artifact_fetches",
        "artifact_loads",
        "artifacts",
        "codelists",
        "codes",
        "dataflows",
        "dimensions",
        "discovered_work",
        "ingestion_generations",
        "measures",
        "observation_stage",
        "observations",
        "observations_latest",
        "observations_rollup_monthly",
        "observations_rollup_monthly_points",
        "observations_rollup_quarterly",
        "observations_rollup_quarterly_points",
        "observations_rollup_weekly",
        "observations_rollup_weekly_points",
        "parse_errors",
        "queue_schedule_occurrences",
        "scorecard_configs",
        "scorecard_snapshot_contributions",
        "scorecard_snapshot_generations",
        "scorecard_snapshots",
        "scorecard_snapshots_as_published",
        "scorecard_snapshots_latest",
        "series",
        "sources",
        "webhook_deliveries",
        "webhook_delivery_attempts",
        "webhook_subscriptions",
    ] {
        assert!(
            table_names.contains(&expected),
            "expected `{expected}` to exist; found {table_names:?}"
        );
    }

    for expected in [
        "dataflows_search_tsv_gin",
        "dataflows_name_trgm_gin",
        "dataflows_description_trgm_gin",
        "measures_search_tsv_gin",
        "measures_name_trgm_gin",
        "measures_description_trgm_gin",
        "series_dataflow_first_observed_series_key_idx",
        "artifact_fetches_artifact_idx",
        "artifact_fetches_source_idx",
        "artifact_fetches_source_url_idx",
        "artifact_loads_artifact_fetch_idx",
        "artifact_loads_source_dataflow_idx",
        "discovered_work_status_idx",
        "ingestion_generations_status_idx",
        "observations_generation_idx",
        "queue_cron_schedules_due_idx",
        "queue_jobs_active_dedupe_idx",
        "scorecard_snapshot_contributions_generation_idx",
        "scorecard_snapshots_latest_idx",
    ] {
        assert!(
            index_exists(&pool, expected).await,
            "expected index `{expected}` to exist"
        );
    }

    for expected in [
        "observations_rollup_weekly_points",
        "observations_rollup_monthly_points",
        "observations_rollup_quarterly_points",
    ] {
        assert!(
            continuous_aggregate_exists(&pool, expected).await,
            "expected continuous aggregate `{expected}` to exist"
        );
        assert!(
            has_continuous_aggregate_policy(&pool, expected).await,
            "expected continuous aggregate `{expected}` to have a refresh policy"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revert_then_run_is_idempotent() {
    let timescale = start_timescale("au_kpis_test")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;

    migrate(&pool).await.expect("initial migrate");
    let first = schema_fingerprint(&pool).await;
    assert!(!first.is_empty(), "migration produced no tables");

    let migrations = applied_migration_count(&pool).await;
    for _ in 0..migrations {
        revert_latest(&pool).await.expect("revert migration");
    }
    let after_revert = schema_fingerprint(&pool).await;
    assert!(
        after_revert.is_empty(),
        "revert should leave no public tables behind; found {after_revert:?}"
    );

    // Extension is instance-level; down migration does not drop it.
    // Re-assert it so the second `run` works even if a stray pool
    // session landed on a different connection.
    ensure_timescale(&pool).await.expect("re-ensure timescale");

    migrate(&pool).await.expect("re-run migrate");
    let second = schema_fingerprint(&pool).await;
    assert_eq!(
        first, second,
        "schema after revert→run must match initial run"
    );
}
