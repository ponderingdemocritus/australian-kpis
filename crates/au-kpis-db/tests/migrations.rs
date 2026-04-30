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
        "api_keys",
        "artifacts",
        "codelists",
        "codes",
        "dataflows",
        "dimensions",
        "measures",
        "observations",
        "observations_latest",
        "parse_errors",
        "series",
        "sources",
    ] {
        assert!(
            table_names.contains(&expected),
            "expected `{expected}` to exist; found {table_names:?}"
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
