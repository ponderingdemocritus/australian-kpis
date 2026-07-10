//! Deterministic production-scale certification dataset generator.

#![forbid(unsafe_code)]

use std::{collections::BTreeSet, fs, path::PathBuf, time::Instant};

use anyhow::{Context, ensure};
use au_kpis_source_register::{SourceRegisterDataflow, SourceStatus, load_source_register};
use clap::Parser;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};

const GENERATOR_VERSION: &str = "scale-seed.v1";
const REQUIRED_ACTIVE_DATAFLOWS: usize = 20;
const REVISION_INTERVAL: u64 = 100;

#[derive(Debug, Parser)]
#[command(name = "au-kpis-scale-seed")]
#[command(about = "Seed an isolated Timescale certification database")]
struct Args {
    #[arg(long, env = "AU_KPIS_DATABASE_URL")]
    database_url: String,
    #[arg(long, default_value = "production-v1")]
    seed: String,
    #[arg(long, default_value_t = 100)]
    catalog_dataflows: usize,
    #[arg(long, default_value_t = 50_000_000)]
    observations: u64,
    #[arg(long, default_value_t = 100)]
    series_per_dataflow: usize,
    #[arg(long, default_value = "target/release-scale-report/seed-manifest.json")]
    manifest: PathBuf,
    #[arg(long)]
    skip_rollup_refresh: bool,
    #[arg(long)]
    skip_compression: bool,
    #[arg(long)]
    verify_only: bool,
    #[arg(long)]
    confirm_certification_database: bool,
}

#[derive(Debug, Serialize)]
struct SeedManifest {
    generator_version: &'static str,
    seed: String,
    dataset_digest: String,
    catalog_dataflows: usize,
    launch_dataflows: usize,
    series_per_dataflow: usize,
    expected_observations: u64,
    actual_observations: i64,
    revision_observations: i64,
    aemo_five_minute_rows: i64,
    timescale_version: String,
    chunks_total: i64,
    chunks_compressed: i64,
    rollups_refreshed: bool,
    compression_requested: bool,
    elapsed_seconds: f64,
    active_dataflow_ids: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    ensure!(
        args.confirm_certification_database,
        "refusing to seed without --confirm-certification-database; use an isolated certification database"
    );
    ensure!(!args.seed.trim().is_empty(), "--seed must not be empty");
    ensure!(args.observations > 0, "--observations must be positive");
    ensure!(
        args.series_per_dataflow > 0,
        "--series-per-dataflow must be positive"
    );

    let register = load_source_register().context("load governed source register")?;
    let mut active = register
        .dataflows
        .into_iter()
        .filter(|entry| entry.status == SourceStatus::Active)
        .collect::<Vec<_>>();
    active.sort_by(|left, right| left.dataflow_id.cmp(&right.dataflow_id));
    ensure!(
        active.len() == REQUIRED_ACTIVE_DATAFLOWS,
        "production v1 requires exactly {REQUIRED_ACTIVE_DATAFLOWS} active dataflows, found {}",
        active.len()
    );
    ensure!(
        args.catalog_dataflows >= active.len(),
        "catalog must contain at least the {} active launch dataflows",
        active.len()
    );

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&args.database_url)
        .await
        .context("connect to certification Timescale database")?;
    assert_certification_database(&pool).await?;

    let started = Instant::now();
    if !args.verify_only {
        seed_catalog(&pool, &args, &active).await?;
        seed_launch_rows(&pool, &args, &active).await?;

        if !args.skip_rollup_refresh {
            refresh_rollups(&pool).await?;
        }
        if !args.skip_compression {
            compress_old_chunks(&pool).await?;
        }
    }

    let active_ids = active
        .iter()
        .map(|entry| entry.dataflow_id.clone())
        .collect::<Vec<_>>();
    let manifest =
        build_manifest(&pool, &args, active_ids, started.elapsed().as_secs_f64()).await?;
    ensure!(
        manifest.actual_observations == args.observations as i64,
        "seed verification counted {} rows, expected {}",
        manifest.actual_observations,
        args.observations
    );
    ensure!(
        manifest.revision_observations > 0,
        "certification dataset must contain revisions"
    );
    ensure!(
        manifest.aemo_five_minute_rows > 0,
        "certification dataset must contain five-minute AEMO rows"
    );

    if let Some(parent) = args.manifest.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create manifest directory {}", parent.display()))?;
    }
    let json = serde_json::to_vec_pretty(&manifest).context("serialize seed manifest")?;
    fs::write(&args.manifest, json)
        .with_context(|| format!("write seed manifest {}", args.manifest.display()))?;
    println!(
        "seeded {} observations across {} launch dataflows; manifest={}",
        manifest.actual_observations,
        manifest.launch_dataflows,
        args.manifest.display()
    );
    Ok(())
}

async fn assert_certification_database(pool: &PgPool) -> anyhow::Result<()> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
        .context("read current database name")?;
    ensure!(
        database.contains("cert") || database.contains("benchmark") || database.contains("scale"),
        "database `{database}` is not named as an isolated certification/benchmark/scale database"
    );
    Ok(())
}

async fn seed_catalog(
    pool: &PgPool,
    args: &Args,
    active: &[SourceRegisterDataflow],
) -> anyhow::Result<()> {
    let mut transaction = pool.begin().await.context("begin catalog seed")?;
    let sources = active
        .iter()
        .map(|entry| entry.source_id.as_str())
        .collect::<BTreeSet<_>>();
    for source_id in sources {
        sqlx::query(
            "INSERT INTO sources (id, name, homepage, description) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
        )
        .bind(source_id)
        .bind(format!("Certification source {source_id}"))
        .bind(format!("https://benchmark.invalid/sources/{source_id}"))
        .bind(format!("{GENERATOR_VERSION} governed launch source"))
        .execute(&mut *transaction)
        .await
        .with_context(|| format!("seed source {source_id}"))?;
    }
    sqlx::query(
        "INSERT INTO sources (id, name, homepage, description) VALUES ('benchmark', 'Certification catalog', 'https://benchmark.invalid', $1) ON CONFLICT (id) DO NOTHING",
    )
    .bind(GENERATOR_VERSION)
    .execute(&mut *transaction)
    .await
    .context("seed synthetic catalog source")?;
    sqlx::query(
        "INSERT INTO measures (id, name, description, unit, scale) VALUES ('benchmark.value', 'Certification value', $1, 'index', 1) ON CONFLICT (id) DO NOTHING",
    )
    .bind(GENERATOR_VERSION)
    .execute(&mut *transaction)
    .await
    .context("seed benchmark measure")?;

    for entry in active {
        sqlx::query(
            r#"INSERT INTO dataflows
               (id, source_id, name, description, dimensions, measures, frequency, license, attribution, source_url)
               VALUES ($1, $2, $3, $4, ARRAY['benchmark_series'], ARRAY['benchmark.value'], $5, $6, $7, $8)
               ON CONFLICT (id) DO NOTHING"#,
        )
        .bind(&entry.dataflow_id)
        .bind(&entry.source_id)
        .bind(format!("Certification {}", entry.dataflow_id))
        .bind(format!("{GENERATOR_VERSION} governed launch dataflow"))
        .bind(database_frequency(&entry.cadence))
        .bind(&entry.license)
        .bind(&entry.attribution)
        .bind(&entry.canonical_url)
        .execute(&mut *transaction)
        .await
        .with_context(|| format!("seed launch dataflow {}", entry.dataflow_id))?;
    }

    for index in active.len()..args.catalog_dataflows {
        let id = format!("benchmark.catalog.{index:03}");
        sqlx::query(
            r#"INSERT INTO dataflows
               (id, source_id, name, description, dimensions, measures, frequency, license, attribution, source_url)
               VALUES ($1, 'benchmark', $2, $3, ARRAY['benchmark_series'], ARRAY['benchmark.value'], 'daily', 'Synthetic certification data', 'Generated by au-kpis-scale-seed', $4)
               ON CONFLICT (id) DO NOTHING"#,
        )
        .bind(&id)
        .bind(format!("Certification catalog {index:03}"))
        .bind(format!("{GENERATOR_VERSION} catalog-only dataflow"))
        .bind(format!("https://benchmark.invalid/dataflows/{id}"))
        .execute(&mut *transaction)
        .await
        .with_context(|| format!("seed synthetic dataflow {id}"))?;
    }
    transaction.commit().await.context("commit catalog seed")?;
    Ok(())
}

async fn seed_launch_rows(
    pool: &PgPool,
    args: &Args,
    active: &[SourceRegisterDataflow],
) -> anyhow::Result<()> {
    for (dataflow_index, entry) in active.iter().enumerate() {
        let rows = rows_for_dataflow(args.observations, active.len(), dataflow_index);
        let artifact_payload = format!(
            "{GENERATOR_VERSION}\n{}\n{}\n",
            args.seed, entry.dataflow_id
        );
        let artifact_id = Sha256::digest(artifact_payload.as_bytes()).to_vec();
        let artifact_hex = hex::encode(&artifact_id);
        let mut transaction = pool
            .begin()
            .await
            .with_context(|| format!("begin seed for {}", entry.dataflow_id))?;
        sqlx::query(
            r#"INSERT INTO artifacts
               (id, source_id, source_url, content_type, response_headers, size_bytes, storage_key, fetched_at)
               VALUES ($1, $2, $3, 'text/plain', '{}'::jsonb, $4, $5, '2026-01-01T00:00:00Z')
               ON CONFLICT (id) DO NOTHING"#,
        )
        .bind(&artifact_id)
        .bind(&entry.source_id)
        .bind(format!(
            "https://benchmark.invalid/artifacts/{}/{}",
            entry.dataflow_id, args.seed
        ))
        .bind(artifact_payload.len() as i64)
        .bind(format!("artifacts/{artifact_hex}"))
        .execute(&mut *transaction)
        .await
        .with_context(|| format!("seed artifact for {}", entry.dataflow_id))?;

        for series_index in 0..args.series_per_dataflow {
            let series_key = Sha256::digest(
                format!(
                    "{GENERATOR_VERSION}\n{}\n{}\n{series_index}\n",
                    args.seed, entry.dataflow_id
                )
                .as_bytes(),
            )
            .to_vec();
            sqlx::query(
                r#"INSERT INTO series
                   (series_key, dataflow_id, measure_id, dimensions, unit, first_observed, last_observed, active)
                   VALUES ($1, $2, 'benchmark.value', jsonb_build_object('benchmark_series', $3::text, 'benchmark_seed', $4::text), 'index', NULL, NULL, true)
                   ON CONFLICT (series_key) DO NOTHING"#,
            )
            .bind(series_key)
            .bind(&entry.dataflow_id)
            .bind(series_index as i64)
            .bind(&args.seed)
            .execute(&mut *transaction)
            .await
            .with_context(|| {
                format!("seed series {series_index} for {}", entry.dataflow_id)
            })?;
        }
        transaction
            .commit()
            .await
            .with_context(|| format!("commit metadata for {}", entry.dataflow_id))?;

        let cadence_seconds = if entry.source_id == "aemo" {
            300_i64
        } else {
            86_400_i64
        };
        let time_precision = if cadence_seconds == 300 {
            "minute"
        } else {
            "day"
        };
        let inserted = sqlx::query(
            r#"WITH generated AS (
                   SELECT ordinal,
                          ordinal - ((ordinal + 1) / $6::bigint) AS logical_ordinal,
                          CASE WHEN ordinal % $6::bigint = $6::bigint - 1 THEN 1 ELSE 0 END AS revision_no
                   FROM generate_series(0::bigint, $1::bigint - 1) AS ordinal
               ), mapped AS (
                   SELECT logical_ordinal % $2::bigint AS series_no,
                          logical_ordinal / $2::bigint AS point_no,
                          revision_no
                   FROM generated
               )
               INSERT INTO observations
                   (series_key, time, revision_no, time_precision, value, status, attributes, ingested_at, source_artifact_id)
               SELECT s.series_key,
                      TIMESTAMPTZ '1950-01-01T00:00:00Z'
                        + make_interval(secs => (mapped.point_no * $3::bigint)::double precision),
                      mapped.revision_no::integer,
                      $4,
                      ($7::double precision * 1000.0)
                        + mapped.series_no::double precision
                        + (mapped.point_no::double precision / 10000.0)
                        + (mapped.revision_no::double precision / 1000000.0),
                      CASE WHEN mapped.revision_no = 1 THEN 'revised' ELSE 'normal' END,
                      jsonb_build_object('benchmark_seed', $5::text, 'generator', 'scale-seed.v1'),
                      TIMESTAMPTZ '2026-01-01T00:00:00Z',
                      $8
               FROM mapped
               JOIN series AS s
                 ON s.dataflow_id = $9
                AND s.dimensions ->> 'benchmark_seed' = $5
                AND (s.dimensions ->> 'benchmark_series')::bigint = mapped.series_no
               ON CONFLICT (series_key, time, revision_no) DO NOTHING"#,
        )
        .bind(rows as i64)
        .bind(args.series_per_dataflow as i64)
        .bind(cadence_seconds)
        .bind(time_precision)
        .bind(&args.seed)
        .bind(REVISION_INTERVAL as i64)
        .bind(dataflow_index as i64)
        .bind(&artifact_id)
        .bind(&entry.dataflow_id)
        .execute(pool)
        .await
        .with_context(|| format!("seed {rows} observations for {}", entry.dataflow_id))?
        .rows_affected();
        println!(
            "{}: requested={rows} inserted={inserted} cadence_seconds={cadence_seconds}",
            entry.dataflow_id
        );
    }
    Ok(())
}

async fn refresh_rollups(pool: &PgPool) -> anyhow::Result<()> {
    for view in [
        "observations_rollup_weekly_points",
        "observations_rollup_monthly_points",
        "observations_rollup_quarterly_points",
    ] {
        let statement = format!("CALL refresh_continuous_aggregate('{view}', NULL, NULL)");
        sqlx::query(&statement)
            .execute(pool)
            .await
            .with_context(|| format!("refresh {view}"))?;
    }
    Ok(())
}

async fn compress_old_chunks(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        r#"SELECT compress_chunk(chunk, if_not_compressed => true)
           FROM show_chunks('observations', older_than => now() - INTERVAL '7 days') AS chunk"#,
    )
    .execute(pool)
    .await
    .context("compress seeded observation chunks")?;
    Ok(())
}

async fn build_manifest(
    pool: &PgPool,
    args: &Args,
    active_dataflow_ids: Vec<String>,
    elapsed_seconds: f64,
) -> anyhow::Result<SeedManifest> {
    let counts = sqlx::query(
        r#"SELECT count(*)::bigint AS observations,
                  count(*) FILTER (WHERE revision_no > 0)::bigint AS revisions,
                  count(*) FILTER (
                      WHERE time_precision = 'minute' AND s.dataflow_id LIKE 'aemo.%'
                  )::bigint AS aemo_rows
           FROM observations AS o
           JOIN series AS s USING (series_key)
           WHERE o.attributes ->> 'benchmark_seed' = $1"#,
    )
    .bind(&args.seed)
    .fetch_one(pool)
    .await
    .context("count seeded observations")?;
    let chunks = sqlx::query(
        r#"SELECT count(*)::bigint AS total,
                  count(*) FILTER (WHERE is_compressed)::bigint AS compressed
           FROM timescaledb_information.chunks
           WHERE hypertable_name = 'observations'"#,
    )
    .fetch_one(pool)
    .await
    .context("read Timescale compression state")?;
    let timescale_version: String =
        sqlx::query_scalar("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
            .fetch_one(pool)
            .await
            .context("read Timescale version")?;
    let dataset_digest = dataset_digest(args, &active_dataflow_ids);
    Ok(SeedManifest {
        generator_version: GENERATOR_VERSION,
        seed: args.seed.clone(),
        dataset_digest,
        catalog_dataflows: args.catalog_dataflows,
        launch_dataflows: active_dataflow_ids.len(),
        series_per_dataflow: args.series_per_dataflow,
        expected_observations: args.observations,
        actual_observations: counts.try_get("observations")?,
        revision_observations: counts.try_get("revisions")?,
        aemo_five_minute_rows: counts.try_get("aemo_rows")?,
        timescale_version,
        chunks_total: chunks.try_get("total")?,
        chunks_compressed: chunks.try_get("compressed")?,
        rollups_refreshed: !args.verify_only && !args.skip_rollup_refresh,
        compression_requested: !args.verify_only && !args.skip_compression,
        elapsed_seconds,
        active_dataflow_ids,
    })
}

fn rows_for_dataflow(total: u64, count: usize, index: usize) -> u64 {
    let count = count as u64;
    (total / count) + u64::from((index as u64) < total % count)
}

fn database_frequency(cadence: &str) -> &'static str {
    match cadence {
        "daily" | "5-minute" => "daily",
        "weekly" => "weekly",
        "monthly" => "monthly",
        "quarterly" => "quarterly",
        "annual" => "annual",
        _ => "irregular",
    }
}

fn dataset_digest(args: &Args, active_ids: &[String]) -> String {
    let mut digest = Sha256::new();
    for value in [
        GENERATOR_VERSION.to_owned(),
        args.seed.clone(),
        args.catalog_dataflows.to_string(),
        args.observations.to_string(),
        args.series_per_dataflow.to_string(),
        REVISION_INTERVAL.to_string(),
        active_ids.join(","),
    ] {
        digest.update(value.as_bytes());
        digest.update(b"\n");
    }
    hex::encode(digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_distribution_is_exact_and_balanced() {
        let rows = (0..REQUIRED_ACTIVE_DATAFLOWS)
            .map(|index| rows_for_dataflow(50_000_003, REQUIRED_ACTIVE_DATAFLOWS, index))
            .collect::<Vec<_>>();
        assert_eq!(rows.iter().sum::<u64>(), 50_000_003);
        assert_eq!(rows[0] - rows[REQUIRED_ACTIVE_DATAFLOWS - 1], 1);
    }

    #[test]
    fn database_frequency_maps_five_minute_catalog_rows() {
        assert_eq!(database_frequency("5-minute"), "daily");
        assert_eq!(database_frequency("quarterly"), "quarterly");
        assert_eq!(database_frequency("ad-hoc"), "irregular");
    }
}
