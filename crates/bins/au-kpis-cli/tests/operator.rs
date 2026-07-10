use std::{fs, path::PathBuf, process::Command, time::SystemTime};

use assert_cmd::cargo::cargo_bin;
use au_kpis_config::DatabaseConfig;
use au_kpis_db::{connect, migrate};
use au_kpis_domain::{DataflowId, SourceId};
use au_kpis_queue::{ApalisPgQueue, CronSchedule, Job, Queue};
use au_kpis_testing::{minio::start_minio, timescale::start_timescale};
use serde_json::{Value, json};
use sqlx::PgPool;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operator_commands_are_audited_and_preserve_durable_state() {
    if !docker_available() {
        eprintln!("skipping testcontainers integration test: Docker socket unavailable");
        return;
    }
    let postgres = start_timescale("au_kpis_cli_operator")
        .await
        .expect("start Timescale");
    let minio = start_minio("au-kpis-cli-operator")
        .await
        .expect("start MinIO");
    let pool = connect(&DatabaseConfig {
        url: postgres.url().to_string(),
    })
    .await
    .expect("connect Timescale");
    migrate(&pool).await.expect("apply migrations");
    seed_manual_catalog(&pool).await;
    let queue = ApalisPgQueue::new(pool.clone());
    queue
        .schedule(
            CronSchedule::new(
                "source-apra-super",
                "0 1 * * *",
                Job::discover_dataflow(
                    SourceId::new("apra").unwrap(),
                    DataflowId::new("apra.super_asset_allocation").unwrap(),
                ),
            )
            .unwrap()
            .with_timezone("Australia/Sydney")
            .unwrap(),
        )
        .await
        .expect("seed schedule");

    let pause = run(command(postgres.url(), &minio).args([
        "source",
        "pause",
        "--dataflow",
        "apra.super_asset_allocation",
        "--actor",
        "data-oncall@example.test",
        "--reason",
        "review upstream mapping",
    ]));
    assert_eq!(pause["paused"], true);
    assert!(!schedule_enabled(&pool).await);
    let resume = run(command(postgres.url(), &minio).args([
        "source",
        "resume",
        "--dataflow",
        "apra.super_asset_allocation",
        "--actor",
        "data-reviewer@example.test",
        "--reason",
        "mapping reviewed",
    ]));
    assert_eq!(resume["paused"], false);
    assert!(schedule_enabled(&pool).await);

    let dead_job = seed_dead_letter(&pool).await;
    let retried = run(command(postgres.url(), &minio).args([
        "queue",
        "retry-dlq",
        "--job-id",
        &dead_job.to_string(),
        "--actor",
        "data-oncall@example.test",
        "--reason",
        "upstream throttle ended",
    ]));
    assert_eq!(retried["status"], "pending");
    let retry_state: (String, i32, i64) = sqlx::query_as(
        "SELECT status, attempts, (SELECT count(*) FROM queue_dead_letters WHERE job_id = $1)
         FROM queue_jobs WHERE id = $1",
    )
    .bind(dead_job)
    .fetch_one(&pool)
    .await
    .expect("read retried job");
    assert_eq!(retry_state, ("pending".to_string(), 0, 0));

    let input = manual_input_path();
    fs::write(
        &input,
        serde_json::to_vec_pretty(&json!({
            "measure_id": "manual.percent",
            "unit": "percent",
            "observations": [{
                "dimensions": {"category": "productive"},
                "time": "2026-06-30T00:00:00Z",
                "time_precision": "quarter",
                "value": 42.5,
                "status": "normal",
                "attributes": {"method": "reviewed mapping"}
            }]
        }))
        .unwrap(),
    )
    .expect("write manual input");
    let loaded = run(command(postgres.url(), &minio).args([
        "manual-input",
        "load",
        "--file",
        input.to_str().unwrap(),
        "--dataflow",
        "apra.super_asset_allocation",
        "--source-url",
        "https://apra.example.test/reviewed-super.xlsx",
        "--license",
        "Creative Commons Attribution 3.0 Australia Licence",
        "--retrieved-at",
        "2026-07-01",
        "--reviewer-role",
        "product-methodology",
        "--reviewed-at",
        "2026-07-02",
        "--evidence-notes",
        "Category mapping approved in methodology review MR-12.",
        "--actor",
        "methodology-reviewer@example.test",
        "--reason",
        "load reviewed launch blocker",
    ]));
    let generation_id = loaded["generation_id"].as_str().unwrap();
    assert_eq!(loaded["status"], "pending_load");
    assert_eq!(loaded["rows_staged"], 1);
    let durable_counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
           (SELECT count(*) FROM observation_stage WHERE generation_id = $1::uuid),
           (SELECT count(*) FROM manual_input_reviews WHERE generation_id = $1::uuid),
           (SELECT count(*) FROM queue_jobs WHERE payload #>> '{kind,generation_id}' = $1 AND stage = 'load')",
    )
    .bind(generation_id)
    .fetch_one(&pool)
    .await
    .expect("read manual durable state");
    assert_eq!(durable_counts, (1, 1, 1));

    let inspected =
        run(command(postgres.url(), &minio).args(["generation", "inspect", "--id", generation_id]));
    assert_eq!(inspected["status"], "pending_load");
    assert_eq!(inspected["artifact_id"], loaded["artifact_id"]);

    let reparsed = run(command(postgres.url(), &minio).args([
        "artifact",
        "reparse",
        "--artifact-id",
        loaded["artifact_id"].as_str().unwrap(),
        "--dataflow",
        "apra.super_asset_allocation",
        "--parser-version",
        "manual-json-v2",
        "--actor",
        "data-reviewer@example.test",
        "--reason",
        "validate parser version upgrade",
    ]));
    assert_ne!(reparsed["generation_id"], loaded["generation_id"]);
    let reparse_state: (String, i64) = sqlx::query_as(
        "SELECT status,
                (SELECT count(*) FROM queue_jobs WHERE payload #>> '{kind,generation_id}' = $1)
         FROM ingestion_generations WHERE id = $1::uuid",
    )
    .bind(reparsed["generation_id"].as_str().unwrap())
    .fetch_one(&pool)
    .await
    .expect("read reparse generation");
    assert_eq!(reparse_state, ("pending_parse".to_string(), 1));

    let actions: Vec<String> =
        sqlx::query_scalar("SELECT action FROM operator_audit_log ORDER BY id")
            .fetch_all(&pool)
            .await
            .expect("read operator audits");
    assert_eq!(
        actions,
        vec![
            "source.pause",
            "source.resume",
            "queue.retry_dlq",
            "manual_input.load",
            "artifact.reparse",
        ]
    );
    let _ = fs::remove_file(input);
}

fn command(database_url: &str, minio: &au_kpis_testing::minio::MinioHarness) -> Command {
    let mut command = Command::new(cargo_bin("au-kpis-cli"));
    command
        .env("AU_KPIS_DATABASE__URL", database_url)
        .env("AU_KPIS_CACHE__URL", "redis://127.0.0.1:1")
        .env("AU_KPIS_OBJECT_STORE__ENDPOINT", minio.endpoint())
        .env("AU_KPIS_OBJECT_STORE__BUCKET", minio.bucket())
        .env("AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID", minio.access_key())
        .env(
            "AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY",
            minio.secret_key(),
        )
        .env("AU_KPIS_OBJECT_STORE__REGION", "us-east-1")
        .env("AU_KPIS_OBJECT_STORE__ALLOW_HTTP", "true");
    command
}

fn run(command: &mut Command) -> Value {
    let output = command.output().expect("run admin CLI");
    assert!(
        output.status.success(),
        "command failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("parse command JSON")
}

async fn seed_manual_catalog(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage) VALUES ('apra', 'APRA', 'https://apra.gov.au')",
    )
    .execute(pool)
    .await
    .expect("seed source");
    sqlx::query(
        "INSERT INTO measures (id, name, unit) VALUES ('manual.percent', 'Manual percent', 'percent')",
    )
    .execute(pool)
    .await
    .expect("seed measure");
    sqlx::query(
        "INSERT INTO dataflows
         (id, source_id, name, dimensions, measures, frequency, license, attribution, source_url)
         VALUES ('apra.super_asset_allocation', 'apra', 'Super allocation', ARRAY['category'],
                 ARRAY['manual.percent'], 'quarterly',
                 'Creative Commons Attribution 3.0 Australia Licence', 'Source: APRA',
                 'https://apra.gov.au/super')",
    )
    .execute(pool)
    .await
    .expect("seed dataflow");
}

async fn schedule_enabled(pool: &PgPool) -> bool {
    sqlx::query_scalar("SELECT enabled FROM queue_cron_schedules WHERE id = 'source-apra-super'")
        .fetch_one(pool)
        .await
        .expect("read schedule")
}

async fn seed_dead_letter(pool: &PgPool) -> i64 {
    let payload = serde_json::to_value(Job::discover_dataflow(
        SourceId::new("apra").unwrap(),
        DataflowId::new("apra.super_asset_allocation").unwrap(),
    ))
    .unwrap();
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO queue_jobs (stage, payload, status, attempts, max_attempts, last_error)
         VALUES ('discover', $1, 'dead', 5, 5, 'throttled') RETURNING id",
    )
    .bind(&payload)
    .fetch_one(pool)
    .await
    .expect("seed dead job");
    sqlx::query(
        "INSERT INTO queue_dead_letters
         (job_id, stage, payload, attempts, error_class, error_message)
         VALUES ($1, 'discover', $2, 5, 'Transient', 'throttled')",
    )
    .bind(id)
    .bind(payload)
    .execute(pool)
    .await
    .expect("seed dead letter");
    id
}

fn manual_input_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("au-kpis-manual-input-{nanos}.json"))
}

fn docker_available() -> bool {
    std::env::var_os("DOCKER_HOST").is_some()
        || std::path::Path::new("/var/run/docker.sock").exists()
}
