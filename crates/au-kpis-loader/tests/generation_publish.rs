use std::time::Duration;

use au_kpis_config::DatabaseConfig;
use au_kpis_db::{
    DiscoveredWorkInput, GenerationInput, GenerationStatus, ObservationStageRow, StageDigest,
    append_observation_stage, begin_ingestion_parse, complete_ingestion_parse, connect,
    create_ingestion_generation, migrate, persist_discovered_work, transition_ingestion_generation,
};
use au_kpis_domain::{
    ArtifactId, DataflowId, MeasureId, Observation, ObservationStatus, SeriesDescriptor, SeriesKey,
    Sha256Digest, SourceId, TimePrecision,
};
use au_kpis_loader::publish_ingestion_generation;
use au_kpis_testing::timescale::start_timescale;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::json;
use sqlx::{PgPool, Row};
use uuid::Uuid;

async fn migrated_pool(database: &str) -> (au_kpis_testing::timescale::TimescaleHarness, PgPool) {
    let timescale = start_timescale(database)
        .await
        .expect("start timescaledb container");
    let config = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let mut last_error = None;
    for _ in 0..10 {
        match connect(&config).await {
            Ok(pool) => {
                migrate(&pool).await.expect("apply migrations");
                return (timescale, pool);
            }
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    panic!("timescaledb did not accept connections: {last_error:?}");
}

async fn seed_catalog(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage)
         VALUES ('abs', 'ABS', 'https://www.abs.gov.au')",
    )
    .execute(pool)
    .await
    .expect("seed source");
    sqlx::query("INSERT INTO measures (id, name, unit) VALUES ('index', 'Index', 'index')")
        .execute(pool)
        .await
        .expect("seed measure");
    sqlx::query(
        "INSERT INTO dataflows
            (id, source_id, name, dimensions, measures, frequency, license, attribution, source_url)
         VALUES
            ('abs.cpi', 'abs', 'CPI', ARRAY[]::TEXT[], ARRAY['index'], 'quarterly',
             'CC-BY-4.0', 'Source: ABS', 'https://example.test/releases')",
    )
    .execute(pool)
    .await
    .expect("seed dataflow");
}

async fn seed_fetch(pool: &PgPool, byte: u8) -> (ArtifactId, i64) {
    let artifact_id = ArtifactId::from_digest(Sha256Digest::from_bytes([byte; 32]));
    let source_url = format!("https://example.test/releases/{byte}");
    let storage_key = format!("artifacts/{byte:02x}/{artifact_id}");
    sqlx::query(
        "INSERT INTO artifacts
            (id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at)
         VALUES ($1, 'abs', $2, 'application/json', '{}'::JSONB, 10, $3, now())",
    )
    .bind(artifact_id.digest().as_bytes().as_slice())
    .bind(&source_url)
    .bind(&storage_key)
    .execute(pool)
    .await
    .expect("seed artifact");
    let fetch_id = sqlx::query_scalar(
        "INSERT INTO artifact_fetches
            (artifact_id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at)
         VALUES ($1, 'abs', $2, 'application/json', '{}'::JSONB, 10, $3, now())
         RETURNING id",
    )
    .bind(artifact_id.digest().as_bytes().as_slice())
    .bind(source_url)
    .bind(storage_key)
    .fetch_one(pool)
    .await
    .expect("seed artifact fetch");
    (artifact_id, fetch_id)
}

async fn create_generation(
    pool: &PgPool,
    artifact_fetch_id: i64,
    revision: &str,
    parser_version: &str,
) -> Uuid {
    let source_id = SourceId::new("abs").unwrap();
    let dataflow_id = DataflowId::new("abs.cpi").unwrap();
    let work = persist_discovered_work(
        pool,
        DiscoveredWorkInput {
            occurrence_id: None,
            source_id: &source_id,
            dataflow_id: &dataflow_id,
            source_url: &format!("https://example.test/releases/{revision}"),
            upstream_revision: revision,
            discovery_metadata: json!({"revision": revision}),
        },
    )
    .await
    .expect("persist discovered work");
    create_ingestion_generation(
        pool,
        GenerationInput {
            discovered_work_id: work.id,
            artifact_fetch_id,
            source_id: &source_id,
            dataflow_id: &dataflow_id,
            parser_version,
            transform_version: "identity-v1",
            job_id: None,
            trace_parent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            actor: "test",
            reason: None,
        },
    )
    .await
    .expect("create generation")
    .id
}

fn series_descriptor() -> SeriesDescriptor {
    let dataflow_id = DataflowId::new("abs.cpi").unwrap();
    let measure_id = MeasureId::new("index").unwrap();
    let series_key = SeriesKey::derive(&dataflow_id, &measure_id, std::iter::empty());
    SeriesDescriptor {
        series_key,
        dataflow_id,
        measure_id,
        dimensions: Default::default(),
        unit: "index".to_string(),
    }
}

async fn stage_generation(
    pool: &PgPool,
    generation_id: Uuid,
    artifact_id: ArtifactId,
    time: DateTime<Utc>,
    value: f64,
) {
    begin_ingestion_parse(pool, generation_id)
        .await
        .expect("begin parsing");
    let series = series_descriptor();
    let observation = Observation {
        series_key: series.series_key,
        time,
        time_precision: TimePrecision::Quarter,
        value: Some(value),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes: Default::default(),
        ingested_at: Utc::now(),
        source_artifact_id: artifact_id,
    };
    let row = ObservationStageRow {
        row_no: 0,
        series: &series,
        observation: &observation,
    };
    let mut digest = StageDigest::new();
    digest.update(row).expect("digest staged row");
    append_observation_stage(pool, generation_id, &[row])
        .await
        .expect("append staged row");
    complete_ingestion_parse(pool, generation_id, 1, 0, digest.finalize(), false)
        .await
        .expect("complete parse");
    transition_ingestion_generation(
        pool,
        generation_id,
        GenerationStatus::ParsedClean,
        GenerationStatus::PendingLoad,
    )
    .await
    .expect("queue generation load");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publication_is_atomic_idempotent_and_serializes_revisions() {
    let (_timescale, pool) = migrated_pool("au_kpis_generation_publish").await;
    seed_catalog(&pool).await;
    let time_a = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();

    let (artifact_a, fetch_a) = seed_fetch(&pool, 0x21).await;
    let generation_a = create_generation(&pool, fetch_a, "2026-q1", "parser-v1").await;
    stage_generation(&pool, generation_a, artifact_a, time_a, 140.0).await;
    let published = publish_ingestion_generation(&pool, generation_a)
        .await
        .expect("publish first generation");
    assert_eq!(published.observations_loaded, 1);
    assert_eq!(published.series_upserted, 1);
    let replayed = publish_ingestion_generation(&pool, generation_a)
        .await
        .expect("replay published generation");
    assert_eq!(replayed.observations_loaded, 1);

    let parser_replay = create_generation(&pool, fetch_a, "2026-q1", "parser-v2").await;
    assert_ne!(parser_replay, generation_a);
    stage_generation(&pool, parser_replay, artifact_a, time_a, 140.0).await;
    let parser_publication = publish_ingestion_generation(&pool, parser_replay)
        .await
        .expect("publish new parser generation");
    assert_eq!(parser_publication.observations_loaded, 0);
    let published_generations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ingestion_generations WHERE status = 'published'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(published_generations, 2);

    let (artifact_failed, fetch_failed) = seed_fetch(&pool, 0x22).await;
    let failed_generation =
        create_generation(&pool, fetch_failed, "2026-q2-failure", "parser-v1").await;
    let failed_time = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    stage_generation(
        &pool,
        failed_generation,
        artifact_failed,
        failed_time,
        141.0,
    )
    .await;
    sqlx::query(
        "CREATE FUNCTION test_abort_generation_publication() RETURNS trigger
         LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced publication failure'; END $$",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER test_abort_generation_publication
         BEFORE INSERT OR UPDATE ON artifact_loads
         FOR EACH ROW EXECUTE FUNCTION test_abort_generation_publication()",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        publish_ingestion_generation(&pool, failed_generation)
            .await
            .is_err(),
        "failure after observation insertion must abort publication"
    );
    sqlx::query("DROP TRIGGER test_abort_generation_publication ON artifact_loads")
        .execute(&pool)
        .await
        .unwrap();
    let failed_status: String =
        sqlx::query_scalar("SELECT status FROM ingestion_generations WHERE id = $1")
            .bind(failed_generation)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(failed_status, "pending_load");
    let leaked_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM observations WHERE time = $1")
        .bind(failed_time)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(leaked_rows, 0);
    let staged_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM observation_stage WHERE generation_id = $1")
            .bind(failed_generation)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(staged_rows, 1, "rollback must preserve retryable staging");

    let concurrent_time = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap();
    let (artifact_b, fetch_b) = seed_fetch(&pool, 0x23).await;
    let (artifact_c, fetch_c) = seed_fetch(&pool, 0x24).await;
    let generation_b = create_generation(&pool, fetch_b, "2026-q3-b", "parser-v1").await;
    let generation_c = create_generation(&pool, fetch_c, "2026-q3-c", "parser-v1").await;
    stage_generation(&pool, generation_b, artifact_b, concurrent_time, 142.0).await;
    stage_generation(&pool, generation_c, artifact_c, concurrent_time, 143.0).await;
    let (result_b, result_c) = tokio::join!(
        publish_ingestion_generation(&pool, generation_b),
        publish_ingestion_generation(&pool, generation_c)
    );
    assert_eq!(result_b.unwrap().observations_loaded, 1);
    assert_eq!(result_c.unwrap().observations_loaded, 1);
    let revisions = sqlx::query(
        "SELECT revision_no, value, ingestion_generation_id
         FROM observations
         WHERE series_key = $1 AND time = $2
         ORDER BY revision_no",
    )
    .bind(
        series_descriptor()
            .series_key
            .digest()
            .as_bytes()
            .as_slice(),
    )
    .bind(concurrent_time)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(revisions.len(), 2);
    assert_eq!(revisions[0].get::<i32, _>("revision_no"), 0);
    assert_eq!(revisions[1].get::<i32, _>("revision_no"), 1);
    let values = revisions
        .iter()
        .map(|row| row.get::<Option<f64>, _>("value").unwrap())
        .collect::<Vec<_>>();
    assert!(values.contains(&142.0));
    assert!(values.contains(&143.0));
    assert!(revisions.iter().all(|row| {
        row.get::<Option<Uuid>, _>("ingestion_generation_id")
            .is_some()
    }));
}
