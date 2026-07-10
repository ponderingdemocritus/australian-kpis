use std::time::Duration;

use au_kpis_config::DatabaseConfig;
use au_kpis_db::{
    DiscoveredWorkInput, GenerationInput, GenerationStatus, ObservationStageRow, StageDigest,
    append_observation_stage, begin_ingestion_parse, complete_ingestion_parse, connect,
    create_ingestion_generation, migrate, persist_discovered_work, recover_lost_observation_stages,
    transition_ingestion_generation,
};
use au_kpis_domain::{
    ArtifactId, DataflowId, MeasureId, Observation, ObservationStatus, SeriesDescriptor, SeriesKey,
    Sha256Digest, SourceId, TimePrecision,
};
use au_kpis_testing::timescale::start_timescale;
use chrono::{TimeZone, Utc};
use serde_json::json;

async fn migrated_pool(
    database: &str,
) -> (au_kpis_testing::timescale::TimescaleHarness, sqlx::PgPool) {
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

async fn seed_provenance(pool: &sqlx::PgPool) -> i64 {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage)
         VALUES ('abs', 'ABS', 'https://www.abs.gov.au'),
                ('rba', 'RBA', 'https://www.rba.gov.au')",
    )
    .execute(pool)
    .await
    .expect("seed sources");
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
    sqlx::query(
        "INSERT INTO artifacts
            (id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at)
         VALUES
            (decode(repeat('11', 32), 'hex'), 'abs', 'https://example.test/releases?a=1&b=2',
             'application/json', '{}'::JSONB, 10,
             'artifacts/11/1111', now())",
    )
    .execute(pool)
    .await
    .expect("seed artifact");
    sqlx::query_scalar(
        "INSERT INTO artifact_fetches
            (artifact_id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at)
         VALUES
            (decode(repeat('11', 32), 'hex'), 'abs', 'https://example.test/releases?a=1&b=2',
             'application/json', '{}'::JSONB, 10,
             'artifacts/11/1111', now())
         RETURNING id",
    )
    .fetch_one(pool)
    .await
    .expect("seed fetch")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_work_and_generation_are_idempotent_owned_and_recoverable() {
    let (_timescale, pool) = migrated_pool("au_kpis_durable_ingestion").await;
    let fetch_id = seed_provenance(&pool).await;
    let source = SourceId::new("abs").unwrap();
    let dataflow = DataflowId::new("abs.cpi").unwrap();

    let first = persist_discovered_work(
        &pool,
        DiscoveredWorkInput {
            occurrence_id: None,
            source_id: &source,
            dataflow_id: &dataflow,
            source_url: "https://EXAMPLE.test/releases?b=2&a=1#fragment",
            upstream_revision: "2026-Q2",
            discovery_metadata: json!({"first": true}),
        },
    )
    .await
    .expect("persist work");
    let replay = persist_discovered_work(
        &pool,
        DiscoveredWorkInput {
            occurrence_id: None,
            source_id: &source,
            dataflow_id: &dataflow,
            source_url: "https://example.test/releases?a=1&b=2",
            upstream_revision: "2026-Q2",
            discovery_metadata: json!({"replayed": true}),
        },
    )
    .await
    .expect("replay work");
    assert_eq!(first.id, replay.id);
    assert_eq!(first.identity_key, replay.identity_key);

    let input = GenerationInput {
        discovered_work_id: first.id,
        artifact_fetch_id: fetch_id,
        source_id: &source,
        dataflow_id: &dataflow,
        parser_version: "abs-cpi-v1",
        transform_version: "identity-v1",
        job_id: None,
        trace_parent: None,
        actor: "test",
        reason: None,
    };
    let generation = create_ingestion_generation(&pool, input.clone())
        .await
        .expect("create generation");
    let replayed_generation = create_ingestion_generation(&pool, input)
        .await
        .expect("replay generation");
    assert_eq!(generation.id, replayed_generation.id);

    let rba = SourceId::new("rba").unwrap();
    let ownership_error = create_ingestion_generation(
        &pool,
        GenerationInput {
            discovered_work_id: first.id,
            artifact_fetch_id: fetch_id,
            source_id: &rba,
            dataflow_id: &dataflow,
            parser_version: "other-parser",
            transform_version: "identity-v1",
            job_id: None,
            trace_parent: None,
            actor: "test",
            reason: None,
        },
    )
    .await;
    assert!(
        ownership_error.is_err(),
        "work ownership must be database-enforced"
    );

    let invalid = transition_ingestion_generation(
        &pool,
        generation.id,
        GenerationStatus::PendingParse,
        GenerationStatus::Published,
    )
    .await;
    assert!(invalid.is_err(), "state skips must be rejected");

    transition_ingestion_generation(
        &pool,
        generation.id,
        GenerationStatus::PendingParse,
        GenerationStatus::Parsing,
    )
    .await
    .expect("begin parsing");
    transition_ingestion_generation(
        &pool,
        generation.id,
        GenerationStatus::Parsing,
        GenerationStatus::ParsedClean,
    )
    .await
    .expect("finish parsing");
    transition_ingestion_generation(
        &pool,
        generation.id,
        GenerationStatus::ParsedClean,
        GenerationStatus::PendingLoad,
    )
    .await
    .expect("queue load");

    assert_eq!(recover_lost_observation_stages(&pool).await.unwrap(), 1);
    let status: String =
        sqlx::query_scalar("SELECT status FROM ingestion_generations WHERE id = $1")
            .bind(generation.id)
            .fetch_one(&pool)
            .await
            .expect("load recovered status");
    assert_eq!(status, "pending_parse");

    begin_ingestion_parse(&pool, generation.id)
        .await
        .expect("restart parse");
    let measure_id = MeasureId::new("index").unwrap();
    let series_key = SeriesKey::derive(&dataflow, &measure_id, std::iter::empty::<(&str, &str)>());
    let series = SeriesDescriptor {
        series_key,
        dataflow_id: dataflow.clone(),
        measure_id,
        dimensions: Default::default(),
        unit: "index".to_string(),
    };
    let observation = Observation {
        series_key,
        time: Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap(),
        time_precision: TimePrecision::Quarter,
        value: Some(140.2),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes: Default::default(),
        ingested_at: Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap(),
        source_artifact_id: ArtifactId::from_digest(Sha256Digest::from_bytes([0x11; 32])),
    };
    let row = ObservationStageRow {
        row_no: 0,
        series: &series,
        observation: &observation,
    };
    let mut digest = StageDigest::new();
    digest.update(row).expect("hash staged row");
    let digest = digest.finalize();
    append_observation_stage(&pool, generation.id, &[row])
        .await
        .expect("append typed stage row");

    let count_mismatch = complete_ingestion_parse(&pool, generation.id, 2, 0, digest, false).await;
    assert!(
        count_mismatch.is_err(),
        "parse count must match staged rows"
    );
    let completed = complete_ingestion_parse(&pool, generation.id, 1, 0, digest, false)
        .await
        .expect("complete clean parse");
    assert_eq!(completed, GenerationStatus::ParsedClean);
    transition_ingestion_generation(
        &pool,
        generation.id,
        GenerationStatus::ParsedClean,
        GenerationStatus::PendingLoad,
    )
    .await
    .expect("queue persisted load");
    assert_eq!(
        recover_lost_observation_stages(&pool).await.unwrap(),
        0,
        "a generation with typed stage rows must not reset"
    );
}
