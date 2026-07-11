use std::{collections::BTreeMap, env, path::Path};

use anyhow::{Context, bail, ensure};
use au_kpis_db::{
    DiscoveredWorkInput, GenerationInput, GenerationStatus, ObservationStageRow, PgPool,
    StageDigest, append_observation_stage, begin_discovered_work_fetch, begin_ingestion_parse,
    complete_discovered_work_fetch, complete_ingestion_parse, create_ingestion_generation,
    persist_discovered_work, transition_ingestion_generation, upsert_artifact_record,
};
use au_kpis_domain::{
    Artifact, ArtifactId, CodeId, DataflowId, DimensionId, MeasureId, Observation,
    ObservationStatus, SeriesDescriptor, SeriesKey, SourceId, TimePrecision,
};
use au_kpis_queue::{ApalisPgQueue, Job, Queue};
use au_kpis_source_register::{SourceStatus, load_source_register};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use object_store::aws::AmazonS3Builder;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;
use url::Url;
use uuid::Uuid;

const MANUAL_DATAFLOWS: [&str; 2] = ["apra.super_asset_allocation", "curated.oversight_strength"];
const MANUAL_PARSER_VERSION: &str = "manual-json-v1";
const MANUAL_MAX_ROWS: usize = 100_000;

#[derive(Debug, Serialize)]
pub(super) struct SourceControlOutput {
    dataflow_id: String,
    paused: bool,
    schedules_updated: u64,
    audit_id: i64,
}

#[derive(Debug, Serialize)]
pub(super) struct QueueRetryOutput {
    job_id: i64,
    status: &'static str,
    audit_id: i64,
}

#[derive(Debug, Serialize)]
pub(super) struct ReparseOutput {
    artifact_id: String,
    dataflow_id: String,
    parser_version: String,
    generation_id: Uuid,
    queue_job_id: i64,
    audit_id: i64,
}

#[derive(Debug, Serialize)]
pub(super) struct ManualLoadOutput {
    artifact_id: String,
    generation_id: Uuid,
    queue_job_id: Option<i64>,
    rows_staged: usize,
    status: String,
    audit_id: i64,
}

#[derive(Debug, Clone)]
pub(super) struct ManualReview<'a> {
    pub dataflow: &'a str,
    pub source_url: &'a str,
    pub license: &'a str,
    pub retrieved_at: NaiveDate,
    pub reviewer_role: &'a str,
    pub reviewed_at: NaiveDate,
    pub evidence_notes: &'a str,
    pub actor: &'a str,
    pub reason: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManualInputDocument {
    measure_id: MeasureId,
    unit: String,
    observations: Vec<ManualInputObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManualInputObservation {
    dimensions: BTreeMap<DimensionId, CodeId>,
    time: DateTime<Utc>,
    time_precision: TimePrecision,
    value: Option<f64>,
    #[serde(default = "normal_status")]
    status: ObservationStatus,
    #[serde(default)]
    attributes: BTreeMap<String, String>,
}

const fn normal_status() -> ObservationStatus {
    ObservationStatus::Normal
}

pub(super) async fn set_source_control(
    pool: &PgPool,
    dataflow: &str,
    actor: &str,
    reason: &str,
    paused: bool,
) -> anyhow::Result<SourceControlOutput> {
    let dataflow = DataflowId::new(dataflow).context("invalid dataflow id")?;
    require_audit(actor, reason)?;
    let mut tx = pool.begin().await.context("begin source control")?;
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dataflows WHERE id = $1)")
        .bind(dataflow.as_str())
        .fetch_one(&mut *tx)
        .await
        .context("validate source dataflow")?;
    ensure!(exists, "dataflow `{dataflow}` does not exist");
    sqlx::query(
        r#"INSERT INTO source_dataflow_controls
           (dataflow_id, paused, actor, reason, paused_at, resumed_at)
           VALUES ($1, $2, $3, $4,
                   CASE WHEN $2 THEN now() ELSE NULL END,
                   CASE WHEN $2 THEN NULL ELSE now() END)
           ON CONFLICT (dataflow_id) DO UPDATE
           SET paused = EXCLUDED.paused,
               actor = EXCLUDED.actor,
               reason = EXCLUDED.reason,
               paused_at = CASE
                   WHEN EXCLUDED.paused AND NOT source_dataflow_controls.paused THEN now()
                   WHEN EXCLUDED.paused THEN source_dataflow_controls.paused_at
                   ELSE source_dataflow_controls.paused_at
               END,
               resumed_at = CASE WHEN EXCLUDED.paused THEN NULL ELSE now() END,
               updated_at = now()"#,
    )
    .bind(dataflow.as_str())
    .bind(paused)
    .bind(actor)
    .bind(reason)
    .execute(&mut *tx)
    .await
    .context("persist source control")?;
    let schedules = sqlx::query(
        "UPDATE queue_cron_schedules
         SET enabled = $2,
             next_run_at = CASE WHEN $2 THEN LEAST(next_run_at, now()) ELSE next_run_at END,
             updated_at = now()
         WHERE payload #>> '{kind,dataflow_id}' = $1",
    )
    .bind(dataflow.as_str())
    .bind(!paused)
    .execute(&mut *tx)
    .await
    .context("update source schedules")?
    .rows_affected();
    let action = if paused {
        "source.pause"
    } else {
        "source.resume"
    };
    let audit_id = insert_audit(
        &mut tx,
        action,
        "dataflow",
        dataflow.as_str(),
        actor,
        reason,
        json!({"paused": paused, "schedules_updated": schedules}),
    )
    .await?;
    tx.commit().await.context("commit source control")?;
    Ok(SourceControlOutput {
        dataflow_id: dataflow.to_string(),
        paused,
        schedules_updated: schedules,
        audit_id,
    })
}

pub(super) async fn retry_dead_letter(
    pool: &PgPool,
    job_id: i64,
    actor: &str,
    reason: &str,
) -> anyhow::Result<QueueRetryOutput> {
    ensure!(job_id > 0, "job id must be positive");
    require_audit(actor, reason)?;
    let mut tx = pool.begin().await.context("begin DLQ retry")?;
    let row = sqlx::query(
        "SELECT q.stage, q.payload, d.error_class, d.error_message
         FROM queue_jobs q
         JOIN queue_dead_letters d ON d.job_id = q.id
         WHERE q.id = $1 AND q.status = 'dead'
         FOR UPDATE OF q, d",
    )
    .bind(job_id)
    .fetch_optional(&mut *tx)
    .await
    .context("load dead-lettered job")?
    .ok_or_else(|| anyhow::anyhow!("job `{job_id}` is not dead-lettered"))?;
    let details = json!({
        "stage": row.try_get::<String, _>("stage")?,
        "payload": row.try_get::<Value, _>("payload")?,
        "prior_error_class": row.try_get::<String, _>("error_class")?,
        "prior_error_message": row.try_get::<String, _>("error_message")?,
    });
    sqlx::query(
        "UPDATE queue_jobs
         SET status = 'pending', attempts = 0, run_at = now(), locked_by = NULL,
             locked_at = NULL, lease_version = lease_version + 1,
             last_error = NULL, updated_at = now()
         WHERE id = $1 AND status = 'dead'",
    )
    .bind(job_id)
    .execute(&mut *tx)
    .await
    .context("reset dead-lettered job")?;
    sqlx::query("DELETE FROM queue_dead_letters WHERE job_id = $1")
        .bind(job_id)
        .execute(&mut *tx)
        .await
        .context("remove DLQ snapshot")?;
    sqlx::query("UPDATE queue_schedule_occurrences SET status = 'enqueued' WHERE job_id = $1")
        .bind(job_id)
        .execute(&mut *tx)
        .await
        .context("reset schedule occurrence")?;
    let audit_id = insert_audit(
        &mut tx,
        "queue.retry_dlq",
        "queue_job",
        &job_id.to_string(),
        actor,
        reason,
        details,
    )
    .await?;
    tx.commit().await.context("commit DLQ retry")?;
    Ok(QueueRetryOutput {
        job_id,
        status: "pending",
        audit_id,
    })
}

pub(super) async fn inspect_generation(pool: &PgPool, id: Uuid) -> anyhow::Result<Value> {
    sqlx::query_scalar(
        r#"SELECT jsonb_build_object(
             'id', g.id,
             'discovered_work_id', g.discovered_work_id,
             'artifact_fetch_id', g.artifact_fetch_id,
             'artifact_id', encode(f.artifact_id, 'hex'),
             'source_id', g.source_id,
             'dataflow_id', g.dataflow_id,
             'parser_version', g.parser_version,
             'transform_version', g.transform_version,
             'status', g.status,
             'parsed_count', g.parsed_count,
             'loaded_count', g.loaded_count,
             'error_count', g.error_count,
             'stage_digest', CASE WHEN g.stage_digest IS NULL THEN NULL ELSE encode(g.stage_digest, 'hex') END,
             'actor', g.actor,
             'reason', g.reason,
             'trace_parent', g.trace_parent,
             'created_at', g.created_at,
             'parsed_at', g.parsed_at,
             'published_at', g.published_at,
             'failed_at', g.failed_at,
             'parse_errors', COALESCE((
                 SELECT jsonb_agg(jsonb_build_object(
                     'kind', error_kind, 'message', error_message,
                     'row_context', row_context, 'created_at', created_at
                 ) ORDER BY created_at)
                 FROM parse_errors WHERE ingestion_generation_id = g.id
             ), '[]'::jsonb)
           )
           FROM ingestion_generations g
           JOIN artifact_fetches f ON f.id = g.artifact_fetch_id
           WHERE g.id = $1"#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .context("inspect generation")?
    .ok_or_else(|| anyhow::anyhow!("generation `{id}` not found"))
}

pub(super) async fn reparse_artifact(
    pool: &PgPool,
    artifact_hex: &str,
    dataflow: &str,
    parser_version: &str,
    actor: &str,
    reason: &str,
) -> anyhow::Result<ReparseOutput> {
    let artifact = ArtifactId::from_hex(artifact_hex).context("invalid artifact SHA-256")?;
    let dataflow = DataflowId::new(dataflow).context("invalid dataflow id")?;
    require_text("parser version", parser_version)?;
    require_audit(actor, reason)?;
    let prior = sqlx::query(
        r#"SELECT g.discovered_work_id, g.artifact_fetch_id, g.source_id,
                  g.transform_version, g.trace_parent
           FROM ingestion_generations g
           JOIN artifact_fetches f ON f.id = g.artifact_fetch_id
           WHERE f.artifact_id = $1 AND g.dataflow_id = $2
           ORDER BY g.created_at DESC, g.id DESC
           LIMIT 1"#,
    )
    .bind(artifact.digest().as_bytes().as_slice())
    .bind(dataflow.as_str())
    .fetch_optional(pool)
    .await
    .context("load prior artifact generation")?
    .ok_or_else(|| anyhow::anyhow!("artifact `{artifact}` has no generation for `{dataflow}`"))?;
    let source = SourceId::new(prior.try_get::<String, _>("source_id")?)?;
    let trace_parent: Option<String> = prior.try_get("trace_parent")?;
    let generation = create_ingestion_generation(
        pool,
        GenerationInput {
            discovered_work_id: prior.try_get("discovered_work_id")?,
            artifact_fetch_id: prior.try_get("artifact_fetch_id")?,
            source_id: &source,
            dataflow_id: &dataflow,
            parser_version,
            transform_version: &prior.try_get::<String, _>("transform_version")?,
            job_id: None,
            trace_parent: trace_parent.as_deref(),
            actor,
            reason: Some(reason),
        },
    )
    .await
    .context("create reparse generation")?;
    let queue_job = ApalisPgQueue::new(pool.clone())
        .push(
            Job::parse(generation.id)
                .with_max_attempts(3)
                .with_trace_parent(trace_parent.unwrap_or_else(new_trace_parent)),
        )
        .await
        .context("enqueue reparse generation")?;
    let audit_id = record_audit(
        pool,
        "artifact.reparse",
        "artifact",
        artifact_hex,
        actor,
        reason,
        json!({
            "dataflow_id": dataflow,
            "parser_version": parser_version,
            "generation_id": generation.id,
            "queue_job_id": queue_job.get(),
        }),
    )
    .await?;
    Ok(ReparseOutput {
        artifact_id: artifact.to_string(),
        dataflow_id: dataflow.to_string(),
        parser_version: parser_version.to_string(),
        generation_id: generation.id,
        queue_job_id: queue_job.get(),
        audit_id,
    })
}

pub(super) async fn load_manual_input(
    pool: &PgPool,
    file: &Path,
    review: ManualReview<'_>,
) -> anyhow::Result<ManualLoadOutput> {
    require_audit(review.actor, review.reason)?;
    require_text("license", review.license)?;
    require_text("reviewer role", review.reviewer_role)?;
    require_text("evidence notes", review.evidence_notes)?;
    ensure!(
        MANUAL_DATAFLOWS.contains(&review.dataflow),
        "manual input is limited to the two production-v1 blockers: {}",
        MANUAL_DATAFLOWS.join(", ")
    );
    ensure!(
        review.reviewed_at >= review.retrieved_at,
        "review date must not precede retrieval date"
    );
    let parsed_url = Url::parse(review.source_url).context("invalid manual source URL")?;
    ensure!(
        parsed_url.scheme() == "https",
        "manual source URL must use HTTPS"
    );
    let raw = tokio::fs::read(file)
        .await
        .with_context(|| format!("read manual input {}", file.display()))?;
    let document: ManualInputDocument =
        serde_json::from_slice(&raw).context("parse canonical manual input JSON")?;
    let (dataflow, source) = ensure_manual_catalog(pool, &review, &document).await?;
    validate_manual_document(pool, &document).await?;
    let canonical = serde_json::to_vec(&document).context("canonicalize manual input JSON")?;
    let blob_store = production_blob_store()?;
    let artifact_id = blob_store
        .put_artifact(Bytes::from(canonical.clone()))
        .await
        .context("store canonical manual artifact")?;
    let storage_key = StorageKey::canonical_for(&artifact_id).to_string();
    let fetched_at = Utc::now();
    let response_headers = BTreeMap::from([
        (
            "x-au-kpis-manual-reviewer-role".to_string(),
            vec![review.reviewer_role.to_string()],
        ),
        (
            "x-au-kpis-manual-reviewed-at".to_string(),
            vec![review.reviewed_at.to_string()],
        ),
        (
            "x-au-kpis-manual-retrieved-at".to_string(),
            vec![review.retrieved_at.to_string()],
        ),
    ]);
    let mut artifact = Artifact {
        id: artifact_id,
        fetch_id: None,
        source_id: source.clone(),
        source_url: review.source_url.to_string(),
        content_type: "application/vnd.au-kpis.manual-input+json".to_string(),
        response_headers,
        storage_key,
        size_bytes: canonical.len() as u64,
        fetched_at,
    };
    if let Some(fetch_id) = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM artifact_fetches
         WHERE artifact_id = $1 AND source_id = $2 AND source_url = $3
         ORDER BY id DESC LIMIT 1",
    )
    .bind(artifact_id.digest().as_bytes().as_slice())
    .bind(source.as_str())
    .bind(review.source_url)
    .fetch_optional(pool)
    .await
    .context("find existing manual artifact fetch")?
    {
        artifact.fetch_id = Some(fetch_id);
    } else {
        artifact = upsert_artifact_record(pool, &artifact)
            .await
            .context("record manual artifact provenance")?;
    }
    let fetch_id = artifact.fetch_id.context("manual artifact fetch id")?;
    let work = persist_discovered_work(
        pool,
        DiscoveredWorkInput {
            occurrence_id: None,
            source_id: &source,
            dataflow_id: &dataflow,
            source_url: review.source_url,
            upstream_revision: &artifact_id.to_string(),
            discovery_metadata: json!({
                "manual_input": true,
                "artifact_id": artifact_id,
                "reviewer_role": review.reviewer_role,
                "reviewed_at": review.reviewed_at,
            }),
        },
    )
    .await
    .context("persist manual discovered work")?;
    if matches!(work.status.as_str(), "pending_fetch" | "fetching") {
        begin_discovered_work_fetch(pool, work.id)
            .await
            .context("fence manual artifact fetch")?;
        complete_discovered_work_fetch(pool, work.id)
            .await
            .context("complete manual artifact fetch")?;
    }
    let generation = create_ingestion_generation(
        pool,
        GenerationInput {
            discovered_work_id: work.id,
            artifact_fetch_id: fetch_id,
            source_id: &source,
            dataflow_id: &dataflow,
            parser_version: MANUAL_PARSER_VERSION,
            transform_version: "identity-v1",
            job_id: None,
            trace_parent: None,
            actor: review.actor,
            reason: Some(review.reason),
        },
    )
    .await
    .context("create manual generation")?;
    let rows_staged =
        stage_manual_rows(pool, generation.id, &dataflow, artifact_id, &document).await?;

    sqlx::query(
        r#"INSERT INTO manual_input_reviews
           (generation_id, artifact_id, dataflow_id, source_url, license,
            retrieved_at, reviewer_role, reviewed_at, evidence_notes, actor, reason)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
           ON CONFLICT (generation_id) DO NOTHING"#,
    )
    .bind(generation.id)
    .bind(artifact_id.digest().as_bytes().as_slice())
    .bind(dataflow.as_str())
    .bind(review.source_url)
    .bind(review.license)
    .bind(review.retrieved_at)
    .bind(review.reviewer_role)
    .bind(review.reviewed_at)
    .bind(review.evidence_notes)
    .bind(review.actor)
    .bind(review.reason)
    .execute(pool)
    .await
    .context("record manual review provenance")?;
    let status: String =
        sqlx::query_scalar("SELECT status FROM ingestion_generations WHERE id = $1")
            .bind(generation.id)
            .fetch_one(pool)
            .await
            .context("load staged manual generation state")?;
    let queue_job_id = if status == "published" {
        None
    } else {
        Some(
            ApalisPgQueue::new(pool.clone())
                .push(Job::load(dataflow.clone(), generation.id).with_max_attempts(5))
                .await
                .context("enqueue manual generation load")?
                .get(),
        )
    };
    let audit_id = record_audit(
        pool,
        "manual_input.load",
        "generation",
        &generation.id.to_string(),
        review.actor,
        review.reason,
        json!({
            "artifact_id": artifact_id,
            "dataflow_id": dataflow,
            "rows_staged": rows_staged,
            "reviewer_role": review.reviewer_role,
            "reviewed_at": review.reviewed_at,
            "queue_job_id": queue_job_id,
            "status": status,
        }),
    )
    .await?;
    Ok(ManualLoadOutput {
        artifact_id: artifact_id.to_string(),
        generation_id: generation.id,
        queue_job_id,
        rows_staged,
        status,
        audit_id,
    })
}

async fn ensure_manual_catalog(
    pool: &PgPool,
    review: &ManualReview<'_>,
    document: &ManualInputDocument,
) -> anyhow::Result<(DataflowId, SourceId)> {
    let register = load_source_register().context("load manual-input source governance")?;
    let entry = register
        .dataflows
        .iter()
        .find(|entry| entry.dataflow_id == review.dataflow)
        .ok_or_else(|| anyhow::anyhow!("manual dataflow `{}` is not governed", review.dataflow))?;
    ensure!(
        entry.status == SourceStatus::ManualPending,
        "manual dataflow `{}` is not manual_pending in the source register",
        review.dataflow
    );
    ensure!(
        entry.license == review.license,
        "manual licence does not match governed licence `{}`",
        entry.license
    );
    let dataflow = DataflowId::new(&entry.dataflow_id)?;
    let source = SourceId::new(&entry.source_id)?;
    let canonical_url = Url::parse(&entry.canonical_url).context("invalid governed source URL")?;
    let homepage = format!(
        "{}://{}",
        canonical_url.scheme(),
        canonical_url
            .host_str()
            .context("governed source URL has no host")?
    );
    let dimensions = document
        .observations
        .iter()
        .flat_map(|observation| observation.dimensions.keys())
        .map(|dimension| dimension.as_str().to_string())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut tx = pool.begin().await.context("begin manual catalog seed")?;
    sqlx::query(
        "INSERT INTO sources (id, name, homepage, description)
         VALUES ($1, $2, $3, 'Reviewed manual source')
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(source.as_str())
    .bind(entry.source_id.to_uppercase())
    .bind(homepage)
    .execute(&mut *tx)
    .await
    .context("seed manual source")?;
    sqlx::query(
        "INSERT INTO measures (id, name, description, unit)
         VALUES ($1, $2, 'Reviewed manual input measure', $3)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(document.measure_id.as_str())
    .bind(document.measure_id.as_str())
    .bind(&document.unit)
    .execute(&mut *tx)
    .await
    .context("seed manual measure")?;
    sqlx::query(
        r#"INSERT INTO dataflows
           (id, source_id, name, description, dimensions, measures, frequency,
            license, attribution, source_url)
           VALUES ($1, $2, $3, 'Reviewed production-v1 manual input', $4,
                   ARRAY[$5]::text[], $6, $7, $8, $9)
           ON CONFLICT (id) DO NOTHING"#,
    )
    .bind(dataflow.as_str())
    .bind(source.as_str())
    .bind(entry.dataflow_id.replace(['.', '_'], " "))
    .bind(dimensions)
    .bind(document.measure_id.as_str())
    .bind(database_frequency(&entry.cadence))
    .bind(&entry.license)
    .bind(&entry.attribution)
    .bind(&entry.canonical_url)
    .execute(&mut *tx)
    .await
    .context("seed manual dataflow")?;
    tx.commit().await.context("commit manual catalog seed")?;

    let catalog: (String, String) =
        sqlx::query_as("SELECT source_id, license FROM dataflows WHERE id = $1")
            .bind(dataflow.as_str())
            .fetch_one(pool)
            .await
            .context("load manual dataflow catalog")?;
    ensure!(
        catalog.0 == source.as_str() && catalog.1 == review.license,
        "existing manual catalog ownership or licence does not match governed input"
    );
    Ok((dataflow, source))
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

async fn validate_manual_document(
    pool: &PgPool,
    document: &ManualInputDocument,
) -> anyhow::Result<()> {
    require_text("unit", &document.unit)?;
    ensure!(
        !document.observations.is_empty(),
        "manual input has no observations"
    );
    ensure!(
        document.observations.len() <= MANUAL_MAX_ROWS,
        "manual input has {} rows, maximum is {MANUAL_MAX_ROWS}",
        document.observations.len()
    );
    let measure_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM measures WHERE id = $1)")
            .bind(document.measure_id.as_str())
            .fetch_one(pool)
            .await
            .context("validate manual measure")?;
    ensure!(
        measure_exists,
        "manual measure `{}` does not exist",
        document.measure_id
    );
    for observation in &document.observations {
        ensure!(
            observation.value.is_none_or(f64::is_finite),
            "manual input contains a non-finite value"
        );
        ensure!(
            observation.value.is_some() || observation.status == ObservationStatus::Missing,
            "only missing observations may have null values"
        );
    }
    Ok(())
}

async fn stage_manual_rows(
    pool: &PgPool,
    generation_id: Uuid,
    dataflow: &DataflowId,
    artifact_id: ArtifactId,
    document: &ManualInputDocument,
) -> anyhow::Result<usize> {
    let status: String =
        sqlx::query_scalar("SELECT status FROM ingestion_generations WHERE id = $1")
            .bind(generation_id)
            .fetch_one(pool)
            .await
            .context("load manual generation state")?;
    if matches!(status.as_str(), "pending_load" | "loading" | "published") {
        return Ok(document.observations.len());
    }
    ensure!(
        matches!(status.as_str(), "pending_parse" | "failed"),
        "manual generation `{generation_id}` is in unsupported state `{status}`"
    );
    begin_ingestion_parse(pool, generation_id)
        .await
        .context("begin manual parse stage")?;
    let ingested_at = Utc::now();
    let rows = document
        .observations
        .iter()
        .map(|input| {
            let series_key = SeriesKey::derive(
                dataflow,
                &document.measure_id,
                input
                    .dimensions
                    .iter()
                    .map(|(dimension, code)| (dimension.as_str(), code.as_str())),
            );
            let series = SeriesDescriptor {
                series_key,
                dataflow_id: dataflow.clone(),
                measure_id: document.measure_id.clone(),
                dimensions: input.dimensions.clone(),
                unit: document.unit.clone(),
            };
            let observation = Observation {
                series_key,
                time: input.time,
                time_precision: input.time_precision,
                value: input.value,
                status: input.status,
                revision_no: 0,
                attributes: input.attributes.clone(),
                ingested_at,
                source_artifact_id: artifact_id,
            };
            (series, observation)
        })
        .collect::<Vec<_>>();
    let mut digest = StageDigest::new();
    for (chunk_index, chunk) in rows.chunks(1_000).enumerate() {
        let base = chunk_index * 1_000;
        let staged = chunk
            .iter()
            .enumerate()
            .map(|(offset, (series, observation))| ObservationStageRow {
                row_no: (base + offset) as u64,
                series,
                observation,
            })
            .collect::<Vec<_>>();
        for row in &staged {
            digest.update(*row).context("digest manual stage row")?;
        }
        append_observation_stage(pool, generation_id, &staged)
            .await
            .context("append manual observation stage")?;
    }
    complete_ingestion_parse(
        pool,
        generation_id,
        rows.len() as u64,
        0,
        digest.finalize(),
        false,
    )
    .await
    .context("complete manual parse stage")?;
    transition_ingestion_generation(
        pool,
        generation_id,
        GenerationStatus::ParsedClean,
        GenerationStatus::PendingLoad,
    )
    .await
    .context("queue manual generation for load")?;
    Ok(rows.len())
}

fn production_blob_store() -> anyhow::Result<BlobStore> {
    let required =
        |name: &str| env::var(name).with_context(|| format!("{name} is required for manual input"));
    let endpoint = required("AU_KPIS_OBJECT_STORE__ENDPOINT")?;
    let bucket = required("AU_KPIS_OBJECT_STORE__BUCKET")?;
    let access_key = required("AU_KPIS_OBJECT_STORE__ACCESS_KEY_ID")?;
    let secret_key = required("AU_KPIS_OBJECT_STORE__SECRET_ACCESS_KEY")?;
    let region = env::var("AU_KPIS_OBJECT_STORE__REGION").unwrap_or_else(|_| "auto".to_string());
    let allow_http = env::var("AU_KPIS_OBJECT_STORE__ALLOW_HTTP")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes"));
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_region(region)
        .with_bucket_name(bucket)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_allow_http(allow_http)
        .with_virtual_hosted_style_request(false)
        .build()
        .context("build manual-input object store")?;
    Ok(BlobStore::new(store).with_delete_enabled(false))
}

async fn record_audit(
    pool: &PgPool,
    action: &str,
    target_type: &str,
    target_id: &str,
    actor: &str,
    reason: &str,
    details: Value,
) -> anyhow::Result<i64> {
    let mut tx = pool.begin().await.context("begin operator audit")?;
    let id = insert_audit(
        &mut tx,
        action,
        target_type,
        target_id,
        actor,
        reason,
        details,
    )
    .await?;
    tx.commit().await.context("commit operator audit")?;
    Ok(id)
}

async fn insert_audit(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    action: &str,
    target_type: &str,
    target_id: &str,
    actor: &str,
    reason: &str,
    details: Value,
) -> anyhow::Result<i64> {
    sqlx::query_scalar(
        "INSERT INTO operator_audit_log
         (action, target_type, target_id, actor, reason, details)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING id",
    )
    .bind(action)
    .bind(target_type)
    .bind(target_id)
    .bind(actor)
    .bind(reason)
    .bind(details)
    .fetch_one(&mut **tx)
    .await
    .context("write operator audit")
}

fn require_audit(actor: &str, reason: &str) -> anyhow::Result<()> {
    require_text("actor", actor)?;
    require_text("reason", reason)
}

fn require_text(field: &str, value: &str) -> anyhow::Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty")
    }
    Ok(())
}

fn new_trace_parent() -> String {
    format!(
        "00-{}-{}-01",
        Uuid::new_v4().simple(),
        &Uuid::new_v4().simple().to_string()[..16]
    )
}
