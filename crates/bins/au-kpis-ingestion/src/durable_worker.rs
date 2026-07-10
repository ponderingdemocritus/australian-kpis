use std::{fmt, time::Duration};

use au_kpis_adapter::{AdapterHttpClient, DiscoveredJob, DiscoveryCtx, ParseCtx};
use au_kpis_db::{
    DiscoveredWorkInput, GenerationInput, GenerationStageContext, GenerationStatus,
    ObservationStageRow, StageDigest, append_observation_stage, begin_discovered_work_fetch,
    begin_ingestion_parse, complete_discovered_work_fetch, complete_ingestion_parse,
    create_ingestion_generation, fail_ingestion_generation, get_discovered_work,
    get_ingestion_generation_context, persist_discovered_work, reject_discovered_work,
    reject_ingestion_generation, transition_ingestion_generation,
};
use au_kpis_domain::{DataflowId, Observation, SeriesDescriptor};
use au_kpis_error::{Classify, ErrorClass};
use au_kpis_ingestion_core::fetch_ctx;
use au_kpis_loader::{LoadError, publish_ingestion_generation};
use au_kpis_queue::{ApalisPgQueue, Job, JobKind, LeasedJob, Queue};
use au_kpis_source_register::{SourceStatus, ValidationPolicy, load_source_register};
use futures::StreamExt;
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

use super::Runtime;

const FETCH_MAX_ATTEMPTS: i32 = 8;
const PARSE_MAX_ATTEMPTS: i32 = 3;
const LOAD_MAX_ATTEMPTS: i32 = 5;
const STAGE_BATCH_ROWS: usize = 1_000;
const RECONCILE_LIMIT: i64 = 5_000;

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DurableJobStats {
    pub(super) discovered: u64,
    pub(super) fetched: u64,
    pub(super) parsed: u64,
    pub(super) loaded: u64,
}

#[derive(Debug)]
pub(super) struct DurableJobError {
    class: ErrorClass,
    retry_after: Option<Duration>,
    message: String,
}

impl DurableJobError {
    pub(super) const fn class(&self) -> ErrorClass {
        self.class
    }

    pub(super) const fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    fn classified(error: &impl Classify, message: impl Into<String>) -> Self {
        Self {
            class: error.class(),
            retry_after: error.retry_after(),
            message: message.into(),
        }
    }

    fn permanent(message: impl Into<String>) -> Self {
        Self {
            class: ErrorClass::Permanent,
            retry_after: None,
            message: message.into(),
        }
    }
}

impl fmt::Display for DurableJobError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for DurableJobError {}

pub(super) async fn process_job(
    runtime: &Runtime,
    queue: &ApalisPgQueue,
    leased: &LeasedJob,
) -> Result<DurableJobStats, DurableJobError> {
    match leased.job().kind() {
        JobKind::Discover {
            source_id,
            dataflow_id,
        }
        | JobKind::Backfill {
            source_id,
            dataflow_id,
        } => process_discover(runtime, queue, leased, source_id, dataflow_id.as_ref()).await,
        JobKind::Fetch { discovered_work_id } => {
            process_fetch(runtime, queue, leased, *discovered_work_id).await
        }
        JobKind::Parse { generation_id } => process_parse(runtime, queue, *generation_id).await,
        JobKind::Load {
            dataflow_id,
            generation_id,
        } => process_load(runtime, dataflow_id, *generation_id).await,
    }
}

pub(super) async fn reconcile_durable_jobs(
    runtime: &Runtime,
    queue: &ApalisPgQueue,
) -> Result<u64, DurableJobError> {
    let work_ids = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM discovered_work
         WHERE status IN ('pending_fetch', 'fetching')
         ORDER BY discovered_at, id
         LIMIT $1",
    )
    .bind(RECONCILE_LIMIT)
    .fetch_all(&runtime.db)
    .await
    .map_err(|error| DurableJobError::classified_db(&error, "list resumable Fetch work"))?;
    let mut enqueued = 0_u64;
    for work_id in work_ids {
        push_job(
            queue,
            Job::fetch(work_id).with_max_attempts(FETCH_MAX_ATTEMPTS),
        )
        .await?;
        enqueued += 1;
    }

    let generations = sqlx::query(
        "SELECT id, dataflow_id, status
         FROM ingestion_generations
         WHERE status IN (
             'pending_parse', 'parsing', 'parsed_clean', 'parsed_partial',
             'pending_load', 'loading', 'failed'
         )
         ORDER BY created_at, id
         LIMIT $1",
    )
    .bind(RECONCILE_LIMIT)
    .fetch_all(&runtime.db)
    .await
    .map_err(|error| DurableJobError::classified_db(&error, "list resumable generations"))?;
    for row in generations {
        let generation_id = row
            .try_get("id")
            .map_err(|error| DurableJobError::classified_db(&error, "decode generation id"))?;
        let dataflow_id =
            DataflowId::new(row.try_get::<String, _>("dataflow_id").map_err(|error| {
                DurableJobError::classified_db(&error, "decode generation dataflow")
            })?)
            .map_err(|error| DurableJobError::permanent(error.to_string()))?;
        let status: String = row
            .try_get("status")
            .map_err(|error| DurableJobError::classified_db(&error, "decode generation status"))?;
        ensure_generation_job(runtime, queue, generation_id, dataflow_id, &status).await?;
        enqueued += 1;
    }
    Ok(enqueued)
}

async fn process_discover(
    runtime: &Runtime,
    queue: &ApalisPgQueue,
    leased: &LeasedJob,
    source_id: &au_kpis_domain::SourceId,
    requested_dataflow: Option<&DataflowId>,
) -> Result<DurableJobStats, DurableJobError> {
    let adapter = runtime
        .adapters
        .get(source_id.as_str())
        .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;
    let trace_parent = leased
        .trace_parent()
        .map(str::to_owned)
        .unwrap_or_else(new_trace_parent);
    let http = AdapterHttpClient::new(adapter.manifest().rate_limit);
    let mut context =
        DiscoveryCtx::new(http, chrono::Utc::now()).with_trace_parent(trace_parent.clone());
    if let Some(dataflow_id) = requested_dataflow {
        context = context.with_requested_dataflow_id(dataflow_id.clone());
    }
    let jobs = match adapter.discover(&context).await {
        Ok(jobs) => jobs,
        Err(error) => {
            runtime.metrics.record_adapter_error(&error);
            return Err(DurableJobError::classified(&error, error.to_string()));
        }
    };
    let occurrence_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM queue_schedule_occurrences WHERE job_id = $1",
    )
    .bind(leased.id().get())
    .fetch_optional(&runtime.db)
    .await
    .map_err(|error| DurableJobError::classified_db(&error, "load schedule occurrence"))?;

    let mut discovered = 0_u64;
    for mut adapter_job in jobs {
        if adapter_job.source_id != *source_id {
            return Err(DurableJobError::permanent(format!(
                "Discover `{source_id}` emitted work owned by `{}`",
                adapter_job.source_id
            )));
        }
        if requested_dataflow.is_some_and(|expected| expected != &adapter_job.dataflow_id) {
            return Err(DurableJobError::permanent(format!(
                "Discover `{source_id}` emitted `{}` while scoped to `{}`",
                adapter_job.dataflow_id,
                requested_dataflow.expect("checked as some")
            )));
        }
        active_validation_policy(source_id.as_str(), adapter_job.dataflow_id.as_str())?;
        if adapter_job.trace_parent.is_none() {
            adapter_job.trace_parent = Some(trace_parent.clone());
        }
        let metadata = json!({"adapter_job": adapter_job});
        let work = persist_discovered_work(
            &runtime.db,
            DiscoveredWorkInput {
                occurrence_id,
                source_id,
                dataflow_id: &adapter_job.dataflow_id,
                source_url: &adapter_job.source_url,
                upstream_revision: &adapter_job.id,
                discovery_metadata: metadata,
            },
        )
        .await
        .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;
        if matches!(work.status.as_str(), "pending_fetch" | "fetching") {
            push_job(
                queue,
                Job::fetch(work.id)
                    .with_max_attempts(FETCH_MAX_ATTEMPTS)
                    .with_trace_parent(trace_parent.clone()),
            )
            .await?;
        } else if work.status == "fetched" {
            enqueue_generation_for_work(runtime, queue, work.id).await?;
        }
        discovered += 1;
    }
    Ok(DurableJobStats {
        discovered,
        ..DurableJobStats::default()
    })
}

async fn process_fetch(
    runtime: &Runtime,
    queue: &ApalisPgQueue,
    leased: &LeasedJob,
    work_id: Uuid,
) -> Result<DurableJobStats, DurableJobError> {
    let work = get_discovered_work(&runtime.db, work_id)
        .await
        .map_err(|error| DurableJobError::classified(&error, error.to_string()))?
        .ok_or_else(|| {
            DurableJobError::permanent(format!("discovered work `{work_id}` not found"))
        })?;
    if !begin_discovered_work_fetch(&runtime.db, work_id)
        .await
        .map_err(|error| DurableJobError::classified(&error, error.to_string()))?
    {
        enqueue_generation_for_work(runtime, queue, work_id).await?;
        return Ok(DurableJobStats::default());
    }
    let adapter_job = match adapter_job(&work.discovery_metadata) {
        Ok(job) => job,
        Err(error) => {
            reject_work(runtime, work_id, &error.to_string()).await?;
            return Err(error);
        }
    };
    if adapter_job.source_id != work.source_id || adapter_job.dataflow_id != work.dataflow_id {
        return Err(DurableJobError::permanent(format!(
            "discovered work `{work_id}` adapter metadata ownership mismatch"
        )));
    }
    let adapter = runtime
        .adapters
        .get(work.source_id.as_str())
        .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;
    let context = fetch_ctx(
        AdapterHttpClient::new(adapter.manifest().rate_limit),
        runtime.blob_store.clone(),
        chrono::Utc::now(),
        runtime.db.clone(),
    )
    .with_cancellation(runtime.shutdown.child_token());
    let artifact = match adapter.fetch(adapter_job, &context).await {
        Ok(artifact) => artifact,
        Err(error) => {
            runtime.metrics.record_adapter_error(&error);
            if !error.class().is_retryable() {
                reject_work(runtime, work_id, &error.to_string()).await?;
            }
            return Err(DurableJobError::classified(&error, error.to_string()));
        }
    };
    if artifact.source_id != work.source_id {
        let error = DurableJobError::permanent(format!(
            "Fetch `{work_id}` returned artifact owned by `{}`",
            artifact.source_id
        ));
        reject_work(runtime, work_id, &error.to_string()).await?;
        return Err(error);
    }
    let fetch_id = match artifact.fetch_id {
        Some(fetch_id) => fetch_id,
        None => {
            let error = DurableJobError::permanent(format!(
                "Fetch `{work_id}` did not persist fetch provenance"
            ));
            reject_work(runtime, work_id, &error.to_string()).await?;
            return Err(error);
        }
    };
    let generation_result = create_ingestion_generation(
        &runtime.db,
        GenerationInput {
            discovered_work_id: work_id,
            artifact_fetch_id: fetch_id,
            source_id: &work.source_id,
            dataflow_id: &work.dataflow_id,
            parser_version: &adapter.manifest().version,
            transform_version: "identity-v1",
            job_id: Some(leased.id().get()),
            trace_parent: leased.trace_parent(),
            actor: "system",
            reason: None,
        },
    )
    .await;
    let generation = match generation_result {
        Ok(generation) => generation,
        Err(error) => {
            if !error.class().is_retryable() {
                reject_work(runtime, work_id, &error.to_string()).await?;
            }
            return Err(DurableJobError::classified(&error, error.to_string()));
        }
    };
    complete_discovered_work_fetch(&runtime.db, work_id)
        .await
        .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;
    ensure_generation_job(
        runtime,
        queue,
        generation.id,
        generation.dataflow_id,
        &generation.status.to_string(),
    )
    .await?;
    Ok(DurableJobStats {
        fetched: 1,
        ..DurableJobStats::default()
    })
}

async fn process_parse(
    runtime: &Runtime,
    queue: &ApalisPgQueue,
    generation_id: Uuid,
) -> Result<DurableJobStats, DurableJobError> {
    let context = generation_context(runtime, generation_id).await?;
    match context.status {
        GenerationStatus::Published | GenerationStatus::Rejected => {
            return Ok(DurableJobStats::default());
        }
        GenerationStatus::ParsedClean | GenerationStatus::ParsedPartial => {
            transition_ingestion_generation(
                &runtime.db,
                generation_id,
                context.status,
                GenerationStatus::PendingLoad,
            )
            .await
            .map_err(|error| DurableJobError::permanent(error.to_string()))?;
            push_load(
                queue,
                &context.dataflow_id,
                generation_id,
                context.trace_parent.as_deref(),
            )
            .await?;
            return Ok(DurableJobStats::default());
        }
        GenerationStatus::PendingLoad | GenerationStatus::Loading => {
            push_load(
                queue,
                &context.dataflow_id,
                generation_id,
                context.trace_parent.as_deref(),
            )
            .await?;
            return Ok(DurableJobStats::default());
        }
        GenerationStatus::Parsing => {
            fail_ingestion_generation(&runtime.db, generation_id, "reclaimed parser lease")
                .await
                .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;
        }
        GenerationStatus::PendingParse | GenerationStatus::Failed => {}
    }
    begin_ingestion_parse(&runtime.db, generation_id)
        .await
        .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;

    let adapter = runtime
        .adapters
        .get(context.source_id.as_str())
        .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;
    let adapter_job = adapter_job(&context.discovery_metadata)?;
    let parse_context = ParseCtx::new(
        AdapterHttpClient::new(adapter.manifest().rate_limit),
        runtime.blob_store.clone(),
        chrono::Utc::now(),
    )
    .with_expected_dataflow(context.dataflow_id.clone(), adapter_job.metadata)
    .with_job_correlation(adapter_job.id, context.trace_parent.clone())
    .with_cancellation(runtime.shutdown.child_token());
    let mut stream = adapter.parse(context.artifact.clone().into(), &parse_context);
    let mut digest = StageDigest::new();
    let mut batch = Vec::<(SeriesDescriptor, Observation)>::with_capacity(STAGE_BATCH_ROWS);
    let mut row_no = 0_u64;

    while let Some(result) = stream.next().await {
        match result {
            Ok((series, observation)) => {
                batch.push((series, observation));
                if batch.len() == STAGE_BATCH_ROWS {
                    flush_stage_batch(runtime, generation_id, &mut row_no, &mut digest, &mut batch)
                        .await?;
                }
            }
            Err(error) => {
                runtime.metrics.record_adapter_error(&error);
                let class = error.class();
                record_generation_parse_error(runtime, &context, &error.to_string()).await?;
                finish_failed_parse(runtime, generation_id, class, &error.to_string()).await?;
                return Err(DurableJobError::classified(&error, error.to_string()));
            }
        }
    }
    flush_stage_batch(runtime, generation_id, &mut row_no, &mut digest, &mut batch).await?;
    if row_no == 0 {
        let message = "parser emitted zero observations";
        record_generation_parse_error(runtime, &context, message).await?;
        pause_generation_dataflow(runtime, generation_id, message).await?;
        reject_ingestion_generation(&runtime.db, generation_id, message)
            .await
            .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;
        return Err(DurableJobError::permanent(message));
    }
    let validation =
        active_validation_policy(context.source_id.as_str(), context.dataflow_id.as_str())?;
    let staged_series: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT series_key) FROM observation_stage WHERE generation_id = $1",
    )
    .bind(generation_id)
    .fetch_one(&runtime.db)
    .await
    .map_err(|error| DurableJobError::classified_db(&error, "count staged series"))?;
    if staged_series < 0 || staged_series as u64 > validation.max_series_cardinality {
        let message = format!(
            "generation emitted {staged_series} series, maximum is {}",
            validation.max_series_cardinality
        );
        record_generation_parse_error(runtime, &context, &message).await?;
        pause_generation_dataflow(runtime, generation_id, &message).await?;
        reject_ingestion_generation(&runtime.db, generation_id, &message)
            .await
            .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;
        return Err(DurableJobError::permanent(message));
    }
    complete_ingestion_parse(
        &runtime.db,
        generation_id,
        row_no,
        0,
        digest.finalize(),
        false,
    )
    .await
    .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;
    transition_ingestion_generation(
        &runtime.db,
        generation_id,
        GenerationStatus::ParsedClean,
        GenerationStatus::PendingLoad,
    )
    .await
    .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;
    push_load(
        queue,
        &context.dataflow_id,
        generation_id,
        context.trace_parent.as_deref(),
    )
    .await?;
    Ok(DurableJobStats {
        parsed: row_no,
        ..DurableJobStats::default()
    })
}

async fn process_load(
    runtime: &Runtime,
    expected_dataflow: &DataflowId,
    generation_id: Uuid,
) -> Result<DurableJobStats, DurableJobError> {
    let context = generation_context(runtime, generation_id).await?;
    if &context.dataflow_id != expected_dataflow {
        return Err(DurableJobError::permanent(format!(
            "Load generation `{generation_id}` belongs to `{}`, not `{expected_dataflow}`",
            context.dataflow_id
        )));
    }
    if context.status == GenerationStatus::Published {
        return Ok(DurableJobStats::default());
    }
    let published = publish_ingestion_generation(&runtime.db, generation_id)
        .await
        .map_err(load_error)?;
    Ok(DurableJobStats {
        loaded: published.observations_loaded,
        ..DurableJobStats::default()
    })
}

async fn flush_stage_batch(
    runtime: &Runtime,
    generation_id: Uuid,
    row_no: &mut u64,
    digest: &mut StageDigest,
    batch: &mut Vec<(SeriesDescriptor, Observation)>,
) -> Result<(), DurableJobError> {
    if batch.is_empty() {
        return Ok(());
    }
    let rows = batch
        .iter()
        .enumerate()
        .map(|(offset, (series, observation))| ObservationStageRow {
            row_no: *row_no + offset as u64,
            series,
            observation,
        })
        .collect::<Vec<_>>();
    for row in &rows {
        digest
            .update(*row)
            .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;
    }
    append_observation_stage(&runtime.db, generation_id, &rows)
        .await
        .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;
    *row_no += rows.len() as u64;
    batch.clear();
    Ok(())
}

async fn finish_failed_parse(
    runtime: &Runtime,
    generation_id: Uuid,
    class: ErrorClass,
    reason: &str,
) -> Result<(), DurableJobError> {
    let result = if class.is_retryable() {
        fail_ingestion_generation(&runtime.db, generation_id, reason).await
    } else {
        pause_generation_dataflow(runtime, generation_id, reason).await?;
        reject_ingestion_generation(&runtime.db, generation_id, reason).await
    };
    result.map_err(|error| DurableJobError::classified(&error, error.to_string()))
}

async fn pause_generation_dataflow(
    runtime: &Runtime,
    generation_id: Uuid,
    reason: &str,
) -> Result<(), DurableJobError> {
    let mut transaction =
        runtime.db.begin().await.map_err(|error| {
            DurableJobError::classified_db(&error, "begin automatic source pause")
        })?;
    let dataflow_id: String = sqlx::query_scalar(
        "SELECT dataflow_id FROM ingestion_generations WHERE id = $1 FOR UPDATE",
    )
    .bind(generation_id)
    .fetch_one(&mut *transaction)
    .await
    .map_err(|error| DurableJobError::classified_db(&error, "load generation for source pause"))?;
    sqlx::query(
        r#"INSERT INTO source_dataflow_controls
           (dataflow_id, paused, actor, reason, paused_at, resumed_at)
           VALUES ($1, true, 'system:ingestion', $2, now(), NULL)
           ON CONFLICT (dataflow_id) DO UPDATE
           SET paused = true,
               actor = EXCLUDED.actor,
               reason = EXCLUDED.reason,
               paused_at = CASE
                   WHEN source_dataflow_controls.paused
                   THEN source_dataflow_controls.paused_at
                   ELSE now()
               END,
               resumed_at = NULL,
               updated_at = now()"#,
    )
    .bind(&dataflow_id)
    .bind(reason)
    .execute(&mut *transaction)
    .await
    .map_err(|error| DurableJobError::classified_db(&error, "pause failed dataflow"))?;
    sqlx::query(
        "UPDATE queue_cron_schedules
         SET enabled = false, updated_at = now()
         WHERE payload #>> '{kind,dataflow_id}' = $1",
    )
    .bind(&dataflow_id)
    .execute(&mut *transaction)
    .await
    .map_err(|error| DurableJobError::classified_db(&error, "disable failed dataflow schedule"))?;
    sqlx::query(
        r#"INSERT INTO operator_audit_log
           (action, target_type, target_id, actor, reason, details)
           VALUES ('source.auto_pause', 'dataflow', $1, 'system:ingestion', $2,
                   jsonb_build_object('generation_id', $3::text))"#,
    )
    .bind(&dataflow_id)
    .bind(reason)
    .bind(generation_id)
    .execute(&mut *transaction)
    .await
    .map_err(|error| DurableJobError::classified_db(&error, "audit automatic source pause"))?;
    transaction
        .commit()
        .await
        .map_err(|error| DurableJobError::classified_db(&error, "commit automatic source pause"))?;
    Ok(())
}

async fn record_generation_parse_error(
    runtime: &Runtime,
    context: &GenerationStageContext,
    message: &str,
) -> Result<(), DurableJobError> {
    sqlx::query(
        "INSERT INTO parse_errors (
             artifact_id, error_kind, error_message, row_context, ingestion_generation_id
         )
         SELECT $1, 'adapter_parse', $2, $3, $4
         WHERE NOT EXISTS (
             SELECT 1 FROM parse_errors
             WHERE ingestion_generation_id = $4
               AND error_kind = 'adapter_parse'
               AND error_message = $2
         )",
    )
    .bind(context.artifact.id.digest().as_bytes().as_slice())
    .bind(message)
    .bind(json!({
        "dataflow_id": context.dataflow_id,
        "source_id": context.source_id,
        "artifact_id": context.artifact.id,
        "artifact_fetch_id": context.artifact.fetch_id,
        "generation_id": context.generation_id,
        "trace_parent": context.trace_parent,
        "fatal": true,
    }))
    .bind(context.generation_id)
    .execute(&runtime.db)
    .await
    .map_err(|error| DurableJobError::classified_db(&error, "record generation parse error"))?;
    Ok(())
}

async fn enqueue_generation_for_work(
    runtime: &Runtime,
    queue: &ApalisPgQueue,
    work_id: Uuid,
) -> Result<(), DurableJobError> {
    let rows = sqlx::query(
        "SELECT id, dataflow_id, status
         FROM ingestion_generations
         WHERE discovered_work_id = $1
         ORDER BY created_at DESC",
    )
    .bind(work_id)
    .fetch_all(&runtime.db)
    .await
    .map_err(|error| DurableJobError::classified_db(&error, "load work generations"))?;
    if rows.is_empty() {
        return Err(DurableJobError::permanent(format!(
            "fetched work `{work_id}` has no ingestion generation"
        )));
    }
    for row in rows {
        let generation_id = row
            .try_get("id")
            .map_err(|error| DurableJobError::classified_db(&error, "decode generation id"))?;
        let dataflow_id =
            DataflowId::new(row.try_get::<String, _>("dataflow_id").map_err(|error| {
                DurableJobError::classified_db(&error, "decode generation dataflow")
            })?)
            .map_err(|error| DurableJobError::permanent(error.to_string()))?;
        let status: String = row
            .try_get("status")
            .map_err(|error| DurableJobError::classified_db(&error, "decode generation status"))?;
        ensure_generation_job(runtime, queue, generation_id, dataflow_id, &status).await?;
    }
    Ok(())
}

async fn ensure_generation_job(
    runtime: &Runtime,
    queue: &ApalisPgQueue,
    generation_id: Uuid,
    dataflow_id: DataflowId,
    status: &str,
) -> Result<(), DurableJobError> {
    let context = get_ingestion_generation_context(&runtime.db, generation_id)
        .await
        .map_err(|error| DurableJobError::classified(&error, error.to_string()))?
        .ok_or_else(|| {
            DurableJobError::permanent(format!("generation `{generation_id}` not found"))
        })?;
    match status {
        "pending_parse" | "parsing" | "failed" => {
            let mut job = Job::parse(generation_id).with_max_attempts(PARSE_MAX_ATTEMPTS);
            if let Some(trace_parent) = context.trace_parent {
                job = job.with_trace_parent(trace_parent);
            }
            push_job(queue, job).await?;
        }
        "parsed_clean" | "parsed_partial" => {
            let current = if status == "parsed_clean" {
                GenerationStatus::ParsedClean
            } else {
                GenerationStatus::ParsedPartial
            };
            transition_ingestion_generation(
                &runtime.db,
                generation_id,
                current,
                GenerationStatus::PendingLoad,
            )
            .await
            .map_err(|error| DurableJobError::classified(&error, error.to_string()))?;
            push_load(
                queue,
                &dataflow_id,
                generation_id,
                context.trace_parent.as_deref(),
            )
            .await?;
        }
        "pending_load" | "loading" => {
            push_load(
                queue,
                &dataflow_id,
                generation_id,
                context.trace_parent.as_deref(),
            )
            .await?;
        }
        "published" | "rejected" => {}
        other => {
            return Err(DurableJobError::permanent(format!(
                "unknown generation status `{other}`"
            )));
        }
    }
    Ok(())
}

async fn push_load(
    queue: &ApalisPgQueue,
    dataflow_id: &DataflowId,
    generation_id: Uuid,
    trace_parent: Option<&str>,
) -> Result<(), DurableJobError> {
    let mut job =
        Job::load(dataflow_id.clone(), generation_id).with_max_attempts(LOAD_MAX_ATTEMPTS);
    if let Some(trace_parent) = trace_parent {
        job = job.with_trace_parent(trace_parent);
    }
    push_job(queue, job).await
}

async fn push_job(queue: &ApalisPgQueue, job: Job) -> Result<(), DurableJobError> {
    queue
        .push(job)
        .await
        .map(|_| ())
        .map_err(|error| DurableJobError::classified(&error, error.to_string()))
}

async fn generation_context(
    runtime: &Runtime,
    generation_id: Uuid,
) -> Result<GenerationStageContext, DurableJobError> {
    get_ingestion_generation_context(&runtime.db, generation_id)
        .await
        .map_err(|error| DurableJobError::classified(&error, error.to_string()))?
        .ok_or_else(|| {
            DurableJobError::permanent(format!("generation `{generation_id}` not found"))
        })
}

fn adapter_job(metadata: &Value) -> Result<DiscoveredJob, DurableJobError> {
    serde_json::from_value(
        metadata
            .get("adapter_job")
            .cloned()
            .ok_or_else(|| DurableJobError::permanent("discovered work has no adapter_job"))?,
    )
    .map_err(|error| DurableJobError::permanent(format!("invalid adapter_job metadata: {error}")))
}

fn active_validation_policy(
    source_id: &str,
    dataflow_id: &str,
) -> Result<ValidationPolicy, DurableJobError> {
    let register =
        load_source_register().map_err(|error| DurableJobError::permanent(error.to_string()))?;
    let entry = register
        .dataflows
        .into_iter()
        .find(|entry| entry.dataflow_id == dataflow_id)
        .ok_or_else(|| {
            DurableJobError::permanent(format!(
                "dataflow `{dataflow_id}` is outside the source register"
            ))
        })?;
    if entry.source_id != source_id || entry.status != SourceStatus::Active {
        return Err(DurableJobError::permanent(format!(
            "dataflow `{dataflow_id}` is not an active `{source_id}` source"
        )));
    }
    entry.validation_policy.ok_or_else(|| {
        DurableJobError::permanent(format!(
            "active dataflow `{dataflow_id}` has no validation policy"
        ))
    })
}

async fn reject_work(
    runtime: &Runtime,
    work_id: Uuid,
    reason: &str,
) -> Result<(), DurableJobError> {
    reject_discovered_work(&runtime.db, work_id, reason)
        .await
        .map_err(|error| DurableJobError::classified(&error, error.to_string()))
}

fn load_error(error: LoadError) -> DurableJobError {
    let class = match &error {
        LoadError::Db(_) => ErrorClass::Transient,
        LoadError::Durable(error) => error.class(),
        LoadError::Validation(_) | LoadError::Json(_) => ErrorClass::Permanent,
    };
    DurableJobError {
        class,
        retry_after: None,
        message: error.to_string(),
    }
}

fn new_trace_parent() -> String {
    let trace_id = Uuid::new_v4().as_u128();
    let span_id = Uuid::new_v4().as_u128() as u64;
    format!("00-{trace_id:032x}-{span_id:016x}-01")
}

impl DurableJobError {
    fn classified_db(error: &sqlx::Error, context: &str) -> Self {
        Self {
            class: ErrorClass::Transient,
            retry_after: None,
            message: format!("{context}: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use au_kpis_adapter::Adapters;
    use au_kpis_adapter_abs::AbsAdapter;
    use au_kpis_config::DatabaseConfig;
    use au_kpis_db::{connect, migrate};
    use au_kpis_domain::SourceId;
    use au_kpis_ingestion_core::PipelineOptions;
    use au_kpis_queue::{QueueStage, WorkerId};
    use au_kpis_storage::BlobStore;
    use au_kpis_testing::timescale::start_timescale;
    use object_store::memory::InMemory;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::WorkerMetrics;

    const DATAFLOW_LISTING: &str = r#"{
      "data": {
        "dataflows": [{
          "id": "CPI",
          "agencyID": "ABS",
          "version": "2.0.0",
          "name": "Consumer Price Index",
          "updated": "2026-04-28T00:00:00Z",
          "links": [
            { "href": "https://data.api.abs.gov.au/rest/dataflow/ABS/CPI/2.0.0", "rel": "self" }
          ]
        }]
      }
    }"#;
    const CPI_FIXTURE: &[u8] = include_bytes!("../../../adapters/abs/tests/fixtures/cpi_sdmx.json");

    #[test]
    fn adapter_job_round_trips_from_durable_metadata() {
        let expected = DiscoveredJob {
            id: "release-1".to_string(),
            source_id: SourceId::new("abs").unwrap(),
            dataflow_id: DataflowId::new("abs.cpi").unwrap(),
            source_url: "https://example.test/release".to_string(),
            trace_parent: Some(new_trace_parent()),
            metadata: BTreeMap::from([("sheet".to_string(), "CPI".to_string())]),
        };
        let metadata = json!({"adapter_job": expected});
        assert_eq!(adapter_job(&metadata).unwrap(), expected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn durable_stages_reconcile_and_publish_without_partial_visibility() {
        let timescale = start_timescale("au_kpis_durable_worker")
            .await
            .expect("start timescaledb container");
        let config = DatabaseConfig {
            url: timescale.url().to_string(),
        };
        let pool = connect_with_retry(&config).await;
        migrate(&pool).await.expect("apply migrations");
        seed_cpi_catalog(&pool).await;

        let base_url = serve_abs_cpi_once().await;
        let adapter = AbsAdapter::builder().base_url(base_url).build();
        let mut adapters = Adapters::builder();
        adapters.register(adapter).expect("register ABS adapter");
        let runtime = Runtime {
            adapters: adapters.build(),
            db: pool.clone(),
            blob_store: BlobStore::new(InMemory::new()),
            metrics: Arc::new(WorkerMetrics::default()),
            pipeline_options: PipelineOptions::default(),
            shutdown: CancellationToken::new(),
            poll_interval: Duration::from_millis(1),
            worker_id: "durable-worker-test".to_string(),
        };
        let queue = ApalisPgQueue::new(pool.clone());
        queue
            .push(Job::discover_dataflow(
                SourceId::new("abs").unwrap(),
                DataflowId::new("abs.cpi").unwrap(),
            ))
            .await
            .expect("enqueue discovery");

        let discover = pop(&queue, QueueStage::Discover).await;
        let discovered = process_job(&runtime, &queue, &discover)
            .await
            .expect("process Discover");
        assert_eq!(discovered.discovered, 1);
        queue.ack(&discover).await.expect("ack Discover");

        let fetch = pop(&queue, QueueStage::Fetch).await;
        let fetched = process_job(&runtime, &queue, &fetch)
            .await
            .expect("process Fetch");
        assert_eq!(fetched.fetched, 1);
        queue.ack(&fetch).await.expect("ack Fetch");

        let generation_id: Uuid =
            sqlx::query_scalar("SELECT id FROM ingestion_generations LIMIT 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        sqlx::query(
            "DELETE FROM queue_jobs
             WHERE stage = 'parse' AND status = 'pending'",
        )
        .execute(&pool)
        .await
        .expect("simulate crash before Parse job handoff");
        assert_eq!(
            reconcile_durable_jobs(&runtime, &queue).await.unwrap(),
            1,
            "reconciler must recreate the missing Parse job"
        );

        let parse = pop(&queue, QueueStage::Parse).await;
        let parsed = process_job(&runtime, &queue, &parse)
            .await
            .expect("process Parse");
        assert_eq!(parsed.parsed, 6);
        queue.ack(&parse).await.expect("ack Parse");
        let visible_before_load: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(visible_before_load, 0);
        let staged: i64 =
            sqlx::query_scalar("SELECT count(*) FROM observation_stage WHERE generation_id = $1")
                .bind(generation_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(staged, 6);

        let load = pop(&queue, QueueStage::Load).await;
        let loaded = process_job(&runtime, &queue, &load)
            .await
            .expect("process Load");
        assert_eq!(loaded.loaded, 6);
        queue.ack(&load).await.expect("ack Load");
        let published: (i64, i64, String) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE ingestion_generation_id = $1),
                    count(*),
                    (SELECT status FROM ingestion_generations WHERE id = $1)
             FROM observations",
        )
        .bind(generation_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(published, (6, 6, "published".to_string()));
        let staged_after: i64 =
            sqlx::query_scalar("SELECT count(*) FROM observation_stage WHERE generation_id = $1")
                .bind(generation_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(staged_after, 0);

        let context = get_ingestion_generation_context(&pool, generation_id)
            .await
            .unwrap()
            .unwrap();
        let rejected = create_ingestion_generation(
            &pool,
            GenerationInput {
                discovered_work_id: context.discovered_work_id,
                artifact_fetch_id: context.artifact.fetch_id.unwrap(),
                source_id: &context.source_id,
                dataflow_id: &context.dataflow_id,
                parser_version: "synthetic-format-error-v1",
                transform_version: "identity-v1",
                job_id: None,
                trace_parent: None,
                actor: "test",
                reason: Some("verify automatic source pause"),
            },
        )
        .await
        .unwrap();
        begin_ingestion_parse(&pool, rejected.id).await.unwrap();
        finish_failed_parse(
            &runtime,
            rejected.id,
            ErrorClass::Permanent,
            "synthetic schema drift",
        )
        .await
        .unwrap();
        let paused: (bool, String, i64) = sqlx::query_as(
            "SELECT control.paused, control.actor,
                    (SELECT count(*) FROM operator_audit_log
                     WHERE action = 'source.auto_pause' AND target_id = control.dataflow_id)
             FROM source_dataflow_controls control
             WHERE control.dataflow_id = 'abs.cpi'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(paused, (true, "system:ingestion".to_string(), 1));
        drop(timescale);
    }

    async fn connect_with_retry(config: &DatabaseConfig) -> sqlx::PgPool {
        let mut last_error = None;
        for _ in 0..10 {
            match connect(config).await {
                Ok(pool) => return pool,
                Err(error) => {
                    last_error = Some(error);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
        panic!("timescaledb did not accept connections: {last_error:?}");
    }

    async fn seed_cpi_catalog(pool: &sqlx::PgPool) {
        sqlx::query(
            "INSERT INTO sources (id, name, homepage)
             VALUES ('abs', 'Australian Bureau of Statistics', 'https://www.abs.gov.au')",
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO measures (id, name, unit) VALUES ('index', 'CPI index', 'index')")
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO dataflows (
                 id, source_id, name, dimensions, measures, frequency,
                 license, attribution, source_url
             ) VALUES (
                 'abs.cpi', 'abs', 'Consumer Price Index',
                 ARRAY['region', 'measure'], ARRAY['index'], 'quarterly',
                 'CC-BY-4.0', 'Source: ABS', 'https://www.abs.gov.au/cpi'
             )",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn pop(queue: &ApalisPgQueue, stage: QueueStage) -> LeasedJob {
        queue
            .pop(stage, WorkerId::new(format!("worker-{stage}")).unwrap())
            .await
            .expect("pop stage")
            .unwrap_or_else(|| panic!("{stage} job should be ready"))
    }

    async fn serve_abs_cpi_once() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 4096];
                let read = stream.read(&mut request).await.unwrap();
                let request = String::from_utf8_lossy(&request[..read]);
                if request.starts_with("GET /rest/dataflow/ABS/CPI") {
                    write_response(
                        &mut stream,
                        "application/vnd.sdmx.structure+json",
                        DATAFLOW_LISTING.as_bytes(),
                    )
                    .await;
                } else if request.starts_with("GET /rest/data/ABS,CPI,2.0.0/all") {
                    write_response(&mut stream, "application/vnd.sdmx.data+json", CPI_FIXTURE)
                        .await;
                } else {
                    panic!("unexpected fixture request: {request}");
                }
            }
        });
        format!("http://{address}/rest")
    }

    async fn write_response(stream: &mut tokio::net::TcpStream, content_type: &str, body: &[u8]) {
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
    }
}
