use std::collections::BTreeMap;

use au_kpis_db::PgPool;
use au_kpis_domain::ids::{ArtifactId, DataflowId, SourceId};
use au_kpis_loader::{LoadItem, LoadItemAudit, LoadOptions, LoadStats, StagedLoad};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Level, info_span, trace_span};

use crate::{IngestionError, PipelineRunStats, restore_trace_parent};

/// Correlation carried from discovery through artifact loading and audit rows.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ArtifactLoadCorrelation {
    /// Source id for the discovered job.
    pub(crate) source_id: String,
    /// Adapter job id from discovery.
    pub(crate) job_id: String,
    /// W3C traceparent propagated through the ingestion pipeline.
    pub(crate) trace_parent: Option<String>,
}

impl ArtifactLoadCorrelation {
    /// Build parse-error row context from this correlation.
    pub(crate) fn row_context(&self) -> serde_json::Value {
        serde_json::json!({
            "source_id": self.source_id.as_str(),
            "job_id": self.job_id.as_str(),
            "trace_parent": self.trace_parent.as_deref(),
        })
    }
}

/// Event consumed by the artifact-load stage.
#[derive(Debug)]
pub(crate) enum ArtifactLoadEvent {
    /// Parsed observation waiting for its artifact-level accept/reject event.
    Observation {
        /// Loader item to stage or promote.
        item: LoadItem,
        /// Discovery-to-load correlation for audit locality.
        correlation: ArtifactLoadCorrelation,
    },
    /// Accept all rows for an artifact.
    AcceptArtifact {
        /// Accepted artifact id.
        artifact_id: ArtifactId,
        /// Durable fetch row id, when available.
        artifact_fetch_id: Option<i64>,
        /// Source id for the artifact.
        source_id: SourceId,
        /// Dataflow id discovered for the artifact.
        dataflow_id: DataflowId,
        /// Rows parsed from the artifact.
        observations_parsed: u64,
        /// Parser errors emitted before acceptance.
        parse_errors: u64,
        /// Discovery-to-load correlation for audit locality.
        correlation: ArtifactLoadCorrelation,
    },
    /// Reject all rows for an artifact.
    RejectArtifact {
        /// Artifact that must not be promoted.
        artifact_id: ArtifactId,
        /// Discovery-to-load correlation for audit locality.
        correlation: ArtifactLoadCorrelation,
    },
    /// Persist a parser or provenance error for the artifact.
    ParseError(ParseErrorRecord),
}

/// Durable parser/provenance error payload.
#[derive(Debug)]
pub(crate) struct ParseErrorRecord {
    /// Artifact that produced the error.
    pub(crate) artifact_id: ArtifactId,
    /// Stable machine-readable error kind.
    pub(crate) error_kind: &'static str,
    /// Human-readable error message.
    pub(crate) error_message: String,
    /// Optional row context persisted with `parse_errors`.
    pub(crate) row_context: Option<serde_json::Value>,
}

/// Build the reject + parse-error event pair for fatal provenance mismatches.
pub(crate) fn fatal_provenance_events(
    artifact_id: ArtifactId,
    artifact_fetch_id: Option<i64>,
    dataflow_id: &DataflowId,
    source_id: &SourceId,
    correlation: ArtifactLoadCorrelation,
    error_kind: &'static str,
    error_message: &str,
) -> Vec<ArtifactLoadEvent> {
    vec![
        ArtifactLoadEvent::RejectArtifact {
            artifact_id,
            correlation: correlation.clone(),
        },
        ArtifactLoadEvent::ParseError(provenance_error_record(
            artifact_id,
            artifact_fetch_id,
            dataflow_id,
            source_id,
            &correlation,
            error_kind,
            error_message,
        )),
    ]
}

fn provenance_error_record(
    artifact_id: ArtifactId,
    artifact_fetch_id: Option<i64>,
    dataflow_id: &DataflowId,
    source_id: &SourceId,
    correlation: &ArtifactLoadCorrelation,
    error_kind: &'static str,
    error_message: &str,
) -> ParseErrorRecord {
    ParseErrorRecord {
        artifact_id,
        error_kind,
        error_message: error_message.to_string(),
        row_context: Some(serde_json::json!({
            "dataflow_id": dataflow_id,
            "source_id": source_id,
            "artifact_id": artifact_id,
            "artifact_fetch_id": artifact_fetch_id,
            "job_id": correlation.job_id.as_str(),
            "trace_parent": correlation.trace_parent.as_deref(),
            "fatal": true,
        })),
    }
}

/// Drain artifact-load events, promoting accepted artifacts and rolling back rejected ones.
pub(crate) async fn load_stage(
    pool: PgPool,
    mut rx: mpsc::Receiver<ArtifactLoadEvent>,
    options: LoadOptions,
    cancellation: CancellationToken,
) -> Result<PipelineRunStats, IngestionError> {
    let mut loaded = LoadStats::default();
    let mut pending = BTreeMap::<PendingLoadKey, PendingArtifactLoad>::new();
    let mut accepted = AcceptedLoadBuffer::new(options);
    let mut draining = false;

    loop {
        let item = if draining {
            rx.recv().await
        } else {
            tokio::select! {
                () = cancellation.cancelled() => {
                    draining = true;
                    continue;
                }
                value = rx.recv() => value,
            }
        };

        let Some(item) = item else {
            break;
        };

        match item {
            ArtifactLoadEvent::Observation { item, correlation } => {
                let artifact_id = item.observation.source_artifact_id;
                let item_bytes = au_kpis_loader::estimate_load_item_bytes(&item)?;
                let key = PendingLoadKey::new(artifact_id, correlation.clone());
                let artifact = pending.entry(key).or_default();
                if artifact.will_stage(item_bytes, options) {
                    flush_accepted_if_needed(&pool, &mut accepted, options, &mut loaded).await?;
                }
                artifact
                    .push(&pool, options, item, correlation, item_bytes)
                    .await?;
            }
            ArtifactLoadEvent::AcceptArtifact {
                artifact_id,
                artifact_fetch_id,
                source_id,
                dataflow_id,
                observations_parsed,
                parse_errors,
                correlation,
            } => {
                let key = PendingLoadKey::new(artifact_id, correlation);
                let mut artifact_stats = LoadStats::default();
                if let Some(artifact) = pending.remove(&key) {
                    flush_accepted_if_needed(&pool, &mut accepted, options, &mut loaded).await?;
                    let before = loaded;
                    accept_artifact_load(artifact, &pool, options, &mut accepted, &mut loaded)
                        .await?;
                    flush_accepted_if_needed(&pool, &mut accepted, options, &mut loaded).await?;
                    artifact_stats = load_stats_delta(loaded, before);
                }
                if observations_parsed > 0 && parse_errors == 0 && artifact_stats.parse_errors == 0
                {
                    au_kpis_db::record_artifact_load_completion(
                        &pool,
                        au_kpis_db::ArtifactLoadCompletion {
                            artifact_id,
                            artifact_fetch_id,
                            source_id: &source_id,
                            dataflow_id: &dataflow_id,
                            observations_parsed,
                            observations_loaded: artifact_stats.observations_loaded,
                            job_id: Some(key.correlation.job_id.as_str()),
                            trace_parent: key.correlation.trace_parent.as_deref(),
                        },
                    )
                    .await?;
                }
            }
            ArtifactLoadEvent::RejectArtifact {
                artifact_id,
                correlation,
            } => {
                let key = PendingLoadKey::new(artifact_id, correlation);
                if let Some(artifact) = pending.remove(&key) {
                    flush_accepted_if_needed(&pool, &mut accepted, options, &mut loaded).await?;
                    let staged_stats = artifact.rollback(&pool, options).await?;
                    add_load_stats(&mut loaded, staged_stats);
                }
            }
            ArtifactLoadEvent::ParseError(record) => {
                flush_accepted_if_needed(&pool, &mut accepted, options, &mut loaded).await?;
                let trace_parent = parse_error_trace_parent(&record).map(str::to_owned);
                let span = info_span!(
                    "ingestion_load_parse_error",
                    trace_parent = trace_parent.as_deref().unwrap_or("")
                );
                restore_trace_parent(&span, trace_parent.as_deref());
                au_kpis_loader::record_parse_error(
                    &pool,
                    record.artifact_id,
                    record.error_kind,
                    &record.error_message,
                    record.row_context,
                )
                .instrument(span)
                .await?;
                loaded.parse_errors += 1;
            }
        }
    }
    flush_accepted_if_needed(&pool, &mut accepted, options, &mut loaded).await?;
    for (_, artifact) in pending {
        let staged_stats = artifact.rollback(&pool, options).await?;
        add_load_stats(&mut loaded, staged_stats);
    }
    Ok(PipelineRunStats {
        loaded,
        ..PipelineRunStats::default()
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingLoadKey {
    artifact_id: ArtifactId,
    correlation: ArtifactLoadCorrelation,
}

impl PendingLoadKey {
    const fn new(artifact_id: ArtifactId, correlation: ArtifactLoadCorrelation) -> Self {
        Self {
            artifact_id,
            correlation,
        }
    }
}

async fn accept_artifact_load(
    artifact: PendingArtifactLoad,
    pool: &PgPool,
    options: LoadOptions,
    accepted: &mut AcceptedLoadBuffer,
    loaded: &mut LoadStats,
) -> Result<(), au_kpis_loader::LoadError> {
    match artifact.accept(pool, options).await? {
        AcceptedArtifactLoad::Buffered {
            items,
            correlations,
        } => {
            append_accepted_load_items(pool, options, accepted, loaded, items, correlations)
                .await?;
        }
        AcceptedArtifactLoad::Committed(stats) => {
            add_load_stats(loaded, stats);
        }
    }
    Ok(())
}

async fn append_accepted_load_items(
    pool: &PgPool,
    options: LoadOptions,
    accepted: &mut AcceptedLoadBuffer,
    loaded: &mut LoadStats,
    items: Vec<LoadItem>,
    correlations: Vec<ArtifactLoadCorrelation>,
) -> Result<(), au_kpis_loader::LoadError> {
    if items.len() != correlations.len() {
        return Err(au_kpis_loader::LoadError::Validation(
            "accepted load item/correlation count mismatch".into(),
        ));
    }
    let audited_items = items
        .iter()
        .zip(correlations.iter())
        .map(|(item, correlation)| LoadItemAudit {
            item: item.clone(),
            row_context: Some(correlation.row_context()),
        })
        .collect::<Vec<_>>();
    let reference_validation =
        au_kpis_loader::validate_load_references(pool, &audited_items).await?;
    if reference_validation.stats.parse_errors > 0 {
        flush_accepted_if_needed(pool, accepted, options, loaded).await?;
        add_load_stats(loaded, reference_validation.stats);
    }

    for ((item, correlation), valid) in items
        .into_iter()
        .zip(correlations)
        .zip(reference_validation.valid_rows)
    {
        if !valid {
            continue;
        }
        let item_bytes = au_kpis_loader::estimate_load_item_bytes(&item)?;
        if au_kpis_loader::should_flush_load_batch(
            accepted.batch.len(),
            accepted.batch_bytes,
            item_bytes,
            options,
        ) {
            add_load_stats(
                loaded,
                flush_accepted_load_batch(pool, accepted, options).await?,
            );
        }

        accepted.batch_bytes += item_bytes;
        accepted.batch.push(item);
        accepted.correlations.push(correlation);

        if au_kpis_loader::load_batch_boundary_reached(
            accepted.batch.len(),
            accepted.batch_bytes,
            options,
        ) {
            add_load_stats(
                loaded,
                flush_accepted_load_batch(pool, accepted, options).await?,
            );
        }
    }
    Ok(())
}

async fn flush_accepted_if_needed(
    pool: &PgPool,
    accepted: &mut AcceptedLoadBuffer,
    options: LoadOptions,
    loaded: &mut LoadStats,
) -> Result<(), au_kpis_loader::LoadError> {
    if accepted.batch.is_empty() {
        return Ok(());
    }
    add_load_stats(
        loaded,
        flush_accepted_load_batch(pool, accepted, options).await?,
    );
    Ok(())
}

#[derive(Debug)]
struct AcceptedLoadBuffer {
    batch: Vec<LoadItem>,
    correlations: Vec<ArtifactLoadCorrelation>,
    batch_bytes: usize,
}

impl AcceptedLoadBuffer {
    fn new(options: LoadOptions) -> Self {
        Self {
            batch: Vec::with_capacity(options.max_rows.min(1024)),
            correlations: Vec::with_capacity(options.max_rows.min(1024)),
            batch_bytes: 0,
        }
    }
}

#[derive(Debug, Default)]
struct PendingArtifactLoad {
    staged: Option<StagedLoad>,
    batch: Vec<LoadItem>,
    correlations: Vec<ArtifactLoadCorrelation>,
    batch_bytes: usize,
}

impl PendingArtifactLoad {
    async fn push(
        &mut self,
        pool: &PgPool,
        options: LoadOptions,
        item: LoadItem,
        correlation: ArtifactLoadCorrelation,
        item_bytes: usize,
    ) -> Result<(), au_kpis_loader::LoadError> {
        if au_kpis_loader::should_flush_load_batch(
            self.batch.len(),
            self.batch_bytes,
            item_bytes,
            options,
        ) {
            self.stage(pool, options).await?;
        }

        self.batch_bytes += item_bytes;
        self.batch.push(item);
        self.correlations.push(correlation);

        if au_kpis_loader::load_batch_boundary_reached(self.batch.len(), self.batch_bytes, options)
        {
            self.stage(pool, options).await?;
        }

        Ok(())
    }

    fn will_stage(&self, next_item_bytes: usize, options: LoadOptions) -> bool {
        au_kpis_loader::should_flush_load_batch(
            self.batch.len(),
            self.batch_bytes,
            next_item_bytes,
            options,
        ) || self.batch.len() + 1 >= options.max_rows
            || self.batch_bytes.saturating_add(next_item_bytes) >= options.max_bytes
    }

    async fn accept(
        mut self,
        pool: &PgPool,
        options: LoadOptions,
    ) -> Result<AcceptedArtifactLoad, au_kpis_loader::LoadError> {
        if self.staged.is_some() {
            self.stage(pool, options).await?;
            let staged = self.staged.expect("staged load remains after stage");
            Ok(AcceptedArtifactLoad::Committed(staged.commit().await?))
        } else {
            Ok(AcceptedArtifactLoad::Buffered {
                items: self.batch,
                correlations: self.correlations,
            })
        }
    }

    async fn rollback(
        mut self,
        pool: &PgPool,
        options: LoadOptions,
    ) -> Result<LoadStats, au_kpis_loader::LoadError> {
        if !self.batch.is_empty() {
            self.stage(pool, options).await?;
        }
        if let Some(staged) = self.staged {
            return staged.rollback().await;
        }
        Ok(LoadStats::default())
    }

    async fn stage(
        &mut self,
        pool: &PgPool,
        options: LoadOptions,
    ) -> Result<(), au_kpis_loader::LoadError> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let items = match audited_load_items(&mut self.batch, &mut self.correlations) {
            Ok(items) => items,
            Err(err) => {
                if let Some(staged) = self.staged.take() {
                    return match staged.rollback().await {
                        Ok(_) => Err(err),
                        Err(cleanup_err) => Err(cleanup_err),
                    };
                }
                return Err(err);
            }
        };
        emit_load_correlation_spans(&items);
        let mut staged = match self.staged.take() {
            Some(staged) => staged,
            None => au_kpis_loader::begin_staged_load(pool, options).await?,
        };
        let span = info_span!("ingestion_load_stage_batch", rows = items.len(),);
        let result = staged
            .stage(
                items
                    .into_iter()
                    .map(|(item, correlation)| LoadItemAudit {
                        item,
                        row_context: Some(correlation.row_context()),
                    })
                    .collect(),
            )
            .instrument(span)
            .await;
        if let Err(err) = result {
            return match staged.rollback().await {
                Ok(_) => Err(err),
                Err(cleanup_err) => Err(cleanup_err),
            };
        }
        self.staged = Some(staged);
        self.batch_bytes = 0;
        Ok(())
    }
}

#[derive(Debug)]
enum AcceptedArtifactLoad {
    Buffered {
        items: Vec<LoadItem>,
        correlations: Vec<ArtifactLoadCorrelation>,
    },
    Committed(LoadStats),
}

fn audited_load_items(
    batch: &mut Vec<LoadItem>,
    correlations: &mut Vec<ArtifactLoadCorrelation>,
) -> Result<Vec<(LoadItem, ArtifactLoadCorrelation)>, au_kpis_loader::LoadError> {
    let items = std::mem::take(batch);
    let correlations = std::mem::take(correlations);
    if items.len() != correlations.len() {
        return Err(au_kpis_loader::LoadError::Validation(
            "load batch item/correlation count mismatch".into(),
        ));
    }
    Ok(items.into_iter().zip(correlations).collect())
}

fn emit_load_correlation_spans(items: &[(LoadItem, ArtifactLoadCorrelation)]) {
    if !tracing::enabled!(Level::TRACE) {
        return;
    }

    let mut row_counts = BTreeMap::<(String, Option<String>), u64>::new();
    for (_, correlation) in items {
        *row_counts
            .entry((correlation.job_id.clone(), correlation.trace_parent.clone()))
            .or_default() += 1;
    }
    for ((job_id, trace_parent), rows) in row_counts {
        let span = trace_span!(
            "ingestion_load_batch",
            job_id = %job_id,
            trace_parent = trace_parent.as_deref().unwrap_or(""),
            rows
        );
        restore_trace_parent(&span, trace_parent.as_deref());
        let _entered = span.enter();
        tracing::info!("load batch correlation");
    }
}

async fn flush_accepted_load_batch(
    pool: &PgPool,
    accepted: &mut AcceptedLoadBuffer,
    options: LoadOptions,
) -> Result<LoadStats, au_kpis_loader::LoadError> {
    let items = audited_load_items(&mut accepted.batch, &mut accepted.correlations)?;
    accepted.batch_bytes = 0;
    emit_load_correlation_spans(&items);
    let span = info_span!("ingestion_load_commit_batch", rows = items.len(),);
    au_kpis_loader::load_batch_with_options_and_audit_context(
        pool,
        items
            .into_iter()
            .map(|(item, correlation)| LoadItemAudit {
                item,
                row_context: Some(correlation.row_context()),
            })
            .collect(),
        options,
    )
    .instrument(span)
    .await
}

fn parse_error_trace_parent(record: &ParseErrorRecord) -> Option<&str> {
    record
        .row_context
        .as_ref()
        .and_then(|context| context.get("trace_parent"))
        .and_then(serde_json::Value::as_str)
}

fn add_load_stats(total: &mut LoadStats, batch: LoadStats) {
    total.observations_loaded += batch.observations_loaded;
    total.series_upserted += batch.series_upserted;
    total.parse_errors += batch.parse_errors;
    total.batches += batch.batches;
}

fn load_stats_delta(after: LoadStats, before: LoadStats) -> LoadStats {
    LoadStats {
        observations_loaded: after
            .observations_loaded
            .saturating_sub(before.observations_loaded),
        series_upserted: after.series_upserted.saturating_sub(before.series_upserted),
        parse_errors: after.parse_errors.saturating_sub(before.parse_errors),
        batches: after.batches.saturating_sub(before.batches),
    }
}
