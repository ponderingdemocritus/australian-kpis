use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterHttpClient, AdapterManifest, ArtifactRecorder, ArtifactRef, DiscoveredJob,
    DiscoveryCtx, FetchCtx, ObservationStream, ParseCtx, RateLimit, SourceAdapter,
};
use au_kpis_config::DatabaseConfig;
use au_kpis_db::{PgPool, connect, migrate};
use au_kpis_domain::{
    Artifact, ArtifactId, DataflowId, MeasureId, Observation, ObservationStatus, SeriesDescriptor,
    SourceId, TimePrecision,
    ids::{CodeId, DimensionId, SeriesKey},
};
use au_kpis_ingestion_core::{IngestionPipeline, PipelineContexts, PipelineOptions};
use au_kpis_storage::BlobStore;
use au_kpis_testing::timescale::start_timescale;
use chrono::{TimeZone, Utc};
use futures::{future::BoxFuture, stream};
use object_store::memory::InMemory;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::TracerProvider;
use tokio_util::sync::CancellationToken;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::prelude::*;

const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
const TRACE_PARENT_ALT: &str = "00-11111111111111111111111111111111-2222222222222222-01";

#[derive(Clone, Debug, Default)]
struct TestSpanExporter(Arc<Mutex<Vec<opentelemetry_sdk::export::trace::SpanData>>>);

impl TestSpanExporter {
    fn finished_spans(&self) -> Vec<opentelemetry_sdk::export::trace::SpanData> {
        self.0.lock().expect("span exporter lock").clone()
    }
}

impl opentelemetry_sdk::export::trace::SpanExporter for TestSpanExporter {
    fn export(
        &mut self,
        mut batch: Vec<opentelemetry_sdk::export::trace::SpanData>,
    ) -> BoxFuture<'static, opentelemetry_sdk::export::trace::ExportResult> {
        let spans = self.0.clone();
        Box::pin(async move {
            spans.lock().expect("span exporter lock").append(&mut batch);
            Ok(())
        })
    }
}

#[derive(Debug, Default)]
struct PassthroughRecorder;

#[async_trait]
impl ArtifactRecorder for PassthroughRecorder {
    async fn get(&self, _id: ArtifactId) -> Result<Option<Artifact>, AdapterError> {
        Ok(None)
    }

    async fn record(&self, artifact: &Artifact) -> Result<Artifact, AdapterError> {
        Ok(artifact.clone())
    }

    async fn repair_storage_key(
        &self,
        artifact: &Artifact,
        _observed_storage_key: &str,
    ) -> Result<Artifact, AdapterError> {
        Ok(artifact.clone())
    }
}

#[derive(Debug)]
struct TraceParentAdapter {
    manifest: AdapterManifest,
    mode: TraceParentMode,
}

#[derive(Clone, Copy, Debug)]
enum TraceParentMode {
    ExplicitJobParents,
    DiscoveryParent,
}

impl TraceParentAdapter {
    fn new(mode: TraceParentMode) -> Self {
        Self {
            manifest: AdapterManifest {
                source_id: SourceId::new("stub").unwrap(),
                name: "Stub source".into(),
                version: "test".into(),
                rate_limit: RateLimit::new(600, Duration::from_secs(60)).unwrap(),
                dataflows: vec![DataflowId::new("stub.cpi").unwrap()],
            },
            mode,
        }
    }
}

#[async_trait]
impl SourceAdapter for TraceParentAdapter {
    fn id(&self) -> &'static str {
        "stub"
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        match self.mode {
            TraceParentMode::ExplicitJobParents => Ok(vec![
                trace_job("job-1", "https://example.test/cpi.json", TRACE_PARENT),
                trace_job("job-2", "https://example.test/cpi-2.json", TRACE_PARENT_ALT),
            ]),
            TraceParentMode::DiscoveryParent => Ok(vec![trace_job(
                "job-1",
                "https://example.test/cpi.json",
                ctx.trace_parent().expect("discovery trace parent"),
            )]),
        }
    }

    async fn fetch(
        &self,
        job: DiscoveredJob,
        _ctx: &FetchCtx,
    ) -> Result<ArtifactRef, AdapterError> {
        Ok(ArtifactRef {
            id: ArtifactId::of_content(job.id.as_bytes()),
            source_id: job.source_id,
            source_url: job.source_url,
            content_type: "application/json".into(),
            response_headers: BTreeMap::new(),
            storage_key: "artifacts/stub".into(),
            size_bytes: 2,
            fetched_at: Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
        })
    }

    fn parse<'a>(&'a self, artifact: ArtifactRef, _ctx: &'a ParseCtx) -> ObservationStream<'a> {
        let (series, mut observation) = load_row(artifact.id);
        if artifact.id == ArtifactId::of_content(b"job-2") {
            observation.time = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
        }
        Box::pin(stream::iter([Ok((series, observation))]))
    }
}

fn trace_job(id: &str, source_url: &str, trace_parent: &str) -> DiscoveredJob {
    DiscoveredJob {
        id: id.into(),
        source_id: SourceId::new("stub").unwrap(),
        dataflow_id: DataflowId::new("stub.cpi").unwrap(),
        source_url: source_url.into(),
        trace_parent: Some(trace_parent.into()),
        metadata: BTreeMap::from([("revision_key".into(), "ABS:CPI".into())]),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn per_job_trace_parents_are_restored_on_fetch_parse_and_load_spans() {
    let timescale = start_timescale("au_kpis_pipeline_trace_parent_spans")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    seed_stub_reference_data(&pool, ArtifactId::of_content(b"job-1")).await;
    seed_stub_artifact(
        &pool,
        ArtifactId::of_content(b"job-2"),
        "https://example.test/cpi-2.json",
    )
    .await;

    let exporter = TestSpanExporter::default();
    let provider = TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("ingestion-core-test");
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(LevelFilter::TRACE),
    );

    let guard = tracing::subscriber::set_default(subscriber);
    let stats = pipeline_with_load_max_rows(pool, TraceParentMode::ExplicitJobParents, 1)
        .run_source(
            SourceId::new("stub").unwrap(),
            contexts(),
            CancellationToken::new(),
        )
        .await
        .expect("pipeline should preserve per-job trace parents");
    drop(guard);

    assert_eq!(stats.loaded.observations_loaded, 2);
    for result in provider.force_flush() {
        result.expect("flush exported spans");
    }

    let spans = exporter.finished_spans();
    let expected = BTreeSet::from([
        (
            "00f067aa0ba902b7".to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
        ),
        (
            "2222222222222222".to_string(),
            "11111111111111111111111111111111".to_string(),
        ),
    ]);

    assert_eq!(span_parent_pairs(&spans, "ingestion_fetch_job"), expected);
    assert_eq!(span_parent_pairs(&spans, "ingestion_parse_job"), expected);
    assert_eq!(span_parent_pairs(&spans, "ingestion_load_batch"), expected);
}

#[tokio::test(flavor = "current_thread")]
async fn trace_parent_changes_do_not_fragment_beyond_artifact_boundaries() {
    let timescale = start_timescale("au_kpis_pipeline_trace_batch_size")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    seed_stub_reference_data(&pool, ArtifactId::of_content(b"job-1")).await;
    seed_stub_artifact(
        &pool,
        ArtifactId::of_content(b"job-2"),
        "https://example.test/cpi-2.json",
    )
    .await;

    let exporter = TestSpanExporter::default();
    let provider = TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("ingestion-core-test");
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(LevelFilter::TRACE),
    );

    let guard = tracing::subscriber::set_default(subscriber);
    let stats = pipeline(pool, TraceParentMode::ExplicitJobParents)
        .run_source(
            SourceId::new("stub").unwrap(),
            contexts(),
            CancellationToken::new(),
        )
        .await
        .expect("trace metadata must not fragment loader batches");
    drop(guard);

    assert_eq!(stats.loaded.observations_loaded, 2);
    assert_eq!(stats.loaded.batches, 1);
    for result in provider.force_flush() {
        result.expect("flush exported spans");
    }

    let spans = exporter.finished_spans();
    let expected = BTreeSet::from([
        (
            "00f067aa0ba902b7".to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
        ),
        (
            "2222222222222222".to_string(),
            "11111111111111111111111111111111".to_string(),
        ),
    ]);
    assert_eq!(span_parent_pairs(&spans, "ingestion_load_batch"), expected);
}

#[tokio::test(flavor = "current_thread")]
async fn load_correlation_spans_are_not_emitted_at_info_level() {
    let timescale = start_timescale("au_kpis_pipeline_trace_info_level")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    seed_stub_reference_data(&pool, ArtifactId::of_content(b"job-1")).await;
    seed_stub_artifact(
        &pool,
        ArtifactId::of_content(b"job-2"),
        "https://example.test/cpi-2.json",
    )
    .await;

    let exporter = TestSpanExporter::default();
    let provider = TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("ingestion-core-test");
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(LevelFilter::INFO),
    );

    let guard = tracing::subscriber::set_default(subscriber);
    let stats = pipeline(pool, TraceParentMode::ExplicitJobParents)
        .run_source(
            SourceId::new("stub").unwrap(),
            contexts(),
            CancellationToken::new(),
        )
        .await
        .expect("pipeline should preserve load correlation without info-level per-job spans");
    drop(guard);

    assert_eq!(stats.loaded.observations_loaded, 2);
    for result in provider.force_flush() {
        result.expect("flush exported spans");
    }

    let spans = exporter.finished_spans();
    assert!(span_parent_pairs(&spans, "ingestion_load_batch").is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn downstream_job_spans_descend_from_discovery_span() {
    let timescale = start_timescale("au_kpis_pipeline_discovery_trace_tree")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    seed_stub_reference_data(&pool, ArtifactId::of_content(b"job-1")).await;

    let exporter = TestSpanExporter::default();
    let provider = TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("ingestion-core-test");
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(LevelFilter::TRACE),
    );

    let guard = tracing::subscriber::set_default(subscriber);
    let stats = pipeline(pool, TraceParentMode::DiscoveryParent)
        .run_source(
            SourceId::new("stub").unwrap(),
            contexts(),
            CancellationToken::new(),
        )
        .await
        .expect("pipeline should propagate discovery span context");
    drop(guard);

    assert_eq!(stats.loaded.observations_loaded, 1);
    for result in provider.force_flush() {
        result.expect("flush exported spans");
    }

    let spans = exporter.finished_spans();
    let expected = BTreeSet::from([span_context_pair(&spans, "ingestion_discover")]);

    assert_eq!(span_parent_pairs(&spans, "ingestion_fetch_job"), expected);
    assert_eq!(span_parent_pairs(&spans, "ingestion_parse_job"), expected);
    assert_eq!(span_parent_pairs(&spans, "ingestion_load_batch"), expected);
}

fn pipeline(pool: PgPool, mode: TraceParentMode) -> IngestionPipeline {
    pipeline_with_load_max_rows(pool, mode, 64)
}

fn pipeline_with_load_max_rows(
    pool: PgPool,
    mode: TraceParentMode,
    load_max_rows: usize,
) -> IngestionPipeline {
    let mut builder = au_kpis_adapter::Adapters::builder();
    builder.register(TraceParentAdapter::new(mode)).unwrap();
    IngestionPipeline::new(builder.build(), pool).with_options(PipelineOptions {
        channel_capacity: 2,
        fetch_concurrency: 2,
        parse_concurrency: 2,
        load_max_rows,
        shutdown_grace: Duration::from_secs(5),
        ..PipelineOptions::default()
    })
}

fn contexts() -> PipelineContexts {
    let http = AdapterHttpClient::new(RateLimit::new(600, Duration::from_secs(60)).unwrap());
    let blob_store = BlobStore::new(InMemory::new());
    let started_at = Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap();
    PipelineContexts {
        discovery: DiscoveryCtx::new(http.clone(), started_at),
        fetch: FetchCtx::new(
            http.clone(),
            blob_store.clone(),
            started_at,
            Arc::new(PassthroughRecorder),
        ),
        parse: ParseCtx::new(http, blob_store, started_at),
    }
}

fn span_parent_pairs(
    spans: &[opentelemetry_sdk::export::trace::SpanData],
    name: &str,
) -> BTreeSet<(String, String)> {
    spans
        .iter()
        .filter(|span| span.name == name)
        .map(|span| {
            (
                span.parent_span_id.to_string(),
                span.span_context.trace_id().to_string(),
            )
        })
        .collect()
}

fn span_context_pair(
    spans: &[opentelemetry_sdk::export::trace::SpanData],
    name: &str,
) -> (String, String) {
    let span = spans
        .iter()
        .find(|span| span.name == name)
        .unwrap_or_else(|| panic!("missing span `{name}`"));
    (
        span.span_context.span_id().to_string(),
        span.span_context.trace_id().to_string(),
    )
}

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

async fn seed_stub_reference_data(pool: &PgPool, artifact_id: ArtifactId) {
    sqlx::query(
        "INSERT INTO sources (id, name, homepage, description)
         VALUES ('stub', 'Stub source', 'https://example.test', NULL)",
    )
    .execute(pool)
    .await
    .expect("insert source");

    sqlx::query(
        "INSERT INTO measures (id, name, description, unit, scale)
         VALUES ('index', 'CPI index', NULL, 'index', NULL)",
    )
    .execute(pool)
    .await
    .expect("insert measure");

    sqlx::query(
        "INSERT INTO dataflows (
             id, source_id, name, description, dimensions, measures,
             frequency, license, attribution, source_url
         )
         VALUES (
             'stub.cpi', 'stub', 'Stub CPI', NULL,
             ARRAY['region'], ARRAY['index'], 'quarterly', 'CC-BY-4.0',
             'Source: Stub source', 'https://example.test/cpi'
         )",
    )
    .execute(pool)
    .await
    .expect("insert dataflow");

    seed_stub_artifact(pool, artifact_id, "https://example.test/cpi.json").await;
}

async fn seed_stub_artifact(pool: &PgPool, artifact_id: ArtifactId, source_url: &str) {
    sqlx::query(
        "INSERT INTO artifacts (
             id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at
         )
         VALUES ($1, 'stub', $2, 'application/json',
                 '{}'::jsonb, 2, 'artifacts/stub', $3)",
    )
    .bind(artifact_id.digest().as_bytes().as_slice())
    .bind(source_url)
    .bind(Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap())
    .execute(pool)
    .await
    .expect("insert artifact");
}

fn load_row(artifact_id: ArtifactId) -> (SeriesDescriptor, Observation) {
    let dataflow_id = DataflowId::new("stub.cpi").unwrap();
    let dimensions = BTreeMap::from([(
        DimensionId::new("region").unwrap(),
        CodeId::new("AUS").unwrap(),
    )]);
    let series_key = SeriesKey::derive(
        &dataflow_id,
        dimensions
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    let descriptor = SeriesDescriptor {
        series_key,
        dataflow_id,
        measure_id: MeasureId::new("index").unwrap(),
        dimensions,
        unit: "index".into(),
    };
    let observation = Observation {
        series_key,
        time: Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap(),
        time_precision: TimePrecision::Quarter,
        value: Some(123.4),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes: BTreeMap::new(),
        ingested_at: Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap(),
        source_artifact_id: artifact_id,
    };
    (descriptor, observation)
}
