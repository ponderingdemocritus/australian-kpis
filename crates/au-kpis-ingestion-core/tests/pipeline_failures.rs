use std::{collections::BTreeMap, sync::Arc, time::Duration};

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
use au_kpis_ingestion_core::{
    IngestionError, IngestionPipeline, PipelineContexts, PipelineOptions,
};
use au_kpis_storage::BlobStore;
use au_kpis_testing::timescale::start_timescale;
use chrono::{TimeZone, Utc};
use futures::stream::{self, BoxStream};
use object_store::memory::InMemory;
use sqlx::postgres::PgPoolOptions;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy)]
enum StubMode {
    SlowFetch,
    WrongDiscoveredSource,
    ManyRows,
    RequireParseDataflow,
    CancelAfterFirstParse,
    SlowParse,
    WrongArtifactId,
    ParseErrorAfterRow,
    FatalParseError,
    TwoArtifactsCancelAfterFirstParse,
}

#[derive(Debug)]
struct StubAdapter {
    mode: StubMode,
    manifest: AdapterManifest,
    cancel_on_second_parse_poll: Option<CancellationToken>,
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

impl StubAdapter {
    fn with_cancel(mode: StubMode, cancel_on_second_parse_poll: Option<CancellationToken>) -> Self {
        Self {
            mode,
            manifest: AdapterManifest {
                source_id: SourceId::new("stub").unwrap(),
                name: "Stub source".into(),
                version: "test".into(),
                rate_limit: RateLimit::new(600, Duration::from_secs(60)).unwrap(),
                dataflows: vec![DataflowId::new("stub.cpi").unwrap()],
            },
            cancel_on_second_parse_poll,
        }
    }
}

#[async_trait]
impl SourceAdapter for StubAdapter {
    fn id(&self) -> &'static str {
        "stub"
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    async fn discover(&self, _ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        let source_id = match self.mode {
            StubMode::WrongDiscoveredSource => SourceId::new("other").unwrap(),
            StubMode::SlowFetch
            | StubMode::ManyRows
            | StubMode::RequireParseDataflow
            | StubMode::CancelAfterFirstParse
            | StubMode::SlowParse
            | StubMode::WrongArtifactId
            | StubMode::ParseErrorAfterRow
            | StubMode::FatalParseError
            | StubMode::TwoArtifactsCancelAfterFirstParse => self.manifest.source_id.clone(),
        };

        let mut jobs = vec![DiscoveredJob {
            id: "job-1".into(),
            source_id: source_id.clone(),
            dataflow_id: self.manifest.dataflows[0].clone(),
            source_url: "https://example.test/cpi.json".into(),
            metadata: BTreeMap::from([("revision_key".into(), "ABS:CPI".into())]),
        }];

        if matches!(self.mode, StubMode::TwoArtifactsCancelAfterFirstParse) {
            jobs.push(DiscoveredJob {
                id: "job-2".into(),
                source_id,
                dataflow_id: self.manifest.dataflows[0].clone(),
                source_url: "https://example.test/cpi-2.json".into(),
                metadata: BTreeMap::from([("revision_key".into(), "ABS:CPI".into())]),
            });
        }

        Ok(jobs)
    }

    async fn fetch(
        &self,
        job: DiscoveredJob,
        _ctx: &FetchCtx,
    ) -> Result<ArtifactRef, AdapterError> {
        if matches!(self.mode, StubMode::SlowFetch) {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }

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

    fn parse<'a>(&'a self, artifact: ArtifactRef, ctx: &'a ParseCtx) -> ObservationStream<'a> {
        if matches!(self.mode, StubMode::RequireParseDataflow) {
            let expected = self.manifest.dataflows[0].clone();
            if ctx.expected_dataflow_id() != Some(&expected)
                || ctx.metadata().get("revision_key").map(String::as_str) != Some("ABS:CPI")
            {
                return Box::pin(stream::iter([Err(AdapterError::Validation(
                    "missing expected dataflow provenance".into(),
                ))]));
            }
            return Box::pin(stream::empty());
        }

        let row = load_row(artifact.id);
        match self.mode {
            StubMode::ManyRows => Box::pin(stream::iter([Ok(row.clone()), Ok(row)])),
            StubMode::CancelAfterFirstParse => {
                cancel_after_first_row(row, self.cancel_token().expect("cancel token configured"))
            }
            StubMode::SlowParse => Box::pin(stream::pending()),
            StubMode::TwoArtifactsCancelAfterFirstParse
                if artifact.id == ArtifactId::of_content(b"job-1") =>
            {
                cancel_after_first_row_after_delay(
                    row,
                    self.cancel_token().expect("cancel token configured"),
                    Duration::from_millis(100),
                )
            }
            StubMode::WrongArtifactId => {
                let (series, mut observation) = row;
                observation.source_artifact_id = ArtifactId::of_content(b"wrong artifact");
                Box::pin(stream::iter([Ok((series, observation))]))
            }
            StubMode::ParseErrorAfterRow => Box::pin(stream::iter([
                Ok(row),
                Err(AdapterError::FormatDrift("bad row shape".into())),
            ])),
            StubMode::FatalParseError => Box::pin(stream::iter([Err(AdapterError::FormatDrift(
                "artifact-level schema drift".into(),
            ))])),
            StubMode::TwoArtifactsCancelAfterFirstParse => {
                let (series, mut observation) = row;
                if artifact.id == ArtifactId::of_content(b"job-2") {
                    observation.time = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
                }
                Box::pin(stream::iter([Ok((series, observation))]))
            }
            StubMode::SlowFetch | StubMode::WrongDiscoveredSource => {
                Box::pin(stream::iter([Ok(row)]))
            }
            StubMode::RequireParseDataflow => unreachable!("handled above"),
        }
    }
}

impl StubAdapter {
    fn cancel_token(&self) -> Option<CancellationToken> {
        self.cancel_on_second_parse_poll.clone()
    }
}

fn cancel_after_first_row(
    row: (SeriesDescriptor, Observation),
    cancellation: CancellationToken,
) -> BoxStream<'static, Result<(SeriesDescriptor, Observation), AdapterError>> {
    cancel_after_first_row_after_delay(row, cancellation, Duration::ZERO)
}

fn cancel_after_first_row_after_delay(
    row: (SeriesDescriptor, Observation),
    cancellation: CancellationToken,
    delay: Duration,
) -> BoxStream<'static, Result<(SeriesDescriptor, Observation), AdapterError>> {
    Box::pin(stream::unfold(0_u8, move |state| {
        let row = row.clone();
        let cancellation = cancellation.clone();
        async move {
            match state {
                0 => Some((Ok(row), 1)),
                1 => {
                    tokio::time::sleep(delay).await;
                    cancellation.cancel();
                    None
                }
                _ => None,
            }
        }
    }))
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

fn pipeline(mode: StubMode) -> IngestionPipeline {
    pipeline_with_cancel(mode, None)
}

fn pipeline_with_cancel(
    mode: StubMode,
    cancel_on_second_parse_poll: Option<CancellationToken>,
) -> IngestionPipeline {
    let mut builder = au_kpis_adapter::Adapters::builder();
    builder
        .register(StubAdapter::with_cancel(mode, cancel_on_second_parse_poll))
        .unwrap();
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/unused")
        .unwrap();
    IngestionPipeline::new(builder.build(), pool).with_options(PipelineOptions {
        channel_capacity: 1,
        load_max_rows: 1,
        shutdown_grace: Duration::from_millis(100),
        ..PipelineOptions::default()
    })
}

fn pipeline_with_pool(
    mode: StubMode,
    pool: PgPool,
    options: PipelineOptions,
    cancel_on_second_parse_poll: Option<CancellationToken>,
) -> IngestionPipeline {
    let mut builder = au_kpis_adapter::Adapters::builder();
    builder
        .register(StubAdapter::with_cancel(mode, cancel_on_second_parse_poll))
        .unwrap();
    IngestionPipeline::new(builder.build(), pool).with_options(options)
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

    sqlx::query(
        "INSERT INTO artifacts (
             id, source_id, source_url, content_type, response_headers,
             size_bytes, storage_key, fetched_at
         )
         VALUES ($1, 'stub', 'https://example.test/cpi.json', 'application/json',
                 '{}'::jsonb, 2, 'artifacts/stub', $2)",
    )
    .bind(artifact_id.digest().as_bytes().as_slice())
    .bind(Utc.with_ymd_and_hms(2026, 4, 29, 0, 0, 0).unwrap())
    .execute(pool)
    .await
    .expect("insert artifact");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_bounds_busy_fetch_stage_by_shutdown_grace() {
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
    });

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        pipeline(StubMode::SlowFetch).run_source(
            SourceId::new("stub").unwrap(),
            contexts(),
            cancellation,
        ),
    )
    .await
    .expect("pipeline should honor the shutdown grace");

    assert!(
        matches!(
            result,
            Err(IngestionError::Cancelled | IngestionError::ShutdownTimeout(_))
        ),
        "{result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_reaches_already_started_parse_jobs() {
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
    });

    let result = tokio::time::timeout(
        Duration::from_secs(1),
        pipeline(StubMode::SlowParse).run_source(
            SourceId::new("stub").unwrap(),
            contexts(),
            cancellation,
        ),
    )
    .await
    .expect("pipeline should not wait for a wedged parser stream");

    assert!(
        matches!(result, Err(IngestionError::Cancelled)),
        "{result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_rejects_discovered_jobs_for_other_sources() {
    let result = pipeline(StubMode::WrongDiscoveredSource)
        .run_source(
            SourceId::new("stub").unwrap(),
            contexts(),
            CancellationToken::new(),
        )
        .await;

    assert!(
        matches!(
            result,
            Err(IngestionError::SourceMismatch {
                ref expected,
                ref actual,
                ..
            }) if expected == "stub" && actual == "other"
        ),
        "{result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loader_failure_is_reported_instead_of_upstream_channel_close() {
    let cancellation = CancellationToken::new();
    let result = pipeline(StubMode::ManyRows)
        .run_source(
            SourceId::new("stub").unwrap(),
            contexts(),
            cancellation.clone(),
        )
        .await;

    assert!(matches!(result, Err(IngestionError::Load(_))), "{result:?}");
    assert!(
        !cancellation.is_cancelled(),
        "pipeline errors must not cancel caller-owned root token"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_receives_discovery_dataflow_provenance() {
    let stats = pipeline(StubMode::RequireParseDataflow)
        .run_source(
            SourceId::new("stub").unwrap(),
            contexts(),
            CancellationToken::new(),
        )
        .await
        .expect("parse should receive expected dataflow provenance");

    assert_eq!(stats.discovered, 1);
    assert_eq!(stats.fetched, 1);
    assert_eq!(stats.parsed, 0);
    assert_eq!(stats.loaded.observations_loaded, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parse_rejects_observations_for_the_wrong_artifact_and_audits_error() {
    let timescale = start_timescale("au_kpis_pipeline_artifact_mismatch")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    let artifact_id = ArtifactId::of_content(b"job-1");
    seed_stub_reference_data(&pool, artifact_id).await;

    let result = pipeline_with_pool(
        StubMode::WrongArtifactId,
        pool.clone(),
        PipelineOptions {
            channel_capacity: 1,
            load_max_rows: 64,
            shutdown_grace: Duration::from_secs(5),
            ..PipelineOptions::default()
        },
        None,
    )
    .run_source(
        SourceId::new("stub").unwrap(),
        contexts(),
        CancellationToken::new(),
    )
    .await;

    assert!(
        matches!(
            result,
            Err(IngestionError::ArtifactMismatch {
                ref expected,
                ref actual,
            }) if expected != actual
        ),
        "{result:?}"
    );

    let parse_error_count: i64 = sqlx::query_scalar("SELECT count(*) FROM parse_errors")
        .fetch_one(&pool)
        .await
        .expect("count parse errors");
    assert_eq!(parse_error_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fatal_parse_error_before_any_rows_is_audited_and_fails_pipeline() {
    let timescale = start_timescale("au_kpis_pipeline_fatal_parse_error")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    let artifact_id = ArtifactId::of_content(b"job-1");
    seed_stub_reference_data(&pool, artifact_id).await;

    let result = pipeline_with_pool(
        StubMode::FatalParseError,
        pool.clone(),
        PipelineOptions {
            channel_capacity: 1,
            load_max_rows: 64,
            shutdown_grace: Duration::from_secs(5),
            ..PipelineOptions::default()
        },
        None,
    )
    .run_source(
        SourceId::new("stub").unwrap(),
        contexts(),
        CancellationToken::new(),
    )
    .await;

    assert!(
        matches!(
            result,
            Err(IngestionError::Adapter(AdapterError::FormatDrift(_)))
        ),
        "{result:?}"
    );

    let parse_error_count: i64 = sqlx::query_scalar("SELECT count(*) FROM parse_errors")
        .fetch_one(&pool)
        .await
        .expect("count parse errors");
    assert_eq!(parse_error_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn midstream_parse_errors_are_recorded_while_valid_rows_load() {
    let timescale = start_timescale("au_kpis_pipeline_parse_error")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    let artifact_id = ArtifactId::of_content(b"job-1");
    seed_stub_reference_data(&pool, artifact_id).await;

    let stats = pipeline_with_pool(
        StubMode::ParseErrorAfterRow,
        pool.clone(),
        PipelineOptions {
            channel_capacity: 1,
            load_max_rows: 64,
            shutdown_grace: Duration::from_secs(5),
            ..PipelineOptions::default()
        },
        None,
    )
    .run_source(
        SourceId::new("stub").unwrap(),
        contexts(),
        CancellationToken::new(),
    )
    .await
    .expect("parse error should be audited without failing valid rows");

    assert_eq!(stats.loaded.observations_loaded, 1);
    assert_eq!(stats.loaded.parse_errors, 1);

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    let parse_error_count: i64 = sqlx::query_scalar("SELECT count(*) FROM parse_errors")
        .fetch_one(&pool)
        .await
        .expect("count parse errors");

    assert_eq!(observation_count, 1);
    assert_eq!(parse_error_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_flushes_partial_load_batch() {
    let timescale = start_timescale("au_kpis_pipeline_cancel_flush")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    let artifact_id = ArtifactId::of_content(b"job-1");
    seed_stub_reference_data(&pool, artifact_id).await;

    let cancellation = CancellationToken::new();
    let result = pipeline_with_pool(
        StubMode::CancelAfterFirstParse,
        pool.clone(),
        PipelineOptions {
            channel_capacity: 1,
            load_max_rows: 64,
            shutdown_grace: Duration::from_secs(5),
            ..PipelineOptions::default()
        },
        Some(cancellation.clone()),
    )
    .run_source(SourceId::new("stub").unwrap(), contexts(), cancellation)
    .await;

    assert!(
        matches!(result, Err(IngestionError::Cancelled)),
        "{result:?}"
    );

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    assert_eq!(observation_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_stops_buffered_artifacts_that_are_not_parse_started() {
    let timescale = start_timescale("au_kpis_pipeline_cancel_artifact_drain")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    let first_artifact_id = ArtifactId::of_content(b"job-1");
    let second_artifact_id = ArtifactId::of_content(b"job-2");
    seed_stub_reference_data(&pool, first_artifact_id).await;
    seed_stub_artifact(&pool, second_artifact_id, "https://example.test/cpi-2.json").await;

    let cancellation = CancellationToken::new();
    let result = pipeline_with_pool(
        StubMode::TwoArtifactsCancelAfterFirstParse,
        pool.clone(),
        PipelineOptions {
            channel_capacity: 2,
            fetch_concurrency: 2,
            parse_concurrency: 1,
            load_max_rows: 64,
            shutdown_grace: Duration::from_secs(5),
            ..PipelineOptions::default()
        },
        Some(cancellation.clone()),
    )
    .run_source(SourceId::new("stub").unwrap(), contexts(), cancellation)
    .await;

    assert!(
        matches!(result, Err(IngestionError::Cancelled)),
        "{result:?}"
    );

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    assert_eq!(observation_count, 1);
}
