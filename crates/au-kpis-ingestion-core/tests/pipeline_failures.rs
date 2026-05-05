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

const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

#[derive(Debug, Clone, Copy)]
enum StubMode {
    SlowFetch,
    FetchCompletesAfterCancellation,
    WrongDiscoveredSource,
    ManyRows,
    RequireParseDataflow,
    RequireDiscoveryTraceParent,
    MissingJobTraceParent,
    CancelAfterFirstParse,
    SlowParse,
    WrongArtifactId,
    WrongArtifactAfterRow,
    PanicAfterRow,
    ParseErrorBeforeRow,
    ParseErrorAfterRow,
    ParseErrorAfterCancellation,
    FatalParseError,
    ReadyRowsAfterCancellation,
    AsyncRowsAfterCancellation,
    LoaderValidationError,
    LoaderValidationThenWrongArtifact,
    MissingReferenceFirstAccepted,
    MixedReferenceFirstAccepted,
    AcceptedThenStagedLoadError,
    RevisionRows,
    TwoJobsCancelAfterFirstFetch,
    TwoArtifactsCancelAfterFirstParse,
    DuplicateArtifactRejectSecondJob,
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

    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        let source_id = match self.mode {
            StubMode::WrongDiscoveredSource => SourceId::new("other").unwrap(),
            StubMode::SlowFetch
            | StubMode::FetchCompletesAfterCancellation
            | StubMode::ManyRows
            | StubMode::RequireParseDataflow
            | StubMode::RequireDiscoveryTraceParent
            | StubMode::MissingJobTraceParent
            | StubMode::CancelAfterFirstParse
            | StubMode::SlowParse
            | StubMode::WrongArtifactId
            | StubMode::WrongArtifactAfterRow
            | StubMode::PanicAfterRow
            | StubMode::ParseErrorBeforeRow
            | StubMode::ParseErrorAfterRow
            | StubMode::ParseErrorAfterCancellation
            | StubMode::FatalParseError
            | StubMode::ReadyRowsAfterCancellation
            | StubMode::AsyncRowsAfterCancellation
            | StubMode::LoaderValidationError
            | StubMode::LoaderValidationThenWrongArtifact
            | StubMode::MissingReferenceFirstAccepted
            | StubMode::MixedReferenceFirstAccepted
            | StubMode::AcceptedThenStagedLoadError
            | StubMode::RevisionRows
            | StubMode::TwoJobsCancelAfterFirstFetch
            | StubMode::TwoArtifactsCancelAfterFirstParse
            | StubMode::DuplicateArtifactRejectSecondJob => self.manifest.source_id.clone(),
        };

        let trace_parent = match self.mode {
            StubMode::RequireDiscoveryTraceParent => ctx.trace_parent().map(ToOwned::to_owned),
            StubMode::MissingJobTraceParent => None,
            _ => Some(TRACE_PARENT.into()),
        };

        let mut jobs = vec![DiscoveredJob {
            id: "job-1".into(),
            source_id: source_id.clone(),
            dataflow_id: self.manifest.dataflows[0].clone(),
            source_url: "https://example.test/cpi.json".into(),
            trace_parent,
            metadata: BTreeMap::from([("revision_key".into(), "ABS:CPI".into())]),
        }];

        if matches!(
            self.mode,
            StubMode::TwoArtifactsCancelAfterFirstParse
                | StubMode::TwoJobsCancelAfterFirstFetch
                | StubMode::AcceptedThenStagedLoadError
                | StubMode::DuplicateArtifactRejectSecondJob
        ) {
            jobs.push(DiscoveredJob {
                id: "job-2".into(),
                source_id,
                dataflow_id: self.manifest.dataflows[0].clone(),
                source_url: "https://example.test/cpi-2.json".into(),
                trace_parent: Some(TRACE_PARENT.into()),
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
        if matches!(self.mode, StubMode::FetchCompletesAfterCancellation) {
            tokio::time::sleep(Duration::from_millis(75)).await;
        }
        if matches!(self.mode, StubMode::TwoJobsCancelAfterFirstFetch) && job.id == "job-1" {
            self.cancel_token()
                .expect("cancel token configured")
                .cancel();
        }

        let artifact_id = if matches!(self.mode, StubMode::DuplicateArtifactRejectSecondJob) {
            ArtifactId::of_content(b"shared-artifact")
        } else {
            ArtifactId::of_content(job.id.as_bytes())
        };

        Ok(ArtifactRef {
            id: artifact_id,
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
        if matches!(
            self.mode,
            StubMode::RequireDiscoveryTraceParent | StubMode::MissingJobTraceParent
        ) {
            if ctx.job_id() != Some("job-1") || ctx.trace_parent().is_none() {
                return Box::pin(stream::iter([Err(AdapterError::Validation(
                    "missing discovery trace correlation".into(),
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
            StubMode::WrongArtifactAfterRow => {
                let (series, mut observation) = row.clone();
                observation.source_artifact_id = ArtifactId::of_content(b"wrong artifact");
                Box::pin(stream::iter([Ok(row), Ok((series, observation))]))
            }
            StubMode::PanicAfterRow => panic_after_first_row(row),
            StubMode::ParseErrorBeforeRow => Box::pin(stream::iter([
                Err(AdapterError::FormatDrift("bad first row shape".into())),
                Ok(row),
            ])),
            StubMode::ParseErrorAfterRow => Box::pin(stream::iter([
                Ok(row),
                Err(AdapterError::FormatDrift("bad row shape".into())),
            ])),
            StubMode::ParseErrorAfterCancellation => parse_error_after_cancellation(
                self.cancel_token().expect("cancel token configured"),
            ),
            StubMode::FatalParseError => Box::pin(stream::iter([Err(AdapterError::FormatDrift(
                "artifact-level schema drift".into(),
            ))])),
            StubMode::ReadyRowsAfterCancellation => ready_rows_after_cancellation(
                row,
                self.cancel_token().expect("cancel token configured"),
            ),
            StubMode::AsyncRowsAfterCancellation => async_rows_after_cancellation(
                row,
                self.cancel_token().expect("cancel token configured"),
            ),
            StubMode::LoaderValidationError => {
                Box::pin(stream::iter([Ok(loader_validation_error_row(artifact.id))]))
            }
            StubMode::LoaderValidationThenWrongArtifact => {
                let (series, mut observation) = row;
                observation.source_artifact_id = ArtifactId::of_content(b"wrong artifact");
                Box::pin(stream::iter([
                    Ok(loader_validation_error_row(artifact.id)),
                    Ok((series, observation)),
                ]))
            }
            StubMode::MissingReferenceFirstAccepted => {
                Box::pin(stream::iter([Ok(missing_measure_row(artifact.id))]))
            }
            StubMode::MixedReferenceFirstAccepted => Box::pin(stream::iter([
                Ok(row),
                Ok(missing_measure_row(artifact.id)),
            ])),
            StubMode::AcceptedThenStagedLoadError => {
                if artifact.id == ArtifactId::of_content(b"job-2") {
                    let first = missing_measure_row(artifact.id);
                    let mut second = first.clone();
                    second.1.time = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
                    Box::pin(stream::iter([Ok(first), Ok(second)]))
                } else {
                    Box::pin(stream::iter([Ok(row)]))
                }
            }
            StubMode::RevisionRows => {
                let (series, revision_0) = row;
                let mut revision_1 = revision_0.clone();
                revision_1.revision_no = 1;
                revision_1.value = Some(456.7);
                Box::pin(stream::iter([
                    Ok((series.clone(), revision_0)),
                    Ok((series, revision_1)),
                ]))
            }
            StubMode::TwoArtifactsCancelAfterFirstParse => {
                let (series, mut observation) = row;
                if artifact.id == ArtifactId::of_content(b"job-2") {
                    observation.time = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
                }
                Box::pin(stream::iter([Ok((series, observation))]))
            }
            StubMode::TwoJobsCancelAfterFirstFetch => {
                let (series, mut observation) = row;
                if artifact.id == ArtifactId::of_content(b"job-2") {
                    observation.time = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
                }
                Box::pin(stream::iter([Ok((series, observation))]))
            }
            StubMode::DuplicateArtifactRejectSecondJob => match ctx.job_id() {
                Some("job-1") => delayed_single_row(row, Duration::from_millis(20)),
                Some("job-2") => row_then_delayed_wrong_artifact(row, Duration::from_millis(100)),
                other => panic!("unexpected duplicate artifact job id: {other:?}"),
            },
            StubMode::SlowFetch
            | StubMode::FetchCompletesAfterCancellation
            | StubMode::WrongDiscoveredSource => Box::pin(stream::iter([Ok(row)])),
            StubMode::RequireParseDataflow => unreachable!("handled above"),
            StubMode::RequireDiscoveryTraceParent | StubMode::MissingJobTraceParent => {
                unreachable!("handled above")
            }
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

fn ready_rows_after_cancellation(
    row: (SeriesDescriptor, Observation),
    cancellation: CancellationToken,
) -> BoxStream<'static, Result<(SeriesDescriptor, Observation), AdapterError>> {
    Box::pin(stream::unfold(0_u8, move |state| {
        let mut row = row.clone();
        let cancellation = cancellation.clone();
        async move {
            match state {
                0 => Some((Ok(row), 1)),
                1 => {
                    cancellation.cancel();
                    row.1.time = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
                    Some((Ok(row), 2))
                }
                2 => {
                    row.1.time = Utc.with_ymd_and_hms(2024, 9, 1, 0, 0, 0).unwrap();
                    Some((Ok(row), 3))
                }
                _ => None,
            }
        }
    }))
}

fn async_rows_after_cancellation(
    row: (SeriesDescriptor, Observation),
    cancellation: CancellationToken,
) -> BoxStream<'static, Result<(SeriesDescriptor, Observation), AdapterError>> {
    Box::pin(stream::unfold(0_u8, move |state| {
        let mut row = row.clone();
        let cancellation = cancellation.clone();
        async move {
            match state {
                0 => Some((Ok(row), 1)),
                1 => {
                    cancellation.cancel();
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    row.1.time = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
                    Some((Ok(row), 2))
                }
                2 => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    row.1.time = Utc.with_ymd_and_hms(2024, 9, 1, 0, 0, 0).unwrap();
                    Some((Ok(row), 3))
                }
                _ => None,
            }
        }
    }))
}

fn delayed_single_row(
    row: (SeriesDescriptor, Observation),
    delay: Duration,
) -> BoxStream<'static, Result<(SeriesDescriptor, Observation), AdapterError>> {
    Box::pin(stream::unfold(0_u8, move |state| {
        let row = row.clone();
        async move {
            match state {
                0 => {
                    tokio::time::sleep(delay).await;
                    Some((Ok(row), 1))
                }
                _ => None,
            }
        }
    }))
}

fn row_then_delayed_wrong_artifact(
    row: (SeriesDescriptor, Observation),
    delay: Duration,
) -> BoxStream<'static, Result<(SeriesDescriptor, Observation), AdapterError>> {
    Box::pin(stream::unfold(0_u8, move |state| {
        let row = row.clone();
        async move {
            match state {
                0 => {
                    let (series, mut observation) = row.clone();
                    observation.time = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
                    Some((Ok((series, observation)), 1))
                }
                1 => {
                    tokio::time::sleep(delay).await;
                    let (series, mut observation) = row;
                    observation.time = Utc.with_ymd_and_hms(2024, 9, 1, 0, 0, 0).unwrap();
                    observation.source_artifact_id = ArtifactId::of_content(b"wrong artifact");
                    Some((Ok((series, observation)), 2))
                }
                _ => None,
            }
        }
    }))
}

fn panic_after_first_row(
    row: (SeriesDescriptor, Observation),
) -> BoxStream<'static, Result<(SeriesDescriptor, Observation), AdapterError>> {
    Box::pin(stream::unfold(0_u8, move |state| {
        let row = row.clone();
        async move {
            match state {
                0 => Some((Ok(row), 1)),
                1 => panic!("parser panicked after emitting a row"),
                _ => None,
            }
        }
    }))
}

fn parse_error_after_cancellation(
    cancellation: CancellationToken,
) -> BoxStream<'static, Result<(SeriesDescriptor, Observation), AdapterError>> {
    Box::pin(stream::unfold(0_u8, move |state| {
        let cancellation = cancellation.clone();
        async move {
            match state {
                0 => {
                    cancellation.cancel();
                    Some((
                        Err(AdapterError::FormatDrift(
                            "bad row shape while shutting down".into(),
                        )),
                        1,
                    ))
                }
                _ => {
                    std::future::pending::<
                        Option<(Result<(SeriesDescriptor, Observation), AdapterError>, u8)>,
                    >()
                    .await
                }
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

fn loader_validation_error_row(artifact_id: ArtifactId) -> (SeriesDescriptor, Observation) {
    let (mut descriptor, mut observation) = load_row(artifact_id);
    descriptor.series_key =
        SeriesKey::derive(&descriptor.dataflow_id, std::iter::once(("region", "NZ")));
    observation.series_key = descriptor.series_key;
    (descriptor, observation)
}

fn missing_measure_row(artifact_id: ArtifactId) -> (SeriesDescriptor, Observation) {
    let (mut descriptor, mut observation) = load_row(artifact_id);
    descriptor.dimensions = BTreeMap::from([(
        DimensionId::new("region").unwrap(),
        CodeId::new("NSW").unwrap(),
    )]);
    descriptor.series_key = SeriesKey::derive(
        &descriptor.dataflow_id,
        descriptor
            .dimensions
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    descriptor.measure_id = MeasureId::new("missing").unwrap();
    observation.series_key = descriptor.series_key;
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
async fn cancellation_drains_fetch_that_completes_within_shutdown_grace() {
    let timescale = start_timescale("au_kpis_pipeline_cancel_fetch_drain")
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
    let cancel = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
    });

    let result = pipeline_with_pool(
        StubMode::FetchCompletesAfterCancellation,
        pool.clone(),
        PipelineOptions {
            channel_capacity: 1,
            load_max_rows: 64,
            shutdown_grace: Duration::from_secs(5),
            ..PipelineOptions::default()
        },
        None,
    )
    .run_source(SourceId::new("stub").unwrap(), contexts(), cancellation)
    .await;

    assert!(
        matches!(result, Ok(_) | Err(IngestionError::Cancelled)),
        "{result:?}"
    );

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    assert_eq!(observation_count, 1);
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
async fn run_source_seeds_trace_parent_when_discovery_context_has_none() {
    let stats = pipeline(StubMode::RequireDiscoveryTraceParent)
        .run_source(
            SourceId::new("stub").unwrap(),
            contexts(),
            CancellationToken::new(),
        )
        .await
        .expect("pipeline should seed discovery trace correlation");

    assert_eq!(stats.discovered, 1);
    assert_eq!(stats.fetched, 1);
    assert_eq!(stats.parsed, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_source_backfills_missing_job_trace_parent_from_discovery_context() {
    let stats = pipeline(StubMode::MissingJobTraceParent)
        .run_source(
            SourceId::new("stub").unwrap(),
            contexts(),
            CancellationToken::new(),
        )
        .await
        .expect("pipeline should backfill missing job trace correlation");

    assert_eq!(stats.discovered, 1);
    assert_eq!(stats.fetched, 1);
    assert_eq!(stats.parsed, 0);
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

    let row_context: serde_json::Value = sqlx::query_scalar("SELECT row_context FROM parse_errors")
        .fetch_one(&pool)
        .await
        .expect("read parse error row context");
    assert_eq!(row_context["job_id"], "job-1");
    assert_eq!(row_context["trace_parent"], TRACE_PARENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_artifact_mismatch_rolls_back_current_rows_without_deleting_prior_observations() {
    let timescale = start_timescale("au_kpis_pipeline_late_artifact_mismatch")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    let artifact_id = ArtifactId::of_content(b"job-1");
    seed_stub_reference_data(&pool, artifact_id).await;
    let (series, mut observation) = load_row(artifact_id);
    observation.value = Some(999.0);
    au_kpis_loader::load_batch(
        &pool,
        vec![au_kpis_loader::LoadItem {
            series,
            observation,
        }],
    )
    .await
    .expect("seed prior accepted observation");

    let result = pipeline_with_pool(
        StubMode::WrongArtifactAfterRow,
        pool.clone(),
        PipelineOptions {
            channel_capacity: 1,
            load_max_rows: 1,
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

    let (observation_count, value): (i64, Option<f64>) =
        sqlx::query_as("SELECT count(*), max(value) FROM observations")
            .fetch_one(&pool)
            .await
            .expect("count observations");
    let parse_error_count: i64 = sqlx::query_scalar("SELECT count(*) FROM parse_errors")
        .fetch_one(&pool)
        .await
        .expect("count parse errors");
    assert_eq!(observation_count, 1);
    assert_eq!(value, Some(999.0));
    assert_eq!(parse_error_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_artifact_jobs_keep_pending_loads_separate() {
    let timescale = start_timescale("au_kpis_pipeline_duplicate_artifact_jobs")
        .await
        .expect("start timescaledb container");
    let cfg = DatabaseConfig {
        url: timescale.url().to_string(),
    };
    let pool = connect_with_retry(&cfg).await;
    migrate(&pool).await.expect("apply migrations");
    let artifact_id = ArtifactId::of_content(b"shared-artifact");
    seed_stub_reference_data(&pool, artifact_id).await;

    let result = pipeline_with_pool(
        StubMode::DuplicateArtifactRejectSecondJob,
        pool.clone(),
        PipelineOptions {
            channel_capacity: 4,
            fetch_concurrency: 2,
            parse_concurrency: 2,
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

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    assert_eq!(observation_count, 1);

    let row_context: serde_json::Value = sqlx::query_scalar("SELECT row_context FROM parse_errors")
        .fetch_one(&pool)
        .await
        .expect("read parse error row context");
    assert_eq!(row_context["job_id"], "job-2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parser_panic_after_row_does_not_commit_unaccepted_artifact() {
    let timescale = start_timescale("au_kpis_pipeline_panic_after_row")
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
        StubMode::PanicAfterRow,
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

    assert!(matches!(result, Err(IngestionError::Join(_))), "{result:?}");

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    assert_eq!(observation_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_after_first_parse_error_does_not_wait_for_more_rows() {
    let timescale = start_timescale("au_kpis_pipeline_cancel_after_error")
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
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        pipeline_with_pool(
            StubMode::ParseErrorAfterCancellation,
            pool.clone(),
            PipelineOptions {
                channel_capacity: 1,
                load_max_rows: 64,
                shutdown_grace: Duration::from_millis(100),
                ..PipelineOptions::default()
            },
            Some(cancellation.clone()),
        )
        .run_source(SourceId::new("stub").unwrap(), contexts(), cancellation),
    )
    .await
    .expect("pipeline should not keep polling the parser after cancellation");

    assert!(
        matches!(result, Err(IngestionError::Cancelled)),
        "{result:?}"
    );

    let (observation_count, parse_error_count): (i64, i64) =
        sqlx::query_as("SELECT (SELECT count(*) FROM observations), count(*) FROM parse_errors")
            .fetch_one(&pool)
            .await
            .expect("count observations and parse errors");
    assert_eq!(observation_count, 0);
    assert_eq!(parse_error_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_loader_validation_audit_survives_later_artifact_rejection() {
    let timescale = start_timescale("au_kpis_pipeline_validation_then_reject")
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
        StubMode::LoaderValidationThenWrongArtifact,
        pool.clone(),
        PipelineOptions {
            channel_capacity: 1,
            load_max_rows: 1,
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

    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT error_kind FROM parse_errors
         ORDER BY error_kind",
    )
    .fetch_all(&pool)
    .await
    .expect("read parse error kinds");
    let error_kinds: Vec<_> = rows.into_iter().map(|(kind,)| kind).collect();
    assert_eq!(error_kinds, vec!["artifact_mismatch", "loader_validation"]);

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    assert_eq!(observation_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_loader_validation_audit_survives_artifact_rejection() {
    let timescale = start_timescale("au_kpis_pipeline_buffered_validation_reject")
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
        StubMode::LoaderValidationThenWrongArtifact,
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

    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT error_kind FROM parse_errors
         ORDER BY error_kind",
    )
    .fetch_all(&pool)
    .await
    .expect("read parse error kinds");
    let error_kinds: Vec<_> = rows.into_iter().map(|(kind,)| kind).collect();
    assert_eq!(error_kinds, vec!["artifact_mismatch", "loader_validation"]);

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    assert_eq!(observation_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_buffered_accepted_artifact_missing_reference_is_audited() {
    let timescale = start_timescale("au_kpis_pipeline_first_buffered_reference")
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
        StubMode::MissingReferenceFirstAccepted,
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

    let stats = stats.expect("missing reference should be audited and dropped");
    assert_eq!(stats.loaded.parse_errors, 1);

    let rows: Vec<(String,)> = sqlx::query_as("SELECT error_kind FROM parse_errors")
        .fetch_all(&pool)
        .await
        .expect("read parse error kinds");
    let error_kinds: Vec<_> = rows.into_iter().map(|(kind,)| kind).collect();
    assert_eq!(error_kinds, vec!["loader_validation"]);

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    assert_eq!(observation_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_buffered_accepted_artifact_loads_valid_rows_and_audits_bad_reference() {
    let timescale = start_timescale("au_kpis_pipeline_mixed_buffered_reference")
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
        StubMode::MixedReferenceFirstAccepted,
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
    .expect("valid rows should load while missing references are audited");

    assert_eq!(stats.loaded.observations_loaded, 1);
    assert_eq!(stats.loaded.parse_errors, 1);

    let rows: Vec<(String,)> = sqlx::query_as("SELECT error_kind FROM parse_errors")
        .fetch_all(&pool)
        .await
        .expect("read parse error kinds");
    let error_kinds: Vec<_> = rows.into_iter().map(|(kind,)| kind).collect();
    assert_eq!(error_kinds, vec!["loader_validation"]);

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    assert_eq!(observation_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_accepted_artifact_missing_reference_is_audited() {
    let timescale = start_timescale("au_kpis_pipeline_staged_reference")
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
        StubMode::MissingReferenceFirstAccepted,
        pool.clone(),
        PipelineOptions {
            channel_capacity: 1,
            load_max_rows: 1,
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
    .expect("staged reference validation should audit and drop invalid rows");

    assert_eq!(stats.loaded.parse_errors, 1);

    let rows: Vec<(String,)> = sqlx::query_as("SELECT error_kind FROM parse_errors")
        .fetch_all(&pool)
        .await
        .expect("read parse error kinds");
    let error_kinds: Vec<_> = rows.into_iter().map(|(kind,)| kind).collect();
    assert_eq!(error_kinds, vec!["loader_validation"]);

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    assert_eq!(observation_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_rows_commit_before_later_staged_reference_rejection() {
    let timescale = start_timescale("au_kpis_pipeline_accepted_before_staged_failure")
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

    let result = pipeline_with_pool(
        StubMode::AcceptedThenStagedLoadError,
        pool.clone(),
        PipelineOptions {
            channel_capacity: 2,
            fetch_concurrency: 1,
            parse_concurrency: 1,
            load_max_rows: 2,
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

    let stats = result.expect("later staged reference failures should be audited and dropped");
    assert_eq!(stats.loaded.parse_errors, 2);

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    assert_eq!(observation_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_rows_flush_before_later_buffered_load_failure() {
    let timescale = start_timescale("au_kpis_pipeline_accepted_before_buffered_failure")
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

    let result = pipeline_with_pool(
        StubMode::AcceptedThenStagedLoadError,
        pool.clone(),
        PipelineOptions {
            channel_capacity: 2,
            fetch_concurrency: 1,
            parse_concurrency: 1,
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

    assert!(matches!(result, Err(IngestionError::Load(_))), "{result:?}");

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    assert_eq!(observation_count, 1);
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
async fn first_row_parse_error_is_audited_while_later_valid_rows_load() {
    let timescale = start_timescale("au_kpis_pipeline_first_parse_error")
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
        StubMode::ParseErrorBeforeRow,
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
    .expect("row-level parse errors before valid rows should not fail the artifact");

    assert_eq!(stats.loaded.observations_loaded, 1);
    assert_eq!(stats.loaded.parse_errors, 1);

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    let row_context: serde_json::Value = sqlx::query_scalar("SELECT row_context FROM parse_errors")
        .fetch_one(&pool)
        .await
        .expect("read parse error row context");

    assert_eq!(observation_count, 1);
    assert_eq!(row_context["fatal"], false);
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
async fn loader_validation_errors_preserve_job_and_trace_correlation() {
    let timescale = start_timescale("au_kpis_pipeline_loader_validation_trace")
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
        StubMode::LoaderValidationError,
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
    .expect("loader validation should be audited without failing the pipeline");

    assert_eq!(stats.loaded.observations_loaded, 0);
    assert_eq!(stats.loaded.parse_errors, 1);

    let (error_kind, row_context): (String, serde_json::Value) =
        sqlx::query_as("SELECT error_kind, row_context FROM parse_errors")
            .fetch_one(&pool)
            .await
            .expect("read loader validation audit row");
    assert_eq!(error_kind, "loader_validation");
    assert_eq!(row_context["source_id"], "stub");
    assert_eq!(row_context["job_id"], "job-1");
    assert_eq!(row_context["trace_parent"], TRACE_PARENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_preserves_revision_chain_and_latest_view_selects_highest_revision() {
    let timescale = start_timescale("au_kpis_pipeline_revision_latest")
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
        StubMode::RevisionRows,
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
    .expect("pipeline should load both revisions");

    assert_eq!(stats.loaded.observations_loaded, 2);

    let observation_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations")
        .fetch_one(&pool)
        .await
        .expect("count observations");
    let latest_count: i64 = sqlx::query_scalar("SELECT count(*) FROM observations_latest")
        .fetch_one(&pool)
        .await
        .expect("count latest observations");
    let (revision_no, value): (i32, Option<f64>) =
        sqlx::query_as("SELECT revision_no, value FROM observations_latest")
            .fetch_one(&pool)
            .await
            .expect("read latest observation");

    assert_eq!(observation_count, 2);
    assert_eq!(latest_count, 1);
    assert_eq!(revision_no, 1);
    assert_eq!(value, Some(456.7));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_rejects_incomplete_artifact_after_draining_parse_rows() {
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

    let (observation_count, parse_error_count): (i64, i64) =
        sqlx::query_as("SELECT (SELECT count(*) FROM observations), count(*) FROM parse_errors")
            .fetch_one(&pool)
            .await
            .expect("count observations and parse errors");
    assert_eq!(observation_count, 0);
    assert_eq!(parse_error_count, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_drains_ready_parser_rows_after_shutdown() {
    let timescale = start_timescale("au_kpis_pipeline_ready_cancel")
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
        StubMode::ReadyRowsAfterCancellation,
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

    result.expect("ready parser rows should drain before shutdown completes");

    let (observation_count, parse_error_count): (i64, i64) =
        sqlx::query_as("SELECT (SELECT count(*) FROM observations), count(*) FROM parse_errors")
            .fetch_one(&pool)
            .await
            .expect("count observations and parse errors");
    assert_eq!(observation_count, 3);
    assert_eq!(parse_error_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_drains_async_parser_rows_after_shutdown() {
    let timescale = start_timescale("au_kpis_pipeline_async_cancel")
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
        StubMode::AsyncRowsAfterCancellation,
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

    result.expect("async parser rows should drain before shutdown completes");

    let (observation_count, parse_error_count): (i64, i64) =
        sqlx::query_as("SELECT (SELECT count(*) FROM observations), count(*) FROM parse_errors")
            .fetch_one(&pool)
            .await
            .expect("count observations and parse errors");
    assert_eq!(observation_count, 3);
    assert_eq!(parse_error_count, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_stops_new_discovered_jobs_but_drains_in_flight_fetch() {
    let timescale = start_timescale("au_kpis_pipeline_cancel_discovery_drain")
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
        StubMode::TwoJobsCancelAfterFirstFetch,
        pool.clone(),
        PipelineOptions {
            channel_capacity: 2,
            fetch_concurrency: 1,
            parse_concurrency: 2,
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
async fn cancellation_drains_buffered_artifacts_that_are_already_fetched() {
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
    assert_eq!(observation_count, 2);
}
