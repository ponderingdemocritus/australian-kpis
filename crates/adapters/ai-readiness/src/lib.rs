//! AI readiness, adoption, R&D, and talent proxy adapter.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{collections::BTreeMap, io, time::Duration};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterManifest, ArtifactRef, DiscoveredJob, DiscoveryCtx, FetchCtx,
    ObservationStream, ParseCtx, RateLimit, SourceAdapter, UpstreamRevision,
    capture_response_headers, retry_after_delta,
};
use au_kpis_domain::{
    Artifact, CodeId, Dataflow, DataflowId, DimensionId, Frequency, License, MeasureId,
    Observation, ObservationStatus, SeriesDescriptor, SeriesKey, Source, SourceId, TimePrecision,
};
use au_kpis_storage::{BlobStore, StorageKey};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use csv_async::AsyncReaderBuilder;
use futures::{StreamExt, stream};
use tokio_util::{io::StreamReader, sync::CancellationToken};

const DEFAULT_INDEX_URL: &str = "https://www.industry.gov.au/data-and-publications";
const SOURCE_ID: &str = "ai-readiness";
const SOURCE_NAME: &str = "AI readiness sources";
const USER_AGENT: &str = concat!("au-kpis-adapter-ai-readiness/", env!("CARGO_PKG_VERSION"));
const LICENSE_NAME: &str = "Source publication terms";
const ATTRIBUTION: &str =
    "AI readiness sources: Oxford Insights, National AI Centre, ABS, and Home Affairs";

const OXFORD_GARI_DATAFLOW_ID: &str = "oxford.gari";
const NAIC_ADOPTION_DATAFLOW_ID: &str = "naic.ai_adoption_tracker";
const ABS_AI_RD_DATAFLOW_ID: &str = "abs.ai_rd";
const HOME_AFFAIRS_TALENT_DATAFLOW_ID: &str = "home_affairs.skillselect_talent_proxy";

const AI_READINESS_SCORE: &str = "ai_readiness_score";
const ADOPTION_RATE_PCT: &str = "adoption_rate_pct";
const VALUE: &str = "value";

/// AI readiness and adoption adapter.
#[derive(Debug, Clone)]
pub struct AiReadinessAdapter {
    manifest: AdapterManifest,
    index_url: String,
}

impl Default for AiReadinessAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl AiReadinessAdapter {
    /// Start building an AI-readiness adapter.
    #[must_use]
    pub fn builder() -> AiReadinessAdapterBuilder {
        AiReadinessAdapterBuilder::default()
    }

    /// Parse an AI-readiness publication index into CSV artifact links.
    pub fn parse_publications(body: &str) -> Result<Vec<AiReadinessPublication>, AdapterError> {
        parse_publications_with_base(body, DEFAULT_INDEX_URL)
    }

    /// Convert current publications into jobs for the supplied timestamp.
    #[must_use]
    pub fn current_jobs_with_started_at(
        current: &[AiReadinessPublication],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Diff current publications against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[AiReadinessPublication],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        discoverable_jobs(current, known_revisions, started_at, trace_parent, None)
    }

    /// Static metadata for AI-readiness dataflows.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![
            Dataflow {
                id: oxford_gari_dataflow_id(),
                source_id: source_id(),
                name: "Oxford Government AI Readiness Index".into(),
                description: Some(
                    "Annual Australia Government AI Readiness Index observations.".into(),
                ),
                dimensions: vec![
                    DimensionId::new("country").expect("static dimension id is valid"),
                ],
                measures: vec![
                    MeasureId::new(AI_READINESS_SCORE).expect("static measure id is valid")
                ],
                frequency: Frequency::Annual,
                license: License::Other("Oxford Insights terms".into()),
                attribution: "Source: Oxford Insights Government AI Readiness Index".into(),
                source_url: "https://oxfordinsights.com/ai-readiness/ai-readiness-index/".into(),
            },
            Dataflow {
                id: naic_adoption_dataflow_id(),
                source_id: source_id(),
                name: "National AI Centre adoption tracker".into(),
                description: Some("AI adoption tracker observations for Australian segments.".into()),
                dimensions: vec![
                    DimensionId::new("country").expect("static dimension id is valid"),
                    DimensionId::new("segment").expect("static dimension id is valid"),
                ],
                measures: vec![
                    MeasureId::new(ADOPTION_RATE_PCT).expect("static measure id is valid")
                ],
                frequency: Frequency::Quarterly,
                license: License::Other("National AI Centre terms".into()),
                attribution: "Source: National AI Centre".into(),
                source_url: "https://www.industry.gov.au/science-technology-and-innovation/technology/national-ai-centre".into(),
            },
            Dataflow {
                id: abs_ai_rd_dataflow_id(),
                source_id: source_id(),
                name: "ABS AI research and development".into(),
                description: Some(
                    "ABS-derived AI research and development spend proxy observations.".into(),
                ),
                dimensions: vec![
                    DimensionId::new("country").expect("static dimension id is valid"),
                    DimensionId::new("sector").expect("static dimension id is valid"),
                    DimensionId::new("metric").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new(VALUE).expect("static measure id is valid")],
                frequency: Frequency::Annual,
                license: License::CcBy40,
                attribution: "Source: Australian Bureau of Statistics".into(),
                source_url: "https://www.abs.gov.au/statistics/research-and-development".into(),
            },
            Dataflow {
                id: home_affairs_talent_dataflow_id(),
                source_id: source_id(),
                name: "Home Affairs SkillSelect talent proxy".into(),
                description: Some(
                    "SkillSelect invitation observations used as an AI talent-flow proxy.".into(),
                ),
                dimensions: vec![
                    DimensionId::new("country").expect("static dimension id is valid"),
                    DimensionId::new("occupation_group").expect("static dimension id is valid"),
                    DimensionId::new("metric").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new(VALUE).expect("static measure id is valid")],
                frequency: Frequency::Quarterly,
                license: License::Other("Home Affairs publication terms".into()),
                attribution: "Source: Department of Home Affairs".into(),
                source_url: "https://immi.homeaffairs.gov.au/what-we-do/skilled-migration-program".into(),
            },
        ]
    }

    fn index_url(&self) -> &str {
        &self.index_url
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "AI readiness fetch received job for source `{}`",
                job.source_id.as_str()
            )));
        }
        if !self
            .manifest
            .dataflows
            .iter()
            .any(|dataflow_id| dataflow_id == &job.dataflow_id)
        {
            return Err(AdapterError::Validation(format!(
                "AI readiness fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        let provenance = publication_url_provenance(&job.source_url).ok_or_else(|| {
            AdapterError::Validation(format!(
                "AI readiness fetch URL `{}` is not a target CSV artifact",
                job.source_url
            ))
        })?;
        if provenance.dataflow_id != job.dataflow_id {
            return Err(AdapterError::Validation(format!(
                "AI readiness fetch URL `{}` has dataflow `{}` not `{}`",
                job.source_url,
                provenance.dataflow_id.as_str(),
                job.dataflow_id.as_str()
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl SourceAdapter for AiReadinessAdapter {
    fn id(&self) -> &'static str {
        SOURCE_ID
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    fn source_metadata(&self) -> Option<Source> {
        Some(Source {
            id: source_id(),
            name: SOURCE_NAME.into(),
            homepage: "https://www.industry.gov.au".into(),
            description: Some(
                "AI readiness, adoption, R&D, and talent proxy source publications.".into(),
            ),
        })
    }

    fn dataflow_metadata(&self) -> Vec<Dataflow> {
        AiReadinessAdapter::dataflow_metadata(self)
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        let requested = ctx.requested_dataflow_id().cloned();
        if let Some(requested) = &requested {
            if !self
                .manifest
                .dataflows
                .iter()
                .any(|dataflow| dataflow == requested)
            {
                return Ok(Vec::new());
            }
        }

        let response = ctx
            .http
            .execute(
                ctx.http
                    .raw()
                    .get(self.index_url())
                    .header("user-agent", USER_AGENT)
                    .header("accept", "text/html,application/xhtml+xml"),
            )
            .await?
            .error_for_status()?;
        let body = response.text().await?;
        let publications = parse_publications_with_base(&body, self.index_url())?;
        Ok(discoverable_jobs(
            &publications,
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
            requested.as_ref(),
        ))
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id(), job_id = %job.id))]
    async fn fetch(&self, job: DiscoveredJob, ctx: &FetchCtx) -> Result<ArtifactRef, AdapterError> {
        self.validate_fetch_job(&job)?;
        let response = ctx
            .http
            .execute(
                ctx.http
                    .raw_artifact()
                    .get(&job.source_url)
                    .header("user-agent", USER_AGENT)
                    .header("accept", "text/csv,*/*"),
            )
            .await?;
        let response_headers = capture_response_headers(response.headers());
        let status = response.status();
        if !status.is_success() {
            return Err(AdapterError::UpstreamStatus {
                status,
                retry_after: retry_after_delta(&response_headers),
                response_headers,
            });
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map_or_else(|| "text/csv".to_string(), str::to_string);

        let staged = ctx
            .blob_store
            .stage_artifact_stream(response.bytes_stream().boxed())
            .await?;
        let id = staged.id();
        let storage_key = StorageKey::canonical_for(&id).to_string();
        let artifact = Artifact {
            id,
            fetch_id: None,
            source_id: job.source_id,
            source_url: job.source_url,
            content_type,
            response_headers,
            storage_key,
            size_bytes: staged.size_bytes(),
            fetched_at: Utc::now(),
        };
        ctx.blob_store.commit_staged_artifact(&staged).await?;
        ctx.persist_artifact(artifact).await
    }

    fn parse<'a>(&'a self, artifact: ArtifactRef, ctx: &'a ParseCtx) -> ObservationStream<'a> {
        parse_artifact_stream(artifact, ctx)
    }
}

/// Builder for [`AiReadinessAdapter`].
#[derive(Debug, Clone)]
pub struct AiReadinessAdapterBuilder {
    index_url: String,
}

impl Default for AiReadinessAdapterBuilder {
    fn default() -> Self {
        Self {
            index_url: DEFAULT_INDEX_URL.into(),
        }
    }
}

impl AiReadinessAdapterBuilder {
    /// Override the publication index URL.
    #[must_use]
    pub fn index_url(mut self, url: impl Into<String>) -> Self {
        self.index_url = url.into();
        self
    }

    /// Build an AI-readiness adapter.
    #[must_use]
    pub fn build(self) -> AiReadinessAdapter {
        AiReadinessAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: SOURCE_NAME.into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(30, Duration::from_secs(60))
                    .expect("static AI readiness rate limit is valid"),
                dataflows: vec![
                    oxford_gari_dataflow_id(),
                    naic_adoption_dataflow_id(),
                    abs_ai_rd_dataflow_id(),
                    home_affairs_talent_dataflow_id(),
                ],
            },
            index_url: self.index_url,
        }
    }
}

/// One AI-readiness CSV artifact listed by the index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AiReadinessPublication {
    /// Stable source-local publication id.
    pub publication_id: String,
    /// Dataflow represented by the CSV artifact.
    pub dataflow_id: DataflowId,
    /// Human-readable title from the publication index.
    pub title: String,
    /// Resolved CSV artifact URL.
    pub source_url: String,
    /// Last-updated marker from the index, when supplied.
    pub last_updated: Option<String>,
}

impl AiReadinessPublication {
    /// Stable upstream-revision key.
    #[must_use]
    pub fn revision_key(&self) -> String {
        format!("AI_READINESS:{}", self.publication_id)
    }

    /// Stable upstream-revision version.
    #[must_use]
    pub fn revision_version(&self) -> String {
        self.last_updated
            .clone()
            .unwrap_or_else(|| self.publication_id.clone())
    }

    fn to_discovered_job(&self, trace_parent: Option<&str>) -> DiscoveredJob {
        DiscoveredJob {
            id: format!("ai-readiness:{}", self.publication_id),
            source_id: source_id(),
            dataflow_id: self.dataflow_id.clone(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("artifact_format".into(), "csv".into()),
                ("publication_id".into(), self.publication_id.clone()),
                ("title".into(), self.title.clone()),
                ("revision_key".into(), self.revision_key()),
                ("revision_version".into(), self.revision_version()),
                ("license".into(), LICENSE_NAME.into()),
                ("attribution".into(), ATTRIBUTION.into()),
            ]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AiPublicationProvenance {
    dataflow_id: DataflowId,
    publication_id: String,
}

fn discoverable_jobs(
    current: &[AiReadinessPublication],
    known_revisions: &BTreeMap<String, UpstreamRevision>,
    _started_at: DateTime<Utc>,
    trace_parent: Option<&str>,
    requested: Option<&DataflowId>,
) -> Vec<DiscoveredJob> {
    current
        .iter()
        .filter(|publication| {
            requested.is_none_or(|requested| requested == &publication.dataflow_id)
        })
        .filter_map(|publication| {
            let revision = UpstreamRevision::new(
                publication.revision_version(),
                publication.last_updated.clone(),
            );
            known_revisions
                .get(&publication.revision_key())
                .is_none_or(|known| known != &revision)
                .then(|| publication.to_discovered_job(trace_parent))
        })
        .collect()
}

fn parse_artifact_stream(artifact: ArtifactRef, ctx: &ParseCtx) -> ObservationStream<'_> {
    let provenance = match validate_parse_artifact(&artifact, ctx.expected_dataflow_id()) {
        Ok(provenance) => provenance,
        Err(err) => return Box::pin(stream::once(async move { Err(err) })),
    };

    let blob_store = ctx.blob_store.clone();
    let started_at = ctx.started_at;
    let cancellation = ctx.cancellation().clone();
    let (row_tx, row_rx) = tokio::sync::mpsc::channel(128);

    tokio::spawn(async move {
        let key = StorageKey::from_persisted(artifact.storage_key.clone());
        let identity = tokio::select! {
            () = cancellation.cancelled() => Err(cancelled_parse_error()),
            result = verify_parse_artifact_identity(&blob_store, &key, &artifact) => result,
        };
        if let Err(err) = identity {
            let _ = row_tx.send(Err(err)).await;
            return;
        }

        let result = parse_csv_artifact(
            blob_store,
            key,
            artifact,
            provenance,
            started_at,
            cancellation,
            row_tx.clone(),
        )
        .await;
        if let Err(err) = result {
            let _ = row_tx.send(Err(err)).await;
        }
    });

    Box::pin(stream::unfold(row_rx, |mut row_rx| async {
        row_rx.recv().await.map(|item| (item, row_rx))
    }))
}

async fn parse_csv_artifact(
    blob_store: BlobStore,
    key: StorageKey,
    artifact: ArtifactRef,
    provenance: AiPublicationProvenance,
    ingested_at: DateTime<Utc>,
    cancellation: CancellationToken,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    let mut chunks = tokio::select! {
        () = cancellation.cancelled() => return Err(cancelled_parse_error()),
        chunks = blob_store.get(&key) => chunks?,
    };
    let mut bytes = Vec::new();
    while let Some(chunk) = tokio::select! {
        () = cancellation.cancelled() => return Err(cancelled_parse_error()),
        chunk = chunks.next() => chunk,
    } {
        bytes.extend_from_slice(&chunk?);
    }
    let rows = parse_ai_csv(Bytes::from(bytes), &provenance, &artifact, ingested_at).await?;
    for row in rows {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

async fn parse_ai_csv(
    bytes: Bytes,
    provenance: &AiPublicationProvenance,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let io_stream = stream::iter([Ok::<_, io::Error>(bytes)]);
    let reader = StreamReader::new(io_stream);
    let mut csv = AsyncReaderBuilder::new()
        .has_headers(false)
        .create_reader(reader);
    let mut records = csv.records();
    let header_record = records
        .next()
        .await
        .ok_or_else(|| AdapterError::FormatDrift("AI readiness CSV is empty".into()))?
        .map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
    let header = header_record
        .iter()
        .enumerate()
        .map(|(index, name)| (name.to_ascii_lowercase(), index))
        .collect::<BTreeMap<_, _>>();
    let mut observations = Vec::new();
    while let Some(record) = records.next().await {
        let record = record.map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
        let row = AiCsvRow {
            period: csv_field(&record, &header, "period")?.to_string(),
            country: csv_field(&record, &header, "country")?.to_string(),
            segment: optional_csv_field(&record, &header, "segment").map(str::to_string),
            sector: optional_csv_field(&record, &header, "sector").map(str::to_string),
            occupation_group: optional_csv_field(&record, &header, "occupation_group")
                .map(str::to_string),
            metric: optional_csv_field(&record, &header, "metric").map(str::to_string),
            measure_id: csv_field(&record, &header, "measure_id")?.to_string(),
            value: csv_field(&record, &header, "value")?
                .parse::<f64>()
                .map_err(|_| {
                    AdapterError::FormatDrift(format!(
                        "invalid AI readiness value `{}`",
                        csv_field(&record, &header, "value").unwrap_or_default()
                    ))
                })?,
            unit: csv_field(&record, &header, "unit")?.to_string(),
        };
        observations.push(ai_observation(row, provenance, artifact, ingested_at)?);
    }
    Ok(observations)
}

#[derive(Debug)]
struct AiCsvRow {
    period: String,
    country: String,
    segment: Option<String>,
    sector: Option<String>,
    occupation_group: Option<String>,
    metric: Option<String>,
    measure_id: String,
    value: f64,
    unit: String,
}

fn csv_field<'a>(
    record: &'a csv_async::StringRecord,
    header: &BTreeMap<String, usize>,
    name: &str,
) -> Result<&'a str, AdapterError> {
    let index = header.get(name).ok_or_else(|| {
        AdapterError::FormatDrift(format!("AI readiness CSV missing `{name}` column"))
    })?;
    record
        .get(*index)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AdapterError::FormatDrift(format!("AI readiness CSV missing `{name}`")))
}

fn optional_csv_field<'a>(
    record: &'a csv_async::StringRecord,
    header: &BTreeMap<String, usize>,
    name: &str,
) -> Option<&'a str> {
    header
        .get(name)
        .and_then(|index| record.get(*index))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn ai_observation(
    row: AiCsvRow,
    provenance: &AiPublicationProvenance,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let time = parse_period(&row.period)?;
    let dimensions = dimensions_for_row(&row, &provenance.dataflow_id)?;
    validate_measure(&row.measure_id, &provenance.dataflow_id)?;
    let dataflow_id = provenance.dataflow_id.clone();
    let measure_id = MeasureId::new(row.measure_id.clone()).map_err(|err| {
        AdapterError::FormatDrift(format!(
            "invalid AI readiness measure id `{}`: {err}",
            row.measure_id
        ))
    })?;
    let series_key = SeriesKey::derive(
        &dataflow_id,
        &measure_id,
        dimensions
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    let descriptor = SeriesDescriptor {
        series_key,
        dataflow_id,
        measure_id,
        dimensions,
        unit: row.unit.clone(),
    };
    let observation = Observation {
        series_key,
        time,
        time_precision: TimePrecision::Day,
        value: Some(row.value),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes: BTreeMap::from([
            ("source".into(), SOURCE_NAME.into()),
            ("source_url".into(), artifact.source_url.clone()),
            ("license".into(), LICENSE_NAME.into()),
            ("attribution".into(), ATTRIBUTION.into()),
            ("publication_id".into(), provenance.publication_id.clone()),
        ]),
        ingested_at,
        source_artifact_id: artifact.id,
    };
    Ok((descriptor, observation))
}

fn dimensions_for_row(
    row: &AiCsvRow,
    dataflow_id: &DataflowId,
) -> Result<BTreeMap<DimensionId, CodeId>, AdapterError> {
    match dataflow_id.as_str() {
        OXFORD_GARI_DATAFLOW_ID => Ok(BTreeMap::from([(
            dimension_id("country"),
            code_id("country", &row.country)?,
        )])),
        NAIC_ADOPTION_DATAFLOW_ID => Ok(BTreeMap::from([
            (dimension_id("country"), code_id("country", &row.country)?),
            (
                dimension_id("segment"),
                code_id("segment", required(&row.segment, "segment")?)?,
            ),
        ])),
        ABS_AI_RD_DATAFLOW_ID => Ok(BTreeMap::from([
            (dimension_id("country"), code_id("country", &row.country)?),
            (
                dimension_id("sector"),
                code_id("sector", required(&row.sector, "sector")?)?,
            ),
            (
                dimension_id("metric"),
                code_id("metric", required(&row.metric, "metric")?)?,
            ),
        ])),
        HOME_AFFAIRS_TALENT_DATAFLOW_ID => Ok(BTreeMap::from([
            (dimension_id("country"), code_id("country", &row.country)?),
            (
                dimension_id("occupation_group"),
                code_id(
                    "occupation_group",
                    required(&row.occupation_group, "occupation_group")?,
                )?,
            ),
            (
                dimension_id("metric"),
                code_id("metric", required(&row.metric, "metric")?)?,
            ),
        ])),
        other => Err(AdapterError::Validation(format!(
            "unsupported AI readiness dataflow `{other}`"
        ))),
    }
}

fn validate_measure(measure_id: &str, dataflow_id: &DataflowId) -> Result<(), AdapterError> {
    let valid = match dataflow_id.as_str() {
        OXFORD_GARI_DATAFLOW_ID => measure_id == AI_READINESS_SCORE,
        NAIC_ADOPTION_DATAFLOW_ID => measure_id == ADOPTION_RATE_PCT,
        ABS_AI_RD_DATAFLOW_ID | HOME_AFFAIRS_TALENT_DATAFLOW_ID => measure_id == VALUE,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(AdapterError::FormatDrift(format!(
            "AI readiness measure `{measure_id}` is not valid for `{}`",
            dataflow_id.as_str()
        )))
    }
}

fn validate_parse_artifact(
    artifact: &ArtifactRef,
    expected_dataflow_id: Option<&DataflowId>,
) -> Result<AiPublicationProvenance, AdapterError> {
    if artifact.source_id.as_str() != SOURCE_ID {
        return Err(AdapterError::Validation(format!(
            "AI readiness parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    let provenance = publication_url_provenance(&artifact.source_url).ok_or_else(|| {
        AdapterError::Validation(format!(
            "AI readiness parse artifact `{}` is missing AI readiness publication provenance",
            artifact.source_url
        ))
    })?;
    if let Some(expected) = expected_dataflow_id {
        if expected != &provenance.dataflow_id {
            return Err(AdapterError::Validation(format!(
                "AI readiness parse expected `{}` but artifact URL resolves to `{}`",
                expected.as_str(),
                provenance.dataflow_id.as_str()
            )));
        }
    }
    Ok(provenance)
}

async fn verify_parse_artifact_identity(
    blob_store: &BlobStore,
    key: &StorageKey,
    artifact: &ArtifactRef,
) -> Result<(), AdapterError> {
    let canonical_key = StorageKey::canonical_for(&artifact.id).to_string();
    if artifact.storage_key == canonical_key {
        return Ok(());
    }

    if artifact.storage_key.starts_with("artifacts/") {
        return Err(AdapterError::Validation(format!(
            "AI readiness parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )));
    }

    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "AI readiness parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )))
    }
}

fn parse_publications_with_base(
    body: &str,
    base_url: &str,
) -> Result<Vec<AiReadinessPublication>, AdapterError> {
    let mut publications = Vec::new();
    for anchor in extract_anchor_tags(body) {
        let Some(href) = attr_value(anchor.tag, "href") else {
            continue;
        };
        let resolved_url = resolve_url(base_url, &decode_html_entities(&href))?;
        let Some(provenance) = publication_url_provenance(&resolved_url) else {
            continue;
        };
        let Some(dataflow_attr) = attr_value(anchor.tag, "data-dataflow") else {
            continue;
        };
        if dataflow_attr != provenance.dataflow_id.as_str() {
            continue;
        }
        publications.push(AiReadinessPublication {
            publication_id: provenance.publication_id,
            dataflow_id: provenance.dataflow_id,
            title: clean_text(anchor.text),
            source_url: resolved_url,
            last_updated: attr_value(anchor.tag, "data-updated"),
        });
    }
    publications.sort_by(|left, right| {
        left.dataflow_id
            .cmp(&right.dataflow_id)
            .then(left.publication_id.cmp(&right.publication_id))
    });
    publications.dedup_by(|left, right| left.source_url == right.source_url);
    Ok(publications)
}

#[derive(Debug, Clone, Copy)]
struct AnchorTag<'a> {
    tag: &'a str,
    text: &'a str,
}

fn extract_anchor_tags(body: &str) -> Vec<AnchorTag<'_>> {
    let mut anchors = Vec::new();
    let lower = body.to_ascii_lowercase();
    let mut offset = 0;
    while let Some(relative_start) = lower[offset..].find("<a") {
        let tag_start = offset + relative_start;
        let Some(tag_end_relative) = body[tag_start..].find('>') else {
            break;
        };
        let tag_end = tag_start + tag_end_relative;
        let Some(close_relative) = lower[tag_end..].find("</a>") else {
            break;
        };
        let close_start = tag_end + close_relative;
        anchors.push(AnchorTag {
            tag: &body[tag_start..=tag_end],
            text: &body[tag_end + 1..close_start],
        });
        offset = close_start + "</a>".len();
    }
    anchors
}

fn attr_value(tag: &str, name: &str) -> Option<String> {
    let lower = tag.to_ascii_lowercase();
    let pattern = format!("{}=", name.to_ascii_lowercase());
    let relative = lower.find(&pattern)?;
    let mut value_start = relative + pattern.len();
    while tag
        .as_bytes()
        .get(value_start)
        .is_some_and(u8::is_ascii_whitespace)
    {
        value_start += 1;
    }
    let quote = tag.as_bytes().get(value_start).copied()?;
    if quote != b'"' && quote != b'\'' {
        return None;
    }
    let value_start = value_start + 1;
    let value_end = tag[value_start..].find(quote as char)?;
    Some(decode_html_entities(
        &tag[value_start..value_start + value_end],
    ))
}

fn publication_url_provenance(source_url: &str) -> Option<AiPublicationProvenance> {
    let file_name = source_url
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())?;
    let stem = file_name.strip_suffix(".csv")?;
    let dataflow_id = if stem.contains("gari") && trusted_host_for(source_url, "oxfordinsights.com")
    {
        oxford_gari_dataflow_id()
    } else if stem.contains("naic-ai-adoption-tracker")
        && trusted_host_for(source_url, "industry.gov.au")
    {
        naic_adoption_dataflow_id()
    } else if stem.contains("abs-ai-rd") && trusted_host_for(source_url, "abs.gov.au") {
        abs_ai_rd_dataflow_id()
    } else if stem.contains("home-affairs-skillselect-talent-proxy")
        && trusted_host_for(source_url, "homeaffairs.gov.au")
    {
        home_affairs_talent_dataflow_id()
    } else {
        return None;
    };
    Some(AiPublicationProvenance {
        dataflow_id,
        publication_id: stem.to_string(),
    })
}

fn trusted_host_for(source_url: &str, official_host: &str) -> bool {
    source_url.contains(official_host)
        || source_url.starts_with("http://127.0.0.1:")
        || source_url.starts_with("http://localhost:")
}

fn resolve_url(base_url: &str, href: &str) -> Result<String, AdapterError> {
    if href.starts_with("http://") || href.starts_with("https://") {
        return Ok(href.to_string());
    }
    if href.starts_with('/') {
        let origin = origin_for_url(base_url)?;
        return Ok(format!("{origin}{href}"));
    }
    Ok(format!("{}/{href}", base_url.trim_end_matches('/')))
}

fn origin_for_url(url: &str) -> Result<&str, AdapterError> {
    let scheme_end = url
        .find("://")
        .ok_or_else(|| AdapterError::Validation(format!("invalid base URL `{url}`")))?;
    let after_scheme = scheme_end + 3;
    let path_start = url[after_scheme..]
        .find('/')
        .map_or(url.len(), |offset| after_scheme + offset);
    Ok(&url[..path_start])
}

fn clean_text(value: &str) -> String {
    decode_html_entities(value)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn decode_html_entities(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&#38;", "&")
        .replace("&quot;", "\"")
        .replace("&#34;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
}

fn required<'a>(value: &'a Option<String>, field: &str) -> Result<&'a str, AdapterError> {
    value
        .as_deref()
        .ok_or_else(|| AdapterError::FormatDrift(format!("AI readiness CSV missing `{field}`")))
}

fn parse_period(value: &str) -> Result<DateTime<Utc>, AdapterError> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| AdapterError::FormatDrift(format!("invalid AI readiness period `{value}`")))?;
    Ok(Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight is a valid time")))
}

fn dimension_id(value: &str) -> DimensionId {
    DimensionId::new(value).expect("static dimension id is valid")
}

fn code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!(
            "invalid AI readiness {field} code `{value}`: {err}"
        ))
    })
}

fn cancelled_parse_error() -> AdapterError {
    AdapterError::Validation("AI readiness parse cancelled".into())
}

fn source_id() -> SourceId {
    SourceId::new(SOURCE_ID).expect("static source id is valid")
}

fn oxford_gari_dataflow_id() -> DataflowId {
    DataflowId::new(OXFORD_GARI_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn naic_adoption_dataflow_id() -> DataflowId {
    DataflowId::new(NAIC_ADOPTION_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn abs_ai_rd_dataflow_id() -> DataflowId {
    DataflowId::new(ABS_AI_RD_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn home_affairs_talent_dataflow_id() -> DataflowId {
    DataflowId::new(HOME_AFFAIRS_TALENT_DATAFLOW_ID).expect("static dataflow id is valid")
}
