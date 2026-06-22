//! State planning throughput adapter.

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

const DEFAULT_INDEX_URL: &str = "https://www.planning.nsw.gov.au/data-and-insights";
const NSW_SOURCE_URL: &str = "https://www.planning.nsw.gov.au/data-and-insights";
const VIC_SOURCE_URL: &str =
    "https://www.planning.vic.gov.au/guides-and-resources/data-and-insights";
const USER_AGENT: &str = concat!("au-kpis-adapter-state-planning/", env!("CARGO_PKG_VERSION"));
const SOURCE_ID: &str = "state-planning";
const SOURCE_NAME: &str = "State planning authorities";
const NSW_DA_PROCESSING_DATAFLOW_ID: &str = "state_planning.nsw_da_processing";
const VIC_PERMIT_ACTIVITY_DATAFLOW_ID: &str = "state_planning.vic_permit_activity";
const ATTRIBUTION: &str = "State planning dashboard sources: NSW Planning and Victoria Planning";
const LICENSE_NAME: &str = "State publication terms";

/// State planning throughput adapter.
#[derive(Debug, Clone)]
pub struct StatePlanningAdapter {
    manifest: AdapterManifest,
    index_url: String,
}

impl Default for StatePlanningAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl StatePlanningAdapter {
    /// Start building a state-planning adapter.
    #[must_use]
    pub fn builder() -> StatePlanningAdapterBuilder {
        StatePlanningAdapterBuilder::default()
    }

    /// Parse a state-planning publication index into CSV artifact links.
    pub fn parse_publications(body: &str) -> Result<Vec<StatePlanningPublication>, AdapterError> {
        parse_publications_with_base(body, DEFAULT_INDEX_URL)
    }

    /// Convert current publications into jobs for the supplied timestamp.
    #[must_use]
    pub fn current_jobs_with_started_at(
        current: &[StatePlanningPublication],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Diff current publications against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[StatePlanningPublication],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        discoverable_jobs(current, known_revisions, started_at, trace_parent, None)
    }

    /// Static metadata for state-planning dataflows.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![
            Dataflow {
                id: nsw_da_processing_dataflow_id(),
                source_id: source_id(),
                name: "NSW development assessment processing".into(),
                description: Some(
                    "NSW development assessment throughput and approval-time observations from planning dashboard CSV exports."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("jurisdiction").expect("static dimension id is valid"),
                    DimensionId::new("council").expect("static dimension id is valid"),
                    DimensionId::new("development_type").expect("static dimension id is valid"),
                    DimensionId::new("metric").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new("value").expect("static measure id is valid")],
                frequency: Frequency::Quarterly,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: NSW_SOURCE_URL.into(),
            },
            Dataflow {
                id: vic_permit_activity_dataflow_id(),
                source_id: source_id(),
                name: "VIC planning permit activity".into(),
                description: Some(
                    "Victorian planning permit throughput and approval-time observations from planning dashboard CSV exports."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("jurisdiction").expect("static dimension id is valid"),
                    DimensionId::new("permit_type").expect("static dimension id is valid"),
                    DimensionId::new("metric").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new("value").expect("static measure id is valid")],
                frequency: Frequency::Quarterly,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: VIC_SOURCE_URL.into(),
            },
        ]
    }

    fn index_url(&self) -> &str {
        &self.index_url
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "state planning fetch received job for source `{}`",
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
                "state planning fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        let provenance = publication_url_provenance(&job.source_url).ok_or_else(|| {
            AdapterError::Validation(format!(
                "state planning fetch URL `{}` is not a target CSV artifact",
                job.source_url
            ))
        })?;
        if provenance.dataflow_id != job.dataflow_id {
            return Err(AdapterError::Validation(format!(
                "state planning fetch URL `{}` has dataflow `{}` not `{}`",
                job.source_url,
                provenance.dataflow_id.as_str(),
                job.dataflow_id.as_str()
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl SourceAdapter for StatePlanningAdapter {
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
            homepage: "https://www.planning.nsw.gov.au".into(),
            description: Some(
                "State planning dashboard CSV exports for APS approval-time and throughput inputs."
                    .into(),
            ),
        })
    }

    fn dataflow_metadata(&self) -> Vec<Dataflow> {
        StatePlanningAdapter::dataflow_metadata(self)
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

/// Builder for [`StatePlanningAdapter`].
#[derive(Debug, Clone)]
pub struct StatePlanningAdapterBuilder {
    index_url: String,
}

impl Default for StatePlanningAdapterBuilder {
    fn default() -> Self {
        Self {
            index_url: DEFAULT_INDEX_URL.into(),
        }
    }
}

impl StatePlanningAdapterBuilder {
    /// Override the publication index URL.
    #[must_use]
    pub fn index_url(mut self, url: impl Into<String>) -> Self {
        self.index_url = url.into();
        self
    }

    /// Build a state-planning adapter.
    #[must_use]
    pub fn build(self) -> StatePlanningAdapter {
        StatePlanningAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: SOURCE_NAME.into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(30, Duration::from_secs(60))
                    .expect("static state planning rate limit is valid"),
                dataflows: vec![
                    nsw_da_processing_dataflow_id(),
                    vic_permit_activity_dataflow_id(),
                ],
            },
            index_url: self.index_url,
        }
    }
}

/// One state-planning CSV artifact listed by the index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatePlanningPublication {
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
    jurisdiction: &'static str,
}

impl StatePlanningPublication {
    /// Stable upstream-revision key.
    #[must_use]
    pub fn revision_key(&self) -> String {
        format!("STATE_PLANNING:{}", self.publication_id)
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
            id: format!("state-planning:{}", self.publication_id),
            source_id: source_id(),
            dataflow_id: self.dataflow_id.clone(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("artifact_format".into(), "csv".into()),
                ("publication_id".into(), self.publication_id.clone()),
                ("title".into(), self.title.clone()),
                ("jurisdiction".into(), self.jurisdiction.into()),
                ("revision_key".into(), self.revision_key()),
                ("revision_version".into(), self.revision_version()),
                ("license".into(), LICENSE_NAME.into()),
                ("attribution".into(), ATTRIBUTION.into()),
            ]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlanningProvenance {
    dataflow_id: DataflowId,
    jurisdiction: &'static str,
    publication_id: String,
}

fn discoverable_jobs(
    current: &[StatePlanningPublication],
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
    provenance: PlanningProvenance,
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
    let rows = parse_planning_csv(Bytes::from(bytes), &provenance, &artifact, ingested_at).await?;
    for row in rows {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

async fn parse_planning_csv(
    bytes: Bytes,
    provenance: &PlanningProvenance,
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
        .ok_or_else(|| AdapterError::FormatDrift("state planning CSV is empty".into()))?
        .map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
    let header = header_record
        .iter()
        .enumerate()
        .map(|(index, name)| (name.to_ascii_lowercase(), index))
        .collect::<BTreeMap<_, _>>();
    let mut observations = Vec::new();
    while let Some(record) = records.next().await {
        let record = record.map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
        let row = PlanningCsvRow {
            period: csv_field(&record, &header, "period")?.to_string(),
            jurisdiction: csv_field(&record, &header, "jurisdiction")?.to_string(),
            council: optional_csv_field(&record, &header, "council").map(str::to_string),
            development_type: optional_csv_field(&record, &header, "development_type")
                .map(str::to_string),
            permit_type: optional_csv_field(&record, &header, "permit_type").map(str::to_string),
            metric: csv_field(&record, &header, "metric")?.to_string(),
            value: csv_field(&record, &header, "value")?
                .parse::<f64>()
                .map_err(|_| {
                    AdapterError::FormatDrift(format!(
                        "invalid state planning value `{}`",
                        csv_field(&record, &header, "value").unwrap_or_default()
                    ))
                })?,
            unit: csv_field(&record, &header, "unit")?.to_string(),
        };
        if row.jurisdiction != provenance.jurisdiction {
            return Err(AdapterError::Validation(format!(
                "state planning row jurisdiction `{}` does not match `{}`",
                row.jurisdiction, provenance.jurisdiction
            )));
        }
        observations.push(planning_observation(
            row,
            provenance,
            artifact,
            ingested_at,
        )?);
    }
    Ok(observations)
}

#[derive(Debug)]
struct PlanningCsvRow {
    period: String,
    jurisdiction: String,
    council: Option<String>,
    development_type: Option<String>,
    permit_type: Option<String>,
    metric: String,
    value: f64,
    unit: String,
}

fn csv_field<'a>(
    record: &'a csv_async::StringRecord,
    header: &BTreeMap<String, usize>,
    name: &str,
) -> Result<&'a str, AdapterError> {
    let index = header.get(name).ok_or_else(|| {
        AdapterError::FormatDrift(format!("state planning CSV missing `{name}` column"))
    })?;
    record
        .get(*index)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AdapterError::FormatDrift(format!("state planning CSV missing `{name}`")))
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

fn planning_observation(
    row: PlanningCsvRow,
    provenance: &PlanningProvenance,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let time = parse_period(&row.period)?;
    let dimensions = match provenance.dataflow_id.as_str() {
        NSW_DA_PROCESSING_DATAFLOW_ID => BTreeMap::from([
            (
                DimensionId::new("jurisdiction").expect("static dimension id is valid"),
                code_id("jurisdiction", &row.jurisdiction)?,
            ),
            (
                DimensionId::new("council").expect("static dimension id is valid"),
                code_id("council", required(&row.council, "council")?)?,
            ),
            (
                DimensionId::new("development_type").expect("static dimension id is valid"),
                code_id(
                    "development_type",
                    required(&row.development_type, "development_type")?,
                )?,
            ),
            (
                DimensionId::new("metric").expect("static dimension id is valid"),
                code_id("metric", &row.metric)?,
            ),
        ]),
        VIC_PERMIT_ACTIVITY_DATAFLOW_ID => BTreeMap::from([
            (
                DimensionId::new("jurisdiction").expect("static dimension id is valid"),
                code_id("jurisdiction", &row.jurisdiction)?,
            ),
            (
                DimensionId::new("permit_type").expect("static dimension id is valid"),
                code_id("permit_type", required(&row.permit_type, "permit_type")?)?,
            ),
            (
                DimensionId::new("metric").expect("static dimension id is valid"),
                code_id("metric", &row.metric)?,
            ),
        ]),
        other => {
            return Err(AdapterError::Validation(format!(
                "unsupported state planning dataflow `{other}`"
            )));
        }
    };
    let dataflow_id = provenance.dataflow_id.clone();
    let series_key = SeriesKey::derive(
        &dataflow_id,
        dimensions
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    let descriptor = SeriesDescriptor {
        series_key,
        dataflow_id,
        measure_id: MeasureId::new("value").expect("static measure id is valid"),
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

fn validate_parse_artifact(
    artifact: &ArtifactRef,
    expected_dataflow_id: Option<&DataflowId>,
) -> Result<PlanningProvenance, AdapterError> {
    if artifact.source_id.as_str() != SOURCE_ID {
        return Err(AdapterError::Validation(format!(
            "state planning parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    let provenance = publication_url_provenance(&artifact.source_url).ok_or_else(|| {
        AdapterError::Validation(format!(
            "state planning parse artifact `{}` is missing state planning publication provenance",
            artifact.source_url
        ))
    })?;
    if let Some(expected) = expected_dataflow_id {
        if expected != &provenance.dataflow_id {
            return Err(AdapterError::Validation(format!(
                "state planning parse expected `{}` but artifact URL resolves to `{}`",
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
            "state planning parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )));
    }

    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "state planning parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )))
    }
}

fn parse_publications_with_base(
    body: &str,
    base_url: &str,
) -> Result<Vec<StatePlanningPublication>, AdapterError> {
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
        publications.push(StatePlanningPublication {
            publication_id: provenance.publication_id,
            dataflow_id: provenance.dataflow_id,
            title: clean_text(anchor.text),
            source_url: resolved_url,
            last_updated: attr_value(anchor.tag, "data-updated"),
            jurisdiction: provenance.jurisdiction,
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

fn publication_url_provenance(source_url: &str) -> Option<PlanningProvenance> {
    let file_name = source_url
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())?;
    let stem = file_name.strip_suffix(".csv")?;
    if stem.contains("nsw-da-processing") && trusted_host_for(source_url, "planning.nsw.gov.au") {
        return Some(PlanningProvenance {
            dataflow_id: nsw_da_processing_dataflow_id(),
            jurisdiction: "NSW",
            publication_id: stem.to_string(),
        });
    }
    if stem.contains("vic-permit-activity") && trusted_host_for(source_url, "planning.vic.gov.au") {
        return Some(PlanningProvenance {
            dataflow_id: vic_permit_activity_dataflow_id(),
            jurisdiction: "VIC",
            publication_id: stem.to_string(),
        });
    }
    None
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
        .ok_or_else(|| AdapterError::FormatDrift(format!("state planning CSV missing `{field}`")))
}

fn parse_period(value: &str) -> Result<DateTime<Utc>, AdapterError> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| AdapterError::FormatDrift(format!("invalid planning period `{value}`")))?;
    Ok(Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight is a valid time")))
}

fn code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!(
            "invalid state planning {field} code `{value}`: {err}"
        ))
    })
}

fn cancelled_parse_error() -> AdapterError {
    AdapterError::Validation("state planning parse cancelled".into())
}

fn source_id() -> SourceId {
    SourceId::new(SOURCE_ID).expect("static source id is valid")
}

fn nsw_da_processing_dataflow_id() -> DataflowId {
    DataflowId::new(NSW_DA_PROCESSING_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn vic_permit_activity_dataflow_id() -> DataflowId {
    DataflowId::new(VIC_PERMIT_ACTIVITY_DATAFLOW_ID).expect("static dataflow id is valid")
}
