//! AEMO NEMWeb adapter for high-frequency dispatch CSV artifacts.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{
    collections::BTreeMap,
    io::{self, Cursor, Read},
    time::Duration,
};

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
use chrono::{DateTime, NaiveDateTime, SecondsFormat, TimeZone, Utc};
use csv_async::AsyncReaderBuilder;
use futures::{StreamExt, stream};
use tokio_util::{io::StreamReader, sync::CancellationToken};
use zip::ZipArchive;

const DEFAULT_DISPATCH_LISTING_URL: &str =
    "https://www.nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/";
const DEFAULT_GENERATION_MIX_LISTING_URL: &str =
    "https://www.nemweb.com.au/Reports/CURRENT/FuelMix/";
const DEFAULT_DISPATCHABILITY_CAPACITY_LISTING_URL: &str =
    "https://www.nemweb.com.au/Reports/CURRENT/DispatchCapacity/";
const USER_AGENT: &str = concat!("au-kpis-adapter-aemo/", env!("CARGO_PKG_VERSION"));
const DISPATCH_DATAFLOW_ID: &str = "aemo.dispatch";
const GENERATION_MIX_DATAFLOW_ID: &str = "aemo.generation_mix";
const DISPATCHABILITY_CAPACITY_DATAFLOW_ID: &str = "aemo.dispatchability_capacity";
const SOURCE_NAME: &str = "Australian Energy Market Operator";
const ATTRIBUTION: &str = "Source: Australian Energy Market Operator";
const LICENSE_NAME: &str = "AEMO Copyright and Disclaimer Notice";
const LICENSE_URL: &str = "https://aemo.com.au/privacy-and-legal-notices/copyright-permissions";

/// AEMO NEMWeb dispatch adapter.
#[derive(Debug, Clone)]
pub struct AemoAdapter {
    manifest: AdapterManifest,
    dispatch_listing_url: String,
    generation_mix_listing_url: String,
    dispatchability_capacity_listing_url: String,
}

impl Default for AemoAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl AemoAdapter {
    /// Start building an AEMO adapter.
    #[must_use]
    pub fn builder() -> AemoAdapterBuilder {
        AemoAdapterBuilder::default()
    }

    /// Parse a NEMWeb DispatchIS directory listing into dispatch ZIP artifacts.
    pub fn parse_dispatch_listing(body: &str) -> Result<Vec<AemoDispatchArtifact>, AdapterError> {
        parse_dispatch_listing_with_base(body, DEFAULT_DISPATCH_LISTING_URL)
    }

    /// Parse a NEMWeb FuelMix directory listing into generation-mix ZIP artifacts.
    pub fn parse_generation_mix_listing(
        body: &str,
    ) -> Result<Vec<AemoEnergyArtifact>, AdapterError> {
        parse_energy_listing_with_base(
            body,
            DEFAULT_GENERATION_MIX_LISTING_URL,
            AemoArtifactKind::GenerationMix,
        )
    }

    /// Parse a NEMWeb DispatchCapacity directory listing into capacity ZIP artifacts.
    pub fn parse_dispatchability_capacity_listing(
        body: &str,
    ) -> Result<Vec<AemoEnergyArtifact>, AdapterError> {
        parse_energy_listing_with_base(
            body,
            DEFAULT_DISPATCHABILITY_CAPACITY_LISTING_URL,
            AemoArtifactKind::DispatchabilityCapacity,
        )
    }

    /// Diff current DispatchIS artifacts against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[AemoDispatchArtifact],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        current
            .iter()
            .filter_map(|artifact| {
                let revision =
                    UpstreamRevision::new(artifact.revision_version(), Some(&artifact.file_name));
                known_revisions
                    .get(&artifact.revision_key())
                    .is_none_or(|known| known != &revision)
                    .then(|| artifact.to_discovered_job(started_at, trace_parent))
            })
            .collect()
    }

    /// Convert current DispatchIS artifacts into jobs for the supplied timestamp.
    #[must_use]
    pub fn current_jobs_with_started_at(
        current: &[AemoDispatchArtifact],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Diff current AEMO energy artifacts against stored upstream revisions.
    #[must_use]
    pub fn discoverable_energy_jobs_with_started_at(
        current: &[AemoEnergyArtifact],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        current
            .iter()
            .filter_map(|artifact| {
                let revision =
                    UpstreamRevision::new(artifact.revision_version(), Some(&artifact.file_name));
                known_revisions
                    .get(&artifact.revision_key())
                    .is_none_or(|known| known != &revision)
                    .then(|| artifact.to_discovered_job(started_at, trace_parent))
            })
            .collect()
    }

    /// Static metadata for the AEMO dispatch dataflow.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![
            Dataflow {
                id: dataflow_id(AemoArtifactKind::Dispatch),
                source_id: source_id(),
                name: "AEMO NEM dispatch".into(),
                description: Some(
                    "Five-minute National Electricity Market dispatch prices and regional demand from NEMWeb DispatchIS reports."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("region").expect("static dimension id is valid"),
                    DimensionId::new("metric").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new("value").expect("static measure id is valid")],
                frequency: Frequency::Irregular,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: DEFAULT_DISPATCH_LISTING_URL.into(),
            },
            Dataflow {
                id: dataflow_id(AemoArtifactKind::GenerationMix),
                source_id: source_id(),
                name: "AEMO NEM generation mix".into(),
                description: Some(
                    "Five-minute regional generation by fuel type from AEMO NEMWeb fuel mix reports."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("region").expect("static dimension id is valid"),
                    DimensionId::new("fuel_type").expect("static dimension id is valid"),
                ],
                measures: vec![
                    MeasureId::new("generation_mw").expect("static measure id is valid"),
                ],
                frequency: Frequency::Irregular,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: DEFAULT_GENERATION_MIX_LISTING_URL.into(),
            },
            Dataflow {
                id: dataflow_id(AemoArtifactKind::DispatchabilityCapacity),
                source_id: source_id(),
                name: "AEMO NEM dispatchability capacity".into(),
                description: Some(
                    "Five-minute regional available generation, dispatchable capacity, and interchange capacity signals from AEMO NEMWeb."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("region").expect("static dimension id is valid"),
                    DimensionId::new("metric").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new("value").expect("static measure id is valid")],
                frequency: Frequency::Irregular,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: DEFAULT_DISPATCHABILITY_CAPACITY_LISTING_URL.into(),
            },
        ]
    }

    fn dispatch_listing_url(&self) -> &str {
        &self.dispatch_listing_url
    }

    fn generation_mix_listing_url(&self) -> &str {
        &self.generation_mix_listing_url
    }

    fn dispatchability_capacity_listing_url(&self) -> &str {
        &self.dispatchability_capacity_listing_url
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "AEMO fetch received job for source `{}`",
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
                "AEMO fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        let provenance = aemo_url_provenance(&job.source_url).ok_or_else(|| {
            AdapterError::Validation(format!(
                "AEMO fetch URL `{}` is not a supported AEMO ZIP artifact",
                job.source_url
            ))
        })?;
        let expected = dataflow_id(provenance.kind());
        if job.dataflow_id != expected {
            return Err(AdapterError::Validation(format!(
                "AEMO fetch URL `{}` resolves to `{}` but job requested `{}`",
                job.source_url,
                expected.as_str(),
                job.dataflow_id.as_str()
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl SourceAdapter for AemoAdapter {
    fn id(&self) -> &'static str {
        "aemo"
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    fn source_metadata(&self) -> Option<Source> {
        Some(Source {
            id: source_id(),
            name: SOURCE_NAME.into(),
            homepage: "https://aemo.com.au".into(),
            description: Some(
                "Operator of Australia's electricity and gas systems and markets.".into(),
            ),
        })
    }

    fn dataflow_metadata(&self) -> Vec<Dataflow> {
        AemoAdapter::dataflow_metadata(self)
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        let dispatch_body = fetch_listing_body(ctx, self.dispatch_listing_url()).await?;
        let dispatch_current =
            parse_dispatch_listing_with_base(&dispatch_body, self.dispatch_listing_url())?;
        let mut jobs = Self::discoverable_jobs_with_started_at(
            &dispatch_current,
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
        );

        let generation_mix_body =
            fetch_listing_body(ctx, self.generation_mix_listing_url()).await?;
        let generation_mix_current = parse_energy_listing_with_base(
            &generation_mix_body,
            self.generation_mix_listing_url(),
            AemoArtifactKind::GenerationMix,
        )?;
        jobs.extend(Self::discoverable_energy_jobs_with_started_at(
            &generation_mix_current,
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
        ));

        let capacity_body =
            fetch_listing_body(ctx, self.dispatchability_capacity_listing_url()).await?;
        let capacity_current = parse_energy_listing_with_base(
            &capacity_body,
            self.dispatchability_capacity_listing_url(),
            AemoArtifactKind::DispatchabilityCapacity,
        )?;
        jobs.extend(Self::discoverable_energy_jobs_with_started_at(
            &capacity_current,
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
        ));

        Ok(jobs)
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
                    .header("accept", "application/zip,application/octet-stream,*/*"),
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
            .map_or_else(|| "application/zip".to_string(), str::to_string);

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

/// Builder for [`AemoAdapter`].
#[derive(Debug, Clone)]
pub struct AemoAdapterBuilder {
    dispatch_listing_url: String,
    generation_mix_listing_url: String,
    dispatchability_capacity_listing_url: String,
}

impl Default for AemoAdapterBuilder {
    fn default() -> Self {
        Self {
            dispatch_listing_url: DEFAULT_DISPATCH_LISTING_URL.into(),
            generation_mix_listing_url: DEFAULT_GENERATION_MIX_LISTING_URL.into(),
            dispatchability_capacity_listing_url: DEFAULT_DISPATCHABILITY_CAPACITY_LISTING_URL
                .into(),
        }
    }
}

impl AemoAdapterBuilder {
    /// Override the DispatchIS directory listing URL.
    #[must_use]
    pub fn dispatch_listing_url(mut self, url: impl Into<String>) -> Self {
        self.dispatch_listing_url = url.into();
        self
    }

    /// Override the FuelMix directory listing URL.
    #[must_use]
    pub fn generation_mix_listing_url(mut self, url: impl Into<String>) -> Self {
        self.generation_mix_listing_url = url.into();
        self
    }

    /// Override the DispatchCapacity directory listing URL.
    #[must_use]
    pub fn dispatchability_capacity_listing_url(mut self, url: impl Into<String>) -> Self {
        self.dispatchability_capacity_listing_url = url.into();
        self
    }

    /// Build an AEMO adapter.
    #[must_use]
    pub fn build(self) -> AemoAdapter {
        AemoAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: SOURCE_NAME.into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(120, Duration::from_secs(60))
                    .expect("static AEMO rate limit is valid"),
                dataflows: vec![
                    dataflow_id(AemoArtifactKind::Dispatch),
                    dataflow_id(AemoArtifactKind::GenerationMix),
                    dataflow_id(AemoArtifactKind::DispatchabilityCapacity),
                ],
            },
            dispatch_listing_url: self.dispatch_listing_url,
            generation_mix_listing_url: self.generation_mix_listing_url,
            dispatchability_capacity_listing_url: self.dispatchability_capacity_listing_url,
        }
    }
}

/// One DispatchIS ZIP listed by NEMWeb.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AemoDispatchArtifact {
    /// ZIP file name from the NEMWeb directory.
    pub file_name: String,
    /// Settlement interval encoded in the file name.
    pub dispatch_interval: DateTime<Utc>,
    /// Resolved artifact URL.
    pub source_url: String,
}

impl AemoDispatchArtifact {
    /// Stable upstream-revision key for this DispatchIS artifact.
    #[must_use]
    pub fn revision_key(&self) -> String {
        format!("AEMO:dispatch:{}", self.file_name)
    }

    /// Stable upstream-revision version for this DispatchIS artifact.
    #[must_use]
    pub fn revision_version(&self) -> String {
        self.dispatch_interval
            .to_rfc3339_opts(SecondsFormat::Secs, true)
    }

    fn to_discovered_job(
        &self,
        _started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> DiscoveredJob {
        DiscoveredJob {
            id: format!("aemo:dispatch:{}", self.file_name),
            source_id: source_id(),
            dataflow_id: dataflow_id(AemoArtifactKind::Dispatch),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("artifact_format".into(), "zip".into()),
                ("file_name".into(), self.file_name.clone()),
                (
                    "dispatch_interval".into(),
                    self.dispatch_interval
                        .to_rfc3339_opts(SecondsFormat::Secs, true),
                ),
                ("revision_key".into(), self.revision_key()),
                ("revision_version".into(), self.revision_version()),
                ("cadence".into(), "5-minute".into()),
                ("attribution".into(), ATTRIBUTION.into()),
                ("license".into(), LICENSE_NAME.into()),
                ("license_url".into(), LICENSE_URL.into()),
            ]),
        }
    }
}

/// One AEMO APS energy ZIP listed by NEMWeb.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AemoEnergyArtifact {
    /// ZIP file name from the NEMWeb directory.
    pub file_name: String,
    /// Five-minute interval encoded in the file name.
    pub interval: DateTime<Utc>,
    /// Resolved artifact URL.
    pub source_url: String,
    kind: AemoArtifactKind,
}

impl AemoEnergyArtifact {
    /// Stable upstream-revision key for this AEMO energy artifact.
    #[must_use]
    pub fn revision_key(&self) -> String {
        format!("AEMO:{}:{}", self.kind.revision_name(), self.file_name)
    }

    /// Stable upstream-revision version for this AEMO energy artifact.
    #[must_use]
    pub fn revision_version(&self) -> String {
        self.interval.to_rfc3339_opts(SecondsFormat::Secs, true)
    }

    fn to_discovered_job(
        &self,
        _started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> DiscoveredJob {
        DiscoveredJob {
            id: format!("aemo:{}:{}", self.kind.revision_name(), self.file_name),
            source_id: source_id(),
            dataflow_id: dataflow_id(self.kind),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("artifact_format".into(), "zip".into()),
                ("file_name".into(), self.file_name.clone()),
                (
                    "interval".into(),
                    self.interval.to_rfc3339_opts(SecondsFormat::Secs, true),
                ),
                ("revision_key".into(), self.revision_key()),
                ("revision_version".into(), self.revision_version()),
                ("cadence".into(), "5-minute".into()),
                ("attribution".into(), ATTRIBUTION.into()),
                ("license".into(), LICENSE_NAME.into()),
                ("license_url".into(), LICENSE_URL.into()),
            ]),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AemoArtifactKind {
    Dispatch,
    GenerationMix,
    DispatchabilityCapacity,
}

impl AemoArtifactKind {
    const fn dataflow_id(self) -> &'static str {
        match self {
            Self::Dispatch => DISPATCH_DATAFLOW_ID,
            Self::GenerationMix => GENERATION_MIX_DATAFLOW_ID,
            Self::DispatchabilityCapacity => DISPATCHABILITY_CAPACITY_DATAFLOW_ID,
        }
    }

    const fn revision_name(self) -> &'static str {
        match self {
            Self::Dispatch => "dispatch",
            Self::GenerationMix => "generation_mix",
            Self::DispatchabilityCapacity => "dispatchability_capacity",
        }
    }

    const fn url_path(self) -> &'static str {
        match self {
            Self::Dispatch => "/DispatchIS_Reports/",
            Self::GenerationMix => "/FuelMix/",
            Self::DispatchabilityCapacity => "/DispatchCapacity/",
        }
    }

    const fn file_prefix(self) -> &'static str {
        match self {
            Self::Dispatch => "PUBLIC_DISPATCHIS_",
            Self::GenerationMix => "PUBLIC_FUEL_MIX_",
            Self::DispatchabilityCapacity => "PUBLIC_DISPATCHCAPACITY_",
        }
    }

    const fn format_name(self) -> &'static str {
        match self {
            Self::Dispatch => "AEMO DispatchIS",
            Self::GenerationMix => "AEMO FuelMix",
            Self::DispatchabilityCapacity => "AEMO DispatchCapacity",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AemoArtifactProvenance {
    Dispatch(AemoDispatchArtifact),
    Energy(AemoEnergyArtifact),
}

impl AemoArtifactProvenance {
    const fn kind(&self) -> AemoArtifactKind {
        match self {
            Self::Dispatch(_) => AemoArtifactKind::Dispatch,
            Self::Energy(artifact) => artifact.kind,
        }
    }
}

fn parse_artifact_stream(artifact: ArtifactRef, ctx: &ParseCtx) -> ObservationStream<'_> {
    let provenance = match validate_parse_artifact(&artifact) {
        Ok(provenance) => provenance,
        Err(err) => return Box::pin(stream::once(async move { Err(err) })),
    };
    let kind = provenance.kind();

    let blob_store = ctx.blob_store.clone();
    let started_at = ctx.started_at;
    let cancellation = ctx.cancellation().clone();
    let (row_tx, row_rx) = tokio::sync::mpsc::channel(512);

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

        let result = parse_aemo_artifact(
            kind,
            blob_store,
            key,
            artifact,
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

async fn parse_aemo_artifact(
    kind: AemoArtifactKind,
    blob_store: BlobStore,
    key: StorageKey,
    artifact: ArtifactRef,
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
    let csv_bytes = tokio::task::spawn_blocking(move || unzip_aemo_csv(bytes, kind))
        .await
        .map_err(parse_worker_error)??;
    let records = parse_aemo_csv(csv_bytes, cancellation.clone()).await?;

    let rows = match kind {
        AemoArtifactKind::Dispatch => dispatch_observations(records, &artifact, ingested_at)?,
        AemoArtifactKind::GenerationMix => {
            generation_mix_observations(records, &artifact, ingested_at)?
        }
        AemoArtifactKind::DispatchabilityCapacity => {
            dispatchability_capacity_observations(records, &artifact, ingested_at)?
        }
    };
    for row in rows {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

fn unzip_aemo_csv(bytes: Vec<u8>, kind: AemoArtifactKind) -> Result<Vec<u8>, AdapterError> {
    let mut archive = ZipArchive::new(Cursor::new(bytes))
        .map_err(|err| AdapterError::FormatDrift(format!("{} ZIP: {err}", kind.format_name())))?;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|err| {
            AdapterError::FormatDrift(format!("{} ZIP: {err}", kind.format_name()))
        })?;
        if entry.name().to_ascii_lowercase().ends_with(".csv") {
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).map_err(|err| {
                AdapterError::FormatDrift(format!("{} CSV: {err}", kind.format_name()))
            })?;
            return Ok(bytes);
        }
    }
    Err(AdapterError::FormatDrift(format!(
        "{} ZIP has no CSV member",
        kind.format_name()
    )))
}

async fn parse_aemo_csv(
    bytes: Vec<u8>,
    cancellation: CancellationToken,
) -> Result<Vec<Vec<String>>, AdapterError> {
    let io_stream = stream::iter([Ok::<_, io::Error>(Bytes::from(bytes))]);
    let reader = StreamReader::new(io_stream);
    let mut csv = AsyncReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .create_reader(reader);
    let mut records = csv.records();
    let mut rows = Vec::new();
    while let Some(record) = tokio::select! {
        () = cancellation.cancelled() => return Err(cancelled_parse_error()),
        record = records.next() => record,
    } {
        let record = record.map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
        rows.push(record.iter().map(|cell| cell.trim().to_string()).collect());
    }
    Ok(rows)
}

fn dispatch_observations(
    rows: Vec<Vec<String>>,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let mut headers = BTreeMap::<(String, String), BTreeMap<String, usize>>::new();
    let mut observations = Vec::new();
    for row in rows {
        if row.len() < 4 {
            continue;
        }
        let key = (row[1].clone(), row[2].clone());
        match row[0].as_str() {
            "I" => {
                headers.insert(
                    key,
                    row.iter()
                        .enumerate()
                        .skip(4)
                        .map(|(index, name)| (name.to_ascii_uppercase(), index))
                        .collect(),
                );
            }
            "D" if row[1] == "DISPATCH" && row[2] == "PRICE" => {
                let Some(header) = headers.get(&key) else {
                    return Err(AdapterError::FormatDrift(
                        "AEMO DispatchIS PRICE row appeared before its header".into(),
                    ));
                };
                observations.push(dispatch_observation(
                    &row,
                    header,
                    DispatchMetric {
                        id: "regional_reference_price",
                        source_field: "RRP",
                        unit: "AUD/MWh",
                        table: "PRICE",
                    },
                    artifact,
                    ingested_at,
                )?);
            }
            "D" if row[1] == "DISPATCH" && row[2] == "REGIONSUM" => {
                let Some(header) = headers.get(&key) else {
                    return Err(AdapterError::FormatDrift(
                        "AEMO DispatchIS REGIONSUM row appeared before its header".into(),
                    ));
                };
                observations.push(dispatch_observation(
                    &row,
                    header,
                    DispatchMetric {
                        id: "total_demand",
                        source_field: "TOTALDEMAND",
                        unit: "MW",
                        table: "REGIONSUM",
                    },
                    artifact,
                    ingested_at,
                )?);
            }
            _ => {}
        }
    }
    Ok(observations)
}

fn generation_mix_observations(
    rows: Vec<Vec<String>>,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let mut headers = BTreeMap::<(String, String), BTreeMap<String, usize>>::new();
    let mut observations = Vec::new();
    for row in rows {
        if row.len() < 4 {
            continue;
        }
        let key = (row[1].clone(), row[2].clone());
        match row[0].as_str() {
            "I" => {
                headers.insert(
                    key,
                    row.iter()
                        .enumerate()
                        .skip(4)
                        .map(|(index, name)| (name.to_ascii_uppercase(), index))
                        .collect(),
                );
            }
            "D" if row[1] == "FUELMIX" && row[2] == "FUELREGION" => {
                let Some(header) = headers.get(&key) else {
                    return Err(AdapterError::FormatDrift(
                        "AEMO FuelMix FUELREGION row appeared before its header".into(),
                    ));
                };
                observations.push(generation_mix_observation(
                    &row,
                    header,
                    artifact,
                    ingested_at,
                )?);
            }
            _ => {}
        }
    }
    Ok(observations)
}

fn dispatchability_capacity_observations(
    rows: Vec<Vec<String>>,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let mut headers = BTreeMap::<(String, String), BTreeMap<String, usize>>::new();
    let mut observations = Vec::new();
    for row in rows {
        if row.len() < 4 {
            continue;
        }
        let key = (row[1].clone(), row[2].clone());
        match row[0].as_str() {
            "I" => {
                headers.insert(
                    key,
                    row.iter()
                        .enumerate()
                        .skip(4)
                        .map(|(index, name)| (name.to_ascii_uppercase(), index))
                        .collect(),
                );
            }
            "D" if row[1] == "DISPATCH" && row[2] == "CAPACITY" => {
                let Some(header) = headers.get(&key) else {
                    return Err(AdapterError::FormatDrift(
                        "AEMO DispatchCapacity row appeared before its header".into(),
                    ));
                };
                for metric in [
                    CapacityMetric {
                        id: "available_generation",
                        source_field: "AVAILABLEGENERATION",
                    },
                    CapacityMetric {
                        id: "dispatchable_capacity",
                        source_field: "DISPATCHABLECAPACITY",
                    },
                    CapacityMetric {
                        id: "net_interchange",
                        source_field: "NETINTERCHANGE",
                    },
                ] {
                    observations.push(dispatchability_capacity_observation(
                        &row,
                        header,
                        metric,
                        artifact,
                        ingested_at,
                    )?);
                }
            }
            _ => {}
        }
    }
    Ok(observations)
}

#[derive(Debug, Clone, Copy)]
struct DispatchMetric {
    id: &'static str,
    source_field: &'static str,
    unit: &'static str,
    table: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct CapacityMetric {
    id: &'static str,
    source_field: &'static str,
}

fn dispatch_observation(
    row: &[String],
    header: &BTreeMap<String, usize>,
    metric: DispatchMetric,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let settlement_date = field(row, header, "SETTLEMENTDATE")?;
    let region = field(row, header, "REGIONID")?;
    let value = parse_number(field(row, header, metric.source_field)?)?;
    let time = parse_dispatch_time(settlement_date)?;
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("region").expect("static dimension id is valid"),
            aemo_code_id("region", region)?,
        ),
        (
            DimensionId::new("metric").expect("static dimension id is valid"),
            CodeId::new(metric.id).expect("static code id is valid"),
        ),
    ]);
    let dataflow_id = dataflow_id(AemoArtifactKind::Dispatch);
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
        unit: metric.unit.into(),
    };
    let mut attributes = BTreeMap::from([
        ("source".into(), SOURCE_NAME.into()),
        ("source_url".into(), artifact.source_url.clone()),
        ("license".into(), LICENSE_NAME.into()),
        ("license_url".into(), LICENSE_URL.into()),
        ("aemo_table".into(), metric.table.into()),
        ("aemo_field".into(), metric.source_field.into()),
    ]);
    if let Ok(status) = field(row, header, "PRICE_STATUS") {
        if !status.is_empty() {
            attributes.insert("price_status".into(), status.to_string());
        }
    }
    let observation = Observation {
        series_key,
        time,
        time_precision: TimePrecision::Minute,
        value: Some(value),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes,
        ingested_at,
        source_artifact_id: artifact.id,
    };
    Ok((descriptor, observation))
}

fn generation_mix_observation(
    row: &[String],
    header: &BTreeMap<String, usize>,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let settlement_date = field(row, header, "SETTLEMENTDATE")?;
    let region = field(row, header, "REGIONID")?;
    let fuel_type = field(row, header, "FUELTYPE")?;
    let value = parse_number(field(row, header, "GENERATIONMW")?)?;
    let time = parse_dispatch_time(settlement_date)?;
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("region").expect("static dimension id is valid"),
            aemo_code_id("region", region)?,
        ),
        (
            DimensionId::new("fuel_type").expect("static dimension id is valid"),
            aemo_code_id("fuel_type", fuel_type)?,
        ),
    ]);
    let dataflow_id = dataflow_id(AemoArtifactKind::GenerationMix);
    let measure_id = MeasureId::new("generation_mw").expect("static measure id is valid");
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
        unit: "MW".into(),
    };
    let attributes = BTreeMap::from([
        ("source".into(), SOURCE_NAME.into()),
        ("source_url".into(), artifact.source_url.clone()),
        ("license".into(), LICENSE_NAME.into()),
        ("license_url".into(), LICENSE_URL.into()),
        ("aemo_table".into(), "FUELREGION".into()),
        ("aemo_field".into(), "GENERATIONMW".into()),
    ]);
    let observation = Observation {
        series_key,
        time,
        time_precision: TimePrecision::Minute,
        value: Some(value),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes,
        ingested_at,
        source_artifact_id: artifact.id,
    };
    Ok((descriptor, observation))
}

fn dispatchability_capacity_observation(
    row: &[String],
    header: &BTreeMap<String, usize>,
    metric: CapacityMetric,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let settlement_date = field(row, header, "SETTLEMENTDATE")?;
    let region = field(row, header, "REGIONID")?;
    let value = parse_number(field(row, header, metric.source_field)?)?;
    let time = parse_dispatch_time(settlement_date)?;
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("region").expect("static dimension id is valid"),
            aemo_code_id("region", region)?,
        ),
        (
            DimensionId::new("metric").expect("static dimension id is valid"),
            CodeId::new(metric.id).expect("static code id is valid"),
        ),
    ]);
    let dataflow_id = dataflow_id(AemoArtifactKind::DispatchabilityCapacity);
    let measure_id = MeasureId::new("value").expect("static measure id is valid");
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
        unit: "MW".into(),
    };
    let attributes = BTreeMap::from([
        ("source".into(), SOURCE_NAME.into()),
        ("source_url".into(), artifact.source_url.clone()),
        ("license".into(), LICENSE_NAME.into()),
        ("license_url".into(), LICENSE_URL.into()),
        ("aemo_table".into(), "CAPACITY".into()),
        ("aemo_field".into(), metric.source_field.into()),
    ]);
    let observation = Observation {
        series_key,
        time,
        time_precision: TimePrecision::Minute,
        value: Some(value),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes,
        ingested_at,
        source_artifact_id: artifact.id,
    };
    Ok((descriptor, observation))
}

fn field<'a>(
    row: &'a [String],
    header: &BTreeMap<String, usize>,
    name: &str,
) -> Result<&'a str, AdapterError> {
    let index = header
        .get(name)
        .ok_or_else(|| AdapterError::FormatDrift(format!("AEMO DispatchIS missing `{name}`")))?;
    row.get(*index)
        .map(String::as_str)
        .ok_or_else(|| AdapterError::FormatDrift(format!("AEMO DispatchIS missing `{name}`")))
}

fn parse_number(value: &str) -> Result<f64, AdapterError> {
    value
        .replace(',', "")
        .parse::<f64>()
        .map_err(|_| AdapterError::FormatDrift(format!("invalid AEMO numeric value `{value}`")))
}

fn parse_dispatch_time(value: &str) -> Result<DateTime<Utc>, AdapterError> {
    let date_time = NaiveDateTime::parse_from_str(value, "%Y/%m/%d %H:%M:%S")
        .map_err(|_| AdapterError::FormatDrift(format!("invalid AEMO dispatch time `{value}`")))?;
    Ok(Utc.from_utc_datetime(&date_time))
}

fn validate_parse_artifact(artifact: &ArtifactRef) -> Result<AemoArtifactProvenance, AdapterError> {
    if artifact.source_id.as_str() != "aemo" {
        return Err(AdapterError::Validation(format!(
            "AEMO parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    aemo_url_provenance(&artifact.source_url).ok_or_else(|| {
        AdapterError::Validation(format!(
            "AEMO parse artifact `{}` is missing supported AEMO provenance",
            artifact.source_url
        ))
    })
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
            "AEMO parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )));
    }

    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "AEMO parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )))
    }
}

fn parse_dispatch_listing_with_base(
    body: &str,
    base_url: &str,
) -> Result<Vec<AemoDispatchArtifact>, AdapterError> {
    let mut artifacts = Vec::new();
    for href in extract_hrefs(body) {
        let resolved_url = resolve_url(base_url, &href)?;
        if let Some(AemoArtifactProvenance::Dispatch(artifact)) = aemo_url_provenance(&resolved_url)
        {
            artifacts.push(artifact);
        }
    }
    artifacts.sort_by(|left, right| {
        left.dispatch_interval
            .cmp(&right.dispatch_interval)
            .then(left.file_name.cmp(&right.file_name))
    });
    artifacts.dedup_by(|left, right| left.source_url == right.source_url);
    Ok(artifacts)
}

async fn fetch_listing_body(ctx: &DiscoveryCtx, listing_url: &str) -> Result<String, AdapterError> {
    let response = ctx
        .http
        .execute(
            ctx.http
                .raw()
                .get(listing_url)
                .header("user-agent", USER_AGENT)
                .header("accept", "text/html,application/xhtml+xml"),
        )
        .await?
        .error_for_status()?;
    Ok(response.text().await?)
}

fn parse_energy_listing_with_base(
    body: &str,
    base_url: &str,
    kind: AemoArtifactKind,
) -> Result<Vec<AemoEnergyArtifact>, AdapterError> {
    let mut artifacts = Vec::new();
    for href in extract_hrefs(body) {
        let resolved_url = resolve_url(base_url, &href)?;
        if let Some(AemoArtifactProvenance::Energy(artifact)) = aemo_url_provenance(&resolved_url) {
            if artifact.kind == kind {
                artifacts.push(artifact);
            }
        }
    }
    artifacts.sort_by(|left, right| {
        left.interval
            .cmp(&right.interval)
            .then(left.file_name.cmp(&right.file_name))
    });
    artifacts.dedup_by(|left, right| left.source_url == right.source_url);
    Ok(artifacts)
}

fn extract_hrefs(body: &str) -> Vec<String> {
    let mut hrefs = Vec::new();
    let lower = body.to_ascii_lowercase();
    let mut offset = 0;
    while let Some(relative_index) = lower[offset..].find("href=") {
        let mut value_start = offset + relative_index + "href=".len();
        while body
            .as_bytes()
            .get(value_start)
            .is_some_and(u8::is_ascii_whitespace)
        {
            value_start += 1;
        }
        let Some(quote) = body.as_bytes().get(value_start).copied() else {
            break;
        };
        if quote != b'"' && quote != b'\'' {
            offset = value_start + 1;
            continue;
        }
        let value_start = value_start + 1;
        if let Some(value_end) = body[value_start..].find(quote as char) {
            hrefs.push(decode_html_entities(
                &body[value_start..value_start + value_end],
            ));
            offset = value_start + value_end + 1;
        } else {
            break;
        }
    }
    hrefs
}

fn aemo_url_provenance(source_url: &str) -> Option<AemoArtifactProvenance> {
    if let Some(dispatch) = dispatch_url_provenance(source_url) {
        return Some(AemoArtifactProvenance::Dispatch(dispatch));
    }
    for kind in [
        AemoArtifactKind::GenerationMix,
        AemoArtifactKind::DispatchabilityCapacity,
    ] {
        if let Some(artifact) = energy_url_provenance(source_url, kind) {
            return Some(AemoArtifactProvenance::Energy(artifact));
        }
    }
    None
}

fn dispatch_url_provenance(source_url: &str) -> Option<AemoDispatchArtifact> {
    let file_name = source_url
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())?
        .to_string();
    let dispatch_interval = interval_from_file_name(&file_name, AemoArtifactKind::Dispatch)?;
    if !source_url.contains(AemoArtifactKind::Dispatch.url_path()) {
        return None;
    }
    Some(AemoDispatchArtifact {
        file_name,
        dispatch_interval,
        source_url: source_url.to_string(),
    })
}

fn energy_url_provenance(source_url: &str, kind: AemoArtifactKind) -> Option<AemoEnergyArtifact> {
    let file_name = source_url
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())?
        .to_string();
    let interval = interval_from_file_name(&file_name, kind)?;
    if !source_url.contains(kind.url_path()) {
        return None;
    }
    Some(AemoEnergyArtifact {
        file_name,
        interval,
        source_url: source_url.to_string(),
        kind,
    })
}

fn interval_from_file_name(file_name: &str, kind: AemoArtifactKind) -> Option<DateTime<Utc>> {
    let lower = file_name.to_ascii_lowercase();
    let prefix = kind.file_prefix();
    if !lower.starts_with(&prefix.to_ascii_lowercase()) || !lower.ends_with(".zip") {
        return None;
    }
    let timestamp = file_name.get(prefix.len()..)?.split('_').next()?;
    if timestamp.len() != 12 || !timestamp.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let date_time = NaiveDateTime::parse_from_str(timestamp, "%Y%m%d%H%M").ok()?;
    Some(Utc.from_utc_datetime(&date_time))
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

fn aemo_code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!("invalid AEMO {field} code `{value}`: {err}"))
    })
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

fn decode_html_entities(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&#38;", "&")
        .replace("&quot;", "\"")
        .replace("&#34;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
}

fn cancelled_parse_error() -> AdapterError {
    AdapterError::Validation("AEMO parse cancelled".into())
}

fn parse_worker_error(err: tokio::task::JoinError) -> AdapterError {
    if err.is_panic() {
        AdapterError::FormatDrift("AEMO parse worker panicked".into())
    } else {
        AdapterError::Validation(format!("AEMO parse worker cancelled: {err}"))
    }
}

fn source_id() -> SourceId {
    SourceId::new("aemo").expect("static source id is valid")
}

fn dataflow_id(kind: AemoArtifactKind) -> DataflowId {
    DataflowId::new(kind.dataflow_id()).expect("static dataflow id is valid")
}
