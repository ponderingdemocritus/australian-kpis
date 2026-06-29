//! ASX adapter for public announcements, EOD prices, and market statistics.

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
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use csv_async::AsyncReaderBuilder;
use futures::{StreamExt, stream};
use quick_xml::{Reader as XmlReader, events::Event};
use tokio_util::{io::StreamReader, sync::CancellationToken};

const DEFAULT_MARKET_STATISTICS_URL: &str =
    "https://www.asx.com.au/about/market-statistics/historical-market-statistics";
const DEFAULT_ANNOUNCEMENTS_RSS_URL: &str = "";
const DEFAULT_EOD_CSV_URL: &str = "";
const ANNOUNCEMENTS_PRODUCT_URL: &str =
    "https://www.asx.com.au/connectivity-and-data/information-services/company-news";
const EOD_PRODUCT_URL: &str =
    "https://www.asx.com.au/connectivity-and-data/information-services/reference-data";
const USER_AGENT: &str = concat!("au-kpis-adapter-asx/", env!("CARGO_PKG_VERSION"));
const MARKET_STATISTICS_DATAFLOW_ID: &str = "asx.market_statistics";
const ANNOUNCEMENTS_DATAFLOW_ID: &str = "asx.announcements";
const EOD_DATAFLOW_ID: &str = "asx.eod";
const SOURCE_NAME: &str = "ASX";
const ATTRIBUTION: &str = "Source: ASX";
const LICENSE_NAME: &str = "ASX Terms of Use";
const LICENSE_URL: &str = "https://www.asx.com.au/legals/terms-of-use";

/// ASX public market-statistics adapter.
#[derive(Debug, Clone)]
pub struct AsxAdapter {
    manifest: AdapterManifest,
    market_statistics_url: String,
    announcements_rss_url: String,
    eod_csv_url: String,
}

impl Default for AsxAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl AsxAdapter {
    /// Start building an ASX adapter.
    #[must_use]
    pub fn builder() -> AsxAdapterBuilder {
        AsxAdapterBuilder::default()
    }

    /// Convert the public market-statistics page into the current fetch job.
    #[must_use]
    pub fn current_jobs_with_started_at(
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        current_jobs_for_urls(
            DEFAULT_MARKET_STATISTICS_URL,
            DEFAULT_ANNOUNCEMENTS_RSS_URL,
            DEFAULT_EOD_CSV_URL,
            started_at,
            trace_parent,
        )
    }

    /// Diff the current monthly page revision against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        market_statistics_url: &str,
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        discoverable_jobs_for_urls(
            market_statistics_url,
            DEFAULT_ANNOUNCEMENTS_RSS_URL,
            DEFAULT_EOD_CSV_URL,
            known_revisions,
            started_at,
            trace_parent,
        )
    }

    /// Static metadata for ASX market-statistics observations.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![
            Dataflow {
                id: market_statistics_dataflow_id(),
                source_id: source_id(),
                name: "ASX historical market statistics".into(),
                description: Some(
                    "Monthly ASX index, market capitalisation, and listed-entity counts from the public historical market statistics page."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("metric").expect("static dimension id is valid")
                ],
                measures: vec![MeasureId::new("value").expect("static measure id is valid")],
                frequency: Frequency::Monthly,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: DEFAULT_MARKET_STATISTICS_URL.into(),
            },
            Dataflow {
                id: announcements_dataflow_id(),
                source_id: source_id(),
                name: "ASX company announcements".into(),
                description: Some(
                    "Timestamped ASX announcement-feed observations when a licensed or otherwise configured feed URL is supplied."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("symbol").expect("static dimension id is valid"),
                    DimensionId::new("category").expect("static dimension id is valid"),
                ],
                measures: vec![
                    MeasureId::new("announcement_count").expect("static measure id is valid"),
                ],
                frequency: Frequency::Irregular,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: ANNOUNCEMENTS_PRODUCT_URL.into(),
            },
            Dataflow {
                id: eod_dataflow_id(),
                source_id: source_id(),
                name: "ASX end-of-day prices".into(),
                description: Some(
                    "Daily ASX open, high, low, close, and volume observations when a licensed or otherwise configured EOD CSV URL is supplied."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("symbol").expect("static dimension id is valid"),
                    DimensionId::new("metric").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new("value").expect("static measure id is valid")],
                frequency: Frequency::Daily,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: EOD_PRODUCT_URL.into(),
            },
        ]
    }

    fn market_statistics_url(&self) -> &str {
        &self.market_statistics_url
    }

    fn announcements_rss_url(&self) -> &str {
        &self.announcements_rss_url
    }

    fn eod_csv_url(&self) -> &str {
        &self.eod_csv_url
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<AsxArtifactKind, AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "ASX fetch received job for source `{}`",
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
                "ASX fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        let expected = asx_artifact_kind_for_dataflow(&job.dataflow_id).ok_or_else(|| {
            AdapterError::Validation(format!(
                "ASX fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            ))
        })?;
        let observed = asx_artifact_kind(&job.source_url).ok_or_else(|| {
            AdapterError::Validation(format!(
                "ASX fetch URL `{}` does not identify an ASX artifact kind",
                job.source_url
            ))
        })?;
        if observed != expected {
            return Err(AdapterError::Validation(format!(
                "ASX fetch URL `{}` does not match dataflow `{}`",
                job.source_url,
                job.dataflow_id.as_str()
            )));
        }
        Ok(expected)
    }
}

#[async_trait]
impl SourceAdapter for AsxAdapter {
    fn id(&self) -> &'static str {
        "asx"
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    fn source_metadata(&self) -> Option<Source> {
        Some(Source {
            id: source_id(),
            name: SOURCE_NAME.into(),
            homepage: "https://www.asx.com.au".into(),
            description: Some(
                "Australian Securities Exchange market operator and market-statistics publisher."
                    .into(),
            ),
        })
    }

    fn dataflow_metadata(&self) -> Vec<Dataflow> {
        AsxAdapter::dataflow_metadata(self)
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        Ok(discoverable_jobs_for_urls(
            self.market_statistics_url(),
            self.announcements_rss_url(),
            self.eod_csv_url(),
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
        ))
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id(), job_id = %job.id))]
    async fn fetch(&self, job: DiscoveredJob, ctx: &FetchCtx) -> Result<ArtifactRef, AdapterError> {
        let kind = self.validate_fetch_job(&job)?;
        let response = ctx
            .http
            .execute(
                ctx.http
                    .raw_artifact()
                    .get(&job.source_url)
                    .header("user-agent", USER_AGENT)
                    .header("accept", kind.accept_header()),
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
            .map_or_else(|| kind.default_content_type().to_string(), str::to_string);

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

/// Builder for [`AsxAdapter`].
#[derive(Debug, Clone)]
pub struct AsxAdapterBuilder {
    market_statistics_url: String,
    announcements_rss_url: String,
    eod_csv_url: String,
}

impl Default for AsxAdapterBuilder {
    fn default() -> Self {
        Self {
            market_statistics_url: DEFAULT_MARKET_STATISTICS_URL.into(),
            announcements_rss_url: DEFAULT_ANNOUNCEMENTS_RSS_URL.into(),
            eod_csv_url: DEFAULT_EOD_CSV_URL.into(),
        }
    }
}

impl AsxAdapterBuilder {
    /// Override the public market-statistics page URL.
    #[must_use]
    pub fn market_statistics_url(mut self, url: impl Into<String>) -> Self {
        self.market_statistics_url = url.into();
        self
    }

    /// Override the public announcements RSS URL.
    #[must_use]
    pub fn announcements_rss_url(mut self, url: impl Into<String>) -> Self {
        self.announcements_rss_url = url.into();
        self
    }

    /// Override the public end-of-day CSV URL.
    #[must_use]
    pub fn eod_csv_url(mut self, url: impl Into<String>) -> Self {
        self.eod_csv_url = url.into();
        self
    }

    /// Build an ASX adapter.
    #[must_use]
    pub fn build(self) -> AsxAdapter {
        AsxAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: SOURCE_NAME.into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(30, Duration::from_secs(60))
                    .expect("static ASX rate limit is valid"),
                dataflows: vec![
                    market_statistics_dataflow_id(),
                    announcements_dataflow_id(),
                    eod_dataflow_id(),
                ],
            },
            market_statistics_url: self.market_statistics_url,
            announcements_rss_url: self.announcements_rss_url,
            eod_csv_url: self.eod_csv_url,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsxArtifactKind {
    MarketStatistics,
    Announcements,
    Eod,
}

impl AsxArtifactKind {
    fn dataflow_id(self) -> DataflowId {
        match self {
            Self::MarketStatistics => market_statistics_dataflow_id(),
            Self::Announcements => announcements_dataflow_id(),
            Self::Eod => eod_dataflow_id(),
        }
    }

    fn job_slug(self) -> &'static str {
        match self {
            Self::MarketStatistics => "market-statistics",
            Self::Announcements => "announcements",
            Self::Eod => "eod",
        }
    }

    fn revision_key(self) -> &'static str {
        match self {
            Self::MarketStatistics => "ASX:market-statistics",
            Self::Announcements => "ASX:announcements",
            Self::Eod => "ASX:eod",
        }
    }

    fn artifact_format(self) -> &'static str {
        match self {
            Self::MarketStatistics => "html",
            Self::Announcements => "rss",
            Self::Eod => "csv",
        }
    }

    fn cadence(self) -> &'static str {
        match self {
            Self::MarketStatistics => "monthly",
            Self::Announcements | Self::Eod => "daily",
        }
    }

    fn accept_header(self) -> &'static str {
        match self {
            Self::MarketStatistics => "text/html,application/xhtml+xml",
            Self::Announcements => "application/rss+xml,application/xml,text/xml,*/*",
            Self::Eod => "text/csv,text/plain,*/*",
        }
    }

    fn default_content_type(self) -> &'static str {
        match self {
            Self::MarketStatistics => "text/html",
            Self::Announcements => "application/rss+xml",
            Self::Eod => "text/csv",
        }
    }

    fn revision_version(self, started_at: DateTime<Utc>) -> String {
        match self {
            Self::MarketStatistics => format!("{}-{:02}", started_at.year(), started_at.month()),
            Self::Announcements | Self::Eod => started_at.format("%Y-%m-%d").to_string(),
        }
    }
}

fn asx_artifact_kind_for_dataflow(dataflow_id: &DataflowId) -> Option<AsxArtifactKind> {
    match dataflow_id.as_str() {
        MARKET_STATISTICS_DATAFLOW_ID => Some(AsxArtifactKind::MarketStatistics),
        ANNOUNCEMENTS_DATAFLOW_ID => Some(AsxArtifactKind::Announcements),
        EOD_DATAFLOW_ID => Some(AsxArtifactKind::Eod),
        _ => None,
    }
}

fn asx_artifact_kind(source_url: &str) -> Option<AsxArtifactKind> {
    let lower = source_url.to_ascii_lowercase();
    if lower.contains("announcement") || lower.contains("/rss/") {
        return Some(AsxArtifactKind::Announcements);
    }
    if lower.contains("/eod") || lower.contains("end-of-day") || lower.ends_with("eod.csv") {
        return Some(AsxArtifactKind::Eod);
    }
    if lower.contains("market-statistics") || lower.contains("historical-market-statistics") {
        return Some(AsxArtifactKind::MarketStatistics);
    }
    None
}

fn current_jobs_for_urls(
    market_statistics_url: &str,
    announcements_rss_url: &str,
    eod_csv_url: &str,
    started_at: DateTime<Utc>,
    trace_parent: Option<&str>,
) -> Vec<DiscoveredJob> {
    let mut jobs = vec![asx_job(
        AsxArtifactKind::MarketStatistics,
        market_statistics_url,
        started_at,
        trace_parent,
    )];
    if !announcements_rss_url.trim().is_empty() {
        jobs.push(asx_job(
            AsxArtifactKind::Announcements,
            announcements_rss_url,
            started_at,
            trace_parent,
        ));
    }
    if !eod_csv_url.trim().is_empty() {
        jobs.push(asx_job(
            AsxArtifactKind::Eod,
            eod_csv_url,
            started_at,
            trace_parent,
        ));
    }
    jobs
}

fn discoverable_jobs_for_urls(
    market_statistics_url: &str,
    announcements_rss_url: &str,
    eod_csv_url: &str,
    known_revisions: &BTreeMap<String, UpstreamRevision>,
    started_at: DateTime<Utc>,
    trace_parent: Option<&str>,
) -> Vec<DiscoveredJob> {
    current_jobs_for_urls(
        market_statistics_url,
        announcements_rss_url,
        eod_csv_url,
        started_at,
        trace_parent,
    )
    .into_iter()
    .filter(|job| {
        let known = known_revisions.get(&job.metadata["revision_key"]);
        let revision = UpstreamRevision::new(
            job.metadata["revision_version"].clone(),
            Some(job.source_url.clone()),
        );
        known.is_none_or(|known| known != &revision)
    })
    .collect()
}

fn parse_artifact_stream(artifact: ArtifactRef, ctx: &ParseCtx) -> ObservationStream<'_> {
    let kind = match validate_parse_artifact(&artifact) {
        Ok(kind) => kind,
        Err(err) => return Box::pin(stream::once(async move { Err(err) })),
    };

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

        let result = parse_asx_artifact(
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

async fn parse_asx_artifact(
    kind: AsxArtifactKind,
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
    let rows = match kind {
        AsxArtifactKind::MarketStatistics => {
            let html = String::from_utf8(bytes).map_err(|err| {
                AdapterError::FormatDrift(format!("ASX market statistics HTML: {err}"))
            })?;
            market_statistics_observations(&html, &artifact, ingested_at)?
        }
        AsxArtifactKind::Announcements => {
            let rss = String::from_utf8(bytes).map_err(|err| {
                AdapterError::FormatDrift(format!("ASX announcements RSS: {err}"))
            })?;
            announcement_observations(&rss, &artifact, ingested_at)?
        }
        AsxArtifactKind::Eod => {
            let records = parse_eod_csv(bytes, cancellation.clone()).await?;
            eod_observations(records, &artifact, ingested_at)?
        }
    };
    for row in rows {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

fn market_statistics_observations(
    html: &str,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let end_start = find_section_start(html, "id=\"end\"", "End of month values")?;
    let number_start = find_section_start(
        html,
        "id=\"number\"",
        "Number of companies and securities listed on ASX",
    )?;
    let end_section = &html[end_start..number_start];
    let number_section = &html[number_start..];
    let mut observations = Vec::new();
    parse_section_tables(
        end_section,
        AsxSection::EndOfMonthValues,
        artifact,
        ingested_at,
        &mut observations,
    )?;
    parse_section_tables(
        number_section,
        AsxSection::ListedCounts,
        artifact,
        ingested_at,
        &mut observations,
    )?;
    if observations.is_empty() {
        return Err(AdapterError::FormatDrift(
            "ASX market statistics page yielded no observations".into(),
        ));
    }
    Ok(observations)
}

fn announcement_observations(
    rss: &str,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let mut reader = XmlReader::from_reader(io::Cursor::new(rss.as_bytes()));
    reader.config_mut().trim_text(true);
    let mut buffer = Vec::new();
    let mut in_item = false;
    let mut field: Option<Vec<u8>> = None;
    let mut item = AnnouncementItem::default();
    let mut observations = Vec::new();

    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element)) if element.local_name().as_ref() == b"item" => {
                in_item = true;
                item = AnnouncementItem::default();
            }
            Ok(Event::Start(element)) if in_item => {
                field = Some(element.local_name().as_ref().to_vec());
            }
            Ok(Event::Text(text)) if in_item => {
                if let Some(field) = field.as_deref() {
                    item.apply_field(field, xml_text(text.as_ref()));
                }
            }
            Ok(Event::CData(text)) if in_item => {
                if let Some(field) = field.as_deref() {
                    item.apply_field(field, xml_text(text.as_ref()));
                }
            }
            Ok(Event::End(element)) if element.local_name().as_ref() == b"item" => {
                push_announcement_observation(&item, artifact, ingested_at, &mut observations)?;
                in_item = false;
                field = None;
            }
            Ok(Event::End(_)) if in_item => {
                field = None;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => {
                return Err(AdapterError::FormatDrift(format!(
                    "ASX announcements RSS is malformed XML: {err}"
                )));
            }
        }
        buffer.clear();
    }

    if observations.is_empty() {
        return Err(AdapterError::FormatDrift(
            "ASX announcements RSS yielded no observations".into(),
        ));
    }
    Ok(observations)
}

#[derive(Debug, Default)]
struct AnnouncementItem {
    title: Option<String>,
    link: Option<String>,
    guid: Option<String>,
    pub_date: Option<String>,
    category: Option<String>,
    symbol: Option<String>,
}

impl AnnouncementItem {
    fn apply_field(&mut self, field: &[u8], value: String) {
        if value.is_empty() {
            return;
        }
        match field {
            b"title" => self.title = Some(value),
            b"link" => self.link = Some(value),
            b"guid" => self.guid = Some(value),
            b"pubDate" | b"pubdate" => self.pub_date = Some(value),
            b"category" => self.category = Some(value),
            b"code" | b"asxCode" | b"asxcode" => self.symbol = Some(value),
            _ => {}
        }
    }
}

fn push_announcement_observation(
    item: &AnnouncementItem,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
    observations: &mut Vec<(SeriesDescriptor, Observation)>,
) -> Result<(), AdapterError> {
    let title = item.title.as_deref().unwrap_or("untitled ASX announcement");
    let symbol = item
        .symbol
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| title.split([' ', '-']).next())
        .unwrap_or("UNKNOWN")
        .trim()
        .to_ascii_uppercase();
    let category = item.category.as_deref().unwrap_or("uncategorised");
    let time = parse_announcement_time(item.pub_date.as_deref().ok_or_else(|| {
        AdapterError::FormatDrift(format!("ASX announcement `{title}` is missing pubDate"))
    })?)?;
    let dataflow_id = announcements_dataflow_id();
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("symbol").expect("static dimension id is valid"),
            asx_code_id("symbol", &symbol)?,
        ),
        (
            DimensionId::new("category").expect("static dimension id is valid"),
            code_id_from_label("category", category)?,
        ),
    ]);
    let measure_id = MeasureId::new("announcement_count").expect("static measure id is valid");
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
        unit: "count".into(),
    };
    let mut attributes = BTreeMap::from([
        ("source".into(), SOURCE_NAME.into()),
        ("source_url".into(), artifact.source_url.clone()),
        ("license".into(), LICENSE_NAME.into()),
        ("license_url".into(), LICENSE_URL.into()),
        ("announcement_title".into(), title.to_string()),
        ("announcement_category".into(), category.to_string()),
    ]);
    if let Some(link) = item.link.as_deref() {
        attributes.insert("announcement_url".into(), link.to_string());
    }
    if let Some(guid) = item.guid.as_deref() {
        attributes.insert("announcement_guid".into(), guid.to_string());
    }
    let observation = Observation {
        series_key,
        time,
        time_precision: TimePrecision::Minute,
        value: Some(1.0),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes,
        ingested_at,
        source_artifact_id: artifact.id,
    };
    observations.push((descriptor, observation));
    Ok(())
}

async fn parse_eod_csv(
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

fn eod_observations(
    rows: Vec<Vec<String>>,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let Some(header) = rows.first() else {
        return Err(AdapterError::FormatDrift("ASX EOD CSV is empty".into()));
    };
    let header = header
        .iter()
        .enumerate()
        .map(|(index, name)| (normalized_label(name), index))
        .collect::<BTreeMap<_, _>>();
    let mut observations = Vec::new();
    for row in rows.into_iter().skip(1) {
        if row.iter().all(|cell| cell.is_empty()) {
            continue;
        }
        let date = eod_field(&row, &header, "date")?;
        let symbol = eod_field(&row, &header, "symbol")?.to_ascii_uppercase();
        let time = parse_eod_date(date)?;
        for metric in EOD_METRICS {
            push_eod_observation(
                &symbol,
                time,
                metric,
                eod_field(&row, &header, metric.header)?,
                artifact,
                ingested_at,
                &mut observations,
            )?;
        }
    }
    if observations.is_empty() {
        return Err(AdapterError::FormatDrift(
            "ASX EOD CSV yielded no observations".into(),
        ));
    }
    Ok(observations)
}

#[derive(Debug, Clone, Copy)]
struct EodMetric {
    id: &'static str,
    header: &'static str,
    unit: &'static str,
}

const EOD_METRICS: [EodMetric; 5] = [
    EodMetric {
        id: "open",
        header: "open",
        unit: "AUD",
    },
    EodMetric {
        id: "high",
        header: "high",
        unit: "AUD",
    },
    EodMetric {
        id: "low",
        header: "low",
        unit: "AUD",
    },
    EodMetric {
        id: "close",
        header: "close",
        unit: "AUD",
    },
    EodMetric {
        id: "volume",
        header: "volume",
        unit: "shares",
    },
];

fn push_eod_observation(
    symbol: &str,
    time: DateTime<Utc>,
    metric: EodMetric,
    raw_value: &str,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
    observations: &mut Vec<(SeriesDescriptor, Observation)>,
) -> Result<(), AdapterError> {
    let value = parse_number(raw_value)?;
    let dataflow_id = eod_dataflow_id();
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("symbol").expect("static dimension id is valid"),
            asx_code_id("symbol", symbol)?,
        ),
        (
            DimensionId::new("metric").expect("static dimension id is valid"),
            CodeId::new(metric.id).expect("static code id is valid"),
        ),
    ]);
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
        unit: metric.unit.into(),
    };
    let observation = Observation {
        series_key,
        time,
        time_precision: TimePrecision::Day,
        value: Some(value),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes: BTreeMap::from([
            ("source".into(), SOURCE_NAME.into()),
            ("source_url".into(), artifact.source_url.clone()),
            ("license".into(), LICENSE_NAME.into()),
            ("license_url".into(), LICENSE_URL.into()),
            ("asx_section".into(), "eod".into()),
        ]),
        ingested_at,
        source_artifact_id: artifact.id,
    };
    observations.push((descriptor, observation));
    Ok(())
}

fn parse_section_tables(
    section: &str,
    section_kind: AsxSection,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
    observations: &mut Vec<(SeriesDescriptor, Observation)>,
) -> Result<(), AdapterError> {
    let mut offset = 0;
    while let Some(table_start) = find_ascii_case_insensitive(&section[offset..], "<table") {
        let absolute_start = offset + table_start;
        let after_start = &section[absolute_start..];
        let Some(table_end) = find_ascii_case_insensitive(after_start, "</table>") else {
            return Err(AdapterError::FormatDrift(
                "ASX market statistics table is missing closing tag".into(),
            ));
        };
        let absolute_end = absolute_start + table_end + "</table>".len();
        let table = &section[absolute_start..absolute_end];
        let Some(year) = find_year_before(&section[..absolute_start]) else {
            offset = absolute_end;
            continue;
        };
        let rows = extract_table_rows(table);
        if rows.len() > 1 {
            parse_table_rows(
                rows,
                year,
                section_kind,
                artifact,
                ingested_at,
                observations,
            )?;
        }
        offset = absolute_end;
    }
    Ok(())
}

fn parse_table_rows(
    rows: Vec<Vec<String>>,
    year: i32,
    section_kind: AsxSection,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
    observations: &mut Vec<(SeriesDescriptor, Observation)>,
) -> Result<(), AdapterError> {
    let header = rows[0]
        .iter()
        .map(|cell| normalized_label(cell))
        .collect::<Vec<_>>();
    match section_kind {
        AsxSection::EndOfMonthValues if header.iter().any(|cell| cell.contains("all ords")) => {
            for row in rows.into_iter().skip(1).filter(|row| row.len() >= 4) {
                let time = month_start(year, &row[0])?;
                push_observation(
                    AsxMetricValue {
                        metric: "all_ords_price_index",
                        unit: "index",
                        raw_value: &row[1],
                        time,
                        section_kind,
                    },
                    artifact,
                    ingested_at,
                    observations,
                )?;
                push_observation(
                    AsxMetricValue {
                        metric: "sp_asx_200_price_index",
                        unit: "index",
                        raw_value: &row[2],
                        time,
                        section_kind,
                    },
                    artifact,
                    ingested_at,
                    observations,
                )?;
                push_observation(
                    AsxMetricValue {
                        metric: "market_cap_aud_m",
                        unit: "AUD millions",
                        raw_value: &row[3],
                        time,
                        section_kind,
                    },
                    artifact,
                    ingested_at,
                    observations,
                )?;
            }
        }
        AsxSection::ListedCounts if header.iter().any(|cell| cell.contains("total")) => {
            for row in rows.into_iter().skip(1).filter(|row| row.len() >= 3) {
                let time = month_start(year, &row[0])?;
                push_observation(
                    AsxMetricValue {
                        metric: "listed_companies_total",
                        unit: "count",
                        raw_value: &row[1],
                        time,
                        section_kind,
                    },
                    artifact,
                    ingested_at,
                    observations,
                )?;
                push_observation(
                    AsxMetricValue {
                        metric: "all_listed_entities",
                        unit: "count",
                        raw_value: &row[2],
                        time,
                        section_kind,
                    },
                    artifact,
                    ingested_at,
                    observations,
                )?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn push_observation(
    input: AsxMetricValue<'_>,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
    observations: &mut Vec<(SeriesDescriptor, Observation)>,
) -> Result<(), AdapterError> {
    let value = parse_number(input.raw_value)?;
    let dimensions = BTreeMap::from([(
        DimensionId::new("metric").expect("static dimension id is valid"),
        asx_code_id("metric", input.metric)?,
    )]);
    let dataflow_id = market_statistics_dataflow_id();
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
        unit: input.unit.into(),
    };
    let observation = Observation {
        series_key,
        time: input.time,
        time_precision: TimePrecision::Month,
        value: Some(value),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes: BTreeMap::from([
            ("source".into(), SOURCE_NAME.into()),
            ("source_url".into(), artifact.source_url.clone()),
            ("license".into(), LICENSE_NAME.into()),
            ("license_url".into(), LICENSE_URL.into()),
            ("asx_section".into(), input.section_kind.as_str().into()),
        ]),
        ingested_at,
        source_artifact_id: artifact.id,
    };
    observations.push((descriptor, observation));
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct AsxMetricValue<'a> {
    metric: &'static str,
    unit: &'static str,
    raw_value: &'a str,
    time: DateTime<Utc>,
    section_kind: AsxSection,
}

#[derive(Debug, Clone, Copy)]
enum AsxSection {
    EndOfMonthValues,
    ListedCounts,
}

impl AsxSection {
    fn as_str(self) -> &'static str {
        match self {
            Self::EndOfMonthValues => "end_of_month_values",
            Self::ListedCounts => "listed_counts",
        }
    }
}

fn extract_table_rows(table: &str) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let mut offset = 0;
    while let Some(row_start) = find_ascii_case_insensitive(&table[offset..], "<tr") {
        let absolute_start = offset + row_start;
        let Some(row_open_end) = table[absolute_start..].find('>') else {
            break;
        };
        let content_start = absolute_start + row_open_end + 1;
        let Some(row_end) = find_ascii_case_insensitive(&table[content_start..], "</tr>") else {
            break;
        };
        let content_end = content_start + row_end;
        let row = extract_cells(&table[content_start..content_end]);
        if !row.is_empty() {
            rows.push(row);
        }
        offset = content_end + "</tr>".len();
    }
    rows
}

fn extract_cells(row: &str) -> Vec<String> {
    let mut cells = Vec::new();
    let mut offset = 0;
    loop {
        let td = find_ascii_case_insensitive(&row[offset..], "<td");
        let th = find_ascii_case_insensitive(&row[offset..], "<th");
        let Some(cell_start) = (match (td, th) {
            (Some(td), Some(th)) => Some(td.min(th)),
            (Some(td), None) => Some(td),
            (None, Some(th)) => Some(th),
            (None, None) => None,
        }) else {
            break;
        };
        let absolute_start = offset + cell_start;
        let Some(open_end) = row[absolute_start..].find('>') else {
            break;
        };
        let content_start = absolute_start + open_end + 1;
        let end_td = find_ascii_case_insensitive(&row[content_start..], "</td>");
        let end_th = find_ascii_case_insensitive(&row[content_start..], "</th>");
        let Some(cell_end_rel) = (match (end_td, end_th) {
            (Some(td), Some(th)) => Some(td.min(th)),
            (Some(td), None) => Some(td),
            (None, Some(th)) => Some(th),
            (None, None) => None,
        }) else {
            break;
        };
        let content_end = content_start + cell_end_rel;
        let text = strip_tags(&row[content_start..content_end]);
        if !text.is_empty() {
            cells.push(text);
        }
        offset = content_end;
    }
    cells
}

fn strip_tags(input: &str) -> String {
    let mut output = String::new();
    let mut in_tag = false;
    for character in input.chars() {
        match character {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                output.push(' ');
            }
            _ if !in_tag => output.push(character),
            _ => {}
        }
    }
    decode_html_entities(&output)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn decode_html_entities(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&#38;", "&")
        .replace("&nbsp;", " ")
        .replace("&#160;", " ")
        .replace("&quot;", "\"")
        .replace("&#34;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
}

fn find_section_start(html: &str, id_marker: &str, heading: &str) -> Result<usize, AdapterError> {
    html.find(id_marker)
        .or_else(|| html.find(heading))
        .ok_or_else(|| AdapterError::FormatDrift(format!("ASX page missing `{heading}` section")))
}

fn find_year_before(prefix: &str) -> Option<i32> {
    let marker = "dc:title&#34;:&#34;";
    let marker_start = prefix.rfind(marker)?;
    let after_marker = &prefix[marker_start + marker.len()..];
    let digits = after_marker.chars().take(4).collect::<String>();
    if digits.len() == 4 && digits.chars().all(|character| character.is_ascii_digit()) {
        digits.parse().ok()
    } else {
        None
    }
}

fn find_ascii_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    let lower_haystack = haystack.to_ascii_lowercase();
    let lower_needle = needle.to_ascii_lowercase();
    lower_haystack.find(&lower_needle)
}

fn month_start(year: i32, month: &str) -> Result<DateTime<Utc>, AdapterError> {
    let label = normalized_label(month);
    let (resolved_year, month) = if let Some(compact) = compact_month_with_year(&label, year) {
        compact
    } else {
        (year, month_number(&label)?)
    };
    let date = NaiveDate::from_ymd_opt(resolved_year, month, 1).ok_or_else(|| {
        AdapterError::FormatDrift(format!("invalid ASX date `{resolved_year}-{month}`"))
    })?;
    Ok(Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight is valid")))
}

fn compact_month_with_year(label: &str, tab_year: i32) -> Option<(i32, u32)> {
    let compact = label
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>();
    if compact.len() != 5 {
        return None;
    }
    let (month_label, year_suffix) = compact.split_at(3);
    if !month_label
        .chars()
        .all(|character| character.is_ascii_alphabetic())
        || !year_suffix
            .chars()
            .all(|character| character.is_ascii_digit())
    {
        return None;
    }
    let month = month_number(month_label).ok()?;
    let suffix = year_suffix.parse::<i32>().ok()?;
    let century = (tab_year / 100) * 100;
    let mut year = century + suffix;
    if year > tab_year + 1 {
        year -= 100;
    }
    Some((year, month))
}

fn month_number(label: &str) -> Result<u32, AdapterError> {
    match label {
        "january" | "jan" => Ok(1),
        "february" | "feb" => Ok(2),
        "march" | "mar" => Ok(3),
        "april" | "apr" => Ok(4),
        "may" => Ok(5),
        "june" | "jun" => Ok(6),
        "july" | "jul" => Ok(7),
        "august" | "aug" => Ok(8),
        "september" | "sep" => Ok(9),
        "october" | "oct" => Ok(10),
        "november" | "nov" => Ok(11),
        "december" | "dec" => Ok(12),
        other => Err(AdapterError::FormatDrift(format!(
            "invalid ASX market statistics month `{other}`"
        ))),
    }
}

fn normalized_label(value: &str) -> String {
    decode_html_entities(value)
        .to_ascii_lowercase()
        .replace('*', "")
        .replace('&', "and")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_number(value: &str) -> Result<f64, AdapterError> {
    let normalized = value.trim().trim_start_matches('$').replace([',', ' '], "");
    normalized
        .parse::<f64>()
        .map_err(|_| AdapterError::FormatDrift(format!("invalid ASX numeric value `{value}`")))
}

fn parse_announcement_time(value: &str) -> Result<DateTime<Utc>, AdapterError> {
    DateTime::parse_from_rfc2822(value)
        .or_else(|_| DateTime::parse_from_rfc3339(value))
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| AdapterError::FormatDrift(format!("invalid ASX announcement time `{value}`")))
}

fn parse_eod_date(value: &str) -> Result<DateTime<Utc>, AdapterError> {
    let date = NaiveDate::parse_from_str(value.trim(), "%Y-%m-%d")
        .map_err(|_| AdapterError::FormatDrift(format!("invalid ASX EOD date `{value}`")))?;
    Ok(Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight is valid")))
}

fn eod_field<'a>(
    row: &'a [String],
    header: &BTreeMap<String, usize>,
    name: &str,
) -> Result<&'a str, AdapterError> {
    let index = header
        .get(name)
        .ok_or_else(|| AdapterError::FormatDrift(format!("ASX EOD CSV missing `{name}`")))?;
    row.get(*index)
        .map(String::as_str)
        .ok_or_else(|| AdapterError::FormatDrift(format!("ASX EOD CSV missing `{name}`")))
}

fn xml_text(bytes: &[u8]) -> String {
    decode_html_entities(&String::from_utf8_lossy(bytes))
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn validate_parse_artifact(artifact: &ArtifactRef) -> Result<AsxArtifactKind, AdapterError> {
    if artifact.source_id.as_str() != "asx" {
        return Err(AdapterError::Validation(format!(
            "ASX parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    asx_artifact_kind(&artifact.source_url).ok_or_else(|| {
        AdapterError::Validation(format!(
            "ASX parse artifact `{}` is missing ASX dataflow provenance",
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
            "ASX parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )));
    }

    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "ASX parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )))
    }
}

fn asx_job(
    kind: AsxArtifactKind,
    source_url: &str,
    started_at: DateTime<Utc>,
    trace_parent: Option<&str>,
) -> DiscoveredJob {
    let revision_version = kind.revision_version(started_at);
    DiscoveredJob {
        id: format!("asx:{}:{revision_version}", kind.job_slug()),
        source_id: source_id(),
        dataflow_id: kind.dataflow_id(),
        source_url: source_url.into(),
        trace_parent: trace_parent.map(str::to_owned),
        metadata: BTreeMap::from([
            ("artifact_format".into(), kind.artifact_format().into()),
            ("revision_key".into(), kind.revision_key().into()),
            ("revision_version".into(), revision_version),
            ("cadence".into(), kind.cadence().into()),
            ("attribution".into(), ATTRIBUTION.into()),
            ("license".into(), LICENSE_NAME.into()),
            ("license_url".into(), LICENSE_URL.into()),
        ]),
    }
}

fn asx_code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!("invalid ASX {field} code `{value}`: {err}"))
    })
}

fn code_id_from_label(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    let code = normalized_label(value)
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect::<String>()
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_");
    let code = if code.is_empty() {
        "unknown".to_string()
    } else {
        code
    };
    asx_code_id(field, &code)
}

fn cancelled_parse_error() -> AdapterError {
    AdapterError::Validation("ASX parse cancelled".into())
}

fn source_id() -> SourceId {
    SourceId::new("asx").expect("static source id is valid")
}

fn market_statistics_dataflow_id() -> DataflowId {
    DataflowId::new(MARKET_STATISTICS_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn announcements_dataflow_id() -> DataflowId {
    DataflowId::new(ANNOUNCEMENTS_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn eod_dataflow_id() -> DataflowId {
    DataflowId::new(EOD_DATAFLOW_ID).expect("static dataflow id is valid")
}
