//! ASX adapter for market announcements feeds and end-of-day CSV artifacts.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{borrow::Cow, collections::BTreeMap, io, time::Duration};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterManifest, ArtifactRef, DiscoveredJob, DiscoveryCtx, FetchCtx,
    ObservationStream, ParseCtx, RateLimit, SourceAdapter, UpstreamRevision,
    capture_response_headers, retry_after_delta,
};
use au_kpis_domain::{
    Artifact, CodeId, Dataflow, DataflowId, DimensionId, Frequency, License, MeasureId,
    Observation, ObservationStatus, SeriesDescriptor, SeriesKey, SourceId, TimePrecision,
};
use au_kpis_error::CoreError;
use au_kpis_storage::{BlobStore, StorageKey};
use chrono::{DateTime, NaiveDate, SecondsFormat, TimeZone, Utc};
use csv_async::AsyncReaderBuilder;
use futures::{StreamExt, TryStreamExt, stream};
use quick_xml::{Reader as XmlReader, escape::unescape, events::Event};
use serde::Deserialize;
use tokio_util::io::StreamReader;

const DEFAULT_ANNOUNCEMENTS_FEED_URL: &str = "https://asx.api.markitdigital.com/asx-research/1.0/markets/announcements?itemsPerPage=100&page=0";
const DEFAULT_EOD_FILE_URL: &str = "https://www.asxonline.com/referencepoint/eod/latest.csv";
const ANNOUNCEMENT_FILE_BASE_URL: &str = "https://asx.api.markitdigital.com/asx-research/1.0/file";
const USER_AGENT: &str = concat!("au-kpis-adapter-asx/", env!("CARGO_PKG_VERSION"));
const ANNOUNCEMENTS_DATAFLOW_ID: &str = "asx.announcements";
const EOD_DATAFLOW_ID: &str = "asx.eod";
const ATTRIBUTION: &str = "Source: ASX";
const LICENSE_NAME: &str = "ASX market data terms";
const LICENSE_URL: &str = "https://www.asx.com.au/about/terms-use";

/// Stored revision type for ASX feed and file jobs.
pub type AsxRevision = UpstreamRevision;

/// One announcement item parsed from the ASX announcements feed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsxAnnouncement {
    /// Stable announcement id from RSS `guid` or the linked PDF stem.
    pub announcement_id: String,
    /// Resolved ASX ticker code.
    pub ticker: String,
    /// Announcement title.
    pub title: String,
    /// Optional announcement category.
    pub category: Option<String>,
    /// Published timestamp from the announcement feed.
    pub published_at: DateTime<Utc>,
    /// Optional market-sensitive marker exposed by the feed.
    pub market_sensitive: Option<bool>,
    /// Announcement PDF or canonical announcement URL.
    pub source_url: String,
    /// Optional announcement description.
    pub description: Option<String>,
}

/// One end-of-day CSV file to fetch and parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsxEodFile {
    /// CSV source URL.
    pub source_url: String,
    /// Trading date covered by the CSV file.
    pub trading_date: NaiveDate,
    /// Timestamp used as the upstream revision marker.
    pub published_at: DateTime<Utc>,
}

impl AsxEodFile {
    /// Construct one ASX EOD CSV file descriptor.
    #[must_use]
    pub fn new(
        source_url: impl Into<String>,
        trading_date: NaiveDate,
        published_at: DateTime<Utc>,
    ) -> Self {
        Self {
            source_url: source_url.into(),
            trading_date,
            published_at,
        }
    }

    fn revision_key(&self) -> String {
        format!("ASX:EOD:{}", self.trading_date)
    }

    fn revision(&self) -> UpstreamRevision {
        UpstreamRevision::new(
            utc_rfc3339(self.published_at),
            Some(utc_rfc3339(self.published_at)),
        )
    }

    fn to_discovered_job(&self, trace_parent: Option<&str>) -> DiscoveredJob {
        let revision = self.revision();
        let revision_version = revision.version().to_string();
        let revision_key = self.revision_key();
        DiscoveredJob {
            id: format!("asx-eod-{}", self.trading_date),
            source_id: source_id(),
            dataflow_id: eod_dataflow_id(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("adapter".into(), "asx".into()),
                ("artifact_kind".into(), "eod_csv".into()),
                ("attribution".into(), ATTRIBUTION.into()),
                ("cadence".into(), "daily".into()),
                ("dataflow_id".into(), EOD_DATAFLOW_ID.into()),
                ("license".into(), LICENSE_NAME.into()),
                ("license_url".into(), LICENSE_URL.into()),
                ("published_at".into(), utc_rfc3339(self.published_at)),
                ("revision_key".into(), revision_key),
                ("revision_version".into(), revision_version),
                ("trading_date".into(), self.trading_date.to_string()),
            ]),
        }
    }
}

/// ASX announcements and end-of-day pricing adapter.
#[derive(Debug, Clone)]
pub struct AsxAdapter {
    manifest: AdapterManifest,
    announcements_feed_url: String,
    eod_file_url: String,
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

    /// Parse an ASX announcements feed into announcement items.
    pub fn parse_announcements_feed(body: &str) -> Result<Vec<AsxAnnouncement>, AdapterError> {
        parse_announcements_feed(body)
    }

    /// Convert current ASX feed/file state into jobs without known-revision filtering.
    #[must_use]
    pub fn current_jobs_with_started_at(
        announcements: &[AsxAnnouncement],
        eod_files: &[AsxEodFile],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(
            announcements,
            eod_files,
            &BTreeMap::new(),
            started_at,
            None,
        )
    }

    /// Diff current ASX feed/file state against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        announcements: &[AsxAnnouncement],
        eod_files: &[AsxEodFile],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        let mut jobs = Vec::new();
        if let Some(job) =
            announcements_feed_job(announcements, known_revisions, started_at, trace_parent)
        {
            jobs.push(job);
        }
        jobs.extend(
            eod_files
                .iter()
                .filter_map(|file| {
                    let revision = file.revision();
                    known_revisions
                        .get(&file.revision_key())
                        .is_none_or(|known| known != &revision)
                        .then(|| file.to_discovered_job(trace_parent))
                })
                .collect::<Vec<_>>(),
        );
        jobs
    }

    /// Static metadata for the dataflows emitted by the ASX adapter.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![
            Dataflow {
                id: announcements_dataflow_id(),
                source_id: source_id(),
                name: "ASX market announcements".into(),
                description: Some(
                    "Market announcement events parsed from the ASX announcements feed.".into(),
                ),
                dimensions: vec![
                    DimensionId::new("ticker").expect("static dimension id is valid"),
                    DimensionId::new("announcement_id").expect("static dimension id is valid"),
                    DimensionId::new("category").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new("event_count").expect("static measure id is valid")],
                frequency: Frequency::Irregular,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: self.announcements_feed_url.clone(),
            },
            Dataflow {
                id: eod_dataflow_id(),
                source_id: source_id(),
                name: "ASX end-of-day prices".into(),
                description: Some(
                    "Daily OHLCV rows parsed from the configured ASX end-of-day CSV file feed."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("ticker").expect("static dimension id is valid"),
                    DimensionId::new("entity_name").expect("static dimension id is valid"),
                    DimensionId::new("metric").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new("value").expect("static measure id is valid")],
                frequency: Frequency::Daily,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: self.eod_file_url.clone(),
            },
        ]
    }

    fn announcements_feed_url(&self) -> &str {
        &self.announcements_feed_url
    }

    fn eod_file_url(&self) -> &str {
        &self.eod_file_url
    }

    fn eod_file_for_started_at(&self, started_at: DateTime<Utc>) -> AsxEodFile {
        AsxEodFile::new(self.eod_file_url(), started_at.date_naive(), started_at)
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
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
        let expected_kind = artifact_kind_for_dataflow(&job.dataflow_id)?;
        if job.metadata.get("artifact_kind").map(String::as_str) != Some(expected_kind) {
            return Err(AdapterError::Validation(format!(
                "ASX fetch job `{}` is missing `{expected_kind}` provenance",
                job.id
            )));
        }
        Ok(())
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

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        let want_announcements = requested_or_all(ctx, ANNOUNCEMENTS_DATAFLOW_ID);
        let want_eod = requested_or_all(ctx, EOD_DATAFLOW_ID);
        let mut announcements = Vec::new();
        if want_announcements {
            let response = ctx
                .http
                .execute(
                    ctx.http
                        .raw()
                        .get(self.announcements_feed_url())
                        .header("user-agent", USER_AGENT)
                        .header(
                            "accept",
                            "application/rss+xml,application/xml,text/xml,application/json",
                        ),
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
            let body = response.text().await?;
            announcements = parse_announcements_feed(&body)?;
        }

        let eod_files = want_eod
            .then(|| self.eod_file_for_started_at(ctx.started_at))
            .into_iter()
            .collect::<Vec<_>>();
        let mut jobs = Self::discoverable_jobs_with_started_at(
            &announcements,
            &eod_files,
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
        );
        for job in &mut jobs {
            if job.dataflow_id.as_str() == ANNOUNCEMENTS_DATAFLOW_ID {
                job.source_url.clone_from(&self.announcements_feed_url);
            }
        }
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
                    .header("accept", accept_for_job(&job)),
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
            .map_or_else(|| content_type_for_job(&job).to_string(), str::to_string);

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

fn parse_artifact_stream(artifact: ArtifactRef, ctx: &ParseCtx) -> ObservationStream<'_> {
    let kind = match validate_parse_artifact(&artifact, ctx) {
        Ok(kind) => kind,
        Err(err) => return Box::pin(stream::once(async move { Err(err) })),
    };

    let blob_store = ctx.blob_store.clone();
    let started_at = ctx.started_at;
    let cancellation = ctx.cancellation().clone();
    let (row_tx, row_rx) = tokio::sync::mpsc::channel(1024);

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

        let result = match kind {
            AsxArtifactKind::AnnouncementsRss => {
                parse_announcements_artifact(
                    blob_store,
                    key,
                    artifact,
                    started_at,
                    cancellation,
                    row_tx.clone(),
                )
                .await
            }
            AsxArtifactKind::EodCsv => {
                parse_eod_artifact(
                    blob_store,
                    key,
                    artifact,
                    started_at,
                    cancellation,
                    row_tx.clone(),
                )
                .await
            }
        };
        if let Err(err) = result {
            let _ = row_tx.send(Err(err)).await;
        }
    });

    Box::pin(stream::unfold(row_rx, |mut row_rx| async {
        row_rx.recv().await.map(|item| (item, row_rx))
    }))
}

async fn parse_announcements_artifact(
    blob_store: BlobStore,
    key: StorageKey,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    cancellation: tokio_util::sync::CancellationToken,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    let bytes = read_artifact_bytes(blob_store, key, cancellation).await?;
    let body = std::str::from_utf8(&bytes)
        .map_err(|err| AdapterError::FormatDrift(format!("ASX RSS is not UTF-8: {err}")))?;
    for row in parse_announcement_rows(parse_announcements_feed(body)?, &artifact, ingested_at)? {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

async fn parse_eod_artifact(
    blob_store: BlobStore,
    key: StorageKey,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    cancellation: tokio_util::sync::CancellationToken,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    let chunks = tokio::select! {
        () = cancellation.cancelled() => return Err(cancelled_parse_error()),
        chunks = blob_store.get(&key) => chunks?,
    };
    let io_stream = chunks.map_err(|err| io::Error::other(err.to_string()));
    let reader = StreamReader::new(io_stream);
    let mut csv = AsyncReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .create_reader(reader);
    let header = csv
        .headers()
        .await
        .map_err(|err| AdapterError::FormatDrift(err.to_string()))?
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();
    let header = EodHeader::from_header(&header)?;
    let mut records = csv.records();
    while let Some(record) = tokio::select! {
        () = cancellation.cancelled() => return Err(cancelled_parse_error()),
        record = records.next() => record,
    } {
        let record = record.map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
        for row in parse_eod_record(&record, &header, &artifact, ingested_at)? {
            if tx.send(Ok(row)).await.is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

async fn read_artifact_bytes(
    blob_store: BlobStore,
    key: StorageKey,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<Vec<u8>, AdapterError> {
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
    Ok(bytes)
}

fn parse_announcements_feed(body: &str) -> Result<Vec<AsxAnnouncement>, AdapterError> {
    if body.trim_start().starts_with('{') {
        return parse_announcements_json_feed(body);
    }
    parse_announcements_rss_feed(body)
}

fn parse_announcements_rss_feed(body: &str) -> Result<Vec<AsxAnnouncement>, AdapterError> {
    let mut reader = XmlReader::from_str(body);
    reader.config_mut().trim_text(true);
    let mut current = None::<AnnouncementBuilder>;
    let mut field = None::<String>;
    let mut announcements = Vec::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                let name = local_name(element.local_name().as_ref());
                if name == "item" {
                    current = Some(AnnouncementBuilder::default());
                } else if current.is_some() {
                    field = Some(name);
                }
            }
            Ok(Event::End(element)) => {
                let name = local_name(element.local_name().as_ref());
                if name == "item" {
                    let builder = current.take().ok_or_else(|| {
                        AdapterError::FormatDrift("ASX RSS closed item before opening it".into())
                    })?;
                    announcements.push(builder.finish()?);
                }
                if field.as_deref() == Some(name.as_str()) {
                    field = None;
                }
            }
            Ok(Event::Text(text)) => {
                if let (Some(builder), Some(name)) = (&mut current, field.as_deref()) {
                    builder.push(name, decode_xml_text(text.decode())?);
                }
            }
            Ok(Event::CData(text)) => {
                if let (Some(builder), Some(name)) = (&mut current, field.as_deref()) {
                    builder.push(name, decode_xml_cdata(text.decode())?);
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(err) => {
                return Err(AdapterError::FormatDrift(format!(
                    "ASX RSS XML is malformed: {err}"
                )));
            }
        }
    }
    sort_announcements(&mut announcements);
    Ok(announcements)
}

fn parse_announcements_json_feed(body: &str) -> Result<Vec<AsxAnnouncement>, AdapterError> {
    let envelope: MarketAnnouncementsEnvelope = serde_json::from_str(body).map_err(|err| {
        AdapterError::FormatDrift(format!("ASX announcements JSON is malformed: {err}"))
    })?;
    let mut announcements = envelope
        .data
        .items
        .into_iter()
        .map(MarketAnnouncementItem::into_announcement)
        .collect::<Result<Vec<_>, _>>()?;
    sort_announcements(&mut announcements);
    Ok(announcements)
}

fn sort_announcements(announcements: &mut [AsxAnnouncement]) {
    announcements.sort_by(|left, right| {
        right
            .published_at
            .cmp(&left.published_at)
            .then(left.ticker.cmp(&right.ticker))
            .then(left.announcement_id.cmp(&right.announcement_id))
    });
}

#[derive(Debug, Deserialize)]
struct MarketAnnouncementsEnvelope {
    data: MarketAnnouncementsData,
}

#[derive(Debug, Deserialize)]
struct MarketAnnouncementsData {
    items: Vec<MarketAnnouncementItem>,
}

#[derive(Debug, Deserialize)]
struct MarketAnnouncementCompany {
    #[serde(default, rename = "symbolDisplay")]
    symbol_display: String,
}

#[derive(Debug, Deserialize)]
struct MarketAnnouncementItem {
    #[serde(default, rename = "announcementTypes")]
    announcement_types: Vec<String>,
    #[serde(default)]
    companies: Vec<MarketAnnouncementCompany>,
    date: String,
    #[serde(rename = "documentKey")]
    document_key: String,
    headline: String,
    #[serde(default, rename = "isPriceSensitive")]
    is_price_sensitive: Option<bool>,
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    url: String,
}

impl MarketAnnouncementItem {
    fn into_announcement(self) -> Result<AsxAnnouncement, AdapterError> {
        let title = self.headline.trim();
        if title.is_empty() {
            return Err(AdapterError::FormatDrift(
                "ASX announcement JSON item is missing headline".into(),
            ));
        }
        let announcement_id = self.document_key.trim();
        if announcement_id.is_empty() {
            return Err(AdapterError::FormatDrift(
                "ASX announcement JSON item is missing documentKey".into(),
            ));
        }
        let ticker = normalize_ticker(&self.symbol)
            .or_else(|| {
                self.companies
                    .iter()
                    .find_map(|company| normalize_ticker(&company.symbol_display))
            })
            .ok_or_else(|| {
                AdapterError::FormatDrift(format!(
                    "ASX announcement `{title}` is missing resolvable ticker"
                ))
            })?;
        let category = (!self.announcement_types.is_empty())
            .then(|| self.announcement_types.join(", "))
            .filter(|value| !value.trim().is_empty());
        let source_url = if self.url.trim().is_empty() {
            format!("{ANNOUNCEMENT_FILE_BASE_URL}/{announcement_id}")
        } else {
            self.url.trim().to_string()
        };
        Ok(AsxAnnouncement {
            announcement_id: announcement_id.to_string(),
            ticker,
            title: title.to_string(),
            category,
            published_at: parse_json_datetime(&self.date)?,
            market_sensitive: self.is_price_sensitive,
            source_url,
            description: None,
        })
    }
}

#[derive(Debug, Default)]
struct AnnouncementBuilder {
    title: String,
    link: String,
    guid: String,
    pub_date: String,
    category: Option<String>,
    code: Option<String>,
    market_sensitive: Option<String>,
    description: Option<String>,
}

impl AnnouncementBuilder {
    fn push(&mut self, name: &str, value: String) {
        let value = value.trim();
        if value.is_empty() {
            return;
        }
        match name {
            "title" => append_text(&mut self.title, value),
            "link" => append_text(&mut self.link, value),
            "guid" => append_text(&mut self.guid, value),
            "pubdate" | "published" | "updated" => append_text(&mut self.pub_date, value),
            "category" => append_option_text(&mut self.category, value),
            "code" | "ticker" | "asxcode" => append_option_text(&mut self.code, value),
            "marketsensitive" | "market_sensitive" => {
                append_option_text(&mut self.market_sensitive, value);
            }
            "description" | "summary" => append_option_text(&mut self.description, value),
            _ => {}
        }
    }

    fn finish(self) -> Result<AsxAnnouncement, AdapterError> {
        if self.title.trim().is_empty() {
            return Err(AdapterError::FormatDrift(
                "ASX RSS item is missing title".into(),
            ));
        }
        if self.link.trim().is_empty() {
            return Err(AdapterError::FormatDrift(
                "ASX RSS item is missing link".into(),
            ));
        }
        let published_at = parse_xml_datetime(&self.pub_date)?;
        let announcement_id = if self.guid.trim().is_empty() {
            announcement_id_from_url(&self.link)?
        } else {
            self.guid.trim().to_string()
        };
        let ticker = self
            .code
            .as_deref()
            .and_then(normalize_ticker)
            .or_else(|| extract_ticker_from_title(&self.title))
            .ok_or_else(|| {
                AdapterError::FormatDrift(format!(
                    "ASX announcement `{}` is missing resolvable ticker",
                    self.title
                ))
            })?;
        let market_sensitive = self
            .market_sensitive
            .as_deref()
            .map(parse_bool)
            .transpose()?;
        Ok(AsxAnnouncement {
            announcement_id,
            ticker,
            title: self.title.trim().to_string(),
            category: self.category.map(|value| value.trim().to_string()),
            published_at,
            market_sensitive,
            source_url: self.link.trim().to_string(),
            description: self.description.map(|value| value.trim().to_string()),
        })
    }
}

fn append_text(target: &mut String, value: &str) {
    if !target.is_empty() {
        target.push(' ');
    }
    target.push_str(value);
}

fn append_option_text(target: &mut Option<String>, value: &str) {
    match target {
        Some(existing) => append_text(existing, value),
        None => *target = Some(value.to_string()),
    }
}

fn decode_xml_text(
    decoded: Result<Cow<'_, str>, quick_xml::encoding::EncodingError>,
) -> Result<String, AdapterError> {
    let decoded = decoded.map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
    unescape(&decoded)
        .map(Cow::into_owned)
        .map_err(|err| AdapterError::FormatDrift(err.to_string()))
}

fn decode_xml_cdata(
    decoded: Result<Cow<'_, str>, quick_xml::encoding::EncodingError>,
) -> Result<String, AdapterError> {
    decoded
        .map(Cow::into_owned)
        .map_err(|err| AdapterError::FormatDrift(err.to_string()))
}

fn local_name(name: &[u8]) -> String {
    let value = std::str::from_utf8(name).unwrap_or_default();
    value
        .rsplit(':')
        .next()
        .unwrap_or(value)
        .to_ascii_lowercase()
}

fn parse_xml_datetime(value: &str) -> Result<DateTime<Utc>, AdapterError> {
    if value.trim().is_empty() {
        return Err(AdapterError::FormatDrift(
            "ASX RSS item is missing pubDate".into(),
        ));
    }
    DateTime::parse_from_rfc2822(value)
        .or_else(|_| DateTime::parse_from_rfc3339(value))
        .map(|time| time.with_timezone(&Utc))
        .map_err(|_| AdapterError::FormatDrift(format!("invalid ASX RSS pubDate `{value}`")))
}

fn parse_json_datetime(value: &str) -> Result<DateTime<Utc>, AdapterError> {
    DateTime::parse_from_rfc3339(value.trim())
        .map(|time| time.with_timezone(&Utc))
        .map_err(|_| AdapterError::FormatDrift(format!("invalid ASX announcement date `{value}`")))
}

fn parse_bool(value: &str) -> Result<bool, AdapterError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "1" => Ok(true),
        "false" | "f" | "no" | "n" | "0" => Ok(false),
        _ => Err(AdapterError::FormatDrift(format!(
            "invalid ASX boolean `{value}`"
        ))),
    }
}

fn announcement_id_from_url(source_url: &str) -> Result<String, AdapterError> {
    let path = source_url.split(['?', '#']).next().unwrap_or(source_url);
    let filename = path.rsplit('/').next().unwrap_or(path);
    let stem = filename.split('.').next().unwrap_or(filename).trim();
    if stem.is_empty() {
        Err(AdapterError::FormatDrift(format!(
            "ASX announcement URL `{source_url}` has no announcement id"
        )))
    } else {
        Ok(stem.to_string())
    }
}

fn extract_ticker_from_title(title: &str) -> Option<String> {
    let prefix = title
        .split_once(':')
        .map(|(prefix, _)| prefix)
        .or_else(|| title.split_once('-').map(|(prefix, _)| prefix))?;
    normalize_ticker(prefix)
}

fn normalize_ticker(value: &str) -> Option<String> {
    let ticker = value
        .trim()
        .trim_start_matches("ASX")
        .trim_start_matches(':')
        .trim()
        .to_ascii_uppercase();
    let valid = (2..=6).contains(&ticker.len())
        && ticker
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit());
    valid.then_some(ticker)
}

fn announcements_feed_job(
    announcements: &[AsxAnnouncement],
    known_revisions: &BTreeMap<String, UpstreamRevision>,
    _started_at: DateTime<Utc>,
    trace_parent: Option<&str>,
) -> Option<DiscoveredJob> {
    let latest = announcements.iter().max_by(|left, right| {
        left.published_at
            .cmp(&right.published_at)
            .then(left.announcement_id.cmp(&right.announcement_id))
    })?;
    let revision_key = "ASX:ANNOUNCEMENTS:FEED".to_string();
    let revision_version = format!(
        "{}:{}",
        utc_rfc3339(latest.published_at),
        latest.announcement_id
    );
    let revision = UpstreamRevision::new(
        revision_version.clone(),
        Some(utc_rfc3339(latest.published_at)),
    );
    if known_revisions.get(&revision_key) == Some(&revision) {
        return None;
    }
    Some(DiscoveredJob {
        id: format!("asx-announcements-{revision_version}"),
        source_id: source_id(),
        dataflow_id: announcements_dataflow_id(),
        source_url: DEFAULT_ANNOUNCEMENTS_FEED_URL.into(),
        trace_parent: trace_parent.map(str::to_owned),
        metadata: BTreeMap::from([
            ("adapter".into(), "asx".into()),
            ("artifact_kind".into(), "announcements_rss".into()),
            ("attribution".into(), ATTRIBUTION.into()),
            ("cadence".into(), "realtime".into()),
            ("dataflow_id".into(), ANNOUNCEMENTS_DATAFLOW_ID.into()),
            ("item_count".into(), announcements.len().to_string()),
            (
                "latest_announcement_id".into(),
                latest.announcement_id.clone(),
            ),
            (
                "latest_published_at".into(),
                utc_rfc3339(latest.published_at),
            ),
            ("latest_source_url".into(), latest.source_url.clone()),
            ("latest_ticker".into(), latest.ticker.clone()),
            ("license".into(), LICENSE_NAME.into()),
            ("license_url".into(), LICENSE_URL.into()),
            ("revision_key".into(), revision_key),
            ("revision_version".into(), revision_version),
        ]),
    })
}

fn parse_announcement_rows(
    announcements: Vec<AsxAnnouncement>,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    announcements
        .into_iter()
        .map(|announcement| announcement_row(announcement, artifact, ingested_at))
        .collect()
}

fn announcement_row(
    announcement: AsxAnnouncement,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let category = announcement
        .category
        .clone()
        .unwrap_or_else(|| "Announcement".into());
    let dataflow_id = announcements_dataflow_id();
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("ticker").expect("static dimension id is valid"),
            asx_code_id("ticker", &announcement.ticker)?,
        ),
        (
            DimensionId::new("announcement_id").expect("static dimension id is valid"),
            asx_code_id("announcement id", &announcement.announcement_id)?,
        ),
        (
            DimensionId::new("category").expect("static dimension id is valid"),
            asx_code_id("category", &category)?,
        ),
    ]);
    let series_key = SeriesKey::derive(
        &dataflow_id,
        dimensions
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    let descriptor = SeriesDescriptor {
        series_key,
        dataflow_id,
        measure_id: MeasureId::new("event_count").expect("static measure id is valid"),
        dimensions,
        unit: "announcement".into(),
    };
    let mut attributes = BTreeMap::from([
        ("announcement_id".into(), announcement.announcement_id),
        ("category".into(), category),
        ("source_url".into(), announcement.source_url),
        ("ticker".into(), announcement.ticker),
        ("title".into(), announcement.title),
    ]);
    if let Some(value) = announcement.description {
        attributes.insert("description".into(), value);
    }
    if let Some(value) = announcement.market_sensitive {
        attributes.insert("market_sensitive".into(), value.to_string());
    }
    let observation = Observation {
        series_key,
        time: announcement.published_at,
        time_precision: TimePrecision::Day,
        value: Some(1.0),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes,
        ingested_at,
        source_artifact_id: artifact.id,
    };
    Ok((descriptor, observation))
}

#[derive(Debug)]
struct EodHeader {
    ticker: usize,
    date: usize,
    open: usize,
    high: usize,
    low: usize,
    close: usize,
    volume: usize,
    company_name: usize,
}

impl EodHeader {
    fn from_header(header: &[String]) -> Result<Self, AdapterError> {
        Ok(Self {
            ticker: required_header(header, "ticker")?,
            date: required_header(header, "date")?,
            open: required_header(header, "open")?,
            high: required_header(header, "high")?,
            low: required_header(header, "low")?,
            close: required_header(header, "close")?,
            volume: required_header(header, "volume")?,
            company_name: required_header(header, "company_name")
                .or_else(|_| required_header(header, "entity_name"))?,
        })
    }
}

fn required_header(header: &[String], name: &str) -> Result<usize, AdapterError> {
    header
        .iter()
        .position(|value| value == name)
        .ok_or_else(|| AdapterError::FormatDrift(format!("ASX EOD CSV is missing `{name}` column")))
}

fn parse_eod_record(
    record: &csv_async::StringRecord,
    header: &EodHeader,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let ticker = normalize_ticker(record_cell(record, header.ticker)).ok_or_else(|| {
        AdapterError::FormatDrift(format!(
            "invalid ASX EOD ticker `{}`",
            record_cell(record, header.ticker)
        ))
    })?;
    let time = parse_eod_date(record_cell(record, header.date))?;
    let entity_name = record_cell(record, header.company_name).trim();
    if entity_name.is_empty() {
        return Err(AdapterError::FormatDrift(format!(
            "ASX EOD row for `{ticker}` is missing company_name"
        )));
    }

    let metrics = [
        ("open", header.open, "AUD"),
        ("high", header.high, "AUD"),
        ("low", header.low, "AUD"),
        ("close", header.close, "AUD"),
        ("volume", header.volume, "shares"),
    ];
    metrics
        .into_iter()
        .map(|(metric, index, unit)| {
            eod_row(EodMetricInput {
                ticker: &ticker,
                entity_name,
                metric,
                value: record_cell(record, index),
                unit,
                time,
                artifact,
                ingested_at,
            })
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct EodMetricInput<'a> {
    ticker: &'a str,
    entity_name: &'a str,
    metric: &'a str,
    value: &'a str,
    unit: &'a str,
    time: DateTime<Utc>,
    artifact: &'a ArtifactRef,
    ingested_at: DateTime<Utc>,
}

fn eod_row(input: EodMetricInput<'_>) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let (value, status) = parse_numeric(input.value, input.metric)?;
    let dataflow_id = eod_dataflow_id();
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("ticker").expect("static dimension id is valid"),
            asx_code_id("ticker", input.ticker)?,
        ),
        (
            DimensionId::new("entity_name").expect("static dimension id is valid"),
            asx_code_id("entity name", input.entity_name)?,
        ),
        (
            DimensionId::new("metric").expect("static dimension id is valid"),
            asx_code_id("metric", input.metric)?,
        ),
    ]);
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
        unit: input.unit.into(),
    };
    let observation = Observation {
        series_key,
        time: input.time,
        time_precision: TimePrecision::Day,
        value,
        status,
        revision_no: 0,
        attributes: BTreeMap::from([
            ("company_name".into(), input.entity_name.into()),
            ("metric".into(), input.metric.into()),
            ("source_url".into(), input.artifact.source_url.clone()),
            ("ticker".into(), input.ticker.into()),
            ("trading_date".into(), input.time.date_naive().to_string()),
        ]),
        ingested_at: input.ingested_at,
        source_artifact_id: input.artifact.id,
    };
    Ok((descriptor, observation))
}

fn record_cell(record: &csv_async::StringRecord, index: usize) -> &str {
    record.get(index).unwrap_or("").trim()
}

fn parse_eod_date(value: &str) -> Result<DateTime<Utc>, AdapterError> {
    let date = NaiveDate::parse_from_str(value.trim(), "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(value.trim(), "%d/%m/%Y"))
        .map_err(|_| AdapterError::FormatDrift(format!("invalid ASX EOD date `{value}`")))?;
    Ok(Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight is valid")))
}

fn parse_numeric(
    value: &str,
    metric: &str,
) -> Result<(Option<f64>, ObservationStatus), AdapterError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || matches!(trimmed, "na" | "n/a" | "NA" | "N/A" | "-") {
        return Ok((None, ObservationStatus::Missing));
    }
    trimmed
        .replace(',', "")
        .parse::<f64>()
        .map(|value| (Some(value), ObservationStatus::Normal))
        .map_err(|_| AdapterError::FormatDrift(format!("invalid ASX {metric} value `{value}`")))
}

fn validate_parse_artifact(
    artifact: &ArtifactRef,
    ctx: &ParseCtx,
) -> Result<AsxArtifactKind, AdapterError> {
    if artifact.source_id.as_str() != "asx" {
        return Err(AdapterError::Validation(format!(
            "ASX parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    let expected = ctx.expected_dataflow_id().ok_or_else(|| {
        AdapterError::Validation("ASX parse requires expected dataflow provenance".into())
    })?;
    match expected.as_str() {
        ANNOUNCEMENTS_DATAFLOW_ID => Ok(AsxArtifactKind::AnnouncementsRss),
        EOD_DATAFLOW_ID => Ok(AsxArtifactKind::EodCsv),
        other => Err(AdapterError::Validation(format!(
            "ASX parse received unsupported dataflow `{other}`"
        ))),
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsxArtifactKind {
    AnnouncementsRss,
    EodCsv,
}

fn requested_or_all(ctx: &DiscoveryCtx, dataflow_id: &str) -> bool {
    ctx.requested_dataflow_id()
        .is_none_or(|requested| requested.as_str() == dataflow_id)
}

fn artifact_kind_for_dataflow(dataflow_id: &DataflowId) -> Result<&'static str, AdapterError> {
    match dataflow_id.as_str() {
        ANNOUNCEMENTS_DATAFLOW_ID => Ok("announcements_rss"),
        EOD_DATAFLOW_ID => Ok("eod_csv"),
        other => Err(AdapterError::Validation(format!(
            "unsupported ASX dataflow `{other}`"
        ))),
    }
}

fn accept_for_job(job: &DiscoveredJob) -> &'static str {
    match job.metadata.get("artifact_kind").map(String::as_str) {
        Some("announcements_rss") => {
            "application/rss+xml,application/xml,text/xml,application/json"
        }
        Some("eod_csv") => "text/csv",
        _ => "application/octet-stream",
    }
}

fn content_type_for_job(job: &DiscoveredJob) -> &'static str {
    match job.metadata.get("artifact_kind").map(String::as_str) {
        Some("announcements_rss") => "application/rss+xml",
        Some("eod_csv") => "text/csv",
        _ => "application/octet-stream",
    }
}

fn source_id() -> SourceId {
    SourceId::new("asx").expect("static source id is valid")
}

fn announcements_dataflow_id() -> DataflowId {
    DataflowId::new(ANNOUNCEMENTS_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn eod_dataflow_id() -> DataflowId {
    DataflowId::new(EOD_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn asx_code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!("invalid ASX {field} code `{value}`: {err}"))
    })
}

fn utc_rfc3339(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn cancelled_parse_error() -> AdapterError {
    CoreError::Io(io::Error::new(
        io::ErrorKind::Interrupted,
        "ASX parse cancelled",
    ))
    .into()
}

/// Builder for [`AsxAdapter`].
#[derive(Debug, Clone)]
pub struct AsxAdapterBuilder {
    announcements_feed_url: String,
    eod_file_url: String,
}

impl Default for AsxAdapterBuilder {
    fn default() -> Self {
        Self {
            announcements_feed_url: DEFAULT_ANNOUNCEMENTS_FEED_URL.into(),
            eod_file_url: DEFAULT_EOD_FILE_URL.into(),
        }
    }
}

impl AsxAdapterBuilder {
    /// Override the announcements RSS feed URL.
    #[must_use]
    pub fn announcements_feed_url(mut self, url: impl Into<String>) -> Self {
        self.announcements_feed_url = url.into();
        self
    }

    /// Override the end-of-day CSV file URL.
    #[must_use]
    pub fn eod_file_url(mut self, url: impl Into<String>) -> Self {
        self.eod_file_url = url.into();
        self
    }

    /// Build the adapter.
    #[must_use]
    pub fn build(self) -> AsxAdapter {
        AsxAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: "ASX".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(30, Duration::from_secs(60))
                    .expect("static ASX rate limit is valid"),
                dataflows: vec![announcements_dataflow_id(), eod_dataflow_id()],
            },
            announcements_feed_url: self.announcements_feed_url,
            eod_file_url: self.eod_file_url,
        }
    }
}
