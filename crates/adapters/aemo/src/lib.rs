//! AEMO adapter for high-frequency NEMWeb DispatchIS CSV ZIP artifacts.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{
    collections::BTreeMap,
    io::{Cursor, Read},
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
    Observation, ObservationStatus, SeriesDescriptor, SeriesKey, SourceId, TimePrecision,
};
use au_kpis_error::CoreError;
use au_kpis_storage::StorageKey;
use bytes::Bytes;
use chrono::{DateTime, FixedOffset, NaiveDateTime, SecondsFormat, TimeZone, Utc};
use csv_async::AsyncReaderBuilder;
use futures::{StreamExt, stream};
use tokio_util::io::StreamReader;
use zip::ZipArchive;

const DEFAULT_DISPATCH_LISTING_URL: &str =
    "https://www.nemweb.com.au/Reports/Current/DispatchIS_Reports/";
const USER_AGENT: &str = concat!("au-kpis-adapter-aemo/", env!("CARGO_PKG_VERSION"));
const DATAFLOW_ID: &str = "aemo.dispatch";
const ATTRIBUTION: &str = "Source: Australian Energy Market Operator";
const LICENSE_NAME: &str = "AEMO data terms";
const DISPATCHIS_PREFIX: &str = "PUBLIC_DISPATCHIS_";
const DISPATCHIS_SUFFIX: &str = ".zip";
const CSV_PAYLOAD: &str = "aemo-csv-cid";
const POLL_INTERVAL_SECONDS: i64 = 5 * 60;

/// Freshness SLO for 5-minute AEMO dispatch data, in seconds.
pub const FRESHNESS_SLO_SECONDS: i64 = 15 * 60;

/// Alias used by tests and callers that persist AEMO upstream revisions.
pub type AemoDispatchFileRevision = UpstreamRevision;

/// One DispatchIS ZIP file exposed by the NEMWeb current reports listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AemoDispatchFile {
    /// ZIP file name, e.g. `PUBLIC_DISPATCHIS_202605291110_0000000519886550.zip`.
    pub file_name: String,
    /// Dispatch interval encoded in the file name.
    pub dispatch_interval: DateTime<Utc>,
    /// Listing timestamp shown by NEMWeb for the file.
    pub published_at: DateTime<Utc>,
    /// File size in bytes from the NEMWeb listing.
    pub size_bytes: u64,
    /// AEMO sequence number encoded in the file name.
    pub sequence: String,
    /// Absolute URL used to fetch the ZIP artifact.
    pub source_url: String,
}

impl AemoDispatchFile {
    /// Lag between a discovery start timestamp and the NEMWeb listing timestamp.
    #[must_use]
    pub fn freshness_lag_seconds(&self, started_at: DateTime<Utc>) -> i64 {
        (started_at - self.published_at).num_seconds().max(0)
    }

    fn revision_key(&self) -> String {
        format!(
            "AEMO:DISPATCHIS:{}",
            self.dispatch_interval
                .with_timezone(&aemo_market_offset())
                .format("%Y%m%d%H%M")
        )
    }

    fn revision(&self) -> UpstreamRevision {
        UpstreamRevision::new(self.sequence.clone(), Some(utc_rfc3339(self.published_at)))
    }

    fn to_discovered_job(
        &self,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> DiscoveredJob {
        let mut metadata = BTreeMap::new();
        metadata.insert("artifact_format".into(), "zip".into());
        metadata.insert("cadence".into(), "5min".into());
        metadata.insert("csv_payload".into(), CSV_PAYLOAD.into());
        metadata.insert(
            "dispatch_interval".into(),
            utc_rfc3339(self.dispatch_interval),
        );
        metadata.insert(
            "freshness_lag_seconds".into(),
            self.freshness_lag_seconds(started_at).to_string(),
        );
        metadata.insert(
            "freshness_slo_seconds".into(),
            FRESHNESS_SLO_SECONDS.to_string(),
        );
        metadata.insert(
            "poll_interval_seconds".into(),
            POLL_INTERVAL_SECONDS.to_string(),
        );
        metadata.insert("published_at".into(), utc_rfc3339(self.published_at));
        metadata.insert("revision_key".into(), self.revision_key());
        metadata.insert("revision_version".into(), self.sequence.clone());
        metadata.insert("source_file".into(), self.file_name.clone());
        metadata.insert("upstream_size_bytes".into(), self.size_bytes.to_string());

        DiscoveredJob {
            id: format!(
                "aemo-dispatchis-{}-{}",
                self.dispatch_interval
                    .with_timezone(&aemo_market_offset())
                    .format("%Y%m%d%H%M"),
                self.sequence
            ),
            source_id: source_id(),
            dataflow_id: dataflow_id(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_string),
            metadata,
        }
    }
}

/// AEMO NEMWeb DispatchIS adapter.
#[derive(Debug, Clone)]
pub struct AemoAdapter {
    manifest: AdapterManifest,
    dispatch_listing_url: String,
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

    /// Parse the default NEMWeb DispatchIS listing format into ZIP files.
    pub fn parse_dispatch_listing(body: &str) -> Result<Vec<AemoDispatchFile>, AdapterError> {
        parse_dispatch_listing_with_base(body, DEFAULT_DISPATCH_LISTING_URL)
    }

    /// Convert current DispatchIS files into fetch jobs without known-revision filtering.
    #[must_use]
    pub fn current_jobs_with_started_at(
        current: &[AemoDispatchFile],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Diff current DispatchIS files against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[AemoDispatchFile],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        current
            .iter()
            .filter_map(|file| {
                let revision = file.revision();
                known_revisions
                    .get(&file.revision_key())
                    .is_none_or(|known| known != &revision)
                    .then(|| file.to_discovered_job(started_at, trace_parent))
            })
            .collect()
    }

    /// Static metadata for the AEMO dispatch dataflow.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![Dataflow {
            id: dataflow_id(),
            source_id: source_id(),
            name: "AEMO NEM dispatch".into(),
            description: Some(
                "Five-minute NEM dispatch price and regional summary rows from NEMWeb DispatchIS reports."
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
        }]
    }

    fn dispatch_listing_url(&self) -> &str {
        &self.dispatch_listing_url
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "AEMO fetch received job for source `{}`",
                job.source_id.as_str()
            )));
        }
        if job.dataflow_id != dataflow_id() {
            return Err(AdapterError::Validation(format!(
                "AEMO fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        dispatch_file_provenance(&job.source_url).ok_or_else(|| {
            AdapterError::Validation(format!(
                "AEMO fetch URL `{}` is not a NEMWeb DispatchIS ZIP artifact",
                job.source_url
            ))
        })?;
        Ok(())
    }
}

/// Builder for [`AemoAdapter`].
#[derive(Debug, Clone)]
pub struct AemoAdapterBuilder {
    dispatch_listing_url: String,
}

impl Default for AemoAdapterBuilder {
    fn default() -> Self {
        Self {
            dispatch_listing_url: DEFAULT_DISPATCH_LISTING_URL.into(),
        }
    }
}

impl AemoAdapterBuilder {
    /// Override the DispatchIS current reports listing URL.
    #[must_use]
    pub fn dispatch_listing_url(mut self, url: impl Into<String>) -> Self {
        self.dispatch_listing_url = url.into();
        self
    }

    /// Build an adapter.
    #[must_use]
    pub fn build(self) -> AemoAdapter {
        AemoAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: "Australian Energy Market Operator".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(12, Duration::from_secs(60))
                    .expect("static AEMO rate limit is valid"),
                dataflows: vec![dataflow_id()],
            },
            dispatch_listing_url: self.dispatch_listing_url,
        }
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

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        if ctx
            .requested_dataflow_id()
            .is_some_and(|requested| requested != &dataflow_id())
        {
            return Ok(Vec::new());
        }

        let response = ctx
            .http
            .execute(
                ctx.http
                    .raw()
                    .get(self.dispatch_listing_url())
                    .header("user-agent", USER_AGENT)
                    .header("accept", "text/html,application/xhtml+xml"),
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
        let files = parse_dispatch_listing_with_base(&body, self.dispatch_listing_url())?;
        Ok(Self::discoverable_jobs_with_started_at(
            &files,
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
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
                    .header("accept", "application/zip,application/octet-stream"),
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
    if let Err(err) = validate_parse_artifact(&artifact, ctx.expected_dataflow_id()) {
        return Box::pin(stream::once(async move { Err(err) }));
    }

    let blob_store = ctx.blob_store.clone();
    let started_at = ctx.started_at;
    let metadata = ctx.metadata().clone();
    let cancellation = ctx.cancellation().clone();
    let (row_tx, row_rx) = tokio::sync::mpsc::channel(1024);

    tokio::spawn(async move {
        let error_tx = row_tx.clone();
        let key = StorageKey::from_persisted(artifact.storage_key.clone());
        let result = async {
            if cancellation.is_cancelled() {
                return Err(AdapterError::Validation("AEMO parse cancelled".into()));
            }
            if !blob_store.matches_artifact_id(&key, artifact.id).await? {
                return Err(AdapterError::Validation(format!(
                    "AEMO artifact storage key `{}` does not match artifact id `{}`",
                    artifact.storage_key,
                    artifact.id.to_hex()
                )));
            }

            let mut byte_stream = blob_store.get(&key).await?;
            let mut bytes = Vec::with_capacity(artifact.size_bytes.min(usize::MAX as u64) as usize);
            while let Some(chunk) = byte_stream.next().await {
                if cancellation.is_cancelled() {
                    return Err(AdapterError::Validation("AEMO parse cancelled".into()));
                }
                bytes.extend_from_slice(&chunk?);
            }
            parse_dispatch_zip(&artifact, bytes, started_at, &metadata, row_tx).await
        }
        .await;

        if let Err(err) = result {
            let _ = error_tx.send(Err(err)).await;
        }
    });

    Box::pin(stream::unfold(row_rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    }))
}

async fn parse_dispatch_zip(
    artifact: &ArtifactRef,
    bytes: Vec<u8>,
    started_at: DateTime<Utc>,
    metadata: &BTreeMap<String, String>,
    row_tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    let (csv_name, csv_bytes) = extract_single_dispatch_csv(bytes)?;
    let published_at = metadata
        .get("published_at")
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc));
    let csv_stream = stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from(csv_bytes))]);
    let reader = StreamReader::new(csv_stream);
    let mut csv = AsyncReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .create_reader(reader);
    let mut records = csv.records();
    let mut headers: BTreeMap<(String, String, String), Vec<String>> = BTreeMap::new();
    let mut csv_published_at = published_at;

    while let Some(record) = records.next().await {
        let record = record.map_err(|err| {
            AdapterError::FormatDrift(format!("AEMO DispatchIS CSV is malformed: {err}"))
        })?;
        if record.is_empty() {
            continue;
        }
        match record.get(0).unwrap_or_default() {
            "C" => {
                csv_published_at = parse_control_row_published_at(&record).or(csv_published_at);
            }
            "I" => {
                if record.len() >= 5 {
                    let key = table_key(&record);
                    let fields = record.iter().skip(4).map(str::to_string).collect();
                    headers.insert(key, fields);
                }
            }
            "D" => {
                let key = table_key(&record);
                let Some(header) = headers.get(&key) else {
                    return Err(AdapterError::FormatDrift(format!(
                        "AEMO DispatchIS row for {}/{}/{} appeared before its header",
                        record.get(1).unwrap_or_default(),
                        record.get(2).unwrap_or_default(),
                        record.get(3).unwrap_or_default()
                    )));
                };
                let row = values_by_header(header, &record);
                let mut observations = observations_for_dispatch_row(
                    artifact,
                    &csv_name,
                    &record,
                    &row,
                    started_at,
                    csv_published_at,
                )?;
                for item in observations.drain(..) {
                    if row_tx.send(Ok(item)).await.is_err() {
                        return Ok(());
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn extract_single_dispatch_csv(bytes: Vec<u8>) -> Result<(String, Vec<u8>), AdapterError> {
    let mut archive = ZipArchive::new(Cursor::new(bytes)).map_err(|err| {
        AdapterError::FormatDrift(format!("AEMO DispatchIS ZIP is unreadable: {err}"))
    })?;
    let mut csv_entry = None;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(|err| {
            AdapterError::FormatDrift(format!("AEMO DispatchIS ZIP entry is unreadable: {err}"))
        })?;
        if entry.name().to_ascii_uppercase().ends_with(".CSV") {
            if csv_entry.is_some() {
                return Err(AdapterError::FormatDrift(
                    "AEMO DispatchIS ZIP contains more than one CSV payload".into(),
                ));
            }
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).map_err(CoreError::Io)?;
            csv_entry = Some((entry.name().to_string(), bytes));
        }
    }
    csv_entry.ok_or_else(|| {
        AdapterError::FormatDrift("AEMO DispatchIS ZIP did not contain a CSV payload".into())
    })
}

fn observations_for_dispatch_row(
    artifact: &ArtifactRef,
    csv_name: &str,
    record: &csv_async::StringRecord,
    row: &BTreeMap<String, String>,
    started_at: DateTime<Utc>,
    published_at: Option<DateTime<Utc>>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let table = record.get(2).unwrap_or_default();
    match table {
        "PRICE" => metric_observation_rows(
            artifact,
            csv_name,
            record,
            row,
            started_at,
            published_at,
            &[MetricSpec {
                field: "RRP",
                code: "rrp",
                unit: "AUD/MWh",
            }],
        ),
        "REGIONSUM" => metric_observation_rows(
            artifact,
            csv_name,
            record,
            row,
            started_at,
            published_at,
            &[
                MetricSpec {
                    field: "TOTALDEMAND",
                    code: "total_demand",
                    unit: "MW",
                },
                MetricSpec {
                    field: "AVAILABLEGENERATION",
                    code: "available_generation",
                    unit: "MW",
                },
                MetricSpec {
                    field: "NETINTERCHANGE",
                    code: "net_interchange",
                    unit: "MW",
                },
            ],
        ),
        _ => Ok(Vec::new()),
    }
}

#[derive(Debug, Clone, Copy)]
struct MetricSpec {
    field: &'static str,
    code: &'static str,
    unit: &'static str,
}

fn metric_observation_rows(
    artifact: &ArtifactRef,
    csv_name: &str,
    record: &csv_async::StringRecord,
    row: &BTreeMap<String, String>,
    started_at: DateTime<Utc>,
    published_at: Option<DateTime<Utc>>,
    metrics: &[MetricSpec],
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let settlement = row_required(row, "SETTLEMENTDATE")?;
    let time = parse_aemo_timestamp(settlement, "%Y/%m/%d %H:%M:%S")?;
    let region = row_required(row, "REGIONID")?;
    let table = record.get(2).unwrap_or_default();
    let record_version = record.get(3).unwrap_or_default();
    let mut rows = Vec::with_capacity(metrics.len());

    for metric in metrics {
        let raw = row_required(row, metric.field)?;
        let value = parse_optional_f64(raw)?;
        let dimensions = BTreeMap::from([
            (
                DimensionId::new("metric").expect("static dimension id is valid"),
                CodeId::new(metric.code).map_err(|err| {
                    AdapterError::Validation(format!("invalid AEMO metric code: {err}"))
                })?,
            ),
            (
                DimensionId::new("region").expect("static dimension id is valid"),
                CodeId::new(region.to_ascii_uppercase()).map_err(|err| {
                    AdapterError::Validation(format!("invalid AEMO region code: {err}"))
                })?,
            ),
        ]);
        let dataflow_id = dataflow_id();
        let series_key = SeriesKey::derive(
            &dataflow_id,
            dimensions
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        let series = SeriesDescriptor {
            series_key,
            dataflow_id,
            measure_id: MeasureId::new("value").expect("static measure id is valid"),
            dimensions,
            unit: metric.unit.into(),
        };

        let mut attributes = BTreeMap::new();
        attributes.insert("csv_file".into(), csv_name.into());
        attributes.insert("dispatch_interval".into(), time.to_rfc3339());
        if let Some(value) = row
            .get("DISPATCHINTERVAL")
            .filter(|value| !value.is_empty())
        {
            attributes.insert("dispatch_interval_no".into(), value.clone());
        }
        attributes.insert(
            "freshness_slo_seconds".into(),
            FRESHNESS_SLO_SECONDS.to_string(),
        );
        if let Some(value) = row.get("INTERVENTION").filter(|value| !value.is_empty()) {
            attributes.insert("intervention".into(), value.clone());
        }
        if let Some(value) = row.get("LASTCHANGED").filter(|value| !value.is_empty()) {
            attributes.insert("last_changed".into(), value.clone());
        }
        attributes.insert("metric".into(), metric.code.into());
        if let Some(published_at) = published_at {
            attributes.insert("published_at".into(), utc_rfc3339(published_at));
        }
        attributes.insert("record_version".into(), record_version.into());
        if let Some(value) = row.get("RUNNO").filter(|value| !value.is_empty()) {
            attributes.insert("run_no".into(), value.clone());
        }
        attributes.insert("table".into(), table.into());
        if let Some(value) = row.get("PRICE_STATUS").filter(|value| !value.is_empty()) {
            attributes.insert("price_status".into(), value.clone());
        }

        let observation = Observation {
            series_key,
            time,
            time_precision: TimePrecision::Minute,
            value,
            status: if value.is_some() {
                ObservationStatus::Normal
            } else {
                ObservationStatus::Missing
            },
            revision_no: 0,
            attributes,
            ingested_at: started_at,
            source_artifact_id: artifact.id,
        };
        rows.push((series, observation));
    }

    Ok(rows)
}

fn table_key(record: &csv_async::StringRecord) -> (String, String, String) {
    (
        record.get(1).unwrap_or_default().to_string(),
        record.get(2).unwrap_or_default().to_string(),
        record.get(3).unwrap_or_default().to_string(),
    )
}

fn values_by_header(
    header: &[String],
    record: &csv_async::StringRecord,
) -> BTreeMap<String, String> {
    header
        .iter()
        .zip(record.iter().skip(4))
        .map(|(key, value)| (key.clone(), value.trim_matches('"').trim().to_string()))
        .collect()
}

fn row_required<'a>(
    row: &'a BTreeMap<String, String>,
    field: &str,
) -> Result<&'a str, AdapterError> {
    row.get(field)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AdapterError::FormatDrift(format!("AEMO DispatchIS row missing `{field}`")))
}

fn parse_optional_f64(value: &str) -> Result<Option<f64>, AdapterError> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    value.parse::<f64>().map(Some).map_err(|err| {
        AdapterError::FormatDrift(format!(
            "AEMO DispatchIS numeric value `{value}` is invalid: {err}"
        ))
    })
}

fn parse_control_row_published_at(record: &csv_async::StringRecord) -> Option<DateTime<Utc>> {
    let date = record.get(5)?;
    let time = record.get(6)?;
    parse_aemo_timestamp(&format!("{date} {time}"), "%Y/%m/%d %H:%M:%S").ok()
}

fn validate_parse_artifact(
    artifact: &ArtifactRef,
    expected_dataflow: Option<&DataflowId>,
) -> Result<(), AdapterError> {
    if artifact.source_id != source_id() {
        return Err(AdapterError::Validation(format!(
            "AEMO parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    if let Some(expected) = expected_dataflow {
        if expected != &dataflow_id() {
            return Err(AdapterError::Validation(format!(
                "AEMO parse expected dataflow `{}`",
                expected.as_str()
            )));
        }
    }
    dispatch_file_provenance(&artifact.source_url).ok_or_else(|| {
        AdapterError::Validation(format!(
            "AEMO parse URL `{}` is not a trusted NEMWeb DispatchIS ZIP artifact",
            artifact.source_url
        ))
    })?;
    Ok(())
}

fn parse_dispatch_listing_with_base(
    body: &str,
    base_url: &str,
) -> Result<Vec<AemoDispatchFile>, AdapterError> {
    let mut files = Vec::new();
    for raw_line in body.lines() {
        let Some(href) = extract_href(raw_line) else {
            continue;
        };
        let Some(file_name) = href.rsplit('/').next() else {
            continue;
        };
        let Some((dispatch_interval, sequence)) = dispatch_file_name_parts(file_name) else {
            continue;
        };
        let Some(prefix) = raw_line.split("<A ").next() else {
            continue;
        };
        let Some((published_at, size_bytes)) = parse_listing_prefix(prefix)? else {
            continue;
        };
        files.push(AemoDispatchFile {
            file_name: file_name.to_string(),
            dispatch_interval,
            published_at,
            size_bytes,
            sequence,
            source_url: resolve_listing_href(base_url, &href),
        });
    }
    files.sort_by(|left, right| left.dispatch_interval.cmp(&right.dispatch_interval));
    Ok(files)
}

fn extract_href(line: &str) -> Option<String> {
    for marker in ["HREF=\"", "href=\""] {
        if let Some(start) = line.find(marker) {
            let value_start = start + marker.len();
            let value_end = line[value_start..].find('"')? + value_start;
            return Some(html_unescape_minimal(&line[value_start..value_end]));
        }
    }
    None
}

fn parse_listing_prefix(prefix: &str) -> Result<Option<(DateTime<Utc>, u64)>, AdapterError> {
    let tokens = prefix.split_whitespace().collect::<Vec<_>>();
    if tokens.len() < 7 {
        return Ok(None);
    }
    let Some(size_token) = tokens.last() else {
        return Ok(None);
    };
    let Ok(size_bytes) = size_token.parse::<u64>() else {
        return Ok(None);
    };
    let datetime = tokens[..tokens.len() - 1].join(" ");
    let naive =
        NaiveDateTime::parse_from_str(&datetime, "%A, %B %e, %Y %I:%M %p").map_err(|err| {
            AdapterError::FormatDrift(format!(
                "AEMO listing timestamp `{datetime}` is invalid: {err}"
            ))
        })?;
    Ok(Some((market_time_to_utc(naive)?, size_bytes)))
}

fn dispatch_file_name_parts(file_name: &str) -> Option<(DateTime<Utc>, String)> {
    let upper = file_name.to_ascii_uppercase();
    if !upper.starts_with(DISPATCHIS_PREFIX)
        || !upper.ends_with(&DISPATCHIS_SUFFIX.to_ascii_uppercase())
    {
        return None;
    }
    let stem = file_name.strip_suffix(DISPATCHIS_SUFFIX)?;
    let rest = stem.strip_prefix(DISPATCHIS_PREFIX)?;
    let (timestamp, sequence) = rest.split_once('_')?;
    if timestamp.len() != 12 || !timestamp.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if sequence.is_empty() || !sequence.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let naive = NaiveDateTime::parse_from_str(timestamp, "%Y%m%d%H%M").ok()?;
    let dispatch_interval = market_time_to_utc(naive).ok()?;
    Some((dispatch_interval, sequence.to_string()))
}

fn dispatch_file_provenance(source_url: &str) -> Option<(DateTime<Utc>, String)> {
    let file_name = source_url.rsplit('/').next()?;
    if !source_url.contains("/DispatchIS_Reports/") {
        return None;
    }
    let trusted_host = source_url.contains("nemweb.com.au")
        || source_url.starts_with("http://127.0.0.1:")
        || source_url.starts_with("http://localhost:");
    if !trusted_host {
        return None;
    }
    dispatch_file_name_parts(file_name)
}

fn resolve_listing_href(base_url: &str, href: &str) -> String {
    if href.starts_with("http://") || href.starts_with("https://") {
        return href.to_string();
    }
    if href.starts_with('/') {
        let Some(scheme_end) = base_url.find("://") else {
            return href.to_string();
        };
        let origin_start = scheme_end + 3;
        let origin_end = base_url[origin_start..]
            .find('/')
            .map_or(base_url.len(), |offset| origin_start + offset);
        return format!("{}{}", &base_url[..origin_end], href);
    }
    format!("{}/{}", base_url.trim_end_matches('/'), href)
}

fn html_unescape_minimal(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
}

fn parse_aemo_timestamp(value: &str, format: &str) -> Result<DateTime<Utc>, AdapterError> {
    let naive = NaiveDateTime::parse_from_str(value.trim_matches('"'), format).map_err(|err| {
        AdapterError::FormatDrift(format!("AEMO timestamp `{value}` is invalid: {err}"))
    })?;
    market_time_to_utc(naive)
}

fn market_time_to_utc(naive: NaiveDateTime) -> Result<DateTime<Utc>, AdapterError> {
    aemo_market_offset()
        .from_local_datetime(&naive)
        .single()
        .map(|value| value.with_timezone(&Utc))
        .ok_or_else(|| {
            AdapterError::FormatDrift(format!("AEMO market timestamp `{naive}` is ambiguous"))
        })
}

fn aemo_market_offset() -> FixedOffset {
    FixedOffset::east_opt(10 * 60 * 60).expect("static AEMO market offset is valid")
}

fn utc_rfc3339(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn source_id() -> SourceId {
    SourceId::new("aemo").expect("static source id is valid")
}

fn dataflow_id() -> DataflowId {
    DataflowId::new(DATAFLOW_ID).expect("static dataflow id is valid")
}
