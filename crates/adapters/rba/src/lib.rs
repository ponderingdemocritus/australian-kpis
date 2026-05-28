//! RBA adapter for statistical table XLS/CSV artifacts.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{
    collections::BTreeMap,
    io::{self, Cursor},
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
use au_kpis_storage::{BlobStore, StorageKey};
use calamine::{Data, Reader, open_workbook_auto_from_rs};
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use csv_async::AsyncReaderBuilder;
use futures::{StreamExt, TryStreamExt, stream};
use tokio_util::{io::StreamReader, sync::CancellationToken};

const DEFAULT_INDEX_URL: &str = "https://www.rba.gov.au/statistics/tables/";
const USER_AGENT: &str = concat!("au-kpis-adapter-rba/", env!("CARGO_PKG_VERSION"));
const DATAFLOW_ID: &str = "rba.statistical_tables";
const ATTRIBUTION: &str = "Source: Reserve Bank of Australia";
const LICENSE_NAME: &str = "RBA Copyright and Disclaimer Notice";
const LICENSE_URL: &str = "https://www.rba.gov.au/copyright/";

/// RBA statistical-table adapter.
#[derive(Debug, Clone)]
pub struct RbaAdapter {
    manifest: AdapterManifest,
    index_url: String,
}

impl Default for RbaAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl RbaAdapter {
    /// Start building an RBA adapter.
    #[must_use]
    pub fn builder() -> RbaAdapterBuilder {
        RbaAdapterBuilder::default()
    }

    /// Parse the RBA statistical-tables HTML index into tabular artifact links.
    pub fn parse_statistical_tables_index(body: &str) -> Result<Vec<RbaTable>, AdapterError> {
        parse_statistical_tables_index_with_base(body, DEFAULT_INDEX_URL)
    }

    /// Convert discovered tables into jobs for the supplied discovery timestamp.
    #[must_use]
    pub fn current_jobs_with_started_at(
        current: &[RbaTable],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Diff current RBA table links against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[RbaTable],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        current
            .iter()
            .filter_map(|table| {
                let revision = table.revision(started_at);
                known_revisions
                    .get(&table.revision_key())
                    .is_none_or(|known| known != &revision)
                    .then(|| table.to_discovered_job(started_at, trace_parent))
            })
            .collect()
    }

    /// Static metadata for the dataflow emitted by RBA statistical tables.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![Dataflow {
            id: dataflow_id(),
            source_id: source_id(),
            name: "RBA statistical tables".into(),
            description: Some(
                "Tabular Reserve Bank of Australia statistical tables from the weekly RBA index."
                    .into(),
            ),
            dimensions: vec![
                DimensionId::new("table").expect("static dimension id is valid"),
                DimensionId::new("series_id").expect("static dimension id is valid"),
                DimensionId::new("series_name").expect("static dimension id is valid"),
            ],
            measures: vec![MeasureId::new("value").expect("static measure id is valid")],
            frequency: Frequency::Irregular,
            license: License::Other(LICENSE_NAME.into()),
            attribution: ATTRIBUTION.into(),
            source_url: DEFAULT_INDEX_URL.into(),
        }]
    }

    fn index_url(&self) -> &str {
        &self.index_url
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "RBA fetch received job for source `{}`",
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
                "RBA fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        table_url_provenance(&job.source_url).ok_or_else(|| {
            AdapterError::Validation(format!(
                "RBA fetch URL `{}` is not a statistical-table XLS/CSV artifact",
                job.source_url
            ))
        })?;
        Ok(())
    }
}

#[async_trait]
impl SourceAdapter for RbaAdapter {
    fn id(&self) -> &'static str {
        "rba"
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
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
        let tables = parse_statistical_tables_index_with_base(&body, self.index_url())?;
        Ok(Self::discoverable_jobs_with_started_at(
            &tables,
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
    let provenance = match validate_parse_artifact(&artifact) {
        Ok(provenance) => provenance,
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

        let result = match provenance.format {
            RbaTableFormat::Csv => {
                parse_csv_artifact(
                    blob_store,
                    key,
                    artifact,
                    started_at,
                    cancellation,
                    row_tx.clone(),
                )
                .await
            }
            RbaTableFormat::Xls => {
                parse_xls_artifact(
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

async fn parse_csv_artifact(
    blob_store: BlobStore,
    key: StorageKey,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    cancellation: CancellationToken,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    let chunks = tokio::select! {
        () = cancellation.cancelled() => return Err(cancelled_parse_error()),
        chunks = blob_store.get(&key) => chunks?,
    };
    let io_stream = chunks.map_err(|err| io::Error::other(err.to_string()));
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
    send_rows(rows, artifact, ingested_at, tx).await
}

async fn parse_xls_artifact(
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
    let rows = tokio::task::spawn_blocking(move || parse_xls_rows(bytes))
        .await
        .map_err(parse_worker_error)??;
    send_rows(rows, artifact, ingested_at, tx).await
}

async fn send_rows(
    rows: Vec<Vec<String>>,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    for row in parse_table_rows(rows, &artifact, ingested_at)? {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

fn parse_xls_rows(bytes: Vec<u8>) -> Result<Vec<Vec<String>>, AdapterError> {
    let mut workbook = open_workbook_auto_from_rs(Cursor::new(bytes))
        .map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
    let sheet_name = workbook
        .sheet_names()
        .first()
        .cloned()
        .ok_or_else(|| AdapterError::FormatDrift("RBA workbook has no worksheets".into()))?;
    let range = workbook
        .worksheet_range(&sheet_name)
        .map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
    Ok(range
        .rows()
        .map(|row| row.iter().map(cell_to_string).collect())
        .collect())
}

/// Parse one arbitrary CSV byte slice through the RBA CSV parser core for
/// cargo-fuzz.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub async fn parse_csv_bytes_for_fuzz(bytes: &[u8]) -> Result<usize, AdapterError> {
    let input = bytes::Bytes::copy_from_slice(bytes);
    let io_stream = stream::iter([Ok::<_, io::Error>(input)]);
    let reader = StreamReader::new(io_stream);
    let mut csv = AsyncReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .create_reader(reader);
    let mut records = csv.records();
    let mut rows = Vec::new();
    while let Some(record) = records.next().await {
        let record = record.map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
        rows.push(record.iter().map(|cell| cell.trim().to_string()).collect());
    }
    let artifact = fuzz_artifact(
        bytes,
        "https://www.rba.gov.au/statistics/tables/csv/g1-data.csv",
    );
    parse_table_rows(rows, &artifact, fuzz_ingested_at()).map(|rows| rows.len())
}

/// Parse one arbitrary XLS/XLSX byte slice through the RBA XLS parser core for
/// cargo-fuzz.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn parse_xls_bytes_for_fuzz(bytes: &[u8]) -> Result<usize, AdapterError> {
    let rows = parse_xls_rows(bytes.to_vec())?;
    let artifact = fuzz_artifact(
        bytes,
        "https://www.rba.gov.au/statistics/tables/xls/a1-data.xlsx",
    );
    parse_table_rows(rows, &artifact, fuzz_ingested_at()).map(|rows| rows.len())
}

#[cfg(feature = "fuzzing")]
fn fuzz_artifact(bytes: &[u8], source_url: &str) -> ArtifactRef {
    let id = au_kpis_domain::ArtifactId::of_content(bytes);
    ArtifactRef {
        id,
        source_id: SourceId::new("rba").expect("static source id is valid"),
        source_url: source_url.to_string(),
        content_type: "application/octet-stream".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&id).to_string(),
        size_bytes: bytes.len() as u64,
        fetched_at: fuzz_ingested_at(),
    }
}

#[cfg(feature = "fuzzing")]
fn fuzz_ingested_at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
        .single()
        .expect("valid fuzz timestamp")
}

fn cell_to_string(cell: &Data) -> String {
    match cell {
        Data::Empty => String::new(),
        Data::String(value) => value.trim().to_string(),
        Data::Float(value) => number_to_string(*value),
        Data::Int(value) => value.to_string(),
        Data::Bool(value) => value.to_string(),
        Data::DateTime(value) => value
            .as_datetime()
            .map(|date_time| date_time.format("%Y-%m-%dT%H:%M:%S").to_string())
            .unwrap_or_else(|| value.to_string()),
        Data::DateTimeIso(value) | Data::DurationIso(value) => value.trim().to_string(),
        Data::Error(value) => format!("{value:?}"),
    }
}

fn number_to_string(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn parse_table_rows(
    rows: Vec<Vec<String>>,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let mut metadata = TableMetadata::default();
    let mut header_index = None;
    for (index, row) in rows.iter().enumerate() {
        if row.is_empty() {
            continue;
        }
        match row[0].trim().to_ascii_lowercase().as_str() {
            "title" => metadata.title = row.get(1).cloned().filter(|value| !value.is_empty()),
            "table" => metadata.table_id = row.get(1).cloned().filter(|value| !value.is_empty()),
            "frequency" => {
                metadata.frequency = row.get(1).cloned().filter(|value| !value.is_empty());
            }
            "source" => metadata.source = row.get(1).cloned().filter(|value| !value.is_empty()),
            "series id" | "series_id" => {
                metadata.series_ids = row.iter().skip(1).cloned().collect()
            }
            "units" | "unit" => metadata.units = row.iter().skip(1).cloned().collect(),
            "date" => {
                header_index = Some(index);
                break;
            }
            _ => {}
        }
    }

    let header_index = header_index
        .ok_or_else(|| AdapterError::FormatDrift("RBA table is missing `Date` header".into()))?;
    let header = &rows[header_index];
    if header.len() < 2 {
        return Err(AdapterError::FormatDrift(
            "RBA table `Date` header has no series columns".into(),
        ));
    }

    let table_id = metadata.table_id.unwrap_or_else(|| {
        table_url_provenance(&artifact.source_url)
            .map(|provenance| provenance.table_id)
            .unwrap_or_else(|| "RBA".into())
    });
    let title = metadata.title.unwrap_or_else(|| table_id.clone());
    let source = metadata
        .source
        .unwrap_or_else(|| "Reserve Bank of Australia".into());
    let series_names: Vec<String> = header.iter().skip(1).cloned().collect();
    let mut parsed = Vec::new();

    for row in rows.into_iter().skip(header_index + 1) {
        if row.first().is_none_or(|value| value.trim().is_empty()) {
            continue;
        }
        let (time, precision) = parse_time(row[0].trim())?;
        for (column_offset, series_name) in series_names.iter().enumerate() {
            let value_cell = row.get(column_offset + 1).map_or("", String::as_str);
            let (value, status) = parse_value(value_cell)?;
            let series_id = metadata
                .series_ids
                .get(column_offset)
                .filter(|value| !value.is_empty())
                .cloned()
                .unwrap_or_else(|| slugify_code(series_name));
            let unit = metadata
                .units
                .get(column_offset)
                .filter(|value| !value.is_empty())
                .cloned()
                .unwrap_or_else(|| "unknown".into());
            let table_code = rba_code_id("table", &table_id)?;
            let series_id_code = rba_code_id("series_id", &series_id)?;
            let series_name_code = rba_code_id("series_name", series_name)?;
            let dimensions = BTreeMap::from([
                (
                    DimensionId::new("table").expect("static dimension id is valid"),
                    table_code,
                ),
                (
                    DimensionId::new("series_id").expect("static dimension id is valid"),
                    series_id_code,
                ),
                (
                    DimensionId::new("series_name").expect("static dimension id is valid"),
                    series_name_code,
                ),
            ]);
            let dataflow_id = dataflow_id();
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
                unit,
            };
            let observation = Observation {
                series_key,
                time,
                time_precision: precision,
                value,
                status,
                revision_no: 0,
                attributes: BTreeMap::from([
                    ("source".into(), source.clone()),
                    ("source_url".into(), artifact.source_url.clone()),
                    ("table_title".into(), title.clone()),
                    (
                        "frequency".into(),
                        metadata
                            .frequency
                            .clone()
                            .unwrap_or_else(|| "unknown".into()),
                    ),
                    ("rba_series_id".into(), series_id),
                    ("rba_series_name".into(), series_name.clone()),
                ]),
                ingested_at,
                source_artifact_id: artifact.id,
            };
            parsed.push((descriptor, observation));
        }
    }
    Ok(parsed)
}

#[derive(Debug, Default)]
struct TableMetadata {
    title: Option<String>,
    table_id: Option<String>,
    frequency: Option<String>,
    source: Option<String>,
    series_ids: Vec<String>,
    units: Vec<String>,
}

fn parse_time(value: &str) -> Result<(DateTime<Utc>, TimePrecision), AdapterError> {
    for format in [
        "%Y-%m-%d",
        "%d-%b-%Y",
        "%e-%b-%Y",
        "%-d-%b-%Y",
        "%d %b %Y",
        "%e %b %Y",
        "%-d %b %Y",
    ] {
        if let Ok(date) = NaiveDate::parse_from_str(value, format) {
            return Ok((utc_midnight(date), TimePrecision::Day));
        }
    }
    if let Some((date_part, _)) = value.split_once('T') {
        if let Ok(date) = NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
            return Ok((utc_midnight(date), TimePrecision::Day));
        }
    }
    if let Some(date) = parse_quarter_period(value)? {
        return Ok((utc_midnight(date), TimePrecision::Quarter));
    }
    if value.len() == 7 {
        if let Ok(date) = NaiveDate::parse_from_str(&format!("{value}-01"), "%Y-%m-%d") {
            return Ok((utc_midnight(date), TimePrecision::Month));
        }
    }
    for format in ["%b-%Y", "%B-%Y", "%b %Y", "%B %Y"] {
        if let Ok(date) = NaiveDate::parse_from_str(&format!("1-{value}"), &format!("%d-{format}"))
        {
            return Ok((utc_midnight(date), TimePrecision::Month));
        }
    }
    if value.len() == 4 {
        let year = value
            .parse::<i32>()
            .map_err(|_| AdapterError::FormatDrift(format!("invalid RBA period `{value}`")))?;
        let date = NaiveDate::from_ymd_opt(year, 1, 1)
            .ok_or_else(|| AdapterError::FormatDrift(format!("invalid RBA period `{value}`")))?;
        return Ok((utc_midnight(date), TimePrecision::Year));
    }
    Err(AdapterError::FormatDrift(format!(
        "unsupported RBA period `{value}`"
    )))
}

fn parse_quarter_period(value: &str) -> Result<Option<NaiveDate>, AdapterError> {
    let quarter_parts = if let Some(parts) = value.split_once("-Q") {
        Some(parts)
    } else if let Some((year, quarter)) = value.split_once('Q') {
        (!year.ends_with('-')).then_some((year, quarter))
    } else {
        None
    };
    let Some((year, quarter)) = quarter_parts else {
        return Ok(None);
    };
    let year = year
        .parse::<i32>()
        .map_err(|_| AdapterError::FormatDrift(format!("invalid RBA period `{value}`")))?;
    let quarter = quarter
        .parse::<u32>()
        .map_err(|_| AdapterError::FormatDrift(format!("invalid RBA period `{value}`")))?;
    let month = match quarter {
        1 => 1,
        2 => 4,
        3 => 7,
        4 => 10,
        _ => {
            return Err(AdapterError::FormatDrift(format!(
                "invalid RBA quarter `{value}`"
            )));
        }
    };
    NaiveDate::from_ymd_opt(year, month, 1)
        .ok_or_else(|| AdapterError::FormatDrift(format!("invalid RBA period `{value}`")))
        .map(Some)
}

fn utc_midnight(date: NaiveDate) -> DateTime<Utc> {
    Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight is valid"))
}

fn parse_value(value: &str) -> Result<(Option<f64>, ObservationStatus), AdapterError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || matches!(trimmed, "na" | "n/a" | "NA" | "N/A") {
        return Ok((None, ObservationStatus::Missing));
    }
    let normalized = trimmed.replace(',', "");
    normalized
        .parse::<f64>()
        .map(|value| (Some(value), ObservationStatus::Normal))
        .map_err(|_| AdapterError::FormatDrift(format!("invalid RBA numeric value `{value}`")))
}

fn validate_parse_artifact(artifact: &ArtifactRef) -> Result<TableUrlProvenance, AdapterError> {
    if artifact.source_id.as_str() != "rba" {
        return Err(AdapterError::Validation(format!(
            "RBA parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    table_url_provenance(&artifact.source_url).ok_or_else(|| {
        AdapterError::Validation(format!(
            "RBA parse artifact `{}` is missing statistical-table provenance",
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
            "RBA parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )));
    }

    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "RBA parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )))
    }
}

fn parse_statistical_tables_index_with_base(
    body: &str,
    base_url: &str,
) -> Result<Vec<RbaTable>, AdapterError> {
    let mut tables = Vec::new();
    let mut rest = body;
    while let Some(anchor_start) = rest.find("<a") {
        rest = &rest[anchor_start..];
        let Some(open_end) = rest.find('>') else {
            break;
        };
        let attrs = &rest[..open_end + 1];
        let Some(close_start) = rest[open_end + 1..].find("</a>") else {
            break;
        };
        let text = &rest[open_end + 1..open_end + 1 + close_start];
        rest = &rest[open_end + 1 + close_start + "</a>".len()..];
        let Some(href) = attr_value(attrs, "href") else {
            continue;
        };
        let source_url = resolve_url(base_url, &href)?;
        let Some(provenance) = table_url_provenance(&source_url) else {
            continue;
        };
        let title = clean_html_text(text)
            .filter(|value| !value.eq_ignore_ascii_case("data"))
            .unwrap_or_else(|| provenance.table_id.clone());
        tables.push(RbaTable {
            table_id: provenance.table_id,
            table_slug: provenance.table_slug,
            title,
            format: provenance.format,
            source_url,
            last_updated: attr_value(attrs, "data-updated"),
        });
    }
    tables.sort_by(|left, right| {
        left.table_id
            .cmp(&right.table_id)
            .then(left.format.as_str().cmp(right.format.as_str()))
            .then(left.source_url.cmp(&right.source_url))
    });
    tables
        .dedup_by(|left, right| left.table_slug == right.table_slug && left.format == right.format);
    Ok(tables)
}

fn attr_value(attrs: &str, name: &str) -> Option<String> {
    let needle = format!("{name}=");
    let index = attrs.find(&needle)? + needle.len();
    let quote = attrs[index..].chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let value_start = index + quote.len_utf8();
    let value_end = attrs[value_start..].find(quote)? + value_start;
    Some(attrs[value_start..value_end].to_string())
}

fn clean_html_text(text: &str) -> Option<String> {
    let mut out = String::with_capacity(text.len());
    let mut in_tag = false;
    for ch in text.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    let cleaned = out
        .replace("&amp;", "&")
        .replace("&nbsp;", " ")
        .replace("&ndash;", "-")
        .replace("&#8211;", "-")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    (!cleaned.is_empty()).then_some(cleaned)
}

fn resolve_url(base_url: &str, href: &str) -> Result<String, AdapterError> {
    if href.starts_with("https://") || href.starts_with("http://") {
        return Ok(href.to_string());
    }
    if href.starts_with('/') {
        let scheme_end = base_url.find("://").ok_or_else(|| {
            AdapterError::Validation(format!("RBA index URL `{base_url}` is not absolute"))
        })?;
        let path_start = base_url[scheme_end + 3..]
            .find('/')
            .map_or(base_url.len(), |index| scheme_end + 3 + index);
        return Ok(format!("{}{}", &base_url[..path_start], href));
    }
    let Some((prefix, _)) = base_url.rsplit_once('/') else {
        return Err(AdapterError::Validation(format!(
            "RBA index URL `{base_url}` has no path separator"
        )));
    };
    Ok(format!("{prefix}/{href}"))
}

#[derive(Debug, Clone)]
struct TableUrlProvenance {
    table_id: String,
    table_slug: String,
    format: RbaTableFormat,
}

fn table_url_provenance(source_url: &str) -> Option<TableUrlProvenance> {
    let marker = "/statistics/tables/";
    let (_, path) = source_url.split_once(marker)?;
    let mut parts = path.split('/');
    let format = match parts.next()? {
        "csv" => RbaTableFormat::Csv,
        "xls" => RbaTableFormat::Xls,
        _ => return None,
    };
    let filename = parts.next()?.split('?').next()?.split('#').next()?;
    let stem = filename
        .strip_suffix(".csv")
        .or_else(|| filename.strip_suffix(".xls"))
        .or_else(|| filename.strip_suffix(".xlsx"))?;
    let table_id = table_id_from_slug(stem)?;
    Some(TableUrlProvenance {
        table_id,
        table_slug: stem.to_ascii_uppercase(),
        format,
    })
}

fn table_id_from_slug(stem: &str) -> Option<String> {
    let mut chars = stem.chars();
    let prefix = chars.next()?.to_ascii_uppercase();
    if !prefix.is_ascii_alphabetic() {
        return None;
    }
    let digits: String = chars.take_while(|ch| ch.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    let number = digits.trim_start_matches('0');
    Some(format!(
        "{prefix}{}",
        if number.is_empty() { "0" } else { number }
    ))
}

fn slugify_code(value: &str) -> String {
    let slug = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_");
    if slug.is_empty() {
        "VALUE".into()
    } else {
        slug
    }
}

fn rba_code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!("invalid RBA {field} code `{value}`: {err}"))
    })
}

fn accept_for_job(job: &DiscoveredJob) -> &'static str {
    if job.metadata.get("artifact_format").map(String::as_str) == Some("xls") {
        "application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    } else {
        "text/csv"
    }
}

fn content_type_for_job(job: &DiscoveredJob) -> &'static str {
    if job.metadata.get("artifact_format").map(String::as_str) == Some("xls") {
        "application/vnd.ms-excel"
    } else {
        "text/csv"
    }
}

fn source_id() -> SourceId {
    SourceId::new("rba").expect("static source id is valid")
}

fn dataflow_id() -> DataflowId {
    DataflowId::new(DATAFLOW_ID).expect("static dataflow id is valid")
}

fn parse_worker_error(err: tokio::task::JoinError) -> AdapterError {
    CoreError::Io(io::Error::other(format!("RBA parse worker failed: {err}"))).into()
}

fn cancelled_parse_error() -> AdapterError {
    CoreError::Io(io::Error::new(
        io::ErrorKind::Interrupted,
        "RBA parse cancelled",
    ))
    .into()
}

/// Builder for [`RbaAdapter`].
#[derive(Debug, Clone)]
pub struct RbaAdapterBuilder {
    index_url: String,
}

impl Default for RbaAdapterBuilder {
    fn default() -> Self {
        Self {
            index_url: DEFAULT_INDEX_URL.into(),
        }
    }
}

impl RbaAdapterBuilder {
    /// Override the statistical-tables index URL, usually for fixture tests.
    #[must_use]
    pub fn index_url(mut self, index_url: impl Into<String>) -> Self {
        self.index_url = index_url.into();
        self
    }

    /// Build the adapter.
    #[must_use]
    pub fn build(self) -> RbaAdapter {
        RbaAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: "Reserve Bank of Australia".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(60, Duration::from_secs(60))
                    .expect("static RBA rate limit is valid"),
                dataflows: vec![dataflow_id()],
            },
            index_url: self.index_url,
        }
    }
}

/// Stored revision type for RBA statistical-table links.
pub type RbaTableRevision = UpstreamRevision;

/// Tabular artifact format discovered from the RBA statistical-tables index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RbaTableFormat {
    /// CSV artifact.
    Csv,
    /// XLS or XLSX artifact parsed through `calamine`.
    Xls,
}

impl RbaTableFormat {
    /// Stable lowercase format label used in job metadata.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Xls => "xls",
        }
    }
}

/// One RBA statistical-table artifact link discovered from the index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RbaTable {
    /// Human table id, such as `F1` or `G1`.
    pub table_id: String,
    /// Source-local table file stem, such as `F01D`.
    pub table_slug: String,
    /// Link text or table title from the index.
    pub title: String,
    /// Artifact format.
    pub format: RbaTableFormat,
    /// Canonical artifact URL.
    pub source_url: String,
    /// Optional update marker scraped from the index.
    pub last_updated: Option<String>,
}

impl RbaTable {
    fn revision_key(&self) -> String {
        format!("RBA:{}:{}", self.table_id, self.format.as_str())
    }

    fn revision(&self, started_at: DateTime<Utc>) -> UpstreamRevision {
        let version = self
            .last_updated
            .clone()
            .unwrap_or_else(|| iso_week_version(started_at));
        UpstreamRevision::new(version, self.last_updated.clone())
    }

    fn to_discovered_job(
        &self,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> DiscoveredJob {
        let revision = self.revision(started_at);
        let revision_version = revision.version().to_string();
        let revision_key = self.revision_key();
        DiscoveredJob {
            id: format!(
                "rba:{}:{}:{}",
                self.table_id,
                self.format.as_str(),
                revision_version
            ),
            source_id: source_id(),
            dataflow_id: dataflow_id(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("adapter".into(), "rba".into()),
                ("artifact_format".into(), self.format.as_str().into()),
                ("attribution".into(), ATTRIBUTION.into()),
                ("cadence".into(), "weekly".into()),
                ("dataflow_id".into(), DATAFLOW_ID.into()),
                ("license".into(), LICENSE_NAME.into()),
                ("license_url".into(), LICENSE_URL.into()),
                ("revision_key".into(), revision_key),
                ("revision_version".into(), revision_version),
                ("source_index_url".into(), DEFAULT_INDEX_URL.into()),
                ("table_id".into(), self.table_id.clone()),
                ("table_slug".into(), self.table_slug.clone()),
                ("title".into(), self.title.clone()),
            ]),
        }
    }
}

fn iso_week_version(started_at: DateTime<Utc>) -> String {
    let week = started_at.iso_week();
    format!("{}-W{:02}", week.year(), week.week())
}

#[cfg(test)]
mod tests {
    use super::*;
    use calamine::{ExcelDateTime, ExcelDateTimeType};

    #[test]
    fn cell_to_string_formats_excel_datetime_cells_as_iso_periods() {
        let cell = Data::DateTime(ExcelDateTime::new(
            45_943.541,
            ExcelDateTimeType::DateTime,
            false,
        ));

        assert_eq!(cell_to_string(&cell), "2025-10-13T12:59:02");
    }
}
