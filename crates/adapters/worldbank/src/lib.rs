//! World Bank adapter for B-READY CSV artifacts.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{collections::BTreeMap, io, io::Cursor, time::Duration};

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
use au_kpis_error::CoreError;
use au_kpis_storage::{BlobStore, StorageError, StorageKey};
use bytes::Bytes;
use chrono::{DateTime, Datelike, TimeZone, Utc};
use csv_async::AsyncReaderBuilder;
use futures::{StreamExt, stream};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

const DEFAULT_INDEX_URL: &str = "https://www.worldbank.org/en/businessready";
const DEFAULT_API_URL: &str = "https://api.worldbank.org/v2/country/AUS/indicator/IC.BRE.BE.OS?format=json&source=2&per_page=100";
const DEFAULT_SOURCE_URL: &str = "https://www.worldbank.org/en/businessready";
const USER_AGENT: &str = concat!("au-kpis-adapter-worldbank/", env!("CARGO_PKG_VERSION"));
const DATAFLOW_ID: &str = "worldbank.bready";
const ATTRIBUTION: &str = "Source: World Bank B-READY";
const LICENSE_NAME: &str = "World Bank terms";
const LICENSE_URL: &str = "https://www.worldbank.org/en/about/legal/terms-and-conditions";
const BREADY_API_RELEASE_ID: &str = "bready-australia-api";
const BUSINESS_ENTRY_INDICATOR: &str = "IC.BRE.BE.OS";
const BUSINESS_LOCATION_INDICATOR: &str = "IC.BRE.BL.OS";
const PUBLIC_SERVICES_INDICATOR: &str = "IC.BRE.BE.P2";
const BUSINESS_ENTRY_SCORE: &str = "business_entry_score";
const PUBLIC_SERVICES_SCORE: &str = "public_services_score";
const BUSINESS_LOCATION_SCORE: &str = "business_location_score";

/// World Bank B-READY adapter.
#[derive(Debug, Clone)]
pub struct WorldbankAdapter {
    manifest: AdapterManifest,
    index_url: String,
    api_url: String,
}

impl Default for WorldbankAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl WorldbankAdapter {
    /// Start building a World Bank adapter.
    #[must_use]
    pub fn builder() -> WorldbankAdapterBuilder {
        WorldbankAdapterBuilder::default()
    }

    /// Parse the World Bank B-READY page for CSV links.
    pub fn parse_bready_releases(body: &str) -> Result<Vec<WorldbankBreadyRelease>, AdapterError> {
        parse_bready_releases_with_base(body, DEFAULT_INDEX_URL)
    }

    /// Diff current release links against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[WorldbankBreadyRelease],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        current
            .iter()
            .filter_map(|release| {
                let revision = release.revision(started_at);
                known_revisions
                    .get(&release.revision_key())
                    .is_none_or(|known| known != &revision)
                    .then(|| release.to_discovered_job(started_at, trace_parent, DEFAULT_INDEX_URL))
            })
            .collect()
    }

    /// Convert current release links into jobs for the supplied timestamp.
    #[must_use]
    pub fn current_jobs_with_started_at(
        current: &[WorldbankBreadyRelease],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Static metadata for the World Bank B-READY dataflow.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![Dataflow {
            id: dataflow_id(),
            source_id: source_id(),
            name: "World Bank B-READY".into(),
            description: Some(
                "Annual Australian Business Ready indicators from World Bank B-READY CSV artifacts."
                    .into(),
            ),
            dimensions: vec![
                DimensionId::new("country").expect("static dimension id is valid"),
                DimensionId::new("measure").expect("static dimension id is valid"),
            ],
            measures: vec![
                MeasureId::new(BUSINESS_ENTRY_SCORE).expect("static measure id is valid"),
                MeasureId::new(PUBLIC_SERVICES_SCORE).expect("static measure id is valid"),
                MeasureId::new(BUSINESS_LOCATION_SCORE).expect("static measure id is valid"),
            ],
            frequency: Frequency::Annual,
            license: License::Other(LICENSE_NAME.into()),
            attribution: ATTRIBUTION.into(),
            source_url: DEFAULT_SOURCE_URL.into(),
        }]
    }

    fn index_url(&self) -> &str {
        &self.index_url
    }

    fn api_url(&self) -> &str {
        &self.api_url
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "World Bank fetch received job for source `{}`",
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
                "World Bank fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        release_url_provenance(&job.source_url).ok_or_else(|| {
            AdapterError::Validation(format!(
                "World Bank fetch URL `{}` is not a B-READY artifact",
                job.source_url
            ))
        })?;
        Ok(())
    }
}

#[async_trait]
impl SourceAdapter for WorldbankAdapter {
    fn id(&self) -> &'static str {
        "worldbank"
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    fn source_metadata(&self) -> Option<Source> {
        Some(Source {
            id: source_id(),
            name: "World Bank".into(),
            homepage: "https://www.worldbank.org".into(),
            description: Some("World Bank B-READY business environment indicators.".into()),
        })
    }

    fn dataflow_metadata(&self) -> Vec<Dataflow> {
        WorldbankAdapter::dataflow_metadata(self)
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        if let Some(requested) = ctx.requested_dataflow_id() {
            if requested != &dataflow_id() {
                return Ok(Vec::new());
            }
        }
        if self.api_url() != DEFAULT_API_URL || self.index_url() == DEFAULT_INDEX_URL {
            let response = ctx
                .http
                .execute(
                    ctx.http
                        .raw()
                        .get(self.api_url())
                        .header("user-agent", USER_AGENT)
                        .header("accept", "application/json"),
                )
                .await?
                .error_for_status()?;
            let body = response.text().await?;
            let current = [parse_bready_api_release(
                &body,
                self.api_url(),
                ctx.started_at,
            )?];
            return Ok(current
                .iter()
                .filter_map(|release| {
                    let revision = release.revision(ctx.started_at);
                    ctx.known_revisions()
                        .get(&release.revision_key())
                        .is_none_or(|known| known != &revision)
                        .then(|| {
                            release.to_discovered_job(
                                ctx.started_at,
                                ctx.trace_parent(),
                                self.api_url(),
                            )
                        })
                })
                .collect());
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
        let current = parse_bready_releases_with_base(&body, self.index_url())?;
        Ok(current
            .iter()
            .filter_map(|release| {
                let revision = release.revision(ctx.started_at);
                ctx.known_revisions()
                    .get(&release.revision_key())
                    .is_none_or(|known| known != &revision)
                    .then(|| {
                        release.to_discovered_job(
                            ctx.started_at,
                            ctx.trace_parent(),
                            self.index_url(),
                        )
                    })
            })
            .collect())
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id(), job_id = %job.id))]
    async fn fetch(&self, job: DiscoveredJob, ctx: &FetchCtx) -> Result<ArtifactRef, AdapterError> {
        self.validate_fetch_job(&job)?;
        let is_api_artifact = is_worldbank_indicator_api_url(&job.source_url);
        let response = ctx
            .http
            .execute(
                ctx.http
                    .raw_artifact()
                    .get(&job.source_url)
                    .header("user-agent", USER_AGENT)
                    .header(
                        "accept",
                        if is_api_artifact {
                            "application/json"
                        } else {
                            "text/csv"
                        },
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
        let staged = if is_api_artifact {
            let bytes = response.bytes().await?;
            let csv = bready_api_json_to_csv(&bytes)?;
            ctx.blob_store
                .stage_artifact_stream(
                    stream::once(async move { Ok::<Bytes, StorageError>(Bytes::from(csv)) })
                        .boxed(),
                )
                .await?
        } else {
            ctx.blob_store
                .stage_artifact_stream(response.bytes_stream().boxed())
                .await?
        };
        let id = staged.id();
        let storage_key = StorageKey::canonical_for(&id).to_string();
        let artifact = Artifact {
            id,
            fetch_id: None,
            source_id: job.source_id,
            source_url: job.source_url,
            content_type: "text/csv".into(),
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
    let cancellation = ctx.cancellation().clone();
    let (row_tx, row_rx) = tokio::sync::mpsc::channel(64);

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

        if let Err(err) = parse_csv_artifact(
            blob_store,
            key,
            artifact,
            started_at,
            cancellation,
            row_tx.clone(),
        )
        .await
        {
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

    let mut csv = AsyncReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .create_reader(Cursor::new(bytes));
    let mut records = csv.records();
    let mut rows = Vec::new();
    while let Some(record) = tokio::select! {
        () = cancellation.cancelled() => return Err(cancelled_parse_error()),
        record = records.next() => record,
    } {
        let record = record.map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
        rows.push(record.iter().map(|cell| cell.trim().to_string()).collect());
    }

    for row in parse_bready_rows(rows, &artifact, ingested_at)? {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

fn parse_bready_rows(
    rows: Vec<Vec<String>>,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let mut rows = rows.into_iter().filter(|row| !row_is_empty(row));
    let header = rows
        .next()
        .ok_or_else(|| AdapterError::FormatDrift("World Bank bready CSV is empty".into()))?;
    validate_bready_header(&header)?;

    let mut parsed = Vec::new();
    for row in rows {
        let period = required_cell(&row, 0, "period")?;
        let country = required_cell(&row, 1, "country")?;
        let measure_id = required_cell(&row, 2, "measure_id")?;
        let measure_name = required_cell(&row, 3, "measure_name")?;
        let value = optional_cell(&row, 4);
        let unit = required_cell(&row, 5, "unit")?;
        let status = optional_cell(&row, 6).unwrap_or("normal");

        let time = parse_year(period)?;
        let (value, status) = parse_value_and_status(value, status)?;
        let dataflow_id = dataflow_id();
        let measure = MeasureId::new(measure_id.to_string()).map_err(|err| {
            AdapterError::FormatDrift(format!(
                "invalid World Bank bready measure `{measure_id}`: {err}"
            ))
        })?;
        let dimensions = BTreeMap::from([
            (
                DimensionId::new("measure").expect("static dimension id is valid"),
                worldbank_code_id("measure", measure_id)?,
            ),
            (
                DimensionId::new("country").expect("static dimension id is valid"),
                worldbank_code_id("country", country)?,
            ),
        ]);
        let series_key = SeriesKey::derive(
            &dataflow_id,
            &measure,
            dimensions
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        let descriptor = SeriesDescriptor {
            series_key,
            dataflow_id,
            measure_id: measure,
            dimensions,
            unit: unit.to_string(),
        };
        let observation = Observation {
            series_key,
            time,
            time_precision: TimePrecision::Year,
            value,
            status,
            revision_no: if status == ObservationStatus::Revised {
                1
            } else {
                0
            },
            attributes: BTreeMap::from([
                ("worldbank_measure_id".into(), measure_id.to_string()),
                ("worldbank_measure_name".into(), measure_name.to_string()),
                ("source_url".into(), artifact.source_url.clone()),
            ]),
            ingested_at,
            source_artifact_id: artifact.id,
        };
        parsed.push((descriptor, observation));
    }
    Ok(parsed)
}

fn validate_bready_header(header: &[String]) -> Result<(), AdapterError> {
    let expected = [
        "period",
        "country",
        "measure_id",
        "measure_name",
        "value",
        "unit",
        "status",
    ];
    let actual = header
        .iter()
        .map(|cell| cell.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();
    if actual.len() < expected.len()
        || expected
            .iter()
            .enumerate()
            .any(|(index, expected)| actual.get(index).map(String::as_str) != Some(*expected))
    {
        return Err(AdapterError::FormatDrift(format!(
            "World Bank bready CSV header must start with `{}`",
            expected.join(",")
        )));
    }
    Ok(())
}

fn row_is_empty(row: &[String]) -> bool {
    row.iter().all(|cell| cell.trim().is_empty())
}

fn required_cell<'a>(
    row: &'a [String],
    index: usize,
    field: &str,
) -> Result<&'a str, AdapterError> {
    optional_cell(row, index).ok_or_else(|| {
        AdapterError::FormatDrift(format!("World Bank bready row missing `{field}`"))
    })
}

fn optional_cell(row: &[String], index: usize) -> Option<&str> {
    row.get(index)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn parse_year(value: &str) -> Result<DateTime<Utc>, AdapterError> {
    let year = value
        .parse::<i32>()
        .map_err(|_| AdapterError::FormatDrift(format!("invalid World Bank period `{value}`")))?;
    Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0)
        .single()
        .ok_or_else(|| AdapterError::FormatDrift(format!("invalid World Bank period `{value}`")))
}

fn parse_value_and_status(
    value: Option<&str>,
    status: &str,
) -> Result<(Option<f64>, ObservationStatus), AdapterError> {
    let parsed_value = match value {
        Some(value) if !matches!(value.to_ascii_lowercase().as_str(), "na" | "n/a" | "..") => {
            Some(value.replace(',', "").parse::<f64>().map_err(|_| {
                AdapterError::FormatDrift(format!("invalid World Bank value `{value}`"))
            })?)
        }
        _ => None,
    };
    if parsed_value.is_none() {
        return Ok((None, ObservationStatus::Missing));
    }
    let status = match status.trim().to_ascii_lowercase().as_str() {
        "" | "normal" => ObservationStatus::Normal,
        "revised" => ObservationStatus::Revised,
        "provisional" => ObservationStatus::Provisional,
        "estimated" => ObservationStatus::Estimated,
        other => {
            return Err(AdapterError::FormatDrift(format!(
                "invalid World Bank observation status `{other}`"
            )));
        }
    };
    Ok((parsed_value, status))
}

fn validate_parse_artifact(
    artifact: &ArtifactRef,
    expected_dataflow_id: Option<&DataflowId>,
) -> Result<(), AdapterError> {
    if artifact.source_id.as_str() != "worldbank" {
        return Err(AdapterError::Validation(format!(
            "World Bank parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    if let Some(expected) = expected_dataflow_id {
        let actual = dataflow_id();
        if expected != &actual {
            return Err(AdapterError::Validation(format!(
                "World Bank parse expected dataflow `{}` but adapter emits `{}`",
                expected.as_str(),
                actual.as_str()
            )));
        }
    }
    release_url_provenance(&artifact.source_url).ok_or_else(|| {
        AdapterError::Validation(format!(
            "World Bank parse artifact `{}` is missing B-READY provenance",
            artifact.source_url
        ))
    })?;
    Ok(())
}

fn parse_bready_api_release(
    body: &str,
    api_url: &str,
    started_at: DateTime<Utc>,
) -> Result<WorldbankBreadyRelease, AdapterError> {
    let value: Value = serde_json::from_str(body)
        .map_err(|err| AdapterError::FormatDrift(format!("invalid World Bank API JSON: {err}")))?;
    let metadata = value
        .as_array()
        .and_then(|items| items.first())
        .and_then(Value::as_object)
        .ok_or_else(|| AdapterError::FormatDrift("World Bank API metadata is missing".into()))?;
    let last_updated = metadata
        .get("lastupdated")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.is_empty());
    let version = last_updated
        .clone()
        .unwrap_or_else(|| iso_week_version(started_at));
    Ok(WorldbankBreadyRelease {
        release_id: BREADY_API_RELEASE_ID.into(),
        title: "B-READY Australia indicator API".into(),
        source_url: api_url.to_string(),
        last_updated: Some(version),
    })
}

fn bready_api_json_to_csv(bytes: &[u8]) -> Result<Vec<u8>, AdapterError> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|err| AdapterError::FormatDrift(format!("invalid World Bank API JSON: {err}")))?;
    let rows = value
        .as_array()
        .and_then(|items| items.get(1))
        .and_then(Value::as_array)
        .ok_or_else(|| AdapterError::FormatDrift("World Bank API rows are missing".into()))?;

    let mut csv = String::from("period,country,measure_id,measure_name,value,unit,status\n");
    let mut emitted = 0usize;
    for row in rows {
        let indicator = row
            .get("indicator")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                AdapterError::FormatDrift("World Bank API row missing indicator".into())
            })?;
        let indicator_id = required_json_str(indicator.get("id"), "indicator.id")?;
        let Some(measure_id) = measure_id_for_indicator(indicator_id) else {
            continue;
        };
        let measure_name = required_json_str(indicator.get("value"), "indicator.value")?;
        let country = row
            .get("countryiso3code")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .or_else(|| {
                row.get("country")
                    .and_then(Value::as_object)
                    .and_then(|country| country.get("id"))
                    .and_then(Value::as_str)
            })
            .ok_or_else(|| {
                AdapterError::FormatDrift("World Bank API row missing country code".into())
            })?;
        let period = required_json_str(row.get("date"), "date")?;
        let value = row
            .get("value")
            .and_then(|value| {
                if value.is_null() {
                    None
                } else {
                    Some(json_scalar_to_string(value))
                }
            })
            .transpose()?
            .unwrap_or_default();
        let unit = row
            .get("unit")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("index");
        let status = normalize_api_status(row.get("obs_status").and_then(Value::as_str));
        push_csv_record(
            &mut csv,
            &[
                period,
                country,
                measure_id,
                measure_name,
                &value,
                unit,
                status,
            ],
        );
        emitted += 1;
    }
    if emitted == 0 {
        return Err(AdapterError::FormatDrift(
            "World Bank API response contained no B-READY rows".into(),
        ));
    }
    Ok(csv.into_bytes())
}

fn required_json_str<'a>(value: Option<&'a Value>, field: &str) -> Result<&'a str, AdapterError> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| AdapterError::FormatDrift(format!("World Bank API row missing `{field}`")))
}

fn json_scalar_to_string(value: &Value) -> Result<String, AdapterError> {
    if let Some(value) = value.as_f64() {
        return Ok(value.to_string());
    }
    if let Some(value) = value.as_i64() {
        return Ok(value.to_string());
    }
    if let Some(value) = value.as_u64() {
        return Ok(value.to_string());
    }
    if let Some(value) = value.as_str() {
        return Ok(value.to_string());
    }
    Err(AdapterError::FormatDrift(
        "World Bank API value is not a scalar".into(),
    ))
}

fn normalize_api_status(status: Option<&str>) -> &'static str {
    match status.map(str::trim).filter(|value| !value.is_empty()) {
        None => "normal",
        Some("E") | Some("e") => "estimated",
        Some("P") | Some("p") => "provisional",
        Some("R") | Some("r") => "revised",
        Some(_) => "normal",
    }
}

fn measure_id_for_indicator(indicator_id: &str) -> Option<&'static str> {
    match indicator_id {
        BUSINESS_ENTRY_INDICATOR => Some(BUSINESS_ENTRY_SCORE),
        PUBLIC_SERVICES_INDICATOR => Some(PUBLIC_SERVICES_SCORE),
        BUSINESS_LOCATION_INDICATOR => Some(BUSINESS_LOCATION_SCORE),
        _ => None,
    }
}

fn push_csv_record(out: &mut String, fields: &[&str]) {
    for (index, field) in fields.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        push_csv_field(out, field);
    }
    out.push('\n');
}

fn push_csv_field(out: &mut String, field: &str) {
    if field.contains([',', '"', '\n', '\r']) {
        out.push('"');
        for ch in field.chars() {
            if ch == '"' {
                out.push('"');
            }
            out.push(ch);
        }
        out.push('"');
    } else {
        out.push_str(field);
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
            "World Bank parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )));
    }

    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "World Bank parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )))
    }
}

fn parse_bready_releases_with_base(
    body: &str,
    base_url: &str,
) -> Result<Vec<WorldbankBreadyRelease>, AdapterError> {
    let mut releases = Vec::new();
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
        let Some(provenance) = release_url_provenance(&source_url) else {
            continue;
        };
        let title = clean_html_text(text).unwrap_or_else(|| provenance.release_id.clone());
        releases.push(WorldbankBreadyRelease {
            release_id: provenance.release_id,
            title,
            source_url,
            last_updated: attr_value(attrs, "data-updated"),
        });
    }
    releases.sort_by(|left, right| left.release_id.cmp(&right.release_id));
    releases.dedup_by(|left, right| left.release_id == right.release_id);
    Ok(releases)
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
            AdapterError::Validation(format!("World Bank index URL `{base_url}` is not absolute"))
        })?;
        let path_start = base_url[scheme_end + 3..]
            .find('/')
            .map_or(base_url.len(), |index| scheme_end + 3 + index);
        return Ok(format!("{}{}", &base_url[..path_start], href));
    }
    let Some((prefix, _)) = base_url.rsplit_once('/') else {
        return Err(AdapterError::Validation(format!(
            "World Bank index URL `{base_url}` has no path separator"
        )));
    };
    Ok(format!("{prefix}/{href}"))
}

#[derive(Debug, Clone)]
struct ReleaseUrlProvenance {
    release_id: String,
}

fn release_url_provenance(source_url: &str) -> Option<ReleaseUrlProvenance> {
    if is_worldbank_indicator_api_url(source_url) {
        return Some(ReleaseUrlProvenance {
            release_id: BREADY_API_RELEASE_ID.into(),
        });
    }

    let marker = "/en/businessready/";
    let (_, path) = source_url.split_once(marker)?;
    let filename = path
        .split('?')
        .next()?
        .split('#')
        .next()?
        .rsplit('/')
        .next()?;
    let stem = filename.strip_suffix(".csv")?;
    if !stem.starts_with("bready-australia-") {
        return None;
    }
    Some(ReleaseUrlProvenance {
        release_id: stem.to_string(),
    })
}

fn is_worldbank_indicator_api_url(source_url: &str) -> bool {
    source_url.contains("/v2/country/AUS/indicator/IC.BRE.")
        || source_url.contains("/v2/country/AU/indicator/IC.BRE.")
}

fn worldbank_code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!("invalid World Bank {field} code `{value}`: {err}"))
    })
}

fn source_id() -> SourceId {
    SourceId::new("worldbank").expect("static source id is valid")
}

fn dataflow_id() -> DataflowId {
    DataflowId::new(DATAFLOW_ID).expect("static dataflow id is valid")
}

fn cancelled_parse_error() -> AdapterError {
    CoreError::Io(io::Error::new(
        io::ErrorKind::Interrupted,
        "World Bank parse cancelled",
    ))
    .into()
}

/// Builder for [`WorldbankAdapter`].
#[derive(Debug, Clone)]
pub struct WorldbankAdapterBuilder {
    index_url: String,
    api_url: String,
}

impl Default for WorldbankAdapterBuilder {
    fn default() -> Self {
        Self {
            index_url: DEFAULT_INDEX_URL.into(),
            api_url: DEFAULT_API_URL.into(),
        }
    }
}

impl WorldbankAdapterBuilder {
    /// Override the bready insights index URL, usually for fixture tests.
    #[must_use]
    pub fn index_url(mut self, index_url: impl Into<String>) -> Self {
        self.index_url = index_url.into();
        self
    }

    /// Override the World Bank indicator API URL, usually for fixture tests.
    #[must_use]
    pub fn api_url(mut self, api_url: impl Into<String>) -> Self {
        self.api_url = api_url.into();
        self
    }

    /// Build the adapter.
    #[must_use]
    pub fn build(self) -> WorldbankAdapter {
        WorldbankAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: "World Bank".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(30, Duration::from_secs(60))
                    .expect("static World Bank rate limit is valid"),
                dataflows: vec![dataflow_id()],
            },
            index_url: self.index_url,
            api_url: self.api_url,
        }
    }
}

/// One World Bank B-READY artifact link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldbankBreadyRelease {
    /// Stable source-local release id, derived from the CSV filename.
    pub release_id: String,
    /// Link text or release title from the index.
    pub title: String,
    /// Canonical artifact URL.
    pub source_url: String,
    /// Optional update marker scraped from the index.
    pub last_updated: Option<String>,
}

impl WorldbankBreadyRelease {
    /// Build a World Bank release revision from a version and optional update marker.
    #[must_use]
    pub fn revision_for(version: &str, last_updated: Option<&str>) -> UpstreamRevision {
        UpstreamRevision::new(version, last_updated)
    }

    fn revision_key(&self) -> String {
        format!("WORLDBANK:{}", self.release_id)
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
        index_url: &str,
    ) -> DiscoveredJob {
        let revision = self.revision(started_at);
        let revision_version = revision.version().to_string();
        let revision_key = self.revision_key();
        let artifact_format = if is_worldbank_indicator_api_url(&self.source_url) {
            "worldbank-json"
        } else {
            "csv"
        };
        DiscoveredJob {
            id: format!("worldbank:{}:{revision_version}", self.release_id),
            source_id: source_id(),
            dataflow_id: dataflow_id(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("adapter".into(), "worldbank".into()),
                ("artifact_format".into(), artifact_format.into()),
                ("attribution".into(), ATTRIBUTION.into()),
                ("release_id".into(), self.release_id.clone()),
                ("cadence".into(), "annual".into()),
                ("dataflow_id".into(), DATAFLOW_ID.into()),
                ("license".into(), LICENSE_NAME.into()),
                ("license_url".into(), LICENSE_URL.into()),
                ("revision_key".into(), revision_key),
                ("revision_version".into(), revision_version),
                ("source_index_url".into(), index_url.to_string()),
                ("title".into(), self.title.clone()),
            ]),
        }
    }
}

fn iso_week_version(started_at: DateTime<Utc>) -> String {
    let week = started_at.iso_week();
    format!("{}-W{:02}", week.year(), week.week())
}
