//! NHSAC adapter for Housing Accord progress artifacts.

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
use au_kpis_storage::{BlobStore, StorageKey};
use chrono::{DateTime, Datelike, TimeZone, Utc};
use csv_async::AsyncReaderBuilder;
use futures::{StreamExt, stream};
use tokio_util::sync::CancellationToken;

const DEFAULT_INDEX_URL: &str = "https://nhsac.gov.au/publications";
const DEFAULT_SOURCE_URL: &str = "https://nhsac.gov.au/publications";
const USER_AGENT: &str = concat!("au-kpis-adapter-nhsac/", env!("CARGO_PKG_VERSION"));
const DATAFLOW_ID: &str = "nhsac.housing_accord_progress";
const ATTRIBUTION: &str = "Source: National Housing Supply and Affordability Council";
const LICENSE_NAME: &str = "NHSAC copyright";
const LICENSE_URL: &str = "https://nhsac.gov.au/copyright";
const PROGRESS_TO_TARGET_PCT: &str = "progress_to_target_pct";
const HOMES_COMPLETED: &str = "homes_completed";
const ANNUAL_TARGET: &str = "annual_target";

/// National Housing Supply and Affordability Council Housing Accord progress adapter.
#[derive(Debug, Clone)]
pub struct NhsacAdapter {
    manifest: AdapterManifest,
    index_url: String,
}

impl Default for NhsacAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl NhsacAdapter {
    /// Start building a NHSAC adapter.
    #[must_use]
    pub fn builder() -> NhsacAdapterBuilder {
        NhsacAdapterBuilder::default()
    }

    /// Parse the NHSAC publications page for Housing Accord progress links.
    pub fn parse_housing_accord_releases(
        body: &str,
    ) -> Result<Vec<NhsacHousingAccordRelease>, AdapterError> {
        parse_housing_accord_releases_with_base(body, DEFAULT_INDEX_URL)
    }

    /// Diff current release links against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[NhsacHousingAccordRelease],
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
        current: &[NhsacHousingAccordRelease],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Static metadata for the National Housing Supply and Affordability Council Housing Accord progress dataflow.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![Dataflow {
            id: dataflow_id(),
            source_id: source_id(),
            name: "National Housing Supply and Affordability Council Housing Accord progress".into(),
            description: Some(
                "National Housing Accord progress, completed homes, and target observations from NHSAC publication fixtures."
                    .into(),
            ),
            dimensions: vec![
                DimensionId::new("region").expect("static dimension id is valid"),
                DimensionId::new("measure").expect("static dimension id is valid"),
            ],
            measures: vec![
                MeasureId::new(PROGRESS_TO_TARGET_PCT).expect("static measure id is valid"),
                MeasureId::new(HOMES_COMPLETED).expect("static measure id is valid"),
                MeasureId::new(ANNUAL_TARGET).expect("static measure id is valid"),
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

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "NHSAC fetch received job for source `{}`",
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
                "NHSAC fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        release_url_provenance(&job.source_url).ok_or_else(|| {
            AdapterError::Validation(format!(
                "NHSAC fetch URL `{}` is not a Housing Accord progress artifact",
                job.source_url
            ))
        })?;
        Ok(())
    }
}

#[async_trait]
impl SourceAdapter for NhsacAdapter {
    fn id(&self) -> &'static str {
        "nhsac"
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    fn source_metadata(&self) -> Option<Source> {
        Some(Source {
            id: source_id(),
            name: "National Housing Supply and Affordability Council".into(),
            homepage: "https://nhsac.gov.au".into(),
            description: Some(
                "Australian Government advisory body publishing housing accord research and data."
                    .into(),
            ),
        })
    }

    fn dataflow_metadata(&self) -> Vec<Dataflow> {
        NhsacAdapter::dataflow_metadata(self)
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        if let Some(requested) = ctx.requested_dataflow_id() {
            if requested != &dataflow_id() {
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
        let current = parse_housing_accord_releases_with_base(&body, self.index_url())?;
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
        let response = ctx
            .http
            .execute(
                ctx.http
                    .raw_artifact()
                    .get(&job.source_url)
                    .header("user-agent", USER_AGENT)
                    .header("accept", "text/html,text/csv"),
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

fn parse_artifact_stream(artifact: ArtifactRef, ctx: &ParseCtx) -> ObservationStream<'_> {
    let provenance = match validate_parse_artifact(&artifact, ctx.expected_dataflow_id()) {
        Ok(provenance) => provenance,
        Err(err) => return Box::pin(stream::once(async move { Err(err) })),
    };

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

        if let Err(err) = parse_artifact(
            blob_store,
            key,
            artifact,
            provenance.artifact_kind,
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

async fn parse_artifact(
    blob_store: BlobStore,
    key: StorageKey,
    artifact: ArtifactRef,
    artifact_kind: HousingAccordArtifactKind,
    ingested_at: DateTime<Utc>,
    cancellation: CancellationToken,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    let bytes = load_artifact_bytes(&blob_store, &key, &cancellation).await?;
    let mut rows = match artifact_kind {
        HousingAccordArtifactKind::Csv => parse_csv_rows(bytes, &cancellation).await?,
        HousingAccordArtifactKind::QuarterlyHtml => parse_quarterly_report_rows(&bytes)?,
    };
    if matches!(artifact_kind, HousingAccordArtifactKind::QuarterlyHtml) {
        mark_html_derivations(&mut rows);
    }

    for row in parse_housing_accord_rows(rows, &artifact, ingested_at)? {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

async fn load_artifact_bytes(
    blob_store: &BlobStore,
    key: &StorageKey,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, AdapterError> {
    let mut chunks = tokio::select! {
        () = cancellation.cancelled() => return Err(cancelled_parse_error()),
        chunks = blob_store.get(key) => chunks?,
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

async fn parse_csv_rows(
    bytes: Vec<u8>,
    cancellation: &CancellationToken,
) -> Result<Vec<Vec<String>>, AdapterError> {
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
    Ok(rows)
}

fn parse_quarterly_report_rows(bytes: &[u8]) -> Result<Vec<Vec<String>>, AdapterError> {
    let body = std::str::from_utf8(bytes).map_err(|err| {
        AdapterError::FormatDrift(format!("NHSAC quarterly report HTML is not UTF-8: {err}"))
    })?;
    let text = clean_html_text(body)
        .ok_or_else(|| AdapterError::FormatDrift("NHSAC quarterly report HTML is empty".into()))?;
    let year = extract_report_year(body, &text)?;
    let completed = number_before_phrase(&text, "new homes completed")?;
    let target_total = number_before_phrase(&text, "million new homes")? * 1_000_000.0;
    let years = number_before_phrase(&text, "years to")?;
    if years <= 0.0 {
        return Err(AdapterError::FormatDrift(
            "NHSAC quarterly report target duration must be positive".into(),
        ));
    }
    let annual_target = target_total / years;
    let progress_pct = aus_built_share_pct(body)?;

    Ok(vec![
        housing_accord_header(),
        vec![
            year.clone(),
            "AUS".into(),
            PROGRESS_TO_TARGET_PCT.into(),
            "Housing Accord progress to target".into(),
            format_metric_value(progress_pct),
            "percent".into(),
            "normal".into(),
        ],
        vec![
            year.clone(),
            "AUS".into(),
            HOMES_COMPLETED.into(),
            "Homes completed under Housing Accord".into(),
            format_metric_value(completed),
            "dwellings".into(),
            "normal".into(),
        ],
        vec![
            year,
            "AUS".into(),
            ANNUAL_TARGET.into(),
            "Annual pro-rata Housing Accord target".into(),
            format_metric_value(annual_target),
            "dwellings".into(),
            "normal".into(),
        ],
    ])
}

fn mark_html_derivations(rows: &mut [Vec<String>]) {
    for row in rows.iter_mut().skip(1) {
        row.push(
            match row.get(2).map(String::as_str) {
                Some(ANNUAL_TARGET) => "derived",
                _ => "reported",
            }
            .into(),
        );
    }
}

fn parse_housing_accord_rows(
    rows: Vec<Vec<String>>,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let mut rows = rows.into_iter().filter(|row| !row_is_empty(row));
    let header = rows
        .next()
        .ok_or_else(|| AdapterError::FormatDrift("NHSAC housing accord CSV is empty".into()))?;
    validate_housing_accord_header(&header)?;

    let mut parsed = Vec::new();
    for row in rows {
        let period = required_cell(&row, 0, "period")?;
        let region = required_cell(&row, 1, "region")?;
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
                "invalid NHSAC housing accord measure `{measure_id}`: {err}"
            ))
        })?;
        let dimensions = BTreeMap::from([
            (
                DimensionId::new("measure").expect("static dimension id is valid"),
                nhsac_code_id("measure", measure_id)?,
            ),
            (
                DimensionId::new("region").expect("static dimension id is valid"),
                nhsac_code_id("region", region)?,
            ),
        ]);
        let series_key = SeriesKey::derive(
            &dataflow_id,
            &measure,
            dimensions
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        let mut attributes = BTreeMap::from([
            ("nhsac_measure_id".into(), measure_id.to_string()),
            ("nhsac_measure_name".into(), measure_name.to_string()),
            ("source_url".into(), artifact.source_url.clone()),
        ]);
        if let Some(value_derivation) = optional_cell(&row, 7) {
            attributes.insert(
                "nhsac_value_derivation".into(),
                value_derivation.to_string(),
            );
        }

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
            attributes,
            ingested_at,
            source_artifact_id: artifact.id,
        };
        parsed.push((descriptor, observation));
    }
    Ok(parsed)
}

fn validate_housing_accord_header(header: &[String]) -> Result<(), AdapterError> {
    let expected = [
        "period",
        "region",
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
            "NHSAC housing accord CSV header must start with `{}`",
            expected.join(",")
        )));
    }
    Ok(())
}

fn housing_accord_header() -> Vec<String> {
    [
        "period",
        "region",
        "measure_id",
        "measure_name",
        "value",
        "unit",
        "status",
        "value_derivation",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
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
        AdapterError::FormatDrift(format!("NHSAC housing accord row missing `{field}`"))
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
        .map_err(|_| AdapterError::FormatDrift(format!("invalid NHSAC period `{value}`")))?;
    Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0)
        .single()
        .ok_or_else(|| AdapterError::FormatDrift(format!("invalid NHSAC period `{value}`")))
}

fn parse_value_and_status(
    value: Option<&str>,
    status: &str,
) -> Result<(Option<f64>, ObservationStatus), AdapterError> {
    let parsed_value = match value {
        Some(value) if !matches!(value.to_ascii_lowercase().as_str(), "na" | "n/a" | "..") => Some(
            value
                .replace(',', "")
                .parse::<f64>()
                .map_err(|_| AdapterError::FormatDrift(format!("invalid NHSAC value `{value}`")))?,
        ),
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
                "invalid NHSAC observation status `{other}`"
            )));
        }
    };
    Ok((parsed_value, status))
}

fn validate_parse_artifact(
    artifact: &ArtifactRef,
    expected_dataflow_id: Option<&DataflowId>,
) -> Result<ReleaseUrlProvenance, AdapterError> {
    if artifact.source_id.as_str() != "nhsac" {
        return Err(AdapterError::Validation(format!(
            "NHSAC parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    if let Some(expected) = expected_dataflow_id {
        let actual = dataflow_id();
        if expected != &actual {
            return Err(AdapterError::Validation(format!(
                "NHSAC parse expected dataflow `{}` but adapter emits `{}`",
                expected.as_str(),
                actual.as_str()
            )));
        }
    }
    release_url_provenance(&artifact.source_url).ok_or_else(|| {
        AdapterError::Validation(format!(
            "NHSAC parse artifact `{}` is missing Housing Accord progress provenance",
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
            "NHSAC parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )));
    }

    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "NHSAC parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )))
    }
}

fn parse_housing_accord_releases_with_base(
    body: &str,
    base_url: &str,
) -> Result<Vec<NhsacHousingAccordRelease>, AdapterError> {
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
        releases.push(NhsacHousingAccordRelease {
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
        .replace("&#160;", " ")
        .replace("&ndash;", "-")
        .replace("&mdash;", "-")
        .replace("&#8211;", "-")
        .replace('\u{a0}', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    (!cleaned.is_empty()).then_some(cleaned)
}

fn extract_report_year(body: &str, text: &str) -> Result<String, AdapterError> {
    if let Some(datetime) = attr_value_after(body, "<time", "datetime") {
        if let Some(year) = datetime.get(..4) {
            if year.chars().all(|ch| ch.is_ascii_digit()) {
                return Ok(year.to_string());
            }
        }
    }
    find_year(text).map(|year| year.to_string()).ok_or_else(|| {
        AdapterError::FormatDrift("NHSAC quarterly report is missing a report year".into())
    })
}

fn attr_value_after(haystack: &str, marker: &str, name: &str) -> Option<String> {
    let start = haystack.find(marker)?;
    let rest = &haystack[start..];
    let open_end = rest.find('>')?;
    attr_value(&rest[..open_end + 1], name)
}

fn find_year(text: &str) -> Option<i32> {
    text.split(|ch: char| !ch.is_ascii_digit())
        .filter(|part| part.len() == 4)
        .find_map(|part| {
            let year = part.parse::<i32>().ok()?;
            (2000..=2100).contains(&year).then_some(year)
        })
}

fn number_before_phrase(text: &str, phrase: &str) -> Result<f64, AdapterError> {
    let lower = text.to_ascii_lowercase();
    let index = lower.find(phrase).ok_or_else(|| {
        AdapterError::FormatDrift(format!("NHSAC quarterly report is missing `{phrase}`"))
    })?;
    last_number(&text[..index]).ok_or_else(|| {
        AdapterError::FormatDrift(format!(
            "NHSAC quarterly report is missing a number before `{phrase}`"
        ))
    })
}

fn last_number(text: &str) -> Option<f64> {
    text.split_whitespace().rev().find_map(parse_number_token)
}

fn parse_number_token(token: &str) -> Option<f64> {
    let cleaned = token
        .trim_matches(|ch: char| !ch.is_ascii_digit() && ch != '.' && ch != '-' && ch != ',')
        .replace(',', "");
    if cleaned.is_empty() || cleaned == "-" {
        return None;
    }
    cleaned.parse::<f64>().ok()
}

fn aus_built_share_pct(body: &str) -> Result<f64, AdapterError> {
    let row = find_html_table_row(body, "AUS").ok_or_else(|| {
        AdapterError::FormatDrift("NHSAC quarterly report is missing the AUS progress row".into())
    })?;
    let percent_values = html_table_cells(row)
        .into_iter()
        .filter_map(|cell| percent_cell_value(&cell))
        .collect::<Vec<_>>();
    percent_values.get(3).copied().ok_or_else(|| {
        AdapterError::FormatDrift(
            "NHSAC quarterly report AUS row is missing built-to-date percent".into(),
        )
    })
}

fn find_html_table_row<'a>(body: &'a str, required_cell: &str) -> Option<&'a str> {
    let mut rest = body;
    loop {
        let lower = rest.to_ascii_lowercase();
        let start = lower.find("<tr")?;
        rest = &rest[start..];
        let lower = rest.to_ascii_lowercase();
        let end = lower.find("</tr>")? + "</tr>".len();
        let row = &rest[..end];
        if html_table_cells(row)
            .iter()
            .any(|cell| cell.eq_ignore_ascii_case(required_cell))
        {
            return Some(row);
        }
        rest = &rest[end..];
    }
}

fn html_table_cells(row: &str) -> Vec<String> {
    let mut cells = Vec::new();
    let mut rest = row;
    while let Some((start, tag)) = next_cell_start(rest) {
        rest = &rest[start..];
        let Some(open_end) = rest.find('>') else {
            break;
        };
        let close = format!("</{tag}>");
        let lower = rest[open_end + 1..].to_ascii_lowercase();
        let Some(close_start) = lower.find(&close) else {
            break;
        };
        let text = &rest[open_end + 1..open_end + 1 + close_start];
        if let Some(cleaned) = clean_html_text(text) {
            cells.push(cleaned);
        }
        rest = &rest[open_end + 1 + close_start + close.len()..];
    }
    cells
}

fn next_cell_start(haystack: &str) -> Option<(usize, &'static str)> {
    let lower = haystack.to_ascii_lowercase();
    match (lower.find("<td"), lower.find("<th")) {
        (Some(td), Some(th)) if td < th => Some((td, "td")),
        (Some(_), Some(th)) => Some((th, "th")),
        (Some(td), None) => Some((td, "td")),
        (None, Some(th)) => Some((th, "th")),
        (None, None) => None,
    }
}

fn percent_cell_value(cell: &str) -> Option<f64> {
    cell.strip_suffix('%').and_then(parse_number_token)
}

fn format_metric_value(value: f64) -> String {
    if value.fract().abs() < f64::EPSILON {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn resolve_url(base_url: &str, href: &str) -> Result<String, AdapterError> {
    if href.starts_with("https://") || href.starts_with("http://") {
        return Ok(href.to_string());
    }
    if href.starts_with('/') {
        let scheme_end = base_url.find("://").ok_or_else(|| {
            AdapterError::Validation(format!("NHSAC index URL `{base_url}` is not absolute"))
        })?;
        let path_start = base_url[scheme_end + 3..]
            .find('/')
            .map_or(base_url.len(), |index| scheme_end + 3 + index);
        return Ok(format!("{}{}", &base_url[..path_start], href));
    }
    let Some((prefix, _)) = base_url.rsplit_once('/') else {
        return Err(AdapterError::Validation(format!(
            "NHSAC index URL `{base_url}` has no path separator"
        )));
    };
    Ok(format!("{prefix}/{href}"))
}

#[derive(Debug, Clone)]
struct ReleaseUrlProvenance {
    release_id: String,
    artifact_kind: HousingAccordArtifactKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HousingAccordArtifactKind {
    Csv,
    QuarterlyHtml,
}

impl HousingAccordArtifactKind {
    fn metadata_format(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::QuarterlyHtml => "html",
        }
    }
}

fn release_url_provenance(source_url: &str) -> Option<ReleaseUrlProvenance> {
    if let Some((_, path)) = source_url.split_once("/publications/") {
        let filename = last_clean_path_segment(path)?;
        let stem = filename.strip_suffix(".csv")?;
        if stem.starts_with("housing-accord-progress-") {
            return Some(ReleaseUrlProvenance {
                release_id: stem.to_string(),
                artifact_kind: HousingAccordArtifactKind::Csv,
            });
        }
    }
    if let Some((_, path)) = source_url.split_once("/reports-and-submissions/") {
        let slug = last_clean_path_segment(path)?
            .strip_suffix(".html")
            .unwrap_or_else(|| last_clean_path_segment(path).expect("path segment exists"));
        if slug.starts_with("quarterly-report-") {
            return Some(ReleaseUrlProvenance {
                release_id: slug.to_string(),
                artifact_kind: HousingAccordArtifactKind::QuarterlyHtml,
            });
        }
    }
    None
}

fn last_clean_path_segment(path: &str) -> Option<&str> {
    path.split('?')
        .next()?
        .split('#')
        .next()?
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty())
}

fn nhsac_code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!("invalid NHSAC {field} code `{value}`: {err}"))
    })
}

fn source_id() -> SourceId {
    SourceId::new("nhsac").expect("static source id is valid")
}

fn dataflow_id() -> DataflowId {
    DataflowId::new(DATAFLOW_ID).expect("static dataflow id is valid")
}

fn cancelled_parse_error() -> AdapterError {
    CoreError::Io(io::Error::new(
        io::ErrorKind::Interrupted,
        "NHSAC parse cancelled",
    ))
    .into()
}

/// Builder for [`NhsacAdapter`].
#[derive(Debug, Clone)]
pub struct NhsacAdapterBuilder {
    index_url: String,
}

impl Default for NhsacAdapterBuilder {
    fn default() -> Self {
        Self {
            index_url: DEFAULT_INDEX_URL.into(),
        }
    }
}

impl NhsacAdapterBuilder {
    /// Override the housing accord insights index URL, usually for fixture tests.
    #[must_use]
    pub fn index_url(mut self, index_url: impl Into<String>) -> Self {
        self.index_url = index_url.into();
        self
    }

    /// Build the adapter.
    #[must_use]
    pub fn build(self) -> NhsacAdapter {
        NhsacAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: "National Housing Supply and Affordability Council".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(30, Duration::from_secs(60))
                    .expect("static NHSAC rate limit is valid"),
                dataflows: vec![dataflow_id()],
            },
            index_url: self.index_url,
        }
    }
}

/// One National Housing Supply and Affordability Council Housing Accord progress artifact link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NhsacHousingAccordRelease {
    /// Stable source-local release id, derived from the CSV filename.
    pub release_id: String,
    /// Link text or release title from the index.
    pub title: String,
    /// Canonical artifact URL.
    pub source_url: String,
    /// Optional update marker scraped from the index.
    pub last_updated: Option<String>,
}

impl NhsacHousingAccordRelease {
    /// Build a NHSAC release revision from a version and optional update marker.
    #[must_use]
    pub fn revision_for(version: &str, last_updated: Option<&str>) -> UpstreamRevision {
        UpstreamRevision::new(version, last_updated)
    }

    fn revision_key(&self) -> String {
        format!("NHSAC:{}", self.release_id)
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
        let artifact_format = release_url_provenance(&self.source_url)
            .map(|provenance| provenance.artifact_kind.metadata_format())
            .unwrap_or("csv");
        DiscoveredJob {
            id: format!("nhsac:{}:{revision_version}", self.release_id),
            source_id: source_id(),
            dataflow_id: dataflow_id(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("adapter".into(), "nhsac".into()),
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
