//! Productivity Commission adapter for productivity bulletin artifacts.

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

const DEFAULT_INDEX_URL: &str = "https://www.pc.gov.au/ongoing/productivity-insights/bulletins/";
const DEFAULT_SOURCE_URL: &str = "https://www.pc.gov.au/ongoing/productivity-insights";
const USER_AGENT: &str = concat!("au-kpis-adapter-pc/", env!("CARGO_PKG_VERSION"));
const DATAFLOW_ID: &str = "pc.productivity_bulletin";
const ATTRIBUTION: &str = "Source: Productivity Commission";
const LICENSE_NAME: &str = "CC-BY-4.0";
const LICENSE_URL: &str = "https://www.pc.gov.au/copyright";
const MARKET_SECTOR_GROWTH: &str = "market_sector_growth";
const MULTIFACTOR_PRODUCTIVITY_GROWTH: &str = "multifactor_productivity_growth";

/// Productivity Commission productivity bulletin adapter.
#[derive(Debug, Clone)]
pub struct PcAdapter {
    manifest: AdapterManifest,
    index_url: String,
}

impl Default for PcAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl PcAdapter {
    /// Start building a PC adapter.
    #[must_use]
    pub fn builder() -> PcAdapterBuilder {
        PcAdapterBuilder::default()
    }

    /// Parse the Productivity Commission productivity insights page.
    pub fn parse_productivity_bulletins(
        body: &str,
    ) -> Result<Vec<PcProductivityBulletin>, AdapterError> {
        parse_productivity_bulletins_with_base(body, DEFAULT_INDEX_URL)
    }

    /// Diff current bulletin links against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[PcProductivityBulletin],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        current
            .iter()
            .filter_map(|bulletin| {
                let revision = bulletin.revision(started_at);
                known_revisions
                    .get(&bulletin.revision_key())
                    .is_none_or(|known| known != &revision)
                    .then(|| {
                        bulletin.to_discovered_job(started_at, trace_parent, DEFAULT_INDEX_URL)
                    })
            })
            .collect()
    }

    /// Convert current bulletin links into jobs for the supplied timestamp.
    #[must_use]
    pub fn current_jobs_with_started_at(
        current: &[PcProductivityBulletin],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Static metadata for the Productivity Commission productivity bulletin dataflow.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![Dataflow {
            id: dataflow_id(),
            source_id: source_id(),
            name: "Productivity Commission productivity bulletin".into(),
            description: Some(
                "Headline national labour productivity and multifactor productivity growth observations from Productivity Commission productivity bulletin fixtures."
                    .into(),
            ),
            dimensions: vec![
                DimensionId::new("region").expect("static dimension id is valid"),
                DimensionId::new("measure").expect("static dimension id is valid"),
            ],
            measures: vec![
                MeasureId::new(MARKET_SECTOR_GROWTH).expect("static measure id is valid"),
                MeasureId::new(MULTIFACTOR_PRODUCTIVITY_GROWTH)
                    .expect("static measure id is valid"),
            ],
            frequency: Frequency::Annual,
            license: License::CcBy40,
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
                "PC fetch received job for source `{}`",
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
                "PC fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        bulletin_url_provenance(&job.source_url).ok_or_else(|| {
            AdapterError::Validation(format!(
                "PC fetch URL `{}` is not a productivity bulletin artifact",
                job.source_url
            ))
        })?;
        Ok(())
    }
}

#[async_trait]
impl SourceAdapter for PcAdapter {
    fn id(&self) -> &'static str {
        "pc"
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    fn source_metadata(&self) -> Option<Source> {
        Some(Source {
            id: source_id(),
            name: "Productivity Commission".into(),
            homepage: "https://www.pc.gov.au".into(),
            description: Some(
                "Australian Government advisory body publishing productivity research and data."
                    .into(),
            ),
        })
    }

    fn dataflow_metadata(&self) -> Vec<Dataflow> {
        PcAdapter::dataflow_metadata(self)
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
        let current = parse_productivity_bulletins_with_base(&body, self.index_url())?;
        Ok(current
            .iter()
            .filter_map(|bulletin| {
                let revision = bulletin.revision(ctx.started_at);
                ctx.known_revisions()
                    .get(&bulletin.revision_key())
                    .is_none_or(|known| known != &revision)
                    .then(|| {
                        bulletin.to_discovered_job(
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
    artifact_kind: BulletinArtifactKind,
    ingested_at: DateTime<Utc>,
    cancellation: CancellationToken,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    let bytes = load_artifact_bytes(&blob_store, &key, &cancellation).await?;
    let rows = match artifact_kind {
        BulletinArtifactKind::Csv => parse_csv_rows(bytes, &cancellation).await?,
        BulletinArtifactKind::AnnualHtml => parse_annual_bulletin_rows(&bytes)?,
    };

    for row in parse_productivity_rows(rows, &artifact, ingested_at)? {
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

fn parse_annual_bulletin_rows(bytes: &[u8]) -> Result<Vec<Vec<String>>, AdapterError> {
    let body = std::str::from_utf8(bytes).map_err(|err| {
        AdapterError::FormatDrift(format!(
            "PC annual productivity bulletin HTML is not UTF-8: {err}"
        ))
    })?;
    let text = clean_html_text(body).ok_or_else(|| {
        AdapterError::FormatDrift("PC annual productivity bulletin HTML is empty".into())
    })?;
    let release_date = extract_release_date(&text)?;
    let reference_period = extract_reference_period(&text)?;
    let period_year = financial_year_end_year(&reference_period)?;
    let mfp_growth = extract_mfp_growth(&text)?;

    Ok(vec![
        productivity_header(),
        vec![
            period_year.clone(),
            "AUS".into(),
            MARKET_SECTOR_GROWTH.into(),
            "Market-sector productivity growth proxy".into(),
            format_metric_value(mfp_growth),
            "percent".into(),
            "normal".into(),
            release_date.clone(),
            reference_period.clone(),
            "multifactor productivity (MFP)".into(),
            "mapped_from_mfp_annual_bulletin".into(),
        ],
        vec![
            period_year,
            "AUS".into(),
            MULTIFACTOR_PRODUCTIVITY_GROWTH.into(),
            "Multifactor productivity growth".into(),
            format_metric_value(mfp_growth),
            "percent".into(),
            "normal".into(),
            release_date,
            reference_period,
            "multifactor productivity (MFP)".into(),
            "reported".into(),
        ],
    ])
}

fn parse_productivity_rows(
    rows: Vec<Vec<String>>,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let mut rows = rows.into_iter().filter(|row| !row_is_empty(row));
    let header = rows
        .next()
        .ok_or_else(|| AdapterError::FormatDrift("PC productivity CSV is empty".into()))?;
    validate_productivity_header(&header)?;

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
                "invalid PC productivity measure `{measure_id}`: {err}"
            ))
        })?;
        let dimensions = BTreeMap::from([
            (
                DimensionId::new("measure").expect("static dimension id is valid"),
                pc_code_id("measure", measure_id)?,
            ),
            (
                DimensionId::new("region").expect("static dimension id is valid"),
                pc_code_id("region", region)?,
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
            ("pc_measure_id".into(), measure_id.to_string()),
            ("pc_measure_name".into(), measure_name.to_string()),
            ("source_url".into(), artifact.source_url.clone()),
        ]);
        if let Some(release_date) = optional_cell(&row, 7) {
            attributes.insert("pc_release_date".into(), release_date.to_string());
        }
        if let Some(reference_period) = optional_cell(&row, 8) {
            attributes.insert("pc_reference_period".into(), reference_period.to_string());
        }
        if let Some(metric_label) = optional_cell(&row, 9) {
            attributes.insert("pc_metric_label".into(), metric_label.to_string());
        }
        if let Some(value_derivation) = optional_cell(&row, 10) {
            attributes.insert("pc_value_derivation".into(), value_derivation.to_string());
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

fn validate_productivity_header(header: &[String]) -> Result<(), AdapterError> {
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
            "PC productivity CSV header must start with `{}`",
            expected.join(",")
        )));
    }
    Ok(())
}

fn productivity_header() -> Vec<String> {
    [
        "period",
        "region",
        "measure_id",
        "measure_name",
        "value",
        "unit",
        "status",
        "release_date",
        "reference_period",
        "metric_label",
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
    optional_cell(row, index)
        .ok_or_else(|| AdapterError::FormatDrift(format!("PC productivity row missing `{field}`")))
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
        .map_err(|_| AdapterError::FormatDrift(format!("invalid PC period `{value}`")))?;
    Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0)
        .single()
        .ok_or_else(|| AdapterError::FormatDrift(format!("invalid PC period `{value}`")))
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
                .map_err(|_| AdapterError::FormatDrift(format!("invalid PC value `{value}`")))?,
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
                "invalid PC observation status `{other}`"
            )));
        }
    };
    Ok((parsed_value, status))
}

fn validate_parse_artifact(
    artifact: &ArtifactRef,
    expected_dataflow_id: Option<&DataflowId>,
) -> Result<BulletinUrlProvenance, AdapterError> {
    if artifact.source_id.as_str() != "pc" {
        return Err(AdapterError::Validation(format!(
            "PC parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    if let Some(expected) = expected_dataflow_id {
        let actual = dataflow_id();
        if expected != &actual {
            return Err(AdapterError::Validation(format!(
                "PC parse expected dataflow `{}` but adapter emits `{}`",
                expected.as_str(),
                actual.as_str()
            )));
        }
    }
    bulletin_url_provenance(&artifact.source_url).ok_or_else(|| {
        AdapterError::Validation(format!(
            "PC parse artifact `{}` is missing productivity bulletin provenance",
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
            "PC parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )));
    }

    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "PC parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )))
    }
}

fn parse_productivity_bulletins_with_base(
    body: &str,
    base_url: &str,
) -> Result<Vec<PcProductivityBulletin>, AdapterError> {
    let mut bulletins = Vec::new();
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
        let Some(provenance) = bulletin_url_provenance(&source_url) else {
            continue;
        };
        let source_url = canonical_bulletin_url(source_url, provenance.artifact_kind);
        let title = clean_html_text(text).unwrap_or_else(|| provenance.bulletin_id.clone());
        bulletins.push(PcProductivityBulletin {
            bulletin_id: provenance.bulletin_id,
            title,
            source_url,
            last_updated: attr_value(attrs, "data-updated"),
        });
    }
    bulletins.sort_by(|left, right| left.bulletin_id.cmp(&right.bulletin_id));
    bulletins.dedup_by(|left, right| left.bulletin_id == right.bulletin_id);
    retain_latest_annual_html_bulletin(&mut bulletins);
    Ok(bulletins)
}

fn retain_latest_annual_html_bulletin(bulletins: &mut Vec<PcProductivityBulletin>) {
    let latest_annual = bulletins
        .iter()
        .filter(|bulletin| {
            bulletin_url_provenance(&bulletin.source_url).is_some_and(|provenance| {
                provenance.artifact_kind == BulletinArtifactKind::AnnualHtml
            })
        })
        .map(|bulletin| bulletin.bulletin_id.clone())
        .max();
    let Some(latest_annual) = latest_annual else {
        return;
    };
    bulletins.retain(|bulletin| {
        bulletin_url_provenance(&bulletin.source_url).is_none_or(|provenance| {
            provenance.artifact_kind != BulletinArtifactKind::AnnualHtml
                || bulletin.bulletin_id == latest_annual
        })
    });
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

fn extract_release_date(text: &str) -> Result<String, AdapterError> {
    let lower = text.to_ascii_lowercase();
    let released_at = lower.find("released").ok_or_else(|| {
        AdapterError::FormatDrift("PC annual productivity bulletin missing release date".into())
    })?;
    let date_text = &text[released_at..text.len().min(released_at + 80)];
    let numbers = date_text
        .split(|ch: char| !ch.is_ascii_digit())
        .filter(|part| !part.is_empty())
        .take(3)
        .map(str::to_string)
        .collect::<Vec<_>>();
    if numbers.len() != 3 {
        return Err(AdapterError::FormatDrift(
            "PC annual productivity bulletin release date is incomplete".into(),
        ));
    }
    let day = numbers[0].parse::<u32>().map_err(|_| {
        AdapterError::FormatDrift("PC annual productivity bulletin release day is invalid".into())
    })?;
    let month = numbers[1].parse::<u32>().map_err(|_| {
        AdapterError::FormatDrift("PC annual productivity bulletin release month is invalid".into())
    })?;
    let year = numbers[2].parse::<i32>().map_err(|_| {
        AdapterError::FormatDrift("PC annual productivity bulletin release year is invalid".into())
    })?;
    Utc.with_ymd_and_hms(year, month, day, 0, 0, 0)
        .single()
        .ok_or_else(|| {
            AdapterError::FormatDrift(
                "PC annual productivity bulletin release date is invalid".into(),
            )
        })?;
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

fn extract_reference_period(text: &str) -> Result<String, AdapterError> {
    let lower = text.to_ascii_lowercase();
    for marker in [" in ", " over ", " between "] {
        let mut offset = 0;
        while let Some(found) = lower[offset..].find(marker) {
            let start = offset + found + marker.len();
            if let Some(period) = financial_year_at(&text[start..]) {
                return Ok(period.to_string());
            }
            offset = start;
        }
    }
    Err(AdapterError::FormatDrift(
        "PC annual productivity bulletin missing reference period".into(),
    ))
}

fn financial_year_at(text: &str) -> Option<&str> {
    let bytes = text.as_bytes();
    if bytes.len() < 7 {
        return None;
    }
    for start in 0..=bytes.len() - 7 {
        let candidate = &text[start..start + 7];
        let candidate_bytes = candidate.as_bytes();
        if candidate_bytes[0..4].iter().all(u8::is_ascii_digit)
            && candidate_bytes[4] == b'-'
            && candidate_bytes[5..7].iter().all(u8::is_ascii_digit)
        {
            return Some(candidate);
        }
    }
    None
}

fn financial_year_end_year(period: &str) -> Result<String, AdapterError> {
    let (start, end) = period.split_once('-').ok_or_else(|| {
        AdapterError::FormatDrift(format!("invalid PC reference period `{period}`"))
    })?;
    let century = start.get(..2).ok_or_else(|| {
        AdapterError::FormatDrift(format!("invalid PC reference period `{period}`"))
    })?;
    let year = format!("{century}{end}");
    year.parse::<i32>().map_err(|_| {
        AdapterError::FormatDrift(format!("invalid PC reference period `{period}`"))
    })?;
    Ok(year)
}

fn extract_mfp_growth(text: &str) -> Result<f64, AdapterError> {
    let lower = text.to_ascii_lowercase();
    let mfp_at = lower
        .find("mfp")
        .or_else(|| lower.find("multifactor productivity"))
        .ok_or_else(|| {
            AdapterError::FormatDrift("PC annual productivity bulletin missing MFP label".into())
        })?;
    let window = &text[mfp_at..text.len().min(mfp_at + 700)];
    let window_lower = window.to_ascii_lowercase();
    for (phrase, sign) in [
        ("declined by", -1.0),
        ("decreased by", -1.0),
        ("fell by", -1.0),
        ("rose by", 1.0),
        ("increased by", 1.0),
        ("grew by", 1.0),
    ] {
        if let Some(index) = window_lower.find(phrase) {
            let after = &window[index + phrase.len()..];
            let value = first_number(before_percent(after)?)?;
            return Ok(sign * value);
        }
    }
    Err(AdapterError::FormatDrift(
        "PC annual productivity bulletin missing MFP growth value".into(),
    ))
}

fn before_percent(text: &str) -> Result<&str, AdapterError> {
    let percent = text.find('%').ok_or_else(|| {
        AdapterError::FormatDrift(
            "PC annual productivity bulletin MFP value missing percent".into(),
        )
    })?;
    Ok(&text[..percent])
}

fn first_number(text: &str) -> Result<f64, AdapterError> {
    text.split_whitespace()
        .find_map(parse_number_token)
        .ok_or_else(|| {
            AdapterError::FormatDrift("PC annual productivity bulletin MFP value is missing".into())
        })
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
            AdapterError::Validation(format!("PC index URL `{base_url}` is not absolute"))
        })?;
        let path_start = base_url[scheme_end + 3..]
            .find('/')
            .map_or(base_url.len(), |index| scheme_end + 3 + index);
        return Ok(format!("{}{}", &base_url[..path_start], href));
    }
    let Some((prefix, _)) = base_url.rsplit_once('/') else {
        return Err(AdapterError::Validation(format!(
            "PC index URL `{base_url}` has no path separator"
        )));
    };
    Ok(format!("{prefix}/{href}"))
}

#[derive(Debug, Clone)]
struct BulletinUrlProvenance {
    bulletin_id: String,
    artifact_kind: BulletinArtifactKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BulletinArtifactKind {
    Csv,
    AnnualHtml,
}

impl BulletinArtifactKind {
    fn metadata_format(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::AnnualHtml => "html",
        }
    }
}

fn bulletin_url_provenance(source_url: &str) -> Option<BulletinUrlProvenance> {
    let marker = "/ongoing/productivity-insights/";
    let (_, path) = source_url.split_once(marker)?;
    let segment = last_clean_path_segment(path)?;
    if let Some(stem) = segment.strip_suffix(".csv") {
        if stem.starts_with("productivity-bulletin-") {
            return Some(BulletinUrlProvenance {
                bulletin_id: stem.to_string(),
                artifact_kind: BulletinArtifactKind::Csv,
            });
        }
    }
    if segment.starts_with("bulletin-")
        && segment["bulletin-".len()..]
            .chars()
            .all(|ch| ch.is_ascii_digit())
    {
        return Some(BulletinUrlProvenance {
            bulletin_id: segment.to_string(),
            artifact_kind: BulletinArtifactKind::AnnualHtml,
        });
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

fn canonical_bulletin_url(mut source_url: String, artifact_kind: BulletinArtifactKind) -> String {
    if matches!(artifact_kind, BulletinArtifactKind::AnnualHtml)
        && !source_url.ends_with('/')
        && !source_url.contains('?')
        && !source_url.contains('#')
    {
        source_url.push('/');
    }
    source_url
}

fn pc_code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!("invalid PC {field} code `{value}`: {err}"))
    })
}

fn source_id() -> SourceId {
    SourceId::new("pc").expect("static source id is valid")
}

fn dataflow_id() -> DataflowId {
    DataflowId::new(DATAFLOW_ID).expect("static dataflow id is valid")
}

fn cancelled_parse_error() -> AdapterError {
    CoreError::Io(io::Error::new(
        io::ErrorKind::Interrupted,
        "PC parse cancelled",
    ))
    .into()
}

/// Builder for [`PcAdapter`].
#[derive(Debug, Clone)]
pub struct PcAdapterBuilder {
    index_url: String,
}

impl Default for PcAdapterBuilder {
    fn default() -> Self {
        Self {
            index_url: DEFAULT_INDEX_URL.into(),
        }
    }
}

impl PcAdapterBuilder {
    /// Override the productivity insights index URL, usually for fixture tests.
    #[must_use]
    pub fn index_url(mut self, index_url: impl Into<String>) -> Self {
        self.index_url = index_url.into();
        self
    }

    /// Build the adapter.
    #[must_use]
    pub fn build(self) -> PcAdapter {
        PcAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: "Productivity Commission".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(30, Duration::from_secs(60))
                    .expect("static PC rate limit is valid"),
                dataflows: vec![dataflow_id()],
            },
            index_url: self.index_url,
        }
    }
}

/// One Productivity Commission productivity bulletin artifact link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PcProductivityBulletin {
    /// Stable source-local bulletin id, derived from the CSV filename.
    pub bulletin_id: String,
    /// Link text or bulletin title from the index.
    pub title: String,
    /// Canonical artifact URL.
    pub source_url: String,
    /// Optional update marker scraped from the index.
    pub last_updated: Option<String>,
}

impl PcProductivityBulletin {
    /// Build a PC bulletin revision from a version and optional update marker.
    #[must_use]
    pub fn revision_for(version: &str, last_updated: Option<&str>) -> UpstreamRevision {
        UpstreamRevision::new(version, last_updated)
    }

    fn revision_key(&self) -> String {
        format!("PC:{}", self.bulletin_id)
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
        let artifact_format = bulletin_url_provenance(&self.source_url)
            .map(|provenance| provenance.artifact_kind.metadata_format())
            .unwrap_or("csv");
        DiscoveredJob {
            id: format!("pc:{}:{revision_version}", self.bulletin_id),
            source_id: source_id(),
            dataflow_id: dataflow_id(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("adapter".into(), "pc".into()),
                ("artifact_format".into(), artifact_format.into()),
                ("attribution".into(), ATTRIBUTION.into()),
                ("bulletin_id".into(), self.bulletin_id.clone()),
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
