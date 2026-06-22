//! ASX adapter for public historical market statistics.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{collections::BTreeMap, time::Duration};

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
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use futures::{StreamExt, stream};
use tokio_util::sync::CancellationToken;

const DEFAULT_MARKET_STATISTICS_URL: &str =
    "https://www.asx.com.au/about/market-statistics/historical-market-statistics";
const USER_AGENT: &str = concat!("au-kpis-adapter-asx/", env!("CARGO_PKG_VERSION"));
const DATAFLOW_ID: &str = "asx.market_statistics";
const SOURCE_NAME: &str = "ASX";
const ATTRIBUTION: &str = "Source: ASX";
const LICENSE_NAME: &str = "ASX Terms of Use";
const LICENSE_URL: &str = "https://www.asx.com.au/terms-of-use";

/// ASX public market-statistics adapter.
#[derive(Debug, Clone)]
pub struct AsxAdapter {
    manifest: AdapterManifest,
    market_statistics_url: String,
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
        vec![market_statistics_job(
            DEFAULT_MARKET_STATISTICS_URL,
            started_at,
            trace_parent,
        )]
    }

    /// Diff the current monthly page revision against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        market_statistics_url: &str,
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        let job = market_statistics_job(market_statistics_url, started_at, trace_parent);
        let known = known_revisions.get(&job.metadata["revision_key"]);
        let revision = UpstreamRevision::new(
            job.metadata["revision_version"].clone(),
            Some(job.source_url.clone()),
        );
        if known.is_none_or(|known| known != &revision) {
            vec![job]
        } else {
            Vec::new()
        }
    }

    /// Static metadata for ASX market-statistics observations.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![Dataflow {
            id: dataflow_id(),
            source_id: source_id(),
            name: "ASX historical market statistics".into(),
            description: Some(
                "Monthly ASX index, market capitalisation, and listed-entity counts from the public historical market statistics page."
                    .into(),
            ),
            dimensions: vec![DimensionId::new("metric").expect("static dimension id is valid")],
            measures: vec![MeasureId::new("value").expect("static measure id is valid")],
            frequency: Frequency::Monthly,
            license: License::Other(LICENSE_NAME.into()),
            attribution: ATTRIBUTION.into(),
            source_url: DEFAULT_MARKET_STATISTICS_URL.into(),
        }]
    }

    fn market_statistics_url(&self) -> &str {
        &self.market_statistics_url
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
        market_statistics_provenance(&job.source_url).ok_or_else(|| {
            AdapterError::Validation(format!(
                "ASX fetch URL `{}` is not the market-statistics page",
                job.source_url
            ))
        })?;
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
        Ok(Self::discoverable_jobs_with_started_at(
            self.market_statistics_url(),
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
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map_or_else(|| "text/html".to_string(), str::to_string);

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

/// Builder for [`AsxAdapter`].
#[derive(Debug, Clone)]
pub struct AsxAdapterBuilder {
    market_statistics_url: String,
}

impl Default for AsxAdapterBuilder {
    fn default() -> Self {
        Self {
            market_statistics_url: DEFAULT_MARKET_STATISTICS_URL.into(),
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
                dataflows: vec![dataflow_id()],
            },
            market_statistics_url: self.market_statistics_url,
        }
    }
}

fn parse_artifact_stream(artifact: ArtifactRef, ctx: &ParseCtx) -> ObservationStream<'_> {
    if let Err(err) = validate_parse_artifact(&artifact) {
        return Box::pin(stream::once(async move { Err(err) }));
    }

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

        let result = parse_market_statistics_artifact(
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

async fn parse_market_statistics_artifact(
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
    let html = String::from_utf8(bytes)
        .map_err(|err| AdapterError::FormatDrift(format!("ASX market statistics HTML: {err}")))?;
    let rows = market_statistics_observations(&html, &artifact, ingested_at)?;
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
    let dataflow_id = dataflow_id();
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

fn validate_parse_artifact(artifact: &ArtifactRef) -> Result<(), AdapterError> {
    if artifact.source_id.as_str() != "asx" {
        return Err(AdapterError::Validation(format!(
            "ASX parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    market_statistics_provenance(&artifact.source_url).ok_or_else(|| {
        AdapterError::Validation(format!(
            "ASX parse artifact `{}` is missing market-statistics provenance",
            artifact.source_url
        ))
    })?;
    Ok(())
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

fn market_statistics_job(
    market_statistics_url: &str,
    started_at: DateTime<Utc>,
    trace_parent: Option<&str>,
) -> DiscoveredJob {
    let revision_version = format!("{}-{:02}", started_at.year(), started_at.month());
    DiscoveredJob {
        id: format!("asx:market-statistics:{revision_version}"),
        source_id: source_id(),
        dataflow_id: dataflow_id(),
        source_url: market_statistics_url.into(),
        trace_parent: trace_parent.map(str::to_owned),
        metadata: BTreeMap::from([
            ("artifact_format".into(), "html".into()),
            ("revision_key".into(), "ASX:market-statistics".into()),
            ("revision_version".into(), revision_version),
            ("cadence".into(), "monthly".into()),
            ("attribution".into(), ATTRIBUTION.into()),
            ("license".into(), LICENSE_NAME.into()),
            ("license_url".into(), LICENSE_URL.into()),
        ]),
    }
}

fn market_statistics_provenance(source_url: &str) -> Option<()> {
    source_url
        .contains("/about/market-statistics/historical-market-statistics")
        .then_some(())
}

fn asx_code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!("invalid ASX {field} code `{value}`: {err}"))
    })
}

fn cancelled_parse_error() -> AdapterError {
    AdapterError::Validation("ASX parse cancelled".into())
}

fn source_id() -> SourceId {
    SourceId::new("asx").expect("static source id is valid")
}

fn dataflow_id() -> DataflowId {
    DataflowId::new(DATAFLOW_ID).expect("static dataflow id is valid")
}
