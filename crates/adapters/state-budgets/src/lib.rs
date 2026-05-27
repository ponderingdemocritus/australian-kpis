//! State budget adapters backed by curated PDF publications.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{collections::BTreeMap, io, time::Duration};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterManifest, ArtifactDateRange, ArtifactRef, DiscoveredJob, DiscoveryCtx,
    ExpectedSchemaHash, FetchCtx, ObservationStream, ParseCtx, ParserVersion, RateLimit,
    SourceAdapter, UpstreamRevision, capture_response_headers, retry_after_delta,
    select_parser_version, validate_schema_hash,
};
use au_kpis_domain::{
    Artifact, ArtifactId, CodeId, Dataflow, DataflowId, DimensionId, Frequency, License, MeasureId,
    Observation, ObservationStatus, SeriesDescriptor, SeriesKey, SourceId, TimePrecision,
};
use au_kpis_error::{Classify, CoreError, ErrorClass};
use au_kpis_pdf_client::{
    BackendInfo, ExtractRequest, ExtractionBackendKind, ExtractionResponse, ExtractionStrategy,
    PdfClient, PdfClientError, TableCandidate,
};
use au_kpis_storage::{BlobStore, StorageKey};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use futures::{StreamExt, stream};

const DEFAULT_PDF_BASE_URL: &str = "http://127.0.0.1:8010";
const USER_AGENT: &str = concat!("au-kpis-adapter-state-budgets/", env!("CARGO_PKG_VERSION"));
const SOURCE_ID: &str = "state-budgets";
const DATAFLOW_ID: &str = "state_budgets.nsw_budget";
const JURISDICTION: &str = "NSW";
const JURISDICTION_NAME: &str = "New South Wales";
const SOURCE_NAME: &str = "NSW Treasury";
const ATTRIBUTION: &str = "Source: NSW Treasury";
const LICENSE_NAME: &str = "Creative Commons Attribution 3.0 Australia Licence";
const LICENSE_URL: &str = "https://creativecommons.org/licenses/by/3.0/au/";
const DEFAULT_SOURCE_INDEX_URL: &str = "https://www.budget.nsw.gov.au/2025-26/budget-papers";
const DEFAULT_BUDGET_PDF_URL: &str = "https://www.budget.nsw.gov.au/sites/default/files/2025-06/bp1-budget-statement-nsw-budget-2025-26.pdf";
const PAPER: &str = "Budget Paper No. 1";
const PAPER_SLUG: &str = "bp1-budget-statement";
const TARGET_TITLE: &str = "Budget Statement";
const NSW_KEY_AGGREGATES_SCHEMA_KEY: &str = "table_1_1_key_fiscal_aggregates_m";
const NSW_KEY_AGGREGATES_SCHEMA_HASH: &str =
    "61014127d5e49374262775674f0abd3bf87731a276cc1deecb69381d4bf811aa";

/// NSW state budget PDF adapter.
#[derive(Debug, Clone)]
pub struct NswBudgetAdapter {
    manifest: AdapterManifest,
    publications: Vec<NswBudgetPublication>,
    pdf_client: PdfClient,
}

impl Default for NswBudgetAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl NswBudgetAdapter {
    /// Start building an NSW budget adapter.
    #[must_use]
    pub fn builder() -> NswBudgetAdapterBuilder {
        NswBudgetAdapterBuilder::default()
    }

    /// Convert current curated publications into jobs for the supplied timestamp.
    #[must_use]
    pub fn current_jobs_with_started_at(
        current: &[NswBudgetPublication],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Diff current NSW publications against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[NswBudgetPublication],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        discoverable_jobs_with_source_index(
            current,
            known_revisions,
            started_at,
            trace_parent,
            DEFAULT_SOURCE_INDEX_URL,
        )
    }

    /// Static metadata for the NSW state budget dataflow.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![Dataflow {
            id: dataflow_id(),
            source_id: source_id(),
            name: "NSW state budget".into(),
            description: Some(
                "Annual New South Wales budget aggregates parsed from NSW Treasury budget PDFs."
                    .into(),
            ),
            dimensions: vec![
                DimensionId::new("jurisdiction").expect("static dimension id is valid"),
                DimensionId::new("budget_year").expect("static dimension id is valid"),
                DimensionId::new("paper").expect("static dimension id is valid"),
                DimensionId::new("table").expect("static dimension id is valid"),
                DimensionId::new("line_item").expect("static dimension id is valid"),
            ],
            measures: vec![MeasureId::new("value").expect("static measure id is valid")],
            frequency: Frequency::Annual,
            license: License::Other(LICENSE_NAME.into()),
            attribution: ATTRIBUTION.into(),
            source_url: DEFAULT_SOURCE_INDEX_URL.into(),
        }]
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "NSW budget fetch received job for source `{}`",
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
                "NSW budget fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        nsw_budget_provenance_for_fetch(&job.source_url, &job.metadata).ok_or_else(|| {
            AdapterError::Validation(format!(
                "NSW budget fetch URL `{}` is not a curated NSW budget PDF artifact",
                job.source_url
            ))
        })?;
        Ok(())
    }
}

#[async_trait]
impl SourceAdapter for NswBudgetAdapter {
    fn id(&self) -> &'static str {
        SOURCE_ID
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
        Ok(discoverable_jobs_with_source_index(
            &self.publications,
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
            DEFAULT_SOURCE_INDEX_URL,
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
                    .header("accept", "application/pdf"),
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
            .map_or_else(|| "application/pdf".to_string(), str::to_string);

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
        parse_artifact_stream(self.pdf_client.clone(), artifact, ctx)
    }
}

fn parse_artifact_stream(
    pdf_client: PdfClient,
    artifact: ArtifactRef,
    ctx: &ParseCtx,
) -> ObservationStream<'_> {
    let provenance = match validate_parse_artifact(&artifact, ctx) {
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

        let result =
            parse_pdf_artifact(pdf_client, artifact, provenance, started_at, row_tx.clone()).await;
        if let Err(err) = result {
            let _ = row_tx.send(Err(err)).await;
        }
    });

    Box::pin(stream::unfold(row_rx, |mut row_rx| async {
        row_rx.recv().await.map(|item| (item, row_rx))
    }))
}

async fn parse_pdf_artifact(
    pdf_client: PdfClient,
    artifact: ArtifactRef,
    provenance: NswBudgetProvenance,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    let mut request = ExtractRequest::new(artifact.storage_key.clone(), SOURCE_ID)
        .strategy(ExtractionStrategy::Deterministic);
    if let Some(artifact_date) = &provenance.artifact_date {
        request = request.artifact_date(artifact_date.clone());
    }
    let response = pdf_client
        .extract(request)
        .await
        .map_err(pdf_client_error)?;
    if response.artifact_key != artifact.storage_key {
        return Err(AdapterError::Validation(format!(
            "NSW budget sidecar returned artifact key `{}` for requested artifact `{}`",
            response.artifact_key, artifact.storage_key
        )));
    }
    if response.backend.kind != ExtractionBackendKind::Deterministic {
        return Err(AdapterError::FormatDrift(format!(
            "NSW budget sidecar used unsupported backend `{}`",
            response.backend.name
        )));
    }

    let rows = parse_table_candidates(response, &artifact, &provenance, ingested_at)?;
    for row in rows {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

fn parse_table_candidates(
    response: ExtractionResponse,
    artifact: &ArtifactRef,
    provenance: &NswBudgetProvenance,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let backend = response.backend;
    let mut parsed = Vec::new();
    for (index, table) in response.tables.iter().enumerate() {
        if let Some(rows) =
            parse_nsw_budget_table(table, index, &backend, artifact, provenance, ingested_at)?
        {
            parsed.extend(rows);
        }
    }

    if parsed.is_empty() {
        return Err(AdapterError::FormatDrift(
            "NSW budget PDF sidecar returned no recognised budget tables".into(),
        ));
    }
    Ok(parsed)
}

fn parse_nsw_budget_table(
    table: &TableCandidate,
    table_index: usize,
    backend: &BackendInfo,
    artifact: &ArtifactRef,
    provenance: &NswBudgetProvenance,
    ingested_at: DateTime<Utc>,
) -> Result<Option<Vec<(SeriesDescriptor, Observation)>>, AdapterError> {
    let rows = table
        .cells
        .iter()
        .map(|row| row.iter().map(|cell| clean_cell(cell)).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let Some(periods) = find_budget_period_columns(&rows)? else {
        return Ok(None);
    };
    let table_title = table_title_for_candidate(&rows, periods.row_index, table.page, table_index);
    let schema_key = slugify_code(&table_title);
    if schema_key != NSW_KEY_AGGREGATES_SCHEMA_KEY {
        return Ok(None);
    }
    let schema_hash = schema_hash_for_candidate(&table_title, &rows[periods.row_index]);
    let versions = parser_versions();
    let parser_version = select_parser_version(&versions, artifact_date_for_version(provenance)?)?;
    let expected = ExpectedSchemaHash::new(
        source_id(),
        dataflow_id(),
        parser_version.name(),
        schema_key.clone(),
        NSW_KEY_AGGREGATES_SCHEMA_HASH,
    )?;
    validate_schema_hash(&expected, &schema_hash)?;

    let unit = unit_from_rows(&rows);
    let first_period_col = periods
        .columns
        .first()
        .map(|period| period.column)
        .expect("period finder returns at least one column");
    let mut parsed = Vec::new();

    for row in rows.iter().skip(periods.row_index + 1) {
        let Some(line_item) = label_before(row, first_period_col) else {
            continue;
        };
        let mut row_values = Vec::new();
        let mut numeric_values = 0_usize;
        let mut invalid_value = None;
        for period in &periods.columns {
            let cell = row.get(period.column).map_or("", String::as_str);
            match parse_value(cell) {
                Ok((value, status)) => {
                    if value.is_some() {
                        numeric_values += 1;
                    }
                    let status = if matches!(status, ObservationStatus::Normal) {
                        period.status
                    } else {
                        status
                    };
                    row_values.push((period, value, status));
                }
                Err(err) => invalid_value = Some(err),
            }
        }
        if numeric_values == 0 {
            continue;
        }
        if let Some(err) = invalid_value {
            return Err(err);
        }
        for (period, value, status) in row_values {
            parsed.push(build_row(BuildRow {
                provenance,
                table_title: &table_title,
                table_page: table.page,
                line_item: &line_item,
                period_label: &period.label,
                unit: &unit,
                time: period.time,
                precision: TimePrecision::Year,
                value,
                status,
                parser_version: parser_version.name(),
                schema_hash: &schema_hash,
                backend,
                artifact,
                ingested_at,
            })?);
        }
    }

    if parsed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(parsed))
    }
}

struct BuildRow<'a> {
    provenance: &'a NswBudgetProvenance,
    table_title: &'a str,
    table_page: u32,
    line_item: &'a str,
    period_label: &'a str,
    unit: &'a str,
    time: DateTime<Utc>,
    precision: TimePrecision,
    value: Option<f64>,
    status: ObservationStatus,
    parser_version: &'a str,
    schema_hash: &'a str,
    backend: &'a BackendInfo,
    artifact: &'a ArtifactRef,
    ingested_at: DateTime<Utc>,
}

fn build_row(input: BuildRow<'_>) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let dataflow_id = dataflow_id();
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("jurisdiction").expect("static dimension id is valid"),
            nsw_code_id("jurisdiction", JURISDICTION)?,
        ),
        (
            DimensionId::new("budget_year").expect("static dimension id is valid"),
            nsw_code_id("budget_year", &input.provenance.budget_year)?,
        ),
        (
            DimensionId::new("paper").expect("static dimension id is valid"),
            nsw_code_id("paper", &input.provenance.paper_slug)?,
        ),
        (
            DimensionId::new("table").expect("static dimension id is valid"),
            nsw_code_id("table", &slugify_code(input.table_title))?,
        ),
        (
            DimensionId::new("line_item").expect("static dimension id is valid"),
            nsw_code_id("line_item", &slugify_code(input.line_item))?,
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
        unit: input.unit.to_string(),
    };
    let mut attributes = BTreeMap::from([
        ("source".into(), SOURCE_NAME.into()),
        ("source_url".into(), input.artifact.source_url.clone()),
        ("license".into(), LICENSE_NAME.into()),
        ("license_url".into(), LICENSE_URL.into()),
        ("attribution".into(), ATTRIBUTION.into()),
        ("jurisdiction".into(), JURISDICTION.into()),
        ("jurisdiction_name".into(), JURISDICTION_NAME.into()),
        ("budget_year".into(), input.provenance.budget_year.clone()),
        ("paper".into(), input.provenance.paper.clone()),
        ("paper_slug".into(), input.provenance.paper_slug.clone()),
        ("publication_title".into(), input.provenance.title.clone()),
        ("table_title".into(), input.table_title.to_string()),
        ("table_page".into(), input.table_page.to_string()),
        ("parser_version".into(), input.parser_version.to_string()),
        ("schema_hash".into(), input.schema_hash.to_string()),
        ("nsw_line_item".into(), input.line_item.to_string()),
        ("nsw_period_label".into(), input.period_label.to_string()),
        ("extraction_backend".into(), input.backend.name.clone()),
        (
            "extraction_backend_version".into(),
            input.backend.version.clone(),
        ),
    ]);
    if let Some(artifact_date) = &input.provenance.artifact_date {
        attributes.insert("artifact_date".into(), artifact_date.clone());
    }
    if let Some(model_sha256) = &input.backend.model_sha256 {
        attributes.insert("extraction_model_sha256".into(), model_sha256.clone());
    }
    let observation = Observation {
        series_key,
        time: input.time,
        time_precision: input.precision,
        value: input.value,
        status: input.status,
        revision_no: 0,
        attributes,
        ingested_at: input.ingested_at,
        source_artifact_id: input.artifact.id,
    };
    Ok((descriptor, observation))
}

fn validate_parse_artifact(
    artifact: &ArtifactRef,
    ctx: &ParseCtx,
) -> Result<NswBudgetProvenance, AdapterError> {
    if artifact.source_id.as_str() != SOURCE_ID {
        return Err(AdapterError::Validation(format!(
            "NSW budget parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    if let Some(expected) = ctx.expected_dataflow_id() {
        let actual = dataflow_id();
        if expected != &actual {
            return Err(AdapterError::Validation(format!(
                "NSW budget parse expected dataflow `{}` but adapter emits `{}`",
                expected.as_str(),
                actual.as_str()
            )));
        }
    }
    nsw_budget_provenance_for_parse(&artifact.source_url, ctx.metadata()).ok_or_else(|| {
        AdapterError::Validation(format!(
            "NSW budget parse artifact `{}` is missing curated NSW budget provenance",
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
            "NSW budget parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )));
    }

    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "NSW budget parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )))
    }
}

#[derive(Debug, Clone)]
struct NswBudgetProvenance {
    budget_year: String,
    paper: String,
    paper_slug: String,
    title: String,
    artifact_date: Option<String>,
}

fn nsw_budget_provenance_for_fetch(
    source_url: &str,
    metadata: &BTreeMap<String, String>,
) -> Option<NswBudgetProvenance> {
    nsw_budget_provenance(source_url, metadata, false)
}

fn nsw_budget_provenance_for_parse(
    source_url: &str,
    metadata: &BTreeMap<String, String>,
) -> Option<NswBudgetProvenance> {
    nsw_budget_provenance(source_url, metadata, true)
}

fn nsw_budget_provenance(
    source_url: &str,
    metadata: &BTreeMap<String, String>,
    require_nsw_host: bool,
) -> Option<NswBudgetProvenance> {
    let (_, after_scheme) = source_url.split_once("://")?;
    let (host, path_with_suffix) = after_scheme.split_once('/')?;
    if require_nsw_host
        && !matches!(
            host,
            "budget.nsw.gov.au" | "www.budget.nsw.gov.au" | "www.nsw.gov.au"
        )
    {
        return None;
    }
    let path = path_with_suffix
        .split('?')
        .next()
        .unwrap_or(path_with_suffix)
        .split('#')
        .next()
        .unwrap_or(path_with_suffix);
    if !path.ends_with(".pdf") {
        return None;
    }
    if metadata.get("jurisdiction").map(String::as_str) != Some(JURISDICTION) {
        return None;
    }
    if metadata.get("paper_slug").map(String::as_str) != Some(PAPER_SLUG) {
        return None;
    }
    let budget_year = metadata
        .get("budget_year")
        .filter(|value| !value.trim().is_empty())?
        .clone();
    let artifact_date = metadata
        .get("artifact_date")
        .filter(|value| !value.trim().is_empty())
        .cloned();
    Some(NswBudgetProvenance {
        budget_year,
        paper: metadata
            .get("paper")
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| PAPER.into()),
        paper_slug: PAPER_SLUG.into(),
        title: metadata
            .get("title")
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| TARGET_TITLE.into()),
        artifact_date,
    })
}

fn discoverable_jobs_with_source_index(
    current: &[NswBudgetPublication],
    known_revisions: &BTreeMap<String, UpstreamRevision>,
    started_at: DateTime<Utc>,
    trace_parent: Option<&str>,
    source_index_url: &str,
) -> Vec<DiscoveredJob> {
    let mut current = current.to_vec();
    current.sort_by(|left, right| {
        left.budget_year
            .cmp(&right.budget_year)
            .then(left.paper_slug.cmp(&right.paper_slug))
            .then(left.source_url.cmp(&right.source_url))
    });
    current
        .iter()
        .filter_map(|publication| {
            let revision = publication.revision(started_at);
            known_revisions
                .get(&publication.revision_key())
                .is_none_or(|known| known != &revision)
                .then(|| publication.to_discovered_job(started_at, trace_parent, source_index_url))
        })
        .collect()
}

fn find_budget_period_columns(rows: &[Vec<String>]) -> Result<Option<BudgetPeriods>, AdapterError> {
    for (row_index, row) in rows.iter().enumerate() {
        let columns = row
            .iter()
            .enumerate()
            .filter_map(|(column, cell)| {
                parse_budget_period(cell)
                    .transpose()
                    .map(|result| result.map(|period| (column, period)))
            })
            .map(|result| {
                result.map(|(column, mut period)| {
                    period.column = column;
                    period
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if columns.len() >= 2 {
            return Ok(Some(BudgetPeriods { row_index, columns }));
        }
    }
    Ok(None)
}

#[derive(Debug, Clone)]
struct BudgetPeriods {
    row_index: usize,
    columns: Vec<BudgetPeriod>,
}

#[derive(Debug, Clone)]
struct BudgetPeriod {
    column: usize,
    label: String,
    time: DateTime<Utc>,
    status: ObservationStatus,
}

fn parse_budget_period(value: &str) -> Result<Option<BudgetPeriod>, AdapterError> {
    let Some(label) = find_fiscal_year(value) else {
        return Ok(None);
    };
    let Some((start_year, _)) = label.split_once('-') else {
        return Ok(None);
    };
    let year = start_year.parse::<i32>().map_err(|_| {
        AdapterError::FormatDrift(format!("invalid NSW budget fiscal period `{value}`"))
    })?;
    let date = NaiveDate::from_ymd_opt(year, 7, 1).ok_or_else(|| {
        AdapterError::FormatDrift(format!("invalid NSW budget fiscal period `{value}`"))
    })?;
    let normalized = normalize_header(value);
    let status = if normalized.contains("revised") || normalized.contains("estimated actual") {
        ObservationStatus::Estimated
    } else if normalized.contains("estimate")
        || normalized.contains("budget")
        || normalized.contains("forward")
    {
        ObservationStatus::Forecast
    } else {
        ObservationStatus::Normal
    };
    Ok(Some(BudgetPeriod {
        column: 0,
        label,
        time: utc_midnight(date),
        status,
    }))
}

fn find_fiscal_year(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    for index in 0..bytes.len().saturating_sub(6) {
        if !(bytes[index].is_ascii_digit()
            && bytes.get(index + 1).is_some_and(u8::is_ascii_digit)
            && bytes.get(index + 2).is_some_and(u8::is_ascii_digit)
            && bytes.get(index + 3).is_some_and(u8::is_ascii_digit))
        {
            continue;
        }
        let separator = *bytes.get(index + 4)?;
        if separator != b'-' && separator != b'_' {
            continue;
        }
        if bytes.get(index + 5).is_some_and(u8::is_ascii_digit)
            && bytes.get(index + 6).is_some_and(u8::is_ascii_digit)
        {
            return Some(format!(
                "{}-{}",
                &value[index..index + 4],
                &value[index + 5..index + 7]
            ));
        }
    }
    None
}

fn label_before(row: &[String], first_period_col: usize) -> Option<String> {
    let label = row
        .iter()
        .take(first_period_col)
        .map(|cell| cell.trim())
        .filter(|cell| !cell.is_empty())
        .collect::<Vec<_>>()
        .join(" / ");
    if label.is_empty() || normalize_header(&label) == "fiscal aggregate" {
        None
    } else {
        Some(label)
    }
}

fn table_title_for_candidate(
    rows: &[Vec<String>],
    header_index: usize,
    page: u32,
    table_index: usize,
) -> String {
    rows.iter()
        .take(header_index)
        .flat_map(|row| row.iter())
        .find_map(|cell| {
            let trimmed = cell.trim();
            (!trimmed.is_empty()).then_some(trimmed.to_string())
        })
        .unwrap_or_else(|| format!("NSW budget table page {page} #{}", table_index + 1))
}

fn schema_hash_for_candidate(table_title: &str, header_row: &[String]) -> String {
    let headers = header_row
        .iter()
        .map(|cell| normalize_schema_header(cell))
        .collect::<Vec<_>>()
        .join("|");
    let material = format!(
        "table:{}\nheaders:{headers}\n",
        normalize_header(table_title)
    );
    ArtifactId::of_content(material.as_bytes()).to_hex()
}

fn normalize_schema_header(value: &str) -> String {
    let normalized = normalize_header(value);
    let mut out = String::with_capacity(normalized.len());
    let bytes = normalized.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if index + 6 < bytes.len()
            && bytes[index].is_ascii_digit()
            && bytes[index + 1].is_ascii_digit()
            && bytes[index + 2].is_ascii_digit()
            && bytes[index + 3].is_ascii_digit()
            && matches!(bytes[index + 4], b'-' | b'_')
            && bytes[index + 5].is_ascii_digit()
            && bytes[index + 6].is_ascii_digit()
        {
            out.push_str("{fiscal_year}");
            index += 7;
        } else {
            out.push(bytes[index] as char);
            index += 1;
        }
    }
    out
}

fn unit_from_rows(rows: &[Vec<String>]) -> String {
    for cell in rows.iter().flat_map(|row| row.iter()) {
        let lower = cell.to_ascii_lowercase();
        if lower.contains("$ million") || lower.contains("$m") || lower.contains("($m)") {
            return "$ million".into();
        }
        if lower.contains("per cent") || lower.contains("percent") || lower.contains('%') {
            return "percent".into();
        }
        if lower.contains("number") {
            return "number".into();
        }
    }
    "unknown".into()
}

fn parse_value(value: &str) -> Result<(Option<f64>, ObservationStatus), AdapterError> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || matches!(
            trimmed.to_ascii_lowercase().as_str(),
            "-" | "*" | "**" | "na" | "n/a" | "nfp"
        )
    {
        return Ok((None, ObservationStatus::Missing));
    }
    let mut normalized = trimmed.replace([',', ' ', '$'], "");
    let negative = normalized.starts_with('(') && normalized.ends_with(')');
    if negative {
        normalized = normalized
            .trim_start_matches('(')
            .trim_end_matches(')')
            .to_string();
    }
    normalized.parse::<f64>().map_or_else(
        |_| {
            Err(AdapterError::FormatDrift(format!(
                "invalid NSW budget numeric value `{value}`"
            )))
        },
        |value| {
            Ok((
                Some(if negative { -value } else { value }),
                ObservationStatus::Normal,
            ))
        },
    )
}

fn clean_cell(value: &str) -> String {
    value
        .replace(['\n', '\u{a0}'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_header(value: &str) -> String {
    clean_cell(value).trim().to_ascii_lowercase()
}

fn slugify_code(value: &str) -> String {
    let mut slug = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
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
        slug = "value".into();
    }
    if slug.len() > 128 {
        slug.truncate(128);
        slug = slug.trim_end_matches('_').to_string();
    }
    slug
}

fn nsw_code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!("invalid NSW budget {field} code `{value}`: {err}"))
    })
}

fn artifact_date_for_version(provenance: &NswBudgetProvenance) -> Result<NaiveDate, AdapterError> {
    if let Some(artifact_date) = &provenance.artifact_date {
        return NaiveDate::parse_from_str(artifact_date, "%Y-%m-%d").map_err(|err| {
            AdapterError::FormatDrift(format!(
                "invalid NSW budget artifact date `{artifact_date}`: {err}"
            ))
        });
    }
    let (start_year, _) = provenance.budget_year.split_once('-').ok_or_else(|| {
        AdapterError::FormatDrift(format!(
            "invalid NSW budget year `{}`",
            provenance.budget_year
        ))
    })?;
    let year = start_year.parse::<i32>().map_err(|err| {
        AdapterError::FormatDrift(format!(
            "invalid NSW budget year `{}`: {err}",
            provenance.budget_year
        ))
    })?;
    NaiveDate::from_ymd_opt(year, 7, 1).ok_or_else(|| {
        AdapterError::FormatDrift(format!(
            "invalid NSW budget year `{}`",
            provenance.budget_year
        ))
    })
}

fn parser_versions() -> [ParserVersion; 1] {
    [ParserVersion::new(
        "parse_v1",
        ArtifactDateRange::from(NaiveDate::from_ymd_opt(2020, 1, 1).expect("date is valid")),
    )]
}

fn utc_midnight(date: NaiveDate) -> DateTime<Utc> {
    Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight is valid"))
}

fn source_id() -> SourceId {
    SourceId::new(SOURCE_ID).expect("static source id is valid")
}

fn dataflow_id() -> DataflowId {
    DataflowId::new(DATAFLOW_ID).expect("static dataflow id is valid")
}

fn pdf_client_error(err: PdfClientError) -> AdapterError {
    match err.class() {
        ErrorClass::Validation => AdapterError::Validation(err.to_string()),
        ErrorClass::Permanent => AdapterError::FormatDrift(err.to_string()),
        ErrorClass::Transient => {
            CoreError::Io(io::Error::other(format!("NSW budget PDF sidecar: {err}"))).into()
        }
    }
}

fn cancelled_parse_error() -> AdapterError {
    CoreError::Io(io::Error::new(
        io::ErrorKind::Interrupted,
        "NSW budget parse cancelled",
    ))
    .into()
}

fn default_publications() -> Vec<NswBudgetPublication> {
    vec![NswBudgetPublication {
        budget_year: "2025-26".into(),
        paper: PAPER.into(),
        paper_slug: PAPER_SLUG.into(),
        title: TARGET_TITLE.into(),
        source_url: DEFAULT_BUDGET_PDF_URL.into(),
        last_updated: Some("2025-06-24".into()),
    }]
}

/// Builder for [`NswBudgetAdapter`].
#[derive(Debug, Clone)]
pub struct NswBudgetAdapterBuilder {
    publications: Vec<NswBudgetPublication>,
    pdf_base_url: String,
    pdf_client: Option<PdfClient>,
}

impl Default for NswBudgetAdapterBuilder {
    fn default() -> Self {
        Self {
            publications: default_publications(),
            pdf_base_url: DEFAULT_PDF_BASE_URL.into(),
            pdf_client: None,
        }
    }
}

impl NswBudgetAdapterBuilder {
    /// Override the curated NSW budget publications, usually for fixture tests.
    #[must_use]
    pub fn publications(mut self, publications: Vec<NswBudgetPublication>) -> Self {
        self.publications = publications;
        self
    }

    /// Override the PDF sidecar base URL.
    #[must_use]
    pub fn pdf_base_url(mut self, pdf_base_url: impl Into<String>) -> Self {
        self.pdf_base_url = pdf_base_url.into();
        self
    }

    /// Inject a prebuilt PDF client, usually for tests.
    #[must_use]
    pub fn pdf_client(mut self, pdf_client: PdfClient) -> Self {
        self.pdf_client = Some(pdf_client);
        self
    }

    /// Build the adapter, returning validation errors for invalid sidecar URLs.
    pub fn try_build(self) -> Result<NswBudgetAdapter, AdapterError> {
        if self.publications.is_empty() {
            return Err(AdapterError::Validation(
                "at least one NSW budget publication must be configured".into(),
            ));
        }
        let pdf_client = match self.pdf_client {
            Some(pdf_client) => pdf_client,
            None => PdfClient::new(&self.pdf_base_url).map_err(pdf_client_error)?,
        };
        let mut publications = self.publications;
        publications.sort_by(|left, right| {
            left.budget_year
                .cmp(&right.budget_year)
                .then(left.paper_slug.cmp(&right.paper_slug))
                .then(left.source_url.cmp(&right.source_url))
        });
        publications.dedup_by(|left, right| left.source_url == right.source_url);
        Ok(NswBudgetAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: "Australian state budgets".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(20, Duration::from_secs(60))
                    .expect("static NSW budget rate limit is valid"),
                dataflows: vec![dataflow_id()],
            },
            publications,
            pdf_client,
        })
    }

    /// Build the adapter.
    #[must_use]
    pub fn build(self) -> NswBudgetAdapter {
        self.try_build()
            .expect("valid static NSW budget adapter configuration")
    }
}

/// Stored revision type for NSW budget PDF links.
pub type NswBudgetRevision = UpstreamRevision;

/// One NSW budget PDF publication from the curated adapter inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NswBudgetPublication {
    /// Fiscal year of the budget publication, such as `2025-26`.
    pub budget_year: String,
    /// Human paper name.
    pub paper: String,
    /// Stable paper slug for metadata and series dimensions.
    pub paper_slug: String,
    /// Publication title.
    pub title: String,
    /// Canonical PDF artifact URL.
    pub source_url: String,
    /// Optional update marker from the curated inventory.
    pub last_updated: Option<String>,
}

impl NswBudgetPublication {
    fn revision_key(&self) -> String {
        format!("NSW:{}:{}", self.paper_slug, self.budget_year)
    }

    fn revision(&self, _started_at: DateTime<Utc>) -> UpstreamRevision {
        let version = self
            .last_updated
            .clone()
            .unwrap_or_else(|| self.source_url.clone());
        UpstreamRevision::new(version, self.last_updated.clone())
    }

    fn to_discovered_job(
        &self,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
        source_index_url: &str,
    ) -> DiscoveredJob {
        let revision = self.revision(started_at);
        let revision_version = revision.version().to_string();
        let revision_key = self.revision_key();
        let artifact_date = self
            .last_updated
            .clone()
            .unwrap_or_else(|| self.budget_year.clone());
        DiscoveredJob {
            id: format!(
                "state-budgets:nsw:{}:{}:{}",
                self.paper_slug, self.budget_year, revision_version
            ),
            source_id: source_id(),
            dataflow_id: dataflow_id(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("adapter".into(), "state-budgets".into()),
                ("artifact_date".into(), artifact_date),
                ("artifact_format".into(), "pdf".into()),
                ("attribution".into(), ATTRIBUTION.into()),
                ("budget_year".into(), self.budget_year.clone()),
                ("cadence".into(), "annual".into()),
                ("dataflow_id".into(), DATAFLOW_ID.into()),
                ("jurisdiction".into(), JURISDICTION.into()),
                ("jurisdiction_name".into(), JURISDICTION_NAME.into()),
                ("license".into(), LICENSE_NAME.into()),
                ("license_url".into(), LICENSE_URL.into()),
                ("paper".into(), self.paper.clone()),
                ("paper_slug".into(), self.paper_slug.clone()),
                ("revision_key".into(), revision_key),
                ("revision_version".into(), revision_version),
                (
                    "schema_drift_policy".into(),
                    "hash-pdf-table-candidates".into(),
                ),
                ("source_index_url".into(), source_index_url.to_string()),
                ("title".into(), self.title.clone()),
            ]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_hash_normalizes_fiscal_years_but_not_header_statuses() {
        let revised = vec![
            "Fiscal aggregate".into(),
            "2024-25 Revised".into(),
            "2025-26 Budget".into(),
            "2026-27 Forward Estimates".into(),
            "2027-28 Forward Estimates".into(),
        ];
        let previous_year = vec![
            "Fiscal aggregate".into(),
            "2023-24 Revised".into(),
            "2024-25 Budget".into(),
            "2025-26 Forward Estimates".into(),
            "2026-27 Forward Estimates".into(),
        ];
        let actual = vec![
            "Fiscal aggregate".into(),
            "2024-25 Actual".into(),
            "2025-26 Budget".into(),
            "2026-27 Forward Estimates".into(),
            "2027-28 Forward Estimates".into(),
        ];

        let hash = schema_hash_for_candidate("Table 1.1: Key fiscal aggregates ($m)", &revised);
        assert_eq!(hash, NSW_KEY_AGGREGATES_SCHEMA_HASH);
        assert_eq!(
            hash,
            schema_hash_for_candidate("Table 1.1: Key fiscal aggregates ($m)", &previous_year)
        );
        assert_ne!(
            hash,
            schema_hash_for_candidate("Table 1.1: Key fiscal aggregates ($m)", &actual)
        );
    }

    #[test]
    fn fetch_job_validation_rejects_wrong_provenance() {
        let adapter = NswBudgetAdapter::default();
        let publication = default_publications()
            .into_iter()
            .next()
            .expect("default publication");
        let job = NswBudgetAdapter::current_jobs_with_started_at(
            &[publication],
            Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
        )
        .into_iter()
        .next()
        .expect("fixture job");

        let mut wrong_source = job.clone();
        wrong_source.source_id = SourceId::new("abs").unwrap();
        assert!(adapter.validate_fetch_job(&wrong_source).is_err());

        let mut wrong_dataflow = job.clone();
        wrong_dataflow.dataflow_id = DataflowId::new("state_budgets.unsupported").unwrap();
        assert!(adapter.validate_fetch_job(&wrong_dataflow).is_err());

        let mut wrong_paper = job;
        wrong_paper
            .metadata
            .insert("paper_slug".into(), "bp2".into());
        assert!(adapter.validate_fetch_job(&wrong_paper).is_err());
    }

    #[test]
    fn builder_validation_covers_empty_publications_and_bad_sidecar_url() {
        let empty = NswBudgetAdapter::builder()
            .publications(Vec::new())
            .try_build()
            .expect_err("empty publication list should fail");
        assert!(
            empty
                .to_string()
                .contains("at least one NSW budget publication")
        );

        let bad_url = NswBudgetAdapter::builder()
            .pdf_base_url("not a valid url")
            .try_build()
            .expect_err("invalid PDF sidecar URL should fail");
        assert!(bad_url.to_string().contains("invalid url"));
    }

    #[test]
    fn provenance_requires_curated_pdf_metadata_and_official_parse_host() {
        let metadata = BTreeMap::from([
            ("artifact_date".into(), "2025-06-24".into()),
            ("budget_year".into(), "2025-26".into()),
            ("jurisdiction".into(), JURISDICTION.into()),
            ("paper_slug".into(), PAPER_SLUG.into()),
        ]);

        assert!(
            nsw_budget_provenance_for_fetch(
                "http://127.0.0.1:3000/bp1-budget-statement-nsw-budget-2025-26.pdf",
                &metadata,
            )
            .is_some()
        );
        assert!(
            nsw_budget_provenance_for_parse(
                "https://www.budget.nsw.gov.au/sites/default/files/2025-06/bp1-budget-statement-nsw-budget-2025-26.pdf",
                &metadata,
            )
            .is_some()
        );
        assert!(
            nsw_budget_provenance_for_parse(
                "https://mirror.example.invalid/bp1-budget-statement-nsw-budget-2025-26.pdf",
                &metadata,
            )
            .is_none()
        );
        assert!(
            nsw_budget_provenance_for_fetch(
                "http://127.0.0.1:3000/bp1-budget-statement-nsw-budget-2025-26.docx",
                &metadata,
            )
            .is_none()
        );

        let mut missing_jurisdiction = metadata;
        missing_jurisdiction.remove("jurisdiction");
        assert!(
            nsw_budget_provenance_for_fetch(
                "http://127.0.0.1:3000/bp1-budget-statement-nsw-budget-2025-26.pdf",
                &missing_jurisdiction,
            )
            .is_none()
        );
    }

    #[test]
    fn period_and_value_helpers_cover_budget_formats() {
        assert_eq!(
            find_fiscal_year("bp1-budget-statement-nsw-budget-2025-26.pdf"),
            Some("2025-26".into())
        );
        assert_eq!(
            parse_budget_period("2024-25 Revised")
                .unwrap()
                .unwrap()
                .status,
            ObservationStatus::Estimated
        );
        assert_eq!(
            parse_budget_period("2025-26 Budget")
                .unwrap()
                .unwrap()
                .status,
            ObservationStatus::Forecast
        );
        assert_eq!(parse_value("(1,234.5)").unwrap().0, Some(-1234.5));
        assert_eq!(parse_value("-").unwrap().1, ObservationStatus::Missing);
        assert!(parse_value("not numeric").is_err());

        assert_eq!(
            unit_from_rows(&[vec!["Share of revenue (per cent)".into()]]),
            "percent"
        );
        assert_eq!(
            unit_from_rows(&[vec!["Number of projects".into()]]),
            "number"
        );
        assert_eq!(unit_from_rows(&[vec!["Plain label".into()]]), "unknown");
        assert_eq!(
            table_title_for_candidate(&[], 0, 7, 1),
            "NSW budget table page 7 #2"
        );
    }
}
