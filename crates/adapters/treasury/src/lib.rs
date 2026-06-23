//! Treasury Budget Paper PDF adapter.

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
    Artifact, ArtifactId, CodeId, Dataflow, DataflowId, DimensionId, Frequency, License, MeasureId,
    Observation, ObservationStatus, SeriesDescriptor, SeriesKey, Source, SourceId, TimePrecision,
};
use au_kpis_error::{Classify, CoreError, ErrorClass};
use au_kpis_pdf_client::{
    BackendInfo, ExtractRequest, ExtractionBackendKind, ExtractionResponse, ExtractionStrategy,
    PdfClient, PdfClientError, TableCandidate,
};
use au_kpis_storage::{BlobStore, StorageKey};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use futures::{StreamExt, stream};

const DEFAULT_BUDGET_URL: &str = "https://budget.gov.au/content/bp4/index.htm";
const DEFAULT_PDF_BASE_URL: &str = "http://127.0.0.1:8010";
const USER_AGENT: &str = concat!("au-kpis-adapter-treasury/", env!("CARGO_PKG_VERSION"));
const DATAFLOW_ID: &str = "treasury.budget_papers";
const ATTRIBUTION: &str = "Source: Australian Government, The Treasury";
const LICENSE_NAME: &str = "CC-BY-4.0";
const LICENSE_URL: &str = "https://creativecommons.org/licenses/by/4.0/";
const SOURCE_NAME: &str = "Australian Government, The Treasury";
const PAPER: &str = "Budget Paper No. 4";
const PAPER_SLUG: &str = "bp4-agency-resourcing";
const TARGET_TITLE: &str = "Agency resourcing table";
const TARGET_FILENAME: &str = "bp4_05_agency_resourcing_tables.pdf";
const EXTRACT_FIRST_PAGE: u32 = 1;
const EXTRACT_LAST_PAGE: u32 = 85;

/// Treasury Budget Paper adapter that parses PDF tables via the sidecar.
#[derive(Debug, Clone)]
pub struct TreasuryAdapter {
    manifest: AdapterManifest,
    budget_url: String,
    pdf_client: PdfClient,
}

impl Default for TreasuryAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl TreasuryAdapter {
    /// Start building a Treasury adapter.
    #[must_use]
    pub fn builder() -> TreasuryAdapterBuilder {
        TreasuryAdapterBuilder::default()
    }

    /// Parse the Treasury Budget Paper 4 page into target PDF publications.
    pub fn parse_budget_publications_page(
        body: &str,
    ) -> Result<Vec<TreasuryBudgetPublication>, AdapterError> {
        parse_budget_publications_page_with_base(body, DEFAULT_BUDGET_URL)
    }

    /// Convert discovered publications into jobs for the supplied timestamp.
    #[must_use]
    pub fn current_jobs_with_started_at(
        current: &[TreasuryBudgetPublication],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Diff current Treasury links against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[TreasuryBudgetPublication],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        discoverable_jobs_with_budget_url(
            current,
            known_revisions,
            started_at,
            trace_parent,
            DEFAULT_BUDGET_URL,
        )
    }

    /// Static metadata for the Treasury budget papers dataflow.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![Dataflow {
            id: dataflow_id(),
            source_id: source_id(),
            name: "Treasury budget papers".into(),
            description: Some(
                "Annual Australian Government Budget Paper No. 4 agency resourcing tables parsed from PDFs."
                    .into(),
            ),
            dimensions: vec![
                DimensionId::new("budget_year").expect("static dimension id is valid"),
                DimensionId::new("paper").expect("static dimension id is valid"),
                DimensionId::new("table").expect("static dimension id is valid"),
                DimensionId::new("line_item").expect("static dimension id is valid"),
            ],
            measures: vec![MeasureId::new("value").expect("static measure id is valid")],
            frequency: Frequency::Annual,
            license: License::CcBy40,
            attribution: ATTRIBUTION.into(),
            source_url: DEFAULT_BUDGET_URL.into(),
        }]
    }

    fn budget_url(&self) -> &str {
        &self.budget_url
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "Treasury fetch received job for source `{}`",
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
                "Treasury fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        budget_publication_provenance_for_fetch(&job.source_url, &job.metadata).ok_or_else(
            || {
                AdapterError::Validation(format!(
                    "Treasury fetch URL `{}` is not a target budget PDF artifact",
                    job.source_url
                ))
            },
        )?;
        Ok(())
    }
}

#[async_trait]
impl SourceAdapter for TreasuryAdapter {
    fn id(&self) -> &'static str {
        "treasury"
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    fn source_metadata(&self) -> Option<Source> {
        Some(Source {
            id: source_id(),
            name: SOURCE_NAME.into(),
            homepage: "https://treasury.gov.au".into(),
            description: Some(
                "Australian Government Treasury budget and economic publications.".into(),
            ),
        })
    }

    fn dataflow_metadata(&self) -> Vec<Dataflow> {
        TreasuryAdapter::dataflow_metadata(self)
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        let response = ctx
            .http
            .execute(
                ctx.http
                    .raw()
                    .get(self.budget_url())
                    .header("user-agent", USER_AGENT)
                    .header("accept", "text/html,application/xhtml+xml"),
            )
            .await?
            .error_for_status()?;
        let body = response.text().await?;
        let publications = parse_budget_publications_page_with_base(&body, self.budget_url())?;
        Ok(discoverable_jobs_with_budget_url(
            &publications,
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
            self.budget_url(),
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
    provenance: TreasuryBudgetProvenance,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    let mut request = ExtractRequest::new(artifact.storage_key.clone(), "treasury")
        .strategy(ExtractionStrategy::Deterministic)
        .page_range(EXTRACT_FIRST_PAGE, EXTRACT_LAST_PAGE);
    if let Some(artifact_date) = &provenance.artifact_date {
        request = request.artifact_date(artifact_date.clone());
    }
    let response = pdf_client
        .extract(request)
        .await
        .map_err(pdf_client_error)?;
    if response.artifact_key != artifact.storage_key {
        return Err(AdapterError::Validation(format!(
            "Treasury sidecar returned artifact key `{}` for requested artifact `{}`",
            response.artifact_key, artifact.storage_key
        )));
    }
    if response.backend.kind != ExtractionBackendKind::Deterministic {
        return Err(AdapterError::FormatDrift(format!(
            "Treasury sidecar used unsupported backend `{}`",
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
    provenance: &TreasuryBudgetProvenance,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let backend = response.backend;
    let mut parsed = Vec::new();
    for (index, table) in response.tables.iter().enumerate() {
        if let Some(rows) =
            parse_budget_table(table, index, &backend, artifact, provenance, ingested_at)?
        {
            parsed.extend(rows);
        }
    }

    if parsed.is_empty() {
        return Err(AdapterError::FormatDrift(
            "Treasury PDF sidecar returned no recognised budget tables".into(),
        ));
    }
    Ok(parsed)
}

fn parse_budget_table(
    table: &TableCandidate,
    table_index: usize,
    backend: &BackendInfo,
    artifact: &ArtifactRef,
    provenance: &TreasuryBudgetProvenance,
    ingested_at: DateTime<Utc>,
) -> Result<Option<Vec<(SeriesDescriptor, Observation)>>, AdapterError> {
    let rows = table
        .cells
        .iter()
        .map(|row| row.iter().map(|cell| clean_cell(cell)).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let Some(periods) = find_budget_period_columns(&rows)? else {
        return parse_bp4_stream_table(
            table,
            table_index,
            backend,
            artifact,
            provenance,
            ingested_at,
            &rows,
        );
    };
    let unit = unit_from_rows(&rows);
    let table_title = table_title_for_candidate(&rows, periods.row_index, table.page, table_index);
    let schema_hash = schema_hash_for_candidate(table, &rows);
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

fn parse_bp4_stream_table(
    table: &TableCandidate,
    _table_index: usize,
    backend: &BackendInfo,
    artifact: &ArtifactRef,
    provenance: &TreasuryBudgetProvenance,
    ingested_at: DateTime<Utc>,
    rows: &[Vec<String>],
) -> Result<Option<Vec<(SeriesDescriptor, Observation)>>, AdapterError> {
    let Some(periods) = find_bp4_stream_periods(rows) else {
        return Ok(None);
    };
    let Some((header_row, total_column)) = find_total_column(rows) else {
        return Ok(None);
    };
    let unit = unit_from_rows(rows);
    let schema_hash = schema_hash_for_candidate(table, rows);
    let row_context = Bp4StreamRowContext {
        table,
        backend,
        artifact,
        provenance,
        ingested_at,
        unit: &unit,
        schema_hash: &schema_hash,
    };
    let mut parsed = Vec::new();
    let mut context = Vec::new();
    let mut pending_current: Option<PendingBp4Row> = None;

    for row in rows.iter().skip(header_row + 1) {
        if row.iter().any(|cell| normalize_header(cell) == "$'000") {
            continue;
        }
        let label = row
            .first()
            .map(|cell| cell.trim())
            .filter(|cell| !cell.is_empty() && !looks_like_bp4_page_chrome(cell));
        let total_cell = row.get(total_column).map_or("", String::as_str);
        let (value, status) = parse_value(total_cell)?;
        if value.is_none() {
            if let Some(label) = label {
                if let Some(pending) = pending_current.as_mut() {
                    pending.line_item.push(' ');
                    pending.line_item.push_str(label);
                } else {
                    context.push(label.to_string());
                }
            }
            continue;
        }

        if let Some(label) = label {
            if let Some(pending) = pending_current.take() {
                parsed.push(build_bp4_stream_row(
                    &row_context,
                    &periods.current,
                    &pending.line_item,
                    pending.value,
                    pending.status,
                )?);
            }
            let context_label = std::mem::take(&mut context).join(" ");
            let line_item = if context_label.is_empty() {
                label.to_string()
            } else {
                format!("{context_label} / {label}")
            };
            pending_current = Some(PendingBp4Row {
                line_item,
                value,
                status,
            });
            continue;
        }

        let Some(pending) = pending_current.take() else {
            continue;
        };
        let line_item = pending.line_item;
        parsed.push(build_bp4_stream_row(
            &row_context,
            &periods.current,
            &line_item,
            pending.value,
            pending.status,
        )?);
        parsed.push(build_bp4_stream_row(
            &row_context,
            &periods.previous,
            &line_item,
            value,
            status,
        )?);
    }

    if let Some(pending) = pending_current {
        parsed.push(build_bp4_stream_row(
            &row_context,
            &periods.current,
            &pending.line_item,
            pending.value,
            pending.status,
        )?);
    }

    if parsed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(parsed))
    }
}

struct Bp4StreamPeriods {
    current: BudgetPeriod,
    previous: BudgetPeriod,
}

struct PendingBp4Row {
    line_item: String,
    value: Option<f64>,
    status: ObservationStatus,
}

struct Bp4StreamRowContext<'a> {
    table: &'a TableCandidate,
    backend: &'a BackendInfo,
    artifact: &'a ArtifactRef,
    provenance: &'a TreasuryBudgetProvenance,
    ingested_at: DateTime<Utc>,
    unit: &'a str,
    schema_hash: &'a str,
}

fn find_bp4_stream_periods(rows: &[Vec<String>]) -> Option<Bp4StreamPeriods> {
    let mut current = None;
    let mut previous = None;
    for cell in rows.iter().flat_map(|row| row.iter()) {
        let normalized = normalize_header(cell);
        let Some(label) = find_fiscal_year(cell) else {
            continue;
        };
        let period = budget_period_from_label(&label, cell).ok()?;
        if normalized.contains("agency resourcing") {
            current = Some(BudgetPeriod {
                status: ObservationStatus::Forecast,
                ..period
            });
        } else if normalized.contains("estimated actual") {
            previous = Some(BudgetPeriod {
                status: ObservationStatus::Estimated,
                ..period
            });
        }
    }
    Some(Bp4StreamPeriods {
        current: current?,
        previous: previous?,
    })
}

fn find_total_column(rows: &[Vec<String>]) -> Option<(usize, usize)> {
    rows.iter().enumerate().find_map(|(row_index, row)| {
        row.iter().enumerate().find_map(|(column, cell)| {
            (normalize_header(cell) == "total"
                && rows.iter().skip(row_index + 1).take(2).any(|candidate| {
                    candidate
                        .get(column)
                        .is_some_and(|unit| normalize_header(unit) == "$'000")
                }))
            .then_some((row_index, column))
        })
    })
}

fn build_bp4_stream_row(
    context: &Bp4StreamRowContext<'_>,
    period: &BudgetPeriod,
    line_item: &str,
    value: Option<f64>,
    status: ObservationStatus,
) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let status = if matches!(status, ObservationStatus::Normal) {
        period.status
    } else {
        status
    };
    build_row(BuildRow {
        provenance: context.provenance,
        table_title: TARGET_TITLE,
        table_page: context.table.page,
        line_item,
        period_label: &period.label,
        unit: context.unit,
        time: period.time,
        precision: TimePrecision::Year,
        value,
        status,
        schema_hash: context.schema_hash,
        backend: context.backend,
        artifact: context.artifact,
        ingested_at: context.ingested_at,
    })
}

struct BuildRow<'a> {
    provenance: &'a TreasuryBudgetProvenance,
    table_title: &'a str,
    table_page: u32,
    line_item: &'a str,
    period_label: &'a str,
    unit: &'a str,
    time: DateTime<Utc>,
    precision: TimePrecision,
    value: Option<f64>,
    status: ObservationStatus,
    schema_hash: &'a str,
    backend: &'a BackendInfo,
    artifact: &'a ArtifactRef,
    ingested_at: DateTime<Utc>,
}

fn build_row(input: BuildRow<'_>) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let dataflow_id = dataflow_id();
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("budget_year").expect("static dimension id is valid"),
            treasury_code_id("budget_year", &input.provenance.budget_year)?,
        ),
        (
            DimensionId::new("paper").expect("static dimension id is valid"),
            treasury_code_id("paper", &input.provenance.paper_slug)?,
        ),
        (
            DimensionId::new("table").expect("static dimension id is valid"),
            treasury_code_id("table", &slugify_code(input.table_title))?,
        ),
        (
            DimensionId::new("line_item").expect("static dimension id is valid"),
            treasury_code_id("line_item", &slugify_code(input.line_item))?,
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
        unit: input.unit.to_string(),
    };
    let mut attributes = BTreeMap::from([
        ("source".into(), SOURCE_NAME.into()),
        ("source_url".into(), input.artifact.source_url.clone()),
        ("license".into(), LICENSE_NAME.into()),
        ("license_url".into(), LICENSE_URL.into()),
        ("attribution".into(), ATTRIBUTION.into()),
        ("budget_year".into(), input.provenance.budget_year.clone()),
        ("paper".into(), input.provenance.paper.clone()),
        ("paper_slug".into(), input.provenance.paper_slug.clone()),
        ("publication_title".into(), input.provenance.title.clone()),
        ("table_title".into(), input.table_title.to_string()),
        ("table_page".into(), input.table_page.to_string()),
        ("schema_hash".into(), input.schema_hash.to_string()),
        ("treasury_line_item".into(), input.line_item.to_string()),
        (
            "treasury_period_label".into(),
            input.period_label.to_string(),
        ),
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
) -> Result<TreasuryBudgetProvenance, AdapterError> {
    if artifact.source_id.as_str() != "treasury" {
        return Err(AdapterError::Validation(format!(
            "Treasury parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    if let Some(expected) = ctx.expected_dataflow_id() {
        let actual = dataflow_id();
        if expected != &actual {
            return Err(AdapterError::Validation(format!(
                "Treasury parse expected dataflow `{}` but adapter emits `{}`",
                expected.as_str(),
                actual.as_str()
            )));
        }
    }
    budget_publication_provenance_for_parse(&artifact.source_url, ctx.metadata()).ok_or_else(|| {
        AdapterError::Validation(format!(
            "Treasury parse artifact `{}` is missing budget-paper provenance",
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
            "Treasury parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )));
    }

    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "Treasury parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )))
    }
}

fn parse_budget_publications_page_with_base(
    body: &str,
    base_url: &str,
) -> Result<Vec<TreasuryBudgetPublication>, AdapterError> {
    let page_budget_year = find_fiscal_year(&clean_html_text(body).unwrap_or_default());
    let mut publications = Vec::new();
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
        let metadata = BTreeMap::from([
            (
                "budget_year".to_string(),
                page_budget_year.clone().unwrap_or_default(),
            ),
            (
                "artifact_date".to_string(),
                attr_value(attrs, "data-updated").unwrap_or_default(),
            ),
        ]);
        let Some(provenance) = budget_publication_provenance_for_fetch(&source_url, &metadata)
        else {
            continue;
        };
        let anchor_text = clean_html_text(text);
        let title = anchor_text
            .filter(|value| !looks_like_download_label(value))
            .unwrap_or_else(|| provenance.title.clone());
        publications.push(TreasuryBudgetPublication {
            budget_year: provenance.budget_year,
            paper: provenance.paper,
            paper_slug: provenance.paper_slug,
            title,
            source_url,
            last_updated: attr_value(attrs, "data-updated")
                .or_else(|| attr_value(attrs, "datetime")),
        });
    }
    publications.sort_by(|left, right| {
        left.budget_year
            .cmp(&right.budget_year)
            .then(left.paper_slug.cmp(&right.paper_slug))
            .then(left.source_url.cmp(&right.source_url))
    });
    publications.dedup_by(|left, right| left.source_url == right.source_url);
    Ok(publications)
}

fn attr_value(attrs: &str, name: &str) -> Option<String> {
    let bytes = attrs.as_bytes();
    for (name_start, _) in attrs.match_indices(name) {
        if name_start > 0 && is_attr_name_char(bytes[name_start - 1]) {
            continue;
        }
        let mut cursor = name_start + name.len();
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
        if bytes.get(cursor) != Some(&b'=') {
            continue;
        }
        cursor += 1;
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
        let quote = attrs[cursor..].chars().next()?;
        if quote != '"' && quote != '\'' {
            return None;
        }
        let value_start = cursor + quote.len_utf8();
        let value_end = attrs[value_start..].find(quote)? + value_start;
        return Some(attrs[value_start..value_end].to_string());
    }
    None
}

fn is_attr_name_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':')
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
    let cleaned = decode_url_component(&out)
        .replace("&amp;", "&")
        .replace("&nbsp;", " ")
        .replace("&ndash;", "-")
        .replace("&#8211;", "-")
        .replace('\u{a0}', " ")
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
            AdapterError::Validation(format!("Treasury budget URL `{base_url}` is not absolute"))
        })?;
        let path_start = base_url[scheme_end + 3..]
            .find('/')
            .map_or(base_url.len(), |index| scheme_end + 3 + index);
        return Ok(format!("{}{}", &base_url[..path_start], href));
    }
    let Some((prefix, _)) = base_url.rsplit_once('/') else {
        return Err(AdapterError::Validation(format!(
            "Treasury budget URL `{base_url}` has no path separator"
        )));
    };
    Ok(format!("{prefix}/{href}"))
}

#[derive(Debug, Clone)]
struct TreasuryBudgetProvenance {
    budget_year: String,
    paper: String,
    paper_slug: String,
    title: String,
    artifact_date: Option<String>,
}

fn budget_publication_provenance_for_fetch(
    source_url: &str,
    metadata: &BTreeMap<String, String>,
) -> Option<TreasuryBudgetProvenance> {
    budget_publication_provenance(source_url, metadata, false)
}

fn budget_publication_provenance_for_parse(
    source_url: &str,
    metadata: &BTreeMap<String, String>,
) -> Option<TreasuryBudgetProvenance> {
    budget_publication_provenance(source_url, metadata, true)
}

fn budget_publication_provenance(
    source_url: &str,
    metadata: &BTreeMap<String, String>,
    require_treasury_host: bool,
) -> Option<TreasuryBudgetProvenance> {
    let (_, after_scheme) = source_url.split_once("://")?;
    let (host, path_with_suffix) = after_scheme.split_once('/')?;
    if require_treasury_host
        && !matches!(
            host,
            "budget.gov.au" | "www.budget.gov.au" | "archive.budget.gov.au"
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
    if !path.ends_with(TARGET_FILENAME) {
        return None;
    }
    if !path.contains("bp4/download/") && !path.contains("content/bp4/download/") {
        return None;
    }
    let budget_year = find_fiscal_year(source_url)
        .or_else(|| {
            metadata
                .get("budget_year")
                .filter(|value| !value.is_empty())
                .cloned()
        })
        .unwrap_or_else(|| "unknown".into());
    let artifact_date = metadata
        .get("artifact_date")
        .filter(|value| !value.is_empty())
        .cloned();
    Some(TreasuryBudgetProvenance {
        budget_year,
        paper: PAPER.into(),
        paper_slug: PAPER_SLUG.into(),
        title: TARGET_TITLE.into(),
        artifact_date,
    })
}

fn discoverable_jobs_with_budget_url(
    current: &[TreasuryBudgetPublication],
    known_revisions: &BTreeMap<String, UpstreamRevision>,
    started_at: DateTime<Utc>,
    trace_parent: Option<&str>,
    budget_url: &str,
) -> Vec<DiscoveredJob> {
    current
        .iter()
        .filter_map(|publication| {
            let revision = publication.revision(started_at);
            known_revisions
                .get(&publication.revision_key())
                .is_none_or(|known| known != &revision)
                .then(|| publication.to_discovered_job(started_at, trace_parent, budget_url))
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
    budget_period_from_label(&label, value).map(Some)
}

fn budget_period_from_label(label: &str, value: &str) -> Result<BudgetPeriod, AdapterError> {
    let Some((start_year, _)) = label.split_once('-') else {
        return Err(AdapterError::FormatDrift(format!(
            "invalid Treasury fiscal period `{value}`"
        )));
    };
    let year = start_year.parse::<i32>().map_err(|_| {
        AdapterError::FormatDrift(format!("invalid Treasury fiscal period `{value}`"))
    })?;
    let date = NaiveDate::from_ymd_opt(year, 7, 1).ok_or_else(|| {
        AdapterError::FormatDrift(format!("invalid Treasury fiscal period `{value}`"))
    })?;
    let normalized = normalize_header(value);
    let status = if normalized.contains("estimated actual") {
        ObservationStatus::Estimated
    } else if normalized.contains("estimate") || normalized.contains("budget") {
        ObservationStatus::Forecast
    } else {
        ObservationStatus::Normal
    };
    Ok(BudgetPeriod {
        column: 0,
        label: label.to_string(),
        time: utc_midnight(date),
        status,
    })
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
            && bytes.get(index + 7).is_some_and(u8::is_ascii_digit)
            && bytes.get(index + 8).is_some_and(u8::is_ascii_digit)
        {
            return Some(format!(
                "{}-{}",
                &value[index..index + 4],
                &value[index + 7..index + 9]
            ));
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
    if label.is_empty() || normalize_header(&label) == "agency" {
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
        .unwrap_or_else(|| format!("{PAPER} table page {page} #{}", table_index + 1))
}

fn schema_hash_for_candidate(table: &TableCandidate, rows: &[Vec<String>]) -> String {
    let mut material = format!("page:{}\nbbox:{:?}\n", table.page, table.bbox);
    for row in rows
        .iter()
        .filter(|row| row.iter().any(|cell| !cell.trim().is_empty()))
        .take(12)
    {
        material.push_str(
            &row.iter()
                .map(|cell| cell.trim())
                .collect::<Vec<_>>()
                .join("\t"),
        );
        material.push('\n');
    }
    ArtifactId::of_content(material.as_bytes()).to_hex()
}

fn unit_from_rows(rows: &[Vec<String>]) -> String {
    for cell in rows.iter().flat_map(|row| row.iter()) {
        let lower = cell.to_ascii_lowercase();
        if lower.contains("$'000") || lower.contains("$000") {
            return "$ thousand".into();
        }
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
    normalized
        .parse::<f64>()
        .map(|value| {
            (
                Some(if negative { -value } else { value }),
                ObservationStatus::Normal,
            )
        })
        .map_err(|_| AdapterError::FormatDrift(format!("invalid Treasury numeric value `{value}`")))
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

fn looks_like_bp4_page_chrome(value: &str) -> bool {
    let normalized = normalize_header(value);
    normalized.contains("budget paper no. 4") || normalized.starts_with("page ")
}

fn decode_url_component(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = String::with_capacity(value.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hex = &value[index + 1..index + 3];
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                out.push(byte as char);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index] as char);
        index += 1;
    }
    out
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

fn treasury_code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!("invalid Treasury {field} code `{value}`: {err}"))
    })
}

fn utc_midnight(date: NaiveDate) -> DateTime<Utc> {
    Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight is valid"))
}

fn looks_like_download_label(value: &str) -> bool {
    let normalized = normalize_header(value);
    normalized.starts_with("pdf ") || normalized == "pdf" || normalized.ends_with(" mb")
}

fn source_id() -> SourceId {
    SourceId::new("treasury").expect("static source id is valid")
}

fn dataflow_id() -> DataflowId {
    DataflowId::new(DATAFLOW_ID).expect("static dataflow id is valid")
}

fn pdf_client_error(err: PdfClientError) -> AdapterError {
    match err.class() {
        ErrorClass::Validation => AdapterError::Validation(err.to_string()),
        ErrorClass::Permanent => AdapterError::FormatDrift(err.to_string()),
        ErrorClass::Transient => {
            CoreError::Io(io::Error::other(format!("Treasury PDF sidecar: {err}"))).into()
        }
    }
}

fn cancelled_parse_error() -> AdapterError {
    CoreError::Io(io::Error::new(
        io::ErrorKind::Interrupted,
        "Treasury parse cancelled",
    ))
    .into()
}

/// Builder for [`TreasuryAdapter`].
#[derive(Debug, Clone)]
pub struct TreasuryAdapterBuilder {
    budget_url: String,
    pdf_base_url: String,
    pdf_client: Option<PdfClient>,
}

impl Default for TreasuryAdapterBuilder {
    fn default() -> Self {
        Self {
            budget_url: DEFAULT_BUDGET_URL.into(),
            pdf_base_url: DEFAULT_PDF_BASE_URL.into(),
            pdf_client: None,
        }
    }
}

impl TreasuryAdapterBuilder {
    /// Override the Budget Paper 4 publications URL, usually for fixture tests.
    #[must_use]
    pub fn budget_url(mut self, budget_url: impl Into<String>) -> Self {
        self.budget_url = budget_url.into();
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
    pub fn try_build(self) -> Result<TreasuryAdapter, AdapterError> {
        let pdf_client = match self.pdf_client {
            Some(pdf_client) => pdf_client,
            None => PdfClient::new(&self.pdf_base_url).map_err(pdf_client_error)?,
        };
        Ok(TreasuryAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: "Australian Government Treasury".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(30, Duration::from_secs(60))
                    .expect("static Treasury rate limit is valid"),
                dataflows: vec![dataflow_id()],
            },
            budget_url: self.budget_url,
            pdf_client,
        })
    }

    /// Build the adapter.
    #[must_use]
    pub fn build(self) -> TreasuryAdapter {
        self.try_build()
            .expect("valid static Treasury adapter configuration")
    }
}

/// Stored revision type for Treasury Budget Paper PDF links.
pub type TreasuryBudgetRevision = UpstreamRevision;

/// One Treasury Budget Paper PDF publication discovered from the budget page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreasuryBudgetPublication {
    /// Fiscal year of the Budget publication, such as `2026-27`.
    pub budget_year: String,
    /// Human paper name.
    pub paper: String,
    /// Stable paper slug for metadata and series dimensions.
    pub paper_slug: String,
    /// Link text or target table title.
    pub title: String,
    /// Canonical artifact URL.
    pub source_url: String,
    /// Optional update marker scraped from the budget page.
    pub last_updated: Option<String>,
}

impl TreasuryBudgetPublication {
    fn revision_key(&self) -> String {
        format!("TREASURY:{}:{}", self.paper_slug, self.budget_year)
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
        budget_url: &str,
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
                "treasury:{}:{}:{}",
                self.paper_slug, self.budget_year, revision_version
            ),
            source_id: source_id(),
            dataflow_id: dataflow_id(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("adapter".into(), "treasury".into()),
                ("artifact_date".into(), artifact_date),
                ("artifact_format".into(), "pdf".into()),
                ("attribution".into(), ATTRIBUTION.into()),
                ("budget_year".into(), self.budget_year.clone()),
                ("cadence".into(), "annual".into()),
                ("dataflow_id".into(), DATAFLOW_ID.into()),
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
                ("source_index_url".into(), budget_url.to_string()),
                ("title".into(), self.title.clone()),
            ]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn fiscal_year_finder_handles_dash_and_underscore_forms() {
        assert_eq!(find_fiscal_year("Budget 2026-27"), Some("2026-27".into()));
        assert_eq!(find_fiscal_year("bp4_2026_27.pdf"), Some("2026-27".into()));
    }

    #[test]
    fn parse_budget_period_marks_estimates() {
        assert_eq!(
            parse_budget_period("2025-26 Estimated actual")
                .unwrap()
                .unwrap()
                .status,
            ObservationStatus::Estimated
        );
        assert_eq!(
            parse_budget_period("2026-27 Forward estimate")
                .unwrap()
                .unwrap()
                .status,
            ObservationStatus::Forecast
        );
    }

    #[test]
    fn duplicate_budget_periods_are_not_enough_for_a_table() {
        let rows = vec![vec!["Agency".into(), "2026-27 Budget".into()]];
        assert!(find_budget_period_columns(&rows).unwrap().is_none());
    }

    #[test]
    fn candidate_schema_hash_is_stable() {
        let table = TableCandidate {
            page: 1,
            bbox: [1.0, 2.0, 3.0, 4.0],
            cells: vec![vec!["Agency".into(), "2026-27 Budget".into()]],
            spans: vec![],
            diagnostics: BTreeMap::new(),
        };
        let rows = table.cells.clone();
        assert_eq!(
            schema_hash_for_candidate(&table, &rows),
            schema_hash_for_candidate(&table, &rows)
        );
    }

    #[test]
    fn no_duplicate_dimension_ids() {
        let dataflows = TreasuryAdapter::default().dataflow_metadata();
        let ids = dataflows[0]
            .dimensions
            .iter()
            .map(DimensionId::as_str)
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), 4);
    }

    #[test]
    fn fetch_job_validation_rejects_wrong_provenance() {
        let adapter = TreasuryAdapter::default();
        let mut job = fixture_job();

        job.source_id = SourceId::new("abs").unwrap();
        assert!(adapter.validate_fetch_job(&job).is_err());

        let mut job = fixture_job();
        job.dataflow_id = DataflowId::new("treasury.unsupported").unwrap();
        assert!(adapter.validate_fetch_job(&job).is_err());

        let mut job = fixture_job();
        job.source_url = "https://budget.gov.au/content/bp4/download/not_the_target.pdf".into();
        assert!(adapter.validate_fetch_job(&job).is_err());
    }

    #[test]
    fn html_url_and_period_helpers_cover_edge_cases() {
        assert_eq!(
            attr_value("<a href='one.pdf'>", "href"),
            Some("one.pdf".into())
        );
        assert_eq!(attr_value("<a href=one.pdf>", "href"), None);
        assert_eq!(attr_value("<a data-href=\"one.pdf\">", "href"), None);

        assert_eq!(
            resolve_url(DEFAULT_BUDGET_URL, "download/table.pdf").unwrap(),
            "https://budget.gov.au/content/bp4/download/table.pdf"
        );
        assert_eq!(
            resolve_url(DEFAULT_BUDGET_URL, "/content/bp4/download/table.pdf").unwrap(),
            "https://budget.gov.au/content/bp4/download/table.pdf"
        );
        assert_eq!(
            resolve_url(
                DEFAULT_BUDGET_URL,
                "https://archive.budget.gov.au/table.pdf"
            )
            .unwrap(),
            "https://archive.budget.gov.au/table.pdf"
        );
        assert!(resolve_url("budget.gov.au/content/bp4/index.htm", "/table.pdf").is_err());
        assert!(resolve_url("budget.gov.au", "table.pdf").is_err());

        assert_eq!(decode_url_component("Budget%20Paper%ZZ"), "Budget Paper%ZZ");
        assert!(looks_like_download_label("PDF 2.4 MB"));
        assert!(looks_like_download_label("PDF"));
        assert!(!looks_like_download_label("Agency resourcing table"));

        assert_eq!(
            parse_budget_period("2024-25 Actual")
                .unwrap()
                .unwrap()
                .status,
            ObservationStatus::Normal
        );
        assert!(parse_budget_period("not a period").unwrap().is_none());
        assert_eq!(find_fiscal_year("Budget 2026/27"), None);
        assert_eq!(find_fiscal_year("Budget 2026-A7"), None);
    }

    #[test]
    fn provenance_rejects_ambiguous_sources_and_uses_metadata_fallback() {
        let metadata = BTreeMap::from([
            ("budget_year".into(), "2026-27".into()),
            ("artifact_date".into(), "2026-05-12".into()),
        ]);

        assert!(
            budget_publication_provenance(
                "https://example.com/content/bp4/download/bp4_05_agency_resourcing_tables.pdf",
                &metadata,
                true,
            )
            .is_none()
        );
        assert!(
            budget_publication_provenance(
                "https://budget.gov.au/content/bp4/download/other.pdf",
                &metadata,
                true,
            )
            .is_none()
        );
        assert!(
            budget_publication_provenance(
                "https://budget.gov.au/content/other/download/bp4_05_agency_resourcing_tables.pdf",
                &metadata,
                true,
            )
            .is_none()
        );

        let provenance = budget_publication_provenance(
            "https://budget.gov.au/content/bp4/download/bp4_05_agency_resourcing_tables.pdf",
            &metadata,
            true,
        )
        .expect("target Treasury publication");
        assert_eq!(provenance.budget_year, "2026-27");
        assert_eq!(provenance.artifact_date.as_deref(), Some("2026-05-12"));

        let provenance = budget_publication_provenance(
            "https://mirror.example/content/bp4/download/bp4_05_agency_resourcing_tables.pdf",
            &BTreeMap::new(),
            false,
        )
        .expect("host check disabled for recorded fetch metadata");
        assert_eq!(provenance.budget_year, "unknown");
    }

    #[test]
    fn table_helpers_cover_units_values_labels_and_slugs() {
        assert_eq!(unit_from_rows(&[vec!["Budget ($m)".into()]]), "$ million");
        assert_eq!(unit_from_rows(&[vec!["Growth per cent".into()]]), "percent");
        assert_eq!(unit_from_rows(&[vec!["Employee number".into()]]), "number");
        assert_eq!(unit_from_rows(&[vec!["No unit marker".into()]]), "unknown");

        assert_eq!(parse_value("").unwrap(), (None, ObservationStatus::Missing));
        assert_eq!(
            parse_value("n/a").unwrap(),
            (None, ObservationStatus::Missing)
        );
        assert_eq!(
            parse_value("(1,234.5)").unwrap(),
            (Some(-1234.5), ObservationStatus::Normal)
        );
        assert!(parse_value("not numeric").is_err());

        assert_eq!(label_before(&["Agency".into()], 1), None);
        assert_eq!(label_before(&["".into(), " ".into()], 2), None);
        assert_eq!(
            label_before(
                &["Department".into(), "Program".into(), "2026-27".into()],
                2
            ),
            Some("Department / Program".into())
        );

        assert_eq!(
            table_title_for_candidate(&[vec!["".into()], vec!["".into()]], 2, 7, 3),
            "Budget Paper No. 4 table page 7 #4"
        );
        assert_eq!(
            table_title_for_candidate(&[vec!["Table 1".into()]], 1, 7, 0),
            "Table 1"
        );
        assert_eq!(
            find_total_column(&[
                vec!["Total".into(), "1".into()],
                vec!["".into(), "2".into()],
                vec!["Label".into(), "Total".into()],
                vec!["".into(), "$'000".into()],
            ]),
            Some((2, 1))
        );

        assert_eq!(slugify_code("!!!"), "value");
        let long_slug = slugify_code(&"A".repeat(140));
        assert_eq!(long_slug.len(), 128);
        assert!(long_slug.chars().all(|ch| ch == 'a'));
    }

    fn fixture_job() -> DiscoveredJob {
        let publication = TreasuryAdapter::parse_budget_publications_page(
            r#"<title>Budget Paper No. 4 | Budget 2026-27</title>
            <a href="https://budget.gov.au/content/bp4/download/bp4_05_agency_resourcing_tables.pdf">
            Agency resourcing table</a>"#,
        )
        .expect("fixture publication")
        .into_iter()
        .next()
        .expect("fixture job");
        TreasuryAdapter::current_jobs_with_started_at(
            &[publication],
            Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
        )
        .into_iter()
        .next()
        .expect("job emitted")
    }
}
