//! State capital project-performance adapter.

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
use au_kpis_error::CoreError;
use au_kpis_storage::{BlobStore, StorageKey};
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use futures::{StreamExt, stream};
use serde::Deserialize;

const DEFAULT_INDEX_URL: &str =
    "https://www.audit.vic.gov.au/report/major-projects-performance-reporting-2025";
const USER_AGENT: &str = concat!("au-kpis-adapter-state-capital/", env!("CARGO_PKG_VERSION"));
const SOURCE_ID: &str = "state_capital";
const VIC_MAJOR_PROJECTS_DATAFLOW_ID: &str = "state_capital.vic_major_projects";
const BUDGET_CAPITAL_PAPERS_DATAFLOW_ID: &str = "state_capital.budget_capital_papers";
const ATTRIBUTION: &str = "Pilot state capital sources: VAGO and Victorian Budget";
const LICENSE_NAME: &str = "State publication terms";

/// State capital project-performance adapter.
#[derive(Debug, Clone)]
pub struct StateCapitalAdapter {
    manifest: AdapterManifest,
    index_url: String,
}

impl Default for StateCapitalAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl StateCapitalAdapter {
    /// Start building a state-capital adapter.
    #[must_use]
    pub fn builder() -> StateCapitalAdapterBuilder {
        StateCapitalAdapterBuilder::default()
    }

    /// Parse a state-capital publication index into sidecar JSON links.
    pub fn parse_publications(body: &str) -> Result<Vec<StateCapitalPublication>, AdapterError> {
        parse_publications_with_base(body, DEFAULT_INDEX_URL)
    }

    /// Convert current publications into jobs for the supplied timestamp.
    #[must_use]
    pub fn current_jobs_with_started_at(
        current: &[StateCapitalPublication],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Diff current publications against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[StateCapitalPublication],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        discoverable_jobs_with_index_url(
            current,
            known_revisions,
            started_at,
            trace_parent,
            DEFAULT_INDEX_URL,
            None,
        )
    }

    /// Static metadata for state-capital dataflows.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![
            Dataflow {
                id: vic_major_projects_dataflow_id(),
                source_id: source_id(),
                name: "VIC major project performance".into(),
                description: Some(
                    "Pilot Victorian major-project cost and schedule observations parsed from validated PDF-sidecar JSON."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("jurisdiction").expect("static dimension id is valid"),
                    DimensionId::new("project").expect("static dimension id is valid"),
                    DimensionId::new("category").expect("static dimension id is valid"),
                    DimensionId::new("metric").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new("value").expect("static measure id is valid")],
                frequency: Frequency::Annual,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: DEFAULT_INDEX_URL.into(),
            },
            Dataflow {
                id: budget_capital_papers_dataflow_id(),
                source_id: source_id(),
                name: "State budget capital papers".into(),
                description: Some(
                    "Pilot state budget capital-program observations parsed from validated PDF-sidecar JSON."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("jurisdiction").expect("static dimension id is valid"),
                    DimensionId::new("category").expect("static dimension id is valid"),
                    DimensionId::new("metric").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new("value").expect("static measure id is valid")],
                frequency: Frequency::Annual,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: DEFAULT_INDEX_URL.into(),
            },
        ]
    }

    fn index_url(&self) -> &str {
        &self.index_url
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "state capital fetch received job for source `{}`",
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
                "state capital fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        let provenance = publication_url_provenance(&job.source_url).ok_or_else(|| {
            AdapterError::Validation(format!(
                "state capital fetch URL `{}` is not a target sidecar artifact",
                job.source_url
            ))
        })?;
        if provenance.dataflow_id != job.dataflow_id {
            return Err(AdapterError::Validation(format!(
                "state capital fetch URL `{}` has dataflow `{}` not `{}`",
                job.source_url,
                provenance.dataflow_id.as_str(),
                job.dataflow_id.as_str()
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl SourceAdapter for StateCapitalAdapter {
    fn id(&self) -> &'static str {
        SOURCE_ID
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    fn source_metadata(&self) -> Option<Source> {
        Some(Source {
            id: source_id(),
            name: "State capital pilot".into(),
            homepage: "https://www.audit.vic.gov.au".into(),
            description: Some(
                "Pilot adapter for state major-project and budget capital performance inputs."
                    .into(),
            ),
        })
    }

    fn dataflow_metadata(&self) -> Vec<Dataflow> {
        StateCapitalAdapter::dataflow_metadata(self)
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        let requested = ctx.requested_dataflow_id().cloned();
        if let Some(requested) = &requested {
            if !self
                .manifest
                .dataflows
                .iter()
                .any(|dataflow| dataflow == requested)
            {
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
        let publications = parse_publications_with_base(&body, self.index_url())?;
        Ok(discoverable_jobs_with_index_url(
            &publications,
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
            self.index_url(),
            requested.as_ref(),
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
                    .header("accept", "application/json"),
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
            .map_or_else(|| "application/json".to_string(), str::to_string);
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
    let dataflow_id = match validate_parse_artifact(&artifact, ctx.expected_dataflow_id()) {
        Ok(dataflow_id) => dataflow_id,
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
        if let Err(err) = parse_json_artifact(
            blob_store,
            key,
            artifact,
            dataflow_id,
            started_at,
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

async fn parse_json_artifact(
    blob_store: BlobStore,
    key: StorageKey,
    artifact: ArtifactRef,
    dataflow_id: DataflowId,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    let mut chunks = blob_store.get(&key).await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = chunks.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    let sidecar: SidecarDocument =
        serde_json::from_slice(&bytes).map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
    validate_sidecar(&sidecar)?;
    let rows = match dataflow_id.as_str() {
        VIC_MAJOR_PROJECTS_DATAFLOW_ID => parse_major_projects(&sidecar, &artifact, ingested_at)?,
        BUDGET_CAPITAL_PAPERS_DATAFLOW_ID => {
            parse_budget_capital(&sidecar, &artifact, ingested_at)?
        }
        other => {
            return Err(AdapterError::Validation(format!(
                "state capital parse expected unsupported dataflow `{other}`"
            )));
        }
    };
    for row in rows {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

fn validate_sidecar(sidecar: &SidecarDocument) -> Result<(), AdapterError> {
    require_non_empty(&sidecar.artifact_key, "artifact_key")?;
    require_non_empty(&sidecar.retrieved_at, "retrieved_at")?;
    require_non_empty(&sidecar.reviewer, "reviewer")?;
    require_non_empty(&sidecar.source_document, "source_document")?;
    if sidecar.tables.is_empty() {
        return Err(AdapterError::FormatDrift(
            "state capital sidecar contains no tables".into(),
        ));
    }
    for table in &sidecar.tables {
        require_non_empty(&table.name, "table.name")?;
        if table.rows.is_empty() {
            return Err(AdapterError::FormatDrift(format!(
                "state capital sidecar table `{}` contains no rows",
                table.name
            )));
        }
    }
    Ok(())
}

fn parse_major_projects(
    sidecar: &SidecarDocument,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let mut parsed = Vec::new();
    for row in sidecar
        .tables
        .iter()
        .filter(|table| table.name == "major_projects")
        .flat_map(|table| table.rows.iter().map(|row| (table.page, row)))
    {
        let (page, row) = row;
        let major = row.as_major_project()?;
        let time = parse_year(major.period)?;
        let cost_overrun_pct = round4(
            ((major.actual_cost_million - major.budget_million) / major.budget_million) * 100.0,
        );
        let delay_months = round4(months_between(
            major.planned_completion,
            major.forecast_completion,
        )?);
        parsed.push(build_row(BuildRow {
            dataflow_id: vic_major_projects_dataflow_id(),
            jurisdiction: major.jurisdiction,
            project: Some(major.project),
            category: major.category,
            metric: "cost_overrun_pct",
            unit: "percent",
            time,
            value: cost_overrun_pct,
            sidecar,
            page,
            artifact,
            ingested_at,
        })?);
        parsed.push(build_row(BuildRow {
            dataflow_id: vic_major_projects_dataflow_id(),
            jurisdiction: major.jurisdiction,
            project: Some(major.project),
            category: major.category,
            metric: "schedule_delay_months",
            unit: "months",
            time,
            value: delay_months,
            sidecar,
            page,
            artifact,
            ingested_at,
        })?);
    }
    if parsed.is_empty() {
        return Err(AdapterError::FormatDrift(
            "state capital sidecar contains no major project rows".into(),
        ));
    }
    Ok(parsed)
}

fn parse_budget_capital(
    sidecar: &SidecarDocument,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let mut parsed = Vec::new();
    for row in sidecar
        .tables
        .iter()
        .filter(|table| table.name == "capital_program")
        .flat_map(|table| table.rows.iter().map(|row| (table.page, row)))
    {
        let (page, row) = row;
        let budget = row.as_budget_capital()?;
        let time = parse_year(budget.period)?;
        let delivery_pct =
            round4((budget.actual_capex_million / budget.budgeted_capex_million) * 100.0);
        let variance = round4(budget.actual_capex_million - budget.budgeted_capex_million);
        parsed.push(build_row(BuildRow {
            dataflow_id: budget_capital_papers_dataflow_id(),
            jurisdiction: budget.jurisdiction,
            project: None,
            category: budget.category,
            metric: "capital_delivery_pct",
            unit: "percent",
            time,
            value: delivery_pct,
            sidecar,
            page,
            artifact,
            ingested_at,
        })?);
        parsed.push(build_row(BuildRow {
            dataflow_id: budget_capital_papers_dataflow_id(),
            jurisdiction: budget.jurisdiction,
            project: None,
            category: budget.category,
            metric: "capital_variance_million",
            unit: "$ million",
            time,
            value: variance,
            sidecar,
            page,
            artifact,
            ingested_at,
        })?);
    }
    if parsed.is_empty() {
        return Err(AdapterError::FormatDrift(
            "state capital sidecar contains no budget capital rows".into(),
        ));
    }
    Ok(parsed)
}

struct BuildRow<'a> {
    dataflow_id: DataflowId,
    jurisdiction: &'a str,
    project: Option<&'a str>,
    category: &'a str,
    metric: &'a str,
    unit: &'a str,
    time: DateTime<Utc>,
    value: f64,
    sidecar: &'a SidecarDocument,
    page: u32,
    artifact: &'a ArtifactRef,
    ingested_at: DateTime<Utc>,
}

fn build_row(input: BuildRow<'_>) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let mut dimensions = BTreeMap::from([
        (
            DimensionId::new("category").expect("static dimension id is valid"),
            code_id("category", &slugify_code(input.category))?,
        ),
        (
            DimensionId::new("jurisdiction").expect("static dimension id is valid"),
            code_id("jurisdiction", input.jurisdiction)?,
        ),
        (
            DimensionId::new("metric").expect("static dimension id is valid"),
            code_id("metric", input.metric)?,
        ),
    ]);
    if let Some(project) = input.project {
        dimensions.insert(
            DimensionId::new("project").expect("static dimension id is valid"),
            code_id("project", &slugify_code(project))?,
        );
    }
    let measure_id = MeasureId::new("value").expect("static measure id is valid");
    let series_key = SeriesKey::derive(
        &input.dataflow_id,
        &measure_id,
        dimensions
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    );
    let descriptor = SeriesDescriptor {
        series_key,
        dataflow_id: input.dataflow_id,
        measure_id,
        dimensions,
        unit: input.unit.into(),
    };
    let observation = Observation {
        series_key,
        time: input.time,
        time_precision: TimePrecision::Year,
        value: Some(input.value),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes: BTreeMap::from([
            ("source_url".into(), input.artifact.source_url.clone()),
            (
                "source_document".into(),
                input.sidecar.source_document.clone(),
            ),
            (
                "sidecar_artifact_key".into(),
                input.sidecar.artifact_key.clone(),
            ),
            (
                "sidecar_retrieved_at".into(),
                input.sidecar.retrieved_at.clone(),
            ),
            ("sidecar_reviewer".into(), input.sidecar.reviewer.clone()),
            ("sidecar_page".into(), page_to_string(input.page)),
            ("attribution".into(), ATTRIBUTION.into()),
            ("license".into(), LICENSE_NAME.into()),
        ]),
        ingested_at: input.ingested_at,
        source_artifact_id: input.artifact.id,
    };
    Ok((descriptor, observation))
}

fn validate_parse_artifact(
    artifact: &ArtifactRef,
    expected_dataflow_id: Option<&DataflowId>,
) -> Result<DataflowId, AdapterError> {
    if artifact.source_id.as_str() != SOURCE_ID {
        return Err(AdapterError::Validation(format!(
            "state capital parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    let provenance = publication_url_provenance(&artifact.source_url).ok_or_else(|| {
        AdapterError::Validation(format!(
            "state capital parse artifact `{}` is missing state capital publication provenance",
            artifact.source_url
        ))
    })?;
    if let Some(expected) = expected_dataflow_id {
        if expected != &provenance.dataflow_id {
            return Err(AdapterError::Validation(format!(
                "state capital parse expected dataflow `{}` but artifact implies `{}`",
                expected.as_str(),
                provenance.dataflow_id.as_str()
            )));
        }
    }
    Ok(provenance.dataflow_id)
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
            "state capital parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )));
    }
    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "state capital parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )))
    }
}

fn parse_publications_with_base(
    body: &str,
    base_url: &str,
) -> Result<Vec<StateCapitalPublication>, AdapterError> {
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
        let Some(provenance) = publication_url_provenance(&source_url) else {
            continue;
        };
        let title = clean_html_text(text).unwrap_or_else(|| provenance.publication_id.clone());
        publications.push(StateCapitalPublication {
            publication_id: provenance.publication_id,
            dataflow_id: provenance.dataflow_id,
            title,
            source_url,
            last_updated: attr_value(attrs, "data-updated"),
        });
    }
    publications.sort_by(|left, right| left.publication_id.cmp(&right.publication_id));
    publications.dedup_by(|left, right| left.publication_id == right.publication_id);
    Ok(publications)
}

fn discoverable_jobs_with_index_url(
    current: &[StateCapitalPublication],
    known_revisions: &BTreeMap<String, UpstreamRevision>,
    started_at: DateTime<Utc>,
    trace_parent: Option<&str>,
    index_url: &str,
    requested: Option<&DataflowId>,
) -> Vec<DiscoveredJob> {
    current
        .iter()
        .filter(|publication| {
            requested.is_none_or(|requested| requested == &publication.dataflow_id)
        })
        .filter_map(|publication| {
            let revision = publication.revision(started_at);
            known_revisions
                .get(&publication.revision_key())
                .is_none_or(|known| known != &revision)
                .then(|| publication.to_discovered_job(started_at, trace_parent, index_url))
        })
        .collect()
}

#[derive(Debug, Clone)]
struct PublicationUrlProvenance {
    publication_id: String,
    dataflow_id: DataflowId,
}

fn publication_url_provenance(source_url: &str) -> Option<PublicationUrlProvenance> {
    let filename = source_url
        .split('?')
        .next()?
        .split('#')
        .next()?
        .rsplit('/')
        .next()?;
    let stem = filename.strip_suffix(".json")?;
    let dataflow_id = if stem.contains("major-projects") {
        vic_major_projects_dataflow_id()
    } else if stem.contains("budget-capital") {
        budget_capital_papers_dataflow_id()
    } else {
        return None;
    };
    let host_port = source_url.split_once("://")?.1.split('/').next()?;
    let host = host_port.split(':').next().unwrap_or(host_port);
    if !matches!(
        host,
        "www.audit.vic.gov.au"
            | "audit.vic.gov.au"
            | "www.budget.vic.gov.au"
            | "budget.vic.gov.au"
            | "127.0.0.1"
    ) {
        return None;
    }
    Some(PublicationUrlProvenance {
        publication_id: stem.to_string(),
        dataflow_id,
    })
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
            AdapterError::Validation(format!(
                "state capital index URL `{base_url}` is not absolute"
            ))
        })?;
        let path_start = base_url[scheme_end + 3..]
            .find('/')
            .map_or(base_url.len(), |index| scheme_end + 3 + index);
        return Ok(format!("{}{}", &base_url[..path_start], href));
    }
    let Some((prefix, _)) = base_url.rsplit_once('/') else {
        return Err(AdapterError::Validation(format!(
            "state capital index URL `{base_url}` has no path separator"
        )));
    };
    Ok(format!("{prefix}/{href}"))
}

#[derive(Debug, Deserialize)]
struct SidecarDocument {
    artifact_key: String,
    retrieved_at: String,
    reviewer: String,
    source_document: String,
    tables: Vec<SidecarTable>,
}

#[derive(Debug, Deserialize)]
struct SidecarTable {
    name: String,
    page: u32,
    rows: Vec<SidecarRow>,
}

#[derive(Debug, Deserialize)]
struct SidecarRow {
    jurisdiction: String,
    project: Option<String>,
    category: String,
    period: String,
    budget_million: Option<f64>,
    actual_cost_million: Option<f64>,
    planned_completion: Option<String>,
    forecast_completion: Option<String>,
    budgeted_capex_million: Option<f64>,
    actual_capex_million: Option<f64>,
}

impl SidecarRow {
    fn as_major_project(&self) -> Result<MajorProjectRow<'_>, AdapterError> {
        Ok(MajorProjectRow {
            jurisdiction: &self.jurisdiction,
            project: self
                .project
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    AdapterError::FormatDrift(
                        "state capital major-project row missing project".into(),
                    )
                })?,
            category: &self.category,
            period: &self.period,
            budget_million: required_number(self.budget_million, "budget_million")?,
            actual_cost_million: required_number(self.actual_cost_million, "actual_cost_million")?,
            planned_completion: required_text(
                self.planned_completion.as_deref(),
                "planned_completion",
            )?,
            forecast_completion: required_text(
                self.forecast_completion.as_deref(),
                "forecast_completion",
            )?,
        })
    }

    fn as_budget_capital(&self) -> Result<BudgetCapitalRow<'_>, AdapterError> {
        Ok(BudgetCapitalRow {
            jurisdiction: &self.jurisdiction,
            category: &self.category,
            period: &self.period,
            budgeted_capex_million: required_number(
                self.budgeted_capex_million,
                "budgeted_capex_million",
            )?,
            actual_capex_million: required_number(
                self.actual_capex_million,
                "actual_capex_million",
            )?,
        })
    }
}

struct MajorProjectRow<'a> {
    jurisdiction: &'a str,
    project: &'a str,
    category: &'a str,
    period: &'a str,
    budget_million: f64,
    actual_cost_million: f64,
    planned_completion: &'a str,
    forecast_completion: &'a str,
}

struct BudgetCapitalRow<'a> {
    jurisdiction: &'a str,
    category: &'a str,
    period: &'a str,
    budgeted_capex_million: f64,
    actual_capex_million: f64,
}

fn required_number(value: Option<f64>, field: &str) -> Result<f64, AdapterError> {
    let value = value
        .ok_or_else(|| AdapterError::FormatDrift(format!("state capital row missing `{field}`")))?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(AdapterError::FormatDrift(format!(
            "state capital row has non-finite `{field}`"
        )))
    }
}

fn required_text<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str, AdapterError> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AdapterError::FormatDrift(format!("state capital row missing `{field}`")))
}

fn require_non_empty(value: &str, field: &str) -> Result<(), AdapterError> {
    if value.trim().is_empty() {
        Err(AdapterError::FormatDrift(format!(
            "state capital sidecar missing `{field}`"
        )))
    } else {
        Ok(())
    }
}

fn parse_year(value: &str) -> Result<DateTime<Utc>, AdapterError> {
    let year = value.parse::<i32>().map_err(|_| {
        AdapterError::FormatDrift(format!("invalid state capital period `{value}`"))
    })?;
    Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0)
        .single()
        .ok_or_else(|| AdapterError::FormatDrift(format!("invalid state capital period `{value}`")))
}

fn months_between(start: &str, end: &str) -> Result<f64, AdapterError> {
    let start = NaiveDate::parse_from_str(start, "%Y-%m-%d")
        .map_err(|_| AdapterError::FormatDrift(format!("invalid date `{start}`")))?;
    let end = NaiveDate::parse_from_str(end, "%Y-%m-%d")
        .map_err(|_| AdapterError::FormatDrift(format!("invalid date `{end}`")))?;
    Ok(f64::from((end.year() - start.year()) * 12)
        + f64::from(end.month() as i32 - start.month() as i32))
}

fn round4(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}

fn page_to_string(page: u32) -> String {
    page.to_string()
}

fn slugify_code(value: &str) -> String {
    let slug = value
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
        "value".into()
    } else {
        slug
    }
}

fn code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!(
            "invalid state capital {field} code `{value}`: {err}"
        ))
    })
}

fn source_id() -> SourceId {
    SourceId::new(SOURCE_ID).expect("static source id is valid")
}

fn vic_major_projects_dataflow_id() -> DataflowId {
    DataflowId::new(VIC_MAJOR_PROJECTS_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn budget_capital_papers_dataflow_id() -> DataflowId {
    DataflowId::new(BUDGET_CAPITAL_PAPERS_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn cancelled_parse_error() -> AdapterError {
    CoreError::Io(io::Error::new(
        io::ErrorKind::Interrupted,
        "state capital parse cancelled",
    ))
    .into()
}

/// Builder for [`StateCapitalAdapter`].
#[derive(Debug, Clone)]
pub struct StateCapitalAdapterBuilder {
    index_url: String,
}

impl Default for StateCapitalAdapterBuilder {
    fn default() -> Self {
        Self {
            index_url: DEFAULT_INDEX_URL.into(),
        }
    }
}

impl StateCapitalAdapterBuilder {
    /// Override the publication index URL, usually for fixture tests.
    #[must_use]
    pub fn index_url(mut self, index_url: impl Into<String>) -> Self {
        self.index_url = index_url.into();
        self
    }

    /// Build the adapter.
    #[must_use]
    pub fn build(self) -> StateCapitalAdapter {
        StateCapitalAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: "State capital pilot".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(30, Duration::from_secs(60))
                    .expect("static state capital rate limit is valid"),
                dataflows: vec![
                    vic_major_projects_dataflow_id(),
                    budget_capital_papers_dataflow_id(),
                ],
            },
            index_url: self.index_url,
        }
    }
}

/// One state-capital sidecar publication link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateCapitalPublication {
    /// Stable source-local publication id.
    pub publication_id: String,
    /// Dataflow represented by this publication.
    pub dataflow_id: DataflowId,
    /// Link text or publication title.
    pub title: String,
    /// Canonical sidecar artifact URL.
    pub source_url: String,
    /// Optional update marker scraped from the index.
    pub last_updated: Option<String>,
}

impl StateCapitalPublication {
    /// Build a stored upstream revision for tests and revision comparisons.
    #[must_use]
    pub fn revision_for(version: &str, last_updated: Option<&str>) -> UpstreamRevision {
        UpstreamRevision::new(version.to_string(), last_updated.map(str::to_owned))
    }

    fn revision_key(&self) -> String {
        format!("STATE_CAPITAL:{}", self.publication_id)
    }

    fn revision(&self, started_at: DateTime<Utc>) -> UpstreamRevision {
        let version = self
            .last_updated
            .clone()
            .unwrap_or_else(|| started_at.format("%Y-%m-%d").to_string());
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
        DiscoveredJob {
            id: format!("state-capital:{}:{revision_version}", self.publication_id),
            source_id: source_id(),
            dataflow_id: self.dataflow_id.clone(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("adapter".into(), "state-capital".into()),
                ("artifact_format".into(), "pdf-sidecar-json".into()),
                ("attribution".into(), ATTRIBUTION.into()),
                ("cadence".into(), "annual".into()),
                ("dataflow_id".into(), self.dataflow_id.as_str().into()),
                ("jurisdiction".into(), "VIC".into()),
                ("license".into(), LICENSE_NAME.into()),
                ("publication_id".into(), self.publication_id.clone()),
                ("revision_key".into(), self.revision_key()),
                ("revision_version".into(), revision_version),
                (
                    "schema_drift_policy".into(),
                    "validate-pdf-sidecar-json".into(),
                ),
                ("source_index_url".into(), index_url.into()),
                ("title".into(), self.title.clone()),
            ]),
        }
    }
}
