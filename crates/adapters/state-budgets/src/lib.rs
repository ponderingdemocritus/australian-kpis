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

const DEFAULT_PDF_BASE_URL: &str = "http://127.0.0.1:8010";
const USER_AGENT: &str = concat!("au-kpis-adapter-state-budgets/", env!("CARGO_PKG_VERSION"));
const SOURCE_ID: &str = "state-budgets";
const DATAFLOW_ID: &str = "state_budgets.nsw_budget";
const VIC_DATAFLOW_ID: &str = "state_budgets.vic_budget";
const QLD_DATAFLOW_ID: &str = "state_budgets.qld_budget";
const JURISDICTION: &str = "NSW";
const JURISDICTION_NAME: &str = "New South Wales";
const SOURCE_NAME: &str = "NSW Treasury";
const ATTRIBUTION: &str = "Source: NSW Treasury";
const LICENSE_NAME: &str = "Creative Commons Attribution 3.0 Australia Licence";
const LICENSE_URL: &str = "https://creativecommons.org/licenses/by/3.0/au/";
const DEFAULT_SOURCE_INDEX_URL: &str =
    "https://www.nsw.gov.au/business-and-economy/nsw-budget/2025-26-budget-papers";
const DEFAULT_BUDGET_PDF_URL: &str = "https://www.nsw.gov.au/sites/default/files/noindex/2026-03/bp1-budget-statement-nsw-budget-2025-26.pdf";
const PAPER: &str = "Budget Paper No. 1";
const PAPER_SLUG: &str = "bp1-budget-statement";
const TARGET_TITLE: &str = "Budget Statement";
const NSW_KEY_AGGREGATES_SCHEMA_KEY: &str = "table_1_1_key_fiscal_aggregates_m";
const NSW_KEY_AGGREGATES_SCHEMA_HASH: &str =
    "61014127d5e49374262775674f0abd3bf87731a276cc1deecb69381d4bf811aa";
const NSW_KEY_BUDGET_AGGREGATES_SCHEMA_KEY: &str =
    "table_1_2_key_budget_aggregates_for_the_general_government_sector";
const NSW_KEY_BUDGET_AGGREGATES_SCHEMA_HASH: &str =
    "8682d8d86a4591880d0f82cf85acc0eabb8608b2e04fd4dc0934eb8317f7c385";
const VIC_KEY_AGGREGATES_SCHEMA_KEY: &str =
    "table_1_1_estimated_financial_statements_for_the_general_government_sector_million";
const VIC_KEY_AGGREGATES_SCHEMA_HASH: &str =
    "25df1806f48ed1a256abeb5778785adddf4d5e970eb211b6d47965aecde36c6b";
const VIC_OPERATING_STATEMENT_SCHEMA_KEY: &str = "comprehensive_operating_statement";
const VIC_OPERATING_STATEMENT_SCHEMA_HASH: &str =
    "a3a19b2782eee5343c3bc530b81ce9468e814bd22e5d007155b4df02d92bc706";
const VIC_JURISDICTION: &str = "VIC";
const VIC_JURISDICTION_NAME: &str = "Victoria";
const VIC_SOURCE_NAME: &str = "Department of Treasury and Finance Victoria";
const VIC_ATTRIBUTION: &str = "© Copyright State Government of Victoria";
const VIC_LICENSE_NAME: &str = "Creative Commons Attribution 4.0 International licence";
const VIC_LICENSE_URL: &str = "https://creativecommons.org/licenses/by/4.0/";
const VIC_SOURCE_INDEX_URL: &str = "https://www.budget.vic.gov.au/budget-papers";
const VIC_BUDGET_PDF_URL: &str = "https://s3.ap-southeast-2.amazonaws.com/vicbudgetfiles2026.27vicbudget/2026-27+State+Budget+-+Statement+of+Finances.pdf";
const VIC_PAPER: &str = "Budget Paper No. 5";
const VIC_PAPER_SLUG: &str = "bp5-statement-of-finances";
const VIC_TARGET_TITLE: &str = "Statement of Finances";
const QLD_KEY_AGGREGATES_SCHEMA_KEY: &str =
    "table_8_1_general_government_sector_operating_statement_million";
const QLD_KEY_AGGREGATES_SCHEMA_HASH: &str =
    "a8706dc9455aff8bc8474454cbf528b69daea0c13e2abf4805ff01fe10ead3a2";
const QLD_OPERATING_STATEMENT_SCHEMA_KEY: &str =
    "table_8_1_general_government_sector_operating_statement1";
const QLD_OPERATING_STATEMENT_SCHEMA_HASH: &str =
    "3dc097502c47f72793ba9bff6b62b77d022d00a69ac6e7e7d81349bb11c8e7ef";
const QLD_JURISDICTION: &str = "QLD";
const QLD_JURISDICTION_NAME: &str = "Queensland";
const QLD_SOURCE_NAME: &str = "Queensland Treasury";
const QLD_ATTRIBUTION: &str = "© The State of Queensland 2025 (Queensland Treasury)";
const QLD_LICENSE_NAME: &str = "Queensland Treasury copyright";
const QLD_LICENSE_URL: &str = "https://www.treasury.qld.gov.au/legal/copyright/";
const QLD_SOURCE_INDEX_URL: &str = "https://budget.qld.gov.au/budget-papers/";
const QLD_BUDGET_PDF_URL: &str =
    "https://budget.qld.gov.au/files/Budget-2025-26-BP2-Budget-Strategy-Outlook.pdf";
const QLD_PAPER: &str = "Budget Paper No. 2";
const QLD_PAPER_SLUG: &str = "bp2-budget-strategy-outlook";
const QLD_TARGET_TITLE: &str = "Budget Strategy and Outlook";

#[derive(Debug, Clone, Copy)]
struct BudgetConfig {
    dataflow_id: &'static str,
    dataflow_name: &'static str,
    dataflow_description: &'static str,
    jurisdiction: &'static str,
    jurisdiction_name: &'static str,
    source_name: &'static str,
    attribution: &'static str,
    license: BudgetLicense,
    license_name: &'static str,
    license_url: &'static str,
    source_index_url: &'static str,
    default_budget_pdf_url: &'static str,
    default_last_updated: &'static str,
    paper: &'static str,
    paper_slug: &'static str,
    target_title: &'static str,
    schema_key: &'static str,
    schema_hash: &'static str,
    extract_first_page: u32,
    extract_last_page: u32,
    official_parse_hosts: &'static [&'static str],
}

#[derive(Debug, Clone, Copy)]
enum BudgetLicense {
    CcBy40,
    Other(&'static str),
}

const NSW_CONFIG: BudgetConfig = BudgetConfig {
    dataflow_id: DATAFLOW_ID,
    dataflow_name: "NSW state budget",
    dataflow_description: "Annual New South Wales budget aggregates parsed from NSW Treasury budget PDFs.",
    jurisdiction: JURISDICTION,
    jurisdiction_name: JURISDICTION_NAME,
    source_name: SOURCE_NAME,
    attribution: ATTRIBUTION,
    license: BudgetLicense::Other(LICENSE_NAME),
    license_name: LICENSE_NAME,
    license_url: LICENSE_URL,
    source_index_url: DEFAULT_SOURCE_INDEX_URL,
    default_budget_pdf_url: DEFAULT_BUDGET_PDF_URL,
    default_last_updated: "2026-03-20",
    paper: PAPER,
    paper_slug: PAPER_SLUG,
    target_title: TARGET_TITLE,
    schema_key: NSW_KEY_AGGREGATES_SCHEMA_KEY,
    schema_hash: NSW_KEY_AGGREGATES_SCHEMA_HASH,
    extract_first_page: 1,
    extract_last_page: 80,
    official_parse_hosts: &[
        "budget.nsw.gov.au",
        "www.budget.nsw.gov.au",
        "www.nsw.gov.au",
    ],
};

const VIC_CONFIG: BudgetConfig = BudgetConfig {
    dataflow_id: VIC_DATAFLOW_ID,
    dataflow_name: "VIC state budget",
    dataflow_description: "Annual Victorian budget aggregates parsed from Victorian Budget statement of finances PDFs.",
    jurisdiction: VIC_JURISDICTION,
    jurisdiction_name: VIC_JURISDICTION_NAME,
    source_name: VIC_SOURCE_NAME,
    attribution: VIC_ATTRIBUTION,
    license: BudgetLicense::CcBy40,
    license_name: VIC_LICENSE_NAME,
    license_url: VIC_LICENSE_URL,
    source_index_url: VIC_SOURCE_INDEX_URL,
    default_budget_pdf_url: VIC_BUDGET_PDF_URL,
    default_last_updated: "2026-05-05",
    paper: VIC_PAPER,
    paper_slug: VIC_PAPER_SLUG,
    target_title: VIC_TARGET_TITLE,
    schema_key: VIC_KEY_AGGREGATES_SCHEMA_KEY,
    schema_hash: VIC_KEY_AGGREGATES_SCHEMA_HASH,
    extract_first_page: 14,
    extract_last_page: 14,
    official_parse_hosts: &[
        "budget.vic.gov.au",
        "www.budget.vic.gov.au",
        "s3.ap-southeast-2.amazonaws.com",
    ],
};

const QLD_CONFIG: BudgetConfig = BudgetConfig {
    dataflow_id: QLD_DATAFLOW_ID,
    dataflow_name: "QLD state budget",
    dataflow_description: "Annual Queensland budget aggregates parsed from Queensland Treasury budget strategy and outlook PDFs.",
    jurisdiction: QLD_JURISDICTION,
    jurisdiction_name: QLD_JURISDICTION_NAME,
    source_name: QLD_SOURCE_NAME,
    attribution: QLD_ATTRIBUTION,
    license: BudgetLicense::Other(QLD_LICENSE_NAME),
    license_name: QLD_LICENSE_NAME,
    license_url: QLD_LICENSE_URL,
    source_index_url: QLD_SOURCE_INDEX_URL,
    default_budget_pdf_url: QLD_BUDGET_PDF_URL,
    default_last_updated: "2025-06-24",
    paper: QLD_PAPER,
    paper_slug: QLD_PAPER_SLUG,
    target_title: QLD_TARGET_TITLE,
    schema_key: QLD_KEY_AGGREGATES_SCHEMA_KEY,
    schema_hash: QLD_KEY_AGGREGATES_SCHEMA_HASH,
    extract_first_page: 113,
    extract_last_page: 113,
    official_parse_hosts: &["budget.qld.gov.au", "www.budget.qld.gov.au"],
};

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
            &NSW_CONFIG,
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
        state_budget_dataflow_metadata(&NSW_CONFIG)
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
        budget_provenance_for_fetch(&NSW_CONFIG, &job.source_url, &job.metadata).ok_or_else(
            || {
                AdapterError::Validation(format!(
                    "NSW budget fetch URL `{}` is not a curated NSW budget PDF artifact",
                    job.source_url
                ))
            },
        )?;
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
            .is_some_and(|requested| requested != &dataflow_id(&NSW_CONFIG))
        {
            return Ok(Vec::new());
        }
        Ok(discoverable_jobs_with_source_index(
            &NSW_CONFIG,
            &self.publications,
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
            NSW_CONFIG.source_index_url,
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
        parse_artifact_stream(&NSW_CONFIG, self.pdf_client.clone(), artifact, ctx)
    }
}

/// VIC state budget PDF adapter.
#[derive(Debug, Clone)]
pub struct VicBudgetAdapter {
    manifest: AdapterManifest,
    publications: Vec<VicBudgetPublication>,
    pdf_client: PdfClient,
}

impl Default for VicBudgetAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl VicBudgetAdapter {
    /// Start building a VIC budget adapter.
    #[must_use]
    pub fn builder() -> VicBudgetAdapterBuilder {
        VicBudgetAdapterBuilder::default()
    }

    /// Convert current curated publications into jobs for the supplied timestamp.
    #[must_use]
    pub fn current_jobs_with_started_at(
        current: &[VicBudgetPublication],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Diff current VIC publications against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[VicBudgetPublication],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        discoverable_jobs_with_source_index(
            &VIC_CONFIG,
            current,
            known_revisions,
            started_at,
            trace_parent,
            VIC_CONFIG.source_index_url,
        )
    }

    /// Static metadata for the VIC state budget dataflow.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        state_budget_dataflow_metadata(&VIC_CONFIG)
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "VIC budget fetch received job for source `{}`",
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
                "VIC budget fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        budget_provenance_for_fetch(&VIC_CONFIG, &job.source_url, &job.metadata).ok_or_else(
            || {
                AdapterError::Validation(format!(
                    "VIC budget fetch URL `{}` is not a curated VIC budget PDF artifact",
                    job.source_url
                ))
            },
        )?;
        Ok(())
    }
}

#[async_trait]
impl SourceAdapter for VicBudgetAdapter {
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
            .is_some_and(|requested| requested != &dataflow_id(&VIC_CONFIG))
        {
            return Ok(Vec::new());
        }
        Ok(discoverable_jobs_with_source_index(
            &VIC_CONFIG,
            &self.publications,
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
            VIC_CONFIG.source_index_url,
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
        parse_artifact_stream(&VIC_CONFIG, self.pdf_client.clone(), artifact, ctx)
    }
}

/// QLD state budget PDF adapter.
#[derive(Debug, Clone)]
pub struct QldBudgetAdapter {
    manifest: AdapterManifest,
    publications: Vec<QldBudgetPublication>,
    pdf_client: PdfClient,
}

impl Default for QldBudgetAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl QldBudgetAdapter {
    /// Start building a QLD budget adapter.
    #[must_use]
    pub fn builder() -> QldBudgetAdapterBuilder {
        QldBudgetAdapterBuilder::default()
    }

    /// Convert current curated publications into jobs for the supplied timestamp.
    #[must_use]
    pub fn current_jobs_with_started_at(
        current: &[QldBudgetPublication],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Diff current QLD publications against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[QldBudgetPublication],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        discoverable_jobs_with_source_index(
            &QLD_CONFIG,
            current,
            known_revisions,
            started_at,
            trace_parent,
            QLD_CONFIG.source_index_url,
        )
    }

    /// Static metadata for the QLD state budget dataflow.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        state_budget_dataflow_metadata(&QLD_CONFIG)
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "QLD budget fetch received job for source `{}`",
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
                "QLD budget fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        budget_provenance_for_fetch(&QLD_CONFIG, &job.source_url, &job.metadata).ok_or_else(
            || {
                AdapterError::Validation(format!(
                    "QLD budget fetch URL `{}` is not a curated QLD budget PDF artifact",
                    job.source_url
                ))
            },
        )?;
        Ok(())
    }
}

#[async_trait]
impl SourceAdapter for QldBudgetAdapter {
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
            .is_some_and(|requested| requested != &dataflow_id(&QLD_CONFIG))
        {
            return Ok(Vec::new());
        }
        Ok(discoverable_jobs_with_source_index(
            &QLD_CONFIG,
            &self.publications,
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
            QLD_CONFIG.source_index_url,
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
        parse_artifact_stream(&QLD_CONFIG, self.pdf_client.clone(), artifact, ctx)
    }
}

/// Combined state budgets adapter registered under the shared `state-budgets` source.
#[derive(Debug, Clone)]
pub struct StateBudgetsAdapter {
    manifest: AdapterManifest,
    nsw: NswBudgetAdapter,
    vic: VicBudgetAdapter,
    qld: QldBudgetAdapter,
}

impl Default for StateBudgetsAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl StateBudgetsAdapter {
    /// Start building a combined state budgets adapter.
    #[must_use]
    pub fn builder() -> StateBudgetsAdapterBuilder {
        StateBudgetsAdapterBuilder::default()
    }

    /// Build a combined adapter from its state-specific adapters.
    #[must_use]
    pub fn new(nsw: NswBudgetAdapter, vic: VicBudgetAdapter, qld: QldBudgetAdapter) -> Self {
        Self {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: "Australian state budgets".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(20, Duration::from_secs(60))
                    .expect("static state budget rate limit is valid"),
                dataflows: vec![
                    dataflow_id(&NSW_CONFIG),
                    dataflow_id(&VIC_CONFIG),
                    dataflow_id(&QLD_CONFIG),
                ],
            },
            nsw,
            vic,
            qld,
        }
    }

    /// Static metadata for all registered state budget dataflows.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        let mut dataflows = self.nsw.dataflow_metadata();
        dataflows.extend(self.vic.dataflow_metadata());
        dataflows.extend(self.qld.dataflow_metadata());
        dataflows
    }
}

#[async_trait]
impl SourceAdapter for StateBudgetsAdapter {
    fn id(&self) -> &'static str {
        SOURCE_ID
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    fn source_metadata(&self) -> Option<Source> {
        Some(Source {
            id: source_id(),
            name: "Australian state budget publications".into(),
            homepage: "https://www.nsw.gov.au/business-and-economy/nsw-budget".into(),
            description: Some("Curated Australian state budget papers from NSW, Victoria, and Queensland treasury sites.".into()),
        })
    }

    fn dataflow_metadata(&self) -> Vec<Dataflow> {
        StateBudgetsAdapter::dataflow_metadata(self)
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        if let Some(requested) = ctx.requested_dataflow_id() {
            if requested == &dataflow_id(&NSW_CONFIG) {
                return self.nsw.discover(ctx).await;
            }
            if requested == &dataflow_id(&VIC_CONFIG) {
                return self.vic.discover(ctx).await;
            }
            if requested == &dataflow_id(&QLD_CONFIG) {
                return self.qld.discover(ctx).await;
            }
            return Ok(Vec::new());
        }

        let mut jobs = self.nsw.discover(ctx).await?;
        jobs.extend(self.vic.discover(ctx).await?);
        jobs.extend(self.qld.discover(ctx).await?);
        Ok(jobs)
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id(), job_id = %job.id))]
    async fn fetch(&self, job: DiscoveredJob, ctx: &FetchCtx) -> Result<ArtifactRef, AdapterError> {
        if job.dataflow_id == dataflow_id(&NSW_CONFIG) {
            return self.nsw.fetch(job, ctx).await;
        }
        if job.dataflow_id == dataflow_id(&VIC_CONFIG) {
            return self.vic.fetch(job, ctx).await;
        }
        if job.dataflow_id == dataflow_id(&QLD_CONFIG) {
            return self.qld.fetch(job, ctx).await;
        }
        Err(AdapterError::Validation(format!(
            "state budgets fetch received unsupported dataflow `{}`",
            job.dataflow_id.as_str()
        )))
    }

    fn parse<'a>(&'a self, artifact: ArtifactRef, ctx: &'a ParseCtx) -> ObservationStream<'a> {
        if let Some(expected) = ctx.expected_dataflow_id() {
            if expected == &dataflow_id(&NSW_CONFIG) {
                return self.nsw.parse(artifact, ctx);
            }
            if expected == &dataflow_id(&VIC_CONFIG) {
                return self.vic.parse(artifact, ctx);
            }
            if expected == &dataflow_id(&QLD_CONFIG) {
                return self.qld.parse(artifact, ctx);
            }
            let expected = expected.as_str().to_string();
            return Box::pin(stream::once(async move {
                Err(AdapterError::Validation(format!(
                    "state budgets parse received unsupported dataflow `{expected}`"
                )))
            }));
        }
        if budget_provenance_for_parse(&NSW_CONFIG, &artifact.source_url, ctx.metadata()).is_some()
        {
            return self.nsw.parse(artifact, ctx);
        }
        if budget_provenance_for_parse(&VIC_CONFIG, &artifact.source_url, ctx.metadata()).is_some()
        {
            return self.vic.parse(artifact, ctx);
        }
        self.qld.parse(artifact, ctx)
    }
}

fn state_budget_dataflow_metadata(config: &BudgetConfig) -> Vec<Dataflow> {
    vec![Dataflow {
        id: dataflow_id(config),
        source_id: source_id(),
        name: config.dataflow_name.into(),
        description: Some(config.dataflow_description.into()),
        dimensions: vec![
            DimensionId::new("jurisdiction").expect("static dimension id is valid"),
            DimensionId::new("budget_year").expect("static dimension id is valid"),
            DimensionId::new("paper").expect("static dimension id is valid"),
            DimensionId::new("table").expect("static dimension id is valid"),
            DimensionId::new("line_item").expect("static dimension id is valid"),
        ],
        measures: vec![MeasureId::new("value").expect("static measure id is valid")],
        frequency: Frequency::Annual,
        license: match config.license {
            BudgetLicense::CcBy40 => License::CcBy40,
            BudgetLicense::Other(name) => License::Other(name.into()),
        },
        attribution: config.attribution.into(),
        source_url: config.source_index_url.into(),
    }]
}

fn parse_artifact_stream<'a>(
    config: &'static BudgetConfig,
    pdf_client: PdfClient,
    artifact: ArtifactRef,
    ctx: &'a ParseCtx,
) -> ObservationStream<'a> {
    let provenance = match validate_parse_artifact(config, &artifact, ctx) {
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
            result = verify_parse_artifact_identity(config, &blob_store, &key, &artifact) => result,
        };
        if let Err(err) = identity {
            let _ = row_tx.send(Err(err)).await;
            return;
        }

        let result = parse_pdf_artifact(
            config,
            pdf_client,
            artifact,
            provenance,
            started_at,
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

async fn parse_pdf_artifact(
    config: &'static BudgetConfig,
    pdf_client: PdfClient,
    artifact: ArtifactRef,
    provenance: BudgetProvenance,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    let mut request = ExtractRequest::new(artifact.storage_key.clone(), SOURCE_ID)
        .strategy(ExtractionStrategy::Deterministic)
        .page_range(config.extract_first_page, config.extract_last_page);
    if let Some(artifact_date) = &provenance.artifact_date {
        request = request.artifact_date(artifact_date.clone());
    }
    let response = pdf_client
        .extract(request)
        .await
        .map_err(pdf_client_error)?;
    if response.artifact_key != artifact.storage_key {
        return Err(AdapterError::Validation(format!(
            "{} budget sidecar returned artifact key `{}` for requested artifact `{}`",
            config.jurisdiction, response.artifact_key, artifact.storage_key
        )));
    }
    if response.backend.kind != ExtractionBackendKind::Deterministic {
        return Err(AdapterError::FormatDrift(format!(
            "{} budget sidecar used unsupported backend `{}`",
            config.jurisdiction, response.backend.name
        )));
    }

    let rows = parse_table_candidates(config, response, &artifact, &provenance, ingested_at)?;
    for row in rows {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

fn parse_table_candidates(
    config: &'static BudgetConfig,
    response: ExtractionResponse,
    artifact: &ArtifactRef,
    provenance: &BudgetProvenance,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let backend = response.backend;
    let mut parsed = Vec::new();
    for (index, table) in response.tables.iter().enumerate() {
        if let Some(rows) = parse_state_budget_table(
            config,
            table,
            index,
            &backend,
            artifact,
            provenance,
            ingested_at,
        )? {
            parsed.extend(rows);
        }
    }

    if parsed.is_empty() {
        return Err(AdapterError::FormatDrift(format!(
            "{} budget PDF sidecar returned no recognised budget tables",
            config.jurisdiction
        )));
    }
    Ok(parsed)
}

fn parse_state_budget_table(
    config: &'static BudgetConfig,
    table: &TableCandidate,
    table_index: usize,
    backend: &BackendInfo,
    artifact: &ArtifactRef,
    provenance: &BudgetProvenance,
    ingested_at: DateTime<Utc>,
) -> Result<Option<Vec<(SeriesDescriptor, Observation)>>, AdapterError> {
    let rows = table
        .cells
        .iter()
        .map(|row| row.iter().map(|cell| clean_cell(cell)).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let rows = normalize_state_budget_rows(config, rows);
    let Some(periods) = find_budget_period_columns(&rows)? else {
        return Ok(None);
    };
    let table_title = table_title_for_candidate(&rows, periods.row_index, table.page, table_index);
    let schema_key = slugify_code(&table_title);
    let Some(expected_schema_hash) = schema_hash_for_key(config, &schema_key) else {
        return Ok(None);
    };
    let schema_hash = schema_hash_for_candidate(&table_title, &rows[periods.row_index]);
    let versions = parser_versions();
    let parser_version = select_parser_version(&versions, artifact_date_for_version(provenance)?)?;
    let expected = ExpectedSchemaHash::new(
        source_id(),
        dataflow_id(config),
        parser_version.name(),
        schema_key.clone(),
        expected_schema_hash,
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
        let (line_item, row_unit) = normalize_line_item_and_unit(&line_item, &unit);
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
                config,
                provenance,
                table_title: &table_title,
                table_page: table.page,
                line_item: &line_item,
                period_label: &period.label,
                unit: &row_unit,
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
    config: &'static BudgetConfig,
    provenance: &'a BudgetProvenance,
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
    let config = input.config;
    let dataflow_id = dataflow_id(config);
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("jurisdiction").expect("static dimension id is valid"),
            budget_code_id(config, "jurisdiction", config.jurisdiction)?,
        ),
        (
            DimensionId::new("budget_year").expect("static dimension id is valid"),
            budget_code_id(config, "budget_year", &input.provenance.budget_year)?,
        ),
        (
            DimensionId::new("paper").expect("static dimension id is valid"),
            budget_code_id(config, "paper", &input.provenance.paper_slug)?,
        ),
        (
            DimensionId::new("table").expect("static dimension id is valid"),
            budget_code_id(config, "table", &slugify_code(input.table_title))?,
        ),
        (
            DimensionId::new("line_item").expect("static dimension id is valid"),
            budget_code_id(config, "line_item", &slugify_code(input.line_item))?,
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
        ("source".into(), config.source_name.into()),
        ("source_url".into(), input.artifact.source_url.clone()),
        ("license".into(), config.license_name.into()),
        ("license_url".into(), config.license_url.into()),
        ("attribution".into(), config.attribution.into()),
        ("jurisdiction".into(), config.jurisdiction.into()),
        ("jurisdiction_name".into(), config.jurisdiction_name.into()),
        ("budget_year".into(), input.provenance.budget_year.clone()),
        ("paper".into(), input.provenance.paper.clone()),
        ("paper_slug".into(), input.provenance.paper_slug.clone()),
        ("publication_title".into(), input.provenance.title.clone()),
        ("table_title".into(), input.table_title.to_string()),
        ("table_page".into(), input.table_page.to_string()),
        ("parser_version".into(), input.parser_version.to_string()),
        ("schema_hash".into(), input.schema_hash.to_string()),
        ("extraction_backend".into(), input.backend.name.clone()),
        (
            "extraction_backend_version".into(),
            input.backend.version.clone(),
        ),
    ]);
    attributes.insert(
        format!("{}_line_item", config.jurisdiction.to_ascii_lowercase()),
        input.line_item.to_string(),
    );
    attributes.insert(
        format!("{}_period_label", config.jurisdiction.to_ascii_lowercase()),
        input.period_label.to_string(),
    );
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
    config: &'static BudgetConfig,
    artifact: &ArtifactRef,
    ctx: &ParseCtx,
) -> Result<BudgetProvenance, AdapterError> {
    if artifact.source_id.as_str() != SOURCE_ID {
        return Err(AdapterError::Validation(format!(
            "{} budget parse received artifact for source `{}`",
            config.jurisdiction,
            artifact.source_id.as_str()
        )));
    }
    if let Some(expected) = ctx.expected_dataflow_id() {
        let actual = dataflow_id(config);
        if expected != &actual {
            return Err(AdapterError::Validation(format!(
                "{} budget parse expected dataflow `{}` but adapter emits `{}`",
                config.jurisdiction,
                expected.as_str(),
                actual.as_str()
            )));
        }
    }
    budget_provenance_for_parse(config, &artifact.source_url, ctx.metadata()).ok_or_else(|| {
        AdapterError::Validation(format!(
            "{} budget parse artifact `{}` is missing curated state budget provenance",
            config.jurisdiction, artifact.source_url
        ))
    })
}

async fn verify_parse_artifact_identity(
    config: &'static BudgetConfig,
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
            "{} budget parse artifact storage key `{}` does not match artifact id `{}`",
            config.jurisdiction, artifact.storage_key, artifact.id
        )));
    }

    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "{} budget parse artifact storage key `{}` does not match artifact id `{}`",
            config.jurisdiction, artifact.storage_key, artifact.id
        )))
    }
}

#[derive(Debug, Clone)]
struct BudgetProvenance {
    budget_year: String,
    paper: String,
    paper_slug: String,
    title: String,
    artifact_date: Option<String>,
}

fn budget_provenance_for_fetch(
    config: &'static BudgetConfig,
    source_url: &str,
    metadata: &BTreeMap<String, String>,
) -> Option<BudgetProvenance> {
    budget_provenance(config, source_url, metadata, false)
}

fn budget_provenance_for_parse(
    config: &'static BudgetConfig,
    source_url: &str,
    metadata: &BTreeMap<String, String>,
) -> Option<BudgetProvenance> {
    budget_provenance(config, source_url, metadata, true)
}

fn budget_provenance(
    config: &'static BudgetConfig,
    source_url: &str,
    metadata: &BTreeMap<String, String>,
    require_nsw_host: bool,
) -> Option<BudgetProvenance> {
    let (_, after_scheme) = source_url.split_once("://")?;
    let (host, path_with_suffix) = after_scheme.split_once('/')?;
    if require_nsw_host && !config.official_parse_hosts.contains(&host) {
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
    if metadata.get("jurisdiction").map(String::as_str) != Some(config.jurisdiction) {
        return None;
    }
    if metadata.get("paper_slug").map(String::as_str) != Some(config.paper_slug) {
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
    Some(BudgetProvenance {
        budget_year,
        paper: metadata
            .get("paper")
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| config.paper.into()),
        paper_slug: config.paper_slug.into(),
        title: metadata
            .get("title")
            .filter(|value| !value.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| config.target_title.into()),
        artifact_date,
    })
}

fn discoverable_jobs_with_source_index(
    config: &'static BudgetConfig,
    current: &[StateBudgetPublication],
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
                .get(&publication.revision_key(config))
                .is_none_or(|known| known != &revision)
                .then(|| {
                    publication.to_discovered_job(
                        config,
                        started_at,
                        trace_parent,
                        source_index_url,
                    )
                })
        })
        .collect()
}

fn schema_hash_for_key(config: &BudgetConfig, schema_key: &str) -> Option<&'static str> {
    match (config.jurisdiction, schema_key) {
        (JURISDICTION, NSW_KEY_AGGREGATES_SCHEMA_KEY) => Some(NSW_KEY_AGGREGATES_SCHEMA_HASH),
        (JURISDICTION, NSW_KEY_BUDGET_AGGREGATES_SCHEMA_KEY) => {
            Some(NSW_KEY_BUDGET_AGGREGATES_SCHEMA_HASH)
        }
        (VIC_JURISDICTION, VIC_OPERATING_STATEMENT_SCHEMA_KEY) => {
            Some(VIC_OPERATING_STATEMENT_SCHEMA_HASH)
        }
        (QLD_JURISDICTION, QLD_OPERATING_STATEMENT_SCHEMA_KEY) => {
            Some(QLD_OPERATING_STATEMENT_SCHEMA_HASH)
        }
        _ if schema_key == config.schema_key => Some(config.schema_hash),
        _ => None,
    }
}

fn normalize_state_budget_rows(config: &BudgetConfig, rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let rows = if config.jurisdiction == JURISDICTION {
        trim_rows_before_table_title(rows)
    } else {
        rows
    };
    let rows = merge_split_table_title_rows(rows);
    let rows = merge_split_period_header_rows(rows);
    if config.jurisdiction == JURISDICTION {
        contextualize_repeated_unit_rows(repair_lagged_value_labels(rows))
    } else {
        rows
    }
}

fn trim_rows_before_table_title(rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    if let Some(index) = rows.iter().position(|row| {
        row.iter()
            .any(|cell| normalize_header(cell).starts_with("table "))
    }) {
        rows.into_iter().skip(index).collect()
    } else {
        rows
    }
}

fn merge_split_table_title_rows(rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .map(|mut row| {
            if row.len() > 1
                && normalize_header(&row[0]).starts_with("table ")
                && !row[1].trim().is_empty()
            {
                row[0] = format!("{} {}", row[0].trim(), row[1].trim());
                for cell in row.iter_mut().skip(1) {
                    cell.clear();
                }
            }
            row
        })
        .collect()
}

fn merge_split_period_header_rows(rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let mut merged = Vec::with_capacity(rows.len());
    let mut index = 0;
    while index < rows.len() {
        if index + 1 < rows.len()
            && fiscal_year_cell_count(&rows[index]) >= 2
            && is_split_status_row(&rows[index + 1])
        {
            let mut header = rows[index].clone();
            let statuses = filled_split_statuses(&rows[index + 1], header.len());
            for (column, cell) in header.iter_mut().enumerate() {
                if find_fiscal_year(cell).is_some() {
                    let status = statuses.get(column).map_or("", String::as_str);
                    if !status.is_empty() {
                        *cell = format!("{} {}", cell.trim(), status);
                    }
                }
            }
            merged.push(header);
            index += 2;
        } else {
            merged.push(rows[index].clone());
            index += 1;
        }
    }
    merged
}

fn is_split_status_row(row: &[String]) -> bool {
    !row_has_budget_values(row)
        && fiscal_year_cell_count(row) == 0
        && row.iter().skip(1).any(|cell| {
            let normalized = normalize_header(cell);
            normalized.contains("actual")
                || normalized.contains("revised")
                || normalized.contains("budget")
                || normalized.contains("estimate")
                || normalized.contains("projection")
        })
}

fn fiscal_year_cell_count(row: &[String]) -> usize {
    row.iter()
        .filter(|cell| find_fiscal_year(cell).is_some())
        .count()
}

fn filled_split_statuses(row: &[String], len: usize) -> Vec<String> {
    let mut statuses = (0..len)
        .map(|index| row.get(index).map_or("", String::as_str).trim().to_string())
        .collect::<Vec<_>>();

    let mut last_forward = String::new();
    for status in &mut statuses {
        if normalize_header(status).contains("forward") {
            last_forward.clone_from(status);
        } else if status.trim().is_empty() && !last_forward.is_empty() {
            status.clone_from(&last_forward);
        }
    }

    let mut next_forward = String::new();
    for status in statuses.iter_mut().rev() {
        if normalize_header(status).contains("forward") {
            next_forward.clone_from(status);
        } else if status.trim().is_empty() && !next_forward.is_empty() {
            status.clone_from(&next_forward);
        }
    }

    statuses
}

fn repair_lagged_value_labels(rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let mut repaired = Vec::with_capacity(rows.len());
    let mut index = 0;
    while index < rows.len() {
        let mut row = rows[index].clone();
        if label_before(&row, 1).is_none()
            && row_has_budget_values(&row)
            && index + 1 < rows.len()
            && label_before(&rows[index + 1], 1).is_some()
            && !row_has_budget_values(&rows[index + 1])
        {
            row[0] = label_before(&rows[index + 1], 1).expect("checked above");
            repaired.push(row);
            index += 2;
        } else {
            repaired.push(row);
            index += 1;
        }
    }
    repaired
}

fn row_has_budget_values(row: &[String]) -> bool {
    row.iter()
        .skip(1)
        .any(|cell| matches!(parse_value(cell), Ok((Some(_), _))))
}

fn contextualize_repeated_unit_rows(rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let mut current_metric = String::new();
    rows.into_iter()
        .map(|mut row| {
            if let Some(label) = row.first().map(String::as_str) {
                let normalized = normalize_header(label);
                if normalized == "per cent of gsp" && !current_metric.is_empty() {
                    row[0] = format!("{current_metric} / Per cent of GSP");
                } else if row_has_budget_values(&row)
                    && !label.trim().is_empty()
                    && normalized != "per cent of gsp"
                {
                    current_metric = strip_budget_unit_from_label(label);
                }
            }
            row
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
        let columns = dedupe_budget_period_columns(columns);
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

fn dedupe_budget_period_columns(columns: Vec<BudgetPeriod>) -> Vec<BudgetPeriod> {
    let mut deduped: Vec<BudgetPeriod> = Vec::with_capacity(columns.len());
    for period in columns {
        if let Some(existing) = deduped
            .iter_mut()
            .find(|existing| existing.label == period.label)
        {
            if period_status_rank(period.status) > period_status_rank(existing.status) {
                *existing = period;
            }
        } else {
            deduped.push(period);
        }
    }
    deduped
}

fn period_status_rank(status: ObservationStatus) -> u8 {
    match status {
        ObservationStatus::Normal => 3,
        ObservationStatus::Estimated | ObservationStatus::Revised => 2,
        ObservationStatus::Forecast | ObservationStatus::Provisional => 1,
        ObservationStatus::Imputed | ObservationStatus::Missing | ObservationStatus::Break => 0,
    }
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
    let status = if normalized.contains("revised")
        || normalized.contains("estimated actual")
        || normalized.contains("est.actual")
    {
        ObservationStatus::Estimated
    } else if normalized.contains("estimate")
        || normalized.contains("budget")
        || normalized.contains("forward")
        || normalized.contains("projection")
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

fn normalize_line_item_and_unit(label: &str, fallback_unit: &str) -> (String, String) {
    let lower = label.to_ascii_lowercase();
    let unit = if lower.contains("$ million") || lower.contains("$m") || lower.contains("($m)") {
        "$ million"
    } else if lower.contains("per cent") || lower.contains("percent") || lower.contains('%') {
        "percent"
    } else if lower.contains("number") {
        "number"
    } else {
        fallback_unit
    }
    .to_string();

    (strip_budget_unit_from_label(label), unit)
}

fn strip_budget_unit_from_label(label: &str) -> String {
    let without_units = label
        .replace("($m)", "")
        .replace("($ million)", "")
        .replace("$ million", "")
        .replace("$m", "")
        .replace("(number)", "")
        .replace("(Number)", "");
    clean_cell(&without_units)
}

fn parse_value(value: &str) -> Result<(Option<f64>, ObservationStatus), AdapterError> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || matches!(
            trimmed.to_ascii_lowercase().as_str(),
            "-" | ".." | "*" | "**" | "na" | "n/a" | "nfp"
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
        .replace(['\u{2013}', '\u{2014}'], "-")
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

fn budget_code_id(config: &BudgetConfig, field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!(
            "invalid {} budget {field} code `{value}`: {err}",
            config.jurisdiction
        ))
    })
}

fn artifact_date_for_version(provenance: &BudgetProvenance) -> Result<NaiveDate, AdapterError> {
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

fn dataflow_id(config: &BudgetConfig) -> DataflowId {
    DataflowId::new(config.dataflow_id).expect("static dataflow id is valid")
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

fn default_publications(config: &BudgetConfig) -> Vec<StateBudgetPublication> {
    vec![StateBudgetPublication {
        budget_year: default_budget_year(config),
        paper: config.paper.into(),
        paper_slug: config.paper_slug.into(),
        title: config.target_title.into(),
        source_url: config.default_budget_pdf_url.into(),
        last_updated: Some(config.default_last_updated.into()),
    }]
}

fn default_budget_year(config: &BudgetConfig) -> String {
    config
        .default_budget_pdf_url
        .split(['/', '+', ' '])
        .filter_map(find_fiscal_year)
        .last()
        .unwrap_or_else(|| config.default_last_updated[..4].to_string())
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
            publications: default_publications(&NSW_CONFIG),
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
                dataflows: vec![dataflow_id(&NSW_CONFIG)],
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

/// Builder for [`VicBudgetAdapter`].
#[derive(Debug, Clone)]
pub struct VicBudgetAdapterBuilder {
    publications: Vec<VicBudgetPublication>,
    pdf_base_url: String,
    pdf_client: Option<PdfClient>,
}

impl Default for VicBudgetAdapterBuilder {
    fn default() -> Self {
        Self {
            publications: default_publications(&VIC_CONFIG),
            pdf_base_url: DEFAULT_PDF_BASE_URL.into(),
            pdf_client: None,
        }
    }
}

impl VicBudgetAdapterBuilder {
    /// Override the curated VIC budget publications, usually for fixture tests.
    #[must_use]
    pub fn publications(mut self, publications: Vec<VicBudgetPublication>) -> Self {
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
    pub fn try_build(self) -> Result<VicBudgetAdapter, AdapterError> {
        if self.publications.is_empty() {
            return Err(AdapterError::Validation(
                "at least one VIC budget publication must be configured".into(),
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
        Ok(VicBudgetAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: "Australian state budgets".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(20, Duration::from_secs(60))
                    .expect("static VIC budget rate limit is valid"),
                dataflows: vec![dataflow_id(&VIC_CONFIG)],
            },
            publications,
            pdf_client,
        })
    }

    /// Build the adapter.
    #[must_use]
    pub fn build(self) -> VicBudgetAdapter {
        self.try_build()
            .expect("valid static VIC budget adapter configuration")
    }
}

/// Builder for [`QldBudgetAdapter`].
#[derive(Debug, Clone)]
pub struct QldBudgetAdapterBuilder {
    publications: Vec<QldBudgetPublication>,
    pdf_base_url: String,
    pdf_client: Option<PdfClient>,
}

impl Default for QldBudgetAdapterBuilder {
    fn default() -> Self {
        Self {
            publications: default_publications(&QLD_CONFIG),
            pdf_base_url: DEFAULT_PDF_BASE_URL.into(),
            pdf_client: None,
        }
    }
}

impl QldBudgetAdapterBuilder {
    /// Override the curated QLD budget publications, usually for fixture tests.
    #[must_use]
    pub fn publications(mut self, publications: Vec<QldBudgetPublication>) -> Self {
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
    pub fn try_build(self) -> Result<QldBudgetAdapter, AdapterError> {
        if self.publications.is_empty() {
            return Err(AdapterError::Validation(
                "at least one QLD budget publication must be configured".into(),
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
        Ok(QldBudgetAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: "Australian state budgets".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(20, Duration::from_secs(60))
                    .expect("static QLD budget rate limit is valid"),
                dataflows: vec![dataflow_id(&QLD_CONFIG)],
            },
            publications,
            pdf_client,
        })
    }

    /// Build the adapter.
    #[must_use]
    pub fn build(self) -> QldBudgetAdapter {
        self.try_build()
            .expect("valid static QLD budget adapter configuration")
    }
}

/// Builder for [`StateBudgetsAdapter`].
#[derive(Debug, Clone)]
pub struct StateBudgetsAdapterBuilder {
    pdf_base_url: String,
    pdf_client: Option<PdfClient>,
}

impl Default for StateBudgetsAdapterBuilder {
    fn default() -> Self {
        Self {
            pdf_base_url: DEFAULT_PDF_BASE_URL.into(),
            pdf_client: None,
        }
    }
}

impl StateBudgetsAdapterBuilder {
    /// Override the PDF sidecar base URL for all state budget parsers.
    #[must_use]
    pub fn pdf_base_url(mut self, pdf_base_url: impl Into<String>) -> Self {
        self.pdf_base_url = pdf_base_url.into();
        self
    }

    /// Inject a prebuilt PDF client for all state budget parsers.
    #[must_use]
    pub fn pdf_client(mut self, pdf_client: PdfClient) -> Self {
        self.pdf_client = Some(pdf_client);
        self
    }

    /// Build the adapter, returning validation errors for invalid sidecar URLs.
    pub fn try_build(self) -> Result<StateBudgetsAdapter, AdapterError> {
        let mut nsw = NswBudgetAdapter::builder().pdf_base_url(self.pdf_base_url.clone());
        let mut vic = VicBudgetAdapter::builder().pdf_base_url(self.pdf_base_url.clone());
        let mut qld = QldBudgetAdapter::builder().pdf_base_url(self.pdf_base_url);
        if let Some(pdf_client) = self.pdf_client {
            nsw = nsw.pdf_client(pdf_client.clone());
            vic = vic.pdf_client(pdf_client.clone());
            qld = qld.pdf_client(pdf_client);
        }
        let nsw = nsw.try_build()?;
        let vic = vic.try_build()?;
        let qld = qld.try_build()?;
        Ok(StateBudgetsAdapter::new(nsw, vic, qld))
    }

    /// Build the adapter.
    #[must_use]
    pub fn build(self) -> StateBudgetsAdapter {
        self.try_build()
            .expect("valid static state budgets adapter configuration")
    }
}

/// Stored revision type for NSW budget PDF links.
pub type NswBudgetRevision = UpstreamRevision;

/// Stored revision type for VIC budget PDF links.
pub type VicBudgetRevision = UpstreamRevision;

/// Stored revision type for QLD budget PDF links.
pub type QldBudgetRevision = UpstreamRevision;

/// One state budget PDF publication from the curated adapter inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateBudgetPublication {
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

/// One NSW budget PDF publication from the curated adapter inventory.
pub type NswBudgetPublication = StateBudgetPublication;

/// One VIC budget PDF publication from the curated adapter inventory.
pub type VicBudgetPublication = StateBudgetPublication;

/// One QLD budget PDF publication from the curated adapter inventory.
pub type QldBudgetPublication = StateBudgetPublication;

impl StateBudgetPublication {
    fn revision_key(&self, config: &BudgetConfig) -> String {
        format!(
            "{}:{}:{}",
            config.jurisdiction, self.paper_slug, self.budget_year
        )
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
        config: &BudgetConfig,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
        source_index_url: &str,
    ) -> DiscoveredJob {
        let revision = self.revision(started_at);
        let revision_version = revision.version().to_string();
        let revision_key = self.revision_key(config);
        let artifact_date = self
            .last_updated
            .clone()
            .unwrap_or_else(|| self.budget_year.clone());
        DiscoveredJob {
            id: format!(
                "state-budgets:{}:{}:{}:{}",
                config.jurisdiction.to_ascii_lowercase(),
                self.paper_slug,
                self.budget_year,
                revision_version
            ),
            source_id: source_id(),
            dataflow_id: dataflow_id(config),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("adapter".into(), "state-budgets".into()),
                ("artifact_date".into(), artifact_date),
                ("artifact_format".into(), "pdf".into()),
                ("attribution".into(), config.attribution.into()),
                ("budget_year".into(), self.budget_year.clone()),
                ("cadence".into(), "annual".into()),
                ("dataflow_id".into(), config.dataflow_id.into()),
                ("jurisdiction".into(), config.jurisdiction.into()),
                ("jurisdiction_name".into(), config.jurisdiction_name.into()),
                ("license".into(), config.license_name.into()),
                ("license_url".into(), config.license_url.into()),
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
        let publication = default_publications(&NSW_CONFIG)
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
            budget_provenance_for_fetch(
                &NSW_CONFIG,
                "http://127.0.0.1:3000/bp1-budget-statement-nsw-budget-2025-26.pdf",
                &metadata,
            )
            .is_some()
        );
        assert!(
            budget_provenance_for_parse(
                &NSW_CONFIG,
                "https://www.budget.nsw.gov.au/sites/default/files/2025-06/bp1-budget-statement-nsw-budget-2025-26.pdf",
                &metadata,
            )
            .is_some()
        );
        assert!(
            budget_provenance_for_parse(
                &NSW_CONFIG,
                "https://mirror.example.invalid/bp1-budget-statement-nsw-budget-2025-26.pdf",
                &metadata,
            )
            .is_none()
        );
        assert!(
            budget_provenance_for_fetch(
                &NSW_CONFIG,
                "http://127.0.0.1:3000/bp1-budget-statement-nsw-budget-2025-26.docx",
                &metadata,
            )
            .is_none()
        );

        let mut missing_jurisdiction = metadata;
        missing_jurisdiction.remove("jurisdiction");
        assert!(
            budget_provenance_for_fetch(
                &NSW_CONFIG,
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
        assert_eq!(
            parse_budget_period("2026-27 Projection")
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
