//! ABS adapter (SDMX-JSON).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterManifest, ArtifactRef, DiscoveredJob, DiscoveryCtx, FetchCtx,
    ObservationStream, ParseCtx, RateLimit, SourceAdapter, UpstreamRevision,
    capture_response_headers, retry_after_delta,
};
use au_kpis_domain::{Artifact, DataflowId, SourceId};
use au_kpis_error::CoreError;
use au_kpis_storage::StorageKey;
use chrono::Utc;
use futures::{StreamExt, stream};
use serde::Deserialize;

const DEFAULT_BASE_URL: &str = "https://data.api.abs.gov.au/rest";
const STRUCTURE_JSON_ACCEPT: &str = "application/vnd.sdmx.structure+json";
const DATA_JSON_ACCEPT: &str = "application/vnd.sdmx.data+json";
const CPI_DATAFLOW_ID: &str = "CPI";
const USER_AGENT: &str = concat!("au-kpis-adapter-abs/", env!("CARGO_PKG_VERSION"));

/// ABS SDMX adapter.
#[derive(Debug, Clone)]
pub struct AbsAdapter {
    manifest: AdapterManifest,
    base_url: String,
}

impl Default for AbsAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl AbsAdapter {
    /// Start building an ABS adapter.
    #[must_use]
    pub fn builder() -> AbsAdapterBuilder {
        AbsAdapterBuilder::default()
    }

    /// Parse an ABS SDMX-JSON dataflow listing.
    pub fn parse_dataflow_listing(body: &str) -> Result<Vec<AbsDataflow>, AdapterError> {
        parse_dataflow_listing_with_base(body, DEFAULT_BASE_URL)
    }

    /// Diff current ABS dataflows against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs(
        current: &[AbsDataflow],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
    ) -> Vec<DiscoveredJob> {
        latest_dataflow_revisions(current)
            .into_values()
            .filter(|flow| {
                known_revisions
                    .get(&flow.revision_key())
                    .is_none_or(|known| known != &flow.revision())
            })
            .map(AbsDataflow::to_discovered_job)
            .collect()
    }

    /// Convert current ABS dataflows into discovery jobs without persisted diff state.
    #[must_use]
    pub fn current_jobs(current: &[AbsDataflow]) -> Vec<DiscoveredJob> {
        latest_dataflow_revisions(current)
            .into_values()
            .map(AbsDataflow::to_discovered_job)
            .collect()
    }

    fn validated_fetch_url(&self, job: &DiscoveredJob) -> Result<String, AdapterError> {
        let agency_id = required_metadata(job, "agency_id")?;
        let dataflow_id = required_metadata(job, "abs_dataflow_id")?;
        let version = required_metadata(job, "version")?;
        if job.dataflow_id.as_str() != "abs.cpi" || agency_id != "ABS" || dataflow_id != "CPI" {
            return Err(AdapterError::Validation(format!(
                "ABS fetch metadata `{agency_id}:{dataflow_id}` does not match dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        let expected = data_url_from_base(&self.base_url, agency_id, dataflow_id, version);

        if job.source_url != expected {
            return Err(AdapterError::Validation(format!(
                "ABS fetch URL `{}` does not match canonical URL `{expected}`",
                job.source_url
            )));
        }

        Ok(expected)
    }

    fn dataflow_url(&self) -> String {
        format!("{}/dataflow/ABS/CPI?detail=allstubs", self.base_url)
    }
}

fn required_metadata<'a>(job: &'a DiscoveredJob, key: &str) -> Result<&'a str, AdapterError> {
    job.metadata
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| AdapterError::Validation(format!("ABS fetch job is missing `{key}`")))
}

#[async_trait]
impl SourceAdapter for AbsAdapter {
    fn id(&self) -> &'static str {
        "abs"
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
                    .get(self.dataflow_url())
                    .header("user-agent", USER_AGENT)
                    .header("accept", STRUCTURE_JSON_ACCEPT),
            )
            .await?
            .error_for_status()?;
        let body = response.text().await?;
        let dataflows = parse_dataflow_listing_with_base(&body, &self.base_url)?;
        Ok(Self::discoverable_jobs(&dataflows, ctx.known_revisions()))
    }

    async fn fetch(&self, job: DiscoveredJob, ctx: &FetchCtx) -> Result<ArtifactRef, AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "ABS fetch received job for source `{}`",
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
                "ABS fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }

        let fetch_url = self.validated_fetch_url(&job)?;
        let response = ctx
            .http
            .execute(
                ctx.http
                    .raw()
                    .get(&fetch_url)
                    .header("user-agent", USER_AGENT)
                    .header("accept", DATA_JSON_ACCEPT),
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
            .map_or_else(|| DATA_JSON_ACCEPT.to_string(), str::to_string);

        let staged = ctx
            .blob_store
            .stage_artifact_stream(response.bytes_stream().boxed())
            .await?;
        let id = staged.id();
        let storage_key = format!("artifacts/{}", id.to_hex());
        let fetched_at = Utc::now();
        let artifact = Artifact {
            id,
            source_id: job.source_id,
            source_url: fetch_url,
            content_type,
            response_headers,
            storage_key: storage_key.clone(),
            size_bytes: staged.size_bytes(),
            fetched_at,
        };

        let mut needs_canonical_repair = false;
        if let Some(existing) = ctx.get_artifact(id).await? {
            let existing_key = StorageKey::from_persisted(existing.storage_key.clone());
            let durable_matches = if existing.storage_key == storage_key {
                ctx.blob_store
                    .exists_with_size(&existing_key, staged.size_bytes())
                    .await?
            } else {
                ctx.blob_store
                    .matches_artifact_id(&existing_key, id)
                    .await?
            };
            if durable_matches {
                ctx.blob_store.discard_staged_artifact(&staged).await?;
                let duplicate = Artifact {
                    storage_key: existing.storage_key,
                    ..artifact
                };
                return ctx.persist_artifact(duplicate).await;
            }
            needs_canonical_repair = true;
        }

        if needs_canonical_repair {
            ctx.blob_store.replace_staged_artifact(&staged).await?;
        } else {
            ctx.blob_store.commit_staged_artifact(&staged).await?;
        }

        let reference = ctx.persist_artifact(artifact.clone()).await?;
        if reference.storage_key == artifact.storage_key {
            return Ok(reference);
        }

        let durable_key = StorageKey::from_persisted(reference.storage_key.clone());
        if ctx.blob_store.matches_artifact_id(&durable_key, id).await? {
            return Ok(reference);
        }

        ctx.repair_artifact_storage_key(artifact, &reference.storage_key)
            .await
    }

    fn parse<'a>(&'a self, _artifact: ArtifactRef, _ctx: &'a ParseCtx) -> ObservationStream<'a> {
        Box::pin(stream::once(async {
            Err(AdapterError::Validation(
                "ABS parse is implemented in issue #26".to_string(),
            ))
        }))
    }
}

/// Builder for [`AbsAdapter`].
#[derive(Debug, Clone)]
pub struct AbsAdapterBuilder {
    base_url: String,
}

impl Default for AbsAdapterBuilder {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
        }
    }
}

impl AbsAdapterBuilder {
    /// Override the ABS REST base URL. Intended for deterministic tests.
    #[must_use]
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into().trim_end_matches('/').to_string();
        self
    }

    /// Build the adapter.
    #[must_use]
    pub fn build(self) -> AbsAdapter {
        AbsAdapter {
            manifest: AdapterManifest {
                source_id: SourceId::new("abs").expect("static source id is valid"),
                name: "Australian Bureau of Statistics".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                rate_limit: RateLimit::new(60, Duration::from_secs(60))
                    .expect("static rate limit is valid"),
                dataflows: vec![DataflowId::new("abs.cpi").expect("static dataflow id is valid")],
            },
            base_url: self.base_url,
        }
    }
}

/// Stored upstream dataflow revision used for discovery diffing.
pub type DataflowRevision = UpstreamRevision;

/// ABS dataflow metadata relevant to discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbsDataflow {
    /// ABS dataflow id, e.g. `CPI`.
    pub id: String,
    /// Maintaining agency, usually `ABS`.
    pub agency_id: String,
    /// ABS dataflow version.
    pub version: String,
    /// Human-readable name.
    pub name: String,
    /// Upstream update timestamp when present.
    pub last_updated: Option<String>,
    /// Canonical SDMX-JSON data URL to fetch for this dataflow.
    pub source_url: String,
    /// Canonical ABS dataflow metadata URL.
    pub dataflow_url: String,
}

impl AbsDataflow {
    fn revision(&self) -> UpstreamRevision {
        UpstreamRevision::new(self.version.clone(), self.last_updated.clone())
    }

    fn revision_key(&self) -> String {
        format!("{}:{}", self.agency_id, self.id)
    }

    fn to_discovered_job(&self) -> DiscoveredJob {
        let mut metadata = BTreeMap::from([
            ("abs_dataflow_id".to_string(), self.id.clone()),
            ("agency_id".to_string(), self.agency_id.clone()),
            ("version".to_string(), self.version.clone()),
            ("revision_key".to_string(), self.revision_key()),
            ("name".to_string(), self.name.clone()),
            ("dataflow_url".to_string(), self.dataflow_url.clone()),
        ]);
        if let Some(last_updated) = &self.last_updated {
            metadata.insert("last_updated".to_string(), last_updated.clone());
        }

        DiscoveredJob {
            id: format!(
                "abs:{}:{}:{}",
                self.id,
                self.version,
                revision_token(self.last_updated.as_deref())
            ),
            source_id: SourceId::new("abs").expect("static source id is valid"),
            dataflow_id: DataflowId::new("abs.cpi").expect("static dataflow id is valid"),
            source_url: self.source_url.clone(),
            metadata,
        }
    }
}

fn parse_dataflow_listing_with_base(
    body: &str,
    source_base_url: &str,
) -> Result<Vec<AbsDataflow>, AdapterError> {
    let message = serde_json::from_str::<RawAbsDataflowMessage>(body).map_err(CoreError::from)?;
    let mut dataflows = Vec::new();
    for raw in message.data.dataflows {
        dataflows.push(AbsDataflow::try_from_raw(raw, source_base_url)?);
    }
    Ok(dataflows)
}

fn latest_dataflow_revisions(current: &[AbsDataflow]) -> BTreeMap<String, &AbsDataflow> {
    let mut latest = BTreeMap::new();
    for flow in current.iter().filter(|flow| flow.id == CPI_DATAFLOW_ID) {
        latest
            .entry(flow.revision_key())
            .and_modify(|stored: &mut &AbsDataflow| {
                if flow.is_newer_revision_than(stored) {
                    *stored = flow;
                }
            })
            .or_insert(flow);
    }
    latest
}

#[derive(Debug, Deserialize)]
struct RawAbsDataflowMessage {
    data: RawAbsDataflowData,
}

#[derive(Debug, Deserialize)]
struct RawAbsDataflowData {
    dataflows: Vec<RawAbsDataflow>,
}

#[derive(Debug, Deserialize)]
struct RawAbsDataflow {
    id: Option<String>,
    #[serde(rename = "agencyID", default)]
    agency_id: Option<String>,
    version: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    names: BTreeMap<String, String>,
    #[serde(default, alias = "lastUpdated", alias = "last_updated")]
    updated: Option<String>,
    #[serde(default)]
    links: Vec<AbsLink>,
}

#[derive(Debug, Deserialize)]
struct AbsLink {
    href: String,
    #[serde(default)]
    rel: Option<String>,
}

impl AbsDataflow {
    fn try_from_raw(raw: RawAbsDataflow, source_base_url: &str) -> Result<Self, AdapterError> {
        let id = raw
            .id
            .ok_or_else(|| AdapterError::FormatDrift("ABS dataflow is missing id".to_string()))?;
        let version = raw.version.ok_or_else(|| {
            AdapterError::FormatDrift(format!("ABS dataflow {id} is missing version"))
        })?;
        let name = raw
            .name
            .or_else(|| raw.names.get("en").cloned())
            .unwrap_or_else(|| id.clone());
        let agency_id = raw.agency_id.unwrap_or_else(|| "ABS".to_string());
        let dataflow_url = canonical_dataflow_url(&raw.links, &agency_id, &id, &version)
            .ok_or_else(|| {
                AdapterError::FormatDrift(format!("ABS dataflow {id} is missing canonical link"))
            })?;
        let source_url = data_url_from_base(source_base_url, &agency_id, &id, &version);

        Ok(Self {
            id,
            agency_id,
            version,
            name,
            last_updated: raw.updated,
            source_url,
            dataflow_url,
        })
    }

    fn is_newer_revision_than(&self, other: &Self) -> bool {
        version_cmp_key(&self.version) > version_cmp_key(&other.version)
            || (self.version == other.version && self.last_updated > other.last_updated)
    }
}

fn version_cmp_key(version: &str) -> Vec<u64> {
    version
        .split('.')
        .map(|part| part.parse::<u64>().unwrap_or(0))
        .collect()
}

fn canonical_dataflow_url(
    links: &[AbsLink],
    agency_id: &str,
    dataflow_id: &str,
    version: &str,
) -> Option<String> {
    let expected_suffix = format!("/dataflow/{agency_id}/{dataflow_id}/{version}");
    links
        .iter()
        .filter(|link| is_supported_dataflow_rel(link.rel.as_deref()))
        .filter(|link| {
            let href = link
                .href
                .split_once('?')
                .map_or(link.href.as_str(), |(href, _)| href);
            href.ends_with(&expected_suffix)
        })
        .min_by_key(|link| dataflow_link_rank(link.rel.as_deref()))
        .map(|link| link.href.clone())
}

fn is_supported_dataflow_rel(rel: Option<&str>) -> bool {
    rel.is_none_or(|rel| {
        matches!(
            rel.to_ascii_lowercase().as_str(),
            "self" | "canonical" | "dataflow" | "external"
        )
    })
}

fn dataflow_link_rank(rel: Option<&str>) -> u8 {
    match rel.map(str::to_ascii_lowercase).as_deref() {
        Some("self" | "canonical" | "dataflow") => 0,
        Some("external") => 1,
        _ => 2,
    }
}

fn data_url_from_base(
    source_base_url: &str,
    agency_id: &str,
    dataflow_id: &str,
    version: &str,
) -> String {
    let base = source_base_url.trim_end_matches('/');
    format!(
        "{base}/data/{agency_id},{dataflow_id},{version}/all?dimensionAtObservation=TIME_PERIOD"
    )
}

fn revision_token(last_updated: Option<&str>) -> String {
    last_updated
        .unwrap_or("unknown")
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect()
}
