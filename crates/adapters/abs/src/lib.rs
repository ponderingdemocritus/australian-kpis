//! ABS adapter (SDMX-JSON).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{collections::BTreeMap, time::Duration};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterManifest, ArtifactRef, DiscoveredJob, DiscoveryCtx, FetchCtx,
    ObservationStream, ParseCtx, RateLimit, SourceAdapter,
};
use au_kpis_domain::{DataflowId, SourceId};
use au_kpis_error::CoreError;
use futures::stream;
use serde::Deserialize;

const DEFAULT_BASE_URL: &str = "https://data.api.abs.gov.au/rest";
const STRUCTURE_JSON_ACCEPT: &str = "application/vnd.sdmx.structure+json";
const CPI_DATAFLOW_ID: &str = "CPI";
const USER_AGENT: &str = concat!("au-kpis-adapter-abs/", env!("CARGO_PKG_VERSION"));

/// ABS SDMX adapter.
#[derive(Debug, Clone)]
pub struct AbsAdapter {
    manifest: AdapterManifest,
    base_url: String,
    known_revisions: BTreeMap<String, DataflowRevision>,
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
        Ok(serde_json::from_str::<AbsDataflowMessage>(body)
            .map_err(CoreError::from)?
            .data
            .dataflows)
    }

    /// Diff current ABS dataflows against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs(
        current: &[AbsDataflow],
        known_revisions: &BTreeMap<String, DataflowRevision>,
    ) -> Vec<DiscoveredJob> {
        current
            .iter()
            .filter(|flow| flow.id == CPI_DATAFLOW_ID)
            .filter(|flow| {
                known_revisions
                    .get(&flow.id)
                    .is_none_or(|known| known != &flow.revision())
            })
            .map(AbsDataflow::to_discovered_job)
            .collect()
    }

    fn dataflow_url(&self) -> String {
        format!("{}/dataflow/ABS/CPI?detail=allstubs", self.base_url)
    }
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
        let message = response.json::<AbsDataflowMessage>().await?;
        Ok(Self::discoverable_jobs(
            &message.data.dataflows,
            &self.known_revisions,
        ))
    }

    async fn fetch(
        &self,
        _job: DiscoveredJob,
        _ctx: &FetchCtx,
    ) -> Result<ArtifactRef, AdapterError> {
        Err(AdapterError::Validation(
            "ABS fetch is implemented in issue #25".to_string(),
        ))
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
    known_revisions: BTreeMap<String, DataflowRevision>,
}

impl Default for AbsAdapterBuilder {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            known_revisions: BTreeMap::new(),
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

    /// Register the stored upstream revision for a dataflow.
    #[must_use]
    pub fn known_revision(
        mut self,
        dataflow_id: impl Into<String>,
        revision: DataflowRevision,
    ) -> Self {
        self.known_revisions.insert(dataflow_id.into(), revision);
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
            known_revisions: self.known_revisions,
        }
    }
}

/// Stored upstream dataflow revision used for discovery diffing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataflowRevision {
    version: String,
    last_updated: Option<String>,
}

impl DataflowRevision {
    /// Construct a stored revision.
    #[must_use]
    pub fn new(version: impl Into<String>, last_updated: Option<impl Into<String>>) -> Self {
        Self {
            version: version.into(),
            last_updated: last_updated.map(Into::into),
        }
    }

    /// Upstream dataflow version.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Upstream update timestamp when exposed by ABS.
    #[must_use]
    pub fn last_updated(&self) -> Option<&str> {
        self.last_updated.as_deref()
    }
}

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
    fn revision(&self) -> DataflowRevision {
        DataflowRevision::new(self.version.clone(), self.last_updated.clone())
    }

    fn to_discovered_job(&self) -> DiscoveredJob {
        let mut metadata = BTreeMap::from([
            ("abs_dataflow_id".to_string(), self.id.clone()),
            ("agency_id".to_string(), self.agency_id.clone()),
            ("version".to_string(), self.version.clone()),
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

#[derive(Debug, Deserialize)]
struct AbsDataflowMessage {
    data: AbsDataflowData,
}

#[derive(Debug, Deserialize)]
struct AbsDataflowData {
    dataflows: Vec<AbsDataflow>,
}

#[derive(Debug, Deserialize)]
struct RawAbsDataflow {
    id: String,
    #[serde(rename = "agencyID", default)]
    agency_id: Option<String>,
    version: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    names: BTreeMap<String, String>,
    #[serde(default, alias = "lastUpdated", alias = "last_updated")]
    updated: Option<String>,
    links: Vec<AbsLink>,
}

#[derive(Debug, Deserialize)]
struct AbsLink {
    href: String,
}

impl<'de> Deserialize<'de> for AbsDataflow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = RawAbsDataflow::deserialize(deserializer)?;
        let name = raw
            .name
            .or_else(|| raw.names.get("en").cloned())
            .unwrap_or_else(|| raw.id.clone());
        let agency_id = raw.agency_id.unwrap_or_else(|| "ABS".to_string());
        let dataflow_url = raw
            .links
            .first()
            .map(|link| link.href.clone())
            .ok_or_else(|| serde::de::Error::custom("ABS dataflow is missing links"))?;
        let source_url =
            data_url_from_dataflow_url(&dataflow_url, &agency_id, &raw.id, &raw.version);

        Ok(Self {
            id: raw.id,
            agency_id,
            version: raw.version,
            name,
            last_updated: raw.updated,
            source_url,
            dataflow_url,
        })
    }
}

fn data_url_from_dataflow_url(
    dataflow_url: &str,
    agency_id: &str,
    dataflow_id: &str,
    version: &str,
) -> String {
    if let Some((base, _)) = dataflow_url.split_once("/dataflow/") {
        return format!(
            "{base}/data/{agency_id},{dataflow_id},{version}/all?dimensionAtObservation=TIME_PERIOD"
        );
    }
    format!(
        "{DEFAULT_BASE_URL}/data/{agency_id},{dataflow_id},{version}/all?dimensionAtObservation=TIME_PERIOD"
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
