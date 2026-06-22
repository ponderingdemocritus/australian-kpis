//! ABS adapter (SDMX-JSON).

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{
    collections::BTreeMap,
    io::{self, Read},
    sync::{Arc, Mutex},
    time::Duration,
};

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
use au_kpis_error::CoreError;
use au_kpis_storage::{BlobStore, StorageError, StorageKey};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use futures::{StreamExt, stream};
use serde::{
    Deserialize,
    de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
};
use tokio_util::sync::CancellationToken;

const DEFAULT_BASE_URL: &str = "https://data.api.abs.gov.au/rest";
const STRUCTURE_JSON_ACCEPT: &str = "application/vnd.sdmx.structure+json";
const DATA_JSON_ACCEPT: &str = "application/vnd.sdmx.data+json;version=1.0.0-wd";
const CPI_DATAFLOW_ID: &str = "CPI";
const CPI_CANONICAL_DATAFLOW_ID: &str = "abs.cpi";
const BUILDING_APPROVALS_CANONICAL_DATAFLOW_ID: &str = "abs.building_approvals";
const BUILDING_ACTIVITY_CANONICAL_DATAFLOW_ID: &str = "abs.building_activity";
const DWELLING_COMPLETION_TIMES_CANONICAL_DATAFLOW_ID: &str = "abs.dwelling_completion_times";
const BUILDING_APPROVALS_DATAFLOW_SLUG: &str = "building-approvals";
const BUILDING_ACTIVITY_DATAFLOW_SLUG: &str = "building-activity";
const DWELLING_COMPLETION_TIMES_DATAFLOW_SLUG: &str = "dwelling-completion-times";
const BUILDING_APPROVALS_MEASURE_ID: &str = "dwellings_approved";
const BUILDING_ACTIVITY_COMMENCED_MEASURE_ID: &str = "dwellings_commenced";
const BUILDING_ACTIVITY_COMPLETED_MEASURE_ID: &str = "dwellings_completed";
const DWELLING_COMPLETION_TIMES_MEASURE_ID: &str = "average_completion_months";
const DEFAULT_BUILDING_APPROVALS_RELEASE_URL: &str = "https://www.abs.gov.au/statistics/industry/building-and-construction/building-approvals-australia/latest-release";
const DEFAULT_BUILDING_ACTIVITY_RELEASE_URL: &str = "https://www.abs.gov.au/statistics/industry/building-and-construction/building-activity-australia/latest-release";
const DEFAULT_DWELLING_COMPLETION_TIMES_URL: &str =
    "https://www.abs.gov.au/articles/average-dwelling-completion-times";
const ABS_ATTRIBUTION: &str = "Source: Australian Bureau of Statistics";
const USER_AGENT: &str = concat!("au-kpis-adapter-abs/", env!("CARGO_PKG_VERSION"));

/// ABS SDMX adapter.
#[derive(Debug, Clone)]
pub struct AbsAdapter {
    manifest: AdapterManifest,
    base_url: String,
    building_approvals_release_url: String,
    building_activity_release_url: String,
    dwelling_completion_times_url: String,
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
        Self::discoverable_jobs_with_trace_parent(current, known_revisions, None)
    }

    fn discoverable_jobs_with_trace_parent(
        current: &[AbsDataflow],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        latest_dataflow_revisions(current)
            .into_values()
            .filter(|flow| {
                known_revisions
                    .get(&flow.revision_key())
                    .is_none_or(|known| known != &flow.revision())
            })
            .map(|flow| flow.to_discovered_job(trace_parent))
            .collect()
    }

    /// Convert current ABS dataflows into discovery jobs without persisted diff state.
    #[must_use]
    pub fn current_jobs(current: &[AbsDataflow]) -> Vec<DiscoveredJob> {
        latest_dataflow_revisions(current)
            .into_values()
            .map(|flow| flow.to_discovered_job(None))
            .collect()
    }

    fn validated_fetch_url(&self, job: &DiscoveredJob) -> Result<String, AdapterError> {
        let agency_id = required_metadata(job, "agency_id")?;
        let dataflow_id = required_metadata(job, "abs_dataflow_id")?;
        let version = required_metadata(job, "version")?;
        if job.dataflow_id.as_str() != CPI_CANONICAL_DATAFLOW_ID
            || agency_id != "ABS"
            || dataflow_id != "CPI"
        {
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

    fn validated_fetch_kind(&self, job: &DiscoveredJob) -> Result<AbsFetchKind, AdapterError> {
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
        if job.dataflow_id.as_str() == CPI_CANONICAL_DATAFLOW_ID {
            return Ok(AbsFetchKind::Cpi {
                url: self.validated_fetch_url(job)?,
            });
        }
        if job.dataflow_id.as_str() == BUILDING_APPROVALS_CANONICAL_DATAFLOW_ID {
            if !is_building_approvals_release_url(&job.source_url) {
                return Err(AdapterError::Validation(format!(
                    "ABS fetch URL `{}` is not a Building Approvals release artifact",
                    job.source_url
                )));
            }
            return Ok(AbsFetchKind::BuildingApprovals {
                url: job.source_url.clone(),
            });
        }
        if job.dataflow_id.as_str() == BUILDING_ACTIVITY_CANONICAL_DATAFLOW_ID {
            if !is_building_activity_release_url(&job.source_url) {
                return Err(AdapterError::Validation(format!(
                    "ABS fetch URL `{}` is not a Building Activity release artifact",
                    job.source_url
                )));
            }
            return Ok(AbsFetchKind::BuildingActivity {
                url: job.source_url.clone(),
            });
        }
        if job.dataflow_id.as_str() == DWELLING_COMPLETION_TIMES_CANONICAL_DATAFLOW_ID {
            if !is_dwelling_completion_times_url(&job.source_url) {
                return Err(AdapterError::Validation(format!(
                    "ABS fetch URL `{}` is not a dwelling completion times article artifact",
                    job.source_url
                )));
            }
            return Ok(AbsFetchKind::DwellingCompletionTimes {
                url: job.source_url.clone(),
            });
        }
        Err(AdapterError::Validation(format!(
            "ABS fetch received unsupported dataflow `{}`",
            job.dataflow_id.as_str()
        )))
    }

    fn dataflow_url(&self) -> String {
        format!("{}/dataflow/ABS/CPI?detail=allstubs", self.base_url)
    }

    fn building_approvals_release_url(&self) -> &str {
        &self.building_approvals_release_url
    }

    fn building_activity_release_url(&self) -> &str {
        &self.building_activity_release_url
    }

    fn dwelling_completion_times_url(&self) -> &str {
        &self.dwelling_completion_times_url
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AbsFetchKind {
    Cpi { url: String },
    BuildingApprovals { url: String },
    BuildingActivity { url: String },
    DwellingCompletionTimes { url: String },
}

impl AbsFetchKind {
    fn url(&self) -> &str {
        match self {
            Self::Cpi { url }
            | Self::BuildingApprovals { url }
            | Self::BuildingActivity { url }
            | Self::DwellingCompletionTimes { url } => url,
        }
    }

    fn accept(&self) -> &'static str {
        match self {
            Self::Cpi { .. } => DATA_JSON_ACCEPT,
            Self::BuildingApprovals { .. }
            | Self::BuildingActivity { .. }
            | Self::DwellingCompletionTimes { .. } => "text/html,application/xhtml+xml",
        }
    }

    fn default_content_type(&self) -> &'static str {
        match self {
            Self::Cpi { .. } => DATA_JSON_ACCEPT,
            Self::BuildingApprovals { .. }
            | Self::BuildingActivity { .. }
            | Self::DwellingCompletionTimes { .. } => "text/html",
        }
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

    fn source_metadata(&self) -> Option<Source> {
        Some(Source {
            id: SourceId::new("abs").expect("static source id is valid"),
            name: "Australian Bureau of Statistics".into(),
            homepage: "https://www.abs.gov.au".into(),
            description: Some("Australia's national statistical agency.".into()),
        })
    }

    fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![
            Dataflow {
                id: DataflowId::new(CPI_CANONICAL_DATAFLOW_ID)
                    .expect("static dataflow id is valid"),
                source_id: SourceId::new("abs").expect("static source id is valid"),
                name: "Consumer Price Index".into(),
                description: Some("ABS CPI observations from SDMX-JSON artifacts.".into()),
                dimensions: vec![
                    DimensionId::new("region").expect("static dimension id is valid"),
                    DimensionId::new("measure").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new("index").expect("static measure id is valid")],
                frequency: Frequency::Quarterly,
                license: License::CcBy40,
                attribution: ABS_ATTRIBUTION.into(),
                source_url: format!("{}/dataflow/ABS/CPI?detail=allstubs", DEFAULT_BASE_URL),
            },
            Dataflow {
                id: DataflowId::new(BUILDING_APPROVALS_CANONICAL_DATAFLOW_ID)
                    .expect("static dataflow id is valid"),
                source_id: SourceId::new("abs").expect("static source id is valid"),
                name: "Building Approvals".into(),
                description: Some(
                    "Monthly national dwelling approvals from the ABS Building Approvals release."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("region").expect("static dimension id is valid"),
                    DimensionId::new("measure").expect("static dimension id is valid"),
                ],
                measures: vec![
                    MeasureId::new(BUILDING_APPROVALS_MEASURE_ID)
                        .expect("static measure id is valid"),
                ],
                frequency: Frequency::Monthly,
                license: License::CcBy40,
                attribution: ABS_ATTRIBUTION.into(),
                source_url: DEFAULT_BUILDING_APPROVALS_RELEASE_URL.into(),
            },
            Dataflow {
                id: DataflowId::new(BUILDING_ACTIVITY_CANONICAL_DATAFLOW_ID)
                    .expect("static dataflow id is valid"),
                source_id: SourceId::new("abs").expect("static source id is valid"),
                name: "Building Activity".into(),
                description: Some(
                    "Quarterly national dwelling commencement and completion observations from the ABS Building Activity release."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("region").expect("static dimension id is valid"),
                    DimensionId::new("measure").expect("static dimension id is valid"),
                ],
                measures: vec![
                    MeasureId::new(BUILDING_ACTIVITY_COMMENCED_MEASURE_ID)
                        .expect("static measure id is valid"),
                    MeasureId::new(BUILDING_ACTIVITY_COMPLETED_MEASURE_ID)
                        .expect("static measure id is valid"),
                ],
                frequency: Frequency::Quarterly,
                license: License::CcBy40,
                attribution: ABS_ATTRIBUTION.into(),
                source_url: DEFAULT_BUILDING_ACTIVITY_RELEASE_URL.into(),
            },
            Dataflow {
                id: DataflowId::new(DWELLING_COMPLETION_TIMES_CANONICAL_DATAFLOW_ID)
                    .expect("static dataflow id is valid"),
                source_id: SourceId::new("abs").expect("static source id is valid"),
                name: "Average dwelling completion times".into(),
                description: Some(
                    "Annual Australian average dwelling completion times by dwelling type from the ABS completion-times article."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("region").expect("static dimension id is valid"),
                    DimensionId::new("measure").expect("static dimension id is valid"),
                    DimensionId::new("dwelling_type").expect("static dimension id is valid"),
                ],
                measures: vec![
                    MeasureId::new(DWELLING_COMPLETION_TIMES_MEASURE_ID)
                        .expect("static measure id is valid"),
                ],
                frequency: Frequency::Annual,
                license: License::CcBy40,
                attribution: ABS_ATTRIBUTION.into(),
                source_url: DEFAULT_DWELLING_COMPLETION_TIMES_URL.into(),
            },
        ]
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        let requested = ctx.requested_dataflow_id();
        let building_dataflow_id = building_approvals_dataflow_id();
        let building_activity_dataflow_id = building_activity_dataflow_id();
        let dwelling_completion_times_dataflow_id = dwelling_completion_times_dataflow_id();
        let cpi_dataflow_id = cpi_dataflow_id();
        if requested == Some(&building_dataflow_id) {
            let response = ctx
                .http
                .execute(
                    ctx.http
                        .raw()
                        .get(self.building_approvals_release_url())
                        .header("user-agent", USER_AGENT)
                        .header("accept", "text/html,application/xhtml+xml"),
                )
                .await?
                .error_for_status()?;
            let body = response.text().await?;
            let release =
                parse_building_approvals_release(&body, self.building_approvals_release_url())?;
            let job = release.to_discovered_job(ctx.trace_parent());
            let should_emit = ctx
                .known_revisions()
                .get(&job.metadata["revision_key"])
                .is_none_or(|known| known != &release.revision());
            return Ok(if should_emit { vec![job] } else { Vec::new() });
        }
        if requested == Some(&building_activity_dataflow_id) {
            let response = ctx
                .http
                .execute(
                    ctx.http
                        .raw()
                        .get(self.building_activity_release_url())
                        .header("user-agent", USER_AGENT)
                        .header("accept", "text/html,application/xhtml+xml"),
                )
                .await?
                .error_for_status()?;
            let body = response.text().await?;
            let release =
                parse_building_activity_release(&body, self.building_activity_release_url())?;
            let job = release.to_discovered_job(ctx.trace_parent());
            let should_emit = ctx
                .known_revisions()
                .get(&job.metadata["revision_key"])
                .is_none_or(|known| known != &release.revision());
            return Ok(if should_emit { vec![job] } else { Vec::new() });
        }
        if requested == Some(&dwelling_completion_times_dataflow_id) {
            let response = ctx
                .http
                .execute(
                    ctx.http
                        .raw()
                        .get(self.dwelling_completion_times_url())
                        .header("user-agent", USER_AGENT)
                        .header("accept", "text/html,application/xhtml+xml"),
                )
                .await?
                .error_for_status()?;
            let body = response.text().await?;
            let article = parse_dwelling_completion_times_article(
                &body,
                self.dwelling_completion_times_url(),
            )?;
            let job = article.to_discovered_job(ctx.trace_parent());
            let should_emit = ctx
                .known_revisions()
                .get(&job.metadata["revision_key"])
                .is_none_or(|known| known != &article.revision());
            return Ok(if should_emit { vec![job] } else { Vec::new() });
        }
        if requested.is_some_and(|requested| requested != &cpi_dataflow_id) {
            return Ok(Vec::new());
        }
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
        Ok(Self::discoverable_jobs_with_trace_parent(
            &dataflows,
            ctx.known_revisions(),
            ctx.trace_parent(),
        ))
    }

    async fn fetch(&self, job: DiscoveredJob, ctx: &FetchCtx) -> Result<ArtifactRef, AdapterError> {
        let fetch_kind = self.validated_fetch_kind(&job)?;
        let fetch_url = fetch_kind.url().to_string();
        let response = ctx
            .http
            .execute(
                ctx.http
                    .raw_artifact()
                    .get(&fetch_url)
                    .header("user-agent", USER_AGENT)
                    .header("accept", fetch_kind.accept()),
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
            .map_or_else(
                || fetch_kind.default_content_type().to_string(),
                str::to_string,
            );

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

        let existing = match ctx.get_artifact(id).await {
            Ok(existing) => existing,
            Err(err) => {
                ctx.blob_store.discard_staged_artifact(&staged).await?;
                return Err(err);
            }
        };

        let mut needs_canonical_repair = false;
        if let Some(existing) = existing {
            let existing_key = StorageKey::from_persisted(existing.storage_key.clone());
            if existing.storage_key == storage_key {
                ctx.blob_store.commit_staged_artifact(&staged).await?;
                let expected_storage_key = existing.storage_key.clone();
                let duplicate = Artifact {
                    storage_key: existing.storage_key,
                    ..artifact
                };
                return persist_expected_artifact(
                    ctx,
                    duplicate,
                    &expected_storage_key,
                    Some(&storage_key),
                )
                .await;
            }
            let existing_key_matches =
                match ctx.blob_store.matches_artifact_id(&existing_key, id).await {
                    Ok(matches) => matches,
                    Err(err) => {
                        ctx.blob_store.discard_staged_artifact(&staged).await?;
                        return Err(err.into());
                    }
                };
            if existing_key_matches {
                ctx.blob_store.discard_staged_artifact(&staged).await?;
                let expected_storage_key = existing.storage_key.clone();
                let duplicate = Artifact {
                    storage_key: existing.storage_key,
                    ..artifact
                };
                return persist_expected_artifact(ctx, duplicate, &expected_storage_key, None)
                    .await;
            }
            needs_canonical_repair = true;
        }

        if needs_canonical_repair {
            ctx.blob_store.replace_staged_artifact(&staged).await?;
        } else {
            ctx.blob_store.commit_staged_artifact(&staged).await?;
        }

        persist_expected_artifact(ctx, artifact, &storage_key, Some(&storage_key)).await
    }

    fn parse<'a>(&'a self, artifact: ArtifactRef, ctx: &'a ParseCtx) -> ObservationStream<'a> {
        parse_artifact_stream(artifact, ctx)
    }
}

fn parse_artifact_stream(artifact: ArtifactRef, ctx: &ParseCtx) -> ObservationStream<'_> {
    match validate_parse_artifact(&artifact, ctx.expected_dataflow_id()) {
        Ok(AbsParseKind::Cpi) => parse_sdmx_artifact_stream(artifact, ctx),
        Ok(AbsParseKind::BuildingApprovals) => {
            parse_building_approvals_artifact_stream(artifact, ctx)
        }
        Ok(AbsParseKind::BuildingActivity) => {
            parse_building_activity_artifact_stream(artifact, ctx)
        }
        Ok(AbsParseKind::DwellingCompletionTimes) => {
            parse_dwelling_completion_times_artifact_stream(artifact, ctx)
        }
        Err(err) => Box::pin(stream::once(async move { Err(err) })),
    }
}

fn parse_sdmx_artifact_stream(artifact: ArtifactRef, ctx: &ParseCtx) -> ObservationStream<'_> {
    let blob_store = ctx.blob_store.clone();
    let started_at = ctx.started_at;
    let cancellation = ctx.cancellation().clone();
    let (row_tx, row_rx) = tokio::sync::mpsc::channel(4_096);

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

        let parse_tx = row_tx.clone();
        let artifact_for_full_parse = artifact.clone();
        let cancellation_for_full_parse = cancellation.clone();
        let result = match parse_blob_stream(
            blob_store.clone(),
            key.clone(),
            cancellation.clone(),
            move |reader| parse_sdmx_json(reader, artifact_for_full_parse, started_at, parse_tx),
        )
        .await
        {
            Ok(ParseOutcome::Complete) => Ok(()),
            Ok(ParseOutcome::DataSetsBeforeStructure(structure)) => {
                let parse_tx = row_tx.clone();
                parse_blob_stream(
                    blob_store,
                    key,
                    cancellation_for_full_parse,
                    move |reader| {
                        parse_sdmx_data_sets_with_structure(
                            reader, structure, artifact, started_at, parse_tx,
                        )
                    },
                )
                .await
            }
            Err(err) => Err(err),
        };

        if let Err(err) = result {
            let _ = row_tx.send(Err(err)).await;
        }
    });

    Box::pin(stream::unfold(row_rx, |mut row_rx| async {
        row_rx.recv().await.map(|item| (item, row_rx))
    }))
}

fn parse_building_approvals_artifact_stream(
    artifact: ArtifactRef,
    ctx: &ParseCtx,
) -> ObservationStream<'_> {
    let blob_store = ctx.blob_store.clone();
    let started_at = ctx.started_at;
    let cancellation = ctx.cancellation().clone();
    let (row_tx, row_rx) = tokio::sync::mpsc::channel(128);

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

        let mut chunks = match tokio::select! {
            () = cancellation.cancelled() => Err(cancelled_parse_error()),
            chunks = blob_store.get(&key) => chunks.map_err(AdapterError::from),
        } {
            Ok(chunks) => chunks,
            Err(err) => {
                let _ = row_tx.send(Err(err)).await;
                return;
            }
        };
        let mut bytes = Vec::new();
        while let Some(chunk) = tokio::select! {
            () = cancellation.cancelled() => {
                let _ = row_tx.send(Err(cancelled_parse_error())).await;
                return;
            }
            chunk = chunks.next() => chunk,
        } {
            match chunk {
                Ok(chunk) => bytes.extend_from_slice(&chunk),
                Err(err) => {
                    let _ = row_tx.send(Err(err.into())).await;
                    return;
                }
            }
        }

        match parse_building_approvals_html(&bytes, &artifact, started_at) {
            Ok(rows) => {
                for row in rows {
                    if row_tx.send(Ok(row)).await.is_err() {
                        return;
                    }
                }
            }
            Err(err) => {
                let _ = row_tx.send(Err(err)).await;
            }
        }
    });

    Box::pin(stream::unfold(row_rx, |mut row_rx| async {
        row_rx.recv().await.map(|item| (item, row_rx))
    }))
}

fn parse_building_activity_artifact_stream(
    artifact: ArtifactRef,
    ctx: &ParseCtx,
) -> ObservationStream<'_> {
    let blob_store = ctx.blob_store.clone();
    let started_at = ctx.started_at;
    let cancellation = ctx.cancellation().clone();
    let (row_tx, row_rx) = tokio::sync::mpsc::channel(128);

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

        let mut chunks = match tokio::select! {
            () = cancellation.cancelled() => Err(cancelled_parse_error()),
            chunks = blob_store.get(&key) => chunks.map_err(AdapterError::from),
        } {
            Ok(chunks) => chunks,
            Err(err) => {
                let _ = row_tx.send(Err(err)).await;
                return;
            }
        };
        let mut bytes = Vec::new();
        while let Some(chunk) = tokio::select! {
            () = cancellation.cancelled() => {
                let _ = row_tx.send(Err(cancelled_parse_error())).await;
                return;
            }
            chunk = chunks.next() => chunk,
        } {
            match chunk {
                Ok(chunk) => bytes.extend_from_slice(&chunk),
                Err(err) => {
                    let _ = row_tx.send(Err(err.into())).await;
                    return;
                }
            }
        }

        match parse_building_activity_html(&bytes, &artifact, started_at) {
            Ok(rows) => {
                for row in rows {
                    if row_tx.send(Ok(row)).await.is_err() {
                        return;
                    }
                }
            }
            Err(err) => {
                let _ = row_tx.send(Err(err)).await;
            }
        }
    });

    Box::pin(stream::unfold(row_rx, |mut row_rx| async {
        row_rx.recv().await.map(|item| (item, row_rx))
    }))
}

fn parse_dwelling_completion_times_artifact_stream(
    artifact: ArtifactRef,
    ctx: &ParseCtx,
) -> ObservationStream<'_> {
    let blob_store = ctx.blob_store.clone();
    let started_at = ctx.started_at;
    let cancellation = ctx.cancellation().clone();
    let (row_tx, row_rx) = tokio::sync::mpsc::channel(128);

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

        let mut chunks = match tokio::select! {
            () = cancellation.cancelled() => Err(cancelled_parse_error()),
            chunks = blob_store.get(&key) => chunks.map_err(AdapterError::from),
        } {
            Ok(chunks) => chunks,
            Err(err) => {
                let _ = row_tx.send(Err(err)).await;
                return;
            }
        };
        let mut bytes = Vec::new();
        while let Some(chunk) = tokio::select! {
            () = cancellation.cancelled() => {
                let _ = row_tx.send(Err(cancelled_parse_error())).await;
                return;
            }
            chunk = chunks.next() => chunk,
        } {
            match chunk {
                Ok(chunk) => bytes.extend_from_slice(&chunk),
                Err(err) => {
                    let _ = row_tx.send(Err(err.into())).await;
                    return;
                }
            }
        }

        match parse_dwelling_completion_times_html(&bytes, &artifact, started_at) {
            Ok(rows) => {
                for row in rows {
                    if row_tx.send(Ok(row)).await.is_err() {
                        return;
                    }
                }
            }
            Err(err) => {
                let _ = row_tx.send(Err(err)).await;
            }
        }
    });

    Box::pin(stream::unfold(row_rx, |mut row_rx| async {
        row_rx.recv().await.map(|item| (item, row_rx))
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbsParseKind {
    Cpi,
    BuildingApprovals,
    BuildingActivity,
    DwellingCompletionTimes,
}

fn validate_parse_artifact(
    artifact: &ArtifactRef,
    expected_dataflow_id: Option<&DataflowId>,
) -> Result<AbsParseKind, AdapterError> {
    if artifact.source_id.as_str() != "abs" {
        return Err(AdapterError::Validation(format!(
            "ABS parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }

    let (kind, actual_dataflow_id) = if matches!(
        abs_data_url_provenance(&artifact.source_url),
        Some(("ABS", CPI_DATAFLOW_ID))
    ) {
        (AbsParseKind::Cpi, cpi_dataflow_id())
    } else if is_building_approvals_release_url(&artifact.source_url) {
        (
            AbsParseKind::BuildingApprovals,
            building_approvals_dataflow_id(),
        )
    } else if is_building_activity_release_url(&artifact.source_url) {
        (
            AbsParseKind::BuildingActivity,
            building_activity_dataflow_id(),
        )
    } else if is_dwelling_completion_times_url(&artifact.source_url) {
        (
            AbsParseKind::DwellingCompletionTimes,
            dwelling_completion_times_dataflow_id(),
        )
    } else {
        return Err(AdapterError::Validation(format!(
            "ABS parse artifact `{}` is missing supported ABS dataflow provenance",
            artifact.source_url
        )));
    };

    if let Some(expected) = expected_dataflow_id {
        if expected != &actual_dataflow_id {
            return Err(AdapterError::Validation(format!(
                "ABS parse expected dataflow `{}` but artifact emits `{}`",
                expected.as_str(),
                actual_dataflow_id.as_str()
            )));
        }
    }

    Ok(kind)
}

fn abs_data_url_provenance(source_url: &str) -> Option<(&str, &str)> {
    let (_, data_path) = source_url.rsplit_once("/data/")?;
    let artifact_ref = data_path
        .split_once('/')
        .map_or(data_path, |(artifact_ref, _)| artifact_ref);
    let mut parts = artifact_ref.split(',');
    let agency_id = parts.next()?;
    let dataflow_id = parts.next()?;
    if agency_id.is_empty() || dataflow_id.is_empty() {
        None
    } else {
        Some((agency_id, dataflow_id))
    }
}

fn parse_building_approvals_html(
    bytes: &[u8],
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let body = String::from_utf8(bytes.to_vec()).map_err(|err| {
        AdapterError::FormatDrift(format!("ABS release HTML is not UTF-8: {err}"))
    })?;
    let release = parse_building_approvals_release(&body, &artifact.source_url)?;
    let mut rows = Vec::new();
    let mut in_dwelling_table = false;
    for line in html_text_lines(&body) {
        let lower = line.to_ascii_lowercase();
        if lower.contains("dwelling units approved") {
            in_dwelling_table = true;
            continue;
        }
        if in_dwelling_table
            && (lower.contains("value of building approved") || lower.contains("data downloads"))
        {
            break;
        }
        if !in_dwelling_table {
            continue;
        }
        let Some((time, value)) = parse_building_approvals_row(&line)? else {
            continue;
        };
        rows.push(building_approvals_observation(
            time,
            value,
            &release,
            artifact,
            ingested_at,
        )?);
    }

    if rows.is_empty() {
        return Err(AdapterError::FormatDrift(
            "ABS Building Approvals release has no dwelling approvals rows".into(),
        ));
    }
    Ok(rows)
}

fn parse_building_approvals_row(line: &str) -> Result<Option<(DateTime<Utc>, f64)>, AdapterError> {
    let mut parts = line.split_whitespace();
    let Some(period) = parts.next() else {
        return Ok(None);
    };
    let Some(time) = parse_abs_month(period)? else {
        return Ok(None);
    };
    let Some(value) = parts.next() else {
        return Err(AdapterError::FormatDrift(format!(
            "ABS Building Approvals row `{line}` is missing seasonally adjusted value"
        )));
    };
    let value = value.replace(',', "").parse::<f64>().map_err(|err| {
        AdapterError::FormatDrift(format!(
            "ABS Building Approvals value `{value}` in `{line}` is invalid: {err}"
        ))
    })?;
    Ok(Some((time, value)))
}

fn parse_abs_month(period: &str) -> Result<Option<DateTime<Utc>>, AdapterError> {
    let Some((month, year)) = period.split_once('-') else {
        return Ok(None);
    };
    let month = match month.to_ascii_lowercase().as_str() {
        "jan" => 1,
        "feb" => 2,
        "mar" => 3,
        "apr" => 4,
        "may" => 5,
        "jun" => 6,
        "jul" => 7,
        "aug" => 8,
        "sep" => 9,
        "oct" => 10,
        "nov" => 11,
        "dec" => 12,
        _ => return Ok(None),
    };
    if year.len() != 2 {
        return Ok(None);
    }
    let year = year.parse::<i32>().map_err(|err| {
        AdapterError::FormatDrift(format!(
            "invalid ABS Building Approvals year `{year}`: {err}"
        ))
    })?;
    let year = if year >= 70 { 1900 + year } else { 2000 + year };
    date_at_midnight(year, month, 1)
        .map(Some)
        .map_err(AdapterError::FormatDrift)
}

fn building_approvals_observation(
    time: DateTime<Utc>,
    value: f64,
    release: &BuildingApprovalsRelease,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let dataflow_id = building_approvals_dataflow_id();
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("measure").expect("static dimension id is valid"),
            CodeId::new(BUILDING_APPROVALS_MEASURE_ID)
                .expect("static building approvals measure code is valid"),
        ),
        (
            DimensionId::new("region").expect("static dimension id is valid"),
            CodeId::new("AUS").expect("static region code is valid"),
        ),
    ]);
    let series_key = SeriesKey::derive(
        &dataflow_id,
        dimensions
            .iter()
            .map(|(dimension, code)| (dimension.as_str(), code.as_str())),
    );
    let descriptor = SeriesDescriptor {
        series_key,
        dataflow_id,
        measure_id: MeasureId::new(BUILDING_APPROVALS_MEASURE_ID)
            .expect("static measure id is valid"),
        dimensions,
        unit: "dwellings".into(),
    };
    let observation = Observation {
        series_key,
        time,
        time_precision: TimePrecision::Month,
        value: Some(value),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes: BTreeMap::from([
            (
                "abs_release_period".into(),
                release.reference_period.clone(),
            ),
            ("abs_series".into(), "seasonally_adjusted".into()),
            ("source_url".into(), artifact.source_url.clone()),
        ]),
        ingested_at,
        source_artifact_id: artifact.id,
    };
    Ok((descriptor, observation))
}

fn parse_building_activity_html(
    bytes: &[u8],
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let body = String::from_utf8(bytes.to_vec()).map_err(|err| {
        AdapterError::FormatDrift(format!("ABS release HTML is not UTF-8: {err}"))
    })?;
    let release = parse_building_activity_release(&body, &artifact.source_url)?;
    let mut rows = Vec::new();
    let mut current_measure: Option<(&str, usize)> = None;

    for line in html_text_lines(&body) {
        let lower = line.to_ascii_lowercase();
        if lower.contains("total dwellings commenced") {
            current_measure = Some((BUILDING_ACTIVITY_COMMENCED_MEASURE_ID, 1));
            continue;
        }
        if lower.contains("total dwellings completed") {
            current_measure = Some((BUILDING_ACTIVITY_COMPLETED_MEASURE_ID, 0));
            continue;
        }
        if lower.contains("private dwellings commenced")
            || lower.contains("private dwellings completed")
            || lower.contains("selected states")
            || lower.contains("dwellings under construction")
            || lower.contains("data downloads")
            || lower.contains("back to top")
        {
            current_measure = None;
            continue;
        }

        let Some((measure_id, seasonally_adjusted_index)) = current_measure else {
            continue;
        };
        let Some((time, value)) = parse_building_activity_row(&line, seasonally_adjusted_index)?
        else {
            continue;
        };
        rows.push(building_activity_observation(
            time,
            value,
            measure_id,
            &release,
            artifact,
            ingested_at,
        )?);
    }

    if rows.is_empty() {
        return Err(AdapterError::FormatDrift(
            "ABS Building Activity release has no national dwelling activity rows".into(),
        ));
    }
    Ok(rows)
}

fn parse_building_activity_row(
    line: &str,
    seasonally_adjusted_index: usize,
) -> Result<Option<(DateTime<Utc>, f64)>, AdapterError> {
    let mut parts = line.split_whitespace();
    let Some(period) = parts.next() else {
        return Ok(None);
    };
    let Some(time) = parse_abs_quarter(period)? else {
        return Ok(None);
    };
    let values = parts
        .take(2)
        .map(|value| {
            value.replace(',', "").parse::<f64>().map_err(|err| {
                AdapterError::FormatDrift(format!(
                    "ABS Building Activity value `{value}` in `{line}` is invalid: {err}"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let Some(value) = values.get(seasonally_adjusted_index).copied() else {
        return Err(AdapterError::FormatDrift(format!(
            "ABS Building Activity row `{line}` is missing seasonally adjusted value"
        )));
    };
    Ok(Some((time, value)))
}

fn parse_abs_quarter(period: &str) -> Result<Option<DateTime<Utc>>, AdapterError> {
    let Some((month, year)) = period.split_once('-') else {
        return Ok(None);
    };
    let month = match month.to_ascii_lowercase().as_str() {
        "mar" => 1,
        "jun" => 4,
        "sep" => 7,
        "dec" => 10,
        _ => return Ok(None),
    };
    if year.len() != 2 {
        return Ok(None);
    }
    let year = year.parse::<i32>().map_err(|err| {
        AdapterError::FormatDrift(format!(
            "invalid ABS Building Activity year `{year}`: {err}"
        ))
    })?;
    let year = if year >= 70 { 1900 + year } else { 2000 + year };
    date_at_midnight(year, month, 1)
        .map(Some)
        .map_err(AdapterError::FormatDrift)
}

fn building_activity_observation(
    time: DateTime<Utc>,
    value: f64,
    measure_id: &str,
    release: &BuildingActivityRelease,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let dataflow_id = building_activity_dataflow_id();
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("measure").expect("static dimension id is valid"),
            CodeId::new(measure_id).expect("static building activity measure code is valid"),
        ),
        (
            DimensionId::new("region").expect("static dimension id is valid"),
            CodeId::new("AUS").expect("static region code is valid"),
        ),
    ]);
    let series_key = SeriesKey::derive(
        &dataflow_id,
        dimensions
            .iter()
            .map(|(dimension, code)| (dimension.as_str(), code.as_str())),
    );
    let descriptor = SeriesDescriptor {
        series_key,
        dataflow_id,
        measure_id: MeasureId::new(measure_id).expect("static measure id is valid"),
        dimensions,
        unit: "dwellings".into(),
    };
    let observation = Observation {
        series_key,
        time,
        time_precision: TimePrecision::Quarter,
        value: Some(value),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes: BTreeMap::from([
            (
                "abs_release_period".into(),
                release.reference_period.clone(),
            ),
            ("abs_series".into(), "seasonally_adjusted".into()),
            ("source_url".into(), artifact.source_url.clone()),
        ]),
        ingested_at,
        source_artifact_id: artifact.id,
    };
    Ok((descriptor, observation))
}

fn parse_dwelling_completion_times_html(
    bytes: &[u8],
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    let body = String::from_utf8(bytes.to_vec()).map_err(|err| {
        AdapterError::FormatDrift(format!("ABS article HTML is not UTF-8: {err}"))
    })?;
    let article = parse_dwelling_completion_times_article(&body, &artifact.source_url)?;
    let mut rows = Vec::new();
    let mut section = CompletionTimesSection::None;

    for line in html_text_lines(&body) {
        let lower = line.to_ascii_lowercase();
        if lower.contains("graph 1:")
            && lower.contains("new houses")
            && lower.contains("new townhouses")
        {
            section = CompletionTimesSection::HousesAndTownhouses;
            continue;
        }
        if lower.contains("graph 2:")
            && (lower.contains("new flats") || lower.contains("new apartments"))
        {
            section = CompletionTimesSection::Apartments;
            continue;
        }
        if lower.contains("graph 3:")
            || lower.contains("state and territories")
            || lower.contains("back to top")
        {
            section = CompletionTimesSection::None;
            continue;
        }

        let Some(row) = parse_completion_times_row(&line, section)? else {
            continue;
        };
        for value in row.values {
            rows.push(dwelling_completion_times_observation(
                row.time,
                &row.financial_year,
                value.dwelling_type,
                value.months,
                &article,
                artifact,
                ingested_at,
            )?);
        }
    }

    if rows.is_empty() {
        return Err(AdapterError::FormatDrift(
            "ABS dwelling completion times article has no national completion-time rows".into(),
        ));
    }
    Ok(rows)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionTimesSection {
    None,
    HousesAndTownhouses,
    Apartments,
}

#[derive(Debug, Clone, PartialEq)]
struct CompletionTimesRow {
    time: DateTime<Utc>,
    financial_year: String,
    values: Vec<CompletionTimesValue>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct CompletionTimesValue {
    dwelling_type: &'static str,
    months: f64,
}

fn parse_completion_times_row(
    line: &str,
    section: CompletionTimesSection,
) -> Result<Option<CompletionTimesRow>, AdapterError> {
    if section == CompletionTimesSection::None {
        return Ok(None);
    }
    let mut parts = line.split_whitespace();
    let Some(period) = parts.next() else {
        return Ok(None);
    };
    let Some(time) = parse_financial_year_start(period)? else {
        return Ok(None);
    };
    let parsed = parts
        .map(|value| {
            value.parse::<f64>().map_err(|err| {
                AdapterError::FormatDrift(format!(
                    "ABS dwelling completion time value `{value}` in `{line}` is invalid: {err}"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let values = match section {
        CompletionTimesSection::HousesAndTownhouses => {
            if parsed.len() < 2 {
                return Err(AdapterError::FormatDrift(format!(
                    "ABS dwelling completion row `{line}` is missing house/townhouse values"
                )));
            }
            vec![
                CompletionTimesValue {
                    dwelling_type: "houses",
                    months: quarters_to_months(parsed[0]),
                },
                CompletionTimesValue {
                    dwelling_type: "townhouses",
                    months: quarters_to_months(parsed[1]),
                },
            ]
        }
        CompletionTimesSection::Apartments => {
            let Some(value) = parsed.first().copied() else {
                return Err(AdapterError::FormatDrift(format!(
                    "ABS dwelling completion row `{line}` is missing apartment value"
                )));
            };
            vec![CompletionTimesValue {
                dwelling_type: "apartments",
                months: quarters_to_months(value),
            }]
        }
        CompletionTimesSection::None => unreachable!("handled above"),
    };
    Ok(Some(CompletionTimesRow {
        time,
        financial_year: period.to_string(),
        values,
    }))
}

fn parse_financial_year_start(period: &str) -> Result<Option<DateTime<Utc>>, AdapterError> {
    let Some((start, end)) = period.split_once('-') else {
        return Ok(None);
    };
    if start.len() != 4 || end.len() != 4 {
        return Ok(None);
    }
    let start = start.parse::<i32>().map_err(|err| {
        AdapterError::FormatDrift(format!(
            "invalid ABS dwelling completion financial year `{period}`: {err}"
        ))
    })?;
    let end = end.parse::<i32>().map_err(|err| {
        AdapterError::FormatDrift(format!(
            "invalid ABS dwelling completion financial year `{period}`: {err}"
        ))
    })?;
    if end != start + 1 {
        return Err(AdapterError::FormatDrift(format!(
            "ABS dwelling completion financial year `{period}` must span one year"
        )));
    }
    date_at_midnight(start, 7, 1)
        .map(Some)
        .map_err(AdapterError::FormatDrift)
}

fn quarters_to_months(quarters: f64) -> f64 {
    (quarters * 300.0).round() / 100.0
}

fn dwelling_completion_times_observation(
    time: DateTime<Utc>,
    financial_year: &str,
    dwelling_type: &str,
    value: f64,
    article: &DwellingCompletionTimesArticle,
    artifact: &ArtifactRef,
    ingested_at: DateTime<Utc>,
) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let dataflow_id = dwelling_completion_times_dataflow_id();
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("dwelling_type").expect("static dimension id is valid"),
            CodeId::new(dwelling_type).expect("static dwelling type code is valid"),
        ),
        (
            DimensionId::new("measure").expect("static dimension id is valid"),
            CodeId::new(DWELLING_COMPLETION_TIMES_MEASURE_ID)
                .expect("static completion-time measure code is valid"),
        ),
        (
            DimensionId::new("region").expect("static dimension id is valid"),
            CodeId::new("AUS").expect("static region code is valid"),
        ),
    ]);
    let series_key = SeriesKey::derive(
        &dataflow_id,
        dimensions
            .iter()
            .map(|(dimension, code)| (dimension.as_str(), code.as_str())),
    );
    let descriptor = SeriesDescriptor {
        series_key,
        dataflow_id,
        measure_id: MeasureId::new(DWELLING_COMPLETION_TIMES_MEASURE_ID)
            .expect("static measure id is valid"),
        dimensions,
        unit: "months".into(),
    };
    let observation = Observation {
        series_key,
        time,
        time_precision: TimePrecision::Year,
        value: Some(value),
        status: ObservationStatus::Normal,
        revision_no: 0,
        attributes: BTreeMap::from([
            ("abs_article".into(), article.title.clone()),
            ("abs_financial_year".into(), financial_year.to_string()),
            ("abs_original_unit".into(), "quarters".into()),
            ("source_url".into(), artifact.source_url.clone()),
        ]),
        ingested_at,
        source_artifact_id: artifact.id,
    };
    Ok((descriptor, observation))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BuildingApprovalsRelease {
    source_url: String,
    reference_period: String,
    released: Option<String>,
}

impl BuildingApprovalsRelease {
    fn revision(&self) -> UpstreamRevision {
        UpstreamRevision::new(self.reference_period.clone(), self.released.clone())
    }

    fn to_discovered_job(&self, trace_parent: Option<&str>) -> DiscoveredJob {
        let revision = self.revision();
        let revision_version = revision.version().to_string();
        let revision_key = "ABS:building-approvals".to_string();
        let mut metadata = BTreeMap::from([
            ("adapter".into(), "abs".into()),
            ("artifact_format".into(), "html".into()),
            ("attribution".into(), ABS_ATTRIBUTION.into()),
            ("cadence".into(), "monthly".into()),
            (
                "dataflow_id".into(),
                BUILDING_APPROVALS_CANONICAL_DATAFLOW_ID.into(),
            ),
            ("license".into(), "CC-BY-4.0".into()),
            ("measure_id".into(), BUILDING_APPROVALS_MEASURE_ID.into()),
            ("revision_key".into(), revision_key),
            ("revision_version".into(), revision_version.clone()),
            ("title".into(), "Building Approvals, Australia".into()),
        ]);
        if let Some(released) = &self.released {
            metadata.insert("released".into(), released.clone());
        }
        DiscoveredJob {
            id: format!(
                "abs:{}:{}",
                BUILDING_APPROVALS_DATAFLOW_SLUG,
                revision_token(Some(&revision_version))
            ),
            source_id: SourceId::new("abs").expect("static source id is valid"),
            dataflow_id: building_approvals_dataflow_id(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(ToOwned::to_owned),
            metadata,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BuildingActivityRelease {
    source_url: String,
    reference_period: String,
    released: Option<String>,
}

impl BuildingActivityRelease {
    fn revision(&self) -> UpstreamRevision {
        UpstreamRevision::new(self.reference_period.clone(), self.released.clone())
    }

    fn to_discovered_job(&self, trace_parent: Option<&str>) -> DiscoveredJob {
        let revision = self.revision();
        let revision_version = revision.version().to_string();
        let revision_key = "ABS:building-activity".to_string();
        let mut metadata = BTreeMap::from([
            ("adapter".into(), "abs".into()),
            ("artifact_format".into(), "html".into()),
            ("attribution".into(), ABS_ATTRIBUTION.into()),
            ("cadence".into(), "quarterly".into()),
            (
                "dataflow_id".into(),
                BUILDING_ACTIVITY_CANONICAL_DATAFLOW_ID.into(),
            ),
            ("license".into(), "CC-BY-4.0".into()),
            (
                "measure_id".into(),
                BUILDING_ACTIVITY_COMMENCED_MEASURE_ID.into(),
            ),
            ("revision_key".into(), revision_key),
            ("revision_version".into(), revision_version.clone()),
            ("title".into(), "Building Activity, Australia".into()),
        ]);
        if let Some(released) = &self.released {
            metadata.insert("released".into(), released.clone());
        }
        DiscoveredJob {
            id: format!(
                "abs:{}:{}",
                BUILDING_ACTIVITY_DATAFLOW_SLUG,
                revision_token(Some(&revision_version))
            ),
            source_id: SourceId::new("abs").expect("static source id is valid"),
            dataflow_id: building_activity_dataflow_id(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(ToOwned::to_owned),
            metadata,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DwellingCompletionTimesArticle {
    source_url: String,
    title: String,
    released: String,
}

impl DwellingCompletionTimesArticle {
    fn revision(&self) -> UpstreamRevision {
        UpstreamRevision::new(self.released.clone(), Some(self.title.clone()))
    }

    fn to_discovered_job(&self, trace_parent: Option<&str>) -> DiscoveredJob {
        let revision = self.revision();
        let revision_version = revision.version().to_string();
        let revision_key = "ABS:dwelling-completion-times".to_string();
        let metadata = BTreeMap::from([
            ("adapter".into(), "abs".into()),
            ("artifact_format".into(), "html".into()),
            ("attribution".into(), ABS_ATTRIBUTION.into()),
            ("cadence".into(), "annual".into()),
            (
                "dataflow_id".into(),
                DWELLING_COMPLETION_TIMES_CANONICAL_DATAFLOW_ID.into(),
            ),
            ("license".into(), "CC-BY-4.0".into()),
            (
                "measure_id".into(),
                DWELLING_COMPLETION_TIMES_MEASURE_ID.into(),
            ),
            ("released".into(), self.released.clone()),
            ("revision_key".into(), revision_key),
            ("revision_version".into(), revision_version.clone()),
            ("title".into(), self.title.clone()),
        ]);
        DiscoveredJob {
            id: format!(
                "abs:{}:{}",
                DWELLING_COMPLETION_TIMES_DATAFLOW_SLUG,
                revision_token(Some(&revision_version))
            ),
            source_id: SourceId::new("abs").expect("static source id is valid"),
            dataflow_id: dwelling_completion_times_dataflow_id(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(ToOwned::to_owned),
            metadata,
        }
    }
}

fn parse_building_approvals_release(
    body: &str,
    source_url: &str,
) -> Result<BuildingApprovalsRelease, AdapterError> {
    if !is_building_approvals_release_url(source_url) {
        return Err(AdapterError::Validation(format!(
            "ABS Building Approvals release URL `{source_url}` is not supported"
        )));
    }
    let lines = html_text_lines(body);
    let reference_period = labeled_text_value(&lines, "Reference period").ok_or_else(|| {
        AdapterError::FormatDrift(
            "ABS Building Approvals release is missing reference period".into(),
        )
    })?;
    let released = labeled_text_value(&lines, "Released");
    Ok(BuildingApprovalsRelease {
        source_url: source_url.to_string(),
        reference_period,
        released,
    })
}

fn parse_building_activity_release(
    body: &str,
    source_url: &str,
) -> Result<BuildingActivityRelease, AdapterError> {
    if !is_building_activity_release_url(source_url) {
        return Err(AdapterError::Validation(format!(
            "ABS Building Activity release URL `{source_url}` is not supported"
        )));
    }
    let lines = html_text_lines(body);
    let reference_period = labeled_text_value(&lines, "Reference period").ok_or_else(|| {
        AdapterError::FormatDrift(
            "ABS Building Activity release is missing reference period".into(),
        )
    })?;
    let released = labeled_text_value(&lines, "Released");
    Ok(BuildingActivityRelease {
        source_url: source_url.to_string(),
        reference_period,
        released,
    })
}

fn parse_dwelling_completion_times_article(
    body: &str,
    source_url: &str,
) -> Result<DwellingCompletionTimesArticle, AdapterError> {
    if !is_dwelling_completion_times_url(source_url) {
        return Err(AdapterError::Validation(format!(
            "ABS dwelling completion times URL `{source_url}` is not supported"
        )));
    }
    let lines = html_text_lines(body);
    let title = lines
        .iter()
        .find(|line| line.eq_ignore_ascii_case("Average dwelling completion times"))
        .cloned()
        .unwrap_or_else(|| "Average dwelling completion times".into());
    let released = labeled_text_value(&lines, "Released").ok_or_else(|| {
        AdapterError::FormatDrift(
            "ABS dwelling completion times article is missing released date".into(),
        )
    })?;
    Ok(DwellingCompletionTimesArticle {
        source_url: source_url.to_string(),
        title,
        released,
    })
}

fn labeled_text_value(lines: &[String], label: &str) -> Option<String> {
    lines.iter().enumerate().find_map(|(index, line)| {
        line.eq_ignore_ascii_case(label).then(|| {
            lines[index + 1..]
                .iter()
                .find(|value| !value.trim().is_empty())
                .cloned()
        })?
    })
}

fn html_text_lines(body: &str) -> Vec<String> {
    let prepared = body
        .replace("</td>", " ")
        .replace("</th>", " ")
        .replace("</tr>", "\n")
        .replace("</p>", "\n")
        .replace("</h1>", "\n")
        .replace("</h2>", "\n")
        .replace("</h3>", "\n")
        .replace("</dt>", "\n")
        .replace("</dd>", "\n");
    let mut out = String::with_capacity(prepared.len());
    let mut in_tag = false;
    for ch in prepared.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|line| !line.is_empty())
        .collect()
}

fn is_building_approvals_release_url(source_url: &str) -> bool {
    let without_query = source_url
        .split_once('?')
        .map_or(source_url, |(source_url, _)| source_url);
    without_query
        .contains("/statistics/industry/building-and-construction/building-approvals-australia/")
        || without_query.contains("/building-approvals/")
}

fn is_building_activity_release_url(source_url: &str) -> bool {
    let without_query = source_url
        .split_once('?')
        .map_or(source_url, |(source_url, _)| source_url);
    without_query
        .contains("/statistics/industry/building-and-construction/building-activity-australia/")
        || without_query.contains("/building-activity/")
}

fn is_dwelling_completion_times_url(source_url: &str) -> bool {
    let without_query = source_url
        .split_once('?')
        .map_or(source_url, |(source_url, _)| source_url);
    without_query.contains("/articles/average-dwelling-completion-times")
}

fn cpi_dataflow_id() -> DataflowId {
    DataflowId::new(CPI_CANONICAL_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn building_approvals_dataflow_id() -> DataflowId {
    DataflowId::new(BUILDING_APPROVALS_CANONICAL_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn building_activity_dataflow_id() -> DataflowId {
    DataflowId::new(BUILDING_ACTIVITY_CANONICAL_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn dwelling_completion_times_dataflow_id() -> DataflowId {
    DataflowId::new(DWELLING_COMPLETION_TIMES_CANONICAL_DATAFLOW_ID)
        .expect("static dataflow id is valid")
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
            "ABS parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )));
    }

    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "ABS parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )))
    }
}

async fn parse_blob_stream<T, F>(
    blob_store: BlobStore,
    key: StorageKey,
    cancellation: CancellationToken,
    parser: F,
) -> Result<T, AdapterError>
where
    T: Send + 'static,
    F: FnOnce(ChannelReader) -> Result<T, AdapterError> + Send + 'static,
{
    if cancellation.is_cancelled() {
        return Err(cancelled_parse_error());
    }

    let mut chunks = tokio::select! {
        () = cancellation.cancelled() => return Err(cancelled_parse_error()),
        chunks = blob_store.get(&key) => chunks?,
    };
    let (byte_tx, byte_rx) = tokio::sync::mpsc::channel(8);
    let read_error = Arc::new(Mutex::new(None));
    let reader_error = Arc::clone(&read_error);
    let reader_cancellation = cancellation.clone();
    let parser = tokio::task::spawn_blocking(move || {
        parser(ChannelReader::new(
            byte_rx,
            reader_error,
            reader_cancellation,
        ))
    });

    let mut cancelled = false;
    loop {
        tokio::select! {
            () = cancellation.cancelled(), if !cancelled => {
                cancelled = true;
                break;
            }
            chunk = chunks.next() => {
                let Some(chunk) = chunk else {
                    break;
                };
                let send = tokio::select! {
                    () = cancellation.cancelled() => {
                        cancelled = true;
                        break;
                    }
                    send = byte_tx.send(chunk) => send,
                };
                if send.is_err() {
                    break;
                }
            }
        }
    }
    drop(byte_tx);

    if cancelled {
        let _ = parser.await;
        return Err(cancelled_parse_error());
    }

    match parser.await {
        Ok(Err(err)) => match read_error.lock().expect("read error mutex poisoned").take() {
            Some(storage_err) => Err(AdapterError::Storage(storage_err)),
            None => Err(err),
        },
        Ok(Ok(result)) => Ok(result),
        Err(err) => Err(parse_worker_error(err)),
    }
}

fn parse_worker_error(err: tokio::task::JoinError) -> AdapterError {
    CoreError::Io(io::Error::other(format!("ABS parse worker failed: {err}"))).into()
}

fn cancelled_parse_error() -> AdapterError {
    CoreError::Io(io::Error::new(
        io::ErrorKind::Interrupted,
        "ABS parse cancelled",
    ))
    .into()
}

fn parse_sdmx_data_sets_with_structure<R: Read>(
    reader: R,
    structure: ParsedStructure,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    DataSetsOnlyTopLevelSeed {
        structure,
        artifact,
        ingested_at,
        tx,
    }
    .deserialize(&mut deserializer)
    .map_err(map_sdmx_json_error)?;
    deserializer.end().map_err(map_sdmx_json_error)?;
    Ok(())
}

fn parse_sdmx_json<R: Read>(
    reader: R,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<ParseOutcome, AdapterError> {
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let outcome = TopLevelSeed {
        artifact,
        ingested_at,
        tx,
    }
    .deserialize(&mut deserializer)
    .map_err(map_sdmx_json_error)?;
    deserializer.end().map_err(map_sdmx_json_error)?;
    Ok(outcome)
}

/// Parse an SDMX-JSON reader through the production parser core and return the
/// number of emitted observations.
///
/// This is intentionally hidden from the public adapter API; it exists so the
/// Criterion parser benchmark can measure SDMX decoding without including
/// object-store reads, artifact identity verification, or async receiver
/// scheduling in the micro-benchmark budget.
#[doc(hidden)]
pub fn parse_sdmx_json_observation_count_for_benchmark<R: Read>(
    reader: R,
) -> Result<usize, AdapterError> {
    let artifact_id = ArtifactId::of_content(b"abs sdmx parser benchmark");
    let artifact = ArtifactRef {
        id: artifact_id,
        source_id: SourceId::new("abs").expect("static source id is valid"),
        source_url:
            "https://data.api.abs.gov.au/rest/data/ABS,CPI,2.0.0/all?dimensionAtObservation=TIME_PERIOD"
                .into(),
        content_type: DATA_JSON_ACCEPT.into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&artifact_id).to_string(),
        size_bytes: 0,
        fetched_at: Utc
            .with_ymd_and_hms(2024, 4, 24, 0, 0, 0)
            .single()
            .expect("valid benchmark timestamp"),
    };
    let (tx, rx) = tokio::sync::mpsc::channel(1_000_000);
    match parse_sdmx_json(
        reader,
        artifact,
        Utc.with_ymd_and_hms(2024, 4, 30, 0, 0, 0)
            .single()
            .expect("valid benchmark timestamp"),
        tx,
    )? {
        ParseOutcome::Complete => Ok(rx.len()),
        ParseOutcome::DataSetsBeforeStructure(_) => Err(AdapterError::FormatDrift(
            "benchmark SDMX fixture must place structure before dataSets".into(),
        )),
    }
}

fn map_sdmx_json_error(err: serde_json::Error) -> AdapterError {
    AdapterError::FormatDrift(err.to_string())
}

struct ChannelReader {
    rx: tokio::sync::mpsc::Receiver<Result<Bytes, StorageError>>,
    read_error: Arc<Mutex<Option<StorageError>>>,
    cancellation: CancellationToken,
    current: Option<Bytes>,
    offset: usize,
}

impl ChannelReader {
    fn new(
        rx: tokio::sync::mpsc::Receiver<Result<Bytes, StorageError>>,
        read_error: Arc<Mutex<Option<StorageError>>>,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            rx,
            read_error,
            cancellation,
            current: None,
            offset: 0,
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }

        loop {
            if self.cancellation.is_cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "ABS parse cancelled",
                ));
            }
            if let Some(current) = &self.current {
                if self.offset < current.len() {
                    let available = &current[self.offset..];
                    let len = available.len().min(out.len());
                    out[..len].copy_from_slice(&available[..len]);
                    self.offset += len;
                    if self.offset == current.len() {
                        self.current = None;
                        self.offset = 0;
                    }
                    return Ok(len);
                }
                self.current = None;
                self.offset = 0;
            }

            match self.rx.blocking_recv() {
                Some(Ok(chunk)) if chunk.is_empty() => {}
                Some(Ok(chunk)) => {
                    self.current = Some(chunk);
                }
                Some(Err(err)) => {
                    *self.read_error.lock().expect("read error mutex poisoned") = Some(err);
                    return Err(io::Error::other("storage read failed"));
                }
                None => return Ok(0),
            }
        }
    }
}

#[derive(Debug)]
enum ParseOutcome {
    Complete,
    DataSetsBeforeStructure(ParsedStructure),
}

struct TopLevelSeed {
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> DeserializeSeed<'de> for TopLevelSeed {
    type Value = ParseOutcome;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(TopLevelVisitor {
            artifact: self.artifact,
            ingested_at: self.ingested_at,
            tx: self.tx,
        })
    }
}

struct TopLevelVisitor {
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> Visitor<'de> for TopLevelVisitor {
    type Value = ParseOutcome;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ABS SDMX-JSON top-level object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut outcome = None;
        while let Some(key) = map.next_key::<String>()? {
            if key == "data" {
                outcome = Some(map.next_value_seed(DataSeed {
                    artifact: self.artifact.clone(),
                    ingested_at: self.ingested_at,
                    tx: self.tx.clone(),
                })?);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        outcome.ok_or_else(|| de::Error::custom("ABS SDMX payload is missing `data`"))
    }
}

struct DataSetsOnlyTopLevelSeed {
    structure: ParsedStructure,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> DeserializeSeed<'de> for DataSetsOnlyTopLevelSeed {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(DataSetsOnlyTopLevelVisitor {
            structure: self.structure,
            artifact: self.artifact,
            ingested_at: self.ingested_at,
            tx: self.tx,
        })
    }
}

struct DataSetsOnlyTopLevelVisitor {
    structure: ParsedStructure,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> Visitor<'de> for DataSetsOnlyTopLevelVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ABS SDMX-JSON top-level object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut saw_data = false;
        while let Some(key) = map.next_key::<String>()? {
            if key == "data" {
                saw_data = true;
                map.next_value_seed(DataSetsOnlyDataSeed {
                    structure: self.structure.clone(),
                    artifact: self.artifact.clone(),
                    ingested_at: self.ingested_at,
                    tx: self.tx.clone(),
                })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        if saw_data {
            Ok(())
        } else {
            Err(de::Error::custom("ABS SDMX payload is missing `data`"))
        }
    }
}

struct DataSetsOnlyDataSeed {
    structure: ParsedStructure,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> DeserializeSeed<'de> for DataSetsOnlyDataSeed {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(DataSetsOnlyDataVisitor {
            structure: self.structure,
            artifact: self.artifact,
            ingested_at: self.ingested_at,
            tx: self.tx,
        })
    }
}

struct DataSetsOnlyDataVisitor {
    structure: ParsedStructure,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> Visitor<'de> for DataSetsOnlyDataVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ABS SDMX `data` object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut saw_data_sets = false;
        while let Some(key) = map.next_key::<String>()? {
            if key == "dataSets" {
                saw_data_sets = true;
                map.next_value_seed(DataSetsSeed {
                    structure: self.structure.clone(),
                    artifact: self.artifact.clone(),
                    ingested_at: self.ingested_at,
                    tx: self.tx.clone(),
                })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        if saw_data_sets {
            Ok(())
        } else {
            Err(de::Error::custom("ABS SDMX data is missing `dataSets`"))
        }
    }
}

struct DataSeed {
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> DeserializeSeed<'de> for DataSeed {
    type Value = ParseOutcome;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(DataVisitor {
            artifact: self.artifact,
            ingested_at: self.ingested_at,
            tx: self.tx,
        })
    }
}

struct DataVisitor {
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> Visitor<'de> for DataVisitor {
    type Value = ParseOutcome;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ABS SDMX `data` object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut structure = None;
        let mut data_sets_before_structure = false;
        let mut saw_data_sets = false;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "structure" => {
                    structure = Some(
                        map.next_value::<SdmxStructure>()?
                            .into_parsed()
                            .map_err(de::Error::custom)?,
                    );
                }
                "dataSets" => {
                    saw_data_sets = true;
                    if let Some(parsed_structure) = structure.clone() {
                        map.next_value_seed(DataSetsSeed {
                            structure: parsed_structure,
                            artifact: self.artifact.clone(),
                            ingested_at: self.ingested_at,
                            tx: self.tx.clone(),
                        })?;
                    } else {
                        data_sets_before_structure = true;
                        map.next_value::<IgnoredAny>()?;
                    }
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        if structure.is_none() {
            return Err(de::Error::custom("ABS SDMX data is missing `structure`"));
        }
        if !saw_data_sets {
            return Err(de::Error::custom("ABS SDMX data is missing `dataSets`"));
        }
        if data_sets_before_structure {
            return Ok(ParseOutcome::DataSetsBeforeStructure(
                structure.expect("structure presence checked above"),
            ));
        }
        Ok(ParseOutcome::Complete)
    }
}

struct DataSetsSeed {
    structure: ParsedStructure,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> DeserializeSeed<'de> for DataSetsSeed {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(DataSetsVisitor {
            structure: self.structure,
            artifact: self.artifact,
            ingested_at: self.ingested_at,
            tx: self.tx,
        })
    }
}

struct DataSetsVisitor {
    structure: ParsedStructure,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> Visitor<'de> for DataSetsVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ABS SDMX dataSets array")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq
            .next_element_seed(DataSetSeed {
                structure: self.structure.clone(),
                artifact: self.artifact.clone(),
                ingested_at: self.ingested_at,
                tx: self.tx.clone(),
            })?
            .is_some()
        {}
        Ok(())
    }
}

struct DataSetSeed {
    structure: ParsedStructure,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> DeserializeSeed<'de> for DataSetSeed {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(DataSetVisitor {
            structure: self.structure,
            artifact: self.artifact,
            ingested_at: self.ingested_at,
            tx: self.tx,
        })
    }
}

struct DataSetVisitor {
    structure: ParsedStructure,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> Visitor<'de> for DataSetVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ABS SDMX dataSet object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut saw_series = false;
        while let Some(key) = map.next_key::<String>()? {
            if key == "series" {
                saw_series = true;
                map.next_value_seed(SeriesMapSeed {
                    structure: self.structure.clone(),
                    artifact: self.artifact.clone(),
                    ingested_at: self.ingested_at,
                    tx: self.tx.clone(),
                })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        if saw_series {
            Ok(())
        } else {
            Err(de::Error::custom(
                "ABS SDMX dataSet is missing `series`; all-dim observations are not supported yet",
            ))
        }
    }
}

struct SeriesMapSeed {
    structure: ParsedStructure,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> DeserializeSeed<'de> for SeriesMapSeed {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(SeriesMapVisitor {
            structure: self.structure,
            artifact: self.artifact,
            ingested_at: self.ingested_at,
            tx: self.tx,
        })
    }
}

struct SeriesMapVisitor {
    structure: ParsedStructure,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> Visitor<'de> for SeriesMapVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ABS SDMX series map")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(series_index_key) = map.next_key::<String>()? {
            let descriptor = self
                .structure
                .series_descriptor(&series_index_key)
                .map_err(de::Error::custom)?;
            map.next_value_seed(SeriesValueSeed {
                structure: self.structure.clone(),
                descriptor,
                artifact: self.artifact.clone(),
                ingested_at: self.ingested_at,
                tx: self.tx.clone(),
            })?;
        }
        Ok(())
    }
}

struct SeriesValueSeed {
    structure: ParsedStructure,
    descriptor: SeriesDescriptor,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> DeserializeSeed<'de> for SeriesValueSeed {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(SeriesValueVisitor {
            structure: self.structure,
            descriptor: self.descriptor,
            artifact: self.artifact,
            ingested_at: self.ingested_at,
            tx: self.tx,
        })
    }
}

struct SeriesValueVisitor {
    structure: ParsedStructure,
    descriptor: SeriesDescriptor,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> Visitor<'de> for SeriesValueVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ABS SDMX series value object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut saw_observations = false;
        while let Some(key) = map.next_key::<String>()? {
            if key == "observations" {
                saw_observations = true;
                map.next_value_seed(ObservationsSeed {
                    structure: self.structure.clone(),
                    descriptor: self.descriptor.clone(),
                    artifact: self.artifact.clone(),
                    ingested_at: self.ingested_at,
                    tx: self.tx.clone(),
                })?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        if saw_observations {
            Ok(())
        } else {
            Err(de::Error::custom(
                "ABS SDMX series is missing `observations`",
            ))
        }
    }
}

struct ObservationsSeed {
    structure: ParsedStructure,
    descriptor: SeriesDescriptor,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> DeserializeSeed<'de> for ObservationsSeed {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(ObservationsVisitor {
            structure: self.structure,
            descriptor: self.descriptor,
            artifact: self.artifact,
            ingested_at: self.ingested_at,
            tx: self.tx,
        })
    }
}

struct ObservationsVisitor {
    structure: ParsedStructure,
    descriptor: SeriesDescriptor,
    artifact: ArtifactRef,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
}

impl<'de> Visitor<'de> for ObservationsVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ABS SDMX observations map")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(observation_index_key) = map.next_key::<String>()? {
            let tuple = map.next_value::<ObservationTuple>()?;
            let observation = self
                .structure
                .observation(
                    &self.descriptor,
                    &observation_index_key,
                    tuple,
                    &self.artifact,
                    self.ingested_at,
                )
                .map_err(de::Error::custom)?;
            let row = Ok((self.descriptor.clone(), observation));
            match self.tx.try_send(row) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(row)) => {
                    self.tx
                        .blocking_send(row)
                        .map_err(|_| de::Error::custom("ABS parse receiver was dropped"))?;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    return Err(de::Error::custom("ABS parse receiver was dropped"));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct SdmxStructure {
    dimensions: SdmxDimensions,
    #[serde(default)]
    attributes: SdmxAttributes,
}

impl SdmxStructure {
    fn into_parsed(self) -> Result<ParsedStructure, String> {
        Ok(ParsedStructure {
            series_dimensions: self
                .dimensions
                .series
                .into_iter()
                .map(ParsedDimension::from_series)
                .collect::<Result<_, _>>()
                .map_err(|err| err.to_string())?,
            observation_dimensions: self
                .dimensions
                .observation
                .into_iter()
                .map(ParsedDimension::try_from)
                .collect::<Result<_, _>>()
                .map_err(|err| err.to_string())?,
            observation_attributes: self
                .attributes
                .observation
                .into_iter()
                .map(ParsedDimension::try_from)
                .collect::<Result<_, _>>()
                .map_err(|err| err.to_string())?,
        })
    }
}

#[derive(Debug, Deserialize)]
struct SdmxDimensions {
    #[serde(default)]
    series: Vec<SdmxDimension>,
    #[serde(default)]
    observation: Vec<SdmxDimension>,
}

#[derive(Debug, Default, Deserialize)]
struct SdmxAttributes {
    #[serde(default)]
    observation: Vec<SdmxDimension>,
}

#[derive(Debug, Deserialize)]
struct SdmxDimension {
    id: String,
    #[serde(default)]
    values: Vec<SdmxCode>,
}

#[derive(Debug, Deserialize)]
struct SdmxCode {
    id: String,
}

#[derive(Debug, Clone)]
struct ParsedStructure {
    series_dimensions: Vec<ParsedDimension>,
    observation_dimensions: Vec<ParsedDimension>,
    observation_attributes: Vec<ParsedDimension>,
}

impl ParsedStructure {
    fn series_descriptor(&self, key: &str) -> Result<SeriesDescriptor, String> {
        let indexes = parse_colon_indexes(key)?;
        if indexes.len() != self.series_dimensions.len() {
            return Err(format!(
                "series key `{key}` has {} dimensions, expected {}",
                indexes.len(),
                self.series_dimensions.len()
            ));
        }

        let dataflow_id = DataflowId::new("abs.cpi").expect("static dataflow id is valid");
        let mut dimensions = BTreeMap::new();
        let mut measure_id = None;
        for (dimension, index) in self.series_dimensions.iter().zip(indexes) {
            let code = dimension
                .values
                .get(index)
                .ok_or_else(|| format!("series key `{key}` references missing code {index}"))?
                .clone();
            if dimension.id.as_str() == "measure"
                && measure_id
                    .replace(MeasureId::new(code.as_str()).map_err(|err| err.to_string())?)
                    .is_some()
            {
                return Err("ABS SDMX series structure has duplicate `MEASURE` dimensions".into());
            }
            if dimensions.insert(dimension.id.clone(), code).is_some() {
                return Err(format!(
                    "ABS SDMX series structure has duplicate `{}` dimensions",
                    dimension.id.as_str()
                ));
            }
        }
        let measure_id = measure_id.ok_or_else(|| {
            "ABS SDMX series structure is missing `MEASURE` dimension".to_string()
        })?;
        let series_key = SeriesKey::derive(
            &dataflow_id,
            dimensions
                .iter()
                .map(|(dimension, code)| (dimension.as_str(), code.as_str())),
        );

        Ok(SeriesDescriptor {
            series_key,
            dataflow_id,
            unit: measure_id.as_str().to_string(),
            measure_id,
            dimensions,
        })
    }

    fn observation(
        &self,
        descriptor: &SeriesDescriptor,
        key: &str,
        tuple: ObservationTuple,
        artifact: &ArtifactRef,
        ingested_at: DateTime<Utc>,
    ) -> Result<Observation, String> {
        if self.observation_dimensions.len() != 1 {
            return Err(format!(
                "expected one ABS observation dimension, got {}",
                self.observation_dimensions.len()
            ));
        }
        let indexes = parse_colon_indexes(key)?;
        if indexes.len() != 1 {
            return Err(format!(
                "observation key `{key}` has {} dimensions, expected 1",
                indexes.len()
            ));
        }
        let time_dimension = &self.observation_dimensions[0];
        if !time_dimension
            .id
            .as_str()
            .eq_ignore_ascii_case("TIME_PERIOD")
        {
            return Err(format!(
                "expected ABS observation dimension `TIME_PERIOD`, got `{}`",
                time_dimension.id.as_str()
            ));
        }
        let (time, time_precision) = time_dimension
            .time_values
            .as_ref()
            .and_then(|periods| periods.get(indexes[0]).copied())
            .ok_or_else(|| format!("observation key `{key}` references missing time period"))?;
        let attributes = self.attributes(&tuple)?;
        let status = observation_status(tuple.value, attributes.get("OBS_STATUS"))?;
        let value = if status == ObservationStatus::Missing {
            None
        } else {
            tuple.value
        };

        Ok(Observation {
            series_key: descriptor.series_key,
            time,
            time_precision,
            value,
            status,
            revision_no: 0,
            attributes,
            ingested_at,
            source_artifact_id: artifact.id,
        })
    }

    fn attributes(&self, tuple: &ObservationTuple) -> Result<BTreeMap<String, String>, String> {
        if tuple.attribute_indexes.len() > self.observation_attributes.len() {
            return Err(format!(
                "observation tuple has {} attribute indexes, expected at most {}",
                tuple.attribute_indexes.len(),
                self.observation_attributes.len()
            ));
        }
        let mut attributes = BTreeMap::new();
        for (attribute, index) in self
            .observation_attributes
            .iter()
            .zip(tuple.attribute_indexes.iter().copied())
        {
            let Some(index) = index else {
                continue;
            };
            let code = attribute
                .values
                .get(index)
                .ok_or_else(|| format!("observation attribute references missing code {index}"))?;
            let attribute_id = if attribute.id.as_str().eq_ignore_ascii_case("OBS_STATUS") {
                "OBS_STATUS".to_string()
            } else {
                attribute.id.to_string()
            };
            if attributes
                .insert(attribute_id.clone(), code.to_string())
                .is_some()
            {
                return Err(format!(
                    "observation tuple has duplicate `{attribute_id}` attributes"
                ));
            }
        }
        Ok(attributes)
    }
}

#[derive(Debug, Clone)]
struct ParsedDimension {
    id: DimensionId,
    values: Vec<CodeId>,
    time_values: Option<Vec<(DateTime<Utc>, TimePrecision)>>,
}

impl ParsedDimension {
    fn from_series(value: SdmxDimension) -> Result<Self, String> {
        let canonical_id = value.id.to_ascii_lowercase();
        let is_measure = canonical_id == "measure";
        Ok(Self {
            id: DimensionId::new(canonical_id).map_err(|err| err.to_string())?,
            values: value
                .values
                .into_iter()
                .map(|code| {
                    let code_id = if is_measure {
                        code.id.to_ascii_lowercase()
                    } else {
                        code.id
                    };
                    CodeId::new(code_id).map_err(|err| err.to_string())
                })
                .collect::<Result<_, _>>()?,
            time_values: None,
        })
    }
}

impl TryFrom<SdmxDimension> for ParsedDimension {
    type Error = String;

    fn try_from(value: SdmxDimension) -> Result<Self, Self::Error> {
        let id = DimensionId::new(value.id).map_err(|err| err.to_string())?;
        let values = value
            .values
            .into_iter()
            .map(|code| CodeId::new(code.id).map_err(|err| err.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let time_values = if id.as_str().eq_ignore_ascii_case("TIME_PERIOD") {
            Some(
                values
                    .iter()
                    .map(|code| parse_time_period(code.as_str()))
                    .collect::<Result<Vec<_>, _>>()?,
            )
        } else {
            None
        };
        Ok(Self {
            id,
            values,
            time_values,
        })
    }
}

#[derive(Debug)]
struct ObservationTuple {
    value: Option<f64>,
    attribute_indexes: Vec<Option<usize>>,
}

impl<'de> Deserialize<'de> for ObservationTuple {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ObservationTupleVisitor;

        impl<'de> Visitor<'de> for ObservationTupleVisitor {
            type Value = ObservationTuple;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an SDMX observation tuple array")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let value = seq
                    .next_element::<Option<f64>>()?
                    .ok_or_else(|| de::Error::custom("observation tuple is missing value"))?;
                let mut attribute_indexes = Vec::new();
                while let Some(index) = seq.next_element::<Option<usize>>()? {
                    attribute_indexes.push(index);
                }
                Ok(ObservationTuple {
                    value,
                    attribute_indexes,
                })
            }
        }

        deserializer.deserialize_seq(ObservationTupleVisitor)
    }
}

fn parse_colon_indexes(key: &str) -> Result<Vec<usize>, String> {
    key.split(':')
        .map(|part| {
            part.parse::<usize>()
                .map_err(|err| format!("invalid SDMX index `{part}` in `{key}`: {err}"))
        })
        .collect()
}

fn parse_time_period(period: &str) -> Result<(DateTime<Utc>, TimePrecision), String> {
    if let Some((year, quarter)) = period.split_once("-Q") {
        let year = year
            .parse::<i32>()
            .map_err(|err| format!("invalid SDMX year `{year}`: {err}"))?;
        let quarter = quarter
            .parse::<u32>()
            .map_err(|err| format!("invalid SDMX quarter `{quarter}`: {err}"))?;
        let month = match quarter {
            1 => 1,
            2 => 4,
            3 => 7,
            4 => 10,
            _ => return Err(format!("invalid SDMX quarter `{quarter}`")),
        };
        return date_at_midnight(year, month, 1).map(|time| (time, TimePrecision::Quarter));
    }

    if let Some((year, month)) = period.split_once('-') {
        if period.len() == 7 {
            let year = year
                .parse::<i32>()
                .map_err(|err| format!("invalid SDMX year `{year}`: {err}"))?;
            let month = month
                .parse::<u32>()
                .map_err(|err| format!("invalid SDMX month `{month}`: {err}"))?;
            return date_at_midnight(year, month, 1).map(|time| (time, TimePrecision::Month));
        }
        return Err(format!(
            "unsupported ABS CPI TIME_PERIOD `{period}`; expected quarterly `YYYY-Qn` or monthly `YYYY-MM`"
        ));
    }

    Err(format!(
        "unsupported ABS CPI TIME_PERIOD `{period}`; expected quarterly `YYYY-Qn` or monthly `YYYY-MM`"
    ))
}

fn date_at_midnight(year: i32, month: u32, day: u32) -> Result<DateTime<Utc>, String> {
    NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| format!("invalid SDMX date components `{year}-{month}-{day}`"))?
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| format!("invalid SDMX time components for `{year}-{month}-{day}`"))
        .map(|time| time.and_utc())
}

fn observation_status(
    value: Option<f64>,
    status: Option<&String>,
) -> Result<ObservationStatus, String> {
    match status.map(String::as_str) {
        None => Ok(if value.is_none() {
            ObservationStatus::Missing
        } else {
            ObservationStatus::Normal
        }),
        Some(status) if status.eq_ignore_ascii_case("A") => {
            if value.is_none() {
                Err("OBS_STATUS `A` cannot be attached to a null observation value".into())
            } else {
                Ok(ObservationStatus::Normal)
            }
        }
        Some(status) if status.eq_ignore_ascii_case("E") => {
            non_missing_status(value, status, ObservationStatus::Estimated)
        }
        Some(status) if status.eq_ignore_ascii_case("F") => {
            non_missing_status(value, status, ObservationStatus::Forecast)
        }
        Some(status) if status.eq_ignore_ascii_case("I") => {
            non_missing_status(value, status, ObservationStatus::Imputed)
        }
        Some(status) if status.eq_ignore_ascii_case("M") => {
            if value.is_some() {
                Err("OBS_STATUS `M` cannot carry a numeric observation value".into())
            } else {
                Ok(ObservationStatus::Missing)
            }
        }
        Some(status) if status.eq_ignore_ascii_case("P") => {
            non_missing_status(value, status, ObservationStatus::Provisional)
        }
        Some(status) if status.eq_ignore_ascii_case("R") => {
            non_missing_status(value, status, ObservationStatus::Revised)
        }
        Some(status) if status.eq_ignore_ascii_case("B") => {
            non_missing_status(value, status, ObservationStatus::Break)
        }
        Some(status) => Err(format!("unknown ABS OBS_STATUS `{status}`")),
    }
}

fn non_missing_status(
    value: Option<f64>,
    status: &str,
    observation_status: ObservationStatus,
) -> Result<ObservationStatus, String> {
    if value.is_none() {
        Err(format!(
            "OBS_STATUS `{status}` cannot be attached to a null observation value"
        ))
    } else {
        Ok(observation_status)
    }
}

async fn persist_expected_artifact(
    ctx: &FetchCtx,
    artifact: Artifact,
    expected_storage_key: &str,
    cleanup_untracked_storage_key: Option<&str>,
) -> Result<ArtifactRef, AdapterError> {
    let reference = ctx.persist_artifact(artifact.clone()).await?;
    if reference.storage_key == expected_storage_key {
        return Ok(reference);
    }
    let reference_key = StorageKey::from_persisted(reference.storage_key.clone());
    if ctx
        .blob_store
        .matches_artifact_id(&reference_key, artifact.id)
        .await?
    {
        if let Some(cleanup_key) = cleanup_untracked_storage_key {
            if cleanup_key != reference.storage_key {
                ctx.blob_store
                    .delete(&StorageKey::from_persisted(cleanup_key))
                    .await?;
            }
        }
        return Ok(reference);
    }
    ctx.repair_artifact_storage_key(artifact, &reference.storage_key)
        .await
}

/// Builder for [`AbsAdapter`].
#[derive(Debug, Clone)]
pub struct AbsAdapterBuilder {
    base_url: String,
    building_approvals_release_url: String,
    building_activity_release_url: String,
    dwelling_completion_times_url: String,
}

impl Default for AbsAdapterBuilder {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            building_approvals_release_url: DEFAULT_BUILDING_APPROVALS_RELEASE_URL.to_string(),
            building_activity_release_url: DEFAULT_BUILDING_ACTIVITY_RELEASE_URL.to_string(),
            dwelling_completion_times_url: DEFAULT_DWELLING_COMPLETION_TIMES_URL.to_string(),
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

    /// Override the Building Approvals latest-release URL. Intended for fixture tests.
    #[must_use]
    pub fn building_approvals_release_url(mut self, url: impl Into<String>) -> Self {
        self.building_approvals_release_url = url.into();
        self
    }

    /// Override the Building Activity latest-release URL. Intended for fixture tests.
    #[must_use]
    pub fn building_activity_release_url(mut self, url: impl Into<String>) -> Self {
        self.building_activity_release_url = url.into();
        self
    }

    /// Override the dwelling completion times article URL. Intended for fixture tests.
    #[must_use]
    pub fn dwelling_completion_times_url(mut self, url: impl Into<String>) -> Self {
        self.dwelling_completion_times_url = url.into();
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
                dataflows: vec![
                    DataflowId::new(CPI_CANONICAL_DATAFLOW_ID)
                        .expect("static dataflow id is valid"),
                    DataflowId::new(BUILDING_APPROVALS_CANONICAL_DATAFLOW_ID)
                        .expect("static dataflow id is valid"),
                    DataflowId::new(BUILDING_ACTIVITY_CANONICAL_DATAFLOW_ID)
                        .expect("static dataflow id is valid"),
                    DataflowId::new(DWELLING_COMPLETION_TIMES_CANONICAL_DATAFLOW_ID)
                        .expect("static dataflow id is valid"),
                ],
            },
            base_url: self.base_url,
            building_approvals_release_url: self.building_approvals_release_url,
            building_activity_release_url: self.building_activity_release_url,
            dwelling_completion_times_url: self.dwelling_completion_times_url,
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

    fn to_discovered_job(&self, trace_parent: Option<&str>) -> DiscoveredJob {
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
            trace_parent: trace_parent.map(ToOwned::to_owned),
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

#[cfg(test)]
mod tests {
    use super::*;
    use au_kpis_error::{Classify, ErrorClass};
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn parse_blob_stream_reports_worker_panic_as_internal_error() {
        let blob_store = BlobStore::new(InMemory::new());
        let artifact_id = blob_store
            .put_artifact_stream(stream::iter([Ok::<_, io::Error>(Bytes::from_static(
                b"{}",
            ))]))
            .await
            .expect("store fixture artifact");
        let key = StorageKey::canonical_for(&artifact_id);

        let err = parse_blob_stream(
            blob_store,
            key,
            CancellationToken::new(),
            |_reader| -> Result<(), AdapterError> { panic!("synthetic parse worker panic") },
        )
        .await
        .expect_err("worker panic should be returned as an adapter error");

        assert!(matches!(err, AdapterError::Core(CoreError::Io(_))));
        assert_eq!(err.class(), ErrorClass::Transient);
        assert!(err.to_string().contains("ABS parse worker failed"));
    }

    #[tokio::test]
    async fn parse_blob_stream_returns_promptly_when_cancelled_before_read() {
        let blob_store = BlobStore::new(InMemory::new());
        let artifact_id = blob_store
            .put_artifact_stream(stream::iter([Ok::<_, io::Error>(Bytes::from_static(
                b"{}",
            ))]))
            .await
            .expect("store fixture artifact");
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let err = parse_blob_stream(
            blob_store,
            StorageKey::canonical_for(&artifact_id),
            cancellation,
            |_reader| -> Result<(), AdapterError> { Ok(()) },
        )
        .await
        .expect_err("cancelled parse should return an adapter error");

        assert!(matches!(err, AdapterError::Core(CoreError::Io(_))));
        assert_eq!(err.class(), ErrorClass::Transient);
        assert!(err.to_string().contains("ABS parse cancelled"));
    }
}
