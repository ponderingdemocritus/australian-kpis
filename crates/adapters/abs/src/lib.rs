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
    Artifact, CodeId, DataflowId, DimensionId, MeasureId, Observation, ObservationStatus,
    SeriesDescriptor, SeriesKey, SourceId, TimePrecision,
};
use au_kpis_error::CoreError;
use au_kpis_storage::{BlobStore, StorageError, StorageKey};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use futures::{StreamExt, stream};
use serde::{
    Deserialize,
    de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
};

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
                    .raw_artifact()
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
    if let Err(err) = validate_cpi_parse_artifact(&artifact) {
        return Box::pin(stream::once(async move { Err(err) }));
    }

    let blob_store = ctx.blob_store.clone();
    let started_at = ctx.started_at;
    let (row_tx, row_rx) = tokio::sync::mpsc::channel(64);

    tokio::spawn(async move {
        let key = StorageKey::from_persisted(artifact.storage_key.clone());
        if let Err(err) = verify_parse_artifact_identity(&blob_store, &key, &artifact).await {
            let _ = row_tx.send(Err(err)).await;
            return;
        }

        let parse_tx = row_tx.clone();
        let artifact_for_full_parse = artifact.clone();
        let result = match parse_blob_stream(blob_store.clone(), key.clone(), move |reader| {
            parse_sdmx_json(reader, artifact_for_full_parse, started_at, parse_tx)
        })
        .await
        {
            Ok(ParseOutcome::Complete) => Ok(()),
            Ok(ParseOutcome::DataSetsBeforeStructure(structure)) => {
                let parse_tx = row_tx.clone();
                parse_blob_stream(blob_store, key, move |reader| {
                    parse_sdmx_data_sets_with_structure(
                        reader, structure, artifact, started_at, parse_tx,
                    )
                })
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

fn validate_cpi_parse_artifact(artifact: &ArtifactRef) -> Result<(), AdapterError> {
    if artifact.source_id.as_str() != "abs" {
        return Err(AdapterError::Validation(format!(
            "ABS parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }

    match abs_data_url_provenance(&artifact.source_url) {
        Some(("ABS", CPI_DATAFLOW_ID)) => {}
        Some(_) => {
            return Err(AdapterError::Validation(format!(
                "ABS parse artifact `{}` does not match CPI dataflow",
                artifact.source_url
            )));
        }
        None => {
            return Err(AdapterError::Validation(format!(
                "ABS parse artifact `{}` is missing CPI dataflow provenance",
                artifact.source_url
            )));
        }
    }

    Ok(())
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
    parser: F,
) -> Result<T, AdapterError>
where
    T: Send + 'static,
    F: FnOnce(ChannelReader) -> Result<T, AdapterError> + Send + 'static,
{
    let mut chunks = blob_store.get(&key).await?;
    let (byte_tx, byte_rx) = tokio::sync::mpsc::channel(8);
    let read_error = Arc::new(Mutex::new(None));
    let reader_error = Arc::clone(&read_error);
    let parser =
        tokio::task::spawn_blocking(move || parser(ChannelReader::new(byte_rx, reader_error)));

    while let Some(chunk) = chunks.next().await {
        if byte_tx.send(chunk).await.is_err() {
            break;
        }
    }
    drop(byte_tx);

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

fn map_sdmx_json_error(err: serde_json::Error) -> AdapterError {
    AdapterError::FormatDrift(err.to_string())
}

struct ChannelReader {
    rx: tokio::sync::mpsc::Receiver<Result<Bytes, StorageError>>,
    read_error: Arc<Mutex<Option<StorageError>>>,
    current: Option<Bytes>,
    offset: usize,
}

impl ChannelReader {
    fn new(
        rx: tokio::sync::mpsc::Receiver<Result<Bytes, StorageError>>,
        read_error: Arc<Mutex<Option<StorageError>>>,
    ) -> Self {
        Self {
            rx,
            read_error,
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
            self.tx
                .blocking_send(Ok((self.descriptor.clone(), observation)))
                .map_err(|_| de::Error::custom("ABS parse receiver was dropped"))?;
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
        let period = time_dimension
            .values
            .get(indexes[0])
            .ok_or_else(|| format!("observation key `{key}` references missing time period"))?;
        let (time, time_precision) = parse_time_period(period.as_str())?;
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
        })
    }
}

impl TryFrom<SdmxDimension> for ParsedDimension {
    type Error = String;

    fn try_from(value: SdmxDimension) -> Result<Self, Self::Error> {
        Ok(Self {
            id: DimensionId::new(value.id).map_err(|err| err.to_string())?,
            values: value
                .values
                .into_iter()
                .map(|code| CodeId::new(code.id).map_err(|err| err.to_string()))
                .collect::<Result<_, _>>()?,
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

        let err = parse_blob_stream(blob_store, key, |_reader| -> Result<(), AdapterError> {
            panic!("synthetic parse worker panic")
        })
        .await
        .expect_err("worker panic should be returned as an adapter error");

        assert!(matches!(err, AdapterError::Core(CoreError::Io(_))));
        assert_eq!(err.class(), ErrorClass::Transient);
        assert!(err.to_string().contains("ABS parse worker failed"));
    }
}
