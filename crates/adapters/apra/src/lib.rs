//! APRA adapter for quarterly XLS releases.

#![forbid(unsafe_code)]
#![deny(missing_docs, missing_debug_implementations)]

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Cursor},
    panic::{self, AssertUnwindSafe},
    time::Duration,
};

use async_trait::async_trait;
use au_kpis_adapter::{
    AdapterError, AdapterManifest, ArtifactRef, DiscoveredJob, DiscoveryCtx, FetchCtx,
    ObservationStream, ParseCtx, RateLimit, SourceAdapter, UpstreamRevision,
    capture_response_headers, retry_after_delta, validate_xlsx_workbook_cell_refs,
};
use au_kpis_domain::{
    Artifact, ArtifactId, CodeId, Dataflow, DataflowId, DimensionId, Frequency, License, MeasureId,
    Observation, ObservationStatus, SeriesDescriptor, SeriesKey, Source, SourceId, TimePrecision,
};
use au_kpis_error::CoreError;
use au_kpis_storage::{BlobStore, StorageKey};
use calamine::{Data, ExcelDateTime, ExcelDateTimeType, Reader, open_workbook_auto_from_rs};
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use futures::{StreamExt, stream};

const DEFAULT_RELEASE_URL: &str =
    "https://www.apra.gov.au/quarterly-authorised-deposit-taking-institution-statistics";
const DEFAULT_SUPER_RELEASE_URL: &str =
    "https://www.apra.gov.au/news-and-publications/quarterly-superannuation-statistics";
const USER_AGENT: &str = concat!("au-kpis-adapter-apra/", env!("CARGO_PKG_VERSION"));
const DATAFLOW_ID: &str = "apra.quarterly_statistics";
const SUPER_ASSET_ALLOCATION_DATAFLOW_ID: &str = "apra.super_asset_allocation";
const ATTRIBUTION: &str = "Source: Australian Prudential Regulation Authority";
const LICENSE_NAME: &str = "Creative Commons Attribution 3.0 Australia Licence";
const LICENSE_URL: &str = "http://creativecommons.org/licenses/by/3.0/au/";

/// APRA quarterly XLS adapter.
#[derive(Debug, Clone)]
pub struct ApraAdapter {
    manifest: AdapterManifest,
    release_url: String,
    super_release_url: String,
}

impl Default for ApraAdapter {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ApraAdapter {
    /// Start building an APRA adapter.
    #[must_use]
    pub fn builder() -> ApraAdapterBuilder {
        ApraAdapterBuilder::default()
    }

    /// Parse an APRA release-calendar page into XLS release links.
    pub fn parse_release_calendar(body: &str) -> Result<Vec<ApraRelease>, AdapterError> {
        parse_release_calendar_with_base(body, DEFAULT_RELEASE_URL)
    }

    /// Convert discovered releases into jobs for the supplied discovery timestamp.
    #[must_use]
    pub fn current_jobs_with_started_at(
        current: &[ApraRelease],
        started_at: DateTime<Utc>,
    ) -> Vec<DiscoveredJob> {
        Self::discoverable_jobs_with_started_at(current, &BTreeMap::new(), started_at, None)
    }

    /// Diff current APRA release links against stored upstream revisions.
    #[must_use]
    pub fn discoverable_jobs_with_started_at(
        current: &[ApraRelease],
        known_revisions: &BTreeMap<String, UpstreamRevision>,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
    ) -> Vec<DiscoveredJob> {
        discoverable_jobs_with_release_url(
            current,
            known_revisions,
            started_at,
            trace_parent,
            DEFAULT_RELEASE_URL,
            ApraDataflow::QuarterlyStatistics,
            None,
        )
    }

    /// Static metadata for the APRA quarterly statistics dataflow.
    #[must_use]
    pub fn dataflow_metadata(&self) -> Vec<Dataflow> {
        vec![
            Dataflow {
                id: dataflow_id(),
                source_id: source_id(),
                name: "APRA quarterly statistics".into(),
                description: Some(
                    "Quarterly APRA authorised deposit-taking institution statistics from XLS releases."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("publication").expect("static dimension id is valid"),
                    DimensionId::new("table").expect("static dimension id is valid"),
                    DimensionId::new("series").expect("static dimension id is valid"),
                    DimensionId::new("entity").expect("static dimension id is valid"),
                    DimensionId::new("sector").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new("value").expect("static measure id is valid")],
                frequency: Frequency::Quarterly,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: DEFAULT_RELEASE_URL.into(),
            },
            Dataflow {
                id: super_asset_allocation_dataflow_id(),
                source_id: source_id(),
                name: "APRA superannuation asset allocation".into(),
                description: Some(
                    "Quarterly APRA superannuation asset-allocation rows mapped to the APS productive-infrastructure cut."
                        .into(),
                ),
                dimensions: vec![
                    DimensionId::new("fund_type").expect("static dimension id is valid"),
                    DimensionId::new("asset_category").expect("static dimension id is valid"),
                    DimensionId::new("mapping").expect("static dimension id is valid"),
                ],
                measures: vec![MeasureId::new("value").expect("static measure id is valid")],
                frequency: Frequency::Quarterly,
                license: License::Other(LICENSE_NAME.into()),
                attribution: ATTRIBUTION.into(),
                source_url: DEFAULT_SUPER_RELEASE_URL.into(),
            },
        ]
    }

    fn release_url(&self) -> &str {
        &self.release_url
    }

    fn super_release_url(&self) -> &str {
        &self.super_release_url
    }

    fn validate_fetch_job(&self, job: &DiscoveredJob) -> Result<(), AdapterError> {
        if job.source_id != self.manifest.source_id {
            return Err(AdapterError::Validation(format!(
                "APRA fetch received job for source `{}`",
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
                "APRA fetch received unsupported dataflow `{}`",
                job.dataflow_id.as_str()
            )));
        }
        let provenance = release_url_provenance_for_fetch(&job.source_url).ok_or_else(|| {
            AdapterError::Validation(format!(
                "APRA fetch URL `{}` is not a release XLS artifact",
                job.source_url
            ))
        })?;
        if job.dataflow_id == super_asset_allocation_dataflow_id()
            && provenance.publication_slug != "super-performance"
        {
            return Err(AdapterError::Validation(format!(
                "APRA super asset-allocation fetch received publication `{}`",
                provenance.publication_slug
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl SourceAdapter for ApraAdapter {
    fn id(&self) -> &'static str {
        "apra"
    }

    fn manifest(&self) -> &AdapterManifest {
        &self.manifest
    }

    fn source_metadata(&self) -> Option<Source> {
        Some(Source {
            id: source_id(),
            name: "Australian Prudential Regulation Authority".into(),
            homepage: "https://www.apra.gov.au".into(),
            description: Some(
                "Australian prudential regulator for banks, insurers, and superannuation.".into(),
            ),
        })
    }

    fn dataflow_metadata(&self) -> Vec<Dataflow> {
        ApraAdapter::dataflow_metadata(self)
    }

    #[tracing::instrument(skip(self, ctx), fields(source = self.id()))]
    async fn discover(&self, ctx: &DiscoveryCtx) -> Result<Vec<DiscoveredJob>, AdapterError> {
        let Some((release_url, dataflow, publication_filter)) =
            self.discovery_target(ctx.requested_dataflow_id())
        else {
            return Ok(Vec::new());
        };
        let response = ctx
            .http
            .execute(
                ctx.http
                    .raw()
                    .get(release_url)
                    .header("user-agent", USER_AGENT)
                    .header("accept", "text/html,application/xhtml+xml"),
            )
            .await?
            .error_for_status()?;
        let body = response.text().await?;
        let releases = parse_release_calendar_with_base(&body, release_url)?;
        Ok(discoverable_jobs_with_release_url(
            &releases,
            ctx.known_revisions(),
            ctx.started_at,
            ctx.trace_parent(),
            release_url,
            dataflow,
            publication_filter,
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
                    .header(
                        "accept",
                        "application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    ),
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
                || "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".to_string(),
                str::to_string,
            );

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

impl ApraAdapter {
    fn discovery_target(
        &self,
        requested: Option<&DataflowId>,
    ) -> Option<(&str, ApraDataflow, Option<&'static str>)> {
        match requested {
            Some(dataflow) if dataflow == &dataflow_id() => {
                Some((self.release_url(), ApraDataflow::QuarterlyStatistics, None))
            }
            Some(dataflow) if dataflow == &super_asset_allocation_dataflow_id() => Some((
                self.super_release_url(),
                ApraDataflow::SuperAssetAllocation,
                Some("super-performance"),
            )),
            Some(_) => None,
            None => Some((self.release_url(), ApraDataflow::QuarterlyStatistics, None)),
        }
    }
}

fn parse_artifact_stream(artifact: ArtifactRef, ctx: &ParseCtx) -> ObservationStream<'_> {
    let plan = match validate_parse_artifact(&artifact, ctx) {
        Ok(plan) => plan,
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
            parse_xls_artifact(blob_store, key, artifact, plan, started_at, row_tx.clone()).await;
        if let Err(err) = result {
            let _ = row_tx.send(Err(err)).await;
        }
    });

    Box::pin(stream::unfold(row_rx, |mut row_rx| async {
        row_rx.recv().await.map(|item| (item, row_rx))
    }))
}

async fn parse_xls_artifact(
    blob_store: BlobStore,
    key: StorageKey,
    artifact: ArtifactRef,
    plan: ApraParsePlan,
    ingested_at: DateTime<Utc>,
    tx: tokio::sync::mpsc::Sender<Result<(SeriesDescriptor, Observation), AdapterError>>,
) -> Result<(), AdapterError> {
    let mut chunks = blob_store.get(&key).await?;
    let mut bytes = Vec::new();
    while let Some(chunk) = chunks.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    let rows =
        tokio::task::spawn_blocking(move || parse_xls_workbook(bytes, artifact, plan, ingested_at))
            .await
            .map_err(parse_worker_error)??;
    for row in rows {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

fn parse_xls_workbook(
    bytes: Vec<u8>,
    artifact: ArtifactRef,
    plan: ApraParsePlan,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    catch_xls_parser_panic(|| match plan.dataflow {
        ApraDataflow::QuarterlyStatistics => {
            parse_xls_workbook_inner(bytes, artifact, plan.provenance, ingested_at)
        }
        ApraDataflow::SuperAssetAllocation => {
            parse_super_asset_allocation_workbook(bytes, artifact, plan.provenance, ingested_at)
        }
    })
}

fn parse_xls_workbook_inner(
    bytes: Vec<u8>,
    artifact: ArtifactRef,
    provenance: ApraReleaseProvenance,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    validate_xlsx_workbook_cell_refs(&bytes, "APRA")?;
    let mut workbook = open_workbook_auto_from_rs(Cursor::new(bytes))
        .map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
    let sheet_names = workbook.sheet_names().to_vec();
    if sheet_names.is_empty() {
        return Err(AdapterError::FormatDrift(
            "APRA workbook has no worksheets".into(),
        ));
    }

    let mut parsed = Vec::new();
    for sheet_name in sheet_names {
        if should_skip_sheet(&sheet_name) {
            continue;
        }
        let range = workbook
            .worksheet_range(&sheet_name)
            .map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
        let rows = range
            .rows()
            .map(|row| row.iter().map(cell_to_string).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        if rows
            .iter()
            .all(|row| row.iter().all(|cell| cell.is_empty()))
        {
            continue;
        }

        let schema_hash = schema_hash_for_sheet(&sheet_name, &rows);
        let table_title = table_title_for_sheet(&sheet_name, &rows);
        let sheet = SheetParseContext {
            sheet_name: &sheet_name,
            table_title: &table_title,
            schema_hash: &schema_hash,
            artifact: &artifact,
            provenance: &provenance,
            ingested_at,
        };
        if let Some(rows) = parse_vertical_table(&rows, &sheet)? {
            parsed.extend(rows);
            continue;
        }
        if let Some(rows) = parse_horizontal_table(&rows, &sheet)? {
            parsed.extend(rows);
        }
    }

    if parsed.is_empty() {
        return Err(AdapterError::FormatDrift(
            "APRA workbook contains no recognised quarterly XLS tables".into(),
        ));
    }
    Ok(parsed)
}

fn parse_super_asset_allocation_workbook(
    bytes: Vec<u8>,
    artifact: ArtifactRef,
    provenance: ApraReleaseProvenance,
    ingested_at: DateTime<Utc>,
) -> Result<Vec<(SeriesDescriptor, Observation)>, AdapterError> {
    validate_xlsx_workbook_cell_refs(&bytes, "APRA")?;
    let mut workbook = open_workbook_auto_from_rs(Cursor::new(bytes))
        .map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
    let mut parsed = Vec::new();
    let mut onshore_totals: BTreeMap<DateTime<Utc>, f64> = BTreeMap::new();
    let sheet_names = workbook.sheet_names().to_vec();
    for sheet_name in sheet_names {
        if should_skip_sheet(&sheet_name) {
            continue;
        }
        let range = workbook
            .worksheet_range(&sheet_name)
            .map_err(|err| AdapterError::FormatDrift(err.to_string()))?;
        let rows = range
            .rows()
            .map(|row| row.iter().map(cell_to_string).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let table_title = table_title_for_sheet(&sheet_name, &rows);
        if !normalize_header(&format!("{sheet_name} {table_title}")).contains("asset allocation") {
            continue;
        }
        let Some(periods) = find_horizontal_period_columns(&rows)? else {
            continue;
        };
        let unit = unit_from_rows(&rows[..periods.row_index]);
        let first_period_col = periods
            .columns
            .first()
            .map(|(column, _)| *column)
            .expect("period finder returns at least one column");
        let schema_hash = schema_hash_for_sheet(&sheet_name, &rows);
        for row in rows.iter().skip(periods.row_index + 1) {
            let Some(original_category) = label_before(row, first_period_col) else {
                continue;
            };
            let Some(mapping) = super_asset_category_mapping(&original_category) else {
                continue;
            };
            for (column, (time, precision)) in &periods.columns {
                let cell = row.get(*column).map_or("", String::as_str);
                let (value, status) = parse_value(cell)?;
                if mapping.included_in_onshore_total {
                    if let Some(value) = value {
                        *onshore_totals.entry(*time).or_insert(0.0) += value;
                    }
                }
                parsed.push(build_super_asset_allocation_row(SuperAssetAllocationRow {
                    provenance: &provenance,
                    table_title: &table_title,
                    sheet_name: &sheet_name,
                    original_category: &original_category,
                    category: mapping.category,
                    mapping: mapping.mapping,
                    mapping_rule: mapping.rule,
                    unit: &unit,
                    time: *time,
                    precision: *precision,
                    value,
                    status,
                    schema_hash: &schema_hash,
                    artifact: &artifact,
                    ingested_at,
                })?);
            }
        }
    }

    for (time, value) in onshore_totals {
        parsed.push(build_super_asset_allocation_row(SuperAssetAllocationRow {
            provenance: &provenance,
            table_title: "Derived productive infrastructure total",
            sheet_name: "derived",
            original_category: "Productive infrastructure onshore total",
            category: "total",
            mapping: "productive_infrastructure_onshore",
            mapping_rule: "sum_included_onshore_infrastructure_categories",
            unit: "$ million",
            time,
            precision: TimePrecision::Quarter,
            value: Some(value),
            status: ObservationStatus::Normal,
            schema_hash: "derived",
            artifact: &artifact,
            ingested_at,
        })?);
    }

    if parsed.is_empty() {
        return Err(AdapterError::FormatDrift(
            "APRA super workbook contains no mapped asset-allocation rows".into(),
        ));
    }
    Ok(parsed)
}

fn catch_xls_parser_panic<T>(
    parse: impl FnOnce() -> Result<T, AdapterError>,
) -> Result<T, AdapterError> {
    panic::catch_unwind(AssertUnwindSafe(parse)).unwrap_or_else(|_| {
        Err(AdapterError::FormatDrift(
            "APRA workbook parser panicked while reading malformed XLS/XLSX".into(),
        ))
    })
}

/// Parse one arbitrary XLS/XLSX byte slice through the APRA XLS parser core for
/// cargo-fuzz.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn parse_xls_bytes_for_fuzz(bytes: &[u8]) -> Result<usize, AdapterError> {
    let id = ArtifactId::of_content(bytes);
    let source_url = "https://www.apra.gov.au/sites/default/files/centralised.xlsx";
    let artifact = ArtifactRef {
        id,
        fetch_id: None,
        source_id: SourceId::new("apra").expect("static source id is valid"),
        source_url: source_url.into(),
        content_type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".into(),
        response_headers: BTreeMap::new(),
        storage_key: StorageKey::canonical_for(&id).to_string(),
        size_bytes: bytes.len() as u64,
        fetched_at: fuzz_ingested_at(),
    };
    let provenance =
        release_url_provenance_for_parse(source_url).expect("static APRA URL has provenance");
    parse_xls_workbook(bytes.to_vec(), artifact, provenance, fuzz_ingested_at())
        .map(|rows| rows.len())
}

#[cfg(feature = "fuzzing")]
fn fuzz_ingested_at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
        .single()
        .expect("valid fuzz timestamp")
}

struct SheetParseContext<'a> {
    sheet_name: &'a str,
    table_title: &'a str,
    schema_hash: &'a str,
    artifact: &'a ArtifactRef,
    provenance: &'a ApraReleaseProvenance,
    ingested_at: DateTime<Utc>,
}

fn parse_vertical_table(
    rows: &[Vec<String>],
    sheet: &SheetParseContext<'_>,
) -> Result<Option<Vec<(SeriesDescriptor, Observation)>>, AdapterError> {
    let Some(header_index) = rows.iter().position(|row| is_vertical_header(row)) else {
        return Ok(None);
    };
    let header = &rows[header_index];
    let Some(period_col) = find_header_col(header, &["entity quarter end"])
        .or_else(|| find_header_col(header, &["period"]))
    else {
        return Ok(None);
    };
    let entity_col = find_header_col(header, &["entity"]);
    let sector_col = find_header_col(header, &["sector"]);
    let abn_col = find_header_col(header, &["abn"]);
    let mutual_col = find_header_col(header, &["mutual bank (y/n)"]);
    let mut descriptor_cols = find_header_cols(header, &["entity quarter end", "period"])
        .into_iter()
        .collect::<BTreeSet<_>>();
    descriptor_cols.extend(
        [entity_col, sector_col, abn_col, mutual_col]
            .into_iter()
            .flatten(),
    );
    let measure_cols = header
        .iter()
        .enumerate()
        .filter(|(index, cell)| !descriptor_cols.contains(index) && !cell.trim().is_empty())
        .map(|(index, cell)| (index, cell.trim().to_string()))
        .collect::<Vec<_>>();
    if measure_cols.is_empty() {
        return Err(AdapterError::FormatDrift(format!(
            "APRA sheet `{}` has no vertical measure columns",
            sheet.sheet_name
        )));
    }

    let unit = unit_from_rows(&rows[..header_index]);
    let mut parsed = Vec::new();
    for row in rows.iter().skip(header_index + 1) {
        let Some(period_cell) = row.get(period_col).map(String::as_str) else {
            continue;
        };
        let Some((time, precision)) = parse_apra_quarter(period_cell)? else {
            continue;
        };
        let entity = entity_col
            .and_then(|index| row.get(index))
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or("aggregate");
        let sector = sector_col
            .and_then(|index| row.get(index))
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or("all");
        for (column, measure_name) in &measure_cols {
            let value_cell = row.get(*column).map_or("", String::as_str);
            let (value, status) = parse_value(value_cell)?;
            parsed.push(build_row(BuildRow {
                publication_slug: &sheet.provenance.publication_slug,
                publication_title: &sheet.provenance.publication_title,
                table_title: sheet.table_title,
                sheet_name: sheet.sheet_name,
                series_name: measure_name,
                entity,
                sector,
                unit: &unit,
                time,
                precision,
                value,
                status,
                schema_hash: sheet.schema_hash,
                artifact: sheet.artifact,
                ingested_at: sheet.ingested_at,
                extra_attributes: BTreeMap::from([
                    ("apra_table_orientation".into(), "vertical".into()),
                    ("apra_measure".into(), measure_name.clone()),
                    (
                        "apra_abn".into(),
                        abn_col
                            .and_then(|index| row.get(index))
                            .map_or_else(String::new, |value| value.trim().to_string()),
                    ),
                    (
                        "apra_mutual_bank".into(),
                        mutual_col
                            .and_then(|index| row.get(index))
                            .map_or_else(String::new, |value| value.trim().to_string()),
                    ),
                ]),
            })?);
        }
    }

    if parsed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(parsed))
    }
}

fn parse_horizontal_table(
    rows: &[Vec<String>],
    sheet: &SheetParseContext<'_>,
) -> Result<Option<Vec<(SeriesDescriptor, Observation)>>, AdapterError> {
    let Some(periods) = find_horizontal_period_columns(rows)? else {
        return Ok(None);
    };
    let unit = unit_from_rows(&rows[..periods.row_index]);
    let first_period_col = periods
        .columns
        .first()
        .map(|(column, _)| *column)
        .expect("period finder returns at least one column");
    let mut parsed = Vec::new();

    for row in rows.iter().skip(periods.row_index + 1) {
        let series_name = label_before(row, first_period_col);
        let Some(series_name) = series_name.as_deref() else {
            continue;
        };
        let mut row_values = Vec::new();
        let mut numeric_values = 0_usize;
        let mut invalid_value = None;
        for (column, (time, precision)) in &periods.columns {
            let cell = row.get(*column).map_or("", String::as_str);
            match parse_value(cell) {
                Ok((value, status)) => {
                    if value.is_some() {
                        numeric_values += 1;
                    }
                    row_values.push((*time, *precision, value, status));
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
        for (time, precision, value, status) in row_values {
            parsed.push(build_row(BuildRow {
                publication_slug: &sheet.provenance.publication_slug,
                publication_title: &sheet.provenance.publication_title,
                table_title: sheet.table_title,
                sheet_name: sheet.sheet_name,
                series_name,
                entity: "aggregate",
                sector: "all",
                unit: &unit,
                time,
                precision,
                value,
                status,
                schema_hash: sheet.schema_hash,
                artifact: sheet.artifact,
                ingested_at: sheet.ingested_at,
                extra_attributes: BTreeMap::from([
                    ("apra_table_orientation".into(), "horizontal".into()),
                    ("apra_series_name".into(), series_name.to_string()),
                ]),
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
    publication_slug: &'a str,
    publication_title: &'a str,
    table_title: &'a str,
    sheet_name: &'a str,
    series_name: &'a str,
    entity: &'a str,
    sector: &'a str,
    unit: &'a str,
    time: DateTime<Utc>,
    precision: TimePrecision,
    value: Option<f64>,
    status: ObservationStatus,
    schema_hash: &'a str,
    artifact: &'a ArtifactRef,
    ingested_at: DateTime<Utc>,
    extra_attributes: BTreeMap<String, String>,
}

fn build_row(input: BuildRow<'_>) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let dataflow_id = dataflow_id();
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("publication").expect("static dimension id is valid"),
            apra_code_id("publication", input.publication_slug)?,
        ),
        (
            DimensionId::new("table").expect("static dimension id is valid"),
            apra_code_id("table", &slugify_code(input.table_title))?,
        ),
        (
            DimensionId::new("series").expect("static dimension id is valid"),
            apra_code_id("series", &slugify_code(input.series_name))?,
        ),
        (
            DimensionId::new("entity").expect("static dimension id is valid"),
            apra_code_id("entity", &slugify_code(input.entity))?,
        ),
        (
            DimensionId::new("sector").expect("static dimension id is valid"),
            apra_code_id("sector", &slugify_code(input.sector))?,
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
        (
            "source".into(),
            "Australian Prudential Regulation Authority".into(),
        ),
        ("source_url".into(), input.artifact.source_url.clone()),
        ("license".into(), LICENSE_NAME.into()),
        ("license_url".into(), LICENSE_URL.into()),
        ("attribution".into(), ATTRIBUTION.into()),
        ("publication".into(), input.publication_slug.to_string()),
        (
            "publication_title".into(),
            input.publication_title.to_string(),
        ),
        ("table_title".into(), input.table_title.to_string()),
        ("sheet_name".into(), input.sheet_name.to_string()),
        ("schema_hash".into(), input.schema_hash.to_string()),
        ("apra_entity".into(), input.entity.to_string()),
        ("apra_sector".into(), input.sector.to_string()),
    ]);
    attributes.extend(
        input
            .extra_attributes
            .into_iter()
            .filter(|(_, value)| !value.is_empty()),
    );
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

struct SuperAssetAllocationRow<'a> {
    provenance: &'a ApraReleaseProvenance,
    table_title: &'a str,
    sheet_name: &'a str,
    original_category: &'a str,
    category: &'a str,
    mapping: &'a str,
    mapping_rule: &'a str,
    unit: &'a str,
    time: DateTime<Utc>,
    precision: TimePrecision,
    value: Option<f64>,
    status: ObservationStatus,
    schema_hash: &'a str,
    artifact: &'a ArtifactRef,
    ingested_at: DateTime<Utc>,
}

fn build_super_asset_allocation_row(
    input: SuperAssetAllocationRow<'_>,
) -> Result<(SeriesDescriptor, Observation), AdapterError> {
    let dataflow_id = super_asset_allocation_dataflow_id();
    let dimensions = BTreeMap::from([
        (
            DimensionId::new("fund_type").expect("static dimension id is valid"),
            apra_code_id("fund type", "all")?,
        ),
        (
            DimensionId::new("asset_category").expect("static dimension id is valid"),
            apra_code_id("asset category", input.category)?,
        ),
        (
            DimensionId::new("mapping").expect("static dimension id is valid"),
            apra_code_id("mapping", input.mapping)?,
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
    let attributes = BTreeMap::from([
        (
            "source".into(),
            "Australian Prudential Regulation Authority".into(),
        ),
        ("source_url".into(), input.artifact.source_url.clone()),
        ("license".into(), LICENSE_NAME.into()),
        ("license_url".into(), LICENSE_URL.into()),
        ("attribution".into(), ATTRIBUTION.into()),
        (
            "publication".into(),
            input.provenance.publication_slug.to_string(),
        ),
        (
            "publication_title".into(),
            input.provenance.publication_title.to_string(),
        ),
        ("table_title".into(), input.table_title.to_string()),
        ("sheet_name".into(), input.sheet_name.to_string()),
        ("schema_hash".into(), input.schema_hash.to_string()),
        (
            "apra_original_category".into(),
            input.original_category.to_string(),
        ),
        (
            "apra_mapping_review".into(),
            "aps-v1-super-asset-allocation".into(),
        ),
        ("apra_mapping_rule".into(), input.mapping_rule.to_string()),
    ]);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SuperAssetCategoryMapping {
    category: &'static str,
    mapping: &'static str,
    included_in_onshore_total: bool,
    rule: &'static str,
}

fn super_asset_category_mapping(category: &str) -> Option<SuperAssetCategoryMapping> {
    let normalized = normalize_header(category);
    if normalized.contains("australian infrastructure")
        || normalized.contains("domestic infrastructure")
        || normalized.contains("onshore infrastructure")
    {
        return Some(SuperAssetCategoryMapping {
            category: "australian_infrastructure",
            mapping: "productive_infrastructure_onshore",
            included_in_onshore_total: true,
            rule: "include_explicit_onshore_infrastructure",
        });
    }
    if normalized.contains("overseas infrastructure")
        || normalized.contains("international infrastructure")
        || normalized.contains("offshore infrastructure")
    {
        return Some(SuperAssetCategoryMapping {
            category: "overseas_infrastructure",
            mapping: "productive_infrastructure_offshore",
            included_in_onshore_total: false,
            rule: "exclude_offshore_infrastructure_from_onshore_cut",
        });
    }
    if normalized.contains("infrastructure") {
        return Some(SuperAssetCategoryMapping {
            category: "infrastructure_unclassified",
            mapping: "productive_infrastructure_unclassified",
            included_in_onshore_total: false,
            rule: "exclude_unclassified_infrastructure_until_jurisdiction_review",
        });
    }
    if normalized.contains("property") {
        return Some(SuperAssetCategoryMapping {
            category: "property",
            mapping: "non_infrastructure_real_asset",
            included_in_onshore_total: false,
            rule: "exclude_property_from_infrastructure_cut",
        });
    }
    if normalized.contains("cash") {
        return Some(SuperAssetCategoryMapping {
            category: "cash",
            mapping: "liquidity",
            included_in_onshore_total: false,
            rule: "exclude_cash_from_productive_infrastructure_cut",
        });
    }
    None
}

fn validate_parse_artifact(
    artifact: &ArtifactRef,
    ctx: &ParseCtx,
) -> Result<ApraParsePlan, AdapterError> {
    if artifact.source_id.as_str() != "apra" {
        return Err(AdapterError::Validation(format!(
            "APRA parse received artifact for source `{}`",
            artifact.source_id.as_str()
        )));
    }
    let provenance = release_url_provenance_for_parse(&artifact.source_url).ok_or_else(|| {
        AdapterError::Validation(format!(
            "APRA parse artifact `{}` is missing APRA release provenance",
            artifact.source_url
        ))
    })?;
    let dataflow = match ctx.expected_dataflow_id() {
        Some(expected) if expected == &dataflow_id() => ApraDataflow::QuarterlyStatistics,
        Some(expected) if expected == &super_asset_allocation_dataflow_id() => {
            ApraDataflow::SuperAssetAllocation
        }
        Some(expected) => {
            return Err(AdapterError::Validation(format!(
                "APRA parse expected unsupported dataflow `{}`",
                expected.as_str()
            )));
        }
        None if provenance.publication_slug == "super-performance" => {
            ApraDataflow::SuperAssetAllocation
        }
        None => ApraDataflow::QuarterlyStatistics,
    };
    if dataflow == ApraDataflow::SuperAssetAllocation
        && provenance.publication_slug != "super-performance"
    {
        return Err(AdapterError::Validation(format!(
            "APRA super asset-allocation parse received publication `{}`",
            provenance.publication_slug
        )));
    }
    Ok(ApraParsePlan {
        provenance,
        dataflow,
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
            "APRA parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )));
    }

    if blob_store.matches_artifact_id(key, artifact.id).await? {
        Ok(())
    } else {
        Err(AdapterError::Validation(format!(
            "APRA parse artifact storage key `{}` does not match artifact id `{}`",
            artifact.storage_key, artifact.id
        )))
    }
}

fn parse_release_calendar_with_base(
    body: &str,
    base_url: &str,
) -> Result<Vec<ApraRelease>, AdapterError> {
    let mut releases = Vec::new();
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
        let Some(provenance) = release_url_provenance_for_fetch(&source_url) else {
            continue;
        };
        let title = clean_html_text(text).unwrap_or_else(|| provenance.publication_title.clone());
        releases.push(ApraRelease {
            publication_slug: provenance.publication_slug,
            title,
            format: ApraReleaseFormat::Xls,
            source_url,
            last_updated: attr_value(attrs, "data-updated")
                .or_else(|| attr_value(attrs, "datetime")),
        });
    }
    releases.sort_by(|left, right| {
        left.publication_slug
            .cmp(&right.publication_slug)
            .then(left.source_url.cmp(&right.source_url))
    });
    releases.dedup_by(|left, right| left.source_url == right.source_url);
    Ok(releases)
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
    let cleaned = decode_url_component(&out)
        .replace("&amp;", "&")
        .replace("&nbsp;", " ")
        .replace("&ndash;", "-")
        .replace("&#8211;", "-")
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
            AdapterError::Validation(format!("APRA release URL `{base_url}` is not absolute"))
        })?;
        let path_start = base_url[scheme_end + 3..]
            .find('/')
            .map_or(base_url.len(), |index| scheme_end + 3 + index);
        return Ok(format!("{}{}", &base_url[..path_start], href));
    }
    let Some((prefix, _)) = base_url.rsplit_once('/') else {
        return Err(AdapterError::Validation(format!(
            "APRA release URL `{base_url}` has no path separator"
        )));
    };
    Ok(format!("{prefix}/{href}"))
}

#[derive(Debug, Clone)]
struct ApraReleaseProvenance {
    publication_slug: String,
    publication_title: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApraDataflow {
    QuarterlyStatistics,
    SuperAssetAllocation,
}

impl ApraDataflow {
    fn id(self) -> DataflowId {
        match self {
            Self::QuarterlyStatistics => dataflow_id(),
            Self::SuperAssetAllocation => super_asset_allocation_dataflow_id(),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::QuarterlyStatistics => DATAFLOW_ID,
            Self::SuperAssetAllocation => SUPER_ASSET_ALLOCATION_DATAFLOW_ID,
        }
    }
}

#[derive(Debug, Clone)]
struct ApraParsePlan {
    provenance: ApraReleaseProvenance,
    dataflow: ApraDataflow,
}

fn release_url_provenance_for_fetch(source_url: &str) -> Option<ApraReleaseProvenance> {
    release_url_provenance(source_url, false)
}

fn release_url_provenance_for_parse(source_url: &str) -> Option<ApraReleaseProvenance> {
    release_url_provenance(source_url, true)
}

fn release_url_provenance(
    source_url: &str,
    require_apra_host: bool,
) -> Option<ApraReleaseProvenance> {
    let (_, after_scheme) = source_url.split_once("://")?;
    let (host, path) = after_scheme.split_once('/')?;
    if require_apra_host && !matches!(host, "www.apra.gov.au" | "apra.gov.au") {
        return None;
    }
    if !is_apra_artifact_path(path) {
        return None;
    }
    let filename = path
        .rsplit('/')
        .next()?
        .split('?')
        .next()?
        .split('#')
        .next()?;
    let stem = filename
        .strip_suffix(".xlsx")
        .or_else(|| filename.strip_suffix(".xls"))?;
    let title = decode_url_component(stem).replace(['_', '-'], " ");
    let normalized = title.to_ascii_lowercase();
    let publication_slug = if normalized.contains("superannuation")
        && normalized.contains("performance")
        && normalized.contains("statistics")
    {
        "super-performance".to_string()
    } else if normalized.contains("mysuper") && normalized.contains("statistics") {
        "super-mysuper".to_string()
    } else if normalized.contains("centralised") {
        "adi-centralised".to_string()
    } else if normalized.contains("property")
        && normalized.contains("exposure")
        && normalized.contains("historical")
    {
        "adi-property-exposures-historical".to_string()
    } else if normalized.contains("property") && normalized.contains("exposure") {
        "adi-property-exposures".to_string()
    } else if normalized.contains("performance") {
        "adi-performance".to_string()
    } else {
        slugify_code(&title).to_ascii_lowercase()
    };
    Some(ApraReleaseProvenance {
        publication_slug,
        publication_title: title.split_whitespace().collect::<Vec<_>>().join(" "),
    })
}

fn is_apra_artifact_path(path: &str) -> bool {
    path.starts_with("sites/default/files/") || path.starts_with("system/files/")
}

fn discoverable_jobs_with_release_url(
    current: &[ApraRelease],
    known_revisions: &BTreeMap<String, UpstreamRevision>,
    started_at: DateTime<Utc>,
    trace_parent: Option<&str>,
    release_url: &str,
    dataflow: ApraDataflow,
    publication_filter: Option<&str>,
) -> Vec<DiscoveredJob> {
    current
        .iter()
        .filter(|release| {
            publication_filter
                .is_none_or(|publication_slug| release.publication_slug == publication_slug)
        })
        .filter_map(|release| {
            let revision = release.revision(started_at);
            known_revisions
                .get(&release.revision_key())
                .is_none_or(|known| known != &revision)
                .then(|| release.to_discovered_job(started_at, trace_parent, release_url, dataflow))
        })
        .collect()
}

fn should_skip_sheet(sheet_name: &str) -> bool {
    matches!(
        normalize_header(sheet_name).as_str(),
        "cover" | "notes" | "contents" | "explanatory notes" | "mozart reports"
    )
}

fn table_title_for_sheet(sheet_name: &str, rows: &[Vec<String>]) -> String {
    rows.iter()
        .flat_map(|row| row.iter())
        .find_map(|cell| {
            let trimmed = cell.trim();
            (!trimmed.is_empty()).then_some(trimmed.to_string())
        })
        .unwrap_or_else(|| sheet_name.trim().to_string())
}

fn schema_hash_for_sheet(sheet_name: &str, rows: &[Vec<String>]) -> String {
    let mut material = String::new();
    material.push_str(sheet_name);
    material.push('\n');
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

fn cell_to_string(cell: &Data) -> String {
    match cell {
        Data::Empty => String::new(),
        Data::String(value) => value.trim().to_string(),
        Data::Float(value) => number_to_string(*value),
        Data::Int(value) => value.to_string(),
        Data::Bool(value) => value.to_string(),
        Data::DateTime(value) => value
            .as_datetime()
            .map(|date_time| date_time.format("%Y-%m-%dT%H:%M:%S").to_string())
            .unwrap_or_else(|| value.to_string()),
        Data::DateTimeIso(value) | Data::DurationIso(value) => value.trim().to_string(),
        Data::Error(value) => format!("{value:?}"),
    }
}

fn number_to_string(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn is_vertical_header(row: &[String]) -> bool {
    let normalized = row
        .iter()
        .map(|cell| normalize_header(cell))
        .collect::<Vec<_>>();
    normalized.iter().any(|cell| cell == "entity quarter end")
        && normalized.iter().any(|cell| cell == "entity")
}

fn find_header_col(row: &[String], candidates: &[&str]) -> Option<usize> {
    row.iter().position(|cell| {
        let normalized = normalize_header(cell);
        candidates.iter().any(|candidate| normalized == *candidate)
    })
}

fn find_header_cols(row: &[String], candidates: &[&str]) -> Vec<usize> {
    row.iter()
        .enumerate()
        .filter_map(|(index, cell)| {
            let normalized = normalize_header(cell);
            candidates
                .iter()
                .any(|candidate| normalized == *candidate)
                .then_some(index)
        })
        .collect()
}

fn normalize_header(value: &str) -> String {
    value
        .replace('\n', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase()
}

#[derive(Debug, Clone)]
struct HorizontalPeriods {
    row_index: usize,
    columns: Vec<(usize, (DateTime<Utc>, TimePrecision))>,
}

fn find_horizontal_period_columns(
    rows: &[Vec<String>],
) -> Result<Option<HorizontalPeriods>, AdapterError> {
    for marker_index in rows.iter().enumerate().filter_map(|(index, row)| {
        row.iter()
            .any(|cell| normalize_header(cell) == "quarter end")
            .then_some(index)
    }) {
        let scan_end = (marker_index + 6).min(rows.len());
        for (row_index, row) in rows.iter().enumerate().take(scan_end).skip(marker_index) {
            let columns = row
                .iter()
                .enumerate()
                .filter_map(|(column, cell)| {
                    parse_apra_quarter(cell)
                        .transpose()
                        .map(|result| result.map(|period| (column, period)))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if columns.len() >= 2 {
                return Ok(Some(HorizontalPeriods { row_index, columns }));
            }
        }
    }
    Ok(None)
}

fn label_before(row: &[String], first_period_col: usize) -> Option<String> {
    let label = row
        .iter()
        .take(first_period_col)
        .map(|cell| cell.trim())
        .filter(|cell| !cell.is_empty())
        .collect::<Vec<_>>()
        .join(" / ");
    (!label.is_empty()).then_some(label)
}

fn unit_from_rows(rows: &[Vec<String>]) -> String {
    for cell in rows.iter().flat_map(|row| row.iter()) {
        let lower = cell.to_ascii_lowercase();
        if lower.contains("$ million") || lower.contains("$m") {
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

fn parse_apra_quarter(value: &str) -> Result<Option<(DateTime<Utc>, TimePrecision)>, AdapterError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if let Some((date_part, _)) = trimmed.split_once('T') {
        if let Ok(date) = NaiveDate::parse_from_str(date_part, "%Y-%m-%d") {
            return Ok(Some((
                utc_midnight(quarter_start(date)?),
                TimePrecision::Quarter,
            )));
        }
    }
    let normalized_month_hyphens = trimmed.replace('-', " ");
    for candidate in [trimmed, normalized_month_hyphens.as_str()] {
        for format in ["%Y-%m-%d", "%d/%m/%Y", "%e %b %Y", "%-d %b %Y"] {
            if let Ok(date) = NaiveDate::parse_from_str(candidate, format) {
                return Ok(Some((
                    utc_midnight(quarter_start(date)?),
                    TimePrecision::Quarter,
                )));
            }
        }
    }
    for format in ["%b %Y", "%B %Y"] {
        if let Ok(date) = NaiveDate::parse_from_str(
            &format!("1 {normalized_month_hyphens}"),
            &format!("%d {format}"),
        ) {
            return Ok(Some((
                utc_midnight(quarter_start(date)?),
                TimePrecision::Quarter,
            )));
        }
    }
    if let Some((year, quarter)) = parse_quarter_label(trimmed)? {
        let date = NaiveDate::from_ymd_opt(year, (quarter - 1) * 3 + 1, 1).ok_or_else(|| {
            AdapterError::FormatDrift(format!("invalid APRA quarter `{trimmed}`"))
        })?;
        return Ok(Some((utc_midnight(date), TimePrecision::Quarter)));
    }
    if let Ok(serial) = trimmed.parse::<f64>() {
        if (20_000.0..=80_000.0).contains(&serial) {
            let Some(date_time) =
                ExcelDateTime::new(serial, ExcelDateTimeType::DateTime, false).as_datetime()
            else {
                return Err(AdapterError::FormatDrift(format!(
                    "invalid APRA Excel period `{trimmed}`"
                )));
            };
            return Ok(Some((
                utc_midnight(quarter_start(date_time.date())?),
                TimePrecision::Quarter,
            )));
        }
    }
    Ok(None)
}

fn parse_quarter_label(value: &str) -> Result<Option<(i32, u32)>, AdapterError> {
    let quarter_parts = if let Some(parts) = value.split_once("-Q") {
        Some(parts)
    } else if let Some((year, quarter)) = value.split_once('Q') {
        (!year.is_empty() && !year.ends_with('-') && year.chars().all(|ch| ch.is_ascii_digit()))
            .then_some((year, quarter))
    } else {
        None
    };
    let Some((year, quarter)) = quarter_parts else {
        return Ok(None);
    };
    let year = year
        .parse::<i32>()
        .map_err(|_| AdapterError::FormatDrift(format!("invalid APRA period `{value}`")))?;
    let quarter = quarter
        .parse::<u32>()
        .map_err(|_| AdapterError::FormatDrift(format!("invalid APRA period `{value}`")))?;
    if !(1..=4).contains(&quarter) {
        return Err(AdapterError::FormatDrift(format!(
            "invalid APRA quarter `{value}`"
        )));
    }
    Ok(Some((year, quarter)))
}

fn quarter_start(date: NaiveDate) -> Result<NaiveDate, AdapterError> {
    let month = match date.month() {
        1..=3 => 1,
        4..=6 => 4,
        7..=9 => 7,
        10..=12 => 10,
        _ => unreachable!("chrono months are 1-12"),
    };
    NaiveDate::from_ymd_opt(date.year(), month, 1)
        .ok_or_else(|| AdapterError::FormatDrift(format!("invalid APRA period `{date}`")))
}

fn utc_midnight(date: NaiveDate) -> DateTime<Utc> {
    Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight is valid"))
}

fn parse_value(value: &str) -> Result<(Option<f64>, ObservationStatus), AdapterError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || matches!(trimmed, "-" | "*" | "**" | "na" | "n/a" | "NA" | "N/A") {
        return Ok((None, ObservationStatus::Missing));
    }
    if is_inline_unit_marker(trimmed) {
        return Ok((None, ObservationStatus::Missing));
    }
    let mut normalized = trimmed.replace([',', ' ', '%'], "");
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
        .map_err(|_| AdapterError::FormatDrift(format!("invalid APRA numeric value `{value}`")))
}

fn is_inline_unit_marker(value: &str) -> bool {
    let normalized = value
        .trim_matches(|ch| matches!(ch, '(' | ')'))
        .trim()
        .to_ascii_lowercase();
    !normalized.chars().any(|ch| ch.is_ascii_digit())
        && (normalized.contains("$ million")
            || normalized.contains("$m")
            || normalized.contains("thousand")
            || normalized.contains("per cent")
            || normalized.contains("percent")
            || normalized.contains("number"))
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

fn apra_code_id(field: &str, value: &str) -> Result<CodeId, AdapterError> {
    CodeId::new(value.to_string()).map_err(|err| {
        AdapterError::FormatDrift(format!("invalid APRA {field} code `{value}`: {err}"))
    })
}

fn source_id() -> SourceId {
    SourceId::new("apra").expect("static source id is valid")
}

fn dataflow_id() -> DataflowId {
    DataflowId::new(DATAFLOW_ID).expect("static dataflow id is valid")
}

fn super_asset_allocation_dataflow_id() -> DataflowId {
    DataflowId::new(SUPER_ASSET_ALLOCATION_DATAFLOW_ID).expect("static dataflow id is valid")
}

fn parse_worker_error(err: tokio::task::JoinError) -> AdapterError {
    CoreError::Io(io::Error::other(format!("APRA parse worker failed: {err}"))).into()
}

fn cancelled_parse_error() -> AdapterError {
    CoreError::Io(io::Error::new(
        io::ErrorKind::Interrupted,
        "APRA parse cancelled",
    ))
    .into()
}

/// Builder for [`ApraAdapter`].
#[derive(Debug, Clone)]
pub struct ApraAdapterBuilder {
    release_url: String,
    super_release_url: String,
}

impl Default for ApraAdapterBuilder {
    fn default() -> Self {
        Self {
            release_url: DEFAULT_RELEASE_URL.into(),
            super_release_url: DEFAULT_SUPER_RELEASE_URL.into(),
        }
    }
}

impl ApraAdapterBuilder {
    /// Override the release-calendar URL, usually for fixture tests.
    #[must_use]
    pub fn release_url(mut self, release_url: impl Into<String>) -> Self {
        self.release_url = release_url.into();
        self
    }

    /// Override the superannuation statistics release URL, usually for fixture tests.
    #[must_use]
    pub fn super_release_url(mut self, release_url: impl Into<String>) -> Self {
        self.super_release_url = release_url.into();
        self
    }

    /// Build the adapter.
    #[must_use]
    pub fn build(self) -> ApraAdapter {
        ApraAdapter {
            manifest: AdapterManifest {
                source_id: source_id(),
                name: "Australian Prudential Regulation Authority".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                rate_limit: RateLimit::new(30, Duration::from_secs(60))
                    .expect("static APRA rate limit is valid"),
                dataflows: vec![dataflow_id(), super_asset_allocation_dataflow_id()],
            },
            release_url: self.release_url,
            super_release_url: self.super_release_url,
        }
    }
}

/// Stored revision type for APRA release links.
pub type ApraReleaseRevision = UpstreamRevision;

/// APRA release artifact format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApraReleaseFormat {
    /// XLS or XLSX artifact parsed through `calamine`.
    Xls,
}

impl ApraReleaseFormat {
    /// Stable lowercase format label used in job metadata.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Xls => "xls",
        }
    }
}

/// One APRA XLS release link discovered from a release-calendar page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApraRelease {
    /// Source-local publication slug.
    pub publication_slug: String,
    /// Link text or inferred publication title.
    pub title: String,
    /// Artifact format.
    pub format: ApraReleaseFormat,
    /// Canonical artifact URL.
    pub source_url: String,
    /// Optional update marker scraped from the release page.
    pub last_updated: Option<String>,
}

impl ApraRelease {
    fn revision_key(&self) -> String {
        format!("APRA:{}", self.publication_slug)
    }

    fn revision(&self, started_at: DateTime<Utc>) -> UpstreamRevision {
        let version = self
            .last_updated
            .clone()
            .or_else(|| release_month_version(&self.source_url))
            .unwrap_or_else(|| quarter_version(started_at));
        UpstreamRevision::new(version, self.last_updated.clone())
    }

    fn to_discovered_job(
        &self,
        started_at: DateTime<Utc>,
        trace_parent: Option<&str>,
        release_url: &str,
        dataflow: ApraDataflow,
    ) -> DiscoveredJob {
        let revision = self.revision(started_at);
        let revision_version = revision.version().to_string();
        let revision_key = self.revision_key();
        DiscoveredJob {
            id: format!("apra:{}:{revision_version}", self.publication_slug),
            source_id: source_id(),
            dataflow_id: dataflow.id(),
            source_url: self.source_url.clone(),
            trace_parent: trace_parent.map(str::to_owned),
            metadata: BTreeMap::from([
                ("adapter".into(), "apra".into()),
                ("artifact_format".into(), self.format.as_str().into()),
                ("attribution".into(), ATTRIBUTION.into()),
                ("cadence".into(), "quarterly".into()),
                ("dataflow_id".into(), dataflow.label().into()),
                ("license".into(), LICENSE_NAME.into()),
                ("license_url".into(), LICENSE_URL.into()),
                ("publication_slug".into(), self.publication_slug.clone()),
                ("revision_key".into(), revision_key),
                ("revision_version".into(), revision_version),
                (
                    "schema_drift_policy".into(),
                    "hash-schema-per-release".into(),
                ),
                ("source_release_url".into(), release_url.into()),
                ("title".into(), self.title.clone()),
            ]),
        }
    }
}

fn release_month_version(source_url: &str) -> Option<String> {
    let marker = "/sites/default/files/";
    let (_, path) = source_url.split_once(marker)?;
    let mut parts = path.split('/');
    let year = parts.next()?;
    let month = parts.next()?;
    (year.len() == 4 && month.len() == 2 && year.chars().all(|ch| ch.is_ascii_digit()))
        .then(|| format!("{year}-{month}"))
}

fn quarter_version(started_at: DateTime<Utc>) -> String {
    let quarter = (started_at.month0() / 3) + 1;
    format!("{}-Q{quarter}", started_at.year())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_hex_fixture(hex: &str) -> Vec<u8> {
        let digits = hex
            .bytes()
            .filter(|byte| !byte.is_ascii_whitespace())
            .collect::<Vec<_>>();
        digits
            .chunks_exact(2)
            .map(|pair| {
                let high = char::from(pair[0]).to_digit(16).expect("hex digit");
                let low = char::from(pair[1]).to_digit(16).expect("hex digit");
                ((high << 4) | low) as u8
            })
            .collect()
    }

    fn apra_fuzz_artifact(bytes: &[u8]) -> (ArtifactRef, ApraReleaseProvenance, DateTime<Utc>) {
        let id = ArtifactId::of_content(bytes);
        let source_url = "https://www.apra.gov.au/sites/default/files/centralised.xlsx";
        let fetched_at = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let artifact = ArtifactRef {
            id,
            fetch_id: None,
            source_id: source_id(),
            source_url: source_url.into(),
            content_type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                .into(),
            response_headers: BTreeMap::new(),
            storage_key: StorageKey::canonical_for(&id).to_string(),
            size_bytes: bytes.len() as u64,
            fetched_at,
        };
        let provenance =
            release_url_provenance_for_parse(source_url).expect("static APRA URL has provenance");
        (artifact, provenance, fetched_at)
    }

    #[test]
    fn parse_apra_quarter_accepts_month_labels_and_excel_serials() {
        let (time, precision) = parse_apra_quarter("Dec 2025")
            .expect("parse month label")
            .expect("period");
        assert_eq!(precision, TimePrecision::Quarter);
        assert_eq!(time.to_rfc3339(), "2025-10-01T00:00:00+00:00");

        let (time, precision) = parse_apra_quarter("46022")
            .expect("parse Excel date")
            .expect("period");
        assert_eq!(precision, TimePrecision::Quarter);
        assert_eq!(time.to_rfc3339(), "2025-10-01T00:00:00+00:00");
    }

    #[test]
    fn malformed_xlsx_returns_format_error_instead_of_panicking() {
        let bytes = decode_hex_fixture(include_str!(
            "../../../../tests/fixtures/calamine-row-overflow.xlsx.hex"
        ));
        let (artifact, provenance, ingested_at) = apra_fuzz_artifact(&bytes);
        let plan = ApraParsePlan {
            provenance,
            dataflow: ApraDataflow::QuarterlyStatistics,
        };

        let parsed =
            std::panic::catch_unwind(|| parse_xls_workbook(bytes, artifact, plan, ingested_at));
        assert!(parsed.is_ok(), "malformed XLSX should not panic");
        let err = parsed
            .expect("panic handled")
            .expect_err("malformed XLSX should be rejected");

        assert!(err.to_string().contains("APRA XLSX worksheet"), "{err}");
    }

    #[test]
    fn malformed_xlsx_with_shifted_zip_header_is_rejected_before_calamine() {
        let bytes = decode_hex_fixture(include_str!(
            "../../../../tests/fixtures/calamine-shifted-zip-header.xlsx.hex"
        ));
        let (artifact, provenance, ingested_at) = apra_fuzz_artifact(&bytes);

        let parsed = std::panic::catch_unwind(|| {
            parse_xls_workbook(bytes, artifact, provenance, ingested_at)
        });
        assert!(parsed.is_ok(), "malformed XLSX should not panic");
        let err = parsed
            .expect("panic handled")
            .expect_err("malformed XLSX should be rejected");

        assert!(
            err.to_string()
                .contains("APRA workbook has unsupported XLS/XLSX signature"),
            "{err}"
        );
    }

    #[test]
    fn malformed_xlsx_with_case_mismatched_worksheet_entry_is_rejected_before_calamine() {
        let bytes = decode_hex_fixture(include_str!(
            "../../../../tests/fixtures/calamine-case-mismatched-worksheet-entry.xlsx.hex"
        ));
        let (artifact, provenance, ingested_at) = apra_fuzz_artifact(&bytes);

        let parsed = std::panic::catch_unwind(|| {
            parse_xls_workbook(bytes, artifact, provenance, ingested_at)
        });
        assert!(parsed.is_ok(), "malformed XLSX should not panic");
        let err = parsed
            .expect("panic handled")
            .expect_err("malformed XLSX should be rejected");

        assert!(err.to_string().contains("APRA XLSX worksheet"), "{err}");
    }

    #[test]
    fn validate_fetch_job_rejects_wrong_source_dataflow_and_url() {
        let adapter = ApraAdapter::default();
        let releases = ApraAdapter::parse_release_calendar(
            r#"<a href="/sites/default/files/2026-03/Quarterly%20authorised%20deposit-taking%20institution%20performance.xlsx">Performance</a>"#,
        )
        .expect("parse release");
        let job = ApraAdapter::current_jobs_with_started_at(
            &releases,
            Utc.with_ymd_and_hms(2026, 5, 27, 0, 0, 0).unwrap(),
        )
        .into_iter()
        .next()
        .expect("job");
        adapter.validate_fetch_job(&job).expect("valid job");

        let mut wrong_source = job.clone();
        wrong_source.source_id = SourceId::new("rba").unwrap();
        assert!(
            adapter
                .validate_fetch_job(&wrong_source)
                .expect_err("wrong source")
                .to_string()
                .contains("received job for source")
        );

        let mut wrong_dataflow = job.clone();
        wrong_dataflow.dataflow_id = DataflowId::new("apra.unsupported").unwrap();
        assert!(
            adapter
                .validate_fetch_job(&wrong_dataflow)
                .expect_err("wrong dataflow")
                .to_string()
                .contains("unsupported dataflow")
        );

        let mut wrong_url = job;
        wrong_url.source_url = "https://www.apra.gov.au/reports/not-a-release.csv".into();
        assert!(
            adapter
                .validate_fetch_job(&wrong_url)
                .expect_err("wrong URL")
                .to_string()
                .contains("not a release XLS artifact")
        );
    }

    #[test]
    fn super_asset_category_mapping_is_reviewed_and_deterministic() {
        let onshore = super_asset_category_mapping("Australian infrastructure")
            .expect("onshore infrastructure mapping");
        assert_eq!(onshore.category, "australian_infrastructure");
        assert_eq!(onshore.mapping, "productive_infrastructure_onshore");
        assert!(onshore.included_in_onshore_total);
        assert_eq!(onshore.rule, "include_explicit_onshore_infrastructure");

        let offshore = super_asset_category_mapping("Overseas infrastructure")
            .expect("offshore infrastructure mapping");
        assert_eq!(offshore.mapping, "productive_infrastructure_offshore");
        assert!(!offshore.included_in_onshore_total);

        let property =
            super_asset_category_mapping("Australian property").expect("property mapping");
        assert_eq!(property.mapping, "non_infrastructure_real_asset");
        assert!(!property.included_in_onshore_total);

        let cash = super_asset_category_mapping("Cash").expect("cash mapping");
        assert_eq!(cash.mapping, "liquidity");
        assert!(!cash.included_in_onshore_total);
    }

    #[test]
    fn release_calendar_helpers_cover_url_and_html_variants() {
        assert_eq!(
            attr_value("<a href='one.xls'>", "href").as_deref(),
            Some("one.xls")
        );
        assert_eq!(attr_value("<a href=one.xls>", "href"), None);
        assert_eq!(
            clean_html_text("<span>Quarterly&nbsp;ADI &amp; data</span>").as_deref(),
            Some("Quarterly ADI & data")
        );
        assert_eq!(clean_html_text("<span></span>"), None);

        assert_eq!(
            resolve_url(
                "https://www.apra.gov.au/releases/page",
                "https://cdn.example.test/file.xlsx"
            )
            .unwrap(),
            "https://cdn.example.test/file.xlsx"
        );
        assert_eq!(
            resolve_url(
                "https://www.apra.gov.au/releases/page",
                "/sites/default/files/file.xlsx"
            )
            .unwrap(),
            "https://www.apra.gov.au/sites/default/files/file.xlsx"
        );
        assert_eq!(
            resolve_url("https://www.apra.gov.au/releases/page", "file.xlsx").unwrap(),
            "https://www.apra.gov.au/releases/file.xlsx"
        );
        assert!(resolve_url("not-absolute", "/file.xlsx").is_err());
        assert!(resolve_url("noslash", "file.xlsx").is_err());

        let releases = parse_release_calendar_with_base(
            r#"
            <a href='https://www.apra.gov.au/sites/default/files/2026-03/Other%20ADI%20Data.xls'>Other <span>ADI</span> Data</a>
            <a href="/sites/default/files/2026-03/Other%20ADI%20Data.xls">duplicate</a>
            <a href="/sites/default/files/2026-03/ignored.pdf">ignored</a>
            <a href=/sites/default/files/2026-03/bad.xlsx>ignored bad attr</a>
            "#,
            "https://www.apra.gov.au/releases/page",
        )
        .expect("parse release page");
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].format.as_str(), "xls");
        assert_eq!(releases[0].title, "Other ADI Data");
    }

    #[test]
    fn release_url_provenance_requires_parse_host_and_infers_publications() {
        let historical = release_url_provenance_for_parse(
            "https://apra.gov.au/sites/default/files/2026-03/ADI_property_exposure_historical.xls",
        )
        .expect("historical property exposure provenance");
        assert_eq!(
            historical.publication_slug,
            "adi-property-exposures-historical"
        );

        let property = release_url_provenance_for_parse(
            "https://www.apra.gov.au/sites/default/files/2026-03/ADI_property_exposure.xlsx",
        )
        .expect("property exposure provenance");
        assert_eq!(property.publication_slug, "adi-property-exposures");

        let fallback = release_url_provenance_for_fetch(
            "https://mirror.example.test/sites/default/files/2026-03/Other_APRA_Release.xls",
        )
        .expect("fetch provenance may be mirrored");
        assert_eq!(fallback.publication_slug, "other_apra_release");
        assert!(
            release_url_provenance_for_parse(
                "https://mirror.example.test/sites/default/files/2026-03/Other_APRA_Release.xls",
            )
            .is_none()
        );
        assert!(
            release_url_provenance_for_parse("https://www.apra.gov.au/reports/file.xls").is_none()
        );
        assert!(
            release_url_provenance_for_parse(
                "https://www.apra.gov.au/sites/default/files/2026-03/file.pdf"
            )
            .is_none()
        );
        assert!(release_url_provenance_for_parse("not-a-url").is_none());
    }

    #[test]
    fn parse_apra_quarter_accepts_supported_labels_and_rejects_bad_ones() {
        assert!(parse_apra_quarter("").unwrap().is_none());
        assert!(parse_apra_quarter("not a period").unwrap().is_none());
        assert!(parse_apra_quarter("100").unwrap().is_none());

        for label in [
            "2025-12-31T00:00:00",
            "2025-12-31",
            "31/12/2025",
            "31-Dec-2025",
            "31 Dec 2025",
            "December 2025",
            "Dec-2025",
            "2025-Q4",
            "2025Q4",
        ] {
            let (time, precision) = parse_apra_quarter(label)
                .unwrap_or_else(|err| panic!("{label}: {err}"))
                .unwrap_or_else(|| panic!("{label}: missing period"));
            assert_eq!(precision, TimePrecision::Quarter);
            assert_eq!(time.to_rfc3339(), "2025-10-01T00:00:00+00:00", "{label}");
        }

        assert!(parse_apra_quarter("2025-Q5").is_err());
        assert!(parse_apra_quarter("20A5-Q1").is_err());
    }

    #[test]
    fn parse_value_unit_decode_and_slug_helpers_cover_edge_cases() {
        assert_eq!(unit_from_rows(&[vec!["values in $m".into()]]), "$ million");
        assert_eq!(
            unit_from_rows(&[vec!["per cent of assets".into()]]),
            "percent"
        );
        assert_eq!(
            unit_from_rows(&[vec!["number of entities".into()]]),
            "number"
        );
        assert_eq!(unit_from_rows(&[vec!["plain text".into()]]), "unknown");

        assert_eq!(
            parse_value("(1,234.5%)").unwrap(),
            (Some(-1234.5), ObservationStatus::Normal)
        );
        assert_eq!(
            parse_value("N/A").unwrap(),
            (None, ObservationStatus::Missing)
        );
        assert_eq!(
            parse_value("(thousands of loans)").unwrap(),
            (None, ObservationStatus::Missing)
        );
        assert!(parse_value("not numeric").is_err());

        assert_eq!(decode_url_component("bad%ZZvalue"), "bad%ZZvalue");
        assert_eq!(slugify_code(""), "value");
        assert_eq!(slugify_code("APRA value"), "apra_value");
        assert_eq!(slugify_code(&format!("{}_", "a".repeat(140))).len(), 128);
    }
}
