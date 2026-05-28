//! `/v1/observations` query handling and streaming renderers.

use std::{collections::BTreeMap, fmt, io, sync::Arc, time::Duration};

use arrow_array::{ArrayRef, Float64Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use au_kpis_domain::{
    ObservationStatus, TimePrecision,
    ids::{ArtifactId, DataflowId, SeriesKey, Sha256Digest},
};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use chrono::{DateTime, NaiveDate, Utc};
use futures::{
    Stream, TryStreamExt,
    future::{BoxFuture, FutureExt},
};
use parquet::{
    arrow::{AsyncArrowWriter, async_writer::AsyncFileWriter},
    basic::Compression,
    errors::ParquetError,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};
use tokio::sync::mpsc;
use utoipa::ToSchema;

use crate::{AppState, error::ApiError};

const DEFAULT_LIMIT: usize = 1_000;
const MAX_LIMIT: usize = 10_000;
const CACHE_CONTROL_VALUE: &str = "public, max-age=60, stale-while-revalidate=300";
const JSON_RESPONSE_CACHE_TTL: Duration = Duration::from_secs(300);
const PARQUET_ROW_GROUP_TARGET_BYTES: usize = 1_000_000;
const PARQUET_BATCH_ROWS: usize = 1_024;

/// Attribution and licensing metadata that applies to an observations page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ObservationsMetadata {
    /// Requested dataflow identifier.
    pub dataflow: DataflowId,
    /// Required source acknowledgement to display with derived charts.
    pub attribution: String,
    /// Data license identifier, e.g. `CC-BY-4.0`.
    pub license: String,
    /// Canonical citation URL for the upstream source.
    pub source_url: String,
}

/// Cursor metadata for the current observations page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PaginationMetadata {
    /// Opaque cursor for the next page, or null when the page is complete.
    pub next_cursor: Option<String>,
}

/// One observation row returned by `/v1/observations`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ObservationsRow {
    /// Deterministic series key.
    pub series_key: SeriesKey,
    /// Observation timestamp.
    pub time: DateTime<Utc>,
    /// Temporal precision of the timestamp.
    pub time_precision: TimePrecision,
    /// Numeric value; null when status is `missing`.
    pub value: Option<f64>,
    /// Observation status.
    pub status: ObservationStatus,
    /// Revision number, where zero is the original publication.
    pub revision_no: u32,
    /// Free-form source attributes for this observation.
    pub attributes: BTreeMap<String, String>,
    /// Ingestion timestamp for this revision.
    pub ingested_at: DateTime<Utc>,
    /// Content-addressed source artifact identifier.
    pub source_artifact_id: ArtifactId,
    /// Series dimension values keyed by dimension id.
    pub dimensions: BTreeMap<String, String>,
    /// Measure identifier for the series.
    pub measure_id: String,
    /// Unit for the series values.
    pub unit: String,
}

/// JSON response envelope for `/v1/observations`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ObservationsResponse {
    /// Dataflow attribution and license metadata.
    pub metadata: ObservationsMetadata,
    /// Observation rows in ascending `(time, series_key)` order.
    pub observations: Vec<ObservationsRow>,
    /// Cursor for continuing the query.
    pub pagination: PaginationMetadata,
}

/// `GET /v1/observations`.
#[utoipa::path(
    get,
    operation_id = "listObservations",
    path = "/v1/observations",
    params(
        ("dataflow" = String, Query, min_length = 1, max_length = 128, description = "Required dataflow id, e.g. abs.cpi."),
        ("dimensions[]" = Option<Vec<String>>, Query, description = "Dimension filters as repeated key=value values. The handler also accepts dimensions[region]=AUS."),
        ("since" = Option<String>, Query, format = Date, min_length = 1, description = "Inclusive lower time bound as YYYY-MM-DD or RFC3339."),
        ("until" = Option<String>, Query, format = Date, min_length = 1, description = "Inclusive upper time bound as YYYY-MM-DD or RFC3339."),
        ("frequency" = Option<String>, Query, min_length = 1, pattern = "^(annual|quarterly|monthly|weekly|daily|irregular)$", description = "Optional dataflow frequency filter, or weekly/monthly/quarterly rollup grain."),
        ("format" = Option<String>, Query, min_length = 3, max_length = 7, pattern = "^(json|csv|parquet)$", description = "Response format: json, csv, or parquet."),
        ("cursor" = Option<String>, Query, min_length = 1, description = "Opaque cursor from the previous page."),
        ("limit" = Option<u32>, Query, maximum = 10000, description = "Page size, maximum 10000.")
    ),
    responses(
        (
            status = 200,
            description = "Observation page.",
            content(
                (ObservationsResponse = "application/json"),
                (String = "text/csv"),
                (String = "application/vnd.apache.parquet")
            ),
            headers(
                ("ETag" = String, description = "Weak entity tag for this page."),
                ("Cache-Control" = String, description = "Public CDN cache policy.")
            )
        ),
        (
            status = 304,
            description = "The client's cached page is still fresh.",
            headers(
                ("ETag" = String, description = "Weak entity tag for this page."),
                ("Cache-Control" = String, description = "Public CDN cache policy.")
            )
        ),
        (
            status = 400,
            description = "Invalid query string.",
            content_type = "application/problem+json",
            body = crate::error::ProblemDetails
        ),
        (
            status = 404,
            description = "Dataflow not found.",
            content_type = "application/problem+json",
            body = crate::error::ProblemDetails
        ),
        (
            status = 408,
            description = "Request timed out.",
            content_type = "application/problem+json",
            body = crate::error::ProblemDetails
        ),
        (
            status = 500,
            description = "Internal server error.",
            content_type = "application/problem+json",
            body = crate::error::ProblemDetails
        )
    ),
    tag = "observations"
)]
pub async fn list_observations(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, ApiError> {
    let query = parse_observations_query(uri.query())?;
    let json_cache_key = observations_json_cache_key(uri.query(), &query);
    if should_cache_json_observations(&query) {
        if let Some(cached) = read_cached_json_observations(&state, &json_cache_key).await {
            return cached_json_observations_response(&headers, cached);
        }
    }

    let metadata = load_observations_metadata(&state.db, &query.dataflow).await?;
    let etag = compute_etag(&state.db, &query).await?;
    let cache_control = HeaderValue::from_static(CACHE_CONTROL_VALUE);
    let etag_header = HeaderValue::from_str(&etag).map_err(|err| {
        tracing::error!(error = %err, "generated invalid ETag header");
        ApiError::Internal
    })?;

    if if_none_match_fresh(&headers, &etag) {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, cache_control);
        response.headers_mut().insert(header::ETAG, etag_header);
        return Ok(response);
    }

    let content_type = match query.format {
        ResponseFormat::Json => HeaderValue::from_static("application/json"),
        ResponseFormat::Csv => HeaderValue::from_static("text/csv; charset=utf-8"),
        ResponseFormat::Parquet => HeaderValue::from_static("application/vnd.apache.parquet"),
    };
    let stream = match query.format {
        ResponseFormat::Json if should_cache_json_observations(&query) => {
            let body =
                render_json_observations(state.db.clone(), query.clone(), metadata.clone()).await?;
            write_cached_json_observations(
                &state,
                &json_cache_key,
                &CachedJsonObservations {
                    etag: etag.clone(),
                    body: body.clone(),
                },
            )
            .await;
            Body::from(body)
        }
        ResponseFormat::Json => {
            Body::from_stream(json_observations_stream(state.db.clone(), query, metadata))
        }
        ResponseFormat::Csv => {
            Body::from_stream(csv_observations_stream(state.db.clone(), query, metadata))
        }
        ResponseFormat::Parquet => Body::from_stream(parquet_observations_stream(
            state.db.clone(),
            query,
            metadata,
        )),
    };

    let mut response = Response::new(stream);
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, cache_control);
    response.headers_mut().insert(header::ETAG, etag_header);
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type);
    Ok(response)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedJsonObservations {
    etag: String,
    body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedObservationsQuery {
    dataflow: DataflowId,
    dimensions: BTreeMap<String, String>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    frequency: Option<FrequencyQuery>,
    format: ResponseFormat,
    cursor: Option<ObservationCursor>,
    limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FrequencyQuery {
    Dataflow(String),
    Rollup(RollupGrain),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RollupGrain {
    Weekly,
    Monthly,
    Quarterly,
}

impl RollupGrain {
    fn view_name(self) -> &'static str {
        match self {
            Self::Weekly => "observations_rollup_weekly",
            Self::Monthly => "observations_rollup_monthly",
            Self::Quarterly => "observations_rollup_quarterly",
        }
    }

    fn query_label(self) -> &'static str {
        match self {
            Self::Weekly => "weekly",
            Self::Monthly => "monthly",
            Self::Quarterly => "quarterly",
        }
    }

    fn time_precision_label(self) -> &'static str {
        match self {
            Self::Weekly => "week",
            Self::Monthly => "month",
            Self::Quarterly => "quarter",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseFormat {
    Json,
    Csv,
    Parquet,
}

impl fmt::Display for ResponseFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResponseFormat::Json => f.write_str("json"),
            ResponseFormat::Csv => f.write_str("csv"),
            ResponseFormat::Parquet => f.write_str("parquet"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct ObservationCursor {
    time: DateTime<Utc>,
    series_key: SeriesKey,
}

#[derive(Debug)]
struct CacheFingerprint {
    row_count: i64,
    max_ingested_at: Option<DateTime<Utc>>,
    max_time: Option<DateTime<Utc>>,
    max_revision_no: Option<i32>,
}

fn parse_observations_query(raw: Option<&str>) -> Result<ParsedObservationsQuery, ApiError> {
    let mut dataflow = None;
    let mut dimensions = BTreeMap::new();
    let mut since = None;
    let mut until = None;
    let mut frequency = None;
    let mut format = ResponseFormat::Json;
    let mut cursor = None;
    let mut limit = DEFAULT_LIMIT;

    for (key, value) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
        let key = key.into_owned();
        let value = value.into_owned();
        match key.as_str() {
            "dataflow" => {
                dataflow = Some(
                    DataflowId::new(value)
                        .map_err(|err| ApiError::Validation(format!("invalid dataflow: {err}")))?,
                );
            }
            "since" => since = Some(parse_time_bound("since", &value)?),
            "until" => until = Some(parse_time_bound("until", &value)?),
            "frequency" => frequency = Some(parse_frequency(&value)?),
            "format" => format = parse_format(&value)?,
            "cursor" => cursor = Some(decode_cursor(&value)?),
            "limit" => limit = parse_limit(&value)?,
            "dimensions" | "dimensions[]" => {
                let (dimension, code) = value
                    .split_once('=')
                    .or_else(|| value.split_once(':'))
                    .ok_or_else(|| {
                        ApiError::Validation("dimensions[] values must use dimension=value".into())
                    })?;
                insert_dimension_filter(&mut dimensions, dimension, code)?;
            }
            _ if key.starts_with("dimensions[") && key.ends_with(']') => {
                let dimension = &key["dimensions[".len()..key.len() - 1];
                insert_dimension_filter(&mut dimensions, dimension, &value)?;
            }
            _ => {}
        }
    }

    let dataflow = dataflow
        .ok_or_else(|| ApiError::Validation("dataflow query parameter is required".into()))?;
    if let (Some(since), Some(until)) = (since, until) {
        if since > until {
            return Err(ApiError::Validation(
                "since must be less than or equal to until".into(),
            ));
        }
    }

    Ok(ParsedObservationsQuery {
        dataflow,
        dimensions,
        since,
        until,
        frequency,
        format,
        cursor,
        limit,
    })
}

fn insert_dimension_filter(
    dimensions: &mut BTreeMap<String, String>,
    dimension: &str,
    code: &str,
) -> Result<(), ApiError> {
    if dimension.is_empty() || code.is_empty() {
        return Err(ApiError::Validation(
            "dimension filters require non-empty dimension and code".into(),
        ));
    }
    dimensions.insert(dimension.to_string(), code.to_string());
    Ok(())
}

fn parse_time_bound(name: &str, value: &str) -> Result<DateTime<Utc>, ApiError> {
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return date
            .and_hms_opt(0, 0, 0)
            .map(|time| DateTime::from_naive_utc_and_offset(time, Utc))
            .ok_or_else(|| ApiError::Validation(format!("{name} is outside supported range")));
    }

    DateTime::parse_from_rfc3339(value)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|err| ApiError::Validation(format!("invalid {name}: {err}")))
}

fn parse_frequency(value: &str) -> Result<FrequencyQuery, ApiError> {
    match value {
        "weekly" => Ok(FrequencyQuery::Rollup(RollupGrain::Weekly)),
        "monthly" => Ok(FrequencyQuery::Rollup(RollupGrain::Monthly)),
        "quarterly" => Ok(FrequencyQuery::Rollup(RollupGrain::Quarterly)),
        "daily" | "annual" | "irregular" => Ok(FrequencyQuery::Dataflow(value.to_string())),
        _ => Err(ApiError::Validation(format!(
            "unsupported frequency `{value}`"
        ))),
    }
}

fn parse_format(value: &str) -> Result<ResponseFormat, ApiError> {
    match value {
        "json" => Ok(ResponseFormat::Json),
        "csv" => Ok(ResponseFormat::Csv),
        "parquet" => Ok(ResponseFormat::Parquet),
        _ => Err(ApiError::Validation(format!(
            "unsupported format `{value}`"
        ))),
    }
}

fn parse_limit(value: &str) -> Result<usize, ApiError> {
    let limit = value
        .parse::<usize>()
        .map_err(|err| ApiError::Validation(format!("invalid limit: {err}")))?;
    if limit == 0 || limit > MAX_LIMIT {
        return Err(ApiError::Validation(format!(
            "limit must be between 1 and {MAX_LIMIT}"
        )));
    }
    Ok(limit)
}

fn encode_cursor(cursor: &ObservationCursor) -> Result<String, ApiError> {
    let json = serde_json::to_vec(cursor).map_err(|err| {
        tracing::error!(error = %err, "cursor serialization failed");
        ApiError::Internal
    })?;
    Ok(BASE64_URL_SAFE_NO_PAD.encode(json))
}

fn decode_cursor(cursor: &str) -> Result<ObservationCursor, ApiError> {
    let bytes = BASE64_URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|err| ApiError::Validation(format!("invalid cursor encoding: {err}")))?;
    serde_json::from_slice(&bytes)
        .map_err(|err| ApiError::Validation(format!("invalid cursor payload: {err}")))
}

async fn load_observations_metadata(
    pool: &PgPool,
    dataflow: &DataflowId,
) -> Result<ObservationsMetadata, ApiError> {
    let row = sqlx::query(
        "SELECT license, attribution, source_url
         FROM dataflows
         WHERE id = $1",
    )
    .bind(dataflow.as_str())
    .fetch_optional(pool)
    .await?;

    let row = row.ok_or_else(|| ApiError::NotFound(format!("dataflow `{dataflow}`")))?;
    Ok(ObservationsMetadata {
        dataflow: dataflow.clone(),
        license: row.try_get("license")?,
        attribution: row.try_get("attribution")?,
        source_url: row.try_get("source_url")?,
    })
}

async fn compute_etag(pool: &PgPool, query: &ParsedObservationsQuery) -> Result<String, ApiError> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT count(*)::bigint AS row_count,
                max(o.ingested_at) AS max_ingested_at,
                max(o.time) AS max_time,
                max(o.revision_no) AS max_revision_no
         FROM ",
    );
    builder.push(observation_source_table(query));
    builder.push(
        " o
         JOIN series s ON s.series_key = o.series_key
         JOIN dataflows d ON d.id = s.dataflow_id",
    );
    push_observation_filters(&mut builder, query);
    let row = builder.build().fetch_one(pool).await?;
    let fingerprint = CacheFingerprint {
        row_count: row.try_get("row_count")?,
        max_ingested_at: row.try_get("max_ingested_at")?,
        max_time: row.try_get("max_time")?,
        max_revision_no: row.try_get("max_revision_no")?,
    };
    Ok(format!(
        "W/\"{}\"",
        Sha256Digest::hash(etag_seed(query, &fingerprint).as_bytes())
    ))
}

fn etag_seed(query: &ParsedObservationsQuery, fingerprint: &CacheFingerprint) -> String {
    format!(
        "dataflow={};dimensions={:?};since={:?};until={:?};frequency={:?};format={};cursor={:?};limit={};row_count={};max_ingested_at={:?};max_time={:?};max_revision_no={:?}",
        query.dataflow,
        query.dimensions,
        query.since,
        query.until,
        query.frequency,
        query.format,
        query.cursor,
        query.limit,
        fingerprint.row_count,
        fingerprint.max_ingested_at,
        fingerprint.max_time,
        fingerprint.max_revision_no
    )
}

fn if_none_match_fresh(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .map(str::trim)
                .any(|candidate| candidate == "*" || candidate == etag)
        })
}

fn should_cache_json_observations(query: &ParsedObservationsQuery) -> bool {
    // Keep the hot-path cache bounded; cursor pages and export formats stay streaming.
    query.format == ResponseFormat::Json && query.cursor.is_none() && query.limit <= DEFAULT_LIMIT
}

fn observations_json_cache_key(raw_query: Option<&str>, query: &ParsedObservationsQuery) -> String {
    format!(
        "observations:v1:{}:{}",
        query.format,
        BASE64_URL_SAFE_NO_PAD.encode(raw_query.unwrap_or_default())
    )
}

fn rollup_grain(query: &ParsedObservationsQuery) -> Option<RollupGrain> {
    match &query.frequency {
        Some(FrequencyQuery::Rollup(grain)) => Some(*grain),
        _ => None,
    }
}

fn observation_source_table(query: &ParsedObservationsQuery) -> &'static str {
    rollup_grain(query).map_or("observations_latest", RollupGrain::view_name)
}

async fn read_cached_json_observations(
    state: &AppState,
    key: &str,
) -> Option<CachedJsonObservations> {
    match state.cache.get_json(key).await {
        Ok(cached) => cached,
        Err(err) => {
            tracing::warn!(error = %err, cache.key = key, "observations JSON cache read failed");
            None
        }
    }
}

async fn write_cached_json_observations(
    state: &AppState,
    key: &str,
    value: &CachedJsonObservations,
) {
    if let Err(err) = state
        .cache
        .set_json(key, value, JSON_RESPONSE_CACHE_TTL)
        .await
    {
        tracing::warn!(error = %err, cache.key = key, "observations JSON cache write failed");
    }
}

fn cached_json_observations_response(
    headers: &HeaderMap,
    cached: CachedJsonObservations,
) -> Result<Response, ApiError> {
    let cache_control = HeaderValue::from_static(CACHE_CONTROL_VALUE);
    let etag_header = HeaderValue::from_str(&cached.etag).map_err(|err| {
        tracing::error!(error = %err, "cached ETag header is invalid");
        ApiError::Internal
    })?;

    if if_none_match_fresh(headers, &cached.etag) {
        let mut response = StatusCode::NOT_MODIFIED.into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, cache_control);
        response.headers_mut().insert(header::ETAG, etag_header);
        return Ok(response);
    }

    let mut response = Response::new(Body::from(cached.body));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, cache_control);
    response.headers_mut().insert(header::ETAG, etag_header);
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Ok(response)
}

async fn render_json_observations(
    pool: PgPool,
    query: ParsedObservationsQuery,
    metadata: ObservationsMetadata,
) -> Result<String, ApiError> {
    let metadata = serialize_json_chunk(&metadata)?;
    let mut body = format!("{{\"metadata\":{metadata},\"observations\":[");

    let limit = query.limit;
    let stream = fetch_observation_rows(pool, query);
    futures::pin_mut!(stream);
    let mut emitted = 0_usize;
    let mut first = true;
    let mut last_cursor = None;
    let mut next_cursor = None;
    while let Some(row) = stream.try_next().await? {
        if emitted == limit {
            next_cursor = last_cursor;
            break;
        }

        if !first {
            body.push(',');
        }
        first = false;
        last_cursor = Some(encode_cursor(&ObservationCursor {
            time: row.time,
            series_key: row.series_key,
        })?);
        emitted += 1;
        body.push_str(&serialize_json_chunk(&row)?);
    }

    let pagination = serialize_json_chunk(&PaginationMetadata { next_cursor })?;
    body.push_str(&format!("],\"pagination\":{pagination}}}"));
    Ok(body)
}

fn json_observations_stream(
    pool: PgPool,
    query: ParsedObservationsQuery,
    metadata: ObservationsMetadata,
) -> impl Stream<Item = Result<Bytes, ApiError>> + Send + 'static {
    async_stream::try_stream! {
        let metadata = serialize_json_chunk(&metadata)?;
        yield Bytes::from(format!("{{\"metadata\":{metadata},\"observations\":["));

        let limit = query.limit;
        let stream = fetch_observation_rows(pool, query);
        futures::pin_mut!(stream);
        let mut emitted = 0_usize;
        let mut first = true;
        let mut last_cursor = None;
        let mut next_cursor = None;
        while let Some(row) = stream.try_next().await? {
            if emitted == limit {
                next_cursor = last_cursor;
                break;
            }

            if !first {
                yield Bytes::from_static(b",");
            }
            first = false;
            last_cursor = Some(encode_cursor(&ObservationCursor {
                time: row.time,
                series_key: row.series_key,
            })?);
            emitted += 1;
            yield Bytes::from(serialize_json_chunk(&row)?);
        }

        let pagination = serialize_json_chunk(&PaginationMetadata { next_cursor })?;
        yield Bytes::from(format!("],\"pagination\":{pagination}}}"));
    }
}

fn csv_observations_stream(
    pool: PgPool,
    query: ParsedObservationsQuery,
    metadata: ObservationsMetadata,
) -> impl Stream<Item = Result<Bytes, ApiError>> + Send + 'static {
    async_stream::try_stream! {
        yield Bytes::from(format!(
            "# dataflow={},license={},attribution={},source_url={}\n",
            csv_escape(metadata.dataflow.as_str()),
            csv_escape(&metadata.license),
            csv_escape(&metadata.attribution),
            csv_escape(&metadata.source_url),
        ));
        yield Bytes::from_static(
            b"series_key,time,time_precision,value,status,revision_no,dimensions,attributes,ingested_at,source_artifact_id,measure_id,unit\n",
        );

        let limit = query.limit;
        let stream = fetch_observation_rows(pool, query);
        futures::pin_mut!(stream);
        let mut emitted = 0_usize;
        let mut last_cursor = None;
        let mut next_cursor = None;
        while let Some(row) = stream.try_next().await? {
            if emitted == limit {
                next_cursor = last_cursor;
                break;
            }
            last_cursor = Some(encode_cursor(&ObservationCursor {
                time: row.time,
                series_key: row.series_key,
            })?);
            emitted += 1;
            yield Bytes::from(row_to_csv(&row)?);
        }
        if let Some(next_cursor) = next_cursor {
            yield Bytes::from(format!("# next_cursor={}\n", csv_escape(&next_cursor)));
        }
    }
}

fn parquet_observations_stream(
    pool: PgPool,
    query: ParsedObservationsQuery,
    metadata: ObservationsMetadata,
) -> impl Stream<Item = Result<Bytes, ApiError>> + Send + 'static {
    let (tx, mut rx) = mpsc::channel(8);
    let error_tx = tx.clone();
    let limit = query.limit;
    let rows = fetch_observation_rows(pool, query);

    tokio::spawn(async move {
        if let Err(err) =
            write_parquet_rows(rows, metadata, limit, tx, PARQUET_ROW_GROUP_TARGET_BYTES).await
        {
            if !error_tx.is_closed() {
                let _ = error_tx.send(Err(err)).await;
            }
        }
    });

    async_stream::stream! {
        while let Some(chunk) = rx.recv().await {
            yield chunk;
        }
    }
}

async fn write_parquet_rows<S>(
    rows: S,
    metadata: ObservationsMetadata,
    limit: usize,
    tx: mpsc::Sender<Result<Bytes, ApiError>>,
    row_group_target_bytes: usize,
) -> Result<(), ApiError>
where
    S: Stream<Item = Result<ObservationsRow, ApiError>> + Send,
{
    let schema = parquet_schema();
    let writer = ChannelParquetWriter { tx: tx.clone() };
    let props = parquet_writer_properties(&metadata);
    let mut writer =
        AsyncArrowWriter::try_new(writer, Arc::clone(&schema), Some(props)).map_err(|err| {
            tracing::error!(error = %err, "parquet writer initialization failed");
            ApiError::Internal
        })?;
    let mut batch = ParquetBatchBuilder::default();
    let mut emitted = 0_usize;

    futures::pin_mut!(rows);
    loop {
        if emitted == limit || tx.is_closed() {
            break;
        }

        let Some(row) = (tokio::select! {
            () = tx.closed() => None,
            row = rows.try_next() => row?,
        }) else {
            break;
        };

        batch.push(row)?;
        emitted += 1;
        if batch.len() >= PARQUET_BATCH_ROWS {
            write_parquet_batch(
                &mut writer,
                Arc::clone(&schema),
                &mut batch,
                row_group_target_bytes,
            )
            .await?;
        }
    }

    if tx.is_closed() {
        return Ok(());
    }

    write_parquet_batch(
        &mut writer,
        Arc::clone(&schema),
        &mut batch,
        row_group_target_bytes,
    )
    .await?;
    if tx.is_closed() {
        return Ok(());
    }
    writer.finish().await.map_err(parquet_to_api_error)?;
    Ok(())
}

async fn write_parquet_batch(
    writer: &mut AsyncArrowWriter<ChannelParquetWriter>,
    schema: SchemaRef,
    batch: &mut ParquetBatchBuilder,
    row_group_target_bytes: usize,
) -> Result<(), ApiError> {
    if batch.is_empty() {
        return Ok(());
    }

    let record_batch = batch.finish(schema)?;
    writer
        .write(&record_batch)
        .await
        .map_err(parquet_to_api_error)?;
    if writer.in_progress_size() >= row_group_target_bytes {
        writer.flush().await.map_err(parquet_to_api_error)?;
    }
    Ok(())
}

fn parquet_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("series_key", DataType::Utf8, false),
        Field::new("time", DataType::Utf8, false),
        Field::new("time_precision", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
        Field::new("status", DataType::Utf8, false),
        Field::new("revision_no", DataType::UInt32, false),
        Field::new("dimensions", DataType::Utf8, false),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("ingested_at", DataType::Utf8, false),
        Field::new("source_artifact_id", DataType::Utf8, false),
        Field::new("measure_id", DataType::Utf8, false),
        Field::new("unit", DataType::Utf8, false),
    ]))
}

fn parquet_writer_properties(metadata: &ObservationsMetadata) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_key_value_metadata(Some(vec![
            KeyValue::new(
                "dataflow".to_string(),
                metadata.dataflow.as_str().to_string(),
            ),
            KeyValue::new("license".to_string(), metadata.license.clone()),
            KeyValue::new("attribution".to_string(), metadata.attribution.clone()),
            KeyValue::new("source_url".to_string(), metadata.source_url.clone()),
        ]))
        .build()
}

#[derive(Debug, Default)]
struct ParquetBatchBuilder {
    series_key: Vec<String>,
    time: Vec<String>,
    time_precision: Vec<String>,
    value: Vec<Option<f64>>,
    status: Vec<String>,
    revision_no: Vec<u32>,
    dimensions: Vec<String>,
    attributes: Vec<String>,
    ingested_at: Vec<String>,
    source_artifact_id: Vec<String>,
    measure_id: Vec<String>,
    unit: Vec<String>,
}

impl ParquetBatchBuilder {
    fn push(&mut self, row: ObservationsRow) -> Result<(), ApiError> {
        self.series_key.push(row.series_key.to_string());
        self.time.push(row.time.to_rfc3339());
        self.time_precision
            .push(time_precision_label(row.time_precision).to_string());
        self.value.push(row.value);
        self.status
            .push(observation_status_label(row.status).to_string());
        self.revision_no.push(row.revision_no);
        self.dimensions.push(serialize_json_chunk(&row.dimensions)?);
        self.attributes.push(serialize_json_chunk(&row.attributes)?);
        self.ingested_at.push(row.ingested_at.to_rfc3339());
        self.source_artifact_id
            .push(row.source_artifact_id.to_string());
        self.measure_id.push(row.measure_id);
        self.unit.push(row.unit);
        Ok(())
    }

    fn len(&self) -> usize {
        self.series_key.len()
    }

    fn is_empty(&self) -> bool {
        self.series_key.is_empty()
    }

    fn finish(&mut self, schema: SchemaRef) -> Result<RecordBatch, ApiError> {
        let batch = std::mem::take(self);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(batch.series_key)),
            Arc::new(StringArray::from(batch.time)),
            Arc::new(StringArray::from(batch.time_precision)),
            Arc::new(Float64Array::from(batch.value)),
            Arc::new(StringArray::from(batch.status)),
            Arc::new(UInt32Array::from(batch.revision_no)),
            Arc::new(StringArray::from(batch.dimensions)),
            Arc::new(StringArray::from(batch.attributes)),
            Arc::new(StringArray::from(batch.ingested_at)),
            Arc::new(StringArray::from(batch.source_artifact_id)),
            Arc::new(StringArray::from(batch.measure_id)),
            Arc::new(StringArray::from(batch.unit)),
        ];
        RecordBatch::try_new(schema, columns).map_err(|err| {
            tracing::error!(error = %err, "parquet record batch construction failed");
            ApiError::Internal
        })
    }
}

#[derive(Debug, Clone)]
struct ChannelParquetWriter {
    tx: mpsc::Sender<Result<Bytes, ApiError>>,
}

impl AsyncFileWriter for ChannelParquetWriter {
    fn write(&mut self, bytes: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        async move {
            if bytes.is_empty() {
                return Ok(());
            }
            self.tx
                .send(Ok(bytes))
                .await
                .map_err(|_| parquet_receiver_dropped())?;
            Ok(())
        }
        .boxed()
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        async { Ok(()) }.boxed()
    }
}

fn parquet_receiver_dropped() -> ParquetError {
    ParquetError::External(Box::new(io::Error::new(
        io::ErrorKind::BrokenPipe,
        "parquet response receiver dropped",
    )))
}

fn parquet_to_api_error(err: ParquetError) -> ApiError {
    tracing::error!(error = %err, "parquet response serialization failed");
    ApiError::Internal
}

/// Helpers used by the dedicated Parquet streaming benchmark and DHAT profile.
#[doc(hidden)]
pub mod benchmark_support {
    use std::time::Duration;

    use futures::Stream;

    use super::*;

    /// The Phase 3 Parquet benchmark row count.
    pub const PARQUET_STREAM_BENCHMARK_ROWS: usize = 1_000_000;
    /// The Phase 3 Parquet benchmark wall-clock budget.
    pub const PARQUET_STREAM_BENCHMARK_BUDGET: Duration = Duration::from_secs(30);
    /// The Phase 3 Parquet benchmark peak heap budget.
    pub const PARQUET_STREAM_DHAT_HEAP_BUDGET_BYTES: usize = 100 * 1024 * 1024;
    /// The Phase 5 Parquet scale-validation row count.
    pub const PARQUET_SCALE_VALIDATION_ROWS: usize = 10_000_000;
    /// The Phase 5 Parquet scale-validation wall-clock budget.
    pub const PARQUET_SCALE_VALIDATION_BUDGET: Duration = Duration::from_secs(30);

    /// Summary of a drained synthetic Parquet stream.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ParquetStreamBenchmarkStats {
        /// Number of synthetic rows requested from the writer.
        pub rows: usize,
        /// Number of Parquet bytes emitted by the stream.
        pub bytes: usize,
        /// Number of chunks emitted through the response channel.
        pub chunks: usize,
    }

    /// Stream one million production-shaped rows through the Parquet writer and
    /// drain the response channel without retaining emitted bytes.
    pub async fn drain_synthetic_parquet_stream(
        row_count: usize,
    ) -> Result<ParquetStreamBenchmarkStats, ApiError> {
        let (tx, mut rx) = mpsc::channel(8);
        let rows = synthetic_observation_rows(row_count);
        let writer = tokio::spawn(write_parquet_rows(
            rows,
            benchmark_metadata(),
            row_count,
            tx,
            PARQUET_ROW_GROUP_TARGET_BYTES,
        ));

        let mut bytes = 0_usize;
        let mut chunks = 0_usize;
        while let Some(chunk) = rx.recv().await {
            let chunk = chunk?;
            bytes += chunk.len();
            chunks += 1;
        }

        writer.await.map_err(|err| {
            tracing::error!(error = %err, "parquet benchmark writer task failed");
            ApiError::Internal
        })??;

        Ok(ParquetStreamBenchmarkStats {
            rows: row_count,
            bytes,
            chunks,
        })
    }

    fn benchmark_metadata() -> ObservationsMetadata {
        ObservationsMetadata {
            dataflow: DataflowId::new("abs.cpi").expect("benchmark dataflow id is valid"),
            attribution: "Source: Australian Bureau of Statistics".to_string(),
            license: "CC-BY-4.0".to_string(),
            source_url: "https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia".to_string(),
        }
    }

    fn synthetic_observation_rows(
        row_count: usize,
    ) -> impl Stream<Item = Result<ObservationsRow, ApiError>> + Send + 'static {
        async_stream::try_stream! {
            let dataflow = DataflowId::new("abs.cpi").expect("benchmark dataflow id is valid");
            let artifact = ArtifactId::of_content(b"parquet 1m benchmark artifact");
            let ingested_at = DateTime::from_timestamp(1_714_000_000, 0)
                .expect("benchmark ingested timestamp is valid");
            let observed_at = DateTime::from_timestamp(1_577_836_800, 0)
                .expect("benchmark observation timestamp is valid");
            let regions = ["AUS", "NSW", "VIC", "QLD", "WA"];

            for idx in 0..row_count {
                let region = regions[idx % regions.len()];
                let dimensions: BTreeMap<String, String> =
                    [("region".to_string(), region.to_string())].into_iter().collect();
                let series_key = SeriesKey::derive(&dataflow, [("region", region)]);
                yield ObservationsRow {
                    series_key,
                    time: observed_at + chrono::Duration::seconds(idx as i64),
                    time_precision: TimePrecision::Day,
                    value: Some(100.0 + (idx as f64 / 10.0)),
                    status: ObservationStatus::Normal,
                    revision_no: 0,
                    attributes: BTreeMap::new(),
                    ingested_at,
                    source_artifact_id: artifact,
                    dimensions,
                    measure_id: "index".to_string(),
                    unit: "index".to_string(),
                };

                if idx % PARQUET_BATCH_ROWS == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }
    }
}

fn fetch_observation_rows(
    pool: PgPool,
    query: ParsedObservationsQuery,
) -> impl Stream<Item = Result<ObservationsRow, ApiError>> + Send + 'static {
    async_stream::try_stream! {
        let mut builder = QueryBuilder::<Postgres>::new("");
        push_observation_select(&mut builder, rollup_grain(&query));
        push_observation_filters(&mut builder, &query);
        builder.push(" ORDER BY o.time ASC, o.series_key ASC LIMIT ");
        builder.push_bind((query.limit + 1) as i64);

        let mut rows = builder.build().fetch(&pool);
        while let Some(row) = rows.try_next().await? {
            yield row_to_observation(row)?;
        }
    }
}

fn push_observation_select(
    builder: &mut QueryBuilder<'_, Postgres>,
    rollup_grain: Option<RollupGrain>,
) {
    builder.push(
        "SELECT o.series_key,
                o.time,
                o.revision_no,
                ",
    );

    if let Some(grain) = rollup_grain {
        builder.push("'");
        builder.push(grain.time_precision_label());
        builder.push(
            "'::text AS time_precision,
                o.value,
                'normal'::text AS status,
                jsonb_build_object(
                    'aggregate', 'avg',
                    'rollup_grain', '",
        );
        builder.push(grain.query_label());
        builder.push(
            "',
                    'observations_count', o.observations_count::text,
                    'min_value', o.min_value::text,
                    'max_value', o.max_value::text
                ) AS attributes,
                o.ingested_at,
                o.source_artifact_id,
                s.dimensions,
                s.measure_id,
                s.unit
         FROM ",
        );
        builder.push(grain.view_name());
        builder.push(" o");
    } else {
        builder.push(
            "o.time_precision,
                o.value,
                o.status,
                o.attributes,
                o.ingested_at,
                o.source_artifact_id,
                s.dimensions,
                s.measure_id,
                s.unit
         FROM observations_latest o",
        );
    }

    builder.push(
        "
         JOIN series s ON s.series_key = o.series_key
         JOIN dataflows d ON d.id = s.dataflow_id",
    );
}

fn push_observation_filters(
    builder: &mut QueryBuilder<'_, Postgres>,
    query: &ParsedObservationsQuery,
) {
    builder.push(" WHERE s.dataflow_id = ");
    builder.push_bind(query.dataflow.as_str().to_string());

    if !query.dimensions.is_empty() {
        builder.push(" AND s.dimensions @> ");
        builder.push_bind(serde_json::json!(query.dimensions));
        builder.push("::jsonb");
    }

    if let Some(since) = query.since {
        builder.push(" AND o.time >= ");
        builder.push_bind(since);
    }

    if let Some(until) = query.until {
        builder.push(" AND o.time <= ");
        builder.push_bind(until);
    }

    if let Some(FrequencyQuery::Dataflow(frequency)) = &query.frequency {
        builder.push(" AND d.frequency = ");
        builder.push_bind(frequency.clone());
    }

    if let Some(cursor) = query.cursor {
        builder.push(" AND (o.time, o.series_key) > (");
        builder.push_bind(cursor.time);
        builder.push(", ");
        builder.push_bind(cursor.series_key.digest().as_bytes().to_vec());
        builder.push(")");
    }
}

fn row_to_observation(row: PgRow) -> Result<ObservationsRow, ApiError> {
    let revision_no = row.try_get::<i32, _>("revision_no")?;
    let attributes = json_map(row.try_get("attributes")?)?;
    let dimensions = json_map(row.try_get("dimensions")?)?;

    Ok(ObservationsRow {
        series_key: series_key_from_bytes(row.try_get("series_key")?)?,
        time: row.try_get("time")?,
        time_precision: parse_time_precision(row.try_get("time_precision")?)?,
        value: row.try_get("value")?,
        status: parse_observation_status(row.try_get("status")?)?,
        revision_no: u32::try_from(revision_no).map_err(|err| {
            tracing::error!(error = %err, revision_no, "database returned invalid revision_no");
            ApiError::Internal
        })?,
        attributes,
        ingested_at: row.try_get("ingested_at")?,
        source_artifact_id: artifact_id_from_bytes(row.try_get("source_artifact_id")?)?,
        dimensions,
        measure_id: row.try_get("measure_id")?,
        unit: row.try_get("unit")?,
    })
}

fn json_map(value: serde_json::Value) -> Result<BTreeMap<String, String>, ApiError> {
    serde_json::from_value(value).map_err(|err| {
        tracing::error!(error = %err, "database JSON map was not string-valued");
        ApiError::Internal
    })
}

fn parse_time_precision(value: &str) -> Result<TimePrecision, ApiError> {
    match value {
        "day" => Ok(TimePrecision::Day),
        "week" => Ok(TimePrecision::Week),
        "month" => Ok(TimePrecision::Month),
        "quarter" => Ok(TimePrecision::Quarter),
        "year" => Ok(TimePrecision::Year),
        _ => {
            tracing::error!(
                time_precision = value,
                "database returned invalid time precision"
            );
            Err(ApiError::Internal)
        }
    }
}

fn parse_observation_status(value: &str) -> Result<ObservationStatus, ApiError> {
    match value {
        "normal" => Ok(ObservationStatus::Normal),
        "estimated" => Ok(ObservationStatus::Estimated),
        "forecast" => Ok(ObservationStatus::Forecast),
        "imputed" => Ok(ObservationStatus::Imputed),
        "missing" => Ok(ObservationStatus::Missing),
        "provisional" => Ok(ObservationStatus::Provisional),
        "revised" => Ok(ObservationStatus::Revised),
        "break" => Ok(ObservationStatus::Break),
        _ => {
            tracing::error!(
                status = value,
                "database returned invalid observation status"
            );
            Err(ApiError::Internal)
        }
    }
}

fn series_key_from_bytes(bytes: Vec<u8>) -> Result<SeriesKey, ApiError> {
    digest_from_bytes(bytes).map(SeriesKey::from_digest)
}

fn artifact_id_from_bytes(bytes: Vec<u8>) -> Result<ArtifactId, ApiError> {
    digest_from_bytes(bytes).map(ArtifactId::from_digest)
}

fn digest_from_bytes(bytes: Vec<u8>) -> Result<Sha256Digest, ApiError> {
    let length = bytes.len();
    let bytes = bytes.try_into().map_err(|_| {
        tracing::error!(length, "database returned invalid SHA-256 digest length");
        ApiError::Internal
    })?;
    Ok(Sha256Digest::from_bytes(bytes))
}

fn serialize_json_chunk<T: Serialize>(value: &T) -> Result<String, ApiError> {
    serde_json::to_string(value).map_err(|err| {
        tracing::error!(error = %err, "response serialization failed");
        ApiError::Internal
    })
}

fn row_to_csv(row: &ObservationsRow) -> Result<String, ApiError> {
    let dimensions = serialize_json_chunk(&row.dimensions)?;
    let attributes = serialize_json_chunk(&row.attributes)?;
    let value = row
        .value
        .map_or_else(String::new, |value| value.to_string());
    let fields = [
        row.series_key.to_string(),
        row.time.to_rfc3339(),
        serde_json::to_string(&row.time_precision)
            .map_err(|err| {
                tracing::error!(error = %err, "time precision serialization failed");
                ApiError::Internal
            })?
            .trim_matches('"')
            .to_string(),
        value,
        serde_json::to_string(&row.status)
            .map_err(|err| {
                tracing::error!(error = %err, "status serialization failed");
                ApiError::Internal
            })?
            .trim_matches('"')
            .to_string(),
        row.revision_no.to_string(),
        dimensions,
        attributes,
        row.ingested_at.to_rfc3339(),
        row.source_artifact_id.to_string(),
        row.measure_id.clone(),
        row.unit.clone(),
    ];
    Ok(format!(
        "{}\n",
        fields
            .into_iter()
            .map(|field| csv_escape(&field))
            .collect::<Vec<_>>()
            .join(",")
    ))
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn time_precision_label(value: TimePrecision) -> &'static str {
    match value {
        TimePrecision::Day => "day",
        TimePrecision::Week => "week",
        TimePrecision::Month => "month",
        TimePrecision::Quarter => "quarter",
        TimePrecision::Year => "year",
    }
}

fn observation_status_label(value: ObservationStatus) -> &'static str {
    match value {
        ObservationStatus::Normal => "normal",
        ObservationStatus::Estimated => "estimated",
        ObservationStatus::Forecast => "forecast",
        ObservationStatus::Imputed => "imputed",
        ObservationStatus::Missing => "missing",
        ObservationStatus::Provisional => "provisional",
        ObservationStatus::Revised => "revised",
        ObservationStatus::Break => "break",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use au_kpis_domain::ids::{ArtifactId, DataflowId, SeriesKey};
    use chrono::{TimeZone, Utc};

    #[test]
    fn parses_dimensions_dates_cursor_and_limit_from_query_string() {
        let dataflow = DataflowId::new("abs.cpi").unwrap();
        let series_key = SeriesKey::derive(&dataflow, [("region", "AUS")]);
        let cursor = encode_cursor(&ObservationCursor {
            time: Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap(),
            series_key,
        })
        .unwrap();

        let query = parse_observations_query(Some(&format!(
            "dataflow=abs.cpi&dimensions%5Bregion%5D=AUS&since=2024-01-01&until=2024-06-30&frequency=quarterly&format=csv&cursor={cursor}&limit=500"
        )))
        .unwrap();

        assert_eq!(query.dataflow.as_str(), "abs.cpi");
        assert_eq!(
            query.dimensions.get("region").map(String::as_str),
            Some("AUS")
        );
        assert_eq!(
            query.since,
            Some(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap())
        );
        assert_eq!(
            query.until,
            Some(Utc.with_ymd_and_hms(2024, 6, 30, 0, 0, 0).unwrap())
        );
        assert_eq!(
            query.frequency,
            Some(FrequencyQuery::Rollup(RollupGrain::Quarterly))
        );
        assert_eq!(query.format, ResponseFormat::Csv);
        assert_eq!(query.limit, 500);
        assert_eq!(query.cursor.unwrap().series_key, series_key);
    }

    #[test]
    fn parses_parquet_format() {
        let query = parse_observations_query(Some("dataflow=abs.cpi&format=parquet")).unwrap();

        assert_eq!(query.format, ResponseFormat::Parquet);
    }

    #[test]
    fn only_first_page_json_observations_use_response_cache() {
        let first_page = parse_observations_query(Some("dataflow=abs.cpi&limit=500")).unwrap();
        assert!(should_cache_json_observations(&first_page));
        let key = observations_json_cache_key(Some("dataflow=abs.cpi&limit=500"), &first_page);
        assert!(key.starts_with("observations:v1:json:"));
        assert!(!key.contains("dataflow=abs.cpi"));

        let csv = parse_observations_query(Some("dataflow=abs.cpi&format=csv")).unwrap();
        assert!(!should_cache_json_observations(&csv));

        let cursor = encode_cursor(&ObservationCursor {
            time: Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap(),
            series_key: SeriesKey::derive(
                &DataflowId::new("abs.cpi").unwrap(),
                [("region", "AUS")],
            ),
        })
        .unwrap();
        let cursor_page =
            parse_observations_query(Some(&format!("dataflow=abs.cpi&cursor={cursor}"))).unwrap();
        assert!(!should_cache_json_observations(&cursor_page));
    }

    #[tokio::test]
    async fn parquet_writer_returns_promptly_when_response_receiver_closes_mid_stream() {
        let (tx, rx) = mpsc::channel(1);
        let (pending_tx, pending_rx) = tokio::sync::oneshot::channel();
        let metadata = ObservationsMetadata {
            dataflow: DataflowId::new("abs.cpi").unwrap(),
            attribution: "Source: Australian Bureau of Statistics".into(),
            license: "CC-BY-4.0".into(),
            source_url: "https://www.abs.gov.au/".into(),
        };
        let rows =
            futures::stream::unfold((0_u8, Some(pending_tx)), |(step, pending_tx)| async move {
                if step == 0 {
                    return Some((Ok(test_observation_row()), (1, pending_tx)));
                }
                if let Some(pending_tx) = pending_tx {
                    let _ = pending_tx.send(());
                }
                futures::future::pending::<Option<(Result<ObservationsRow, ApiError>, _)>>().await
            });
        let writer = tokio::spawn(write_parquet_rows(
            rows,
            metadata,
            10,
            tx,
            PARQUET_ROW_GROUP_TARGET_BYTES,
        ));

        pending_rx
            .await
            .expect("writer should poll the second upstream row");
        drop(rx);

        let result = tokio::time::timeout(std::time::Duration::from_millis(100), writer).await;

        assert!(
            result.is_ok(),
            "writer should not await an abandoned stream"
        );
        assert!(result.unwrap().expect("writer task").is_ok());
    }

    #[test]
    fn rejects_missing_dataflow_and_excessive_limits_before_db_access() {
        let missing = parse_observations_query(Some("limit=10")).unwrap_err();
        assert!(missing.to_string().contains("dataflow"));

        let too_large = parse_observations_query(Some("dataflow=abs.cpi&limit=10001")).unwrap_err();
        assert!(too_large.to_string().contains("limit"));
    }

    #[test]
    fn cursor_roundtrips_as_opaque_base64_payload() {
        let dataflow = DataflowId::new("abs.cpi").unwrap();
        let cursor = ObservationCursor {
            time: Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap(),
            series_key: SeriesKey::derive(&dataflow, [("region", "AUS")]),
        };

        let encoded = encode_cursor(&cursor).unwrap();

        assert!(!encoded.contains("2024-03-01"));
        assert_eq!(decode_cursor(&encoded).unwrap(), cursor);
    }

    fn test_observation_row() -> ObservationsRow {
        let dataflow = DataflowId::new("abs.cpi").unwrap();
        ObservationsRow {
            series_key: SeriesKey::derive(&dataflow, [("region", "AUS")]),
            time: Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap(),
            time_precision: TimePrecision::Quarter,
            value: Some(135.0),
            status: ObservationStatus::Normal,
            revision_no: 1,
            attributes: BTreeMap::new(),
            ingested_at: Utc.with_ymd_and_hms(2024, 4, 24, 0, 0, 0).unwrap(),
            source_artifact_id: ArtifactId::of_content(b"parquet cancellation fixture"),
            dimensions: BTreeMap::from([("region".to_string(), "AUS".to_string())]),
            measure_id: "cpi".into(),
            unit: "index".into(),
        }
    }
}
