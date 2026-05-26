//! `/v1/observations` query handling and streaming renderers.

use std::{collections::BTreeMap, fmt};

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
use futures::{Stream, TryStreamExt};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};
use utoipa::ToSchema;

use crate::{AppState, error::ApiError};

const DEFAULT_LIMIT: usize = 1_000;
const MAX_LIMIT: usize = 10_000;
const CACHE_CONTROL_VALUE: &str = "public, max-age=60, stale-while-revalidate=300";

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
        ("dataflow" = String, Query, description = "Required dataflow id, e.g. abs.cpi."),
        ("dimensions[]" = Option<Vec<String>>, Query, description = "Dimension filters as repeated key=value values. The handler also accepts dimensions[region]=AUS."),
        ("since" = Option<String>, Query, description = "Inclusive lower time bound as YYYY-MM-DD or RFC3339."),
        ("until" = Option<String>, Query, description = "Inclusive upper time bound as YYYY-MM-DD or RFC3339."),
        ("frequency" = Option<String>, Query, description = "Optional dataflow frequency filter."),
        ("format" = Option<String>, Query, description = "Response format: json or csv."),
        ("cursor" = Option<String>, Query, description = "Opaque cursor from the previous page."),
        ("limit" = Option<u32>, Query, description = "Page size, maximum 10000.")
    ),
    responses(
        (
            status = 200,
            description = "Observation page.",
            content(
                (ObservationsResponse = "application/json"),
                (String = "text/csv")
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
    };
    let stream = match query.format {
        ResponseFormat::Json => {
            Body::from_stream(json_observations_stream(state.db.clone(), query, metadata))
        }
        ResponseFormat::Csv => {
            Body::from_stream(csv_observations_stream(state.db.clone(), query, metadata))
        }
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedObservationsQuery {
    dataflow: DataflowId,
    dimensions: BTreeMap<String, String>,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
    frequency: Option<String>,
    format: ResponseFormat,
    cursor: Option<ObservationCursor>,
    limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseFormat {
    Json,
    Csv,
}

impl fmt::Display for ResponseFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResponseFormat::Json => f.write_str("json"),
            ResponseFormat::Csv => f.write_str("csv"),
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

fn parse_frequency(value: &str) -> Result<String, ApiError> {
    match value {
        "daily" | "weekly" | "monthly" | "quarterly" | "annual" | "irregular" => {
            Ok(value.to_string())
        }
        _ => Err(ApiError::Validation(format!(
            "unsupported frequency `{value}`"
        ))),
    }
}

fn parse_format(value: &str) -> Result<ResponseFormat, ApiError> {
    match value {
        "json" => Ok(ResponseFormat::Json),
        "csv" => Ok(ResponseFormat::Csv),
        "parquet" => Err(ApiError::Validation(
            "format=parquet is planned for phase 3".into(),
        )),
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
         FROM observations_latest o
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

fn fetch_observation_rows(
    pool: PgPool,
    query: ParsedObservationsQuery,
) -> impl Stream<Item = Result<ObservationsRow, ApiError>> + Send + 'static {
    async_stream::try_stream! {
        let mut builder = QueryBuilder::<Postgres>::new(
            "SELECT o.series_key,
                    o.time,
                    o.revision_no,
                    o.time_precision,
                    o.value,
                    o.status,
                    o.attributes,
                    o.ingested_at,
                    o.source_artifact_id,
                    s.dimensions,
                    s.measure_id,
                    s.unit
             FROM observations_latest o
             JOIN series s ON s.series_key = o.series_key
             JOIN dataflows d ON d.id = s.dataflow_id",
        );
        push_observation_filters(&mut builder, &query);
        builder.push(" ORDER BY o.time ASC, o.series_key ASC LIMIT ");
        builder.push_bind((query.limit + 1) as i64);

        let mut rows = builder.build().fetch(&pool);
        while let Some(row) = rows.try_next().await? {
            yield row_to_observation(row)?;
        }
    }
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

    if let Some(frequency) = &query.frequency {
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

#[cfg(test)]
mod tests {
    use super::*;
    use au_kpis_domain::ids::{DataflowId, SeriesKey};
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
        assert_eq!(query.frequency.as_deref(), Some("quarterly"));
        assert_eq!(query.format, ResponseFormat::Csv);
        assert_eq!(query.limit, 500);
        assert_eq!(query.cursor.unwrap().series_key, series_key);
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
}
