//! `/v1/search` catalog search endpoint.

use std::time::Duration;

use au_kpis_domain::ids::{DataflowId, SourceId};
use axum::{
    Json,
    extract::State,
    http::{HeaderValue, Uri, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use utoipa::ToSchema;

use crate::{ApiError, AppState};

const SEARCH_CACHE_TTL: Duration = Duration::from_secs(60 * 60);
const SEARCH_CACHE_CONTROL: &str = "public, max-age=3600, stale-while-revalidate=86400";
const DEFAULT_LIMIT: u16 = 20;
const MAX_LIMIT: u16 = 100;

/// Query parameters for `GET /v1/search`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SearchQuery {
    /// Search text. Blank queries are rejected.
    pub q: String,
    /// Maximum number of catalog results to return.
    pub limit: Option<u16>,
}

/// Response envelope for `GET /v1/search`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SearchResponse {
    /// Normalized query text used for ranking.
    pub query: String,
    /// Ranked catalog matches.
    pub results: Vec<SearchResult>,
}

/// A ranked catalog search result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SearchResult {
    /// Matched catalog object type.
    pub kind: SearchResultKind,
    /// Stable catalog identifier.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Optional catalog description.
    pub description: Option<String>,
    /// Relevance score. Larger scores rank first.
    pub score: f64,
    /// Source id for dataflow results.
    pub source_id: Option<SourceId>,
    /// Dataflows directly represented by this match.
    pub dataflow_ids: Vec<DataflowId>,
}

/// Catalog object types returned by search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SearchResultKind {
    /// A dataflow catalog entry.
    Dataflow,
    /// A measure catalog entry.
    Measure,
}

/// `GET /v1/search`.
#[utoipa::path(
    get,
    path = "/v1/search",
    operation_id = "searchCatalog",
    params(
        ("q" = String, Query, min_length = 1, description = "Search text."),
        ("limit" = Option<u16>, Query, maximum = 65535, description = "Maximum number of results, capped at 100.")
    ),
    responses(
        (status = 200, description = "Ranked catalog search results.", body = SearchResponse, content_type = "application/json", headers(
            ("Cache-Control" = String, description = "Public CDN cache policy.")
        )),
        (status = 400, description = "Invalid query.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 500, description = "Internal error.", body = crate::ProblemDetails, content_type = "application/problem+json")
    ),
    tag = "search"
)]
pub async fn search_catalog(State(state): State<AppState>, uri: Uri) -> Result<Response, ApiError> {
    let query = parse_search_query(uri.query())?;
    let cache_key = search_cache_key(&query);
    if let Some(cached) = state.cache.get_json::<SearchResponse>(&cache_key).await? {
        return Ok(search_response(cached));
    }

    let response = SearchResponse {
        results: load_search_results(&state.db, &query).await?,
        query: query.q,
    };
    state
        .cache
        .set_json(&cache_key, &response, SEARCH_CACHE_TTL)
        .await?;
    Ok(search_response(response))
}

fn search_response<T>(body: T) -> Response
where
    T: Serialize,
{
    let mut response = Json(body).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(SEARCH_CACHE_CONTROL),
    );
    response
}

fn parse_search_query(raw: Option<&str>) -> Result<SearchQuery, ApiError> {
    let mut q = None;
    let mut limit = None;

    for (key, value) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
        match key.as_ref() {
            "q" => q = Some(value.trim().to_string()),
            "limit" => limit = Some(parse_limit(&value)?),
            _ => {}
        }
    }

    let q = q.ok_or_else(|| ApiError::Validation("missing search query `q`".into()))?;
    if q.is_empty() {
        return Err(ApiError::Validation(
            "search query `q` must not be blank".into(),
        ));
    }
    if q.contains('\0') {
        return Err(ApiError::Validation(
            "search query `q` must not contain NUL bytes".into(),
        ));
    }

    Ok(SearchQuery { q, limit })
}

fn parse_limit(value: &str) -> Result<u16, ApiError> {
    let limit = value
        .parse::<u16>()
        .map_err(|err| ApiError::Validation(format!("invalid search limit `{value}`: {err}")))?;
    Ok(limit.clamp(1, MAX_LIMIT))
}

fn search_cache_key(query: &SearchQuery) -> String {
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    let encoded_query: String = url::form_urlencoded::byte_serialize(query.q.as_bytes()).collect();
    format!("api:search:q={encoded_query}:limit={limit}")
}

async fn load_search_results(
    pool: &PgPool,
    query: &SearchQuery,
) -> Result<Vec<SearchResult>, ApiError> {
    let rows = sqlx::query(
        "WITH search_input AS (
            SELECT websearch_to_tsquery('english', $1) AS tsq,
                   $1::text AS raw,
                   upper(regexp_replace($1::text, '[^[:alnum:]]', '', 'g')) AS compact_raw
         ),
         dataflow_rows AS (
            SELECT d.*,
                   upper(regexp_replace(d.name, '([[:alnum:]])[[:alnum:]]*[[:space:]]*', '\\1', 'g')) AS name_acronym
            FROM dataflows d
         ),
         dataflow_matches AS (
            SELECT 'dataflow' AS kind,
                   d.id,
                   d.name,
                   d.description,
                   d.source_id,
                   ARRAY[d.id]::text[] AS dataflow_ids,
                   (
                       ts_rank_cd(
                           to_tsvector('english', d.name || ' ' || COALESCE(d.description, '')),
                           si.tsq
                       )
                       + GREATEST(
                           similarity(d.name, si.raw),
                           similarity(COALESCE(d.description, ''), si.raw)
                       ) * 0.2
                       + CASE WHEN d.name_acronym = si.compact_raw THEN 1.0 ELSE 0.0 END
                       + 0.5
                   )::double precision AS score
            FROM dataflow_rows d
            CROSS JOIN search_input si
            WHERE to_tsvector('english', d.name || ' ' || COALESCE(d.description, '')) @@ si.tsq
               OR d.name % si.raw
               OR d.description % si.raw
               OR d.name_acronym = si.compact_raw
         ),
         measure_matches AS (
            SELECT 'measure' AS kind,
                   m.id,
                   m.name,
                   m.description,
                   NULL::text AS source_id,
                   COALESCE(
                       array_agg(DISTINCT d.id ORDER BY d.id) FILTER (WHERE d.id IS NOT NULL),
                       ARRAY[]::text[]
                   ) AS dataflow_ids,
                   (
                       ts_rank_cd(
                           to_tsvector('english', m.name || ' ' || COALESCE(m.description, '')),
                           si.tsq
                       )
                       + GREATEST(
                           similarity(m.name, si.raw),
                           similarity(COALESCE(m.description, ''), si.raw)
                       ) * 0.2
                   )::double precision AS score
            FROM measures m
            CROSS JOIN search_input si
            LEFT JOIN dataflows d ON m.id = ANY(d.measures)
            WHERE to_tsvector('english', m.name || ' ' || COALESCE(m.description, '')) @@ si.tsq
               OR m.name % si.raw
               OR m.description % si.raw
            GROUP BY m.id, m.name, m.description, si.tsq, si.raw
         )
         SELECT kind, id, name, description, source_id, dataflow_ids, score
         FROM (
            SELECT * FROM dataflow_matches
            UNION ALL
            SELECT * FROM measure_matches
         ) matches
         WHERE score > 0
         ORDER BY score DESC, kind, id
         LIMIT $2",
    )
    .bind(&query.q)
    .bind(i64::from(query.limit.unwrap_or(DEFAULT_LIMIT)))
    .fetch_all(pool)
    .await?;

    rows.into_iter().map(search_result_from_row).collect()
}

fn search_result_from_row(row: sqlx::postgres::PgRow) -> Result<SearchResult, ApiError> {
    let kind = match row.try_get::<String, _>("kind")?.as_str() {
        "dataflow" => SearchResultKind::Dataflow,
        "measure" => SearchResultKind::Measure,
        other => {
            tracing::error!(kind = other, "database returned invalid search result kind");
            return Err(ApiError::Internal);
        }
    };
    let source_id = row
        .try_get::<Option<String>, _>("source_id")?
        .map(source_id_from_str)
        .transpose()?;
    let dataflow_ids = row
        .try_get::<Vec<String>, _>("dataflow_ids")?
        .into_iter()
        .map(dataflow_id_from_str)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(SearchResult {
        kind,
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        description: row.try_get("description")?,
        score: row.try_get("score")?,
        source_id,
        dataflow_ids,
    })
}

fn source_id_from_str(value: String) -> Result<SourceId, ApiError> {
    SourceId::new(value).map_err(invalid_db_id)
}

fn dataflow_id_from_str(value: String) -> Result<DataflowId, ApiError> {
    DataflowId::new(value).map_err(invalid_db_id)
}

fn invalid_db_id(err: au_kpis_domain::ids::IdError) -> ApiError {
    tracing::error!(%err, "database returned invalid identifier");
    ApiError::Internal
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_query_trims_and_cache_key_encodes_form_query() {
        let query = parse_search_query(Some("q=%20price%20index%20")).unwrap();

        assert_eq!(
            query,
            SearchQuery {
                q: "price index".into(),
                limit: None,
            }
        );
        assert_eq!(
            search_cache_key(&query),
            "api:search:q=price+index:limit=20"
        );
    }

    #[test]
    fn search_query_rejects_missing_or_blank_query() {
        assert!(parse_search_query(None).is_err());
        assert!(parse_search_query(Some("q=%20")).is_err());
    }

    #[test]
    fn search_query_clamps_limit_to_public_cap() {
        let query = parse_search_query(Some("q=cpi&limit=1000")).unwrap();

        assert_eq!(query.limit, Some(MAX_LIMIT));
        assert_eq!(search_cache_key(&query), "api:search:q=cpi:limit=100");
    }

    #[test]
    fn search_query_rejects_invalid_values_and_ignores_unknown_keys() {
        assert!(parse_search_query(Some("q=bad%00query")).is_err());
        assert!(parse_search_query(Some("q=cpi&limit=not-a-number")).is_err());

        let query = parse_search_query(Some("ignored=true&q=cpi&limit=0")).unwrap();
        assert_eq!(
            query,
            SearchQuery {
                q: "cpi".into(),
                limit: Some(1),
            }
        );
    }
}
