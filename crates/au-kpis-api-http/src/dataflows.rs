//! `/v1/dataflows` catalog endpoints.

use std::time::Duration;

use au_kpis_domain::{
    Code, Codelist, Dataflow, Dimension, Frequency, License,
    ids::{CodeId, CodelistId, DataflowId, DimensionId, MeasureId, SourceId},
};
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderValue, Uri, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use utoipa::ToSchema;

use crate::{ApiError, AppState};

const CATALOG_CACHE_TTL: Duration = Duration::from_secs(60 * 60);
const CATALOG_CACHE_CONTROL: &str = "public, max-age=3600, stale-while-revalidate=86400";

/// Query parameters for `GET /v1/dataflows`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DataflowsQuery {
    /// Optional source id filter, e.g. `abs`.
    pub source: Option<SourceId>,
    /// Optional publication frequency filter.
    pub frequency: Option<Frequency>,
}

/// Response envelope for `GET /v1/dataflows`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DataflowsResponse {
    /// Matching dataflows, ordered by source then dataflow id.
    pub dataflows: Vec<Dataflow>,
}

/// Response envelope for `GET /v1/dataflows/{id}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DataflowDetailResponse {
    /// Dataflow metadata.
    pub dataflow: Dataflow,
    /// Ordered dimension metadata for the dataflow.
    pub dimensions: Vec<Dimension>,
}

/// Response envelope for `GET /v1/dataflows/{id}/codelists/{dim}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DataflowCodelistResponse {
    /// Requested dataflow id.
    pub dataflow_id: DataflowId,
    /// Requested dimension id.
    pub dimension_id: DimensionId,
    /// Codelist attached to the requested dimension.
    pub codelist: Codelist,
}

/// `GET /v1/dataflows`.
#[utoipa::path(
    get,
    path = "/v1/dataflows",
    operation_id = "listDataflows",
    params(
        ("source" = Option<String>, Query, min_length = 1, max_length = 128, description = "Optional source id filter, e.g. abs."),
        ("frequency" = Option<String>, Query, min_length = 1, pattern = "^(annual|quarterly|monthly|weekly|daily|irregular)$", description = "Optional publication frequency filter.")
    ),
    responses(
        (status = 200, description = "Dataflow catalog page.", body = DataflowsResponse, content_type = "application/json", headers(
            ("Cache-Control" = String, description = "Public CDN cache policy.")
        )),
        (status = 400, description = "Invalid query.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 500, description = "Internal error.", body = crate::ProblemDetails, content_type = "application/problem+json")
    ),
    tag = "dataflows"
)]
pub async fn list_dataflows(State(state): State<AppState>, uri: Uri) -> Result<Response, ApiError> {
    let query = parse_dataflows_query(uri.query())?;
    let cache_key = list_cache_key(&query);
    if let Some(cached) = state
        .cache
        .get_json::<DataflowsResponse>(&cache_key)
        .await?
    {
        return Ok(catalog_response(cached));
    }

    let response = DataflowsResponse {
        dataflows: load_dataflows(&state.db, &query).await?,
    };
    state
        .cache
        .set_json(&cache_key, &response, CATALOG_CACHE_TTL)
        .await?;
    Ok(catalog_response(response))
}

/// `GET /v1/dataflows/{id}`.
#[utoipa::path(
    get,
    path = "/v1/dataflows/{id}",
    operation_id = "getDataflow",
    params(
        ("id" = String, Path, min_length = 1, max_length = 128, description = "Dataflow id.")
    ),
    responses(
        (status = 200, description = "Dataflow metadata and dimensions.", body = DataflowDetailResponse, content_type = "application/json", headers(
            ("Cache-Control" = String, description = "Public CDN cache policy.")
        )),
        (status = 400, description = "Invalid path parameter.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 404, description = "Dataflow not found.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 500, description = "Internal error.", body = crate::ProblemDetails, content_type = "application/problem+json")
    ),
    tag = "dataflows"
)]
pub async fn get_dataflow(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let id =
        DataflowId::new(id).map_err(|err| ApiError::Validation(format!("invalid id: {err}")))?;
    let cache_key = format!("api:dataflows:get:{id}");
    if let Some(cached) = state
        .cache
        .get_json::<DataflowDetailResponse>(&cache_key)
        .await?
    {
        return Ok(catalog_response(cached));
    }

    let dataflow = load_dataflow(&state.db, &id).await?;
    let dimensions = load_dimensions(&state.db, &id).await?;
    let response = DataflowDetailResponse {
        dataflow,
        dimensions,
    };
    state
        .cache
        .set_json(&cache_key, &response, CATALOG_CACHE_TTL)
        .await?;
    Ok(catalog_response(response))
}

/// `GET /v1/dataflows/{id}/codelists/{dim}`.
#[utoipa::path(
    get,
    path = "/v1/dataflows/{id}/codelists/{dim}",
    operation_id = "getDataflowCodelist",
    params(
        ("id" = String, Path, min_length = 1, max_length = 128, description = "Dataflow id."),
        ("dim" = String, Path, min_length = 1, max_length = 128, description = "Dimension id.")
    ),
    responses(
        (status = 200, description = "Codelist for the requested dimension.", body = DataflowCodelistResponse, content_type = "application/json", headers(
            ("Cache-Control" = String, description = "Public CDN cache policy.")
        )),
        (status = 400, description = "Invalid path parameter.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 404, description = "Dataflow or dimension not found.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 500, description = "Internal error.", body = crate::ProblemDetails, content_type = "application/problem+json")
    ),
    tag = "dataflows"
)]
pub async fn get_dataflow_codelist(
    State(state): State<AppState>,
    Path((id, dim)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let dataflow_id =
        DataflowId::new(id).map_err(|err| ApiError::Validation(format!("invalid id: {err}")))?;
    let dimension_id = DimensionId::new(dim)
        .map_err(|err| ApiError::Validation(format!("invalid dimension: {err}")))?;
    let cache_key = format!("api:dataflows:codelist:{dataflow_id}:{dimension_id}");
    if let Some(cached) = state
        .cache
        .get_json::<DataflowCodelistResponse>(&cache_key)
        .await?
    {
        return Ok(catalog_response(cached));
    }

    let codelist = load_codelist(&state.db, &dataflow_id, &dimension_id).await?;
    let response = DataflowCodelistResponse {
        dataflow_id,
        dimension_id,
        codelist,
    };
    state
        .cache
        .set_json(&cache_key, &response, CATALOG_CACHE_TTL)
        .await?;
    Ok(catalog_response(response))
}

fn catalog_response<T>(body: T) -> Response
where
    T: Serialize,
{
    let mut response = Json(body).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CATALOG_CACHE_CONTROL),
    );
    response
}

fn parse_dataflows_query(raw: Option<&str>) -> Result<DataflowsQuery, ApiError> {
    let mut source = None;
    let mut frequency = None;

    for (key, value) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
        match key.as_ref() {
            "source" => {
                source = Some(
                    SourceId::new(value.into_owned())
                        .map_err(|err| ApiError::Validation(format!("invalid source: {err}")))?,
                );
            }
            "frequency" => {
                frequency = Some(parse_frequency(&value)?);
            }
            _ => {}
        }
    }

    Ok(DataflowsQuery { source, frequency })
}

fn parse_frequency(value: &str) -> Result<Frequency, ApiError> {
    match value {
        "daily" => Ok(Frequency::Daily),
        "weekly" => Ok(Frequency::Weekly),
        "monthly" => Ok(Frequency::Monthly),
        "quarterly" => Ok(Frequency::Quarterly),
        "annual" => Ok(Frequency::Annual),
        "irregular" => Ok(Frequency::Irregular),
        _ => Err(ApiError::Validation(format!(
            "unsupported frequency `{value}`"
        ))),
    }
}

fn list_cache_key(query: &DataflowsQuery) -> String {
    let source = query.source.as_ref().map_or("-", SourceId::as_str);
    let frequency = query
        .frequency
        .as_ref()
        .map_or("-", |frequency| frequency_as_str(*frequency));
    format!("api:dataflows:list:source={source}:frequency={frequency}")
}

fn frequency_as_str(frequency: Frequency) -> &'static str {
    match frequency {
        Frequency::Daily => "daily",
        Frequency::Weekly => "weekly",
        Frequency::Monthly => "monthly",
        Frequency::Quarterly => "quarterly",
        Frequency::Annual => "annual",
        Frequency::Irregular => "irregular",
    }
}

async fn load_dataflows(pool: &PgPool, query: &DataflowsQuery) -> Result<Vec<Dataflow>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, source_id, name, description, dimensions, measures,
                frequency, license, attribution, source_url
         FROM dataflows
         WHERE ($1::text IS NULL OR source_id = $1)
           AND ($2::text IS NULL OR frequency = $2)
         ORDER BY source_id, id",
    )
    .bind(query.source.as_ref().map(SourceId::as_str))
    .bind(query.frequency.map(frequency_as_str))
    .fetch_all(pool)
    .await?;

    rows.into_iter().map(dataflow_from_row).collect()
}

async fn load_dataflow(pool: &PgPool, id: &DataflowId) -> Result<Dataflow, ApiError> {
    let row = sqlx::query(
        "SELECT id, source_id, name, description, dimensions, measures,
                frequency, license, attribution, source_url
         FROM dataflows
         WHERE id = $1",
    )
    .bind(id.as_str())
    .fetch_optional(pool)
    .await?;

    row.map(dataflow_from_row)
        .transpose()?
        .ok_or_else(|| ApiError::NotFound(format!("dataflow `{id}`")))
}

async fn load_dimensions(
    pool: &PgPool,
    dataflow_id: &DataflowId,
) -> Result<Vec<Dimension>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, name, description, codelist_id, position
         FROM dimensions
         WHERE dataflow_id = $1
         ORDER BY position",
    )
    .bind(dataflow_id.as_str())
    .fetch_all(pool)
    .await?;

    rows.into_iter().map(dimension_from_row).collect()
}

async fn load_codelist(
    pool: &PgPool,
    dataflow_id: &DataflowId,
    dimension_id: &DimensionId,
) -> Result<Codelist, ApiError> {
    let row = sqlx::query(
        "SELECT c.id, c.name, c.description
         FROM dimensions d
         JOIN codelists c ON c.id = d.codelist_id
         WHERE d.dataflow_id = $1 AND d.id = $2",
    )
    .bind(dataflow_id.as_str())
    .bind(dimension_id.as_str())
    .fetch_optional(pool)
    .await?;

    let row = row.ok_or_else(|| {
        ApiError::NotFound(format!(
            "codelist for dataflow `{dataflow_id}` dimension `{dimension_id}`"
        ))
    })?;
    let codelist_id = codelist_id_from_str(row.try_get("id")?)?;
    let codes = load_codes(pool, &codelist_id).await?;

    Ok(Codelist {
        id: codelist_id,
        name: row.try_get("name")?,
        description: row.try_get("description")?,
        codes,
    })
}

async fn load_codes(pool: &PgPool, codelist_id: &CodelistId) -> Result<Vec<Code>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, codelist_id, name, description, parent_id
         FROM codes
         WHERE codelist_id = $1
         ORDER BY parent_id NULLS FIRST, id",
    )
    .bind(codelist_id.as_str())
    .fetch_all(pool)
    .await?;

    rows.into_iter().map(code_from_row).collect()
}

fn dataflow_from_row(row: sqlx::postgres::PgRow) -> Result<Dataflow, ApiError> {
    let frequency: String = row.try_get("frequency")?;
    let license: String = row.try_get("license")?;

    Ok(Dataflow {
        id: dataflow_id_from_str(row.try_get("id")?)?,
        source_id: source_id_from_str(row.try_get("source_id")?)?,
        name: row.try_get("name")?,
        description: row.try_get("description")?,
        dimensions: row
            .try_get::<Vec<String>, _>("dimensions")?
            .into_iter()
            .map(dimension_id_from_str)
            .collect::<Result<Vec<_>, _>>()?,
        measures: row
            .try_get::<Vec<String>, _>("measures")?
            .into_iter()
            .map(measure_id_from_str)
            .collect::<Result<Vec<_>, _>>()?,
        frequency: parse_db_frequency(&frequency)?,
        license: parse_license(&license),
        attribution: row.try_get("attribution")?,
        source_url: row.try_get("source_url")?,
    })
}

fn dimension_from_row(row: sqlx::postgres::PgRow) -> Result<Dimension, ApiError> {
    let position = row.try_get::<i16, _>("position")?;
    let position = u16::try_from(position).map_err(|err| {
        tracing::error!(%err, "database returned invalid dimension position");
        ApiError::Internal
    })?;

    Ok(Dimension {
        id: dimension_id_from_str(row.try_get("id")?)?,
        name: row.try_get("name")?,
        description: row.try_get("description")?,
        codelist_id: codelist_id_from_str(row.try_get("codelist_id")?)?,
        position,
    })
}

fn code_from_row(row: sqlx::postgres::PgRow) -> Result<Code, ApiError> {
    Ok(Code {
        id: code_id_from_str(row.try_get("id")?)?,
        codelist_id: codelist_id_from_str(row.try_get("codelist_id")?)?,
        name: row.try_get("name")?,
        description: row.try_get("description")?,
        parent_id: row
            .try_get::<Option<String>, _>("parent_id")?
            .map(code_id_from_str)
            .transpose()?,
    })
}

fn parse_license(value: &str) -> License {
    match value {
        "CC-BY-4.0" => License::CcBy40,
        "CC-BY-ND-4.0" => License::CcByNd40,
        "CC-BY-SA-4.0" => License::CcBySa40,
        "public-domain" => License::PublicDomain,
        other => License::Other(other.to_string()),
    }
}

fn parse_db_frequency(value: &str) -> Result<Frequency, ApiError> {
    parse_frequency(value).map_err(|err| {
        tracing::error!(frequency = value, %err, "database returned invalid dataflow frequency");
        ApiError::Internal
    })
}

fn source_id_from_str(value: String) -> Result<SourceId, ApiError> {
    SourceId::new(value).map_err(invalid_db_id)
}

fn dataflow_id_from_str(value: String) -> Result<DataflowId, ApiError> {
    DataflowId::new(value).map_err(invalid_db_id)
}

fn dimension_id_from_str(value: String) -> Result<DimensionId, ApiError> {
    DimensionId::new(value).map_err(invalid_db_id)
}

fn codelist_id_from_str(value: String) -> Result<CodelistId, ApiError> {
    CodelistId::new(value).map_err(invalid_db_id)
}

fn code_id_from_str(value: String) -> Result<CodeId, ApiError> {
    CodeId::new(value).map_err(invalid_db_id)
}

fn measure_id_from_str(value: String) -> Result<MeasureId, ApiError> {
    MeasureId::new(value).map_err(invalid_db_id)
}

fn invalid_db_id(err: au_kpis_domain::ids::IdError) -> ApiError {
    tracing::error!(%err, "database returned invalid identifier");
    ApiError::Internal
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_filters_and_rejects_unsupported_frequency() {
        let query = parse_dataflows_query(Some("source=abs&frequency=quarterly")).unwrap();

        assert_eq!(query.source.as_ref().map(SourceId::as_str), Some("abs"));
        assert_eq!(query.frequency, Some(Frequency::Quarterly));

        let err = parse_dataflows_query(Some("frequency=hourly")).unwrap_err();
        assert!(err.to_string().contains("frequency"));

        let err = parse_dataflows_query(Some("source=bad%00source")).unwrap_err();
        assert!(err.to_string().contains("source"));
    }

    #[test]
    fn list_cache_key_is_stable_for_filters() {
        let query = DataflowsQuery {
            source: Some(SourceId::new("abs").unwrap()),
            frequency: Some(Frequency::Quarterly),
        };

        assert_eq!(
            list_cache_key(&query),
            "api:dataflows:list:source=abs:frequency=quarterly"
        );

        assert_eq!(
            list_cache_key(&DataflowsQuery {
                source: None,
                frequency: None,
            }),
            "api:dataflows:list:source=-:frequency=-"
        );
    }

    #[test]
    fn frequency_labels_roundtrip_for_all_public_values() {
        for (label, expected) in [
            ("daily", Frequency::Daily),
            ("weekly", Frequency::Weekly),
            ("monthly", Frequency::Monthly),
            ("quarterly", Frequency::Quarterly),
            ("annual", Frequency::Annual),
            ("irregular", Frequency::Irregular),
        ] {
            let frequency = parse_frequency(label).expect("parse frequency");
            assert_eq!(frequency, expected);
            assert_eq!(frequency_as_str(frequency), label);
            assert_eq!(
                parse_db_frequency(label).expect("parse db frequency"),
                expected
            );
        }

        assert!(matches!(
            parse_db_frequency("hourly"),
            Err(ApiError::Internal)
        ));
    }

    #[test]
    fn license_labels_map_to_domain_variants() {
        assert_eq!(parse_license("CC-BY-4.0"), License::CcBy40);
        assert_eq!(parse_license("CC-BY-ND-4.0"), License::CcByNd40);
        assert_eq!(parse_license("CC-BY-SA-4.0"), License::CcBySa40);
        assert_eq!(parse_license("public-domain"), License::PublicDomain);
        assert_eq!(
            parse_license("custom"),
            License::Other("custom".to_string())
        );
    }
}
