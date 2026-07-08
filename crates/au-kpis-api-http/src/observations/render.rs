use axum::{body::Body, http::HeaderValue};
use sqlx::PgPool;

use super::{ObservationsMetadata, ParsedObservationsQuery, query_plan};

/// HTTP content type for a planned observations response format.
pub(crate) fn content_type_for_format(format: query_plan::ResponseFormat) -> HeaderValue {
    match format {
        query_plan::ResponseFormat::Json => HeaderValue::from_static("application/json"),
        query_plan::ResponseFormat::Csv => HeaderValue::from_static("text/csv; charset=utf-8"),
        query_plan::ResponseFormat::Parquet => {
            HeaderValue::from_static("application/vnd.apache.parquet")
        }
    }
}

/// Render an observations query through the selected streaming adapter.
pub(crate) fn body_for_query(
    pool: PgPool,
    query: ParsedObservationsQuery,
    metadata: ObservationsMetadata,
) -> Body {
    match query.format {
        super::ResponseFormat::Json => {
            Body::from_stream(super::json_observations_stream(pool, query, metadata))
        }
        super::ResponseFormat::Csv => {
            Body::from_stream(super::csv_observations_stream(pool, query, metadata))
        }
        super::ResponseFormat::Parquet => {
            Body::from_stream(super::parquet_observations_stream(pool, query, metadata))
        }
    }
}
