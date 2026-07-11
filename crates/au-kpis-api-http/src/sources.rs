//! `/v1/sources` governed source catalog endpoints.

use std::collections::BTreeMap;

use au_kpis_source_register::{
    OwnerArea, OwnerRole, SourceRegisterDataflow, SourceStatus, load_source_register,
};
use axum::{
    Json,
    extract::Path,
    http::{HeaderValue, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ApiError;

const SOURCE_CACHE_CONTROL: &str = "public, max-age=3600, stale-while-revalidate=86400";

/// Response envelope for `GET /v1/sources`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SourcesResponse {
    /// Governed sources ordered by source id.
    pub sources: Vec<SourceCatalogEntry>,
}

/// One source and its governed dataflows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SourceCatalogEntry {
    /// Stable source id.
    pub source_id: String,
    /// Dataflows governed for this source.
    pub dataflows: Vec<SourceCatalogDataflow>,
}

/// Governed source-dataflow metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SourceCatalogDataflow {
    /// Stable dataflow id.
    pub dataflow_id: String,
    /// Source-register governance status.
    pub status: String,
    /// Coverage state exposed to catalog consumers.
    pub coverage_state: String,
    /// Owning implementation area.
    pub owner_area: String,
    /// Accountable production role, when assigned.
    pub owner_role: Option<String>,
    /// Canonical source and citation URL.
    pub source_url: String,
    /// Source licence or terms identifier.
    pub license: String,
    /// Required attribution.
    pub attribution: String,
    /// Expected source cadence.
    pub cadence: String,
    /// Durable discovery schedule for active sources.
    pub schedule: Option<SourceSchedule>,
    /// Upstream timeout and rate policy for active sources.
    pub request_policy: Option<SourceRequestPolicy>,
    /// Soft and hard freshness thresholds for active sources.
    pub freshness_policy: Option<SourceFreshnessPolicy>,
    /// Range, cardinality, and partial-row policy for active sources.
    pub validation_policy: Option<SourceValidationPolicy>,
    /// Repository-relative representative fixture or reviewed snapshot.
    pub fixture_reference: Option<String>,
}

/// Public source discovery schedule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SourceSchedule {
    /// Five-field cron expression.
    pub cron: String,
    /// IANA timezone name.
    pub timezone: String,
}

/// Public upstream request policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SourceRequestPolicy {
    /// Per-request timeout in seconds.
    pub timeout_seconds: u64,
    /// Maximum steady-state requests per minute.
    pub max_requests_per_minute: u32,
    /// Maximum short request burst.
    pub burst: u32,
}

/// Public source freshness policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SourceFreshnessPolicy {
    /// Age in seconds after which data is soft-stale.
    pub soft_after_seconds: u64,
    /// Age in seconds after which data is hard-expired.
    pub hard_after_seconds: u64,
}

/// Public source validation policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SourceValidationPolicy {
    /// Named adapter range rule.
    pub range_rule: String,
    /// Maximum series cardinality for one generation.
    pub max_series_cardinality: u64,
    /// Whether partial rows may publish.
    pub allow_partial_rows: bool,
}

/// `GET /v1/sources`.
#[utoipa::path(
    get,
    path = "/v1/sources",
    operation_id = "listSources",
    responses(
        (status = 200, description = "Governed source catalog.", body = SourcesResponse, content_type = "application/json", headers(
            ("Cache-Control" = String, description = "Public CDN cache policy.")
        )),
        (status = 500, description = "Invalid server source register.", body = crate::ProblemDetails, content_type = "application/problem+json")
    ),
    tag = "sources"
)]
pub async fn list_sources() -> Result<Response, ApiError> {
    Ok(catalog_response(SourcesResponse {
        sources: source_catalog()?,
    }))
}

/// `GET /v1/sources/{source_id}`.
#[utoipa::path(
    get,
    path = "/v1/sources/{source_id}",
    operation_id = "getSource",
    params(
        ("source_id" = String, Path, min_length = 1, max_length = 128, description = "Source id.")
    ),
    responses(
        (status = 200, description = "Governed source details.", body = SourceCatalogEntry, content_type = "application/json", headers(
            ("Cache-Control" = String, description = "Public CDN cache policy.")
        )),
        (status = 404, description = "Source not found.", body = crate::ProblemDetails, content_type = "application/problem+json"),
        (status = 500, description = "Invalid server source register.", body = crate::ProblemDetails, content_type = "application/problem+json")
    ),
    tag = "sources"
)]
pub async fn get_source(Path(source_id): Path<String>) -> Result<Response, ApiError> {
    let source = source_catalog()?
        .into_iter()
        .find(|source| source.source_id == source_id)
        .ok_or_else(|| ApiError::NotFound(format!("source `{source_id}`")))?;
    Ok(catalog_response(source))
}

fn source_catalog() -> Result<Vec<SourceCatalogEntry>, ApiError> {
    let register = load_source_register().map_err(|error| {
        tracing::error!(%error, "checked-in source register is invalid");
        ApiError::Internal
    })?;
    let mut sources = BTreeMap::<String, Vec<SourceCatalogDataflow>>::new();

    for dataflow in register.dataflows {
        sources
            .entry(dataflow.source_id.clone())
            .or_default()
            .push(public_dataflow(dataflow));
    }

    Ok(sources
        .into_iter()
        .map(|(source_id, mut dataflows)| {
            dataflows.sort_by(|left, right| left.dataflow_id.cmp(&right.dataflow_id));
            SourceCatalogEntry {
                source_id,
                dataflows,
            }
        })
        .collect())
}

fn public_dataflow(dataflow: SourceRegisterDataflow) -> SourceCatalogDataflow {
    let status = source_status(dataflow.status).to_string();
    SourceCatalogDataflow {
        dataflow_id: dataflow.dataflow_id,
        coverage_state: status.clone(),
        status,
        owner_area: owner_area(dataflow.owner_area).to_string(),
        owner_role: dataflow.owner_role.map(owner_role).map(str::to_string),
        source_url: dataflow.canonical_url,
        license: dataflow.license,
        attribution: dataflow.attribution,
        cadence: dataflow.cadence,
        schedule: dataflow.schedule.map(|schedule| SourceSchedule {
            cron: schedule.cron,
            timezone: schedule.timezone,
        }),
        request_policy: dataflow.request_policy.map(|policy| SourceRequestPolicy {
            timeout_seconds: policy.timeout_seconds,
            max_requests_per_minute: policy.max_requests_per_minute,
            burst: policy.burst,
        }),
        freshness_policy: dataflow
            .freshness_policy
            .map(|policy| SourceFreshnessPolicy {
                soft_after_seconds: policy.soft_after_seconds,
                hard_after_seconds: policy.hard_after_seconds,
            }),
        validation_policy: dataflow
            .validation_policy
            .map(|policy| SourceValidationPolicy {
                range_rule: policy.range_rule,
                max_series_cardinality: policy.max_series_cardinality,
                allow_partial_rows: policy.allow_partial_rows,
            }),
        fixture_reference: dataflow.fixture_reference,
    }
}

fn source_status(status: SourceStatus) -> &'static str {
    match status {
        SourceStatus::Active => "active",
        SourceStatus::ManualPending => "manual_pending",
        SourceStatus::VisibleUnscored => "visible_unscored",
        SourceStatus::CoverageGap => "coverage_gap",
        SourceStatus::LicensedFeed => "licensed_feed",
        SourceStatus::Placeholder => "placeholder",
        SourceStatus::Retired => "retired",
    }
}

fn owner_area(area: OwnerArea) -> &'static str {
    match area {
        OwnerArea::Adapter => "adapter",
        OwnerArea::Scorecard => "scorecard",
        OwnerArea::Curated => "curated",
        OwnerArea::Licensed => "licensed",
        OwnerArea::Experimental => "experimental",
    }
}

fn owner_role(role: OwnerRole) -> &'static str {
    match role {
        OwnerRole::Platform => "platform",
        OwnerRole::Data => "data",
        OwnerRole::Api => "api",
        OwnerRole::Web => "web",
        OwnerRole::ProductMethodology => "product_methodology",
    }
}

fn catalog_response<T: Serialize>(body: T) -> Response {
    let mut response = Json(body).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(SOURCE_CACHE_CONTROL),
    );
    response
}

#[cfg(test)]
mod tests {
    use axum::{
        body::to_bytes,
        extract::Path,
        http::{StatusCode, header},
        response::IntoResponse,
    };

    use super::{get_source, list_sources};

    #[tokio::test]
    async fn source_catalog_exposes_active_runtime_contracts() {
        let response = list_sources().await.expect("list source catalog");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "public, max-age=3600, stale-while-revalidate=86400"
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("parse response");
        let abs = body["sources"]
            .as_array()
            .expect("source array")
            .iter()
            .find(|source| source["source_id"] == "abs")
            .expect("ABS source");
        let cpi = abs["dataflows"]
            .as_array()
            .expect("ABS dataflows")
            .iter()
            .find(|dataflow| dataflow["dataflow_id"] == "abs.cpi")
            .expect("ABS CPI");
        assert_eq!(cpi["status"], "active");
        assert_eq!(cpi["owner_role"], "data");
        assert_eq!(cpi["schedule"]["timezone"], "Australia/Sydney");
        assert_eq!(cpi["validation_policy"]["allow_partial_rows"], false);
    }

    #[tokio::test]
    async fn source_detail_returns_problem_for_unknown_source() {
        let error = get_source(Path("missing".to_string()))
            .await
            .expect_err("missing source should fail");
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read problem response");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("parse problem");
        assert_eq!(body["status"], 404);
        assert_eq!(body["detail"], "source `missing`");
    }
}
